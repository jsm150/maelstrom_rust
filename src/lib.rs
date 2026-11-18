use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

use anyhow::bail;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{
        Mutex,
        mpsc::{self, Receiver, Sender},
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    src: String,
    dest: String,
    body: Payload,
}

#[derive(Debug)]
pub enum Packet {
    Outgoing(Message),
    Incoming(Message),
}

impl Message {
    fn unique_id(&self) -> String {
        match &self.body {
            Payload::Request(body) => {
                format!("{}-{}", self.dest, body.msg_id)
            }
            Payload::Response(body) => {
                format!("{}-{}", self.src, body.in_reply_to)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Echo {
        echo: String,
    },
    Generate,
    Broadcast {
        message: usize,
    },
    Unicast {
        messages: Vec<usize>,
    },
    Read,
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    InitOk,
    EchoOk { echo: String },
    GenerateOk { id: String },
    BroadcastOk,
    UnicastOk,
    ReadOk { messages: Vec<usize> },
    TopologyOk,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestBody {
    msg_id: usize,
    #[serde(flatten)]
    payload: RequestPayload,
}

#[derive(Debug, Serialize, Deserialize)]
struct ResponseBody {
    msg_id: usize,
    in_reply_to: usize,
    #[serde(flatten)]
    payload: ResponsePayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum Payload {
    Request(RequestBody),
    Response(ResponseBody),
}

#[derive(Clone)]
struct MessageNumbering {
    msg_seq: Arc<AtomicUsize>,
}

impl MessageNumbering {
    fn new() -> Self {
        Self {
            msg_seq: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn next(&self) -> usize {
        self.msg_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }
}

pub struct Node {
    id: String,
    msg_seq: MessageNumbering,
    messages: HashSet<usize>,
    message_sender: Sender<Packet>,
    batch_sender: Sender<RequestPayload>,
}

impl Node {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub async fn init_and_recv(message: Message) -> anyhow::Result<(Self, Receiver<Packet>)> {
        if let Payload::Request(RequestBody {
            msg_id,
            payload: RequestPayload::Init { node_id, node_ids },
        }) = message.body
        {
            let (message_sender, recv) = mpsc::channel(1024);
            let (batch_sender, batch_recv) = mpsc::channel(1024);

            let neighbors = node_ids.into_iter().filter(|n| *n != node_id).collect();
            let msg_seq: MessageNumbering = MessageNumbering::new();

            let node = Self {
                id: node_id.clone(),
                msg_seq: msg_seq.clone(),
                messages: HashSet::new(),
                message_sender: message_sender.clone(),
                batch_sender,
            };

            tokio::spawn(
                BroadcastBatch::new(node_id, batch_recv, message_sender, msg_seq, neighbors)
                    .run(Duration::from_millis(700)),
            );

            let res = Message {
                src: node.id.to_string(),
                dest: message.src,
                body: Payload::Response(ResponseBody {
                    msg_id: 0,
                    in_reply_to: msg_id,
                    payload: ResponsePayload::InitOk,
                }),
            };

            node.message_sender.send(Packet::Outgoing(res)).await?;

            Ok((node, recv))
        } else {
            bail!("Node의 init 함수는 Payload가 init인 메세지만 받을 수 있습니다.")
        }
    }

    async fn response(&mut self, sender: &str, body: RequestBody) -> anyhow::Result<()> {
        let RequestBody { msg_id, payload } = body;
        let response_id = self.msg_seq.next();
        let response_payload = match payload {
            RequestPayload::Echo { echo } => ResponsePayload::EchoOk { echo },
            RequestPayload::Generate => ResponsePayload::GenerateOk {
                id: format!("{}-{}", self.id, response_id),
            },
            RequestPayload::Broadcast { message } => {
                if self.messages.insert(message) {
                    self.batch_sender.send(payload).await?;
                }

                ResponsePayload::BroadcastOk
            }
            RequestPayload::Unicast { messages } => {
                messages.iter().for_each(|message| {
                    self.messages.insert(*message);
                });

                ResponsePayload::UnicastOk
            }
            RequestPayload::Init { .. } => bail!(
                "init 메세지는 init_and_recv에서 처리되어야 하는데도 recv함수에 전달되었습니다."
            ),
            RequestPayload::Read => ResponsePayload::ReadOk {
                messages: self.messages.iter().copied().collect(),
            },
            RequestPayload::Topology { .. } => ResponsePayload::TopologyOk,
        };

        let res = Message {
            dest: sender.to_string(),
            src: self.id.to_string(),
            body: Payload::Response(ResponseBody {
                msg_id: response_id,
                in_reply_to: msg_id,
                payload: response_payload,
            }),
        };
        self.message_sender.send(Packet::Outgoing(res)).await?;

        Ok(())
    }

    pub async fn recv(&mut self, message: Message) -> anyhow::Result<()> {
        match message.body {
            Payload::Request(request_body) => self.response(&message.src, request_body).await?,
            Payload::Response(response_body) => {
                self.message_sender
                    .send(Packet::Incoming(Message {
                        src: message.src,
                        dest: message.dest,
                        body: Payload::Response(response_body),
                    }))
                    .await?;
            }
        };

        Ok(())
    }
}

pub struct MessageHandler<W>
where
    W: AsyncWrite,
{
    recv: mpsc::Receiver<Packet>,
    writer: Arc<Mutex<W>>,
    sending_state: Arc<Mutex<HashSet<String>>>,
}

impl<W> MessageHandler<W>
where
    W: AsyncWrite + Send + 'static + Unpin,
{
    pub fn new(writer: W, recv: Receiver<Packet>) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
            recv,
            sending_state: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn run_message_handling(mut self) -> anyhow::Result<()> {
        while let Some(packet) = self.recv.recv().await {
            match packet {
                Packet::Incoming(message) => {
                    let message_id = message.unique_id();
                    self.sending_state.lock().await.remove(&message_id);
                }
                Packet::Outgoing(message) => match message.body {
                    Payload::Request(_) => {
                        tokio::spawn(Self::send_until_done(
                            message,
                            self.writer.clone(),
                            self.sending_state.clone(),
                        ));
                    }
                    Payload::Response(_) => {
                        Self::send(&message, self.writer.clone()).await?;
                    }
                },
            }
        }

        Ok(())
    }

    async fn send(message: &Message, writer: Arc<Mutex<W>>) -> anyhow::Result<()> {
        let mut bytes = serde_json::to_vec(&message)?;
        bytes.push(b'\n');

        writer.lock().await.write_all(&bytes).await?;

        writer.lock().await.flush().await?;

        Ok(())
    }

    async fn send_until_done(
        message: Message,
        writer: Arc<Mutex<W>>,
        sending_state: Arc<Mutex<HashSet<String>>>,
    ) -> anyhow::Result<()> {
        sending_state.lock().await.insert(message.unique_id());

        loop {
            Self::send(&message, writer.clone()).await?;

            tokio::time::sleep(Duration::from_millis(500)).await;

            if !sending_state.lock().await.contains(&message.unique_id()) {
                break;
            }
        }

        Ok(())
    }
}

struct BroadcastBatch {
    node_id: String,
    recv: Receiver<RequestPayload>,
    packet_sender: Sender<Packet>,
    msg_seq: MessageNumbering,
    neighbors: Vec<String>,
}

impl BroadcastBatch {
    pub fn new(
        node_id: String,
        recv: Receiver<RequestPayload>,
        packet_sender: Sender<Packet>,
        msg_seq: MessageNumbering,
        neighbors: Vec<String>,
    ) -> Self {
        Self {
            node_id,
            recv,
            packet_sender,
            msg_seq,
            neighbors,
        }
    }

    pub async fn run(mut self, delay: Duration) -> anyhow::Result<()> {
        while !self.recv.is_closed() || !self.recv.is_empty() {
            tokio::time::sleep(delay).await;

            let mut messages = vec![];
            while let Ok(RequestPayload::Broadcast { message }) = self.recv.try_recv() {
                messages.push(message);
            }

            if messages.is_empty() {
                continue;
            }

            for next_node in &self.neighbors {
                let broadcast_message = Message {
                    dest: next_node.clone(),
                    src: self.node_id.clone(),
                    body: Payload::Request(RequestBody {
                        msg_id: self.msg_seq.next(),
                        payload: RequestPayload::Unicast {
                            messages: messages.clone(),
                        },
                    }),
                };

                self.packet_sender
                    .send(Packet::Outgoing(broadcast_message))
                    .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
