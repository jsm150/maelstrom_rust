use std::{collections::{HashMap, HashSet}, sync::Arc, time::Duration};

use anyhow::{Ok, bail};
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncWrite, AsyncWriteExt}, sync::{Mutex, mpsc::{self, Receiver, Sender}}};

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
            Payload::Request(request_payload) => {
                let msg_id = match request_payload {
                    RequestPayload::Init { msg_id, .. } => msg_id,
                    RequestPayload::Echo { msg_id, .. } => msg_id,
                    RequestPayload::Generate { msg_id } => msg_id,
                    RequestPayload::Broadcast { msg_id, .. } => msg_id,
                    RequestPayload::Read { msg_id } => msg_id,
                    RequestPayload::Topology { msg_id, .. } => msg_id,
                    RequestPayload::Unicast { msg_id, .. } => msg_id,
                };

                format!("{}-{}", self.dest, msg_id)
            },
            Payload::Response(response_payload) => {
                let in_reply_to = match response_payload {
                    ResponsePayload::InitOk { in_reply_to, .. } => in_reply_to,
                    ResponsePayload::EchoOk { in_reply_to, .. } => in_reply_to,
                    ResponsePayload::GenerateOk { in_reply_to, .. } => in_reply_to,
                    ResponsePayload::BroadcastOk { in_reply_to, .. } => in_reply_to,
                    ResponsePayload::ReadOk { in_reply_to, .. } => in_reply_to,
                    ResponsePayload::TopologyOk { in_reply_to, .. } => in_reply_to,
                    ResponsePayload::UnicastOk { in_reply_to, .. } => in_reply_to,
                };

                format!("{}-{}", self.src, in_reply_to)
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Init {
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>
    },
    Echo {
        msg_id: usize,
        echo: String,
    },
    Generate {
        msg_id: usize,
    },
    Broadcast {
        msg_id: usize,
        message: usize,
    },
    Unicast {
        msg_id: usize,
        message: usize,
    },
    Read {
        msg_id: usize,
    },
    Topology {
        msg_id: usize,
        topology: HashMap<String, Vec<String>>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    InitOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    EchoOk {
        msg_id: usize,
        in_reply_to: usize,
        echo: String,
    },
    GenerateOk {
        msg_id: usize,
        in_reply_to: usize,
        id: String,
    },
    BroadcastOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    UnicastOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    ReadOk {
        msg_id: usize,
        in_reply_to: usize,
        messages: Vec<usize>,
    },
    TopologyOk {
        msg_id: usize,
        in_reply_to: usize,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum Payload {
    Request(RequestPayload),
    Response(ResponsePayload),
}

pub struct Node {
    id: String,
    msg_seq: usize,
    messages: HashSet<usize>,
    neighbors: Vec<String>,
    message_sender: Sender<Packet>
}

impl Node {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub async fn init_and_recv(
        message: Message,
    ) -> anyhow::Result<(Self, Receiver<Packet>)> {
        if let Payload::Request(RequestPayload::Init {
            msg_id, node_id, node_ids
        }) = message.body
        {
            let (message_sender, recv) = mpsc::channel(1024);
            let neighbors = node_ids.into_iter().filter(|n| *n != node_id).collect();

            let node = Self {
                id: node_id,
                msg_seq: 1,
                messages: HashSet::new(),
                neighbors,
                message_sender
            };
            
            
            let res = Message {
                src: node.id.to_string(),
                dest: message.src,
                body: Payload::Response(ResponsePayload::InitOk {
                    msg_id: 0,
                    in_reply_to: msg_id,
                }),
            };

            node.message_sender.send(Packet::Outgoing(res)).await?;

            Ok((node, recv))
        } else {
            bail!("Node의 init 함수는 Payload가 init인 메세지만 받을 수 있습니다.")
        }
    }

    async fn response(&mut self, sender: &str, payload: RequestPayload) -> anyhow::Result<()> {
        let body = match payload {
            RequestPayload::Echo { echo, msg_id } => Payload::Response(ResponsePayload::EchoOk {
                msg_id: self.msg_seq,
                in_reply_to: msg_id,
                echo,
            }),
            RequestPayload::Generate { msg_id } => Payload::Response(ResponsePayload::GenerateOk {
                msg_id: self.msg_seq,
                in_reply_to: msg_id,
                id: format!("{}-{}", self.id, self.msg_seq),
            }),
            RequestPayload::Broadcast { msg_id, message } => {

                if self.messages.insert(message) {
                    for next_node in &self.neighbors {
                        let broadcast_message = Message {
                            dest: next_node.clone(),
                            src: self.id.clone(),
                            body: Payload::Request(RequestPayload::Unicast { msg_id: self.msg_seq, message })
                        };

                        self.msg_seq += 1;

                        self.message_sender.send(Packet::Outgoing(broadcast_message)).await?;
                    }
                }

                Payload::Response(ResponsePayload::BroadcastOk {
                    msg_id: self.msg_seq,
                    in_reply_to: msg_id,
                })
            },
            RequestPayload::Unicast { msg_id, message } => {
                self.messages.insert(message);
                
                Payload::Response(ResponsePayload::UnicastOk {
                    msg_id: self.msg_seq,
                    in_reply_to: msg_id,
                })
            },
            RequestPayload::Init { .. } => bail!(
                "init 메세지는 init_and_recv에서 처리되어야 하는데도 recv함수에 전달되었습니다."
            ),
            RequestPayload::Read { msg_id } => Payload::Response(ResponsePayload::ReadOk {
                msg_id: self.msg_seq,
                in_reply_to: msg_id,
                messages: self.messages.iter().copied().collect(),
            }),
            RequestPayload::Topology { msg_id, .. } => {

                Payload::Response(ResponsePayload::TopologyOk {
                    msg_id: self.msg_seq,
                    in_reply_to: msg_id,
                })
            },
        };

        let res = Message {
            dest: sender.to_string(),
            src: self.id.to_string(),
            body,
        };
        self.message_sender.send(Packet::Outgoing(res)).await?;

        self.msg_seq += 1;

        Ok(())
    }

    pub async fn recv(
        &mut self,
        message: Message,
    ) -> anyhow::Result<()> {

        match message.body {
            Payload::Request(request_payload) => self.response(&message.src, request_payload).await?,
            Payload::Response(response_payload) => {
                self.message_sender.send(Packet::Incoming(Message {
                    src: message.src,
                    dest: message.dest,
                    body: Payload::Response(response_payload),
                })).await?;
            },
        };

        Ok(())
    }
    
}


pub struct MessageHandler<W>
where W: AsyncWrite {
    recv: mpsc::Receiver<Packet>,
    writer: Arc<Mutex<W>>,
    sending_state: Arc<Mutex<HashSet<String>>>
}

impl<W> MessageHandler<W> 
where W: AsyncWrite + Send + 'static + Unpin {
    pub fn new(writer: W, recv: Receiver<Packet>) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
            recv,
            sending_state: Arc::new(Mutex::new(HashSet::new()))
        }
    }

    pub async fn run_message_handling(mut self) -> anyhow::Result<()> {
        while let Some(packet) = self.recv.recv().await {
            match packet {
                Packet::Incoming(message) => {
                    let message_id = message.unique_id();
                    self.sending_state
                        .lock()
                        .await
                        .remove(&message_id);
                },
                Packet::Outgoing(message) => match message.body {
                        Payload::Request(_) => {
                            tokio::spawn(Self::send_until_done(message, self.writer.clone(), self.sending_state.clone()));
                        },
                        Payload::Response(_) => {
                            Self::send(&message, self.writer.clone()).await?;   
                        },
                    }
            }
        }

        Ok(())
    }


    async fn send(message: &Message, writer: Arc<Mutex<W>>) -> anyhow::Result<()> {
        let mut bytes = serde_json::to_vec(&message)?;
            bytes.push(b'\n');

            writer.lock()
                .await
                .write_all(&bytes)
                .await?;

            writer.lock()
                .await
                .flush()
                .await?;
        
        Ok(())
    }

    async fn send_until_done(message: Message, writer: Arc<Mutex<W>>, sending_state: Arc<Mutex<HashSet<String>>>) -> anyhow::Result<()> {
        sending_state
            .lock()
            .await
            .insert(message.unique_id());

        loop {

            Self::send(&message, writer.clone()).await?;

            tokio::time::sleep(Duration::from_millis(500)).await;

            if !sending_state
                .lock()
                .await
                .contains(&message.unique_id())
            {
                break;
            }
        }

        Ok(())
    }

}
