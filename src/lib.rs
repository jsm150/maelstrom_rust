use std::{collections::HashSet, io::Write, sync::Arc};

use anyhow::{Ok, bail};
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncWrite, AsyncWriteExt}, sync::{Mutex, mpsc::{self, Receiver, Sender}}};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    src: String,
    dest: String,
    body: Payload,
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
    Read {
        msg_id: usize,
    },
    Topology {
        msg_id: usize,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    InitOk {
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
    message_sender: Sender<Message>
}

impl Node {
    pub async fn init_and_recv<W: AsyncWrite + Send + Unpin + 'static>(
        message: Message,
        writer: W
    ) -> anyhow::Result<Self> {
        if let Payload::Request(RequestPayload::Init {
            msg_id, node_id, node_ids
        }) = message.body
        {
            let (message_sender, recv) = mpsc::channel(1024);
            
            let node = Self {
                id: node_id,
                msg_seq: 0,
                messages: HashSet::new(),
                neighbors: node_ids, 
                message_sender
            };
            
            let message_handler = MessageHandler::new(writer, recv);
            message_handler.run_message_handling();
            
            let res = Message {
                src: node.id.to_string(),
                dest: message.src,
                body: Payload::Response(ResponsePayload::InitOk {
                    in_reply_to: msg_id,
                }),
            };

            node.message_sender.send(res).await?;

            Ok(node)
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
                            body: Payload::Request(RequestPayload::Broadcast { msg_id: self.msg_seq, message })
                        };

                        self.msg_seq += 1;

                        self.message_sender.send(broadcast_message).await?;
                    }
                }

                Payload::Response(ResponsePayload::BroadcastOk {
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
            RequestPayload::Topology { msg_id } => Payload::Response(ResponsePayload::TopologyOk {
                msg_id: self.msg_seq,
                in_reply_to: msg_id,
            }),
        };

        let res = Message {
            dest: sender.to_string(),
            src: self.id.to_string(),
            body,
        };
        self.message_sender.send(res).await?;

        self.msg_seq += 1;

        Ok(())
    }

    pub async fn recv(
        &mut self,
        message: Message,
    ) -> anyhow::Result<()> {

        match message.body {
            Payload::Request(request_payload) => self.response(&message.src, request_payload).await?,
            Payload::Response(response_payload) => match response_payload {
                ResponsePayload::BroadcastOk { msg_id, in_reply_to } => {

                },
                _ => { }
            },
        };

        Ok(())
    }

    
    
}

struct MessageHandler<W> 
where W: AsyncWrite {
    recv: mpsc::Receiver<Message>,
    writer: W
}

impl<W> MessageHandler<W> 
where W: AsyncWrite + Send + 'static + Unpin {
    pub fn new(writer: W, recv: Receiver<Message>) -> Self {
        Self {
            writer,
            recv
        }
    }

    pub fn run_message_handling(mut self) {
        tokio::spawn(async move {
            while let Some(message) = self.recv.recv().await {
                let mut bytes = serde_json::to_vec(&message)?;
                bytes.push(b'\n');

                self.writer.write_all(&bytes).await?;
                self.writer.flush().await?;
            }

            Ok(())
        });
    }

}
