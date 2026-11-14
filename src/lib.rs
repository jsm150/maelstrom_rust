use std::{collections::HashSet, io::Write};

use anyhow::{Ok, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    src: String,
    dest: String,
    body: Payload,
}

#[derive(Debug, Serialize, Deserialize)]
enum RequestPayload {
    
}

#[derive(Debug, Serialize, Deserialize)]
enum ResponsePayload {

}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {

    Request(RequestPayload),
    Response(ResponsePayload),

    Init {
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>
    },  
    InitOk {
        in_reply_to: usize,
    },
    Echo {
        msg_id: usize,
        echo: String,
    },
    EchoOk {
        msg_id: usize,
        in_reply_to: usize,
        echo: String,
    },
    Generate {
        msg_id: usize,
    },
    GenerateOk {
        msg_id: usize,
        in_reply_to: usize,
        id: String,
    },
    Broadcast {
        msg_id: usize,
        message: usize,
    },
    BroadcastOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    Read {
        msg_id: usize,
    },
    ReadOk {
        msg_id: usize,
        in_reply_to: usize,
        messages: Vec<usize>,
    },
    Topology {
        msg_id: usize,
    },
    TopologyOk {
        msg_id: usize,
        in_reply_to: usize,
    },
}

pub struct Node<'a, W: Write> {
    id: String,
    msg_seq: usize,
    messages: HashSet<usize>,
    neighbors: Vec<String>,
    message_writer: &'a mut MessageWriter<'a, W>
}

impl<'a, W: Write> Node<'a, W> {
    pub fn init_and_recv(
        message: Message,
        message_writer: &'a mut MessageWriter<'a, W>
    ) -> anyhow::Result<Self> {
        if let Payload::Init {
            msg_id, node_id, node_ids
        } = message.body
        {
            let node = Self {
                id: node_id,
                msg_seq: 0,
                messages: HashSet::new(),
                neighbors: node_ids, 
                message_writer
            };

            let res = Message {
                src: node.id.to_string(),
                dest: message.src,
                body: Payload::InitOk {
                    in_reply_to: msg_id,
                },
            };

            node.message_writer.write(res)?;

            Ok(node)
        } else {
            bail!("Node의 init 함수는 Payload가 init인 메세지만 받을 수 있습니다.")
        }
    }

    pub fn recv(
        &mut self,
        message: Message,
    ) -> anyhow::Result<()> {
        let body = match message.body {
            Payload::Init { .. } => bail!(
                "init 메세지는 init_and_recv에서 처리되어야 하는데도 recv함수에 전달되었습니다."
            ),
            Payload::InitOk { .. } => {
                bail!("init_ok 메세지를 받을 일이 없음에도 이 노드가 메세지를 받았습니다.")
            }
            Payload::Echo { echo, msg_id } => Some(Payload::EchoOk {
                msg_id: self.msg_seq,
                in_reply_to: msg_id,
                echo,
            }),
            Payload::EchoOk { .. } => {
                bail!("echo_ok 메세지를 받을 일이 없음에도 이 노드가 메세지를 받았습니다.")
            }
            Payload::Generate { msg_id } => Some(Payload::GenerateOk {
                msg_id: self.msg_seq,
                in_reply_to: msg_id,
                id: format!("{}-{}", self.id, self.msg_seq),
            }),
            Payload::GenerateOk { .. } => {
                bail!("generate_ok 메세지를 받을 일이 없음에도 이 노드가 메세지를 받았습니다.")
            }
            Payload::Broadcast { msg_id, message } => {

                if self.messages.insert(message) {
                    for next_node in &self.neighbors {
                        let broadcast_message = Message {
                            dest: next_node.clone(),
                            src: self.id.clone(),
                            body: Payload::Broadcast { msg_id: self.msg_seq, message }
                        };

                        self.msg_seq += 1;

                        self.message_writer.write(broadcast_message)?;
                    }
                }

                Some(Payload::BroadcastOk {
                    msg_id: self.msg_seq,
                    in_reply_to: msg_id,
                })
            }
            Payload::BroadcastOk { .. } => None,
            Payload::Read { msg_id } => Some(Payload::ReadOk {
                msg_id: self.msg_seq,
                in_reply_to: msg_id,
                messages: self.messages.iter().copied().collect(),
            }),
            Payload::ReadOk { .. } => {
                bail!("read_ok 메세지를 받을 일이 없음에도 이 노드가 메세지를 받았습니다.")
            }
            Payload::Topology { msg_id } => Some(Payload::TopologyOk {
                msg_id: self.msg_seq,
                in_reply_to: msg_id,
            }),
            Payload::TopologyOk { .. } => {
                bail!("topology_ok 메세지를 받을 일이 없음에도 이 노드가 메세지를 받았습니다.")
            }
        };

        if let Some(body) = body {
            let res = Message {
                dest: message.src,
                src: self.id.to_string(),
                body,
            };
            self.message_writer.write(res)?;

            self.msg_seq += 1;
        }

        Ok(())
    }

    
}


pub struct MessageWriter<'a, W> 
where W: Write {
    writer: &'a mut W
}

impl<'a, W> MessageWriter<'a, W>
where W: Write {
    pub fn new(writer: &'a mut W) -> Self {
        Self { writer }
    }

    pub fn write(&mut self, message: Message) -> anyhow::Result<()> {
        serde_json::to_writer(&mut *self.writer, &message)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;

        Ok(())
    }
}