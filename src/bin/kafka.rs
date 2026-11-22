use std::collections::HashMap;

use anyhow::Result;
use maelstrom_rust::{Body, Init, Message, Node, NodeTx, Payload, main_loop};
use serde::{Deserialize, Serialize};

type Offsets = HashMap<String, usize>;

struct KafkaPayload;

impl Payload for KafkaPayload {
    type Request = KafkaRequest;
    type Response = KafkaResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum KafkaRequest {
    Send { key: String, msg: usize },
    Poll { offsets: Offsets },
    CommitOffsets { offsets: Offsets },
    ListCommittedOffsets { keys: Vec<String> },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[expect(
    clippy::enum_variant_names,
    reason = "Maelstrom 응답 타입 이름은 _ok로 끝난다"
)]
enum KafkaResponse {
    SendOk {
        offset: usize,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsetsOk,
    ListCommittedOffsetsOk {
        offsets: Offsets,
    },
}

struct KafkaNode {
    logs: HashMap<String, Vec<usize>>,
    committed_offsets: Offsets,
    tx: NodeTx<KafkaPayload>,
}

impl Node for KafkaNode {
    type Payload = KafkaPayload;

    fn init(_: Init, tx: NodeTx<KafkaPayload>) -> Result<Self> {
        Ok(Self {
            logs: HashMap::new(),
            committed_offsets: Offsets::new(),
            tx,
        })
    }

    async fn recv(&mut self, message: Message<KafkaPayload>) -> Result<()> {
        let Body::Request { msg_id, payload } = message.body else {
            return Ok(());
        };
        let response = match payload {
            KafkaRequest::Send { key, msg } => {
                let log = self.logs.entry(key).or_default();
                let offset = log.len();
                log.push(msg);
                KafkaResponse::SendOk { offset }
            }
            KafkaRequest::Poll { offsets } => {
                let mut msgs = HashMap::new();
                for (key, start) in offsets {
                    if let Some(log) = self.logs.get(&key)
                        && start < log.len()
                    {
                        let entries = (start..).zip(log[start..].iter().copied()).collect();
                        msgs.insert(key, entries);
                    }
                }
                KafkaResponse::PollOk { msgs }
            }
            KafkaRequest::CommitOffsets { offsets } => {
                for (key, offset) in offsets {
                    let committed = self.committed_offsets.entry(key).or_default();
                    *committed = (*committed).max(offset);
                }
                KafkaResponse::CommitOffsetsOk
            }
            KafkaRequest::ListCommittedOffsets { keys } => {
                let mut offsets = Offsets::new();
                for key in keys {
                    if let Some(&offset) = self.committed_offsets.get(&key) {
                        offsets.insert(key, offset);
                    }
                }
                KafkaResponse::ListCommittedOffsetsOk { offsets }
            }
        };
        self.tx.reply(&message.src, msg_id, |_| response).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    main_loop::<KafkaNode>().await
}
