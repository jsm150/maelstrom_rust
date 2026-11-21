use std::collections::HashSet;

use anyhow::{Context, Result};
use maelstrom_rust::{Body, Init, Message, Node, NodeTx, Payload, main_loop};
use serde::{Deserialize, Serialize};

struct GCountPayload;

impl Payload for GCountPayload {
    type Request = GCountRequest;
    type Response = GCountResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GCountRequest {
    Add { delta: u64 },
    Read,
    Replicate { delta: u64 },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[expect(
    clippy::enum_variant_names,
    reason = "Maelstrom 응답 타입 이름은 _ok로 끝난다"
)]
enum GCountResponse {
    AddOk,
    ReadOk { value: u64 },
    ReplicateOk,
}

enum Role {
    Master { slaves: Vec<String> },
    Slave { master: String },
}

struct GCountNode {
    role: Role,
    value: u64,
    seen: HashSet<(String, usize)>,
    tx: NodeTx<GCountPayload>,
}

impl Node for GCountNode {
    type Payload = GCountPayload;

    fn init(init: Init, tx: NodeTx<Self::Payload>) -> Result<Self> {
        let master = init
            .node_ids
            .iter()
            .min()
            .context("노드 목록이 비어 있습니다")?;
        let role = if *master == init.node_id {
            let slaves = init
                .node_ids
                .into_iter()
                .filter(|id| *id != init.node_id)
                .collect();
            Role::Master { slaves }
        } else {
            Role::Slave {
                master: master.clone(),
            }
        };
        Ok(Self {
            role,
            value: 0,
            seen: HashSet::new(),
            tx,
        })
    }

    async fn recv(&mut self, message: Message<Self::Payload>) -> Result<()> {
        let Body::Request { msg_id, payload } = message.body else {
            return Ok(());
        };
        let key = (message.src.clone(), msg_id);
        let response = match payload {
            GCountRequest::Add { delta } => {
                if self.seen.insert(key) {
                    match &self.role {
                        Role::Master { slaves } => {
                            self.value += delta;
                            for slave in slaves {
                                self.tx
                                    .request_with_retry(slave, GCountRequest::Replicate { delta })
                                    .await?;
                            }
                        }
                        Role::Slave { master } => {
                            self.tx
                                .request_with_retry(master, GCountRequest::Add { delta })
                                .await?;
                        }
                    }
                }
                GCountResponse::AddOk
            }
            GCountRequest::Read => {
                self.seen.insert(key);
                GCountResponse::ReadOk { value: self.value }
            }
            GCountRequest::Replicate { delta } => {
                if !matches!(&self.role, Role::Slave { master } if *master == message.src) {
                    return Ok(());
                }
                if self.seen.insert(key) {
                    self.value += delta;
                }
                GCountResponse::ReplicateOk
            }
        };
        self.tx.reply(&message.src, msg_id, |_| response).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    main_loop::<GCountNode>().await
}
