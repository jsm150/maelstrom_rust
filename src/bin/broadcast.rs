use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::{Result, anyhow};
use maelstrom_rust::{Body, Init, Message, Node, NodeTx, Payload, main_loop};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

struct BroadcastPayload;

impl Payload for BroadcastPayload {
    type Request = BroadcastRequest;
    type Response = BroadcastResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BroadcastRequest {
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

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[expect(
    clippy::enum_variant_names,
    reason = "Maelstrom 응답 타입 이름은 _ok로 끝난다"
)]
enum BroadcastResponse {
    BroadcastOk,
    UnicastOk,
    ReadOk { messages: Vec<usize> },
    TopologyOk,
}

struct BroadcastNode {
    messages: HashSet<usize>,
    tx: NodeTx<BroadcastPayload>,
    batch_sender: mpsc::Sender<usize>,
}

impl Node for BroadcastNode {
    type Payload = BroadcastPayload;

    fn init(init: Init, tx: NodeTx<BroadcastPayload>) -> Result<Self> {
        let (batch_sender, recv) = mpsc::channel(1024);
        let batch = BroadcastBatch {
            neighbors: init
                .node_ids
                .into_iter()
                .filter(|id| *id != init.node_id)
                .collect(),
            recv,
            tx: tx.clone(),
        };
        tokio::spawn(batch.run(Duration::from_millis(700)));
        Ok(Self {
            messages: HashSet::new(),
            tx,
            batch_sender,
        })
    }

    async fn recv(&mut self, message: Message<BroadcastPayload>) -> Result<()> {
        let Body::Request { msg_id, payload } = message.body else {
            return Ok(());
        };
        let reply = match payload {
            BroadcastRequest::Broadcast { message } => {
                if self.messages.insert(message) {
                    self.batch_sender
                        .send(message)
                        .await
                        .map_err(|_| anyhow!("방송 배치 채널이 닫혔습니다"))?;
                }
                BroadcastResponse::BroadcastOk
            }
            BroadcastRequest::Unicast { messages } => {
                self.messages.extend(messages);
                BroadcastResponse::UnicastOk
            }
            BroadcastRequest::Read => BroadcastResponse::ReadOk {
                messages: self.messages.iter().copied().collect(),
            },
            BroadcastRequest::Topology { .. } => BroadcastResponse::TopologyOk,
        };
        self.tx.reply(&message.src, msg_id, |_| reply).await
    }
}

struct BroadcastBatch {
    neighbors: Vec<String>,
    recv: mpsc::Receiver<usize>,
    tx: NodeTx<BroadcastPayload>,
}

impl BroadcastBatch {
    async fn run(mut self, delay: Duration) -> Result<()> {
        while !self.recv.is_closed() || !self.recv.is_empty() {
            tokio::time::sleep(delay).await;

            let mut messages = Vec::new();
            while let Ok(message) = self.recv.try_recv() {
                messages.push(message);
            }
            if messages.is_empty() {
                continue;
            }
            for peer in &self.neighbors {
                self.tx
                    .request_with_retry(
                        peer,
                        BroadcastRequest::Unicast {
                            messages: messages.clone(),
                        },
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    main_loop::<BroadcastNode>().await
}
