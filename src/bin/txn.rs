#[path = "txn/store.rs"]
mod store;

use store::{Replication, TxnOp, TxnStore};

use anyhow::Result;
use maelstrom_rust::{Body, Init, Message, Node, NodeTx, Payload, main_loop};
use serde::{Deserialize, Serialize};

struct TxnPayload;

impl Payload for TxnPayload {
    type Request = TxnRequest;
    type Response = TxnResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TxnRequest {
    Txn { txn: Vec<TxnOp> },
    Replicate(Replication),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TxnResponse {
    TxnOk { txn: Vec<TxnOp> },
    ReplicateOk,
}

struct TxnNode {
    tx: NodeTx<TxnPayload>,
    peers: Vec<String>,
    store: TxnStore,
}

impl Node for TxnNode {
    type Payload = TxnPayload;

    fn init(init: Init, tx: NodeTx<TxnPayload>) -> Result<Self> {
        Ok(Self {
            tx,
            peers: init
                .node_ids
                .into_iter()
                .filter(|id| *id != init.node_id)
                .collect(),
            store: TxnStore::new(init.node_id),
        })
    }

    async fn recv(&mut self, message: Message<TxnPayload>) -> Result<()> {
        let Body::Request { msg_id, payload } = message.body else {
            return Ok(());
        };

        match payload {
            TxnRequest::Txn { mut txn } => {
                let replication = self.store.execute(&mut txn).await;
                self.tx
                    .reply(&message.src, msg_id, |_| TxnResponse::TxnOk { txn })
                    .await?;
                if let Some(replication) = replication {
                    for peer in &self.peers {
                        self.tx
                            .request_with_retry(peer, TxnRequest::Replicate(replication.clone()))
                            .await?;
                    }
                }
            }
            TxnRequest::Replicate(replication) => {
                self.store.merge(replication).await;
                self.tx
                    .reply(&message.src, msg_id, |_| TxnResponse::ReplicateOk)
                    .await?;
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    main_loop::<TxnNode>().await
}
