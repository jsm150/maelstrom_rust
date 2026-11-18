use anyhow::Result;
use maelstrom_rust::{Body, Init, Message, Node, NodeTx, Payload, main_loop};
use serde::{Deserialize, Serialize};

struct UniqueIdsPayload;

impl Payload for UniqueIdsPayload {
    type Request = GenerateRequest;
    type Response = GenerateResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GenerateRequest {
    Generate,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GenerateResponse {
    GenerateOk { id: String },
}

struct UniqueIdsNode {
    tx: NodeTx<UniqueIdsPayload>,
}

impl Node for UniqueIdsNode {
    type Payload = UniqueIdsPayload;

    fn init(_: Init, tx: NodeTx<UniqueIdsPayload>) -> Result<Self> {
        Ok(Self { tx })
    }

    async fn recv(&mut self, message: Message<UniqueIdsPayload>) -> Result<()> {
        if let Body::Request {
            msg_id,
            payload: GenerateRequest::Generate,
        } = message.body
        {
            self.tx
                .reply(&message.src, msg_id, |id| GenerateResponse::GenerateOk {
                    id: format!("{}-{id}", self.tx.node_id()),
                })
                .await?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    main_loop::<UniqueIdsNode>().await
}
