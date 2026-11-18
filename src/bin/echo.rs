use anyhow::Result;
use maelstrom_rust::{Body, Init, Message, Node, NodeTx, Payload, main_loop};
use serde::{Deserialize, Serialize};

struct EchoPayload;

impl Payload for EchoPayload {
    type Request = EchoRequest;
    type Response = EchoResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EchoRequest {
    Echo { echo: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EchoResponse {
    EchoOk { echo: String },
}

struct EchoNode {
    tx: NodeTx<EchoPayload>,
}

impl Node for EchoNode {
    type Payload = EchoPayload;

    fn init(_: Init, tx: NodeTx<EchoPayload>) -> Result<Self> {
        Ok(Self { tx })
    }

    async fn recv(&mut self, message: Message<EchoPayload>) -> Result<()> {
        if let Body::Request {
            msg_id,
            payload: EchoRequest::Echo { echo },
        } = message.body
        {
            self.tx
                .reply(&message.src, msg_id, |_| EchoResponse::EchoOk { echo })
                .await?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    main_loop::<EchoNode>().await
}
