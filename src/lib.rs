mod transport;

use std::future::Future;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, BufReader};

pub use transport::NodeTx;

/// 한 워크로드에서 사용하는 요청·응답 본문 타입을 지정한다. 각 본문은 `type` 필드로 메시지 종류를 구분한다.
pub trait Payload {
    type Request: Serialize + DeserializeOwned + Send + 'static;
    type Response: Serialize + DeserializeOwned + Send + 'static;
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Message<P: Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<P>,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged, bound = "")]
pub enum Body<P: Payload> {
    // Serde는 선언 순서대로 역직렬화를 시도하므로, in_reply_to가 필요한 응답을 먼저 검사한다.
    Response {
        msg_id: usize,
        in_reply_to: usize,
        #[serde(flatten)]
        payload: P::Response,
    },
    Request {
        msg_id: usize,
        #[serde(flatten)]
        payload: P::Request,
    },
}

#[derive(Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

struct InitPayload;

impl Payload for InitPayload {
    type Request = InitRequest;
    type Response = InitResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InitRequest {
    Init(Init),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InitResponse {
    InitOk,
}

pub trait Node: Sized {
    type Payload: Payload;

    fn init(init: Init, tx: NodeTx<Self::Payload>) -> Result<Self>;
    /// 메시지를 하나씩 처리한다. 여기서 보낸 요청의 응답은 이후 호출에서 받는다.
    fn recv(&mut self, message: Message<Self::Payload>) -> impl Future<Output = Result<()>>;
}

pub async fn main_loop<N: Node>() -> Result<()> {
    run::<N>(BufReader::new(tokio::io::stdin()), tokio::io::stdout()).await
}

async fn run<N: Node>(
    input: impl AsyncBufRead + Unpin,
    mut output: impl AsyncWrite + Unpin,
) -> Result<()> {
    let mut lines = input.lines();
    let first = lines
        .next_line()
        .await?
        .context("init 메시지를 받기 전에 입력이 종료되었습니다")?;
    let message: Message<InitPayload> =
        serde_json::from_str(&first).context("init 메시지 해석 실패")?;
    let Body::Request {
        msg_id,
        payload: InitRequest::Init(init),
    } = message.body
    else {
        bail!("첫 메시지는 init 요청이어야 합니다");
    };

    let reply = Message::<InitPayload> {
        src: init.node_id.clone(),
        dest: message.src,
        body: Body::Response {
            msg_id: 0,
            in_reply_to: msg_id,
            payload: InitResponse::InitOk,
        },
    };
    let (tx, commands) = NodeTx::new(&init.node_id);
    let mut node = N::init(init, tx.clone())?;
    transport::write_frame(&mut output, &transport::encode(&reply)?).await?;

    let mut writer = std::pin::pin!(transport::write_messages(commands, output));
    let input_result = tokio::select! {
        result = async move {
            while let Some(line) = lines.next_line().await? {
                let message: Message<N::Payload> = serde_json::from_str(&line)?;
                if let Body::Response { in_reply_to, .. } = &message.body {
                    tx.acknowledge(&message.src, *in_reply_to).await?;
                }
                node.recv(message).await?;
            }
            Ok(())
        } => result,
        result = &mut writer => {
            result?;
            bail!("입력 처리 중 출력이 종료되었습니다");
        }
    };

    // 입력 처리가 끝나면 노드가 해제된다. 배치 태스크가 마지막 배치를 보내고 출력이 끝날 때까지 기다린다.
    writer.await?;
    input_result
}

#[cfg(test)]
mod tests;
