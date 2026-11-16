use anyhow::bail;
use maelstrom_rust::{Message, MessageHandler, Node};
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    let Some(first_line) = lines.next_line().await? else {
        bail!("init 메시지를 받기 전에 입력이 종료되었습니다");
    };

    let init: Message = serde_json::from_str(&first_line)?;
    let (mut node, recv) = Node::init_and_recv(init).await?;

    let handler = MessageHandler::new(tokio::io::stdout(), recv);
    let mut output_task = tokio::spawn(handler.run_message_handling());

    let input_result = tokio::select! {
        // 입력 처리: EOF 또는 오류가 발생하면 완료
        result = async {
            while let Some(line) = lines.next_line().await? {
                let message: Message = serde_json::from_str(&line)?;
                node.recv(message).await?;
            }

            Ok::<(), anyhow::Error>(())
        } => result,

        // 입력 처리 중에도 출력 태스크의 종료를 감지
        result = &mut output_task => {
            result??;
            bail!("입력 처리 중 출력 태스크가 종료되었습니다");
        }
    };

    // 입력 처리가 먼저 끝난 경우: 남은 출력까지 기다림
    drop(node);
    output_task.await??;

    input_result
}
