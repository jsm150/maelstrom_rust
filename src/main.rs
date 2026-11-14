use std::io::{self, BufRead};
use anyhow::bail;
use maelstrom_rust::{Message, MessageWriter, Node};


fn main() -> anyhow::Result<()> {
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();

    let stdout = io::stdout();
    let mut writer = stdout.lock();
    let mut message_writer = MessageWriter::new(&mut writer);

    let Some(first_line) = lines.next() else {
        bail!("init 메시지를 받기 전에 입력이 종료되었습니다");
    };

    let first_line = first_line?;
    let init: Message = serde_json::from_str(&first_line)?;
    let mut node = Node::init_and_recv(init, &mut message_writer)?;

    for line in lines {
        let line = line?;
        let message: Message = serde_json::from_str(&line)?;

        node.recv(message)?;
    }

    Ok(())
}
