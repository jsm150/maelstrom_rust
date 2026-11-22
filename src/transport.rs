use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::mpsc,
    time::{Instant, sleep_until},
};

use crate::{Body, Message, Payload};

const RETRY_DELAY: Duration = Duration::from_millis(500);

pub(crate) enum Command<P: Payload> {
    SendOnce(Message<P>),
    SendUntilAck { message: Message<P>, id: usize },
    AckReceived { peer: String, in_reply_to: usize },
}

/// 복제본끼리 노드의 메시지 번호 발급기와 출력 큐를 공유하는 송신 핸들.
pub struct NodeTx<P: Payload> {
    node_id: Arc<str>,
    next_id: Arc<AtomicUsize>,
    sender: mpsc::Sender<Command<P>>,
}

impl<P: Payload> Clone for NodeTx<P> {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            next_id: self.next_id.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<P: Payload> NodeTx<P> {
    pub(crate) fn new(node_id: &str) -> (Self, mpsc::Receiver<Command<P>>) {
        let (sender, receiver) = mpsc::channel(1024);
        (
            Self {
                node_id: node_id.into(),
                next_id: Arc::new(AtomicUsize::new(1)),
                sender,
            },
            receiver,
        )
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    async fn enqueue(&self, command: Command<P>) -> Result<()> {
        self.sender
            .send(command)
            .await
            .map_err(|_| anyhow!("출력 채널이 닫혔습니다"))
    }

    fn request_message(&self, dest: &str, payload: P::Request) -> (usize, Message<P>) {
        let msg_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        (
            msg_id,
            Message {
                src: self.node_id.to_string(),
                dest: dest.to_owned(),
                body: Body::Request { msg_id, payload },
            },
        )
    }

    /// 요청을 한 번 보내도록 큐에 넣고, 응답을 식별할 요청 ID를 반환한다.
    pub async fn request(&self, dest: &str, payload: P::Request) -> Result<usize> {
        let (id, message) = self.request_message(dest, payload);
        self.enqueue(Command::SendOnce(message)).await?;
        Ok(id)
    }

    /// 요청을 큐에 넣고, 상대 노드가 응답할 때까지 같은 ID로 재전송하도록 한다.
    pub async fn request_with_retry(&self, dest: &str, payload: P::Request) -> Result<usize> {
        let (id, message) = self.request_message(dest, payload);
        self.enqueue(Command::SendUntilAck { message, id }).await?;
        Ok(id)
    }

    pub async fn reply(
        &self,
        peer: &str,
        in_reply_to: usize,
        make_payload: impl FnOnce(usize) -> P::Response,
    ) -> Result<()> {
        let msg_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.enqueue(Command::SendOnce(Message {
            src: self.node_id.to_string(),
            dest: peer.to_owned(),
            body: Body::Response {
                msg_id: Some(msg_id),
                in_reply_to,
                payload: make_payload(msg_id),
            },
        }))
        .await
    }

    pub(crate) async fn acknowledge(&self, peer: &str, in_reply_to: usize) -> Result<()> {
        self.enqueue(Command::AckReceived {
            peer: peer.to_owned(),
            in_reply_to,
        })
        .await
    }
}

pub(crate) fn encode<P: Payload>(message: &Message<P>) -> Result<Vec<u8>> {
    let mut frame = serde_json::to_vec(message)?;
    frame.push(b'\n');
    Ok(frame)
}

pub(crate) async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    frame: &[u8],
) -> Result<()> {
    writer.write_all(frame).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn write_messages<P: Payload>(
    mut commands: mpsc::Receiver<Command<P>>,
    mut writer: impl AsyncWrite + Unpin,
) -> Result<()> {
    let mut pending = HashMap::<(String, usize), Vec<u8>>::new();
    // 재전송 대기 시간이 모두 같으므로 새 재전송 시각은 항상 큐의 맨 뒤에 넣는다.
    let mut retries = VecDeque::new();
    loop {
        let next_retry = async {
            let (deadline, _) = retries.front()?;
            sleep_until(*deadline).await;
            retries.pop_front().map(|(_, key)| key)
        };
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::SendOnce(message)) => write_frame(&mut writer, &encode(&message)?).await?,
                Some(Command::SendUntilAck { message, id }) => {
                    let frame = encode(&message)?;
                    write_frame(&mut writer, &frame).await?;
                    let key = (message.dest, id);
                    pending.insert(key.clone(), frame);
                    retries.push_back((Instant::now() + RETRY_DELAY, key));
                }
                Some(Command::AckReceived { peer, in_reply_to }) => {
                    pending.remove(&(peer, in_reply_to));
                }
                None => return Ok(()),
            },
            Some(key) = next_retry => {
                if let Some(frame) = pending.get(&key) {
                    write_frame(&mut writer, frame).await?;
                    retries.push_back((Instant::now() + RETRY_DELAY, key));
                }
            }
        }
    }
}
