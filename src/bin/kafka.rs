#[path = "kafka/kv.rs"]
mod kv;

use std::{collections::HashMap, fmt::Display, future::Future};

use kv::{KvClient, KvResult, LIN_KV, SEQ_KV};

use anyhow::Result;
use maelstrom_rust::{Body, Init, Message, Node, NodeTx, Payload, main_loop};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::task::JoinSet;

type Offsets = HashMap<String, usize>;
const POLL_READ_BATCH_SIZE: usize = 8;

struct KafkaPayload;

impl Payload for KafkaPayload {
    type Request = KafkaRequest;
    type Response = KafkaResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum KafkaRequest {
    Incoming(IncomingRequest),
    Outgoing(OutgoingRequest),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum IncomingRequest {
    Send { key: String, msg: usize },
    Poll { offsets: Offsets },
    CommitOffsets { offsets: Offsets },
    ListCommittedOffsets { keys: Vec<String> },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OutgoingRequest {
    Read {
        key: Value,
    },
    Write {
        key: Value,
        value: Value,
    },
    Cas {
        key: Value,
        from: Value,
        to: Value,
        #[serde(default)]
        create_if_not_exists: bool,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum KafkaResponse {
    Incoming(IncomingResponse),
    Outgoing(OutgoingResponse),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum IncomingResponse {
    ReadOk { value: Value },
    WriteOk,
    CasOk,
    Error(RpcError),
}

#[derive(Serialize, Deserialize)]
struct RpcError {
    code: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

impl RpcError {
    fn internal(error: impl Display) -> Self {
        Self {
            code: 13,
            text: Some(error.to_string()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OutgoingResponse {
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
    Error(RpcError),
}

struct KafkaNode {
    kv: KvClient,
    tx: NodeTx<KafkaPayload>,
    requests: JoinSet<Result<()>>,
}

impl Node for KafkaNode {
    type Payload = KafkaPayload;

    fn init(_: Init, tx: NodeTx<KafkaPayload>) -> Result<Self> {
        Ok(Self {
            kv: KvClient::new(tx.clone()),
            tx,
            requests: JoinSet::new(),
        })
    }

    async fn recv(&mut self, message: Message<KafkaPayload>) -> Result<()> {
        while let Some(finished) = self.requests.try_join_next() {
            finished??;
        }
        match message.body {
            Body::Request {
                msg_id,
                payload: KafkaRequest::Incoming(request),
            } => {
                let kv = self.kv.clone();
                let tx = self.tx.clone();
                // 클라이언트 요청이 KV 응답을 기다리는 동안에도 recv는 다음 입력을 처리한다.
                self.requests.spawn(async move {
                    let response = handle_request(kv, request)
                        .await
                        .unwrap_or_else(OutgoingResponse::Error);
                    tx.reply(&message.src, msg_id, |_| KafkaResponse::Outgoing(response))
                        .await
                });
            }
            Body::Response {
                in_reply_to,
                payload: KafkaResponse::Incoming(response),
                ..
            } => {
                self.kv.receive(message.src, in_reply_to, response).await;
            }
            _ => {}
        }
        Ok(())
    }
}

async fn handle_request(kv: KvClient, request: IncomingRequest) -> KvResult<OutgoingResponse> {
    match request {
        IncomingRequest::Send { key, msg } => {
            let counter = json!(["next_offset", key]);
            loop {
                let current = kv.read(LIN_KV, counter.clone()).await?;
                let offset = current.unwrap_or(0);
                let next = offset
                    .checked_add(1)
                    .ok_or_else(|| RpcError::internal("오프셋 범위를 초과했습니다"))?;
                if kv.cas(LIN_KV, counter.clone(), current, next).await? {
                    kv.write(SEQ_KV, json!([key, offset]), msg).await?;
                    return Ok(OutgoingResponse::SendOk { offset });
                }
            }
        }
        IncomingRequest::Poll { offsets } => {
            let logs = for_keys(kv, offsets, |kv, (key, start)| async move {
                let entries = poll_log(&kv, &key, start).await?;
                Ok((key, entries))
            })
            .await?;
            let msgs = logs
                .into_iter()
                .filter(|(_, entries)| !entries.is_empty())
                .collect();
            Ok(OutgoingResponse::PollOk { msgs })
        }
        IncomingRequest::CommitOffsets { offsets } => {
            for_keys(kv, offsets, |kv, (key, target)| async move {
                let key = json!(["committed_offset", key]);
                loop {
                    let current = kv.read(LIN_KV, key.clone()).await?;
                    if current.is_some_and(|offset| offset >= target)
                        || kv.cas(LIN_KV, key.clone(), current, target).await?
                    {
                        return Ok(());
                    }
                }
            })
            .await?;
            Ok(OutgoingResponse::CommitOffsetsOk)
        }
        IncomingRequest::ListCommittedOffsets { keys } => {
            let entries = for_keys(kv, keys, |kv, key| async move {
                let offset = kv.read(LIN_KV, json!(["committed_offset", key])).await?;
                Ok(offset.map(|offset| (key, offset)))
            })
            .await?;
            Ok(OutgoingResponse::ListCommittedOffsetsOk {
                offsets: entries.into_iter().flatten().collect(),
            })
        }
    }
}

async fn poll_log(kv: &KvClient, key: &str, start: usize) -> KvResult<Vec<(usize, usize)>> {
    let end = kv
        .read(LIN_KV, json!(["next_offset", key]))
        .await?
        .unwrap_or(0);
    let mut entries = Vec::new();
    let mut reads = JoinSet::new();
    let mut batch = Vec::new();
    for first in (start..end).step_by(POLL_READ_BATCH_SIZE) {
        let batch_end = end.min(first.saturating_add(POLL_READ_BATCH_SIZE));
        for offset in first..batch_end {
            let kv = kv.clone();
            let log_key = json!([key, offset]);
            reads.spawn(async move { (offset, kv.read(SEQ_KV, log_key).await) });
        }
        while let Some(result) = reads.join_next().await {
            batch.push(result.map_err(RpcError::internal)?);
        }
        // 응답 도착 순서와 관계없이 첫 빈 오프셋 앞의 값과 오류만 반영한다.
        batch.sort_unstable_by_key(|(offset, _)| *offset);
        for (offset, result) in batch.drain(..) {
            let Some(msg) = result? else {
                return Ok(entries);
            };
            entries.push((offset, msg));
        }
    }
    Ok(entries)
}

// 키별로 독립적인 RPC를 진행하고 각 작업이 끝난 뒤 클라이언트에 한 번만 응답한다.
async fn for_keys<I, F, T>(
    kv: KvClient,
    items: impl IntoIterator<Item = I>,
    work: impl Fn(KvClient, I) -> F,
) -> KvResult<Vec<T>>
where
    F: Future<Output = KvResult<T>> + Send + 'static,
    T: Send + 'static,
{
    let mut tasks = JoinSet::new();
    for item in items {
        tasks.spawn(work(kv.clone(), item));
    }
    let mut results = Vec::with_capacity(tasks.len());
    while let Some(result) = tasks.join_next().await {
        results.push(result.unwrap_or_else(|error| Err(RpcError::internal(error))));
    }
    results.into_iter().collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    main_loop::<KafkaNode>().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn outgoing_kv_requests_keep_their_wire_format() {
        for body in [
            json!({"type": "read", "msg_id": 1, "key": "counter"}),
            json!({"type": "write", "msg_id": 2, "key": "counter", "value": 5}),
            json!({"type": "cas", "msg_id": 3, "key": "counter", "from": 5, "to": 8, "create_if_not_exists": true}),
        ] {
            let frame = json!({"src": "n0", "dest": "lin-kv", "body": body});
            let request: Message<KafkaPayload> = serde_json::from_value(frame.clone()).unwrap();
            assert!(matches!(
                request.body,
                Body::Request {
                    payload: KafkaRequest::Outgoing(_),
                    ..
                }
            ));
            assert_eq!(serde_json::to_value(request).unwrap(), frame);
        }
    }

    #[test]
    fn incoming_kv_responses_keep_their_wire_format() {
        for body in [
            json!({"type": "read_ok", "in_reply_to": 1, "value": 5}),
            json!({"type": "write_ok", "in_reply_to": 2}),
            json!({"type": "cas_ok", "in_reply_to": 3}),
            json!({"type": "error", "in_reply_to": 4, "code": 20}),
            json!({"type": "error", "in_reply_to": 5, "code": 22, "text": "precondition failed"}),
        ] {
            let frame = json!({"src": "lin-kv", "dest": "n0", "body": body});
            let response: Message<KafkaPayload> = serde_json::from_value(frame.clone()).unwrap();
            assert!(matches!(
                response.body,
                Body::Response {
                    payload: KafkaResponse::Incoming(_),
                    ..
                }
            ));
            assert_eq!(serde_json::to_value(response).unwrap(), frame);
        }
    }
}
