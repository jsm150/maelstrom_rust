use std::{collections::HashMap, sync::Arc, time::Duration};

use maelstrom_rust::NodeTx;
use serde_json::{Value, json};
use tokio::sync::{Mutex, oneshot};

use super::{IncomingResponse, KafkaPayload, KafkaRequest, OutgoingRequest, RpcError};

pub(super) const LIN_KV: &str = "lin-kv";
pub(super) const SEQ_KV: &str = "seq-kv";

type Pending = HashMap<(String, usize), oneshot::Sender<IncomingResponse>>;
pub(super) type KvResult<T> = Result<T, RpcError>;

#[derive(Clone)]
pub(super) struct KvClient {
    tx: NodeTx<KafkaPayload>,
    pending: Arc<Mutex<Pending>>,
}

impl KvClient {
    pub(super) fn new(tx: NodeTx<KafkaPayload>) -> Self {
        Self {
            tx,
            pending: Arc::default(),
        }
    }

    pub(super) async fn receive(
        &self,
        src: String,
        in_reply_to: usize,
        response: IncomingResponse,
    ) {
        if let Some(waiter) = self.pending.lock().await.remove(&(src, in_reply_to)) {
            let _ = waiter.send(response);
        }
    }

    async fn request(&self, service: &str, request: OutgoingRequest) -> KvResult<IncomingResponse> {
        let (sender, receiver) = oneshot::channel();
        // 빠른 응답도 대기자를 찾도록 요청 ID 발급과 등록 사이에 응답 처리가 끼어들지 않게 한다.
        let id = {
            let mut pending = self.pending.lock().await;
            let id = self
                .tx
                .request(service, KafkaRequest::Outgoing(request))
                .await
                .map_err(RpcError::internal)?;
            pending.insert((service.to_owned(), id), sender);
            id
        };
        let response = match tokio::time::timeout(Duration::from_secs(5), receiver).await {
            Ok(result) => result.map_err(RpcError::internal)?,
            Err(_) => {
                self.pending.lock().await.remove(&(service.to_owned(), id));
                return Err(RpcError {
                    code: 0,
                    text: Some(format!("{service} 요청 {id} 응답 시간 초과")),
                });
            }
        };
        match response {
            IncomingResponse::Error(error) => Err(error),
            response => Ok(response),
        }
    }

    pub(super) async fn read(&self, service: &str, key: Value) -> KvResult<Option<usize>> {
        match self.request(service, OutgoingRequest::Read { key }).await {
            Ok(IncomingResponse::ReadOk { value }) => serde_json::from_value(value)
                .map(Some)
                .map_err(RpcError::internal),
            Err(RpcError { code: 20, .. }) => Ok(None),
            Err(error) => Err(error),
            Ok(_) => Err(RpcError::internal("read 응답 형식이 올바르지 않습니다")),
        }
    }

    pub(super) async fn write(&self, service: &str, key: Value, value: usize) -> KvResult<()> {
        match self
            .request(
                service,
                OutgoingRequest::Write {
                    key,
                    value: json!(value),
                },
            )
            .await?
        {
            IncomingResponse::WriteOk => Ok(()),
            _ => Err(RpcError::internal("write 응답 형식이 올바르지 않습니다")),
        }
    }

    // 조건 불일치만 false로 반환한다. 결과가 불확실한 오류는 호출자에게 전달한다.
    pub(super) async fn cas(
        &self,
        service: &str,
        key: Value,
        from: Option<usize>,
        to: usize,
    ) -> KvResult<bool> {
        let request = OutgoingRequest::Cas {
            key,
            from: json!(from),
            to: json!(to),
            create_if_not_exists: from.is_none(),
        };
        match self.request(service, request).await {
            Ok(IncomingResponse::CasOk) => Ok(true),
            Err(RpcError { code: 22, .. }) => Ok(false),
            Err(error) => Err(error),
            Ok(_) => Err(RpcError::internal("cas 응답 형식이 올바르지 않습니다")),
        }
    }
}
