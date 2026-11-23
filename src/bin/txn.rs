use std::collections::HashMap;

use anyhow::Result;
use maelstrom_rust::{Body, Init, Message, Node, NodeTx, Payload, main_loop};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

struct TxnPayload;

impl Payload for TxnPayload {
    type Request = TxnRequest;
    type Response = TxnResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TxnRequest {
    Txn { txn: Vec<TxnOp> },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TxnResponse {
    TxnOk { txn: Vec<TxnOp> },
}

enum TxnOp {
    Read { key: i64, value: Option<i64> },
    Write { key: i64, value: i64 },
}

impl<'de> Deserialize<'de> for TxnOp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (op, key, value) = <(char, i64, Option<i64>)>::deserialize(deserializer)?;
        match (op, value) {
            ('r', value) => Ok(Self::Read { key, value }),
            ('w', Some(value)) => Ok(Self::Write { key, value }),
            ('w', None) => Err(de::Error::custom("쓰기 연산에는 정숫값이 필요합니다")),
            _ => Err(de::Error::custom(format_args!("알 수 없는 연산: {op}"))),
        }
    }
}

impl Serialize for TxnOp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Read { key, value } => ('r', key, value).serialize(serializer),
            Self::Write { key, value } => ('w', key, value).serialize(serializer),
        }
    }
}

struct TxnNode {
    tx: NodeTx<TxnPayload>,
    committed: HashMap<i64, i64>,
}

impl Node for TxnNode {
    type Payload = TxnPayload;

    fn init(_: Init, tx: NodeTx<TxnPayload>) -> Result<Self> {
        Ok(Self {
            tx,
            committed: HashMap::new(),
        })
    }

    async fn recv(&mut self, message: Message<TxnPayload>) -> Result<()> {
        let Body::Request {
            msg_id,
            payload: TxnRequest::Txn { mut txn },
        } = message.body
        else {
            return Ok(());
        };

        // 모든 읽기가 트랜잭션 시작 시점의 값을 보도록 쓰기는 두 번째 순회에서 반영한다.
        for op in &mut txn {
            if let TxnOp::Read { key, value } = op {
                *value = self.committed.get(key).copied();
            }
        }
        for op in &txn {
            if let TxnOp::Write { key, value } = op {
                self.committed.insert(*key, *value);
            }
        }
        self.tx
            .reply(&message.src, msg_id, |_| TxnResponse::TxnOk { txn })
            .await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    main_loop::<TxnNode>().await
}
