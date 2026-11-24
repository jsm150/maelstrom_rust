use std::collections::{BTreeMap, HashMap};

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
    Replicate(Replication),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TxnResponse {
    TxnOk { txn: Vec<TxnOp> },
    ReplicateOk,
}

// 카운터가 같은 동시 쓰기는 발급 노드 ID의 사전순으로 비교한다.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Version {
    counter: u64,
    node_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Replication {
    version: Version,
    writes: Vec<(i64, i64)>,
}

struct Register {
    value: i64,
    version: Version,
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
    peers: Vec<String>,
    clock: u64,
    committed: HashMap<i64, Register>,
}

impl TxnNode {
    fn commit(&mut self, txn: &mut [TxnOp]) -> Option<Replication> {
        // 읽기는 기존 커밋 값에서 채우고, 쓰기는 키별 마지막 값만 모은다.
        let mut writes = BTreeMap::new();
        for op in txn {
            match op {
                TxnOp::Read { key, value } => {
                    *value = self.committed.get(key).map(|register| register.value);
                }
                TxnOp::Write { key, value } => {
                    writes.insert(*key, *value);
                }
            }
        }
        if writes.is_empty() {
            return None;
        }

        self.clock += 1;
        let replication = Replication {
            version: Version {
                counter: self.clock,
                node_id: self.tx.node_id().to_owned(),
            },
            writes: writes.into_iter().collect(),
        };
        self.merge(&replication);
        Some(replication)
    }

    fn merge(&mut self, replication: &Replication) {
        for &(key, value) in &replication.writes {
            if self
                .committed
                .get(&key)
                .is_none_or(|register| register.version < replication.version)
            {
                self.committed.insert(
                    key,
                    Register {
                        value,
                        version: replication.version.clone(),
                    },
                );
            }
        }
    }
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
            clock: 0,
            committed: HashMap::new(),
        })
    }

    async fn recv(&mut self, message: Message<TxnPayload>) -> Result<()> {
        let Body::Request { msg_id, payload } = message.body else {
            return Ok(());
        };

        match payload {
            TxnRequest::Txn { mut txn } => {
                let replication = self.commit(&mut txn);
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
                self.clock = self.clock.max(replication.version.counter) + 1;
                self.merge(&replication);
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
