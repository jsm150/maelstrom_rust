use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

// 카운터가 같은 동시 쓰기는 발급 노드 ID의 사전순으로 비교한다.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Version {
    counter: u64,
    node_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Replication {
    version: Version,
    writes: Vec<(i64, i64)>,
}

pub(super) enum TxnOp {
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

struct Register {
    value: i64,
    version: Version,
}

enum Entry {
    Committed(Register),
    Pending { value: i64, undo: Option<Register> },
}

impl Entry {
    fn committed(&self) -> Option<&Register> {
        match self {
            Self::Committed(register) => Some(register),
            Self::Pending { undo, .. } => undo.as_ref(),
        }
    }
}

#[derive(Default)]
struct State {
    clock: u64,
    entries: HashMap<i64, Entry>,
    writers: HashMap<i64, Arc<AsyncMutex<()>>>,
}

pub(super) struct TxnStore {
    node_id: String,
    state: Mutex<State>,
}

impl TxnStore {
    pub(super) fn new(node_id: String) -> Self {
        Self {
            node_id,
            state: Mutex::default(),
        }
    }

    pub(super) async fn execute(&self, txn: &mut [TxnOp]) -> Option<Replication> {
        self.execute_with(txn, |_| std::future::ready(())).await
    }

    pub(super) async fn execute_with<F: Future<Output = ()>>(
        &self,
        txn: &mut [TxnOp],
        mut after_op: impl FnMut(usize) -> F,
    ) -> Option<Replication> {
        let keys = txn
            .iter()
            .filter_map(|op| match op {
                TxnOp::Write { key, .. } => Some(*key),
                TxnOp::Read { .. } => None,
            })
            .collect();
        let mut writer = self.lock_writes(keys).await;
        for (index, op) in txn.iter_mut().enumerate() {
            match op {
                TxnOp::Read { key, value } => {
                    *value = self
                        .state
                        .lock()
                        .unwrap()
                        .entries
                        .get(key)
                        .and_then(Entry::committed)
                        .map(|register| register.value);
                }
                TxnOp::Write { key, value } => writer.write(*key, *value),
            }
            after_op(index).await;
        }
        writer.commit()
    }

    pub(super) async fn merge(&self, replication: Replication) {
        let keys = replication.writes.iter().map(|&(key, _)| key).collect();
        let _writer = self.lock_writes(keys).await;
        let mut state = self.state.lock().unwrap();
        state.clock = state.clock.max(replication.version.counter) + 1;
        for (key, value) in replication.writes {
            let current = state.entries.get(&key).and_then(Entry::committed);
            if current.is_none_or(|register| register.version < replication.version) {
                state.entries.insert(
                    key,
                    Entry::Committed(Register {
                        value,
                        version: replication.version.clone(),
                    }),
                );
            }
        }
    }

    async fn lock_writes(&self, keys: BTreeSet<i64>) -> WriteTxn<'_> {
        let locks = {
            let mut state = self.state.lock().unwrap();
            keys.into_iter()
                .map(|key| (key, state.writers.entry(key).or_default().clone()))
                .collect()
        };
        let mut writer = WriteTxn {
            store: self,
            locks,
            held: Vec::new(),
            pending: false,
        };
        // 키 순서대로 기다리는 동안 레코드 맵은 다른 트랜잭션도 사용할 수 있다.
        for (_, lock) in &writer.locks {
            writer.held.push(lock.clone().lock_owned().await);
        }
        writer
    }
}

struct WriteTxn<'a> {
    store: &'a TxnStore,
    locks: Vec<(i64, Arc<AsyncMutex<()>>)>,
    held: Vec<OwnedMutexGuard<()>>,
    pending: bool,
}

impl WriteTxn<'_> {
    fn write(&mut self, key: i64, value: i64) {
        let mut state = self.store.state.lock().unwrap();
        let undo = match state.entries.remove(&key) {
            Some(Entry::Committed(register)) => Some(register),
            Some(Entry::Pending { undo, .. }) => undo,
            None => None,
        };
        state.entries.insert(key, Entry::Pending { value, undo });
        self.pending = true;
    }

    fn commit(mut self) -> Option<Replication> {
        if !self.pending {
            return None;
        }
        let mut state = self.store.state.lock().unwrap();
        state.clock += 1;
        let version = Version {
            counter: state.clock,
            node_id: self.store.node_id.clone(),
        };
        let mut writes = Vec::with_capacity(self.locks.len());
        for &(key, _) in &self.locks {
            let entry = state.entries.get_mut(&key).unwrap();
            let Entry::Pending { value, .. } = entry else {
                unreachable!("커밋할 쓰기가 없습니다")
            };
            let value = *value;
            *entry = Entry::Committed(Register {
                value,
                version: version.clone(),
            });
            writes.push((key, value));
        }
        self.pending = false;
        Some(Replication { version, writes })
    }
}

impl Drop for WriteTxn<'_> {
    fn drop(&mut self) {
        if self.locks.is_empty() {
            return;
        }
        let mut state = self.store.state.lock().unwrap();
        // 취소 시 키 잠금을 해제하기 전에 값과 버전을 함께 복원한다.
        if self.pending {
            for (key, _) in &self.locks {
                let Some(Entry::Pending { undo, .. }) = state.entries.get_mut(key) else {
                    continue;
                };
                if let Some(register) = undo.take() {
                    state.entries.insert(*key, Entry::Committed(register));
                } else {
                    state.entries.remove(key);
                }
            }
        }
        self.held.clear();
        // 잠금 맵과 현재 작업만 참조하면 이 키에 남은 대기자가 없다.
        for (key, lock) in self.locks.drain(..) {
            if Arc::strong_count(&lock) == 2 {
                state.writers.remove(&key);
            }
        }
    }
}
