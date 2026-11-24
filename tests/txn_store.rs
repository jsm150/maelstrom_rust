#[path = "../src/bin/txn/store.rs"]
mod store;

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use serde_json::{Value, json};
use store::{Replication, TxnOp, TxnStore};
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};

async fn within<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(5), future)
        .await
        .expect("작업이 완료되지 않았습니다")
}

async fn execute(store: &TxnStore, operations: Value) -> Value {
    let mut txn: Vec<TxnOp> = serde_json::from_value(operations).unwrap();
    store.execute(&mut txn).await;
    serde_json::to_value(txn).unwrap()
}

fn replica(counter: u64, writes: Value) -> Replication {
    serde_json::from_value(json!({"version":{"counter":counter,"node_id":"n1"},"writes":writes}))
        .unwrap()
}

// 지정한 연산이 끝나고 레코드 맵의 잠금이 풀린 시점에 트랜잭션을 멈춘다.
async fn pause_after(
    store: Arc<TxnStore>,
    operations: Value,
    index: usize,
) -> (oneshot::Sender<()>, JoinHandle<Value>) {
    let (reached, paused) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut gate = Some((reached, resumed));
        let mut txn: Vec<TxnOp> = serde_json::from_value(operations).unwrap();
        store
            .execute_with(&mut txn, |i| {
                let pause = if i == index { gate.take() } else { None };
                async move {
                    if let Some((reached, resumed)) = pause {
                        reached.send(()).unwrap();
                        resumed.await.unwrap();
                    }
                }
            })
            .await;
        serde_json::to_value(txn).unwrap()
    });
    within(paused).await.unwrap();
    (resume, task)
}

// 먼저 한 번 폴링해 실제로 잠금에서 대기하는지 확인하고 같은 future를 나중에 재개한다.
async fn assert_waiting(future: Pin<&mut impl Future>) {
    tokio::select! {
        biased;
        _ = future => panic!("쓰기 잠금이 해제되기 전에 완료되었습니다"),
        _ = std::future::ready(()) => {}
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readers_see_undo_and_only_conflicting_writers_wait() {
    let store = Arc::new(TxnStore::new("n0".into()));
    execute(&store, json!([["w", 1, 3]])).await;
    let (resume, writer) = pause_after(
        store.clone(),
        json!([["w", 1, 6], ["w", 1, 7], ["r", 1, null], ["r", 2, null]]),
        2,
    )
    .await;

    let mut conflicting = std::pin::pin!(execute(&store, json!([["w", 1, 8]])));
    assert_waiting(conflicting.as_mut()).await;
    // 같은 키를 읽고 다른 키를 쓰는 트랜잭션은 즉시 커밋할 수 있다.
    assert_eq!(
        within(execute(&store, json!([["w", 2, 9], ["r", 1, null]]))).await,
        json!([["w", 2, 9], ["r", 1, 3]])
    );
    resume.send(()).unwrap();
    assert_eq!(
        within(writer).await.unwrap(),
        json!([["w", 1, 6], ["w", 1, 7], ["r", 1, 3], ["r", 2, 9]])
    );
    assert_eq!(
        within(execute(&store, json!([["r", 1, null]]))).await,
        json!([["r", 1, 7]])
    );
    within(conflicting).await;
    assert_eq!(
        within(execute(&store, json!([["r", 1, null]]))).await,
        json!([["r", 1, 8]])
    );
}

#[tokio::test]
async fn reads_observe_the_latest_commit_at_each_operation() {
    let store = Arc::new(TxnStore::new("n0".into()));
    execute(&store, json!([["w", 1, 3]])).await;
    let (resume, reader) =
        pause_after(store.clone(), json!([["r", 1, null], ["r", 1, null]]), 0).await;
    within(execute(&store, json!([["w", 1, 4]]))).await;
    resume.send(()).unwrap();
    assert_eq!(
        within(reader).await.unwrap(),
        json!([["r", 1, 3], ["r", 1, 4]])
    );
}

#[tokio::test]
async fn cancellation_restores_values_and_versions_before_replication_can_merge() {
    let store = Arc::new(TxnStore::new("n0".into()));
    store.merge(replica(50, json!([[1, 3], [3, 5]]))).await;
    let (_resume, writer) = pause_after(
        store.clone(),
        json!([["w", 1, 6], ["w", 1, 7], ["w", 2, 8], ["w", 3, 9]]),
        2,
    )
    .await;
    let reads = json!([["r", 1, null], ["r", 2, null], ["r", 3, null]]);
    let original = json!([["r", 1, 3], ["r", 2, null], ["r", 3, 5]]);
    assert_eq!(within(execute(&store, reads.clone())).await, original);
    let mut merge = std::pin::pin!(store.merge(replica(49, json!([[1, 99], [4, 10]]))));
    assert_waiting(merge.as_mut()).await;

    writer.abort();
    assert!(within(writer).await.unwrap_err().is_cancelled());
    within(merge).await;
    assert_eq!(within(execute(&store, reads)).await, original);
    assert_eq!(
        within(execute(&store, json!([["r", 4, null]]))).await,
        json!([["r", 4, 10]])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_waiter_releases_only_its_acquired_locks() {
    let store = Arc::new(TxnStore::new("n0".into()));
    let (resume, owner) = pause_after(store.clone(), json!([["w", 2, 20]]), 0).await;
    // 역순으로 요청해도 1번 잠금을 먼저 얻고 2번에서 대기한다.
    let mut waiter = Box::pin(execute(&store, json!([["w", 2, 22], ["w", 1, 11]])));
    assert_waiting(waiter.as_mut()).await;
    let mut first_key = std::pin::pin!(execute(&store, json!([["w", 1, 10]])));
    assert_waiting(first_key.as_mut()).await;
    drop(waiter);
    within(first_key).await;
    resume.send(()).unwrap();
    within(owner).await.unwrap();
    assert_eq!(
        within(execute(&store, json!([["r", 1, null], ["r", 2, null]]))).await,
        json!([["r", 1, 10], ["r", 2, 20]])
    );
}
