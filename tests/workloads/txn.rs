use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::oneshot,
    time::{Duration, timeout},
};

use super::{run, send, start_node, wire};

fn txn(node: &str, id: usize, ops: Value) -> Value {
    wire("c1", node, json!({"type":"txn", "msg_id":id, "txn":ops}))
}

#[tokio::test]
async fn txn_broadcasts_only_local_writes_and_advances_the_clock_on_receive() {
    let replicate = |id, counter, writes| {
        wire(
            "n1",
            "n0",
            json!({
                "type":"replicate", "msg_id":id, "version":{"counter":counter, "node_id":"n1"}, "writes":writes
            }),
        )
    };
    let input = [
        replicate(10, 50, json!([[1, 50]])),
        replicate(11, 4, json!([[1, 4], [2, 40]])),
        txn("n0", 12, json!([["r", 1, null], ["r", 2, null]])),
        txn("n0", 13, json!([["w", 3, 1], ["r", 3, null], ["w", 3, 2]])),
    ];
    let output = run(env!("CARGO_BIN_EXE_txn"), &input).await;
    assert_eq!(output.len(), 6);
    for (frame, id) in output[..2].iter().zip([10, 11]) {
        assert_eq!(frame["dest"], "n1");
        assert_eq!(frame["body"]["type"], "replicate_ok");
        assert_eq!(frame["body"]["in_reply_to"], id);
    }
    assert_eq!(
        output[2]["body"]["txn"],
        json!([["r", 1, 50], ["r", 2, 40]])
    );
    assert_eq!(output[3]["body"]["type"], "txn_ok");
    assert_eq!(output[3]["body"]["txn"], input[3]["body"]["txn"]);

    let mut peers: Vec<_> = output[4..]
        .iter()
        .map(|r| r["dest"].as_str().unwrap())
        .collect();
    peers.sort_unstable();
    assert_eq!(peers, ["n1", "n2"]);
    for frame in &output[4..] {
        assert_eq!(frame["body"]["type"], "replicate");
        assert_eq!(
            frame["body"]["version"],
            json!({"counter":53, "node_id":"n0"})
        );
        assert_eq!(frame["body"]["writes"], json!([[3, 2]]));
    }
}

async fn partition_side(
    node: &str,
    ops: Value,
    outgoing: oneshot::Sender<Value>,
    incoming: oneshot::Receiver<Value>,
) -> Value {
    let (mut child, mut stdin) = start_node(env!("CARGO_BIN_EXE_txn"), node, &["n0", "n1"]).await;
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut next = async || -> Value {
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
    };
    assert_eq!(next().await["body"]["type"], "init_ok");
    send(&mut stdin, &txn(node, 10, ops.clone())).await;
    let reply = next().await;
    assert_eq!(reply["body"]["type"], "txn_ok");
    assert_eq!(reply["body"]["txn"], ops);
    let local = next().await;
    assert_eq!(local["body"]["type"], "replicate");
    outgoing.send(local.clone()).unwrap();

    // 양쪽이 독립적으로 쓰기를 완료한 뒤 상대의 복제 메시지를 두 번 전달한다.
    let remote = incoming.await.unwrap();
    assert_eq!(remote["dest"], node);
    for _ in 0..2 {
        send(&mut stdin, &remote).await;
    }
    let reads = json!([
        ["r", 1, null],
        ["r", 2, null],
        ["r", 3, null],
        ["r", 4, null]
    ]);
    send(&mut stdin, &txn(node, 11, reads)).await;
    drop(stdin);

    let mut acks = 0;
    let result = loop {
        let frame = next().await;
        if frame == local {
            continue;
        }
        if frame["body"]["type"] == "txn_ok" {
            assert_eq!(frame["body"]["in_reply_to"], 11);
            break frame["body"]["txn"].clone();
        }
        assert_eq!(frame["body"]["type"], "replicate_ok");
        assert_eq!(frame["dest"], remote["src"]);
        assert_eq!(frame["body"]["in_reply_to"], remote["body"]["msg_id"]);
        acks += 1;
    };
    assert_eq!(acks, 2);
    assert!(child.wait().await.unwrap().success());
    result
}

#[tokio::test]
async fn txn_converges_after_partition_with_overlapping_and_disjoint_keys() {
    let (to_n0, from_n1) = oneshot::channel();
    let (to_n1, from_n0) = oneshot::channel();
    let (n0, n1) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            partition_side(
                "n0",
                json!([["w", 1, 10], ["w", 2, 20], ["w", 4, 40]]),
                to_n1,
                from_n1
            ),
            partition_side(
                "n1",
                json!([["w", 2, 21], ["w", 3, 30], ["w", 4, 41]]),
                to_n0,
                from_n0
            ),
        )
    })
    .await
    .unwrap();
    assert_eq!(
        n0,
        json!([["r", 1, 10], ["r", 2, 21], ["r", 3, 30], ["r", 4, 41]])
    );
    assert_eq!(n1, n0);
}
