use std::{collections::HashSet, process::Stdio, time::Duration};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    time::timeout,
};

fn wire(src: &str, dest: &str, body: Value) -> Value {
    json!({"src": src, "dest": dest, "body": body})
}

async fn run(binary: &str, input: &[Value]) -> Vec<Value> {
    run_as(binary, "n0", &["n0", "n1", "n2"], input).await
}

async fn send(stdin: &mut ChildStdin, message: &Value) {
    stdin
        .write_all(format!("{message}\n").as_bytes())
        .await
        .unwrap();
}

async fn start_node(binary: &str, node_id: &str, node_ids: &[&str]) -> (Child, ChildStdin) {
    let mut process = Command::new(binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = process.stdin.take().unwrap();
    let init = wire(
        "c0",
        node_id,
        json!({"type": "init", "msg_id": 70, "node_id": node_id, "node_ids": node_ids}),
    );
    send(&mut stdin, &init).await;
    (process, stdin)
}

async fn run_as(binary: &str, node_id: &str, node_ids: &[&str], input: &[Value]) -> Vec<Value> {
    let (process, mut stdin) = start_node(binary, node_id, node_ids).await;
    for message in input {
        send(&mut stdin, message).await;
    }
    drop(stdin);
    let output = timeout(Duration::from_secs(5), process.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut messages = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap());
    assert_eq!(
        messages.next(),
        Some(wire(
            node_id,
            "c0",
            json!({"type": "init_ok", "msg_id": 0, "in_reply_to": 70})
        ))
    );
    messages.collect()
}

#[tokio::test]
async fn counter_master_replicates_each_distinct_add_once_per_slave() {
    let direct = wire("c1", "n1", json!({"type": "add", "msg_id": 11, "delta": 5}));
    let forwarded = wire("n2", "n1", json!({"type": "add", "msg_id": 11, "delta": 3}));
    let read = wire("c1", "n1", json!({"type": "read", "msg_id": 12}));
    let input = [
        direct.clone(),
        read.clone(),
        forwarded.clone(),
        direct,
        forwarded,
        read,
    ];
    let output = run_as(
        env!("CARGO_BIN_EXE_g-counter"),
        "n1",
        &["n3", "n1", "n2"],
        &input,
    )
    .await;

    for peer in ["n2", "n3"] {
        let deltas: Vec<_> = output
            .iter()
            .filter(|m| m["dest"] == peer && m["body"]["type"] == "replicate")
            .map(|m| m["body"]["delta"].as_u64().unwrap())
            .collect();
        assert_eq!(deltas, [5, 3]);
    }
    let replies: Vec<_> = output
        .iter()
        .filter(|m| m["body"].get("in_reply_to").is_some())
        .collect();
    assert_eq!(replies.len(), input.len());
    for ((reply, request), kind) in replies
        .iter()
        .zip(&input)
        .zip(["add_ok", "read_ok", "add_ok", "add_ok", "add_ok", "read_ok"])
    {
        assert_eq!(reply["dest"], request["src"]);
        assert_eq!(reply["body"]["in_reply_to"], request["body"]["msg_id"]);
        assert_eq!(reply["body"]["type"], kind);
    }
    assert_eq!(replies[1]["body"]["value"], 5);
    assert_eq!(replies[5]["body"]["value"], 8);
}

#[tokio::test]
async fn counter_slave_forwards_add_once_and_applies_reordered_deltas_once() {
    let add = wire("c1", "n2", json!({"type": "add", "msg_id": 11, "delta": 3}));
    let read = wire("c1", "n2", json!({"type": "read", "msg_id": 12}));
    let replicate = |id, delta| {
        wire(
            "n10",
            "n2",
            json!({"type": "replicate", "msg_id": id, "delta": delta}),
        )
    };
    let input = [
        add.clone(),
        add,
        read.clone(),
        replicate(42, 3),
        replicate(41, 5),
        replicate(42, 3),
        read,
    ];
    let output = run_as(
        env!("CARGO_BIN_EXE_g-counter"),
        "n2",
        &["n2", "n10"],
        &input,
    )
    .await;
    assert_eq!(output.len(), 1 + input.len());
    assert_eq!(output[0]["dest"], "n10");
    assert_eq!(output[0]["body"]["type"], "add");
    assert_eq!(output[0]["body"]["delta"], 3);
    for (reply, request) in output[1..].iter().zip(&input) {
        let kind = request["body"]["type"].as_str().unwrap();
        assert_eq!(reply["body"]["type"], format!("{kind}_ok"));
        assert_eq!(reply["dest"], request["src"]);
        assert_eq!(reply["body"]["in_reply_to"], request["body"]["msg_id"]);
    }
    assert_eq!(output[3]["body"]["value"], 0);
    assert_eq!(output.last().unwrap()["body"]["value"], 8);
}

async fn check_counter_retry(node: &str, peer: &str, kind: &str) {
    let (mut child, mut stdin) =
        start_node(env!("CARGO_BIN_EXE_g-counter"), node, &["n1", "n2"]).await;
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let add = wire("c1", node, json!({"type": "add", "msg_id": 11, "delta": 5}));
    send(&mut stdin, &add).await;
    let mut receive = async || -> Value {
        let line = lines.next_line().await.unwrap().unwrap();
        serde_json::from_str(&line).unwrap()
    };
    assert_eq!(receive().await["body"]["type"], "init_ok");
    let request = receive().await;
    assert_eq!(request["dest"], peer);
    assert_eq!(request["body"]["type"], kind);
    assert_eq!(receive().await["body"]["type"], "add_ok");
    assert_eq!(receive().await, request);

    let ack = wire(
        peer,
        node,
        json!({
            "type": format!("{kind}_ok"), "msg_id": 20, "in_reply_to": request["body"]["msg_id"]
        }),
    );
    send(&mut stdin, &ack).await;
    let read = wire("c1", node, json!({"type": "read", "msg_id": 12}));
    send(&mut stdin, &read).await;
    // ACK를 처리하기 전에 이미 출력된 재전송은 건너뛴다.
    loop {
        let next = receive().await;
        if next != request {
            assert_eq!(next["body"]["type"], "read_ok");
            break;
        }
    }
    assert!(
        timeout(Duration::from_secs(1), lines.next_line())
            .await
            .is_err()
    );
    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn counter_retries_forwarding_and_replication_until_acknowledged() {
    for (node, peer, kind) in [("n2", "n1", "add"), ("n1", "n2", "replicate")] {
        timeout(
            Duration::from_secs(5),
            check_counter_retry(node, peer, kind),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn echo_replies_to_the_requester() {
    let request = wire(
        "c5",
        "n0",
        json!({"type": "echo", "msg_id": 71, "echo": "hello"}),
    );
    let output = run(env!("CARGO_BIN_EXE_echo"), &[request]).await;
    assert_eq!(
        output,
        [wire(
            "n0",
            "c5",
            json!({"type": "echo_ok", "msg_id": 1, "in_reply_to": 71, "echo": "hello"})
        )]
    );
}

#[tokio::test]
async fn unique_ids_use_the_node_and_response_id() {
    let requests = [71, 72].map(|id| wire("c3", "n0", json!({"type": "generate", "msg_id": id})));
    let output = run(env!("CARGO_BIN_EXE_unique-ids"), &requests).await;
    assert_eq!(
        output,
        [1, 2].map(|id| wire(
            "n0",
            "c3",
            json!({"type": "generate_ok", "msg_id": id, "in_reply_to": 70 + id, "id": format!("n0-{id}")})
        ))
    );
}

#[tokio::test]
async fn broadcast_deduplicates_and_flushes_its_final_batch_to_each_peer_on_eof() {
    let requests = [
        wire(
            "c0",
            "n0",
            json!({"type": "topology", "msg_id": 1, "topology": {"n0": []}}),
        ),
        wire(
            "n1",
            "n0",
            json!({"type": "unicast", "msg_id": 2, "messages": [10, 20, 10]}),
        ),
        wire(
            "c0",
            "n0",
            json!({"type": "broadcast", "msg_id": 3, "message": 30}),
        ),
        wire(
            "c0",
            "n0",
            json!({"type": "broadcast", "msg_id": 4, "message": 30}),
        ),
        wire(
            "c0",
            "n0",
            json!({"type": "broadcast", "msg_id": 5, "message": 40}),
        ),
        wire("c0", "n0", json!({"type": "read", "msg_id": 6})),
    ];
    let output = run(env!("CARGO_BIN_EXE_broadcast"), &requests).await;
    assert_eq!(output.len(), 8);
    let ids: HashSet<_> = output
        .iter()
        .map(|message| message["body"]["msg_id"].as_u64().unwrap())
        .collect();
    assert_eq!(ids.len(), 8);
    let replies: Vec<_> = output
        .iter()
        .filter(|m| m["body"].get("in_reply_to").is_some())
        .collect();
    let types = [
        "topology_ok",
        "unicast_ok",
        "broadcast_ok",
        "broadcast_ok",
        "broadcast_ok",
        "read_ok",
    ];
    assert_eq!(replies.len(), types.len());
    for ((reply, request), kind) in replies.iter().zip(&requests).zip(types) {
        assert_eq!(reply["src"], "n0");
        assert_eq!(reply["dest"], request["src"]);
        assert_eq!(reply["body"]["type"], kind);
        assert_eq!(reply["body"]["in_reply_to"], request["body"]["msg_id"]);
    }
    let mut values: Vec<usize> =
        serde_json::from_value(replies[5]["body"]["messages"].clone()).unwrap();
    values.sort_unstable();
    assert_eq!(values, [10, 20, 30, 40]);

    let batches: Vec<_> = output
        .iter()
        .filter(|m| m["body"]["type"] == "unicast")
        .collect();
    assert_eq!(batches.len(), 2);
    for (batch, peer) in batches.iter().zip(["n1", "n2"]) {
        assert_eq!(batch["src"], "n0");
        assert_eq!(batch["dest"], peer);
        assert_eq!(batch["body"]["messages"], json!([30, 40]));
    }
}
