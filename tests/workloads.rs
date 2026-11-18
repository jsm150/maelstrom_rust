use std::{collections::HashSet, process::Stdio, time::Duration};

use serde_json::{Value, json};
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};

fn wire(src: &str, dest: &str, body: Value) -> Value {
    json!({"src": src, "dest": dest, "body": body})
}

async fn run(binary: &str, input: &[Value]) -> Vec<Value> {
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
        "n0",
        json!({"type": "init", "msg_id": 70, "node_id": "n0", "node_ids": ["n0", "n1", "n2"]}),
    );
    for message in std::iter::once(&init).chain(input) {
        stdin
            .write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
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
            "n0",
            "c0",
            json!({"type": "init_ok", "msg_id": 0, "in_reply_to": 70})
        ))
    );
    messages.collect()
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
