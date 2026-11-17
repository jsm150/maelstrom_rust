use super::*;
use serde_json::{Value, json};

fn wire_messages() -> Vec<Value> {
    [
        json!({"type": "init", "msg_id": 7, "node_id": "n0", "node_ids": ["n0", "n1"]}),
        json!({"type": "echo", "msg_id": 7, "echo": "hello"}),
        json!({"type": "generate", "msg_id": 7}),
        json!({"type": "broadcast", "msg_id": 7, "message": 42}),
        json!({"type": "unicast", "msg_id": 7, "messages": [42, 43]}),
        json!({"type": "read", "msg_id": 7}),
        json!({"type": "topology", "msg_id": 7, "topology": {"n0": ["n1"], "n1": ["n0"]}}),
        json!({"type": "init_ok", "msg_id": 13, "in_reply_to": 7}),
        json!({"type": "echo_ok", "msg_id": 13, "in_reply_to": 7, "echo": "hello"}),
        json!({"type": "generate_ok", "msg_id": 13, "in_reply_to": 7, "id": "n0-13"}),
        json!({"type": "broadcast_ok", "msg_id": 13, "in_reply_to": 7}),
        json!({"type": "unicast_ok", "msg_id": 13, "in_reply_to": 7}),
        json!({"type": "read_ok", "msg_id": 13, "in_reply_to": 7, "messages": [42, 43]}),
        json!({"type": "topology_ok", "msg_id": 13, "in_reply_to": 7}),
    ]
    .into_iter()
    .map(|body| {
        let (src, dest) = if body.get("in_reply_to").is_some() {
            ("n0", "n1")
        } else {
            ("n1", "n0")
        };
        json!({"src": src, "dest": dest, "body": body})
    })
    .collect()
}

#[test]
fn all_message_variants_preserve_the_wire_format() {
    for wire in wire_messages() {
        let message: Message = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            matches!(&message.body, Payload::Response(_)),
            wire["body"].get("in_reply_to").is_some(),
        );
        assert_eq!(serde_json::to_value(message).unwrap(), wire);
    }
}

#[test]
fn common_identifiers_remain_required() {
    for wire in wire_messages() {
        for field in ["msg_id", "in_reply_to"] {
            let mut incomplete = wire.clone();
            if incomplete["body"]
                .as_object_mut()
                .unwrap()
                .remove(field)
                .is_some()
            {
                assert!(
                    serde_json::from_value::<Message>(incomplete).is_err(),
                    "{} must require {field}",
                    wire["body"]["type"],
                );
            }
        }
    }
}

async fn next_packet(recv: &mut Receiver<Packet>) -> Packet {
    tokio::time::timeout(Duration::from_secs(1), recv.recv())
        .await
        .expect("message was not queued")
        .expect("message channel closed")
}

async fn next_outgoing(recv: &mut Receiver<Packet>) -> Message {
    match next_packet(recv).await {
        Packet::Outgoing(message) => message,
        packet => panic!("expected an outgoing message, got {packet:?}"),
    }
}

#[tokio::test]
async fn node_responses_preserve_identifiers_and_data() {
    let init = serde_json::from_value(json!({
        "src": "c0", "dest": "n0",
        "body": {"type": "init", "msg_id": 70, "node_id": "n0", "node_ids": ["n0"]},
    }))
    .unwrap();
    let (mut node, mut recv) = Node::init_and_recv(init).await.unwrap();
    assert_eq!(
        serde_json::to_value(next_outgoing(&mut recv).await).unwrap(),
        json!({
            "src": "n0", "dest": "c0",
            "body": {"type": "init_ok", "msg_id": 0, "in_reply_to": 70},
        }),
    );

    let cases = [
        (
            "c0",
            json!({"type": "echo", "msg_id": 71, "echo": "hello"}),
            json!({"type": "echo_ok", "msg_id": 1, "in_reply_to": 71, "echo": "hello"}),
        ),
        (
            "c0",
            json!({"type": "generate", "msg_id": 72}),
            json!({"type": "generate_ok", "msg_id": 2, "in_reply_to": 72, "id": "n0-2"}),
        ),
        (
            "n1",
            json!({"type": "unicast", "msg_id": 73, "messages": [10, 20]}),
            json!({"type": "unicast_ok", "msg_id": 3, "in_reply_to": 73}),
        ),
        (
            "c0",
            json!({"type": "broadcast", "msg_id": 74, "message": 30}),
            json!({"type": "broadcast_ok", "msg_id": 4, "in_reply_to": 74}),
        ),
        (
            "c0",
            json!({"type": "topology", "msg_id": 75, "topology": {"n0": []}}),
            json!({"type": "topology_ok", "msg_id": 5, "in_reply_to": 75}),
        ),
        (
            "c0",
            json!({"type": "read", "msg_id": 76}),
            json!({"type": "read_ok", "msg_id": 6, "in_reply_to": 76, "messages": [10, 20, 30]}),
        ),
    ];
    for (src, body, expected_body) in cases {
        let request =
            serde_json::from_value(json!({"src": src, "dest": "n0", "body": body})).unwrap();
        node.recv(request).await.unwrap();
        let mut response = serde_json::to_value(next_outgoing(&mut recv).await).unwrap();
        if let Some(messages) = response["body"]
            .get_mut("messages")
            .and_then(Value::as_array_mut)
        {
            messages.sort_by_key(|value| value.as_u64().unwrap());
        }
        assert_eq!(
            response,
            json!({"src": "n0", "dest": src, "body": expected_body}),
        );
    }

    let reply = json!({
        "src": "n1", "dest": "n0",
        "body": {"type": "unicast_ok", "msg_id": 900, "in_reply_to": 27},
    });
    node.recv(serde_json::from_value(reply.clone()).unwrap())
        .await
        .unwrap();
    let Packet::Incoming(message) = next_packet(&mut recv).await else {
        panic!("received reply must be forwarded as Incoming");
    };
    assert_eq!(serde_json::to_value(message).unwrap(), reply);
}

#[tokio::test]
async fn batches_receive_fresh_ids_and_replies_match_requests() {
    let (batch_sender, batch_recv) = mpsc::channel(8);
    let (packet_sender, mut recv) = mpsc::channel(8);
    let numbering = MessageNumbering::new();
    assert_eq!(numbering.next(), 1);
    for message in [10, 20] {
        batch_sender
            .send(RequestPayload::Broadcast { message })
            .await
            .unwrap();
    }
    let batch = BroadcastBatch::new(
        "n0".into(),
        batch_recv,
        packet_sender,
        numbering.clone(),
        vec!["n1".into(), "n2".into()],
    );
    let task = tokio::spawn(batch.run(Duration::from_millis(1)));

    for (dest, msg_id) in [("n1", 2), ("n2", 3)] {
        let request = next_outgoing(&mut recv).await;
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            json!({
                "src": "n0", "dest": dest,
                "body": {"type": "unicast", "msg_id": msg_id, "messages": [10, 20]},
            }),
        );
        let reply: Message = serde_json::from_value(json!({
            "src": dest, "dest": "n0",
            "body": {"type": "unicast_ok", "msg_id": 900, "in_reply_to": msg_id},
        }))
        .unwrap();
        assert_eq!(request.unique_id(), reply.unique_id());
        assert_eq!(request.unique_id(), format!("{dest}-{msg_id}"));
    }
    assert_eq!(numbering.next(), 4);

    task.abort();
    let _ = task.await;
}
