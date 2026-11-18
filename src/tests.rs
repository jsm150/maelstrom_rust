use super::*;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncWriteExt, duplex},
    time::{Duration, timeout},
};

struct ProbePayload;

impl Payload for ProbePayload {
    type Request = ProbeRequest;
    type Response = ProbeResponse;
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ProbeRequest {
    Start,
    Read,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ProbeResponse {
    ReadOk { value: usize },
}

struct ProbeNode {
    tx: NodeTx<ProbePayload>,
}

impl Node for ProbeNode {
    type Payload = ProbePayload;

    fn init(_: Init, tx: NodeTx<ProbePayload>) -> Result<Self> {
        Ok(Self { tx })
    }

    async fn recv(&mut self, message: Message<ProbePayload>) -> Result<()> {
        match message.body {
            Body::Request {
                payload: ProbeRequest::Start,
                ..
            } => {
                self.tx.request("n1", ProbeRequest::Read).await?;
                self.tx.request_with_retry("n2", ProbeRequest::Read).await?;
            }
            Body::Response {
                in_reply_to,
                payload,
                ..
            } => {
                self.tx.reply("c0", in_reply_to, |_| payload).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

fn wire(src: &str, dest: &str, body: Value) -> Value {
    json!({"src": src, "dest": dest, "body": body})
}

fn check_wire<P: Payload>(body: Value) {
    let message = wire("c0", "n0", body);
    let parsed: Message<P> = serde_json::from_value(message.clone()).unwrap();
    assert_eq!(
        matches!(parsed.body, Body::Response { .. }),
        message["body"].get("in_reply_to").is_some()
    );
    assert_eq!(serde_json::to_value(parsed).unwrap(), message);
    for key in ["msg_id", "in_reply_to"] {
        let mut incomplete = message.clone();
        if incomplete["body"]
            .as_object_mut()
            .unwrap()
            .remove(key)
            .is_some()
        {
            assert!(serde_json::from_value::<Message<P>>(incomplete).is_err());
        }
    }
}

#[test]
fn wire_format_and_required_ids() {
    check_wire::<InitPayload>(
        json!({"type": "init", "msg_id": 7, "node_id": "n0", "node_ids": ["n0"]}),
    );
    check_wire::<InitPayload>(json!({"type": "init_ok", "msg_id": 0, "in_reply_to": 7}));
    check_wire::<ProbePayload>(json!({"type": "read", "msg_id": 1}));
    check_wire::<ProbePayload>(
        json!({"type": "read_ok", "msg_id": 8, "in_reply_to": 1, "value": 42}),
    );
}

async fn write_json(writer: &mut (impl AsyncWrite + Unpin), message: Value) {
    writer
        .write_all(format!("{message}\n").as_bytes())
        .await
        .unwrap();
}

#[test]
fn in_reply_to_distinguishes_responses_when_payload_types_overlap() {
    struct SharedPayload;
    impl Payload for SharedPayload {
        type Request = ProbeRequest;
        type Response = ProbeRequest;
    }
    for (body, is_response) in [
        (json!({"type": "read", "msg_id": 1}), false),
        (json!({"type": "read", "msg_id": 2, "in_reply_to": 1}), true),
    ] {
        let message: Message<SharedPayload> =
            serde_json::from_value(wire("n0", "n1", body)).unwrap();
        assert_eq!(matches!(message.body, Body::Response { .. }), is_response);
    }
}

async fn read_json(input: &mut tokio::io::Lines<impl AsyncBufRead + Unpin>) -> Value {
    let line = timeout(Duration::from_secs(2), input.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(&line).unwrap()
}

#[tokio::test(start_paused = true)]
async fn retry_only_the_selected_request_until_the_matching_peer_replies() {
    let (mut client, input) = duplex(4096);
    let (output, reader) = duplex(4096);
    let mut lines = BufReader::new(reader).lines();
    let node = run::<ProbeNode>(BufReader::new(input), output);
    let client_session = async move {
        write_json(
            &mut client,
            wire(
                "c0",
                "n0",
                json!({"type": "init", "msg_id": 70, "node_id": "n0", "node_ids": ["n0", "n1", "n2"]}),
            ),
        )
        .await;
        assert_eq!(
            read_json(&mut lines).await,
            wire(
                "n0",
                "c0",
                json!({"type": "init_ok", "msg_id": 0, "in_reply_to": 70})
            )
        );
        write_json(
            &mut client,
            wire("c0", "n0", json!({"type": "start", "msg_id": 71})),
        )
        .await;
        assert_eq!(
            read_json(&mut lines).await,
            wire("n0", "n1", json!({"type": "read", "msg_id": 1}))
        );
        let request = read_json(&mut lines).await;
        assert_eq!(
            request,
            wire("n0", "n2", json!({"type": "read", "msg_id": 2}))
        );

        for (peer, in_reply_to, value) in [("n1", 2, 10), ("n2", 1, 15), ("n2", 2, 20)] {
            write_json(
                &mut client,
                wire(
                    peer,
                    "n0",
                    json!({"type": "read_ok", "msg_id": 8, "in_reply_to": in_reply_to, "value": value}),
                ),
            )
            .await;
            let forwarded = read_json(&mut lines).await;
            assert_eq!(forwarded["body"]["value"], value);
            assert_eq!(forwarded["body"]["in_reply_to"], in_reply_to);
            if (peer, in_reply_to) != ("n2", 2) {
                assert_eq!(read_json(&mut lines).await, request);
            }
        }
        assert!(
            timeout(Duration::from_secs(2), lines.next_line())
                .await
                .is_err()
        );
        drop(client);
        assert!(
            timeout(Duration::from_secs(1), lines.next_line())
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
    };
    let (result, ()) = tokio::join!(node, client_session);
    result.unwrap();
}
