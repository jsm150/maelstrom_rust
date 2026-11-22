use std::{collections::HashMap, time::Duration};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout},
    time::timeout,
};

use super::{send, start_node, wire};

#[derive(Default)]
struct MockKv {
    data: HashMap<(String, String), Value>,
}

impl MockKv {
    fn handle(&mut self, request: &Value) -> Value {
        let service = request["dest"].as_str().unwrap();
        assert!(matches!(service, "lin-kv" | "seq-kv"), "{request}");
        let body = &request["body"];
        let key = (service.to_owned(), body["key"].to_string());
        let mut reply = match body["type"].as_str().unwrap() {
            "read" => match self.data.get(&key) {
                Some(value) => json!({"type": "read_ok", "value": value}),
                None => json!({"type": "error", "code": 20}),
            },
            "write" => {
                self.data.insert(key, body["value"].clone());
                json!({"type": "write_ok"})
            }
            "cas" => match self.data.get(&key) {
                Some(value) if *value != body["from"] => json!({"type": "error", "code": 22}),
                None if body["create_if_not_exists"] != true => {
                    json!({"type": "error", "code": 20})
                }
                _ => {
                    self.data.insert(key, body["to"].clone());
                    json!({"type": "cas_ok"})
                }
            },
            _ => panic!("알 수 없는 KV 요청: {request}"),
        };
        reply["in_reply_to"] = body["msg_id"].clone();
        wire(service, request["src"].as_str().unwrap(), reply)
    }
}

struct Broker {
    id: &'static str,
    _child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl Broker {
    async fn start(id: &'static str) -> Self {
        let (mut child, stdin) = start_node(env!("CARGO_BIN_EXE_kafka"), id, &["n0", "n1"]).await;
        let stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut broker = Self {
            id,
            _child: child,
            stdin,
            stdout,
        };
        assert_eq!(broker.next().await["body"]["type"], "init_ok");
        broker
    }

    async fn next(&mut self) -> Value {
        let line = timeout(Duration::from_secs(5), self.stdout.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(&line).unwrap()
    }

    async fn request(&mut self, id: usize, mut body: Value) {
        body["msg_id"] = json!(id);
        send(&mut self.stdin, &wire("c1", self.id, body)).await;
    }

    async fn reply_from_kv(&mut self, request: &Value, mut body: Value) {
        body["in_reply_to"] = request["body"]["msg_id"].clone();
        let service = request["dest"].as_str().unwrap();
        send(&mut self.stdin, &wire(service, self.id, body)).await;
    }

    async fn begin_poll(&mut self, start: usize, end: usize) {
        self.request(10, json!({"type":"poll", "offsets":{"orders":start}}))
            .await;
        let read = self.next().await;
        assert_eq!(read["dest"], "lin-kv");
        assert_eq!(read["body"]["type"], "read");
        assert_eq!(read["body"]["key"], json!(["next_offset", "orders"]));
        self.reply_from_kv(&read, json!({"type":"read_ok", "value":end}))
            .await;
    }

    // 묶음 전체가 도착할 때까지 응답을 보류해 병렬 요청을 확인한다.
    async fn poll_reads(&mut self, offsets: std::ops::Range<usize>) -> Vec<Value> {
        let mut reads = Vec::new();
        for _ in offsets.clone() {
            let read = self.next().await;
            assert_eq!(read["dest"], "seq-kv");
            assert_eq!(read["body"]["type"], "read");
            reads.push(read);
        }
        reads.sort_unstable_by_key(|read| read["body"]["key"][1].as_u64().unwrap());
        for (offset, read) in offsets.zip(&reads) {
            assert_eq!(read["body"]["key"], json!(["orders", offset]));
        }
        reads
    }

    // 지정한 메시지는 테스트가 전달 시점을 제어하고, 나머지 KV 요청에는 즉시 응답한다.
    async fn until(&mut self, kv: &mut MockKv, dest: &str, kind: &str) -> Value {
        timeout(Duration::from_secs(5), async {
            loop {
                let frame = self.next().await;
                assert_eq!(frame["src"], self.id);
                if frame["dest"] == dest && frame["body"]["type"] == kind {
                    return frame;
                }
                send(&mut self.stdin, &kv.handle(&frame)).await;
            }
        })
        .await
        .unwrap()
    }

    async fn expect_reply(&mut self, kv: &mut MockKv, id: usize, mut expected: Value) {
        let reply = self
            .until(kv, "c1", expected["type"].as_str().unwrap())
            .await;
        assert!(reply["body"]["msg_id"].is_u64());
        expected["msg_id"] = reply["body"]["msg_id"].clone();
        expected["in_reply_to"] = json!(id);
        assert_eq!(reply["body"], expected);
    }
}

async fn check_kafka(exchanges: &[(Value, Value)]) {
    let mut node = Broker::start("n0").await;
    let mut kv = MockKv::default();
    for (index, (request, response)) in exchanges.iter().enumerate() {
        let id = 100 + index;
        node.request(id, request.clone()).await;
        node.expect_reply(&mut kv, id, response.clone()).await;
    }
}

#[tokio::test]
async fn kafka_assigns_offsets_per_key_and_polls_the_full_suffix() {
    check_kafka(&[
        (
            json!({"type": "send", "key": "a", "msg": 10}),
            json!({"type": "send_ok", "offset": 0}),
        ),
        (
            json!({"type": "send", "key": "b", "msg": 0}),
            json!({"type": "send_ok", "offset": 0}),
        ),
        (
            json!({"type": "send", "key": "a", "msg": 11}),
            json!({"type": "send_ok", "offset": 1}),
        ),
        (
            json!({"type": "send", "key": "a", "msg": usize::MAX}),
            json!({"type": "send_ok", "offset": 2}),
        ),
        (
            json!({"type": "poll", "offsets": {"a": 1, "b": 0, "missing": 0}}),
            json!({"type": "poll_ok", "msgs": {"a": [[1, 11], [2, usize::MAX]], "b": [[0, 0]]}}),
        ),
        (
            json!({"type": "poll", "offsets": {"a": 2}}),
            json!({"type": "poll_ok", "msgs": {"a": [[2, usize::MAX]]}}),
        ),
        (
            json!({"type": "poll", "offsets": {"a": 3, "b": 9}}),
            json!({"type": "poll_ok", "msgs": {}}),
        ),
    ])
    .await;
}

#[tokio::test]
async fn poll_reads_eight_offsets_at_a_time_and_returns_them_in_order() {
    let mut node = Broker::start("n0").await;
    node.begin_poll(10, 20).await;
    let first = node.poll_reads(10..18).await;
    for (offset, read) in (11..18).zip(&first[1..]).rev() {
        node.reply_from_kv(read, json!({"type":"read_ok", "value":offset * 10}))
            .await;
    }

    // 10번 응답을 보류한 동안 다음 묶음의 읽기가 출력되지 않아야 한다.
    node.request(11, json!({"type":"poll", "offsets":{}})).await;
    let empty_poll = node.next().await;
    assert_eq!(empty_poll["body"]["type"], "poll_ok");
    assert_eq!(empty_poll["body"]["in_reply_to"], 11);

    node.reply_from_kv(&first[0], json!({"type":"read_ok", "value":100}))
        .await;
    let second = node.poll_reads(18..20).await;
    for (offset, read) in (18..20).zip(&second).rev() {
        node.reply_from_kv(read, json!({"type":"read_ok", "value":offset * 10}))
            .await;
    }
    let reply = node.next().await;
    let expected: Vec<_> = (10..20).map(|offset| [offset, offset * 10]).collect();
    assert_eq!(reply["body"]["type"], "poll_ok");
    assert_eq!(reply["body"]["in_reply_to"], 10);
    assert_eq!(reply["body"]["msgs"], json!({"orders":expected}));
}

#[tokio::test]
async fn poll_stops_at_the_first_gap_or_error_in_offset_order() {
    for error_offset in [15, 11] {
        let mut node = Broker::start("n0").await;
        node.begin_poll(10, 20).await;
        let reads = node.poll_reads(10..18).await;
        for (offset, read) in (10..18).zip(&reads).rev() {
            let response = match offset {
                12 => json!({"type":"error", "code":20}),
                n if n == error_offset => json!({"type":"error", "code":13, "text":"조회 실패"}),
                _ => json!({"type":"read_ok", "value":offset}),
            };
            node.reply_from_kv(read, response).await;
        }
        let reply = node.next().await;
        assert_eq!(reply["dest"], "c1");
        assert_eq!(reply["body"]["in_reply_to"], 10);
        if error_offset > 12 {
            assert_eq!(reply["body"]["type"], "poll_ok");
            assert_eq!(reply["body"]["msgs"], json!({"orders":[[10,10],[11,11]]}));
        } else {
            assert_eq!(reply["body"]["type"], "error");
            assert_eq!(reply["body"]["code"], 13);
        }
    }
}

#[tokio::test]
async fn kafka_commits_advance_independently_of_poll() {
    check_kafka(&[
        (
            json!({"type": "send", "key": "a", "msg": 10}),
            json!({"type": "send_ok", "offset": 0}),
        ),
        (
            json!({"type": "send", "key": "a", "msg": 11}),
            json!({"type": "send_ok", "offset": 1}),
        ),
        (
            json!({"type": "send", "key": "b", "msg": 20}),
            json!({"type": "send_ok", "offset": 0}),
        ),
        (
            json!({"type": "list_committed_offsets", "keys": ["a", "b"]}),
            json!({"type": "list_committed_offsets_ok", "offsets": {}}),
        ),
        (
            json!({"type": "commit_offsets", "offsets": {"a": 0, "b": 0}}),
            json!({"type": "commit_offsets_ok"}),
        ),
        (
            json!({"type": "commit_offsets", "offsets": {"a": 1}}),
            json!({"type": "commit_offsets_ok"}),
        ),
        (
            json!({"type": "commit_offsets", "offsets": {"a": 0}}),
            json!({"type": "commit_offsets_ok"}),
        ),
        (
            json!({"type": "list_committed_offsets", "keys": ["a", "missing"]}),
            json!({"type": "list_committed_offsets_ok", "offsets": {"a": 1}}),
        ),
        (
            json!({"type": "list_committed_offsets", "keys": ["b"]}),
            json!({"type": "list_committed_offsets_ok", "offsets": {"b": 0}}),
        ),
        (
            json!({"type": "poll", "offsets": {"a": 0}}),
            json!({"type": "poll_ok", "msgs": {"a": [[0, 10], [1, 11]]}}),
        ),
    ])
    .await;
}

#[tokio::test]
async fn concurrent_sends_retry_allocation_and_poll_stops_at_the_unwritten_offset() {
    let mut first = Broker::start("n0").await;
    let mut second = Broker::start("n1").await;
    let mut kv = MockKv::default();
    let append = |msg| json!({"type":"send", "key":"orders", "msg":msg});
    first.request(10, append(100)).await;
    let init = first.until(&mut kv, "lin-kv", "cas").await;
    assert_eq!(init["body"]["key"], json!(["next_offset", "orders"]));
    assert!(init["body"]["from"].is_null());
    assert_eq!(init["body"]["create_if_not_exists"], true);
    send(&mut first.stdin, &kv.handle(&init)).await;
    first
        .expect_reply(&mut kv, 10, json!({"type":"send_ok","offset":0}))
        .await;

    first.request(11, append(101)).await;
    second.request(12, append(102)).await;
    let cas1 = first.until(&mut kv, "lin-kv", "cas").await;
    let cas2 = second.until(&mut kv, "lin-kv", "cas").await;
    for cas in [&cas1, &cas2] {
        assert_eq!(cas["body"]["from"], 1);
        assert_eq!(cas["body"]["to"], 2);
    }
    send(&mut first.stdin, &kv.handle(&cas1)).await;
    let write = first.until(&mut kv, "seq-kv", "write").await;
    assert_eq!(write["body"]["key"], json!(["orders", 1]));
    assert_eq!(write["body"]["value"], 101);

    let conflict = kv.handle(&cas2);
    assert_eq!(conflict["body"]["code"], 22);
    send(&mut second.stdin, &conflict).await;
    let reread = second.next().await;
    assert_eq!(reread["body"]["type"], "read");
    assert_eq!(reread["body"]["key"], cas2["body"]["key"]);
    send(&mut second.stdin, &kv.handle(&reread)).await;
    let retry = second.next().await;
    assert_eq!(retry["body"]["type"], "cas");
    assert_eq!(retry["body"]["from"], 2);
    assert_eq!(retry["body"]["to"], 3);
    assert_ne!(retry["body"]["msg_id"], cas2["body"]["msg_id"]);
    send(&mut second.stdin, &kv.handle(&retry)).await;
    second
        .expect_reply(&mut kv, 12, json!({"type":"send_ok","offset":2}))
        .await;

    second
        .request(13, json!({"type":"poll","offsets":{"orders":0}}))
        .await;
    second
        .expect_reply(
            &mut kv,
            13,
            json!({"type":"poll_ok","msgs":{"orders":[[0,100]]}}),
        )
        .await;
    first
        .request(14, json!({"type":"list_committed_offsets","keys":[]}))
        .await;
    first
        .expect_reply(
            &mut kv,
            14,
            json!({"type":"list_committed_offsets_ok","offsets":{}}),
        )
        .await;

    send(&mut first.stdin, &kv.handle(&write)).await;
    first
        .expect_reply(&mut kv, 11, json!({"type":"send_ok","offset":1}))
        .await;
    second
        .request(15, json!({"type":"poll","offsets":{"orders":1}}))
        .await;
    second
        .expect_reply(
            &mut kv,
            15,
            json!({"type":"poll_ok","msgs":{"orders":[[1,101],[2,102]]}}),
        )
        .await;
}

#[tokio::test]
async fn concurrent_commits_recheck_the_target_after_a_conflict() {
    let mut first = Broker::start("n0").await;
    let mut second = Broker::start("n1").await;
    let mut kv = MockKv::default();
    let commit = |target| json!({"type":"commit_offsets", "offsets":{"orders":target}});
    for (round, (winner, contender)) in [(9, 7), (11, 12)].into_iter().enumerate() {
        let id = 100 + 3 * round;
        first.request(id, commit(winner)).await;
        second.request(id + 1, commit(contender)).await;
        let winning = first.until(&mut kv, "lin-kv", "cas").await;
        let losing = second.until(&mut kv, "lin-kv", "cas").await;
        assert_eq!(
            winning["body"]["key"],
            json!(["committed_offset", "orders"])
        );
        assert_eq!(winning["body"]["from"], losing["body"]["from"]);
        send(&mut first.stdin, &kv.handle(&winning)).await;
        let conflict = kv.handle(&losing);
        assert_eq!(conflict["body"]["code"], 22);
        send(&mut second.stdin, &conflict).await;
        let read = second.next().await;
        assert_eq!(read["body"]["type"], "read");
        send(&mut second.stdin, &kv.handle(&read)).await;
        if contender > winner {
            let retry = second.next().await;
            assert_eq!(retry["body"]["type"], "cas");
            assert_eq!(retry["body"]["from"], winner);
            assert_eq!(retry["body"]["to"], contender);
            send(&mut second.stdin, &kv.handle(&retry)).await;
        }
        let done = second.next().await;
        assert_eq!(done["dest"], "c1");
        assert_eq!(done["body"]["type"], "commit_offsets_ok");
        assert_eq!(done["body"]["in_reply_to"], id + 1);
        first
            .expect_reply(&mut kv, id, json!({"type":"commit_offsets_ok"}))
            .await;
        first
            .request(
                id + 2,
                json!({"type":"list_committed_offsets","keys":["orders"]}),
            )
            .await;
        first.expect_reply(&mut kv, id + 2,
            json!({"type":"list_committed_offsets_ok","offsets":{"orders":winner.max(contender)}})).await;
    }
}

#[tokio::test]
async fn ambiguous_cas_result_does_not_allocate_another_offset() {
    let mut node = Broker::start("n0").await;
    let mut kv = MockKv::default();
    node.request(10, json!({"type":"send","key":"k","msg":5}))
        .await;
    let cas = node.until(&mut kv, "lin-kv", "cas").await;
    let applied = kv.handle(&cas);
    assert_eq!(applied["body"]["type"], "cas_ok");
    send(
        &mut node.stdin,
        &wire(
            "lin-kv",
            "n0",
            json!({
                "type":"error", "code":0, "in_reply_to":cas["body"]["msg_id"]
            }),
        ),
    )
    .await;
    let reply = node.next().await;
    assert_eq!(reply["dest"], "c1");
    assert_eq!(reply["body"]["in_reply_to"], 10);
    assert_eq!(reply["body"]["type"], "error");
    assert_eq!(reply["body"]["code"], 0);
}
