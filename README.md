# maelstrom_rust

Fly.io Gossip Glomers 과제를 Rust로 구현합니다. `echo`, `unique-ids`, `broadcast`는
각자 `Node`와 요청·응답 `Payload`를 정의하고 라이브러리의 `main_loop`에서 실행합니다.
상위 디렉터리에 Maelstrom 0.2.3 배포본이 있습니다.

## 빌드 및 테스트

이 프로젝트 디렉터리에서 실행합니다.

```bash
cargo test --locked
cargo build --release --locked --bins
../maelstrom test -w echo --bin "$PWD/target/release/echo" --node-count 1 --time-limit 10
../maelstrom test -w unique-ids --bin "$PWD/target/release/unique-ids" \
  --node-count 3 --time-limit 30 --rate 1000 --availability total --nemesis partition
../maelstrom test -w broadcast --bin "$PWD/target/release/broadcast" \
  --node-count 5 --time-limit 20 --rate 10 --nemesis partition
../maelstrom test -w broadcast --bin "$PWD/target/release/broadcast" \
  --node-count 25 --time-limit 20 --rate 100 --latency 100
```

stdin에서 한 줄에 하나의 JSON 메시지를 읽고, stdout에 한 줄에 하나의 JSON 메시지를 출력합니다.
로그는 stderr에 출력합니다.

## 테스트 결과

결과는 이 디렉터리의 `store/`에 저장됩니다. `../maelstrom serve`를 실행하면
<http://localhost:8080>에서 확인할 수 있습니다.

## 참고

- [Fly.io Gossip Glomers](https://fly.io/dist-sys/)
- [Maelstrom 프로토콜](https://github.com/jepsen-io/maelstrom/blob/v0.2.3/doc/protocol.md)
