# maelstrom_rust

Fly.io Gossip Glomers 과제를 Rust로 구현하는 프로젝트입니다.
상위 디렉터리에 Maelstrom 0.2.3 배포본이 있습니다.

현재는 프로젝트 골격과 JSON 의존성만 준비되어 있습니다.
`src/main.rs`에 `init`과 `echo` 메시지 처리를 구현한 뒤 테스트하세요.

## 빌드 및 테스트

이 프로젝트 디렉터리에서 실행합니다.

```bash
cargo build --release
../maelstrom test -w echo \
  --bin "$PWD/target/release/maelstrom_rust" \
  --node-count 1 \
  --time-limit 10
```

stdin에서 한 줄에 하나의 JSON 메시지를 읽고, stdout에 한 줄에 하나의
JSON 응답을 출력합니다. 로그는 `eprintln!()` 등으로 stderr에 출력합니다.
출력을 버퍼링한다면 응답을 보낸 뒤 flush해야 합니다.

## 테스트 결과

결과는 이 디렉터리의 `store/`에 저장되며 Git에서 제외합니다.
아래 명령을 실행한 뒤 <http://localhost:8080>에서 확인할 수 있습니다.

```bash
../maelstrom serve
```

## 참고

- [Echo 과제](https://fly.io/dist-sys/1/)
- [Maelstrom 프로토콜](https://github.com/jepsen-io/maelstrom/blob/v0.2.3/doc/protocol.md)
