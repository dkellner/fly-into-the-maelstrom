help:
    just --list

check:
    cargo clippy
    cargo fmt --check --all

test:
    just check
    cargo test

maelstrom-echo:
    cargo build --bin echo && \
    maelstrom test -w echo --bin "$CARGO_TARGET_DIR/debug/echo" \
      --node-count 1 --time-limit 10
