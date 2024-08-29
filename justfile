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
    maelstrom test -w echo \
      --bin "$CARGO_TARGET_DIR/debug/echo" \
      --node-count 1 \
      --time-limit 10

maelstrom-unique-ids:
    cargo build --bin unique-ids && \
    maelstrom test -w unique-ids \
      --bin "$CARGO_TARGET_DIR/debug/unique-ids" \
      --time-limit 30 \
      --rate 1000 \
      --node-count 3 \
      --availability total \
      --nemesis partition
