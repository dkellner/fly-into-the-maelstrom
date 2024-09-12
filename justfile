help:
    just --list

check:
    cargo clippy
    cargo fmt --check --all

fmt:
    cargo fmt --all

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

maelstrom-broadcast-a:
    cargo build --bin broadcast && \
    maelstrom test -w broadcast \
      --bin "$CARGO_TARGET_DIR/debug/broadcast" \
      --node-count 1 \
      --time-limit 20 \
      --rate 10

maelstrom-broadcast-b:
    cargo build --bin broadcast && \
    maelstrom test -w broadcast \
      --bin "$CARGO_TARGET_DIR/debug/broadcast" \
      --node-count 5 \
      --time-limit 20 \
      --rate 10

maelstrom-broadcast-c:
    cargo build --bin broadcast && \
    maelstrom test -w broadcast \
      --bin "$CARGO_TARGET_DIR/debug/broadcast" \
      --node-count 5 \
      --time-limit 20 \
      --rate 10 \
      --nemesis partition

maelstrom-broadcast-d:
    cargo build --bin broadcast && \
    maelstrom test -w broadcast \
      --bin "$CARGO_TARGET_DIR/debug/broadcast" \
      --node-count 25 \
      --time-limit 20 \
      --rate 100 \
      --latency 100 && \
    echo -e "\nRelevant metrics:" && \
    grep -A 5 -E "(:servers|:stable-latencies)" store/latest/jepsen.log \
      | grep -A 5 -E "(:msgs-per-op|:stable-latencies)" && \
    echo -e "\nObjectives:" && \
    echo "- messages per operation < 30" && \
    echo "- median latency < 400ms" && \
    echo "- maximum latency < 600ms"

maelstrom-broadcast-e:
    cargo build --bin broadcast && \
    maelstrom test -w broadcast \
      --bin "$CARGO_TARGET_DIR/debug/broadcast" \
      --node-count 25 \
      --time-limit 20 \
      --rate 100 \
      --latency 100 \
      --nemesis partition && \
    echo -e "\nRelevant metrics:" && \
    grep -A 5 -E "(:servers|:stable-latencies)" store/latest/jepsen.log \
      | grep -A 5 -E "(:msgs-per-op|:stable-latencies)" && \
    echo -e "\nObjectives:" && \
    echo "- messages per operation < 20" && \
    echo "- median latency < 1s" && \
    echo "- maximum latency < 2s"
