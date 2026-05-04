FROM rust:1-bookworm AS builder
WORKDIR /workspace

RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev cmake && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN CARGO_TARGET_DIR=/workspace/target RUSTC_WRAPPER= \
    cargo build --release -p haqlite-turbolite --features s3 --bin haqlite-turbolite-bench

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y ca-certificates curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip && \
    apt-get remove -y curl unzip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/target/release/haqlite-turbolite-bench /usr/local/bin/haqlite-turbolite-bench
COPY benchmark/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
