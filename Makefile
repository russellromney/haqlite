# haqlite development commands
#
# Prerequisites:
#   docker run -d --name rustfs -p 9000:9000 -p 9001:9001 \
#     -e RUSTFS_ACCESS_KEY=devkey -e RUSTFS_SECRET_KEY=devsecret \
#     rustfs/rustfs:latest /data
#
#   docker run -d --name nats -p 4222:4222 nats:2.10-alpine -js
#
#   aws s3 mb s3://haqlite-test --endpoint-url http://localhost:9000 \
#     AWS_ACCESS_KEY_ID=devkey AWS_SECRET_ACCESS_KEY=devsecret
#
#   cargo install cargo-nextest

# Local RustFS + NATS for integration tests
export AWS_ACCESS_KEY_ID := devkey
export AWS_SECRET_ACCESS_KEY := devsecret
export AWS_ENDPOINT_URL := http://localhost:9000
export AWS_REGION := us-east-1
export TIERED_TEST_BUCKET := haqlite-test
export NATS_URL := nats://localhost:4222

FEATURES := turbolite-cloud,nats-lease,nats-manifest

# Run all tests with all features, parallelized across test binaries (requires local RustFS + NATS)
test:
	cargo nextest run --features $(FEATURES)

# Fallback if nextest not installed
test-cargo:
	cargo test --features $(FEATURES)

# Run only unit tests (no infra needed)
test-unit:
	cargo test --lib

# Run tests against remote Tigris (slower, use for CI or when local infra unavailable)
test-remote:
	~/.soup/bin/soup run --project turbolite --env development -- \
		env NATS_URL=nats://localhost:4222 \
		cargo nextest run --features $(FEATURES)
