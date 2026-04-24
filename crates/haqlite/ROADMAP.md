# haqlite Roadmap

## SQLite Extensions Support

haqlite should support SQLite extensions (sqlite-vec, FTS5, sqlean, etc.) across the cluster. Extensions must be loaded on ALL connections, including leader rw, follower reads, and walrust LTX apply.

- **Embedded mode**: `.connection_init(|conn| { ... })` callback on builder
- **Server mode**: `haqlite serve --extension sqlite-vec.so` CLI args
- Extensions loaded on every connection open site
- walrust needs `on_connection_open` callback for apply connections
- S3 verification (server mode): leader PUTs extension list, follower verifies on join

## crates.io Publish

- Verify public API surface
- `cargo publish --dry-run`
- Publish order: walrust -> hadb -> hadb-s3 -> haqlite

## hakuzu (Kuzu/graph HA)

- `KuzuReplicator: Replicator` (wraps graphstream)
- `GraphFollowerBehavior: FollowerBehavior`
- Extract graphstream crate from graphd
- Integrate with `--ha` CLI flags
