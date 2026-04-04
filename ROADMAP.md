# haqlite Roadmap

## Phase Meridian: Wire Up CLI Commands (DONE)

Fixed 7 CLI bugs across hadb-cli and haqlite. Added `--prefix` to S3Args (products apply own default, haqlite defaults to `"haqlite/"`). Replaced fake GFS flags (--hourly/--daily/--weekly/--monthly) with honest `--keep N` (default 47). Added graceful shutdown to replicate via `tokio::select!` + `shutdown_signal()`. Fixed restore to use `println!` instead of `tracing::info!`. Snapshot now returns `SnapshotResult { txid, db_name }` and prints both. Explain overridden with clean formatted ServeConfig output. All 7 ops functions take `prefix: &str`, `normalize_prefix()` helper added. 46 ops tests + 8 prefix tests + 34 HA tests passing. Also fixed walrust Phase Somme: in WAL mode, SQLite does not increment the file change counter per transaction. `sync_wal` now falls back to WAL commit count (deterministic from file content), `take_snapshot` uses file_change_counter + wal_commit_count. `read_frames_as_page_map` returns `commit_count`, new `count_wal_commits()` scans WAL headers only.

## Phase Rampart: Production Hardening (DONE)

HaQLiteError enum (6 variants), forwarding retry (100ms/400ms/1600ms backoff, no retry on 4xx), read semaphore (default 32, distinguishes NoPermits from EngineClosed), graceful shutdown (close semaphore, await handles, leave cluster), follower readiness (caught_up + replay_position from JoinResult, prometheus gauges). Atomic with hadb Phase Beacon and hakuzu Phase Parity. sync() now calls walrust flush(). 157 tests total.

## Phase Drain: Synchronous WAL Flush (DONE)

SqliteReplicator::sync() now calls walrust Replicator::flush() instead of being a no-op. close() path was already flushed via replicator.remove() internally, but handoff() now also calls sync() explicitly. 6 walrust flush tests + regression tests.

## Phase Volt-c: NATS Lease Store Engine Integration (DONE)

Pluggable LeaseStore on HaQLiteBuilder: `.lease_store(Arc<dyn LeaseStore>)` skips S3LeaseStore construction and uses any custom store (NATS, Redis, etcd). `hadb-lease-nats` as optional dependency behind `nats-lease` feature. `haqlite serve` reads `WAL_LEASE_NATS_URL` env var to auto-connect NATS with S3 fallback on failure. S3 client construction deferred to only when needed. Extracted shared test InMemoryStorage into `tests/common/mod.rs`. Fixed 5 snapshot test assertions (SQLite change counter is 2 after CREATE+INSERT, not 1). 164 tests passing.

---

## Phase Crest: HaMode::Shared (Lease-on-Write) (DONE)

> After: Phase Volt-c . Before: SQLite Extensions Support
> Depends on: hadb Phase Signal (ManifestStore trait)

HaMode::Shared for ephemeral compute (Lambda, Fly scale-to-zero). Lease-on-write coordination: every node is self-sufficient, acquires lease per write, catches up from manifest, writes, publishes, releases. No persistent leader, no forwarding server.

- Crest-a: HaMode enum, builder API (.mode, .manifest_store, .manifest_poll_interval, .write_timeout)
- Crest-b: Shared mode write path (lease -> catch-up -> write -> checkpoint -> publish -> release), turbolite integration
- Crest-c: Comprehensive tests (shared_mode.rs, turbolite_shared.rs)
- Crest-d: CLI integration (--mode shared, HAQLITE_MODE env)
- Manifest store wiring: nats-manifest and s3-manifest feature flags, auto-configure in serve.rs from env vars (WAL_MANIFEST_NATS_URL), Dedicated mode passes manifest_store to Coordinator

---

## SQLite Extensions Support

haqlite should support SQLite extensions (sqlite-vec, FTS5, sqlean, etc.) across the cluster. Extensions must be loaded on ALL connections — leader rw, follower reads, and walrust LTX apply — otherwise WAL replay fails or silently corrupts data.

**Dual-mode design:**

- **Embedded mode**: `.connection_init(|conn| { conn.load_extension(...)?; Ok(()) })` callback on `HaQLiteBuilder`. Stored as `Option<Arc<dyn Fn(&Connection) -> Result<()> + Send + Sync>>`. Called after every `Connection::open`. User controls what gets loaded and how. No S3 coordination needed — same binary = same extensions.

- **Server mode**: `haqlite serve --extension sqlite-vec.so --extension sqlean.so` CLI args. haqlite loads each extension on every connection. Leader writes extension name list to S3 cluster metadata (alongside lease). Followers read the list on startup and verify their local config matches — mismatch = crash with clear error (e.g., "leader requires sqlite-vec but this node doesn't have it configured").

**Implementation:**
- Store `Vec<PathBuf>` (server) or `Arc<dyn Fn>` (embedded) in `HaQLiteInner`
- Call on every `Connection::open` site: leader rw connection, follower fresh read connections, walrust apply connections
- walrust needs to accept an `on_connection_open` callback so it loads extensions on its apply connections too
- S3 verification (server mode only): leader PUTs extension list to `{prefix}/extensions.json`, follower GETs and compares on join

**Testing:**
- Test with a real extension (sqlite-vec or sqlean) loaded on leader and follower
- Test that WAL with extension data replays correctly on follower
- Test that missing extension on follower crashes immediately (not silent corruption)
- Test S3 extension list verification (server mode)

## crates.io Publish

- Verify public API surface is clean (no accidental pub internals)
- `cargo publish --dry-run`
- Publish order: walrust → hadb → hadb-s3 → haqlite

## hakuzu (Kuzu/graph HA)

- `KuzuReplicator: Replicator` (wraps graphstream)
- `GraphFollowerBehavior: FollowerBehavior` (checkpoint tracking, not TXID)
- Extract graphstream crate from graphd
- Integrate into graphd with `--ha` CLI flags

