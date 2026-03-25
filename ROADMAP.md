# haqlite Roadmap

## Phase Meridian: Wire Up CLI Commands

> After: hadb-cli + haqlite serve + hrana + ops implementation

`haqlite serve` is production-ready. The 6 non-serve commands (restore, list, verify, compact, replicate, snapshot) plus explain all have issues that make them broken or misleading in practice.

### Problems

1. **No `--prefix` on non-serve commands** -- serve uses prefix `"haqlite/"` but every other command hardcodes `""`. They can't find any data created by serve.
2. **Compact GFS flags are fake** -- `--hourly/--daily/--weekly/--monthly` just get summed into keep-N. No temporal bucketing.
3. **Replicate has no graceful shutdown** -- loops forever, no SIGTERM handling.
4. **Restore uses tracing, not println** -- inconsistent with every other command's output.
5. **Snapshot doesn't report S3 location** -- user can't tell what name was used or where it went.
6. **Explain dumps raw Rust Debug** for product config section.
7. **Arg help text unclear** about "name in S3" vs "local file path".

### Meridian-a: hadb-cli changes

- Add `--prefix` to `S3Args` (`args.rs`): `#[arg(long, env = "HADB_PREFIX", default_value = "")]`. Products apply their own default.
- Replace 4 GFS flags with `--keep N` on `CompactArgs` (default 47, preserves old sum). Honest about what it does.
- Improve help text on positional args (name = "Database name in S3", database = "Path to local database file").
- Update explain default in `runner.rs`, fix test compilation.
- `RetentionSection` in config stays (serve may use it for scheduled snapshots later).

### Meridian-b: haqlite ops prefix threading

- Add `prefix: &str` param to all 7 ops functions (`discover_ltx_files`, `discover_databases`, `list_databases`, `verify_database`, `plan_compact`, `snapshot_database`, `replicate_database`).
- Add `normalize_prefix()` helper (handles missing trailing slash).
- `snapshot_database` returns `SnapshotResult { txid, db_name }` instead of bare `u64`.
- Wrap replicate loop in `tokio::select!` with `hadb_cli::shutdown_signal()`.

### Meridian-c: haqlite CLI dispatch

- Add `DEFAULT_PREFIX = "haqlite/"` and `resolve_prefix()` helper to `main.rs`. If `--prefix` is empty (default from hadb-cli), use `"haqlite/"`.
- Wire `resolve_prefix(&args.s3)` through all 6 commands.
- Fix restore: `println!` instead of `tracing::info!`.
- Fix snapshot: print db_name and TXID from `SnapshotResult`.
- Update compact: `args.keep` instead of `args.hourly + ... + args.monthly`.
- Override `explain()` with clean formatted ServeConfig output.

### Meridian-d: Tests

- Update all 37 existing ops tests: add `""` as prefix param (no test data changes needed).
- Update snapshot tests to destructure `SnapshotResult`.
- Add ~8 new tests: prefix discovery, prefix isolation, prefix normalization, snapshot result fields.
- Fix hadb-cli test compilation for new S3Args/CompactArgs fields.

### Files

| File | Changes |
|------|---------|
| `hadb/hadb-cli/src/args.rs` | prefix on S3Args, --keep on CompactArgs, help text |
| `hadb/hadb-cli/src/runner.rs` | explain default, test fix |
| `hadb/hadb-cli/src/config.rs` | keep field on RetentionSection |
| `haqlite/src/ops.rs` | prefix params, SnapshotResult, shutdown, normalize_prefix |
| `haqlite/src/bin/main.rs` | resolve_prefix, wire prefix, fix outputs, explain override |
| `haqlite/tests/test_ops.rs` | update 37 tests, add ~8 prefix tests |

## Phase Rampart: Production Hardening (from hakuzu)

> After: Phase Meridian · Before: SQLite Extensions Support

hakuzu has been hardened through 10 phases of production work. haqlite is missing 5 features that hakuzu already has. All code should be extracted from hakuzu's existing implementation, not written new.

**Ordering constraint:** Rampart-a must precede Rampart-c (semaphore returns `EngineClosed` variant). Rampart-c must precede Rampart-d (close() shuts down the semaphore). Rampart-e is atomic with hadb Phase Beacon (see below). Rampart-b is independent.

### Rampart-a: Structured Error Types

haqlite uses `anyhow::Result` everywhere (`src/database.rs:377,390,430,531,551,569`). Consumers (like cinch-engine) cannot match on failure modes. hakuzu solved this with a `HakuzuError` enum.

- New file: `src/error.rs` (~55 lines, extracted from `hakuzu/src/error.rs`)
- Variants: `LeaderUnavailable(String)`, `NotLeader`, `DatabaseError(String)`, `ReplicationError(String)`, `CoordinatorError(String)`, `EngineClosed`
- `impl Display`, `impl Error`, `impl From<anyhow::Error>`, `type Result<T>`
- Replace `anyhow::Result` returns in public API: `execute()`, `query_row()`, `query_values()`, `close()`, `handoff()`
- Internal methods can stay `anyhow` where the error gets converted at the boundary
- Add `mod error; pub use error::{HaQLiteError, Result};` to `src/lib.rs`

Source: `hakuzu/src/error.rs:8-51` (copy, rename `HakuzuError` to `HaQLiteError`, rename `JournalError` to `ReplicationError`)

Tests (~10, extracted from `hakuzu/src/error.rs` test module):
- Display for each variant, Error trait, From<anyhow>, Result alias, exhaustive match

### Rampart-b: Forwarding Retry with Backoff

haqlite's `execute_forwarded()` (`src/database.rs:569-604`) makes a single HTTP request with no retry. hakuzu retries with exponential backoff (100ms, 400ms, 1600ms) and skips retry on 4xx client errors.

This is a **pattern extraction**, not a copy-paste. hakuzu sends `{cypher, params}` to `/hakuzu/execute` and parses `{ok: bool}`; haqlite sends `{sql, params}` to `/haqlite/execute` and parses `{rows_affected: u64}`. The retry loop structure (backoff timing, 4xx skip, exhaustion error) is identical; the request/response handling differs.

- Extract the retry loop structure from hakuzu's `execute_forwarded`
- Backoffs: `[100ms, 400ms, 1600ms]` (same as hakuzu)
- Don't retry 4xx (client errors won't succeed on retry)
- Return `HaQLiteError::LeaderUnavailable` on exhaustion, `HaQLiteError::NotLeader` if no leader address

Source: `hakuzu/src/database.rs:816-885` (retry loop pattern to adapt, not copy verbatim)

Tests (~3):
- Forwarding succeeds on first attempt
- Forwarding retries on 5xx, succeeds on second attempt
- Forwarding does NOT retry on 4xx

### Rampart-c: Read Semaphore

haqlite has no read concurrency control. On a follower, every `query_row()` and `query_values()` opens a fresh `rusqlite::Connection` (`src/database.rs:414-421`). Under load this is unbounded.

- Add `read_semaphore: tokio::sync::Semaphore` to `HaQLiteInner`
- Add `read_concurrency: usize` to `HaQLiteBuilder` (default 32)
- Add `.read_concurrency(n)` builder method
- Wrap follower reads in `query_row()` and `query_values()` with `self.inner.read_semaphore.acquire()` (return `HaQLiteError::EngineClosed` if semaphore is closed)
- Leader reads through the persistent rw connection don't need the semaphore (already serialized by the Mutex)

Source: `hakuzu/src/database.rs:74,92,166-167,324,346,609-615,762` (semaphore creation, builder method, acquire in query, close)

Tests (~3):
- Read semaphore limits concurrent follower reads
- Read semaphore does not block leader reads
- Closed semaphore returns EngineClosed

### Rampart-d: Graceful Shutdown with Drain

haqlite's `close()` (`src/database.rs:531-545`) does not drain in-flight writes or seal the journal before leaving. hakuzu's `close()` acquires the write mutex (drains in-flight writes), seals the journal via `replicator.sync()`, then leaves the cluster.

- Acquire write mutex before leaving (drain in-flight writes). haqlite currently has no write mutex; add `write_mutex: Arc<tokio::sync::Mutex<()>>` to `HaQLiteInner` (same pattern as hakuzu `src/database.rs:343`).
- Call `replicator.sync(&db_name)` to flush pending WAL + upload. **Verify first:** confirm walrust's `SqliteReplicator::sync()` actually flushes pending WAL data and uploads to S3. If it's a no-op or partial, the drain is incomplete and we need to fix walrust first.
- Close the read semaphore (new reads get `EngineClosed` immediately)
- Then leave cluster and abort background tasks
- Wait for background task handles (so `Arc<HaQLiteInner>` refs are released before drop)

Source: `hakuzu/src/database.rs:759-793` (complete close() with drain, seal, abort, await)

Tests (~2):
- Close drains in-flight writes (start a write, call close, write completes)
- Close seals journal before leaving cluster

### Rampart-e: Follower Readiness

**Atomicity: Rampart-e MUST land simultaneously with hadb Phase Beacon and hakuzu Phase Parity.** Phase Beacon changes the `FollowerBehavior` trait signature. haqlite's `SqliteFollowerBehavior` implements this trait and must update in the same coordinated change.

After hadb Phase Beacon adds readiness to the coordinator, haqlite gets `is_caught_up()` and `replay_position()` for free.

- Cache `caught_up: Arc<AtomicBool>` and `position: Arc<AtomicU64>` from `Coordinator::join()` `JoinResult` in `HaQLiteInner` (same pattern as hakuzu Phase Parity)
- Add `is_caught_up(&self) -> bool` to `HaQLite` (single atomic load on cached Arc)
- Add `replay_position(&self) -> u64` to `HaQLite` (single atomic load on cached Arc)
- Add readiness to `prometheus_metrics()` output (same format as hakuzu)
- Update `SqliteFollowerBehavior::run_follower_loop()` to accept the new `caught_up: Arc<AtomicBool>` parameter and add state transitions. Currently `SqliteFollowerBehavior` has NO readiness tracking at all (`src/follower_behavior.rs:31-80`). Add `caught_up.store(true/false)` following hakuzu's state machine:
  - Empty poll (no new LTX files) = `caught_up.store(true)`
  - New data downloaded = `caught_up.store(false)`
  - Successful replay = `caught_up.store(true)`

Source: `hakuzu/src/database.rs:682-717` (is_caught_up, replay_position, prometheus readiness), `hakuzu/src/follower_behavior.rs:109-161` (caught_up state machine to replicate)

Tests (~3):
- Leader is always caught up
- Follower caught_up transitions (false after download, true after replay)
- Readiness appears in prometheus output

### Files

| File | Changes |
|------|---------|
| `haqlite/src/error.rs` | NEW: structured error types (from hakuzu/src/error.rs) |
| `haqlite/src/database.rs` | forwarding retry, read semaphore, write mutex, graceful close, readiness, JoinResult destructuring |
| `haqlite/src/lib.rs` | pub use error types |
| `haqlite/src/follower_behavior.rs` | caught_up state transitions (from Phase Beacon trait) |

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

