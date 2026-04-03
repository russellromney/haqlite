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

## HaQLiteLight: Lease-on-Write Mode (turbolite-backed)

Low-write optimized mode. No persistent leader, no forwarding server, no background sync loops. Every node is equal. Writes acquire a short-lived lease, catch up, execute, checkpoint, and release. Reads are always local (accept staleness).

**Core idea:** Instead of always-on leader election with continuous WAL shipping, treat each write as an atomic lease-guarded transaction against S3. Assumes writes are infrequent enough (seconds to minutes between writes) that the per-write overhead of lease acquire + sync + flush is acceptable.

**Write flow (per `execute()` call):**
1. Acquire local write mutex (serialize writes on this node)
2. Acquire S3 lease via `LeaseStore::write_if_not_exists` / `write_if_match` (CAS)
   - If lease held by another node and not expired: retry with backoff up to `write_timeout`
   - If lease held but expired: take over via `write_if_match` with stale etag
3. Catch up from former leaders: pull latest manifest / page groups from S3
4. Execute write on local SQLite
5. Checkpoint: encode dirty pages into new page groups, upload to S3, publish new manifest
6. Release lease via `LeaseStore::delete` (only after checkpoint confirmed on S3)

**Read flow (per `query_row()` / `query_values()` call):**
- Open fresh read-only SQLite connection, query locally, close
- No lease, no S3 calls — reads are always local
- Staleness bounded by how recently this node has written (which triggers catchup) or by optional background manifest polling

**Turbolite as the checkpoint/sync backend:**

Turbolite's architecture is a natural fit for this pattern. Instead of WAL shipping (walrust HADBP changesets → LTX apply), turbolite's manifest + page group model gives us:

- **Manifest = version signal.** A single small S3 object that is the source of truth for the current DB state. Readers check for a new manifest (one HEAD request) instead of polling for new LTX files. If the manifest hasn't changed, nothing has changed.

- **Checkpoint = publish.** After a write, turbolite encodes dirty pages into new page groups (zstd-compressed, optionally AES-256 encrypted), uploads them, and atomically publishes a new manifest. The checkpoint IS the replication step — no separate WAL shipping stage.

- **Catchup = manifest fetch + lazy page pull.** When acquiring the lease, fetch the latest manifest. That's the catch-up. You don't replay a WAL — you just point at the current state. Any pages you actually need get fetched on demand via S3 range GETs. Minimal bandwidth, no wasted work.

- **No background sync loop.** No follower pull interval, no periodic flush. All sync is driven by writes. Reads that want freshness just check the manifest.

This collapses the current walrust flow (WAL frames → HADBP changesets → S3 → LTX pull → apply to DB) into turbolite's flow (dirty pages → page groups → S3 → manifest → lazy page fetch).

**Struct sketch:**

```rust
pub struct HaQLiteLight {
    inner: Arc<HaQLiteLightInner>,
}

struct HaQLiteLightInner {
    db_path: PathBuf,
    db_name: String,
    lease_store: Arc<dyn LeaseStore>,
    // turbolite connection (replaces walrust_storage + replicator)
    // turbolite handles: manifest tracking, page group upload/download,
    // checkpoint, and lazy page fetching via its VFS
    prefix: String,
    instance_id: String,
    lease_key: String,      // "{prefix}{db_name}/lease"
    lease_ttl: Duration,    // safety net for crash recovery
    write_timeout: Duration,
    write_lock: tokio::sync::Mutex<()>,
    closed: AtomicBool,
}
```

**Builder API:**
```rust
let db = HaQLiteLight::builder("my-bucket")
    .prefix("myapp/")
    .endpoint("https://fly.storage.tigris.dev")
    .lease_ttl(Duration::from_secs(30))
    .write_timeout(Duration::from_secs(10))
    .lease_store(custom_store)  // optional, default S3
    .open("/data/my.db", "CREATE TABLE IF NOT EXISTS ...")
    .await?;

// Writes: lease-guarded, catch up + checkpoint automatically
db.execute("INSERT ...", &[...]).await?;

// Reads: always local, no lease needed
let count: i64 = db.query_row("SELECT COUNT(*)", &[], |r| r.get(0))?;
```

**Error handling:**
- New `HaQLiteError::LeaseContention(String)` variant for when write_timeout is exceeded
- `HaQLiteError::CheckpointFailed(String)` for when S3 upload fails after local write
  - On checkpoint failure: retry flush while lease is still valid (TTL is the retry budget)
  - If retries exhaust and lease expires: the local write is lost. Return error to caller.
  - This is the same risk as current leader crash between write and sync — acceptable.

**Latency budget (typical):**
- Lease acquire: ~20-50ms (S3 conditional PUT)
- Catchup (manifest check, no new data): ~20-50ms (S3 HEAD/GET)
- Catchup (new data from another writer): ~20-100ms (manifest + page group GETs)
- SQLite write: ~10-50ms
- Checkpoint + manifest publish: ~20-50ms (page group upload + manifest PUT)
- Lease release: ~20-50ms
- **Total: ~100-300ms per write** (acceptable for low-write workloads)

**Design decisions:**
- **No batching.** Per-write lease cycle. If writes are rare enough to use this mode, batching saves nothing and adds complexity.
- **No lease hold timeout.** Release immediately after checkpoint. Re-acquire cost is cheap enough that speculative holding isn't worth the state.
- **No forwarding server.** Every node is self-sufficient. No inter-node HTTP.
- **No role concept.** No leader/follower distinction. Every node can read and write.
- **Fresh connections per read.** Like current follower mode — avoids stale WAL snapshots after turbolite applies external page updates.
- **Fencing via manifest.** Embed lease epoch/token in the manifest. Readers can verify they're reading from a legitimate writer's output. Prevents stale/split-brain writes from being visible.

**Open questions:**
- Should reads optionally check the manifest for freshness before querying? Could offer `query_row_fresh()` that does a manifest HEAD first.
- Can we embed a monotonic epoch in the lease data AND the manifest, so that a turbolite VFS read can detect if its cached manifest is from the current lease holder vs a stale one?
- Walrust fallback: should HaQLiteLight also support walrust as a backend (for users who don't use turbolite), or is this turbolite-only?
- Mixed clusters: can a regular HaQLite leader coexist with HaQLiteLight nodes using the same S3 bucket? Probably not without careful manifest/WAL format alignment.

## crates.io Publish

- Verify public API surface is clean (no accidental pub internals)
- `cargo publish --dry-run`
- Publish order: walrust → hadb → hadb-s3 → haqlite

## hakuzu (Kuzu/graph HA)

- `KuzuReplicator: Replicator` (wraps graphstream)
- `GraphFollowerBehavior: FollowerBehavior` (checkpoint tracking, not TXID)
- Extract graphstream crate from graphd
- Integrate into graphd with `--ha` CLI flags

