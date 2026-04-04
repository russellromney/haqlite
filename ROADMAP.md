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

## Phase Crest: HaMode::Shared (Lease-on-Write)

> After: Phase Volt-c . Before: SQLite Extensions Support
> Depends on: hadb Phase Signal (ManifestStore trait)

Second coordination mode for haqlite. Today's mode becomes `HaMode::Dedicated` (persistent leader, continuous sync, write forwarding). The new mode is `HaMode::Shared` (ephemeral leader, lease per write, no forwarding). Same `HaQLite` struct, same builder, same ManifestStore and LeaseStore, different coordination strategy.

### Why

HA for ephemeral compute. Agents on Lambda, Fly Machines (scale to zero), Cloud Run, or short-lived containers need to occasionally write and read each other's work, but can't communicate directly. There's no stable leader to forward writes to, no long-lived process for a sync loop, no way for instances to discover each other.

Dedicated mode assumes persistent processes: one node holds the lease, runs a continuous sync loop, accepts forwarded writes via HTTP. That breaks on ephemeral compute where instances spin up for one task and disappear.

Shared mode: every node is self-sufficient. S3 and ManifestStore are the only shared state. Nodes never need to find or talk to each other. Wake up, acquire lease, catch up from manifest, write, publish, release, go away.

### Mode enum

```rust
pub enum HaMode {
    /// Persistent leader. Continuous sync. Write forwarding.
    /// One node holds the lease and renews it. Followers forward
    /// writes to the leader via HTTP.
    Dedicated,
    /// Ephemeral leader. Lease per write. No forwarding.
    /// Any node can write by acquiring the lease. Leadership
    /// lasts one write cycle (~100-300ms). Between writes,
    /// nobody is leader.
    Shared,
}
```

### Builder API

```rust
let db = HaQLite::builder("my-bucket")
    .mode(HaMode::Shared)
    .manifest_store(nats_manifest)   // optional, default S3
    .lease_store(nats_lease)         // optional, default S3
    .manifest_poll_interval(Duration::from_secs(1))  // follower freshness
    .lease_ttl(Duration::from_secs(30))
    .write_timeout(Duration::from_secs(10))
    .open("/data/my.db", "CREATE TABLE IF NOT EXISTS ...")
    .await?;

// Writes: lease-guarded, catch up + checkpoint automatically
db.execute("INSERT ...", &[...]).await?;

// Reads: always local, no lease needed
let count: i64 = db.query_row("SELECT COUNT(*)", &[], |r| r.get(0))?;

// Fresh reads: manifest check before query
let count: i64 = db.query_row_fresh("SELECT COUNT(*)", &[], |r| r.get(0)).await?;
```

### Write flow (Shared mode, per execute() call)

1. Acquire local write mutex (serialize writes on this node)
2. Acquire lease via `LeaseStore::write_if_not_exists` / `write_if_match` (CAS)
   - If lease held by another node and not expired: retry with backoff up to `write_timeout`
   - If lease held but expired: take over via `write_if_match` with stale etag
3. Catch up from manifest: `manifest_store.get(key)`, compare to local state
   - Walrust backend: pull changesets from S3 between local txid and manifest txid, apply LTX
   - Turbolite backend: fetch latest manifest, turbolite VFS lazy-fetches stale pages on demand
4. Execute write on local SQLite
5. Checkpoint + publish:
   - Walrust: walrust sync uploads changeset to S3, then publish manifest with new txid
   - Turbolite: flush_to_s3 uploads page groups, then publish manifest with new page group keys
6. Release lease via `LeaseStore::delete` (only after manifest published)

### Read flow (both modes)

- Open fresh read-only SQLite connection, query locally, close
- No lease, no S3 calls. Reads are always local.
- Staleness bounded by manifest_poll_interval (background polling) or by writes (which trigger catch-up)
- `query_row_fresh()`: manifest meta check before query. If version changed, catch up first. ~2ms overhead on NATS, ~30ms on S3.

### Manifest integration

Both modes publish to ManifestStore (hadb Phase Signal). The manifest carries leadership info + storage state:

**Dedicated mode leader:** publishes manifest after each walrust/turbolite sync cycle. Followers poll `manifest_store.meta()` on interval, catch up when version changes. Leader discovery for write forwarding stays in the lease store or service discovery, not the manifest.

**Shared mode writer:** publishes manifest after each write's checkpoint. No leader address, no forwarding, no discovery. Other nodes poll `manifest_store.meta()` on interval, catch up when version changes. Nodes never need to know about each other.

The manifest polling loop is identical in both modes. The difference is who publishes and how often.

### Latency budget (Shared mode, typical)

- Lease acquire: ~2-5ms (NATS) / ~30-50ms (S3)
- Catch-up manifest check: ~2-5ms (NATS) / ~30-50ms (S3)
- Catch-up data pull (if stale): ~20-100ms (S3, depends on changeset/page group size)
- SQLite write: ~1-50ms
- Checkpoint + manifest publish: ~20-80ms (S3 upload + manifest PUT)
- Lease release: ~2-5ms (NATS) / ~20-50ms (S3)
- **Total: ~50-150ms (NATS lease+manifest) / ~120-350ms (S3 everything)**

### Storage backend agnostic

Shared mode works with both storage backends:

| Concern | Walrust | Turbolite |
|---|---|---|
| Catch-up | Pull changesets from S3, apply LTX | Fetch manifest, VFS lazy-fetches pages |
| Checkpoint | walrust sync uploads changeset | flush_to_s3 uploads page groups |
| Manifest storage variant | `StorageManifest::Walrust { txid, .. }` | `StorageManifest::Turbolite { page_group_keys, .. }` |
| Manifest double duty | No (just coordination) | Yes (coordination + page group index) |
| Write amplification | Full changeset upload | ~256KB per dirty frame (turbolite Phase Drift: Subframe Overrides) |
| Endgame | N/A | S3-primary, no checkpoint (turbolite Phase Zenith: S3-Primary Mode) |

### Error handling

- `HaQLiteError::LeaseContention(String)`: write_timeout exceeded, couldn't acquire lease
- `HaQLiteError::CheckpointFailed(String)`: S3 upload or manifest publish failed after local write
  - Retry while lease TTL allows. If retries exhaust: local write is lost, return error.
  - Walrust: changeset upload failed, walrust can retry on next write via staging
  - Turbolite: staging log persists failed checkpoint, retry on next lease acquisition on same node

### Design decisions

- **No batching.** Per-write lease cycle. If writes are rare enough for Shared mode, batching adds complexity for no gain.
- **No speculative lease hold.** Release immediately after checkpoint. Re-acquire cost is cheap.
- **No forwarding server.** Every node writes directly. No inter-node HTTP in Shared mode.
- **No role concept.** No leader/follower state machine in Shared mode. Just "writing" or "not writing."
- **Fresh connections per read.** Avoids stale snapshots after external updates.
- **Fencing via manifest CAS.** `manifest_store.put()` with `expected_version` prevents stale writers from publishing, even if lease logic has a bug.
- **manifest_poll_interval for background freshness.** Nodes poll the manifest periodically even without writes, keeping local state reasonably fresh for reads.

### Implementation phases

**Crest-a: HaMode enum + builder integration**
- Add `HaMode` enum to haqlite
- Add `.mode()` to `HaQLiteBuilder`
- `HaMode::Dedicated` is the default (backward compat)
- Add `.manifest_store(Arc<dyn ManifestStore>)` to builder
- Add `.manifest_poll_interval(Duration)` to builder
- Dedicated mode publishes manifest after each sync (new behavior, additive)
- Follower polling switches from S3 file scan to manifest meta check (when ManifestStore is provided)

**Crest-b: Shared mode coordination**
- Shared mode write path: mutex -> lease -> catch-up -> write -> checkpoint -> manifest publish -> lease release
- Shared mode read path: local query (unchanged)
- `query_row_fresh()` / `query_values_fresh()`: manifest meta check before read, catch up if stale
- Background manifest polling task (configurable interval)
- No forwarding server started in Shared mode
- No lease renewal loop in Shared mode

**Crest-c: Tests**
- Shared mode single node: write + read + verify manifest published
- Shared mode two nodes: node A writes, node B reads stale, node B polls manifest, node B reads fresh
- Shared mode write contention: two nodes write simultaneously, one gets LeaseContention
- Shared mode failover: node A writes, crashes (lease expires), node B acquires lease and writes
- Shared mode + walrust: changeset uploaded, manifest has correct txid
- Shared mode + turbolite: page groups uploaded, manifest has correct page_group_keys
- Dedicated mode backward compat: existing tests still pass with HaMode::Dedicated
- Manifest publish: CAS rejects stale version
- Fresh read: query_row_fresh triggers catch-up when manifest version is ahead
- Lease TTL expiry: stale lease taken over correctly

**Crest-d: CLI integration**
- `haqlite serve --mode shared` / `haqlite serve --mode dedicated` (default)
- `HAQLITE_MODE=shared` env var
- Shared mode skips forwarding server setup
- Shared mode skips lease renewal loop

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

