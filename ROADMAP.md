# haqlite Roadmap

## Read Replicas (Scale-Out Reads)

Followers currently forward writes to the leader but reads are local. Add explicit read-replica connections that don't join the forwarding path — as long as the app uses read-after-write patterns (write to leader, then read locally), reads can scale to arbitrary nodes.

- `HaQLiteClient` option for read-only mode (no write forwarding, only queries)
- Read-replica connection that queries the local follower DB directly
- Document read-after-write consistency guarantees

### How to build this

The current `HaQLiteClient` (in `src/client.rs`) connects to a remote HaQLite node over HTTP. Both `execute()` and `query_row()` go through the forwarding server (`/haqlite/execute` and `/haqlite/query` endpoints in `src/forwarding.rs`). For read replicas, the client needs a mode where reads go to the local follower's SQLite DB file instead of over HTTP.

**What to change in `src/client.rs`:**
- Add a `.read_replica(db_path)` option to `HaQLiteClientBuilder` that stores a local DB path
- When in read-replica mode, `query_row()` opens a fresh read-only SQLite connection to that local path and queries directly — no HTTP round-trip
- `execute()` still forwards to the leader over HTTP (unchanged)

**Critical constraint — no connection pooling for reads:**
Walrust applies LTX files (WAL deltas) by modifying the DB file directly from outside SQLite. Persistent read-only connections hold stale snapshots and won't see these external changes. The test `regression_fresh_connection_sees_file_changes` in `tests/ha_database.rs` exists specifically for this. A read pool was attempted and reverted (see CHANGELOG). Followers MUST open a fresh `rusqlite::Connection` per read.

**Consistency tradeoff to document:**
If a client writes via `execute()` (forwarded to leader) and immediately calls `query_row()` (local read), the local follower may not have pulled that write yet. The walrust pull interval (configured in `CoordinatorConfig`) determines the replication lag. Users who need strict read-after-write should either query the leader directly (standard `HaQLiteClient` without read-replica mode) or accept a short delay. This is the fundamental tradeoff of read replicas — you get horizontal read scale at the cost of eventual consistency.

**Testing:**
- Unit test: read-replica client reads from local DB file
- Integration test: two nodes, write through client to leader, wait for pull interval, read-replica client on follower sees the write
- Negative test: read-replica `execute()` still goes to leader (doesn't try to write locally)

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

## Future: Extract Generic Lease Monitor

The `FollowerBehavior::run_lease_monitor` is ~90% generic (poll lease, count consecutive expired reads, claim, emit events) and ~10% database-specific (warm catch-up on promotion). Consider splitting into a generic `run_lease_monitor` in hadb (like `run_leader_renewal` already is) with a small `OnPromotionCatchup` hook trait for the DB-specific part.
