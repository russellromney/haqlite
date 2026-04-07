# haqlite Roadmap

## Phase Thermopylae: HA Hardening (turbolite integration)
> After: (current work) · Before: SQLite Extensions Support

Close the remaining safety gaps in haqlite's 5 mode combinations (Shared x Synchronous/Eventual, Dedicated x Replicated/Synchronous/Eventual). All steps require real Tigris S3 (hadb-test-bucket, single-region iad). Run via `soup run --project hadb --env development`.

### a. Dedicated+Synchronous follower reads
Follower is currently warm standby only (no reads). Wire follower reads through turbolite VFS so followers can serve stale-ok reads like the other Dedicated modes.
- [ ] Follower opens turbolite VFS connection on join (not plain SQLite)
- [ ] Follower pull loop updates VFS via fetch_and_apply_s3_manifest (already done)
- [ ] query_row_local / query_values_local read through turbolite conn on follower
- [ ] Test: leader writes 20 rows, follower reads them within 5s
- [ ] Test: leader writes continuously, follower eventually sees all rows
- [ ] Remove "follower read check skipped" workaround from e2e_modes.py

### b. 3-node double failover
Prove the system survives cascading leader failures.
- [ ] Start 3 nodes (1 leader, 2 followers)
- [ ] Write data to leader
- [ ] Kill leader (SIGKILL), wait for follower-1 to promote
- [ ] Write more data to new leader
- [ ] Kill new leader (SIGKILL), wait for follower-2 to promote
- [ ] Verify follower-2 has ALL data from both previous leaders
- [ ] Write more data to final leader
- [ ] Test for Dedicated+Replicated, Dedicated+Synchronous, Dedicated+Eventual

### c. Network partition simulation
Test leader self-demotion when lease renewal fails.
- [ ] Start 2 nodes, leader + follower
- [ ] Write data to leader
- [ ] Block leader's S3 access (kill its lease renewal somehow, or use a proxy)
- [ ] Leader should self-demote after 3 consecutive renewal failures (~6s)
- [ ] Follower should detect expired lease and promote
- [ ] Verify no split-brain: old leader rejects writes after self-demotion
- [ ] Test for Dedicated+Replicated (simplest to reason about)

### d. Long soak test
Sustained writes for 5+ minutes, checking for resource leaks, error accumulation, and performance degradation.
- [ ] Shared+Synchronous: 2 nodes, alternating writes for 5 minutes, verify 0 data loss
- [ ] Dedicated+Replicated: leader writes for 5 minutes, verify follower caught up
- [ ] Monitor: memory usage stable, no goroutine/task leaks, error rate stays low
- [ ] Final verification: all Ok writes readable from all nodes

### e. Concurrent shared writes with NATS lease store
True concurrent multi-node writes require atomic CAS (S3 doesn't provide it). Wire up hadb-lease-nats and prove concurrent writes are safe.
- [ ] Start local NATS server (or use Fly NATS)
- [ ] Configure haqlite experiment binary to use --lease-store nats
- [ ] Run Shared+Synchronous with 8 concurrent writers across 2 nodes, 50 writes
- [ ] Verify 0 data loss (all Ok writes visible)
- [ ] Run Shared+Eventual with same parameters
- [ ] Compare with S3 lease store results (document S3 race rate vs NATS 0%)

## SQLite Extensions Support

haqlite should support SQLite extensions (sqlite-vec, FTS5, sqlean, etc.) across the cluster. Extensions must be loaded on ALL connections, including leader rw, follower reads, and walrust LTX apply. Otherwise WAL replay fails or silently corrupts data.

**Dual-mode design:**

- **Embedded mode**: `.connection_init(|conn| { conn.load_extension(...)?; Ok(()) })` callback on `HaQLiteBuilder`. Stored as `Option<Arc<dyn Fn(&Connection) -> Result<()> + Send + Sync>>`. Called after every `Connection::open`. User controls what gets loaded and how. No S3 coordination needed (same binary = same extensions).

- **Server mode**: `haqlite serve --extension sqlite-vec.so --extension sqlean.so` CLI args. haqlite loads each extension on every connection. Leader writes extension name list to S3 cluster metadata (alongside lease). Followers read the list on startup and verify their local config matches. Mismatch = crash with clear error (e.g., "leader requires sqlite-vec but this node doesn't have it configured").

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
- Publish order: walrust -> hadb -> hadb-s3 -> haqlite

## hakuzu (Kuzu/graph HA)

- `KuzuReplicator: Replicator` (wraps graphstream)
- `GraphFollowerBehavior: FollowerBehavior` (checkpoint tracking, not TXID)
- Extract graphstream crate from graphd
- Integrate into graphd with `--ha` CLI flags
