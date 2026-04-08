# haqlite Roadmap

## Phase Thermopylae: HA Hardening (turbolite integration)
> After: (current work) · Before: SQLite Extensions Support

Close the remaining safety gaps in haqlite's 5 mode combinations (Shared x Synchronous/Eventual, Dedicated x Replicated/Synchronous/Eventual). All steps require real Tigris S3 (hadb-test-bucket, single-region iad). Run via `soup run --project hadb --env development`.

### 0. Code cleanup (post-debugging cruft)

Phases Cannae through Thermopylae involved a lot of churn: multiple "fixes" tried before root causes were found, debug logging added and never removed, defensive code that's no longer needed. Clean it all up before adding more features.

**database.rs:**
- [ ] Remove debug tracing in ensure_turbolite_conn (lines 672, 675, 697: "fast path", "slow path", journal_mode)
- [ ] Remove or demote VFS version delta logging in execute_shared (lines 1236-1239)
- [ ] Remove redundant ensure_turbolite_conn call in execute_shared (line 1221, result thrown away; line 1224 is sufficient)
- [ ] Consolidate duplicate hardlink blocks in open_shared_turbolite (lines 1813-1819 vs 1825-1835)
- [ ] Remove deprecated query_row and query_values wrappers (lines 914-926, 1013-1017), no external callers
- [ ] Rewrite comment at lines 1796-1800 to explain design rationale, not bug-fix context

**follower_behavior.rs:**
- [ ] Remove redundant caught_up stores (lines 106-107, 156-157: set false then immediately true with no work between)
- [ ] Downgrade cancellation logs to debug! (lines 86, 127, 172: normal shutdown, not errors)
- [ ] Fix "Data may be lost" warnings in catchup_on_promotion (lines 205-214): either propagate the error or remove the alarming text

### a. Dedicated+Synchronous follower reads
Follower is currently warm standby only (no reads). Wire follower reads through turbolite VFS so followers can serve stale-ok reads like the other Dedicated modes.
- [ ] Follower opens turbolite VFS connection on join (not plain SQLite)
- [ ] Follower pull loop updates VFS via fetch_and_apply_s3_manifest (already done)
- [ ] query_row_local / query_values_local read through turbolite conn on follower
- [ ] Test: leader writes 20 rows, follower reads them within 5s
- [ ] Test: leader writes continuously, follower eventually sees all rows
- [ ] Remove "follower read check skipped" workaround from e2e_modes.py

### b. Test coverage for missing modes

Current coverage is heavily skewed toward Mode 4 (Shared+Synchronous). The modes people will actually use most (Dedicated+Replicated, Dedicated+Eventual) have the worst coverage. Prioritized by expected usage.

**Mode 3 (Dedicated+Eventual): zero tests exist, most popular mode**
- [ ] Happy path: leader writes, follower catches up via turbolite + walrust
- [ ] Crash recovery: kill leader mid-write, verify walrust WAL frames are replayed correctly on follower
- [ ] Durability: kill all nodes, restart fresh node, verify data from S3
- [ ] Follower reads: follower serves stale-ok reads during leader writes
- [ ] Sustained writes: leader writes for 60s, follower stays caught up

**Mode 1 (Dedicated+Replicated): existing tests use old Coordinator API**
- [ ] Rewrite ha_database.rs tests to use HaQLite::builder() instead of raw Coordinator API
- [ ] Add crash recovery test: kill leader, follower promotes, data intact
- [ ] Add concurrent read test: follower serves stale reads during leader writes

**Mode 5 (Shared+Eventual): only 2 test functions exist**
- [ ] Crash recovery: node dies mid-write, other node takes over
- [ ] Split brain prevention: slow storage causes lease expiration
- [ ] Concurrent writes: 4-node stress test with short lease TTL
- [ ] Durability across restarts: kill all, fresh node recovers from S3 + walrust

**Mode 2 (Dedicated+Synchronous): zero tests exist**
- [ ] Happy path: leader writes, follower catches up via turbolite manifest
- [ ] Crash recovery: kill leader, follower promotes, reads all data from S3
- [ ] Sequential writes after failover: new leader writes, old follower restarts as follower

### c. Fix broken and misleading tests

- [ ] Un-ignore shared_mode_failure_then_recovery_one_writer in shared_mode.rs (fix or delete)
- [ ] Fix SlowStorage in lease_split_brain.rs: delay is set to Duration::ZERO, defeating the test's purpose
- [ ] Rename mode_matrix.rs (only tests Mode 4 encoding combos, not a mode matrix) or expand it to actually test all 5 modes
- [ ] Fix weak assertions in lease tests: "at least one succeeded" should be "exactly N succeeded" where deterministic

### d. 3-node double failover
Prove the system survives cascading leader failures.
- [ ] Start 3 nodes (1 leader, 2 followers)
- [ ] Write data to leader
- [ ] Kill leader (SIGKILL), wait for follower-1 to promote
- [ ] Write more data to new leader
- [ ] Kill new leader (SIGKILL), wait for follower-2 to promote
- [ ] Verify follower-2 has ALL data from both previous leaders
- [ ] Write more data to final leader
- [ ] Test for Dedicated+Replicated, Dedicated+Synchronous, Dedicated+Eventual

### e. Network partition simulation
Test leader self-demotion when lease renewal fails.
- [ ] Start 2 nodes, leader + follower
- [ ] Write data to leader
- [ ] Block leader's S3 access (kill its lease renewal somehow, or use a proxy)
- [ ] Leader should self-demote after 3 consecutive renewal failures (~6s)
- [ ] Follower should detect expired lease and promote
- [ ] Verify no split-brain: old leader rejects writes after self-demotion
- [ ] Test for Dedicated+Replicated (simplest to reason about)

### f. Long soak test
Sustained writes for 5+ minutes, checking for resource leaks, error accumulation, and performance degradation.
- [ ] Shared+Synchronous: 2 nodes, alternating writes for 5 minutes, verify 0 data loss
- [ ] Shared+Eventual: same parameters
- [ ] Dedicated+Replicated: leader writes for 5 minutes, verify follower caught up
- [ ] Monitor: memory usage stable, no task leaks, error rate stays low
- [ ] Final verification: all Ok writes readable from all nodes

### g. Concurrent shared writes with NATS lease store
True concurrent multi-node writes require atomic CAS (S3 doesn't provide it). Wire up hadb-lease-nats and prove concurrent writes are safe.
- [ ] Start local NATS server (or use Fly NATS)
- [ ] Configure haqlite experiment binary to use --lease-store nats
- [ ] Run Shared+Synchronous with 8 concurrent writers across 2 nodes, 50 writes
- [ ] Verify 0 data loss (all Ok writes visible)
- [ ] Run Shared+Eventual with same parameters
- [ ] Compare with S3 lease store results (document S3 race rate vs NATS 0%)

### h. Chaos and resilience tests

Current tests prove the happy path and clean failovers work. These tests prove resilience to messy, real-world failures: mid-operation kills, corruption, slow storage, and adversarial timing.

**SIGKILL during sync (most likely real-world failure)**
- [ ] Write continuously to Ded+Eventual leader, SIGKILL after random 0-100ms delay
- [ ] Restart cluster, verify no corruption (integrity_check passes)
- [ ] Verify all Ok'd writes from before the last successful sync are present
- [ ] Run 10 iterations, track how many recent writes are lost per kill
- [ ] Test for both Ded+Eventual and Ded+Replicated

**Minimum durability window (RPO measurement)**
- [ ] Write a row, SIGKILL leader after N ms, check if row survived on fresh node
- [ ] Sweep N from 0ms to 2000ms in 100ms steps
- [ ] Record the threshold: below N ms, writes are lost; above N ms, writes survive
- [ ] Document this as the RPO for each durability mode
- [ ] Compare Synchronous (should be 0ms RPO) vs Eventual (expected: ~sync_interval)

**Changeset chain integrity after promotion**
- [ ] After double failover, do a full walrust restore from scratch (not incremental pull)
- [ ] Verify restore succeeds (no gaps, no duplicate seqs, valid checksums)
- [ ] Verify restored data matches the surviving leader's local data exactly
- [ ] This tests that add_continuing produces a valid chain, not just that local data survives

**Slow S3 / lease expiry during sync**
- [ ] Wrap StorageBackend with configurable latency injection
- [ ] Set S3 upload latency to 3s, lease TTL to 5s
- [ ] Write to leader, verify leader self-demotes when sync exceeds lease window
- [ ] Verify no split-brain: demoted leader rejects writes immediately
- [ ] Verify follower promotes and has all previously-synced data

**Concurrent readers during failover**
- [ ] Spawn 4 reader threads on a follower doing SELECT COUNT(*) in a loop
- [ ] SIGKILL the leader while readers are active
- [ ] Verify: readers get either valid data or clean errors, never corruption or panic
- [ ] Verify: readers resume normal operation after new leader is elected
- [ ] Test with both plain SQLite reads (Ded+Eventual) and turbolite VFS reads (Ded+Synchronous)

**Rapid kill/restart cycle**
- [ ] Kill leader, wait minimum time for promotion (~6s), write 1 row, kill immediately
- [ ] Repeat 5 times (5 leaders, 5 kills, ~30s total)
- [ ] Verify: final surviving node has all rows that got Ok responses
- [ ] Verify: changeset chain is valid (full restore works)
- [ ] This stress-tests the add_continuing path with minimal settle time

**Corrupted S3 objects**
- [ ] Upload garbage bytes to a changeset key, then restore. Should fail cleanly, not panic
- [ ] Upload truncated changeset (first 50% of bytes). Should fail cleanly
- [ ] Delete state.json, then start fresh node. Should fall back to snapshot discovery
- [ ] Upload changeset with wrong seq (e.g., seq 999 when chain expects seq 3). Should skip or error, not corrupt DB

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
