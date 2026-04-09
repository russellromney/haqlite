# haqlite Roadmap

## Phase Thermopylae: HA Hardening

Close the remaining safety gaps in haqlite's 4 mode combinations. All tests require real Tigris S3 (hadb-test-bucket). Run via `soup run --project hadb --env development`.

Valid modes: Shared+Synchronous, Dedicated+Replicated, Dedicated+Synchronous, Dedicated+Eventual.
Shared+Eventual is invalid (multiwriter + eventual = writers on stale data).

### a. Dedicated+Synchronous durability test

Only mode missing a kill-all-and-recover test. Easy to add.

- [ ] Add durability test to `run_dedicated_synchronous` in e2e_modes.py
- [ ] Write 10 rows, kill all 3 nodes, start fresh node, verify all rows recovered from turbolite S3 manifest
- [ ] Verify fresh node opens through turbolite VFS (not plain SQLite)

### b. Shared linearizability test

The fundamental correctness property of shared mode: concurrent writers must not silently lose updates. Current tests only verify "a value survives", not "the correct value wins".

- [ ] Test: node A reads row X (v1), node B reads row X (v1), node A writes v2, node B writes v3. Verify final value is v3 (last writer wins), not v2
- [ ] Test: 2 nodes do read-modify-write on a counter (SELECT, increment, UPDATE). Run 20 iterations. Final counter value must equal total increments (no lost updates)
- [ ] Test: concurrent INSERT OR REPLACE from 2 nodes with unique IDs. Verify all IDs present (no silent drops)
- [ ] Test: concurrent DELETE + INSERT of same row. Verify consistent final state across both nodes

### c. RPO measurement per mode

How much data can you lose? Depends on sync interval and RTT.

- [ ] Write a row, SIGKILL leader after N ms, check if row survived on fresh node
- [ ] Sweep N from 0ms to 2000ms in 100ms steps
- [ ] Shared+Synchronous: expected RPO = 0 (every write goes to S3 before ack)
- [ ] Dedicated+Synchronous: expected RPO = 0 (S3Primary xSync on every commit)
- [ ] Dedicated+Replicated: expected RPO = sync_interval (walrust async)
- [ ] Dedicated+Eventual: expected RPO = sync_interval (walrust async)
- [ ] Document measured RPO for each mode

### d. Chaos tests for all modes

Current chaos suite only covers Dedicated+Eventual and Dedicated+Replicated. Extend to all 4 modes.

**SIGKILL during sync**
- [ ] Write continuously, SIGKILL after random 0-100ms delay
- [ ] Restart cluster, verify integrity_check passes (no corruption)
- [ ] Verify all Ok'd writes from before last successful sync are present
- [ ] Run 10 iterations, track write loss per kill
- [ ] Test for: all 4 modes

**Concurrent readers during failover**
- [ ] Spawn 4 reader threads doing SELECT COUNT(*) in a loop
- [ ] SIGKILL the leader while readers are active
- [ ] Verify: readers get valid data or clean errors, never corruption
- [ ] Verify: readers resume after new leader elected
- [ ] Test for: Dedicated modes (all 3 durabilities). Shared mode has no followers.

**Rapid kill/restart cycle**
- [ ] Kill leader, wait minimum time for promotion, write 1 row, kill immediately
- [ ] Repeat 5 times
- [ ] Verify: final node has all rows that got Ok responses
- [ ] Verify: changeset chain valid (full restore works)
- [ ] Test for: all 3 Dedicated modes

### e. Shared mode crash chaos

Shared mode has crash recovery but no chaos-level tests.

- [ ] SIGKILL node mid-write (lease held), verify other node takes over after TTL
- [ ] SIGKILL both nodes simultaneously, restart both. Verify no corruption, data consistent
- [ ] Rapid alternating writes: node A writes, node B writes, repeat 50 times. Verify all writes present
- [ ] Stale lease: node A holds lease, node A freezes (SIGSTOP), lease expires, node B writes. Unfreeze node A. Verify node A does NOT overwrite node B's data

### f. Network partition / split brain

- [ ] Block leader's S3 access (or inject latency > lease TTL)
- [ ] Leader should self-demote after consecutive renewal failures
- [ ] Follower detects expired lease and promotes
- [ ] Verify: old leader rejects writes after self-demotion
- [ ] Test for: Dedicated+Replicated first (simplest), then others

### g. Scale tests

Everything currently runs with 5 writes, 2-3 nodes, 10s. Prove it works under real load.

- [ ] Sustained writes: 5 minutes, verify 0 data loss for Ok'd writes
- [ ] Shared+Synchronous: 2 nodes alternating writes for 5 min
- [ ] Dedicated+Replicated: leader writes for 5 min, follower stays caught up
- [ ] Monitor: memory stable, no task leaks, error rate flat
- [ ] Large dataset: 10K rows, full restore from S3, verify all present

### h. Changeset chain integrity

After double failover, the walrust changeset chain has been produced by 3 different leaders. Verify it's valid end-to-end.

- [ ] After double failover, do a full walrust restore from scratch (not incremental)
- [ ] Verify restore succeeds (no gaps, no duplicate seqs, valid checksums)
- [ ] Verify restored data matches surviving leader's local data exactly
- [ ] Test for: Dedicated+Replicated and Dedicated+Eventual

### i. Code cleanup

Phases Cannae through Thermopylae left debug logging and defensive code. Clean up before adding features.

- [ ] Remove debug tracing in ensure_turbolite_conn
- [ ] Remove or demote VFS version delta logging in execute_shared
- [ ] Remove deprecated query_row and query_values wrappers
- [ ] Remove dead Shared+Eventual code paths that may linger in shared_experiment.rs
- [ ] Clean up follower_behavior.rs: redundant caught_up stores, noisy cancellation logs

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
