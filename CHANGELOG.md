# haqlite Changelog

## Phase Crest: HaMode::Shared (Lease-on-Write)

- **HaMode::Shared**: Ephemeral compute coordination. Lease per write, no persistent leader, no forwarding. Builder: `.mode(HaMode::Shared)`, `.manifest_store()`, `.manifest_poll_interval()`, `.write_timeout()`.
- **Shared mode write path**: mutex -> lease acquire -> manifest catch-up -> SQLite write -> checkpoint -> manifest publish (CAS) -> lease release. Works with both walrust and turbolite backends.
- **Turbolite integration**: `TurboliteReplicator`, manifest conversion (`turbolite_to_ha_storage`/`ha_storage_to_turbolite`), `.turbolite_vfs()` builder method. 7 turbolite_shared tests.
- **Manifest store features**: `nats-manifest` and `s3-manifest` Cargo features. `serve.rs` auto-configures from `WAL_MANIFEST_NATS_URL` env var (NATS) or falls back to S3. Dedicated mode now passes manifest_store to Coordinator for follower manifest polling.
- **CLI**: `haqlite serve --mode shared` / `HAQLITE_MODE=shared` env var. Shared mode skips forwarding server and lease renewal loop.
- Comprehensive tests: shared_mode.rs (single node, two node contention, failover, fresh reads) + turbolite_shared.rs (write/read, manifest publishing, conversion roundtrip).

## Phase Meridian: Wire Up CLI Commands

- **Prefix threading**: All 7 ops functions (`discover_ltx_files`, `discover_databases`, `list_databases`, `verify_database`, `plan_compact`, `snapshot_database`, `replicate_database`) take `prefix: &str`. `normalize_prefix()` helper ensures trailing slash. CLI defaults to `"haqlite/"` via `resolve_prefix()`.
- **Compact honesty**: Replaced fake GFS flags (`--hourly/--daily/--weekly/--monthly`) with `--keep N` (default 47). No temporal bucketing was ever implemented.
- **Replicate shutdown**: Wrapped poll loop in `tokio::select!` with `hadb_cli::shutdown_signal()` for graceful SIGTERM/SIGINT handling.
- **Output fixes**: `restore` uses `println!` (not `tracing::info!`). `snapshot` prints `SnapshotResult { db_name, txid }`. `explain` has clean formatted output instead of `{:#?}`.
- **walrust Phase Somme fix**: In WAL mode, SQLite does not increment the file change counter per transaction, causing panics in `sync_wal` and `take_snapshot`. Fixed with deterministic WAL commit counting: `sync_wal` falls back to commit count from WAL batch, `take_snapshot` uses `file_change_counter + wal_commit_count`. `read_frames_as_page_map` now returns `commit_count`, new `count_wal_commits()` scans WAL frame headers only.
- 8 new prefix tests, 46 ops tests + 34 HA tests passing.

## Phase Volt-c: NATS Lease Store Engine Integration

- **Pluggable LeaseStore**: `.lease_store(Arc<dyn LeaseStore>)` on `HaQLiteBuilder`. When set, skips S3LeaseStore and S3 client construction entirely. Works with any `LeaseStore` impl (NATS, Redis, etcd, in-memory).
- **NATS feature**: `hadb-lease-nats` as optional dep behind `nats-lease` Cargo feature. `haqlite serve` auto-connects NATS when `WAL_LEASE_NATS_URL` env var is set, falls back to S3 leases on connection failure.
- **Shared test helpers**: Extracted `InMemoryStorage` (walrust `StorageBackend`) into `tests/common/mod.rs`, replacing 4 identical copies across test files.
- **Snapshot test fixes**: SQLite file change counter is 2 after `CREATE TABLE + INSERT` (two transactions), not 1. Fixed 5 assertion values and added writes between sequential snapshots.
- 7 new tests: custom lease store used, builder method compiles, lease renewal, two-node custom store, default fallback, NATS integration (env-gated), NATS connection failure. 164 tests total.

## hadb Core Framework Extraction (Foundation — 83 tests)

- **hadb workspace created**: Database-agnostic HA framework at `~/Documents/Github/hadb/` with core (`hadb/`) and S3 implementation (`hadb-s3/`) crates. Zero cloud dependencies in core.
- **Core traits extracted** (12 tests): `Replicator`, `Executor`, `LeaseStore`, `StorageBackend` — fully abstract, works with any database or storage backend.
- **types.rs extracted** (12 tests): `Role`, `RoleEvent`, `CoordinatorConfig`, `LeaseConfig` — database-agnostic HA types. Added `Role::to_u8/from_u8` for AtomicU8 storage.
- **metrics.rs extracted** (6 tests): `HaMetrics` with 19 atomic counters, Prometheus export, zero-allocation reads via `MetricsSnapshot`.
- **lease.rs extracted** (22 tests): `DbLease` CAS-based leader election with post-claim verification, session ID tracking, TTL management. Bug fixed: session_id wasn't regenerated on new claim after release.
- **node_registry.rs extracted** (13 tests): `NodeRegistry` trait for read replica discovery, `InMemoryNodeRegistry` for testing, validates nodes by session ID and TTL.
- **follower.rs created**: `FollowerBehavior` trait for pluggable follower pull/monitor logic. `run_leader_renewal()` made generic over Replicator.
- **coordinator.rs extracted** (15 tests): Generic `Coordinator<R, E, L, S, F>` with all lifecycle methods (join, leave, handoff), role/address queries, metrics, replica discovery. Bug fixed: follower leader_address tracking.
- **S3 implementations** (3 tests): `S3LeaseStore` (conditional PUTs via ETag), `S3StorageBackend` (upload/download/list/delete with OOM protection via max_keys), `S3NodeRegistry` (S3-backed node discovery). Type alias `S3Coordinator<R, E, F>` for convenience.
- **All 83 tests passing**: 65 hadb core unit tests + 15 coordinator integration tests + 3 hadb-s3 tests. Comprehensive coverage: single-node mode, HA leader/follower, promotion/demotion, handoff, role events, edge cases.
- **Next step**: Refactor haqlite to use hadb + hadb-s3 (see ROADMAP).

## Auth, Handoff, Prometheus, crates.io Prep

- **Shared-secret auth**: `.secret("token")` on `HaQLiteBuilder` and `HaQLiteClientBuilder`. Forwarding server checks `Authorization: Bearer <token>` header, rejects 401 on mismatch. `--secret` / `HAQLITE_SECRET` env on CLI binaries.
- **Graceful leader handoff**: `HaQLite::handoff()` — stop renewal → final WAL sync → release lease → emit Demoted. Node stays alive as follower for drain. `Coordinator::handoff()` underlying implementation.
- **Prometheus metrics**: `MetricsSnapshot::to_prometheus()` — exposition format for all counters and timing gauges. `HaQLite::prometheus_metrics()` convenience method.
- **crates.io metadata**: Added `repository`, `keywords`, `categories` to Cargo.toml.
- **Read pool investigation**: Attempted follower read connection pool — reverted. Followers must open fresh connections per read because walrust applies LTX files externally (pooled connections hold stale snapshots).
- 2 new integration tests: `auth_rejects_wrong_secret`, `auth_accepts_correct_secret`.

## HaQLite One-Liner API

- **HaQLite struct**: `HaQLite::builder("bucket").open(path, schema).await?` — dead-simple embedded HA SQLite. One line to join a cluster, transparent write forwarding, local reads, automatic failover.
- **HaQLiteBuilder**: `.prefix()`, `.endpoint()`, `.instance_id()`, `.address()`, `.forwarding_port()`, `.coordinator_config()`. Auto-detects instance ID (FLY_MACHINE_ID or UUID) and address (Fly internal DNS or hostname).
- **HaQLite::local()**: Single-node mode — same API, no S3, no HA. Useful for dev/testing.
- **HaQLite::from_coordinator()**: Escape hatch for tests and advanced use cases.
- **SqlValue enum**: `Null | Integer | Real | Text | Blob` — serializable across the wire for `execute()` params.
- **Write forwarding**: Leader runs internal HTTP server (`/haqlite/execute`, `/haqlite/query`). Followers forward writes transparently. Clients never need to know who the leader is.
- **HaQLiteClient**: Stateless client that discovers the leader from S3 and forwards reads/writes over HTTP. Auto-retries on connection failure with leader re-discovery.
- **ha_experiment.rs refactored**: ~400 → ~200 lines. All manual state management replaced with HaQLite.
- **ha_writer.rs refactored**: Uses HaQLiteClient instead of manual S3 lease discovery.
- **Configurable CLI args**: `--sync-interval-ms`, `--lease-ttl`, `--renew-interval-ms`, `--follower-poll-ms`, `--follower-pull-ms`.
- **E2e test suite**: 7 scenarios (basic replication, write forwarding, leader failover, fast/slow sync, fast lease failover, writer reconnect). All 15 checks pass.
- 5 new integration tests (`ha_database.rs`): local mode, single-node HA, two-node forwarded write, forwarding error, clean close.

## P0 — Split-Brain & Data Loss Prevention

- **Catch-up failure aborts promotion**: If warm catch-up fails (S3 down), release the lease and loop back instead of promoting with stale data. (`follower.rs`)
- **Replicator.add() failure aborts promotion**: If `replicator.add()` fails after claiming lease, release lease and retry — no silent stale leaders. (`follower.rs`)
- **S3 error detection via proper SDK types**: Replaced `format!("{:?}").contains("PreconditionFailed")` with proper AWS SDK error type matching. (`lease_store.rs`)
- Regression tests: `regression_catchup_failure_aborts_promotion`, `regression_replicator_add_failure_aborts_promotion`, `regression_failed_promotion_releases_lease`

## P1 — Reliability

- **Timeouts on replicator calls**: `replicator.add()` and `replicator.remove()` wrapped with `tokio::time::timeout(replicator_timeout)`. Configurable via `CoordinatorConfig`. (`coordinator.rs`, `follower.rs`)
- **Fix sync_wal_with_retry state persistence**: Added missing `save_state()` call after upload succeeds in retry path. (`walrust-core sync.rs`)
- **Fault-injection StorageBackend**: `ControlledFailStorage` (deterministic) and `FaultyStorage` (probabilistic) for chaos testing. Chaos tests: `chaos_leader_survives_intermittent_write_failures`, `chaos_promotion_under_flaky_reads`

## P2 — Observability & Chaos Testing

- **Structured metrics**: `HaMetrics` with atomic counters for lease claims/renewals, promotions, demotions, follower pulls, timing. `MetricsSnapshot` serializable for JSON export. Accessible via `coordinator.metrics()`. (`metrics.rs`)
- **Metrics bug fix**: `catchup_start` was set after catchup completed (recording ~0us). Fixed. Initial `lease.try_claim()` in `join()` was not recording metrics. Fixed.
- **Chaos ha_experiment**: Added `/metrics`, `/verify` (data integrity — gap/duplicate detection), `/status` endpoints. (`ha_experiment.rs`)
- **Sync interval benchmarking**: ha_writer supports `--total-writes`, `--verify-nodes`, `--bench` flags. Bench mode prints TSV summary with writes/s, promotion_us, catchup_us. (`ha_writer.rs`)
- Regression tests: `metrics_recorded_on_promotion`, `metrics_renewal_counters`
- Flaky test fix: `regression_failed_promotion_releases_lease` TTL race (1s → 3s)
