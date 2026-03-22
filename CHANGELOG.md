# haqlite Changelog

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
