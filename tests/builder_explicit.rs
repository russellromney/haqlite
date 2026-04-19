//! Phase Lucid: builder no longer silently resolves stores from env vars.
//!
//! These tests pin the new contract:
//! - `lease_store()` must be called for any HA mode.
//! - `manifest_store()` must be called for HaMode::Shared (and Dedicated+Sync,
//!   which is exercised by cinch-cloud's e2e tests since constructing an
//!   in-memory walrust `StorageBackend` here would require pulling in
//!   walrust's private test helpers).
//! - turbolite page storage (`turbolite_storage` / `turbolite_http` /
//!   `turbolite_vfs`) must be configured for HaMode::Shared.
//! - The env-var helpers live in `haqlite::env::*` for callers that want
//!   them by name.
//!
//! Each missing piece produces a loud error from `open()`. No silent
//! S3-from-env fallback, no silent HAQLITE_LEASE_URL/HAQLITE_MANIFEST_URL
//! lookup.
//!
//! Phase Driftwood also pins the lease-timing plumbing contract: the
//! builder's `.lease_ttl()` / `.lease_renew_interval()` /
//! `.lease_follower_poll_interval()` setters reach the Coordinator's
//! `LeaseConfig` instead of being silently dropped.

mod common;

use common::InMemoryStorage;
use haqlite::{HaMode, HaQLite, InMemoryManifestStore};
use hadb::InMemoryLeaseStore;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// `unwrap_err()` won't compile because `HaQLite` doesn't implement `Debug`.
fn err_msg<T>(r: anyhow::Result<T>) -> String {
    match r {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected open() to fail, got Ok"),
    }
}

/// Strip env vars that the old fallback paths used to consult so a host
/// shell with them set doesn't mask the test.
fn unset_env() {
    std::env::remove_var("HAQLITE_LEASE_URL");
    std::env::remove_var("HAQLITE_MANIFEST_URL");
}

#[tokio::test]
async fn dedicated_without_lease_or_walrust_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let result = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("test-1")
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    // Walrust is checked before lease for Dedicated mode, so the walrust message wins.
    // Either is acceptable proof that the silent fallback is gone.
    assert!(
        err.contains("walrust storage") || err.contains("lease_store()"),
        "expected lease/walrust error, got: {err}"
    );
}

#[tokio::test]
async fn shared_without_lease_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let result = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("test-1")
        .mode(HaMode::Shared)
        .durability(haqlite::Durability::Synchronous)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("lease_store()"),
        "expected lease_store() error, got: {err}"
    );
}

#[tokio::test]
async fn shared_without_manifest_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let lease = Arc::new(InMemoryLeaseStore::new());

    let result = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("test-1")
        .mode(HaMode::Shared)
        .durability(haqlite::Durability::Synchronous)
        .lease_store(lease)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("manifest_store()") && err.contains("Shared"),
        "expected Shared+manifest_store() error, got: {err}"
    );
}

#[tokio::test]
async fn shared_without_turbolite_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(InMemoryManifestStore::new());

    let result = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("test-1")
        .mode(HaMode::Shared)
        .durability(haqlite::Durability::Synchronous)
        .lease_store(lease)
        .manifest_store(manifest)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("turbolite_http")
            && err.contains("turbolite_storage")
            && err.contains("turbolite_vfs"),
        "expected error to list all three turbolite options, got: {err}"
    );
}

#[tokio::test]
async fn lease_timing_setters_reach_coordinator() {
    // Phase Driftwood: regression guard for the silent-overwrite bug.
    // Pre-Driftwood, HaQLiteBuilder::open() replaced `config.lease` with
    // a fresh default LeaseConfig, so builder setters like `.lease_ttl(30)`
    // were silently dropped before reaching the Coordinator.
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let lease = Arc::new(InMemoryLeaseStore::new());
    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());

    let db = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("test-1")
        .address("http://127.0.0.1:19090")
        .forwarding_port(19090)
        .lease_store(lease)
        .walrust_storage(walrust_storage)
        .lease_ttl(30)
        .lease_renew_interval(Duration::from_millis(7_500))
        .lease_follower_poll_interval(Duration::from_millis(2_500))
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await
        .expect("open");

    let coord = db.coordinator().expect("dedicated mode has a coordinator");
    let cfg = coord.lease_config().expect("LeaseConfig must be present on Dedicated mode");
    assert_eq!(cfg.ttl_secs, 30, "lease_ttl should reach the Coordinator");
    assert_eq!(
        cfg.renew_interval,
        Duration::from_millis(7_500),
        "lease_renew_interval should reach the Coordinator"
    );
    assert_eq!(
        cfg.follower_poll_interval,
        Duration::from_millis(2_500),
        "lease_follower_poll_interval should reach the Coordinator"
    );
}

#[tokio::test]
async fn caller_lease_config_timing_is_preserved() {
    // If a caller passes a full LeaseConfig through coordinator_config()
    // with non-default timing, the builder must preserve it rather than
    // rebuild a defaults-only LeaseConfig. Builder setters are optional
    // overrides on top.
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let lease = Arc::new(InMemoryLeaseStore::new());
    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());

    // Placeholder store — the builder overwrites `store`/`instance_id`/`address`
    // with the values from `.lease_store()` / `.instance_id()` / `.address()`.
    let placeholder: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let coordinator_config = haqlite::CoordinatorConfig {
        lease: Some(hadb::LeaseConfig {
            ttl_secs: 17,
            renew_interval: Duration::from_millis(9_000),
            follower_poll_interval: Duration::from_millis(4_000),
            required_expired_reads: 2,
            max_consecutive_renewal_errors: 4,
            ..hadb::LeaseConfig::new(placeholder, String::new(), String::new())
        }),
        ..Default::default()
    };

    let db = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("caller-instance")
        .address("http://127.0.0.1:19091")
        .forwarding_port(19091)
        .lease_store(lease)
        .walrust_storage(walrust_storage)
        .coordinator_config(coordinator_config)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await
        .expect("open");

    let coord = db.coordinator().expect("dedicated mode has a coordinator");
    let cfg = coord.lease_config().expect("LeaseConfig must be present");
    assert_eq!(cfg.ttl_secs, 17);
    assert_eq!(cfg.renew_interval, Duration::from_millis(9_000));
    assert_eq!(cfg.follower_poll_interval, Duration::from_millis(4_000));
    assert_eq!(cfg.required_expired_reads, 2);
    assert_eq!(cfg.max_consecutive_renewal_errors, 4);
    assert_eq!(cfg.instance_id, "caller-instance", "builder patches in instance_id");
    assert_eq!(cfg.address, "http://127.0.0.1:19091", "builder patches in address");
}

#[tokio::test]
async fn env_module_is_public_and_returns_clear_errors() {
    // The env helpers are the only blessed way to opt into env-var resolution.
    unset_env();

    let lease_err = match haqlite::env::lease_store_from_env("b", None).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error when HAQLITE_LEASE_URL is unset"),
    };
    assert!(lease_err.contains("HAQLITE_LEASE_URL not set"));

    let manifest_err = match haqlite::env::manifest_store_from_env("b", None).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error when HAQLITE_MANIFEST_URL is unset"),
    };
    assert!(manifest_err.contains("HAQLITE_MANIFEST_URL not set"));
}
