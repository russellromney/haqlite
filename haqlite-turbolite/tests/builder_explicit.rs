//! Phase Lucid: builder no longer silently resolves stores from env vars.
//!
//! These tests pin the new contract:
//! - `lease_store()` must be called for any HA mode.
//! - `manifest_store()` must be called for SingleWriter+Sync.
//! - SharedWriter is visible in the API but intentionally fails clearly until
//!   the real implementation lands.
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
use hadb::InMemoryLeaseStore;
use haqlite_turbolite::{Builder, HaMode, Role};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use turbodb_manifest_mem::MemManifestStore;
use turbolite::tiered::{SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

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

fn dummy_turbolite_vfs(tmp: &TempDir) -> (SharedTurboliteVfs, String) {
    let cache_dir = tmp.path().join(".tl_cache");
    let config = TurboliteConfig {
        cache_dir,
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("create VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    let name = format!("test_{}", uuid::Uuid::new_v4());
    turbolite::tiered::register_shared(&name, shared_vfs.clone()).expect("register");
    (shared_vfs, name)
}

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_local_paths_make_db_path_primary_artifact() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("app.db");
    let sidecar_dir = tmp.path().join("app.db-state");
    let storage = Arc::new(InMemoryStorage::new());

    let db = Builder::new()
        .prefix("layout/")
        .mode(HaMode::SingleWriter)
        .role(Role::Leader)
        .durability(turbodb::Durability::Cloud)
        .lease_store(Arc::new(InMemoryLeaseStore::new()))
        .manifest_store(Arc::new(MemManifestStore::new()))
        .turbolite_storage(storage)
        .turbolite_local_paths(&sidecar_dir, &db_path)
        .instance_id("layout-node")
        .open(
            db_path.to_str().expect("path"),
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)",
        )
        .await
        .expect("file-first open");

    db.execute("INSERT INTO t (id) VALUES (1)", &[])
        .expect("insert through file-first layout");

    assert!(db_path.exists(), "main artifact should be caller db path");
    assert!(
        sidecar_dir.exists(),
        "sidecar should be derived implementation state"
    );
    assert!(
        sidecar_dir.join("local_state.msgpack").exists(),
        "unified local state should live under sidecar"
    );
    assert!(
        tmp.path().join("app.db-lock").exists(),
        "lock anchor should sit beside the db path"
    );
    assert!(
        !tmp.path().join(".tl_cache_app").exists(),
        "file-first product layout must not create the legacy cache path"
    );
    assert!(
        !sidecar_dir.join("locks").exists(),
        "file-first product layout must not create a locks directory"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn default_turbolite_layout_is_file_first() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("app.db");
    let sidecar_dir = tmp.path().join("app.db-turbolite");
    let storage = Arc::new(InMemoryStorage::new());

    let db = Builder::new()
        .prefix("default-layout/")
        .mode(HaMode::SingleWriter)
        .role(Role::Leader)
        .durability(turbodb::Durability::Cloud)
        .lease_store(Arc::new(InMemoryLeaseStore::new()))
        .manifest_store(Arc::new(MemManifestStore::new()))
        .turbolite_storage(storage)
        .instance_id("default-layout-node")
        .open(
            db_path.to_str().expect("path"),
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)",
        )
        .await
        .expect("default file-first open");

    db.execute("INSERT INTO t (id) VALUES (7)", &[])
        .expect("insert through default layout");

    assert!(db_path.exists(), "main artifact should be caller db path");
    assert!(
        sidecar_dir.join("local_state.msgpack").exists(),
        "default sidecar should live beside the db path"
    );
    assert!(
        tmp.path().join("app.db-lock").exists(),
        "lock anchor should sit beside the db path"
    );
    assert!(
        !tmp.path().join(".tl_cache_app").exists(),
        "default layout must not create the legacy cache path"
    );
    assert!(
        !sidecar_dir.join("locks").exists(),
        "default layout must not create a locks directory"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn default_file_first_rebuilds_after_hidden_state_delete() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("recover.db");
    let sidecar_dir = tmp.path().join("recover.db-turbolite");
    let storage = Arc::new(InMemoryStorage::new());
    let manifest_store = Arc::new(MemManifestStore::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let mut writer = Builder::new()
        .prefix("recover-layout/")
        .mode(HaMode::SingleWriter)
        .role(Role::Leader)
        .durability(turbodb::Durability::Cloud)
        .lease_store(lease_store.clone())
        .manifest_store(manifest_store.clone())
        .turbolite_storage(storage.clone())
        .instance_id("recover-layout-writer")
        .open(
            db_path.to_str().expect("path"),
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT)",
        )
        .await
        .expect("open writer");

    writer
        .execute("INSERT INTO t (id, val) VALUES (1, 'remote-survives')", &[])
        .expect("insert");
    writer.close().await.expect("close writer");

    std::fs::remove_dir_all(&sidecar_dir).expect("delete hidden state");
    assert!(
        !sidecar_dir.exists(),
        "test must start reopen without hidden state"
    );

    let mut reopened = Builder::new()
        .prefix("recover-layout/")
        .mode(HaMode::SingleWriter)
        .role(Role::Leader)
        .durability(turbodb::Durability::Cloud)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .turbolite_storage(storage)
        .instance_id("recover-layout-reader")
        .open(
            db_path.to_str().expect("path"),
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT)",
        )
        .await
        .expect("reopen after hidden-state deletion");

    let rows = reopened
        .query_values_local("SELECT val FROM t WHERE id = 1", &[])
        .expect("query recovered row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1);
    match &rows[0][0] {
        haqlite_turbolite::SqlValue::Text(value) => assert_eq!(value, "remote-survives"),
        other => panic!("expected recovered text row, got {other:?}"),
    }
    assert!(
        sidecar_dir.exists(),
        "reopen should recreate the hidden state workspace from the remote substrate"
    );
    reopened.close().await.expect("close reopened");
}

#[tokio::test]
async fn singlewriter_without_lease_errors_before_manifest_preflight() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let result = Builder::new()
        .prefix("p/")
        .instance_id("test-1")
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("lease_store() required"),
        "expected lease_store() preflight error, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_without_lease_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();
    let (vfs, vfs_name) = dummy_turbolite_vfs(&tmp);

    let result = Builder::new()
        .prefix("p/")
        .instance_id("test-1")
        .mode(HaMode::SharedWriter)
        .durability(turbodb::Durability::Cloud)
        .turbolite_vfs(vfs, &vfs_name)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("SharedWriter mode not yet implemented"),
        "expected SharedWriter implementation-stub error, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_without_manifest_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let lease = Arc::new(InMemoryLeaseStore::new());
    let (vfs, vfs_name) = dummy_turbolite_vfs(&tmp);

    let result = Builder::new()
        .prefix("p/")
        .instance_id("test-1")
        .mode(HaMode::SharedWriter)
        .durability(turbodb::Durability::Cloud)
        .turbolite_vfs(vfs, &vfs_name)
        .lease_store(lease)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("SharedWriter mode not yet implemented"),
        "expected SharedWriter implementation-stub error, got: {err}"
    );
}

#[tokio::test]
async fn shared_without_turbolite_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());

    let result = Builder::new()
        .prefix("p/")
        .instance_id("test-1")
        .mode(HaMode::SharedWriter)
        .durability(turbodb::Durability::Cloud)
        .lease_store(lease)
        .manifest_store(manifest)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("SharedWriter mode not yet implemented"),
        "expected SharedWriter implementation-stub error, got: {err}"
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

    let db = haqlite::HaQLite::builder()
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

    let coord = db
        .coordinator()
        .expect("singlewriter mode has a coordinator");
    let cfg = coord
        .lease_config()
        .expect("LeaseConfig must be present on SingleWriter mode");
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

    let db = haqlite::HaQLite::builder()
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

    let coord = db
        .coordinator()
        .expect("singlewriter mode has a coordinator");
    let cfg = coord.lease_config().expect("LeaseConfig must be present");
    assert_eq!(cfg.ttl_secs, 17);
    assert_eq!(cfg.renew_interval, Duration::from_millis(9_000));
    assert_eq!(cfg.follower_poll_interval, Duration::from_millis(4_000));
    assert_eq!(cfg.required_expired_reads, 2);
    assert_eq!(cfg.max_consecutive_renewal_errors, 4);
    assert_eq!(
        cfg.instance_id, "caller-instance",
        "builder patches in instance_id"
    );
    assert_eq!(
        cfg.address, "http://127.0.0.1:19091",
        "builder patches in address"
    );
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
}
