//! Phase Crest: Shared mode integration tests.
//!
//! Uses in-memory storage backends (no S3). Tests the per-write lease cycle:
//! acquire -> catch-up -> write -> checkpoint -> publish -> release.

mod common;

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use common::InMemoryStorage;
use haqlite::{HaMode, HaQLite, HaQLiteError, InMemoryManifestStore, ManifestStore, SqlValue};
use hadb::InMemoryLeaseStore;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT);";

/// Build a Shared mode HaQLite with in-memory backends.
async fn build_shared(
    tmp: &TempDir,
    db_name: &str,
    storage: Arc<InMemoryStorage>,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<InMemoryManifestStore>,
    instance_id: &str,
) -> HaQLite {
    let db_path = tmp.path().join(format!("{}.db", db_name));
    HaQLite::builder("test-bucket")
        .prefix("test/")
        .mode(HaMode::Shared)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .walrust_storage(storage)
        .instance_id(instance_id)
        .manifest_poll_interval(Duration::from_millis(50))
        .write_timeout(Duration::from_secs(2))
        .open(db_path.to_str().unwrap(), SCHEMA)
        .await
        .expect("open shared mode")
}

// ============================================================================
// Single node tests
// ============================================================================

#[tokio::test]
async fn shared_single_node_write_read() {
    let tmp = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db = build_shared(&tmp, "test", storage, lease_store, manifest_store.clone(), "node-1").await;

    // Write
    let rows = db.execute(
        "INSERT INTO t (id, val) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("hello".into())],
    ).await.unwrap();
    assert_eq!(rows, 1);

    // Read
    let val: String = db.query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0)).unwrap();
    assert_eq!(val, "hello");

    // Manifest should be published
    let meta = manifest_store.meta("test/test/_manifest").await.unwrap();
    assert!(meta.is_some(), "manifest should be published after write");
    assert_eq!(meta.unwrap().version, 1);
}

#[tokio::test]
async fn shared_sequential_writes_increment_manifest() {
    let tmp = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db = build_shared(&tmp, "test", storage, lease_store, manifest_store.clone(), "node-1").await;

    for i in 0..3 {
        db.execute(
            "INSERT INTO t (id, val) VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("v{}", i))],
        ).await.unwrap();
    }

    let meta = manifest_store.meta("test/test/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 3, "3 writes should produce manifest v3");

    let count: i64 = db.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn shared_mode_no_forwarding_server() {
    // Shared mode should not start a forwarding server.
    // We verify by checking the HaQLite opens successfully without binding a port.
    let tmp = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db = build_shared(&tmp, "test", storage, lease_store, manifest_store, "node-1").await;

    // If forwarding server was started, this test would fail on port conflict
    // when run in parallel with other tests using the same port.
    let rows = db.execute(
        "INSERT INTO t VALUES (1, 'ok')",
        &[],
    ).await.unwrap();
    assert_eq!(rows, 1);
}

// ============================================================================
// Dedicated mode backward compat
// ============================================================================

#[tokio::test]
async fn dedicated_local_mode_still_works() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("local.db");

    let db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    let rows = db.execute(
        "INSERT INTO t VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("dedicated".into())],
    ).await.unwrap();
    assert_eq!(rows, 1);

    let val: String = db.query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0)).unwrap();
    assert_eq!(val, "dedicated");
}

// ============================================================================
// Error handling
// ============================================================================

#[tokio::test]
async fn shared_lease_contention_error() {
    // Verify LeaseContention error variant exists and formats correctly
    let err = HaQLiteError::LeaseContention("test contention".into());
    let msg = format!("{}", err);
    assert!(msg.contains("contention"), "error message should mention contention: {}", msg);
}
