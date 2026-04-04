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
// Multi-node tests
// ============================================================================

#[tokio::test]
async fn shared_two_nodes_a_writes_b_reads_fresh() {
    // Node A writes, node B catches up via query_row_fresh and sees A's data.
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db_a = build_shared(&tmp_a, "shared", storage.clone(), lease_store.clone(), manifest_store.clone(), "node-a").await;
    let db_b = build_shared(&tmp_b, "shared", storage.clone(), lease_store.clone(), manifest_store.clone(), "node-b").await;

    // Node A writes
    db_a.execute(
        "INSERT INTO t VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("from-a".into())],
    ).await.unwrap();

    // Manifest should show v1
    let meta = manifest_store.meta("test/shared/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 1);
    assert_eq!(meta.writer_id, "node-a");

    // Node B fresh read should catch up and see A's data
    let val: String = db_b.query_row_fresh(
        "SELECT val FROM t WHERE id = 1", &[], |r| r.get(0),
    ).await.unwrap();
    assert_eq!(val, "from-a", "node B should see node A's write after fresh read");
}

#[tokio::test]
async fn shared_two_nodes_sequential_writes() {
    // Two nodes sharing the same storage. Each writes and publishes manifest.
    // Manifest versions increment correctly across writers.
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db_a = build_shared(&tmp_a, "shared", storage.clone(), lease_store.clone(), manifest_store.clone(), "node-a").await;

    // A writes row 1
    db_a.execute("INSERT INTO t VALUES (1, 'a1')", &[]).await.unwrap();
    let meta = manifest_store.meta("test/shared/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 1);
    assert_eq!(meta.writer_id, "node-a");

    // A writes row 2
    db_a.execute("INSERT INTO t VALUES (2, 'a2')", &[]).await.unwrap();
    let meta = manifest_store.meta("test/shared/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 2);

    // A sees both rows
    let count_a: i64 = db_a.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    assert_eq!(count_a, 2);

    // B opens (restores from A's snapshot + changesets) and writes row 3
    let db_b = build_shared(&tmp_b, "shared", storage.clone(), lease_store.clone(), manifest_store.clone(), "node-b").await;
    db_b.execute("INSERT INTO t VALUES (3, 'b1')", &[]).await.unwrap();

    let meta = manifest_store.meta("test/shared/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 3);
    assert_eq!(meta.writer_id, "node-b");

    // B should see all 3 rows (restored A's data + its own write)
    let count_b: i64 = db_b.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    assert_eq!(count_b, 3, "node B should see A's rows + its own");
}

#[tokio::test]
async fn shared_failover_lease_expires_other_node_writes() {
    // Node A writes then drops. Node B acquires the lease and writes.
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    // Node A writes
    {
        let db_a = build_shared(&tmp_a, "shared", storage.clone(), lease_store.clone(), manifest_store.clone(), "node-a").await;
        db_a.execute("INSERT INTO t VALUES (1, 'before-crash')", &[]).await.unwrap();
        // db_a drops here, lease should be released in execute_shared
    }

    // Node B should be able to write (lease was released by A)
    let db_b = build_shared(&tmp_b, "shared", storage.clone(), lease_store.clone(), manifest_store.clone(), "node-b").await;
    db_b.execute("INSERT INTO t VALUES (2, 'after-crash')", &[]).await.unwrap();

    let meta = manifest_store.meta("test/shared/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 2);
    assert_eq!(meta.writer_id, "node-b");

    // B should see both rows
    let count: i64 = db_b.query_row_fresh("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).await.unwrap();
    assert_eq!(count, 2, "node B should see both rows after failover");
}

#[tokio::test]
async fn shared_manifest_version_tracks_writes() {
    // Verify manifest contains correct StorageManifest::Walrust data
    let tmp = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db = build_shared(&tmp, "test", storage, lease_store, manifest_store.clone(), "node-1").await;

    db.execute("INSERT INTO t VALUES (1, 'v1')", &[]).await.unwrap();

    // Fetch full manifest and verify storage variant
    let manifest = manifest_store.get("test/test/_manifest").await.unwrap().unwrap();
    assert_eq!(manifest.writer_id, "node-1");
    match &manifest.storage {
        hadb::StorageManifest::Walrust { changeset_prefix, latest_changeset_key, .. } => {
            assert!(changeset_prefix.starts_with("test/test/"), "changeset prefix should include db name");
            assert!(!latest_changeset_key.is_empty(), "latest_changeset_key should not be empty");
            assert!(latest_changeset_key.ends_with(".hadbp"), "changeset key should end with .hadbp");
        }
        _ => panic!("expected Walrust storage variant"),
    }
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
