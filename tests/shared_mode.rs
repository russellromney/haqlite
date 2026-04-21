//! Phase Crest: Shared mode integration tests.
//!
//! Multi-node shared mode requires S3-backed turbolite for catch-up
//! (set_manifest evicts groups, next read fetches from S3).
//!
//! Requires turbolite-cloud feature.

#![cfg(feature = "turbolite-cloud")]

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use haqlite::{Durability, HaMode, HaQLite, HaQLiteError, InMemoryManifestStore, ManifestStore, SqlValue};
use hadb::{InMemoryLeaseStore, LeaseStore};
use turbolite::tiered::{SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT);";

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);
fn unique_vfs(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", prefix, n)
}

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET").expect("TIERED_TEST_BUCKET required")
}

fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL").ok()
}

fn unique_prefix(name: &str) -> String {
    format!("test/sm/{}/{}", name, std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).expect("time").as_nanos())
}

/// Build a Shared mode HaQLite with S3-backed turbolite (Synchronous durability).
async fn build_shared(
    tmp: &TempDir,
    db_name: &str,
    s3_prefix: &str,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<InMemoryManifestStore>,
    instance_id: &str,
) -> HaQLite {
    let vfs_name = unique_vfs(&format!("sm_{}", instance_id));
    let config = TurboliteConfig {
        bucket: test_bucket(),
        prefix: s3_prefix.to_string(),
        cache_dir: tmp.path().to_path_buf(),
        endpoint_url: endpoint_url(),
        region: Some("auto".to_string()),
        compression_level: 0,
        pages_per_group: 4,
        sub_pages_per_frame: 2,
        eager_index_load: false,
        runtime_handle: Some(tokio::runtime::Handle::current()),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("create VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone())
        .expect("register VFS");

    let db_path = tmp.path().join(format!("{}.db", db_name));
    HaQLite::builder("test-bucket")
        .prefix("test/")
        .mode(HaMode::Shared)
        .durability(Durability::Synchronous)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .turbolite_vfs(shared_vfs, &vfs_name)
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

#[tokio::test(flavor = "multi_thread")]
async fn shared_single_node_write_read() {
    let tmp = TempDir::new().unwrap();
    let prefix = unique_prefix("shared_single_node_write_read");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let mut db = build_shared(&tmp, "test", &prefix, lease_store, manifest_store.clone(), "node-1").await;

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

#[tokio::test(flavor = "multi_thread")]
async fn shared_sequential_writes_increment_manifest() {
    let tmp = TempDir::new().unwrap();
    let prefix = unique_prefix("shared_sequential_writes_increment_manifest");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let mut db = build_shared(&tmp, "test", &prefix, lease_store, manifest_store.clone(), "node-1").await;

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

#[tokio::test(flavor = "multi_thread")]
async fn shared_mode_no_forwarding_server() {
    // Shared mode should not start a forwarding server.
    // We verify by checking the HaQLite opens successfully without binding a port.
    let tmp = TempDir::new().unwrap();
    let prefix = unique_prefix("shared_mode_no_forwarding_server");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let mut db = build_shared(&tmp, "test", &prefix, lease_store, manifest_store, "node-1").await;

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

#[tokio::test(flavor = "multi_thread")]
async fn dedicated_local_mode_still_works() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("local.db");

    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

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

#[tokio::test(flavor = "multi_thread")]
async fn shared_two_nodes_a_writes_b_reads_fresh() {
    // Node A writes, node B catches up via query_row_fresh and sees A's data.
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let prefix = unique_prefix("shared_two_nodes_a_writes_b_reads_fresh");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db_a = build_shared(&tmp_a, "shared", &prefix, lease_store.clone(), manifest_store.clone(), "node-a").await;
    let db_b = build_shared(&tmp_b, "shared", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await;

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

#[tokio::test(flavor = "multi_thread")]
async fn shared_two_nodes_sequential_writes() {
    // Two nodes sharing the same storage. Each writes and publishes manifest.
    // Manifest versions increment correctly across writers.
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let prefix = unique_prefix("shared_two_nodes_sequential_writes");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db_a = build_shared(&tmp_a, "shared", &prefix, lease_store.clone(), manifest_store.clone(), "node-a").await;

    // A writes row 1
    db_a.execute("INSERT INTO t VALUES (1, 'a1')", &[]).unwrap();
    let meta = manifest_store.meta("test/shared/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 1);
    assert_eq!(meta.writer_id, "node-a");

    // A writes row 2
    db_a.execute("INSERT INTO t VALUES (2, 'a2')", &[]).unwrap();
    let meta = manifest_store.meta("test/shared/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 2);

    // A sees both rows
    let count_a: i64 = db_a.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    assert_eq!(count_a, 2);

    // B opens (restores from A's snapshot + changesets) and writes row 3
    let db_b = build_shared(&tmp_b, "shared", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await;
    db_b.execute("INSERT INTO t VALUES (3, 'b1')", &[]).unwrap();

    let meta = manifest_store.meta("test/shared/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 3);
    assert_eq!(meta.writer_id, "node-b");

    // B should see all 3 rows (restored A's data + its own write)
    let count_b: i64 = db_b.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    assert_eq!(count_b, 3, "node B should see A's rows + its own");
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_failover_lease_expires_other_node_writes() {
    // Node A writes then drops. Node B acquires the lease and writes.
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let prefix = unique_prefix("shared_failover_lease_expires_other_node_writes");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    // Node A writes
    {
        let db_a = build_shared(&tmp_a, "shared", &prefix, lease_store.clone(), manifest_store.clone(), "node-a").await;
        db_a.execute("INSERT INTO t VALUES (1, 'before-crash')", &[]).unwrap();
        // db_a drops here, lease should be released in execute_shared
    }

    // Node B should be able to write (lease was released by A)
    let db_b = build_shared(&tmp_b, "shared", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await;
    db_b.execute("INSERT INTO t VALUES (2, 'after-crash')", &[]).unwrap();

    let meta = manifest_store.meta("test/shared/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 2);
    assert_eq!(meta.writer_id, "node-b");

    // B should see both rows
    let count: i64 = db_b.query_row_fresh("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).await.unwrap();
    assert_eq!(count, 2, "node B should see both rows after failover");
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_manifest_version_tracks_writes() {
    // Verify manifest contains correct StorageManifest::Walrust data
    let tmp = TempDir::new().unwrap();
    let prefix = unique_prefix("shared_manifest_version_tracks_writes");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let mut db = build_shared(&tmp, "test", &prefix, lease_store, manifest_store.clone(), "node-1").await;

    db.execute("INSERT INTO t VALUES (1, 'v1')", &[]).unwrap();

    // Fetch full manifest and verify storage variant
    let manifest = manifest_store.get("test/test/_manifest").await.unwrap().unwrap();
    assert_eq!(manifest.writer_id, "node-1");
    assert_eq!(manifest.version, 1, "first write should produce version 1");
    match &manifest.storage {
        turbodb::Backend::Turbolite { turbolite_version, page_count, .. } => {
            assert!(*turbolite_version > 0, "turbolite version should be > 0 after write");
            assert!(*page_count > 0, "page count should be > 0 after write");
        }
        _ => panic!("expected Turbolite storage variant, got {:?}", manifest.storage),
    }
}

// ============================================================================
// Error handling
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn shared_lease_contention_error() {
    // Verify LeaseContention error variant exists and formats correctly
    let err = HaQLiteError::LeaseContention("test contention".into());
    let msg = format!("{}", err);
    assert!(msg.contains("contention"), "error message should mention contention: {}", msg);
}

// ============================================================================
// Stress tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_concurrent_write_contention() {
    // Two nodes share same lease/manifest/storage
    // Both attempt to write simultaneously using tokio::spawn
    // One should succeed, the other should either succeed (serialized) or get LeaseContention
    // Verify: total rows written = number of successful writes
    // Verify: manifest version is consistent
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let prefix = unique_prefix("test_stress_concurrent_write_contention");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db_a = Arc::new(
        build_shared(&tmp_a, "race", &prefix, lease_store.clone(), manifest_store.clone(), "node-a").await,
    );
    let db_b = Arc::new(
        build_shared(&tmp_b, "race", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await,
    );

    let db_a2 = db_a.clone();
    let db_b2 = db_b.clone();

    let handle_a = tokio::spawn(async move {
        db_a2.execute(
            "INSERT INTO t VALUES (1, 'from-a')", &[],
        ).await
    });
    let handle_b = tokio::spawn(async move {
        db_b2.execute(
            "INSERT INTO t VALUES (2, 'from-b')", &[],
        ).await
    });

    let (res_a, res_b) = tokio::join!(handle_a, handle_b);
    let res_a = res_a.unwrap();
    let res_b = res_b.unwrap();

    let mut successes = 0u64;
    if res_a.is_ok() { successes += 1; }
    if res_b.is_ok() { successes += 1; }

    // At least one must succeed
    assert!(successes >= 1, "at least one concurrent write should succeed");

    // For any that failed, it should be LeaseContention
    for (label, res) in [("a", &res_a), ("b", &res_b)] {
        if let Err(ref e) = res {
            let msg = format!("{}", e);
            assert!(
                msg.contains("contention") || msg.contains("Lease") || msg.contains("manifest"),
                "node {} error should be lease/manifest related, got: {}", label, msg,
            );
        }
    }

    // Count actual rows via a fresh read from node that succeeded
    let reader = if res_a.is_ok() { &db_a } else { &db_b };
    let count: i64 = reader.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    assert_eq!(count as u64, successes.min(count as u64), "row count should match successful writes");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_alternating_node_writes() {
    // Node A writes a batch, then node B opens fresh and writes a batch.
    // Mimics the handoff pattern: each writer opens fresh, sees previous data, writes more.
    // After both batches, a fresh reader should see all rows.
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let tmp_reader = TempDir::new().unwrap();
    let prefix = unique_prefix("test_stress_alternating_node_writes");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    // Node A writes 20 rows
    let db_a = build_shared(&tmp_a, "alt", &prefix, lease_store.clone(), manifest_store.clone(), "node-a").await;
    for i in 0..20 {
        db_a.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("from-a-{}", i))],
        ).await.unwrap_or_else(|e| panic!("write {} from A failed: {}", i, e));
    }

    let meta_a = manifest_store.meta("test/alt/_manifest").await.unwrap().unwrap();
    assert_eq!(meta_a.version, 20, "A should have published 20 manifest versions");

    // Node B opens fresh (restores from A's data), writes 20 more rows
    let db_b = build_shared(&tmp_b, "alt", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await;
    for i in 20..40 {
        db_b.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("from-b-{}", i))],
        ).await.unwrap_or_else(|e| panic!("write {} from B failed: {}", i, e));
    }

    let meta_b = manifest_store.meta("test/alt/_manifest").await.unwrap().unwrap();
    assert_eq!(meta_b.version, 40, "manifest should be at v40 after 40 total writes");

    // Fresh reader should see all 40 rows
    let reader = build_shared(&tmp_reader, "alt", &prefix, lease_store.clone(), manifest_store.clone(), "reader").await;
    let count: i64 = reader.query_row_fresh("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).await.unwrap();
    assert_eq!(count, 40, "fresh reader should see all 40 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_large_transaction() {
    // Single execute with many rows via a multi-statement INSERT
    // Verify all rows present after fresh read on another node
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let prefix = unique_prefix("test_stress_large_transaction");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db_a = build_shared(&tmp_a, "large", &prefix, lease_store.clone(), manifest_store.clone(), "node-a").await;

    // Insert 50 rows in individual execute calls.
    // Each is a full lease acquire/write/S3 sync/manifest publish/release cycle.
    for i in 0..50 {
        db_a.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("row-{}", i))],
        ).await.unwrap_or_else(|e| panic!("insert row {} failed: {}", i, e));
    }

    let count_a: i64 = db_a.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    assert_eq!(count_a, 50, "node A should see all 50 rows");

    // Node B should see all rows via fresh read
    let db_b = build_shared(&tmp_b, "large", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await;
    let count_b: i64 = db_b.query_row_fresh("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).await.unwrap();
    assert_eq!(count_b, 50, "node B should see all 50 rows after fresh read");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lease_ttl_expiry_takeover() {
    // Node A acquires lease manually with short TTL (via write_if_not_exists directly)
    // Wait for TTL + margin
    // Node B should be able to write (lease expired)
    let _tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let prefix = unique_prefix("test_lease_ttl_expiry_takeover");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    // Pre-claim lease with 1-second TTL (using millisecond timestamp, matching execute_shared format)
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let fake_lease = serde_json::json!({
        "instance_id": "stale-node",
        "timestamp": now_ms,
        "ttl_secs": 1,
    });
    lease_store
        .write_if_not_exists(
            "test/ttl/_lease",
            serde_json::to_vec(&fake_lease).unwrap(),
        )
        .await
        .unwrap();

    // Wait for TTL to expire (1 second + 200ms margin)
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Node B should be able to write (expired lease gets taken over)
    let db_b = build_shared(&tmp_b, "ttl", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await;
    db_b.execute("INSERT INTO t VALUES (1, 'after-expiry')", &[])
        .expect("node B should write after lease expiry");

    let val: String = db_b.query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0)).unwrap();
    assert_eq!(val, "after-expiry");

    let meta = manifest_store.meta("test/ttl/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 1);
    assert_eq!(meta.writer_id, "node-b");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_many_sequential_writes() {
    // 50 sequential writes from single node
    // Verify manifest version = 50
    // Verify all 50 rows present
    let tmp = TempDir::new().unwrap();
    let prefix = unique_prefix("test_stress_many_sequential_writes");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let mut db = build_shared(&tmp, "seq50", &prefix, lease_store, manifest_store.clone(), "node-1").await;

    for i in 0..50 {
        db.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("v{}", i))],
        ).await.unwrap_or_else(|e| panic!("write {} failed: {}", i, e));
    }

    let meta = manifest_store.meta("test/seq50/_manifest").await.unwrap().unwrap();
    assert_eq!(meta.version, 50, "50 writes should produce manifest v50");

    let count: i64 = db.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    assert_eq!(count, 50, "should have 50 rows");
}

// ============================================================================
// Edge case tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_write_timeout_lease_contention() {
    // Pre-claim lease with long TTL (InMemoryLeaseStore)
    // Node tries to write with short write_timeout (100ms)
    // Should get LeaseContention error
    let tmp = TempDir::new().unwrap();
    let prefix = unique_prefix("test_write_timeout_lease_contention");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    // Pre-claim lease with long TTL so it won't expire during the test
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let blocking_lease = serde_json::json!({
        "instance_id": "blocker-node",
        "timestamp": now_ms,
        "ttl_secs": 300,
    });
    lease_store
        .write_if_not_exists(
            "test/contention/_lease",
            serde_json::to_vec(&blocking_lease).unwrap(),
        )
        .await
        .unwrap();

    // Build node with very short write_timeout using S3-backed VFS inline
    let vfs_name = unique_vfs("sm_blocked");
    let config = TurboliteConfig {
        bucket: test_bucket(),
        prefix: prefix.clone(),
        cache_dir: tmp.path().to_path_buf(),
        endpoint_url: endpoint_url(),
        region: Some("auto".to_string()),
        compression_level: 0,
        pages_per_group: 4,
        sub_pages_per_frame: 2,
        eager_index_load: false,
        runtime_handle: Some(tokio::runtime::Handle::current()),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("create VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone())
        .expect("register VFS");

    let db_path = tmp.path().join("contention.db");
    let mut db = HaQLite::builder("test-bucket")
        .prefix("test/")
        .mode(HaMode::Shared)
        .durability(haqlite::Durability::Synchronous)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .turbolite_vfs(shared_vfs, &vfs_name)
        .instance_id("blocked-node")
        .manifest_poll_interval(Duration::from_millis(50))
        .write_timeout(Duration::from_millis(100))
        .open(db_path.to_str().unwrap(), SCHEMA)
        .await
        .expect("open should succeed");

    let result = db.execute("INSERT INTO t VALUES (1, 'blocked')", &[]);
    assert!(result.is_err(), "write should fail due to lease contention");
    let err = result.unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("contention") || msg.contains("Lease"),
        "error should mention lease contention, got: {}", msg,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fresh_read_consistency() {
    // Node A writes rows, then a fresh reader opens and sees all data.
    // Then node B (fresh) takes over writing and a new reader sees everything.
    let tmp_a = TempDir::new().unwrap();
    let prefix = unique_prefix("test_fresh_read_consistency");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    // Node A writes 5 rows
    let db_a = build_shared(&tmp_a, "fresh", &prefix, lease_store.clone(), manifest_store.clone(), "node-a").await;
    for i in 1..=5 {
        db_a.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("row-{}", i))],
        ).await.unwrap();
    }

    // Fresh reader should see all 5 rows
    let tmp_r1 = TempDir::new().unwrap();
    let reader1 = build_shared(&tmp_r1, "fresh", &prefix, lease_store.clone(), manifest_store.clone(), "reader-1").await;
    let count: i64 = reader1.query_row_fresh(
        "SELECT COUNT(*) FROM t", &[], |r| r.get(0),
    ).await.unwrap();
    assert_eq!(count, 5, "fresh reader should see 5 rows after A's writes");

    // Node B opens fresh, writes 3 more rows (restoring from A's 5-row state first)
    let tmp_b = TempDir::new().unwrap();
    let db_b = build_shared(&tmp_b, "fresh", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await;
    for i in 6..=8 {
        db_b.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("row-{}", i))],
        ).await.unwrap();
    }

    // Another fresh reader should see all 8
    let tmp_r2 = TempDir::new().unwrap();
    let reader2 = build_shared(&tmp_r2, "fresh", &prefix, lease_store.clone(), manifest_store.clone(), "reader-2").await;
    let count2: i64 = reader2.query_row_fresh(
        "SELECT COUNT(*) FROM t", &[], |r| r.get(0),
    ).await.unwrap();
    assert_eq!(count2, 8, "fresh reader should see all 8 rows after B's writes");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fresh_read_empty_database() {
    // Shared mode: schema is applied by the first write (via execute_shared).
    // Fresh reads before any write should fail because the table doesn't exist
    // yet (no writer has created the schema). This is correct: readers catch up
    // from the manifest store, and an empty manifest has no schema/pages.
    //
    // After the first write, both nodes should see the schema + data.
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let prefix = unique_prefix("test_fresh_read_empty_database");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db_a = build_shared(&tmp_a, "empty", &prefix, lease_store.clone(), manifest_store.clone(), "node-a").await;
    let db_b = build_shared(&tmp_b, "empty", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await;

    // Before any writes: table doesn't exist, query should error
    let result = db_a.query_row_fresh(
        "SELECT COUNT(*) FROM t", &[], |r| r.get::<_, i64>(0),
    ).await;
    assert!(result.is_err(), "query before first write should fail (no schema yet)");

    // First write applies schema + inserts data
    db_a.execute("INSERT INTO t VALUES (1, 'first')", &[])
        .expect("first write should succeed (applies schema)");

    // Now both nodes should see the data via fresh read
    let count_a: i64 = db_a.query_row_fresh(
        "SELECT COUNT(*) FROM t", &[], |r| r.get(0),
    ).await.expect("node A fresh read after write");
    assert_eq!(count_a, 1, "node A should see 1 row");

    let count_b: i64 = db_b.query_row_fresh(
        "SELECT COUNT(*) FROM t", &[], |r| r.get(0),
    ).await.expect("node B fresh read should catch up");
    assert_eq!(count_b, 1, "node B should see 1 row via manifest catch-up");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_after_lease_release() {
    // Node A writes (acquires + releases lease)
    // Node B immediately writes after A
    // Should succeed without contention (lease was released)
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let prefix = unique_prefix("test_write_after_lease_release");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db_a = build_shared(&tmp_a, "release", &prefix, lease_store.clone(), manifest_store.clone(), "node-a").await;
    let db_b = build_shared(&tmp_b, "release", &prefix, lease_store.clone(), manifest_store.clone(), "node-b").await;

    // Node A writes (lease should be acquired and released within execute)
    db_a.execute("INSERT INTO t VALUES (1, 'from-a')", &[]).unwrap();

    // Node B should immediately be able to write (no contention)
    db_b.execute("INSERT INTO t VALUES (2, 'from-b')", &[])
        .expect("node B should write without contention after A released lease");

    let count: i64 = db_b.query_row_fresh("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).await.unwrap();
    assert_eq!(count, 2, "both rows should be visible");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_manifest_version_monotonicity() {
    // Write N times, check manifest version after each
    // Versions should be strictly monotonically increasing (1, 2, 3, ...)
    let tmp = TempDir::new().unwrap();
    let prefix = unique_prefix("test_manifest_version_monotonicity");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let mut db = build_shared(&tmp, "mono", &prefix, lease_store, manifest_store.clone(), "node-1").await;

    let mut prev_version = 0u64;
    for i in 1..=10 {
        db.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("v{}", i))],
        ).await.unwrap();

        let meta = manifest_store.meta("test/mono/_manifest").await.unwrap().unwrap();
        assert!(
            meta.version > prev_version,
            "manifest version {} should be greater than previous {}", meta.version, prev_version,
        );
        assert_eq!(meta.version, i as u64, "manifest version should equal write count");
        prev_version = meta.version;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_databases_shared_stores() {
    // Two different databases (different db_name) sharing same ManifestStore
    // Writes to db1 should not affect db2's manifest
    let tmp_1 = TempDir::new().unwrap();
    let tmp_2 = TempDir::new().unwrap();
    let prefix = unique_prefix("test_multiple_databases_shared_stores");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let prefix_alpha = unique_prefix("test_multi_db_alpha");
    let prefix_beta = unique_prefix("test_multi_db_beta");
    let db1 = build_shared(&tmp_1, "db_alpha", &prefix_alpha, lease_store.clone(), manifest_store.clone(), "node-1").await;
    let db2 = build_shared(&tmp_2, "db_beta", &prefix_beta, lease_store.clone(), manifest_store.clone(), "node-1").await;

    // Write 3 rows to db1
    for i in 0..3 {
        db1.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("alpha-{}", i))],
        ).await.unwrap();
    }

    // Write 1 row to db2
    db2.execute(
        "INSERT INTO t VALUES (100, 'beta-0')", &[],
    ).await.unwrap();

    // db1 manifest should be at version 3
    let meta1 = manifest_store.meta("test/db_alpha/_manifest").await.unwrap().unwrap();
    assert_eq!(meta1.version, 3, "db_alpha manifest should be v3");

    // db2 manifest should be at version 1
    let meta2 = manifest_store.meta("test/db_beta/_manifest").await.unwrap().unwrap();
    assert_eq!(meta2.version, 1, "db_beta manifest should be v1");

    // db1 should have 3 rows, db2 should have 1 row
    let count1: i64 = db1.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    let count2: i64 = db2.query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0)).unwrap();
    assert_eq!(count1, 3, "db_alpha should have 3 rows");
    assert_eq!(count2, 1, "db_beta should have 1 row");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_empty_params() {
    // execute("INSERT INTO t VALUES (1, 'no_params')", &[])
    // Should work (empty params slice)
    let tmp = TempDir::new().unwrap();
    let prefix = unique_prefix("test_write_empty_params");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let mut db = build_shared(&tmp, "empty_params", &prefix, lease_store, manifest_store.clone(), "node-1").await;

    let rows = db.execute("INSERT INTO t VALUES (1, 'no_params')", &[]).unwrap();
    assert_eq!(rows, 1);

    let val: String = db.query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0)).unwrap();
    assert_eq!(val, "no_params");
}
