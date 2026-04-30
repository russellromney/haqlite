//! End-to-end tests for each durability mode.
//!
//! Tests the full write path through the public API:
//! execute -> query_values_fresh -> close.
//!
//! Each test opens multiple nodes, writes from each, and verifies
//! all data is visible to all nodes via fresh reads.
//!
//! Requires turbolite-cloud feature and S3 credentials.

#![cfg(feature = "turbolite-cloud")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use haqlite::{HaQLite, InMemoryLeaseStore, SqlValue};
use haqlite_turbolite::{Builder, HaMode};
use tempfile::TempDir;
use turbodb_manifest_mem::MemManifestStore;
use turbolite::tiered::{SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

mod common;
use common::InMemoryStorage;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY,
    node TEXT NOT NULL,
    value TEXT NOT NULL
);";

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);
fn unique_vfs(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("e2e_{}_{}", prefix, n)
}

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET").expect("TIERED_TEST_BUCKET required")
}

fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL").ok()
}

fn unique_prefix(name: &str) -> String {
    format!(
        "test/e2e/{}/{}",
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    )
}

/// Build a sharedwriter-mode node with Cloud durability.
async fn build_node(
    cache_dir: &std::path::Path,
    s3_prefix: &str,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<MemManifestStore>,
    walrust_storage: Option<Arc<InMemoryStorage>>,
    instance_id: &str,
) -> HaQLite {
    let vfs_name = unique_vfs(instance_id);
    let config = TurboliteConfig {
        bucket: test_bucket(),
        prefix: s3_prefix.to_string(),
        cache_dir: cache_dir.to_path_buf(),
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
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register VFS");

    let db_path = cache_dir.join("e2e.db");
    let mut builder = Builder::new()
        .prefix("test/")
        .mode(HaMode::SharedWriter)
        .durability(turbodb::Durability::Cloud)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .turbolite_vfs(shared_vfs, &vfs_name)
        .instance_id(instance_id)
        .manifest_poll_interval(Duration::from_millis(50))
        .write_timeout(Duration::from_secs(10));

    if let Some(ws) = walrust_storage {
        builder = builder.walrust_storage(ws);
    }

    builder
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open node")
}

// ============================================================================
// Synchronous durability (S3Primary)
// ============================================================================

/// Two nodes write sequentially, both see all data.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_synchronous_two_nodes_sequential() {
    let prefix = unique_prefix("sync_seq");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let tmp_a = TempDir::new().expect("tmp");
    let db_a = build_node(
        tmp_a.path(),
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        None,
        "node-a",
    )
    .await;

    let tmp_b = TempDir::new().expect("tmp");
    let db_b = build_node(
        tmp_b.path(),
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        None,
        "node-b",
    )
    .await;

    // Node A writes 5 items
    for i in 0..5 {
        db_a.execute(
            "INSERT INTO items (id, node, value) VALUES (?1, ?2, ?3)",
            &[
                SqlValue::Integer(i),
                SqlValue::Text("a".into()),
                SqlValue::Text(format!("val_{}", i)),
            ],
        )
        .await
        .expect("node-a write");
    }

    // Node B writes 5 items
    for i in 5..10 {
        db_b.execute(
            "INSERT INTO items (id, node, value) VALUES (?1, ?2, ?3)",
            &[
                SqlValue::Integer(i),
                SqlValue::Text("b".into()),
                SqlValue::Text(format!("val_{}", i)),
            ],
        )
        .await
        .expect("node-b write");
    }

    // Both nodes see all 10 items via fresh read
    let rows_a = db_a
        .query_values_fresh("SELECT id FROM items ORDER BY id", &[])
        .await
        .expect("query a");
    let rows_b = db_b
        .query_values_fresh("SELECT id FROM items ORDER BY id", &[])
        .await
        .expect("query b");

    assert_eq!(
        rows_a.len(),
        10,
        "node-a should see all 10 items, got {}",
        rows_a.len()
    );
    assert_eq!(
        rows_b.len(),
        10,
        "node-b should see all 10 items, got {}",
        rows_b.len()
    );
}

/// Four nodes write concurrently, all data preserved.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_synchronous_four_nodes_concurrent() {
    let prefix = unique_prefix("sync_conc");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    // Open nodes sequentially (VFS fetches S3 manifest at creation time)
    let mut tmps = Vec::new();
    let mut dbs = Vec::new();
    for i in 0..4 {
        let tmp = TempDir::new().expect("tmp");
        let db = build_node(
            tmp.path(),
            &prefix,
            lease_store.clone(),
            manifest_store.clone(),
            None,
            &format!("node-{}", i),
        )
        .await;
        tmps.push(tmp);
        dbs.push(Arc::new(tokio::sync::Mutex::new(db)));
    }

    // Each node writes 5 items concurrently
    let successes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::new();
    for (node_id, db) in dbs.iter().enumerate() {
        let db = db.clone();
        let s = successes.clone();
        handles.push(tokio::spawn(async move {
            let db = db.lock().await;
            for i in 0..5 {
                let id = (node_id * 5 + i) as i64;
                match db
                    .execute(
                        "INSERT INTO items (id, node, value) VALUES (?1, ?2, ?3)",
                        &[
                            SqlValue::Integer(id),
                            SqlValue::Text(format!("n{}", node_id)),
                            SqlValue::Text(format!("val_{}", id)),
                        ],
                    )
                    .await
                {
                    Ok(_) => {
                        s.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => eprintln!("node-{} write {} failed: {}", node_id, i, e),
                }
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    let total = successes.load(Ordering::Relaxed);
    assert!(total > 0, "at least one write should succeed");

    // Fresh reader verifies all successful writes are visible
    let tmp_reader = TempDir::new().expect("tmp");
    let reader = build_node(
        tmp_reader.path(),
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        None,
        "reader",
    )
    .await;

    let rows = reader
        .query_values_fresh("SELECT id FROM items ORDER BY id", &[])
        .await
        .expect("query");
    assert_eq!(
        rows.len() as u64,
        total,
        "reader should see {} rows (all successes), got {}",
        total,
        rows.len()
    );
}

// Eventual + Shared is an invalid configuration (haqlite rejects it at open time).
// Multi-writer requires every write to be durable to S3 so each writer sees the
// latest state. Eventual durability with SingleWriter mode (single leader + followers)
// is tested in ha_database.rs.

// ============================================================================
// Write failure without lease
// ============================================================================

/// A node that can't acquire the lease fails with LeaseContention.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_write_fails_without_lease() {
    use hadb::LeaseStore;

    let prefix = unique_prefix("no_lease");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    // Pre-populate a lease that won't expire (held by another node).
    // Key must match what execute_shared constructs: {prefix}{db_name}/_lease
    let lease_key = "test/blocked/_lease";
    let lease_data = serde_json::to_vec(&serde_json::json!({
        "instance_id": "other-node",
        "timestamp": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default().as_millis() as u64,
        "ttl_secs": 300,
    }))
    .expect("json");
    lease_store
        .write_if_not_exists(lease_key, lease_data)
        .await
        .expect("seed lease");

    // Build a node with very short write timeout
    let tmp = TempDir::new().expect("tmp");
    let vfs_name = unique_vfs("blocked");
    let config = TurboliteConfig {
        bucket: test_bucket(),
        prefix: prefix.clone(),
        cache_dir: tmp.path().to_path_buf(),
        endpoint_url: endpoint_url(),
        region: Some("auto".to_string()),
        eager_index_load: false,
        runtime_handle: Some(tokio::runtime::Handle::current()),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("vfs");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("reg");

    let db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SharedWriter)
        .durability(turbodb::Durability::Cloud)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .turbolite_vfs(shared_vfs, &vfs_name)
        .instance_id("blocked-node")
        .write_timeout(Duration::from_millis(200))
        .open(tmp.path().join("blocked.db").to_str().expect("p"), SCHEMA)
        .await
        .expect("open");

    let result = db
        .execute(
            "INSERT INTO items (id, node, value) VALUES (1, 'blocked', 'should_fail')",
            &[],
        )
        .await;

    assert!(result.is_err(), "write without lease should fail");
    let err_str = format!("{}", result.unwrap_err());
    assert!(
        err_str.contains("lease") || err_str.contains("Lease"),
        "error should mention lease, got: {}",
        err_str
    );
}
