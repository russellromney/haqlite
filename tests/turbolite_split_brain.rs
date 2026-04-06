//! Split-brain and concurrent writer tests for haqlite + turbolite + walrust.
//!
//! Architecture: turbolite VFS handles local storage (compression, caching).
//! walrust handles multiwriter coordination (WAL frame shipping via InMemoryStorage).
//! haqlite coordinates writes via leases.
//!
//! No S3 needed: turbolite uses StorageBackend::Local, walrust uses InMemoryStorage.

#![cfg(feature = "turbolite")]

mod common;

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::InMemoryStorage;
use hadb::InMemoryLeaseStore;
use haqlite::{HaMode, HaQLite, InMemoryManifestStore, SqlValue};
use tempfile::TempDir;
use turbolite::tiered::{SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)";

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

fn unique_vfs(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", prefix, n)
}

/// Build a turbolite + walrust shared mode node.
async fn build_tl_node(
    cache_dir: &std::path::Path,
    db_name: &str,
    walrust_storage: Arc<InMemoryStorage>,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<InMemoryManifestStore>,
    instance_id: &str,
    lease_ttl: u64,
    write_timeout_secs: u64,
) -> HaQLite {
    let vfs_name = unique_vfs(&format!("tl_sb_{}", instance_id));
    let config = TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 3,
        pages_per_group: 4,
        sub_pages_per_frame: 2,
        eager_index_load: false,
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("create VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone())
        .expect("register VFS");

    let db_path = cache_dir.join(format!("{}.db", db_name));
    HaQLite::builder("test-bucket")
        .prefix("test/")
        .mode(HaMode::Shared)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .walrust_storage(walrust_storage)
        .turbolite_vfs(shared_vfs, &vfs_name)
        .instance_id(instance_id)
        .manifest_poll_interval(Duration::from_millis(50))
        .write_timeout(Duration::from_secs(write_timeout_secs))
        .lease_ttl(lease_ttl)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open turbolite+walrust shared mode")
}

fn has_key(rows: &[Vec<SqlValue>], key: &str) -> bool {
    rows.iter().any(|r| match &r[0] {
        SqlValue::Text(k) => k == key,
        _ => false,
    })
}

// ============================================================================
// Tests
// ============================================================================

/// Baseline: two nodes write sequentially. Both succeed, both visible.
#[tokio::test(flavor = "multi_thread")]
async fn turbolite_walrust_baseline_sequential() {
    let tmp_a = TempDir::new().expect("tmp");
    let tmp_b = TempDir::new().expect("tmp");

    // Shared walrust storage (both nodes read/write WAL frames here)
    let walrust_storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let mut db_a = build_tl_node(
        tmp_a.path(), "tl_seq", walrust_storage.clone(), lease_store.clone(),
        manifest_store.clone(), "node-a", 5, 10,
    ).await;
    let mut db_b = build_tl_node(
        tmp_b.path(), "tl_seq", walrust_storage.clone(), lease_store.clone(),
        manifest_store.clone(), "node-b", 5, 10,
    ).await;

    // Node A writes
    db_a.execute("INSERT OR REPLACE INTO kv VALUES ('k1', 'from_a')", &[])
        .await
        .expect("node A write");

    // Node B writes (catches up from walrust, then writes)
    db_b.execute("INSERT OR REPLACE INTO kv VALUES ('k2', 'from_b')", &[])
        .await
        .expect("node B write");

    // Both visible via fresh read
    let rows = db_b
        .query_values_fresh("SELECT key, value FROM kv ORDER BY key", &[])
        .await
        .expect("query");
    assert_eq!(rows.len(), 2, "both writes should be visible");
    assert!(has_key(&rows, "k1"), "k1 missing");
    assert!(has_key(&rows, "k2"), "k2 missing");

    db_a.close().await.expect("close a");
    db_b.close().await.expect("close b");
}

/// Concurrent writes from two turbolite+walrust nodes. No data loss.
#[tokio::test(flavor = "multi_thread")]
async fn turbolite_walrust_concurrent_no_data_loss() {
    let tmp_a = TempDir::new().expect("tmp");
    let tmp_b = TempDir::new().expect("tmp");

    let walrust_storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let db_a = Arc::new(tokio::sync::Mutex::new(
        build_tl_node(
            tmp_a.path(), "tl_conc", walrust_storage.clone(), lease_store.clone(),
            manifest_store.clone(), "node-a", 5, 10,
        ).await,
    ));
    let db_b = Arc::new(tokio::sync::Mutex::new(
        build_tl_node(
            tmp_b.path(), "tl_conc", walrust_storage.clone(), lease_store.clone(),
            manifest_store.clone(), "node-b", 5, 10,
        ).await,
    ));

    let a_successes = Arc::new(AtomicU64::new(0));
    let b_successes = Arc::new(AtomicU64::new(0));

    let a = {
        let db = db_a.clone();
        let successes = a_successes.clone();
        tokio::spawn(async move {
            let db = db.lock().await;
            for i in 0..10 {
                match db.execute(
                    "INSERT OR REPLACE INTO kv VALUES (?1, ?2)",
                    &[SqlValue::Text(format!("a_{}", i)), SqlValue::Text(format!("val_a_{}", i))],
                ).await {
                    Ok(_) => { successes.fetch_add(1, Ordering::Relaxed); }
                    Err(e) => eprintln!("node-a write {} failed: {}", i, e),
                }
            }
        })
    };

    let b = {
        let db = db_b.clone();
        let successes = b_successes.clone();
        tokio::spawn(async move {
            let db = db.lock().await;
            for i in 0..10 {
                match db.execute(
                    "INSERT OR REPLACE INTO kv VALUES (?1, ?2)",
                    &[SqlValue::Text(format!("b_{}", i)), SqlValue::Text(format!("val_b_{}", i))],
                ).await {
                    Ok(_) => { successes.fetch_add(1, Ordering::Relaxed); }
                    Err(e) => eprintln!("node-b write {} failed: {}", i, e),
                }
            }
        })
    };

    a.await.expect("join a");
    b.await.expect("join b");

    let a_count = a_successes.load(Ordering::Relaxed);
    let b_count = b_successes.load(Ordering::Relaxed);
    eprintln!("node-a: {} successes, node-b: {} successes", a_count, b_count);

    assert!(a_count > 0, "node-a should succeed");
    assert!(b_count > 0, "node-b should succeed");

    let db_b = db_b.lock().await;
    let rows = db_b
        .query_values_fresh("SELECT key FROM kv ORDER BY key", &[])
        .await
        .expect("query");
    assert_eq!(
        rows.len() as u64, a_count + b_count,
        "total rows ({}) should equal total successes ({}+{}={})",
        rows.len(), a_count, b_count, a_count + b_count,
    );
}

/// 4-node stress test with short lease TTL.
#[tokio::test(flavor = "multi_thread")]
async fn turbolite_walrust_many_writers() {
    let walrust_storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let total_successes = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for node_id in 0..4 {
        let walrust_storage = walrust_storage.clone();
        let lease_store = lease_store.clone();
        let manifest_store = manifest_store.clone();
        let successes = total_successes.clone();

        handles.push(tokio::spawn(async move {
            let tmp = TempDir::new().expect("tmp");
            let mut db = build_tl_node(
                tmp.path(), "tl_stress", walrust_storage, lease_store, manifest_store,
                &format!("node-{}", node_id), 2, 10,
            ).await;

            for i in 0..5 {
                match db.execute(
                    "INSERT OR REPLACE INTO kv VALUES (?1, ?2)",
                    &[
                        SqlValue::Text(format!("n{}_{}", node_id, i)),
                        SqlValue::Text(format!("val_{}_{}", node_id, i)),
                    ],
                ).await {
                    Ok(_) => { successes.fetch_add(1, Ordering::Relaxed); }
                    Err(e) => eprintln!("node-{} write {} failed: {}", node_id, i, e),
                }
            }
            db.close().await.expect("close");
        }));
    }

    for h in handles {
        h.await.expect("join");
    }

    let total = total_successes.load(Ordering::Relaxed);
    eprintln!("total successes: {}", total);
    assert!(total > 0, "at least some writes should succeed");

    // Verify with fresh reader
    let tmp_reader = TempDir::new().expect("tmp");
    let mut reader = build_tl_node(
        tmp_reader.path(), "tl_stress", walrust_storage.clone(), lease_store.clone(),
        manifest_store.clone(), "reader", 5, 10,
    ).await;

    let rows = reader
        .query_values_fresh("SELECT key FROM kv ORDER BY key", &[])
        .await
        .expect("query");

    eprintln!("final rows: {}, expected: {}", rows.len(), total);
    assert_eq!(
        rows.len() as u64, total,
        "row count should match total successful writes"
    );

    reader.close().await.expect("close reader");
}
