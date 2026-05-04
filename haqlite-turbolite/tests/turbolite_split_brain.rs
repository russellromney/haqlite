//! Split-brain and concurrent writer tests for haqlite + S3-backed turbolite.
//!
//! Multi-node sharedwriter mode requires S3-backed turbolite for catch-up.
//! These tests need the s3 feature and S3 credentials.

#![cfg(feature = "legacy-s3-mode-tests")]

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use haqlite::{HaQLite, InMemoryLeaseStore, SqlValue};
use haqlite_turbolite::{Builder, HaMode};
use tempfile::TempDir;
use turbodb_manifest_mem::MemManifestStore;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)";

fn unique_prefix(name: &str) -> String {
    format!(
        "test/tl_sb/{}/{}",
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    )
}

/// Build an S3-backed turbolite sharedwriter mode node.
async fn build_tl_node(
    cache_dir: &std::path::Path,
    db_name: &str,
    s3_prefix: &str,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<MemManifestStore>,
    instance_id: &str,
    lease_ttl: u64,
    write_timeout_secs: u64,
) -> HaQLite {
    let (shared_vfs, vfs_name, _) = common::make_s3_vfs(
        cache_dir,
        &format!("tl_sb_{}", instance_id),
        s3_prefix,
        3,
        None,
    )
    .await;

    let db_path = cache_dir.join(format!("{}.db", db_name));
    Builder::new()
        .prefix("test/")
        .mode(HaMode::SharedWriter)
        .durability(turbodb::Durability::Cloud)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .turbolite_vfs(shared_vfs, &vfs_name)
        .instance_id(instance_id)
        .manifest_poll_interval(Duration::from_millis(50))
        .write_timeout(Duration::from_secs(write_timeout_secs))
        .lease_ttl(lease_ttl)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open S3-backed turbolite sharedwriter mode")
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

    let prefix = unique_prefix("baseline_sequential");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let mut db_a = build_tl_node(
        tmp_a.path(),
        "tl_seq",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "node-a",
        5,
        10,
    )
    .await;
    let mut db_b = build_tl_node(
        tmp_b.path(),
        "tl_seq",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "node-b",
        5,
        10,
    )
    .await;

    // Node A writes
    db_a.execute("INSERT OR REPLACE INTO kv VALUES ('k1', 'from_a')", &[])
        .expect("node A write");

    // Node B writes (catches up from walrust, then writes)
    db_b.execute("INSERT OR REPLACE INTO kv VALUES ('k2', 'from_b')", &[])
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

    let prefix = unique_prefix("concurrent_no_data_loss");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let db_a = Arc::new(tokio::sync::Mutex::new(
        build_tl_node(
            tmp_a.path(),
            "tl_conc",
            &prefix,
            lease_store.clone(),
            manifest_store.clone(),
            "node-a",
            5,
            10,
        )
        .await,
    ));
    let db_b = Arc::new(tokio::sync::Mutex::new(
        build_tl_node(
            tmp_b.path(),
            "tl_conc",
            &prefix,
            lease_store.clone(),
            manifest_store.clone(),
            "node-b",
            5,
            10,
        )
        .await,
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
                    &[
                        SqlValue::Text(format!("a_{}", i)),
                        SqlValue::Text(format!("val_a_{}", i)),
                    ],
                ) {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
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
                    &[
                        SqlValue::Text(format!("b_{}", i)),
                        SqlValue::Text(format!("val_b_{}", i)),
                    ],
                ) {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => eprintln!("node-b write {} failed: {}", i, e),
                }
            }
        })
    };

    a.await.expect("join a");
    b.await.expect("join b");

    let a_count = a_successes.load(Ordering::Relaxed);
    let b_count = b_successes.load(Ordering::Relaxed);
    eprintln!(
        "node-a: {} successes, node-b: {} successes",
        a_count, b_count
    );

    assert!(a_count > 0, "node-a should succeed");
    assert!(b_count > 0, "node-b should succeed");

    let db_b = db_b.lock().await;
    let rows = db_b
        .query_values_fresh("SELECT key FROM kv ORDER BY key", &[])
        .await
        .expect("query");
    assert_eq!(
        rows.len() as u64,
        a_count + b_count,
        "total rows ({}) should equal total successes ({}+{}={})",
        rows.len(),
        a_count,
        b_count,
        a_count + b_count,
    );
}

/// 4-node stress test with short lease TTL and sequential opens.
#[tokio::test(flavor = "multi_thread")]
async fn turbolite_walrust_many_writers() {
    let prefix = unique_prefix("many_writers");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    // Open nodes sequentially to avoid races during open.
    // Write loops run concurrently -- the lease serializes actual writes.
    let mut tmps = Vec::new();
    let mut dbs = Vec::new();
    for node_id in 0..4 {
        let tmp = TempDir::new().expect("tmp");
        let db = build_tl_node(
            tmp.path(),
            "tl_stress",
            &prefix,
            lease_store.clone(),
            manifest_store.clone(),
            &format!("node-{}", node_id),
            2,
            10,
        )
        .await;
        tmps.push(tmp);
        dbs.push(Arc::new(tokio::sync::Mutex::new(db)));
    }

    // Each node writes 5 rows. The lease serializes writes across nodes.
    let mut total_successes = 0u64;
    for (node_id, db) in dbs.iter().enumerate() {
        let db = db.lock().await;
        for i in 0..5 {
            match db.execute(
                "INSERT OR REPLACE INTO kv VALUES (?1, ?2)",
                &[
                    SqlValue::Text(format!("n{}_{}", node_id, i)),
                    SqlValue::Text(format!("val_{}_{}", node_id, i)),
                ],
            ) {
                Ok(_) => {
                    total_successes += 1;
                }
                Err(e) => eprintln!("node-{} write {} failed: {}", node_id, i, e),
            }
        }
    }

    // Close all writer nodes
    for db in dbs {
        if let Ok(db) = Arc::try_unwrap(db) {
            let mut db = db.into_inner();
            db.close().await.expect("close");
        }
    }

    let total = total_successes;
    eprintln!("total successes: {}", total);
    assert!(total > 0, "at least some writes should succeed");

    // Verify with fresh reader
    let tmp_reader = TempDir::new().expect("tmp");
    let mut reader = build_tl_node(
        tmp_reader.path(),
        "tl_stress",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "reader",
        5,
        10,
    )
    .await;

    let local_rows = reader
        .query_values_local("SELECT key FROM kv ORDER BY key", &[])
        .expect("local query");
    eprintln!("reader local rows (before fresh): {}", local_rows.len());

    let rows = reader
        .query_values_fresh("SELECT key FROM kv ORDER BY key", &[])
        .await
        .expect("query");

    eprintln!("final rows: {}, expected: {}", rows.len(), total);
    assert_eq!(
        rows.len() as u64,
        total,
        "row count should match total successful writes"
    );

    reader.close().await.expect("close reader");
}
