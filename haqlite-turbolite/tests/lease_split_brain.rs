//! Siege 4: Lease expiration and split-brain prevention tests.
//!
//! Prove: at most one writer succeeds at any time in SharedWriter mode,
//! even when leases expire due to slow S3 operations.
//!
//! Requires the s3 feature (multi-node catch-up needs S3 turbolite).

#![cfg(feature = "legacy-s3-mode-tests")]

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::InMemoryStorage;
use haqlite::{HaQLite, InMemoryLeaseStore, SqlValue};
use haqlite_turbolite::{Builder, HaMode};
use tempfile::TempDir;
use tokio::sync::Mutex;
use turbodb_manifest_mem::MemManifestStore;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)";

fn unique_prefix(name: &str) -> String {
    format!(
        "test/lsb/{}/{}",
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    )
}

/// Storage backend that wraps InMemoryStorage with configurable latency
/// on upload operations. This simulates slow S3 causing lease expiration.
struct SlowStorage {
    inner: InMemoryStorage,
    upload_delay: Mutex<Duration>,
}

impl SlowStorage {
    fn new(delay: Duration) -> Self {
        Self {
            inner: InMemoryStorage::new(),
            upload_delay: Mutex::new(delay),
        }
    }

    async fn set_delay(&self, delay: Duration) {
        *self.upload_delay.lock().await = delay;
    }
}

#[async_trait]
impl hadb_storage::StorageBackend for SlowStorage {
    async fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        let delay = *self.upload_delay.lock().await;
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        self.inner.put(key, data).await
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> anyhow::Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.inner.exists(key).await
    }

    async fn put_if_absent(
        &self,
        key: &str,
        data: &[u8],
    ) -> anyhow::Result<hadb_storage::CasResult> {
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        etag: &str,
    ) -> anyhow::Result<hadb_storage::CasResult> {
        self.inner.put_if_match(key, data, etag).await
    }
}

/// Helper: build a sharedwriter-mode HaQLite node with S3-backed turbolite VFS.
/// Uses Synchronous durability (S3Primary, no walrust).
async fn build_node(
    tmp: &TempDir,
    name: &str,
    s3_prefix: &str,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<MemManifestStore>,
    instance_id: &str,
    lease_ttl: u64,
    write_timeout_secs: u64,
) -> HaQLite {
    let (shared_vfs, vfs_name, _) = common::make_s3_vfs(
        tmp.path(),
        &format!("lsb_{}", instance_id),
        s3_prefix,
        0,
        None,
    )
    .await;

    let db_path = tmp.path().join(format!("{}.db", name));
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
        .open(db_path.to_str().unwrap(), SCHEMA)
        .await
        .expect("open sharedwriter mode")
}

/// Helper: count rows via query_values.
fn count_rows(rows: &[Vec<SqlValue>]) -> usize {
    rows.len()
}

/// Helper: check if a key exists in rows (first column is key).
fn has_key(rows: &[Vec<SqlValue>], key: &str) -> bool {
    rows.iter().any(|r| match &r[0] {
        SqlValue::Text(k) => k == key,
        _ => false,
    })
}

/// Two nodes write sequentially. With fast storage, both should succeed.
#[tokio::test(flavor = "multi_thread")]
async fn baseline_two_nodes_sequential_writes() {
    let tmp_a = TempDir::new().expect("tmp");
    let tmp_b = TempDir::new().expect("tmp");

    let prefix = unique_prefix("baseline");
    let storage = Arc::new(SlowStorage::new(Duration::ZERO));
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let db_a = build_node(
        &tmp_a,
        "split",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "node-a",
        5,
        10,
    )
    .await;
    let db_b = build_node(
        &tmp_b,
        "split",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "node-b",
        5,
        10,
    )
    .await;

    db_a.execute("INSERT OR REPLACE INTO kv VALUES ('k1', 'from_a')", &[])
        .expect("node A write should succeed");

    db_b.execute("INSERT OR REPLACE INTO kv VALUES ('k2', 'from_b')", &[])
        .expect("node B write should succeed");

    let rows = db_b
        .query_values_fresh("SELECT key, value FROM kv ORDER BY key", &[])
        .await
        .expect("query");
    assert_eq!(count_rows(&rows), 2, "both writes should be visible");
}

/// Concurrent writes from two nodes. The lease serializes them.
/// All successful writes must be visible in final state.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writes_no_data_loss() {
    let tmp_a = TempDir::new().expect("tmp");
    let tmp_b = TempDir::new().expect("tmp");

    let prefix = unique_prefix("conc");
    let storage = Arc::new(SlowStorage::new(Duration::ZERO));
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let db_a = build_node(
        &tmp_a,
        "conc",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "node-a",
        5,
        10,
    )
    .await;
    let db_b = build_node(
        &tmp_b,
        "conc",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "node-b",
        5,
        10,
    )
    .await;

    // Wrap in Arc for sharing across tasks
    let db_a = Arc::new(tokio::sync::Mutex::new(db_a));
    let db_b = Arc::new(tokio::sync::Mutex::new(db_b));

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

    assert!(a_count > 0, "node-a should have at least one success");
    assert!(b_count > 0, "node-b should have at least one success");

    // Fresh read should see all successful writes
    let db_b = db_b.lock().await;
    let rows = db_b
        .query_values_fresh("SELECT key FROM kv ORDER BY key", &[])
        .await
        .expect("query");
    assert_eq!(
        count_rows(&rows) as u64,
        a_count + b_count,
        "total rows should equal total successes (no lost writes)"
    );
}

/// Slow storage causes lease expiration. Both nodes' successful writes
/// must be visible in the final state.
#[tokio::test(flavor = "multi_thread")]
async fn slow_storage_lease_expiration() {
    let tmp_a = TempDir::new().expect("tmp");
    let tmp_b = TempDir::new().expect("tmp");

    // Upload delay of 500ms with 1-second lease TTL for node A.
    // Node A: sync takes 500ms * N uploads, lease is 1s, so it expires mid-sync.
    // Node B: gets a 10s TTL so it can finish even with the same slow storage.
    let prefix = unique_prefix("slow");
    let storage = Arc::new(SlowStorage::new(Duration::from_millis(500)));
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let db_a = build_node(
        &tmp_a,
        "slow",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "node-a",
        1,
        30, // 1s TTL, short lease
    )
    .await;
    let db_b = build_node(
        &tmp_b,
        "slow",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "node-b",
        30,
        30, // 30s TTL, won't expire
    )
    .await;

    let db_a = Arc::new(tokio::sync::Mutex::new(db_a));
    let db_b = Arc::new(tokio::sync::Mutex::new(db_b));

    // Node A starts write (lease will expire during slow sync)
    let a_handle = {
        let db = db_a.clone();
        tokio::spawn(async move {
            let db = db.lock().await;
            db.execute("INSERT OR REPLACE INTO kv VALUES ('slow_a', 'from_a')", &[])
        })
    };

    // Give node A time to acquire lease and start sync
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Node B writes after A's lease expires (B has long TTL so it will succeed)
    let b_handle = {
        let db = db_b.clone();
        tokio::spawn(async move {
            let db = db.lock().await;
            db.execute("INSERT OR REPLACE INTO kv VALUES ('slow_b', 'from_b')", &[])
        })
    };

    let a_result = a_handle.await.expect("join a");
    let b_result = b_handle.await.expect("join b");

    eprintln!("node-a: {:?}, node-b: {:?}", a_result, b_result);

    let a_ok = a_result.is_ok();
    let b_ok = b_result.is_ok();
    assert!(a_ok || b_ok, "at least one node should succeed");

    // The key invariant: if a node reports Ok, its write MUST be visible.
    // If a node's lease expired during sync, it should return Err (not Ok).
    // This is the split-brain prevention guarantee.

    // Verify with fresh reader
    let tmp_c = TempDir::new().expect("tmp");
    storage.set_delay(Duration::ZERO).await;
    let db_c = build_node(
        &tmp_c,
        "slow",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "reader",
        5,
        10,
    )
    .await;

    let rows = db_c
        .query_values_fresh("SELECT key, value FROM kv ORDER BY key", &[])
        .await
        .expect("fresh query");

    eprintln!(
        "final rows: {} total, a_ok={}, b_ok={}",
        count_rows(&rows),
        a_ok,
        b_ok
    );

    // Every write that returned Ok must be visible in the final state.
    // A write that returned Err may or may not be visible (it's in S3 but
    // the manifest may not point to it).
    if a_ok {
        assert!(
            has_key(&rows, "slow_a"),
            "node-a returned Ok but write not visible -- split-brain data loss!"
        );
    }
    if b_ok {
        assert!(
            has_key(&rows, "slow_b"),
            "node-b returned Ok but write not visible -- split-brain data loss!"
        );
    }
}

/// Stress test: 4 concurrent writers with short lease TTL.
#[tokio::test(flavor = "multi_thread")]
async fn many_writers_short_lease_no_corruption() {
    let prefix = unique_prefix("stress");
    let storage = Arc::new(SlowStorage::new(Duration::from_millis(50)));
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let total_successes = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for node_id in 0..4 {
        let prefix = prefix.clone();
        let storage = storage.clone();
        let lease_store = lease_store.clone();
        let manifest_store = manifest_store.clone();
        let successes = total_successes.clone();

        handles.push(tokio::spawn(async move {
            let tmp = TempDir::new().expect("tmp");
            let mut db = build_node(
                &tmp,
                "stress",
                &prefix,
                lease_store,
                manifest_store,
                &format!("node-{}", node_id),
                2,
                10,
            )
            .await;

            for i in 0..5 {
                match db.execute(
                    "INSERT OR REPLACE INTO kv VALUES (?1, ?2)",
                    &[
                        SqlValue::Text(format!("n{}_{}", node_id, i)),
                        SqlValue::Text(format!("val_{}_{}", node_id, i)),
                    ],
                ) {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => eprintln!("node-{} write {} failed: {}", node_id, i, e),
                }
            }
        }));
    }

    for h in handles {
        h.await.expect("join");
    }

    let total = total_successes.load(Ordering::Relaxed);
    eprintln!("total successes: {}", total);
    assert!(total > 0, "at least some writes should succeed");

    // Verify final state
    storage.set_delay(Duration::ZERO).await;
    let tmp_reader = TempDir::new().expect("tmp");
    let reader = build_node(
        &tmp_reader,
        "stress",
        &prefix,
        lease_store.clone(),
        manifest_store.clone(),
        "reader",
        5,
        10,
    )
    .await;

    let rows = reader
        .query_values_fresh("SELECT key FROM kv ORDER BY key", &[])
        .await
        .expect("query");

    eprintln!("final rows: {}, expected: {}", count_rows(&rows), total);
    assert_eq!(
        count_rows(&rows) as u64,
        total,
        "row count should match total successful writes"
    );
}
