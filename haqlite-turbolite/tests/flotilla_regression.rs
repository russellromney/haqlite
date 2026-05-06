//! Phase Flotilla regression tests.
//!
//! - `continuous_cold_start_is_fast`: fresh Continuous DB + 24 DDLs < 1000ms.
//! - `close_reopen_survives_writes`: for each durability mode, open → INSERT →
//!   close → reopen → SELECT returns the row.

mod common;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tempfile::TempDir;

use hadb::InMemoryLeaseStore;
use hadb_storage::{CasResult, StorageBackend};
use haqlite::{HaQLite, SqlValue};
use haqlite_turbolite::{Builder, HaMode};
use turbodb_manifest_mem::MemManifestStore;
use turbolite::tiered::{
    CacheConfig, CompressionConfig, SharedTurboliteVfs, TurboliteConfig, TurboliteVfs,
};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT);";

/// 24 DDL statements mimicking redlite's migrate() schema setup.
const MIGRATE_24: &str = r#"
CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS t2 (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS t3 (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS t4 (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS t5 (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS t6 (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS t7 (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS t8 (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS t9 (id INTEGER PRIMARY KEY);
ALTER TABLE t1 ADD COLUMN c1 TEXT;
ALTER TABLE t2 ADD COLUMN c1 TEXT;
ALTER TABLE t3 ADD COLUMN c1 TEXT;
ALTER TABLE t4 ADD COLUMN c1 TEXT;
ALTER TABLE t5 ADD COLUMN c1 TEXT;
ALTER TABLE t6 ADD COLUMN c1 TEXT;
ALTER TABLE t7 ADD COLUMN c1 TEXT;
ALTER TABLE t8 ADD COLUMN c1 TEXT;
ALTER TABLE t9 ADD COLUMN c1 TEXT;
CREATE INDEX IF NOT EXISTS idx_t1 ON t1(c1);
CREATE INDEX IF NOT EXISTS idx_t2 ON t2(c1);
CREATE INDEX IF NOT EXISTS idx_t3 ON t3(c1);
CREATE INDEX IF NOT EXISTS idx_t4 ON t4(c1);
CREATE INDEX IF NOT EXISTS idx_t5 ON t5(c1);
"#;

async fn build_turbolite_dedicated(
    cache_dir: &std::path::Path,
    db_name: &str,
    durability: turbodb::Durability,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<MemManifestStore>,
    instance_id: &str,
    vfs_name: &str,
) -> HaQLite {
    let walrust_storage: Arc<dyn StorageBackend> = Arc::new(common::InMemoryStorage::new());
    build_turbolite_dedicated_with_walrust_storage(
        cache_dir,
        db_name,
        durability,
        lease_store,
        manifest_store,
        instance_id,
        vfs_name,
        walrust_storage,
    )
    .await
}

async fn build_turbolite_dedicated_with_walrust_storage(
    cache_dir: &std::path::Path,
    db_name: &str,
    durability: turbodb::Durability,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<MemManifestStore>,
    instance_id: &str,
    vfs_name: &str,
    walrust_storage: Arc<dyn StorageBackend>,
) -> HaQLite {
    let config = TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        compression: CompressionConfig {
            level: 1,
            ..Default::default()
        },
        cache: CacheConfig {
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("create turbolite VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(vfs_name, shared_vfs.clone())
        .expect("register turbolite VFS");

    let db_path = cache_dir.join(format!("{}.db", db_name));
    Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .durability(durability)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .walrust_storage(walrust_storage)
        .turbolite_vfs(shared_vfs, vfs_name)
        .instance_id(instance_id)
        .manifest_poll_interval(Duration::from_millis(50))
        .open(db_path.to_str().expect("valid path"), SCHEMA)
        .await
        .expect("open turbolite singlewriter")
}

async fn build_walrust_dedicated(
    cache_dir: &std::path::Path,
    db_name: &str,
    durability: hadb::Durability,
    lease_store: Arc<InMemoryLeaseStore>,
    instance_id: &str,
) -> HaQLite {
    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> =
        Arc::new(common::InMemoryStorage::new());
    let db_path = cache_dir.join(format!("{}.db", db_name));
    haqlite::HaQLite::builder()
        .prefix("test/")
        .mode(haqlite::HaMode::SingleWriter)
        .durability(durability)
        .lease_store(lease_store)
        .walrust_storage(walrust_storage)
        .instance_id(instance_id)
        .open(db_path.to_str().expect("valid path"), SCHEMA)
        .await
        .expect("open walrust singlewriter")
}

#[cfg(unix)]
fn open_fd_count() -> usize {
    std::fs::read_dir("/dev/fd").expect("read /dev/fd").count()
}

struct ToggleFailStorage {
    inner: Arc<common::InMemoryStorage>,
    fail_writes: AtomicBool,
}

impl ToggleFailStorage {
    fn new() -> Self {
        Self {
            inner: Arc::new(common::InMemoryStorage::new()),
            fail_writes: AtomicBool::new(false),
        }
    }

    fn fail_writes(&self) {
        self.fail_writes.store(true, Ordering::SeqCst);
    }

    fn allow_writes(&self) {
        self.fail_writes.store(false, Ordering::SeqCst);
    }

    fn maybe_fail(&self) -> anyhow::Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
            anyhow::bail!("forced walrust storage write failure");
        }
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for ToggleFailStorage {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.maybe_fail()?;
        self.inner.put(key, data).await
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

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> anyhow::Result<CasResult> {
        self.maybe_fail()?;
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> anyhow::Result<CasResult> {
        self.maybe_fail()?;
        self.inner.put_if_match(key, data, etag).await
    }
}

// ============================================================================
// close_reopen_survives_writes
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn close_reopen_checkpoint_survives() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_ckpt_{}", std::process::id());

    let mut db = build_turbolite_dedicated(
        tmp.path(),
        "ckpt",
        turbodb::Durability::Checkpoint(turbodb::CheckpointConfig::default()),
        lease.clone(),
        manifest.clone(),
        "node-1",
        &vfs,
    )
    .await;

    db.execute_async("INSERT INTO t (id, val) VALUES (1, 'checkpoint')", &[])
        .await
        .unwrap();
    {
        let conn = db.connection().expect("get conn");
        let c = conn.lock();
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint");
    }
    db.close().await.unwrap();

    let mut db2 = build_turbolite_dedicated(
        tmp.path(),
        "ckpt",
        turbodb::Durability::Checkpoint(turbodb::CheckpointConfig::default()),
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;
    let val: String = db2
        .query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "checkpoint");
    db2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn close_reopen_continuous_survives() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_cont_{}", std::process::id());

    let mut db = build_turbolite_dedicated(
        tmp.path(),
        "cont",
        turbodb::Durability::default(),
        lease.clone(),
        manifest.clone(),
        "node-1",
        &vfs,
    )
    .await;

    db.execute_async("INSERT INTO t (id, val) VALUES (1, 'continuous')", &[])
        .await
        .unwrap();
    let execute_checkpoint_err = db
        .execute_async("PRAGMA wal_checkpoint(TRUNCATE)", &[])
        .await
        .expect_err("continuous mode rejects user wal_checkpoint through execute_async");
    assert!(
        execute_checkpoint_err
            .to_string()
            .contains("not authorized")
            || execute_checkpoint_err
                .to_string()
                .contains("execute failed"),
        "unexpected checkpoint error: {execute_checkpoint_err}"
    );
    let execute_autocheckpoint_err = db
        .execute_async("PRAGMA wal_autocheckpoint=1", &[])
        .await
        .expect_err("continuous mode rejects user wal_autocheckpoint changes");
    assert!(
        execute_autocheckpoint_err
            .to_string()
            .contains("not authorized")
            || execute_autocheckpoint_err
                .to_string()
                .contains("execute failed"),
        "unexpected autocheckpoint error: {execute_autocheckpoint_err}"
    );
    let execute_journal_mode_err = db
        .execute_async("PRAGMA journal_mode=DELETE", &[])
        .await
        .expect_err("continuous mode rejects user journal mode changes");
    assert!(
        execute_journal_mode_err
            .to_string()
            .contains("not authorized")
            || execute_journal_mode_err
                .to_string()
                .contains("execute failed"),
        "unexpected journal_mode error: {execute_journal_mode_err}"
    );
    {
        let conn = db.connection().expect("get conn");
        let c = conn.lock();
        let err = c
            .execute_batch("/* user sql */ PrAgMa wal_checkpoint(TRUNCATE);")
            .expect_err("continuous mode rejects raw connection wal_checkpoint");
        assert!(
            err.to_string().contains("not authorized"),
            "unexpected raw checkpoint error: {err}"
        );
    }
    db.close().await.unwrap();

    let mut db2 = build_turbolite_dedicated(
        tmp.path(),
        "cont",
        turbodb::Durability::default(),
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;
    let val: String = db2
        .query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "continuous");
    db2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_reader_does_not_block_writer_commit() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_reader_writer_{}", uuid::Uuid::new_v4());

    let mut db = build_turbolite_dedicated(
        tmp.path(),
        "rw",
        turbodb::Durability::default(),
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;

    db.execute_async("INSERT INTO t (id, val) VALUES (1, 'before')", &[])
        .await
        .unwrap();

    let db_path = tmp.path().join("rw.db");
    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        &vfs,
    )
    .expect("open independent reader connection");
    reader.execute_batch("BEGIN;").expect("begin reader txn");
    let snapshot_count: i64 = reader
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .expect("reader snapshot query");
    assert_eq!(snapshot_count, 1);

    tokio::time::timeout(
        Duration::from_secs(2),
        db.execute_async("INSERT INTO t (id, val) VALUES (2, 'during-reader')", &[]),
    )
    .await
    .expect("writer commit should not block behind reader")
    .expect("writer insert succeeds while reader txn is open");

    let still_snapshot_count: i64 = reader
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .expect("reader still sees its snapshot");
    assert_eq!(still_snapshot_count, 1);
    reader.execute_batch("COMMIT;").expect("reader commit");

    let rows = db
        .query_values_fresh("SELECT COUNT(*) FROM t", &[])
        .await
        .expect("fresh count after writer");
    assert_eq!(rows[0][0].as_integer(), Some(2));

    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_close_can_retry_after_failed_final_sync() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_close_retry_{}", uuid::Uuid::new_v4());
    let storage = Arc::new(ToggleFailStorage::new());
    let walrust_storage: Arc<dyn StorageBackend> = storage.clone();

    let mut db = build_turbolite_dedicated_with_walrust_storage(
        tmp.path(),
        "close-retry",
        turbodb::Durability::default(),
        lease.clone(),
        manifest.clone(),
        "node-1",
        &vfs,
        walrust_storage,
    )
    .await;

    db.execute_async("INSERT INTO t (id, val) VALUES (1, 'durable')", &[])
        .await
        .expect("initial synced write succeeds");

    storage.fail_writes();
    let write_err = db
        .execute_async("INSERT INTO t (id, val) VALUES (2, 'pending-sync')", &[])
        .await
        .expect_err("write with failed sync is not acknowledged");
    assert!(
        write_err.to_string().contains("sync/publish failed")
            || write_err
                .to_string()
                .contains("forced walrust storage write failure"),
        "unexpected write error: {write_err}"
    );

    let close_err = db
        .close()
        .await
        .expect_err("close refuses to release while final sync fails");
    assert!(
        close_err.to_string().contains("sync/publish failed")
            || close_err
                .to_string()
                .contains("forced walrust storage write failure"),
        "unexpected close error: {close_err}"
    );

    storage.allow_writes();
    db.close()
        .await
        .expect("close retries final sync and then releases");

    let mut db2 = build_turbolite_dedicated(
        tmp.path(),
        "close-retry",
        turbodb::Durability::default(),
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;
    let rows = db2
        .query_values_fresh("SELECT id FROM t ORDER BY id", &[])
        .await
        .expect("fresh rows after retry close");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_integer(), Some(1));
    assert_eq!(rows[1][0].as_integer(), Some(2));
    db2.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn repeated_continuous_open_close_releases_file_descriptors() {
    assert_repeated_continuous_open_close_fd_bounds(3).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Gate D stress audit; run explicitly when proving close/release behavior"]
async fn stress_repeated_continuous_open_close_releases_file_descriptors() {
    assert_repeated_continuous_open_close_fd_bounds(12).await;
}

#[cfg(unix)]
async fn assert_repeated_continuous_open_close_fd_bounds(cycles: i64) {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> =
        Arc::new(common::InMemoryStorage::new());

    let config = TurboliteConfig {
        cache_dir: tmp.path().join("cache"),
        compression: CompressionConfig {
            level: 1,
            ..Default::default()
        },
        cache: CacheConfig {
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let shared_vfs = SharedTurboliteVfs::new(TurboliteVfs::new_local(config).expect("vfs"));
    let vfs_name = format!("flotilla_fd_audit_{}_{}", cycles, uuid::Uuid::new_v4());
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register VFS");

    let db_path = tmp.path().join("fd-audit.db");
    let baseline = open_fd_count();
    let mut peak = baseline;

    for i in 0..cycles {
        let mut db = Builder::new()
            .prefix("test/")
            .mode(HaMode::SingleWriter)
            .durability(turbodb::Durability::default())
            .lease_store(lease.clone())
            .manifest_store(manifest.clone())
            .walrust_storage(walrust_storage.clone())
            .turbolite_vfs(shared_vfs.clone(), &vfs_name)
            .instance_id("fd-audit-node")
            .manifest_poll_interval(Duration::from_millis(50))
            .open(db_path.to_str().expect("path"), SCHEMA)
            .await
            .expect("open continuous db");

        db.execute_async(
            "INSERT OR REPLACE INTO t (id, val) VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("v{i}"))],
        )
        .await
        .expect("write");
        db.close().await.expect("close");

        let current = open_fd_count();
        peak = peak.max(current);
    }

    let final_count = open_fd_count();
    assert!(
        final_count <= baseline + 4,
        "fd count grew after repeated open/write/close: baseline={baseline}, final={final_count}, peak={peak}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn close_reopen_cloud_survives() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_cloud_{}", std::process::id());

    let mut db = build_turbolite_dedicated(
        tmp.path(),
        "cloud",
        turbodb::Durability::Cloud,
        lease.clone(),
        manifest.clone(),
        "node-1",
        &vfs,
    )
    .await;

    db.execute_async("INSERT INTO t (id, val) VALUES (1, 'cloud')", &[])
        .await
        .unwrap();
    db.close().await.unwrap();

    let mut db2 = build_turbolite_dedicated(
        tmp.path(),
        "cloud",
        turbodb::Durability::Cloud,
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;
    let val: String = db2
        .query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "cloud");
    db2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn close_reopen_walrust_replicated_survives() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());

    let mut db = build_walrust_dedicated(
        tmp.path(),
        "walrust",
        hadb::Durability::Replicated(Duration::from_secs(1)),
        lease.clone(),
        "node-1",
    )
    .await;

    db.execute_async("INSERT INTO t (id, val) VALUES (1, 'walrust')", &[])
        .await
        .unwrap();
    db.close().await.unwrap();

    let mut db2 = build_walrust_dedicated(
        tmp.path(),
        "walrust",
        hadb::Durability::Replicated(Duration::from_secs(1)),
        lease,
        "node-1",
    )
    .await;
    let val: String = db2
        .query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "walrust");
    db2.close().await.unwrap();
}

// ============================================================================
// continuous_cold_start_is_fast
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn continuous_cold_start_is_fast() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_fast_{}", std::process::id());

    let start = Instant::now();
    let mut db = build_turbolite_dedicated(
        tmp.path(),
        "fast",
        turbodb::Durability::default(),
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;

    // Apply 24 DDL statements mimicking redlite migrate().
    {
        let conn = db.connection().expect("get conn");
        let c = conn.lock();
        c.execute_batch(MIGRATE_24).expect("migrate batch");
    }
    let elapsed = start.elapsed();
    db.close().await.unwrap();

    let threshold = if cfg!(debug_assertions) {
        Duration::from_millis(2500)
    } else {
        Duration::from_millis(1000)
    };
    assert!(
        elapsed < threshold,
        "cold start + 24 DDLs took {:?}, expected < {:?}",
        elapsed,
        threshold
    );
}

// ============================================================================
// journal_mode matches durability contract
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn continuous_journal_mode_is_wal() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_jm_cont_{}", std::process::id());

    let mut db = build_turbolite_dedicated(
        tmp.path(),
        "jm_cont",
        turbodb::Durability::default(),
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;

    let conn = db.connection().expect("get conn");
    let c = conn.lock();
    let mode: String = c
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .expect("pragma");
    assert_eq!(
        mode.to_lowercase(),
        "wal",
        "Continuous durability must use WAL journal mode (got {})",
        mode
    );
    drop(c);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_journal_mode_is_delete() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_jm_ckpt_{}", std::process::id());

    let mut db = build_turbolite_dedicated(
        tmp.path(),
        "jm_ckpt",
        turbodb::Durability::Checkpoint(turbodb::CheckpointConfig::default()),
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;

    let conn = db.connection().expect("get conn");
    let c = conn.lock();
    let mode: String = c
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .expect("pragma");
    assert_eq!(
        mode.to_lowercase(),
        "delete",
        "Checkpoint durability must use rollback journal mode so main-db pages are the checkpointed state (got {})",
        mode
    );
    drop(c);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cloud_journal_mode_is_delete() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_jm_cloud_{}", std::process::id());

    let mut db = build_turbolite_dedicated(
        tmp.path(),
        "jm_cloud",
        turbodb::Durability::Cloud,
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;

    let conn = db.connection().expect("get conn");
    let c = conn.lock();
    let mode: String = c
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .expect("pragma");
    assert_eq!(
        mode.to_lowercase(),
        "delete",
        "Cloud durability must use DELETE journal mode (got {})",
        mode
    );
    drop(c);
    db.close().await.unwrap();
}

// ============================================================================
// Write latency: Continuous must be fast (<100ms per INSERT)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn continuous_write_latency_is_fast() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs = format!("flotilla_lat_{}", std::process::id());

    let mut db = build_turbolite_dedicated(
        tmp.path(),
        "lat",
        turbodb::Durability::default(),
        lease,
        manifest,
        "node-1",
        &vfs,
    )
    .await;

    // Warm-up write
    db.execute_async("INSERT INTO t (id, val) VALUES (0, 'warm')", &[])
        .await
        .unwrap();

    let start = Instant::now();
    for i in 1..=10 {
        db.execute_async(
            "INSERT INTO t (id, val) VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("v{}", i))],
        )
        .await
        .unwrap();
    }
    let elapsed = start.elapsed();
    db.close().await.unwrap();

    let threshold = Duration::from_millis(500);
    assert!(
        elapsed < threshold,
        "10 INSERTs in Continuous mode took {:?}, expected < {:?} \
         (if >5s, xSync-per-commit is still firing = journal_mode bug)",
        elapsed,
        threshold
    );
}

// ============================================================================
// SyncReplicated close/reopen survives
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn close_reopen_syncreplicated_survives() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());

    let mut db = build_walrust_dedicated(
        tmp.path(),
        "syncrep",
        hadb::Durability::SyncReplicated,
        lease.clone(),
        "node-1",
    )
    .await;

    db.execute_async("INSERT INTO t (id, val) VALUES (1, 'syncrep')", &[])
        .await
        .unwrap();
    db.close().await.unwrap();

    let mut db2 = build_walrust_dedicated(
        tmp.path(),
        "syncrep",
        hadb::Durability::SyncReplicated,
        lease,
        "node-1",
    )
    .await;
    let val: String = db2
        .query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "syncrep");
    db2.close().await.unwrap();
}

// ============================================================================
// F3 regression: Cloud mode opens without walrust_storage (NoOpReplicator path)
//
// Pre-F3, building Cloud mode without walrust_storage errored at open() with
// "Writer mode requires walrust storage". Post-F3, Cloud uses NoOpReplicator
// internally, so walrust_storage is genuinely optional. We assert open()
// success only — the schema-through-turbolite-VFS persistence path has a
// separate pre-existing bug that affects every Cloud/Continuous/Checkpoint
// flotilla_regression test (ensure_schema opens via plain rusqlite, not the
// registered VFS, so the schema isn't visible to the VFS-backed connection).
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cloud_opens_without_walrust_storage() {
    use haqlite_turbolite::{Builder, HaMode};
    use turbolite::tiered::{CacheConfig, TurboliteConfig, TurboliteVfs};

    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let vfs_name = format!("flotilla_no_walrust_{}", std::process::id());

    let tl_config = TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        cache: CacheConfig {
            gc_enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(tl_config).expect("create VFS");
    let shared_vfs = turbolite::tiered::SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register VFS");

    let db_path = tmp.path().join("no_walrust.db");

    let mut db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .durability(turbodb::Durability::Cloud)
        .lease_store(lease)
        .manifest_store(manifest)
        .turbolite_vfs(shared_vfs, &vfs_name)
        .instance_id("node-1")
        .open(db_path.to_str().expect("utf8 path"), "")
        .await
        .expect("Cloud + no walrust should open cleanly");

    db.close().await.unwrap();
}
