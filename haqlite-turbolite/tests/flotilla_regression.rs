//! Phase Flotilla regression tests.
//!
//! - `continuous_cold_start_is_fast`: fresh Continuous DB + 24 DDLs < 1000ms.
//! - `close_reopen_survives_writes`: for each durability mode, open → INSERT →
//!   close → reopen → SELECT returns the row.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use hadb::InMemoryLeaseStore;
use haqlite::{HaQLite, SqlValue};
use haqlite_turbolite::{Builder, Mode};
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

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> =
        Arc::new(common::InMemoryStorage::new());
    let db_path = cache_dir.join(format!("{}.db", db_name));
    Builder::new()
        .prefix("test/")
        .mode(Mode::SingleWriter)
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
    {
        let conn = db.connection().expect("get conn");
        let c = conn.lock();
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint");
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
async fn checkpoint_journal_mode_is_wal() {
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
        "wal",
        "Checkpoint durability must use WAL journal mode (got {})",
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
    use haqlite_turbolite::{Builder, Mode};
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
        .mode(Mode::SingleWriter)
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
