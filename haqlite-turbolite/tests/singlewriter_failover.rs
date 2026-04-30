mod common;

use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use common::InMemoryStorage;
use hadb::InMemoryLeaseStore;
use haqlite::{HaQLite, Role, SqlValue};
use haqlite_turbolite::{Builder, HaMode};
use tempfile::TempDir;
use turbodb::{Manifest, ManifestStore};
use turbodb_manifest_mem::MemManifestStore;
use turbolite::tiered::{CacheConfig, SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT NOT NULL);";

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);
static FAILOVER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn unique_vfs(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("singlewriter_failover_{}_{}", prefix, n)
}

fn make_remote_vfs(
    cache_dir: &Path,
    storage: Arc<dyn hadb_storage::StorageBackend>,
) -> (SharedTurboliteVfs, String) {
    let vfs_name = unique_vfs("node");
    let config = TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        cache: CacheConfig {
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let vfs = TurboliteVfs::with_backend(config, storage, tokio::runtime::Handle::current())
        .expect("create remote turbolite VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register VFS");
    (shared_vfs, vfs_name)
}

struct BuiltNode {
    db: HaQLite,
    vfs: SharedTurboliteVfs,
    vfs_name: String,
}

async fn build_singlewriter_node(
    cache_dir: &Path,
    db_name: &str,
    durability: turbodb::Durability,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<dyn ManifestStore>,
    tiered_storage: Arc<dyn hadb_storage::StorageBackend>,
    walrust_storage: Arc<dyn hadb_storage::StorageBackend>,
    instance_id: &str,
) -> BuiltNode {
    let (shared_vfs, vfs_name) = make_remote_vfs(cache_dir, tiered_storage);
    let db_path = cache_dir.join(format!("{}.db", db_name));

    let db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .durability(durability)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .walrust_storage(walrust_storage)
        .turbolite_vfs(shared_vfs.clone(), &vfs_name)
        .instance_id(instance_id)
        .manifest_poll_interval(Duration::from_millis(50))
        .disable_forwarding()
        .open(db_path.to_str().expect("valid path"), SCHEMA)
        .await
        .expect("open haqlite-turbolite singlewriter node");
    BuiltNode {
        db,
        vfs: shared_vfs,
        vfs_name,
    }
}

async fn dump_replication_state(
    manifest_store: &Arc<dyn ManifestStore>,
    walrust_storage: &Arc<InMemoryStorage>,
) -> String {
    let manifest: Option<Manifest> = manifest_store
        .get("test/failover/_manifest")
        .await
        .expect("manifest get");
    let wal_keys = walrust_storage.keys().await;
    match manifest {
        Some(m) => {
            let decoded = turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&m.payload)
                .expect("decode manifest payload");
            format!(
                "manifest.version={} change_counter={} walrust={:?} wal_keys={:?}",
                decoded.0.version, decoded.0.change_counter, decoded.1, wal_keys
            )
        }
        None => format!("manifest=<none> wal_keys={:?}", wal_keys),
    }
}

fn local_vfs_state(vfs: &SharedTurboliteVfs) -> String {
    let manifest = vfs.manifest();
    format!(
        "local_vfs(version={}, page_count={}, page_size={}, groups={}, group_pages={:?}, keys={:?})",
        manifest.version,
        manifest.page_count,
        manifest.page_size,
        manifest.page_group_keys.len(),
        manifest.group_pages,
        manifest.page_group_keys
    )
}

fn raw_cache_snapshot(cache_path: &Path) -> String {
    match rusqlite::Connection::open_with_flags(
        cache_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(conn) => {
            let count: Result<i64, _> =
                conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0));
            let rows: Result<Vec<(i64, String)>, _> = (|| {
                let mut stmt = conn.prepare("SELECT id, val FROM t ORDER BY id")?;
                let mapped = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
                mapped.collect()
            })();
            format!("raw_cache(count={:?}, rows={:?})", count.ok(), rows.ok())
        }
        Err(e) => format!("raw_cache(open_error={e})"),
    }
}

fn dir_snapshot(dir: &Path) -> String {
    let mut entries = Vec::new();
    match std::fs::read_dir(dir) {
        Ok(read_dir) => {
            for entry in read_dir.flatten() {
                let path = entry.path();
                let name = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("<invalid>")
                    .to_string();
                let len = entry.metadata().map(|m| m.len()).unwrap_or(0);
                entries.push(format!("{name}:{len}"));
            }
            entries.sort();
            format!("files=[{}]", entries.join(","))
        }
        Err(e) => format!("files_error={e}"),
    }
}

fn vfs_snapshot(cache_dir: &Path, db_name: &str, vfs_name: &str) -> String {
    let uri = format!("file:{db_name}.db?vfs={vfs_name}");
    match rusqlite::Connection::open_with_flags(
        &uri,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    ) {
        Ok(conn) => {
            let page_count: Result<i64, _> =
                conn.query_row("PRAGMA page_count", [], |row| row.get(0));
            let journal_mode: Result<String, _> =
                conn.query_row("PRAGMA journal_mode", [], |row| row.get(0));
            let schema: Result<Vec<(String, String)>, _> = (|| {
                let mut stmt = conn.prepare("SELECT name, sql FROM sqlite_master ORDER BY name")?;
                let mapped = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
                mapped.collect()
            })();
            let count: Result<i64, _> =
                conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0));
            let rows: Result<Vec<(i64, String)>, _> = (|| {
                let mut stmt = conn.prepare("SELECT id, val FROM t ORDER BY id")?;
                let mapped = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
                mapped.collect()
            })();
            format!(
                "vfs(cache_dir={}, page_count={:?}, journal={:?}, schema={:?}, count={:?}, rows={:?})",
                cache_dir.display(),
                page_count.map_err(|e| e.to_string()),
                journal_mode.map_err(|e| e.to_string()),
                schema.map_err(|e| e.to_string()),
                count.map_err(|e| e.to_string()),
                rows.map_err(|e| e.to_string())
            )
        }
        Err(e) => format!("vfs(open_error={e})"),
    }
}

async fn wait_for_role(db: &HaQLite, expected: Role, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if db.role() == Some(expected) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(anyhow!(
        "timed out waiting for role {:?}, current role {:?}",
        expected,
        db.role()
    ))
}

async fn wait_for_count(db: &HaQLite, expected: i64, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_err: Option<String> = None;
    while Instant::now() < deadline {
        match db.query_values_fresh("SELECT COUNT(*) FROM t", &[]).await {
            Ok(rows) => {
                last_err = None;
                if let Some(row) = rows.first() {
                    if let Some(SqlValue::Integer(count)) = row.first() {
                        if *count == expected {
                            return Ok(());
                        }
                    }
                }
            }
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(anyhow!(
        "timed out waiting for row count {expected}; last error: {}",
        last_err.unwrap_or_else(|| "<none>".to_string())
    ))
}

async fn wait_for_value(db: &HaQLite, id: i64, expected: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_err: Option<String> = None;
    while Instant::now() < deadline {
        match db
            .query_values_fresh("SELECT val FROM t WHERE id = ?1", &[SqlValue::Integer(id)])
            .await
        {
            Ok(rows) => {
                last_err = None;
                if let Some(row) = rows.first() {
                    if let Some(SqlValue::Text(val)) = row.first() {
                        if val == expected {
                            return Ok(());
                        }
                    }
                }
            }
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(anyhow!(
        "timed out waiting for row {} to equal {:?}; last error: {}",
        id,
        expected,
        last_err.unwrap_or_else(|| "<none>".to_string())
    ))
}

async fn run_singlewriter_failover(durability: turbodb::Durability) -> Result<()> {
    let leader_tmp = TempDir::new().expect("leader tmp");
    let follower_tmp = TempDir::new().expect("follower tmp");
    let follower_cache_path = follower_tmp.path().join("data.cache");
    let follower_db_path = follower_tmp.path().join("failover.db");

    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store_impl = Arc::new(MemManifestStore::new());
    let manifest_store: Arc<dyn ManifestStore> = manifest_store_impl.clone();
    let tiered_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let walrust_storage_impl = Arc::new(InMemoryStorage::new());
    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = walrust_storage_impl.clone();

    let leader = build_singlewriter_node(
        leader_tmp.path(),
        "failover",
        durability,
        lease_store.clone(),
        manifest_store.clone(),
        tiered_storage.clone(),
        walrust_storage.clone(),
        "node-leader",
    )
    .await;
    wait_for_role(&leader.db, Role::Leader, Duration::from_secs(3)).await?;

    let follower = build_singlewriter_node(
        follower_tmp.path(),
        "failover",
        durability,
        lease_store,
        manifest_store.clone(),
        tiered_storage,
        walrust_storage,
        "node-follower",
    )
    .await;
    wait_for_role(&follower.db, Role::Follower, Duration::from_secs(3)).await?;

    leader
        .db
        .execute_async(
            "INSERT INTO t (id, val) VALUES (?1, ?2)",
            &[
                SqlValue::Integer(1),
                SqlValue::Text("before-crash".to_string()),
            ],
        )
        .await
        .map_err(|e| anyhow!("leader write before crash: {e}"))?;

    if let Err(e) = wait_for_count(&follower.db, 1, Duration::from_secs(5)).await {
        let state_after_timeout =
            dump_replication_state(&manifest_store, &walrust_storage_impl).await;
        let raw_cache_after_timeout = raw_cache_snapshot(&follower_cache_path);
        let raw_db_after_timeout = raw_cache_snapshot(&follower_db_path);
        let files_after_timeout = dir_snapshot(follower_tmp.path());
        let vfs_after_timeout = vfs_snapshot(follower_tmp.path(), "failover", &follower.vfs_name);
        let local_vfs_after_timeout = local_vfs_state(&follower.vfs);
        return Err(anyhow!(
            "{}; state after catch-up timeout: {}; {}; {}; {}; {}; {}",
            e,
            state_after_timeout,
            local_vfs_after_timeout,
            raw_cache_after_timeout,
            raw_db_after_timeout,
            files_after_timeout,
            vfs_after_timeout
        ));
    }
    if let Err(e) = wait_for_value(&follower.db, 1, "before-crash", Duration::from_secs(5)).await {
        let state_after_timeout =
            dump_replication_state(&manifest_store, &walrust_storage_impl).await;
        return Err(anyhow!(
            "{}; state after value catch-up timeout: {}",
            e,
            state_after_timeout
        ));
    }

    leader
        .db
        .coordinator()
        .ok_or_else(|| anyhow!("leader missing coordinator"))?
        .abort_tasks_for_test()
        .await;

    wait_for_role(&follower.db, Role::Leader, Duration::from_secs(12)).await?;
    let state_after_promotion =
        dump_replication_state(&manifest_store, &walrust_storage_impl).await;
    wait_for_value(&follower.db, 1, "before-crash", Duration::from_secs(5))
        .await
        .map_err(|e| {
            let raw_cache_after_promotion = raw_cache_snapshot(&follower_cache_path);
            let raw_db_after_promotion = raw_cache_snapshot(&follower_db_path);
            let files_after_promotion = dir_snapshot(follower_tmp.path());
            let vfs_after_promotion =
                vfs_snapshot(follower_tmp.path(), "failover", &follower.vfs_name);
            let local_vfs_after_promotion = local_vfs_state(&follower.vfs);
            anyhow!(
                "{}; state after promotion: {}; {}; {}; {}; {}; {}",
                e,
                state_after_promotion,
                local_vfs_after_promotion,
                raw_cache_after_promotion,
                raw_db_after_promotion,
                files_after_promotion,
                vfs_after_promotion
            )
        })?;

    follower
        .db
        .execute_async(
            "INSERT INTO t (id, val) VALUES (?1, ?2)",
            &[
                SqlValue::Integer(2),
                SqlValue::Text("after-promotion".to_string()),
            ],
        )
        .await
        .map_err(|e| anyhow!("promoted follower write: {e}"))?;

    wait_for_count(&follower.db, 2, Duration::from_secs(3)).await?;
    wait_for_value(&follower.db, 2, "after-promotion", Duration::from_secs(3)).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn singlewriter_checkpoint_failover_promotes_and_keeps_writing() {
    let _guard = FAILOVER_TEST_LOCK.lock().await;
    run_singlewriter_failover(turbodb::Durability::Checkpoint(
        turbodb::CheckpointConfig::default(),
    ))
    .await
    .expect("checkpoint failover");
}

#[tokio::test(flavor = "multi_thread")]
async fn singlewriter_continuous_failover_promotes_and_keeps_writing() {
    let _guard = FAILOVER_TEST_LOCK.lock().await;
    run_singlewriter_failover(turbodb::Durability::Continuous {
        checkpoint: Default::default(),
        replication_interval: Duration::from_millis(50),
    })
    .await
    .expect("continuous failover");
}
