mod common;

use std::path::{Path, PathBuf};
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

/// Scan the follower tempdir for filenames that look like a temp
/// SQLite restore artifact (`*restore*`, `*.sqlite`, `*.db`).
///
/// Allows the turbolite layout: `data.cache` plus its SQLite shm/wal
/// sidecars, and anything under a `locks/` subdirectory (turbolite's
/// per-database file-guard markers are named `<db>.db` and would
/// otherwise trip `*.db`).
fn scan_for_restore_artifacts(dir: &Path) -> Vec<PathBuf> {
    let mut hits = Vec::new();
    fn walk(dir: &Path, in_locks: bool, hits: &mut Vec<PathBuf>) {
        let entries = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let ft = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            let name = match path.file_name().and_then(|s| s.to_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };
            if ft.is_dir() {
                let entered_locks = in_locks || name == "locks";
                walk(&path, entered_locks, hits);
                continue;
            }
            // Anything under `locks/` is a turbolite file-guard
            // marker, not a replay artifact — skip without checking.
            if in_locks {
                continue;
            }
            // Allow-list the SQLite shm/wal sidecars against
            // `data.cache` itself; SQLite (not us) creates these
            // when a connection opens through the VFS URI.
            if name.starts_with("data.cache") {
                continue;
            }
            let lower = name.to_ascii_lowercase();
            if lower.contains("restore")
                || lower.ends_with(".sqlite")
                || lower.ends_with(".db")
            {
                hits.push(path);
            }
        }
    }
    walk(dir, false, &mut hits);
    hits
}

fn assert_no_restore_artifacts(dir: &Path, label: &str) {
    let hits = scan_for_restore_artifacts(dir);
    assert!(
        hits.is_empty(),
        "{}: found temp-restore-shaped artifact(s) in {}: {:?}",
        label,
        dir.display(),
        hits
    );
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

    // Quiescent point #1 (post catch-up).
    assert_no_restore_artifacts(follower_tmp.path(), "post-follower-catch-up");

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

    // Quiescent point #2 (post promotion + post-promotion write).
    assert_no_restore_artifacts(follower_tmp.path(), "post-promotion");

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

/// A no-op manifest poll (same version, no walrust) must not
/// rewrite `data.cache`. Uses checkpoint durability because the
/// skip is gated on `walrust.is_none()`.
#[tokio::test(flavor = "multi_thread")]
async fn follower_no_op_poll_does_not_touch_data_cache() {
    let _guard = FAILOVER_TEST_LOCK.lock().await;
    let leader_tmp = TempDir::new().expect("leader tmp");
    let follower_tmp = TempDir::new().expect("follower tmp");
    let follower_cache_path = follower_tmp.path().join("data.cache");

    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store_impl = Arc::new(MemManifestStore::new());
    let manifest_store: Arc<dyn ManifestStore> = manifest_store_impl.clone();
    let tiered_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());

    let leader = build_singlewriter_node(
        leader_tmp.path(),
        "failover",
        turbodb::Durability::Checkpoint(turbodb::CheckpointConfig::default()),
        lease_store.clone(),
        manifest_store.clone(),
        tiered_storage.clone(),
        walrust_storage.clone(),
        "node-leader",
    )
    .await;
    wait_for_role(&leader.db, Role::Leader, Duration::from_secs(3))
        .await
        .expect("leader role");

    let follower = build_singlewriter_node(
        follower_tmp.path(),
        "failover",
        turbodb::Durability::Checkpoint(turbodb::CheckpointConfig::default()),
        lease_store,
        manifest_store,
        tiered_storage,
        walrust_storage,
        "node-follower",
    )
    .await;
    wait_for_role(&follower.db, Role::Follower, Duration::from_secs(3))
        .await
        .expect("follower role");

    leader
        .db
        .execute_async(
            "INSERT INTO t (id, val) VALUES (?1, ?2)",
            &[SqlValue::Integer(1), SqlValue::Text("steady".to_string())],
        )
        .await
        .expect("leader write");

    wait_for_count(&follower.db, 1, Duration::from_secs(5))
        .await
        .expect("follower catch up");

    // Wait one extra poll interval so any in-flight apply finishes
    // before we snapshot.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let cache_before = std::fs::read(&follower_cache_path).expect("read data.cache before");
    let mtime_before = std::fs::metadata(&follower_cache_path)
        .expect("metadata before")
        .modified()
        .expect("mtime before");

    // Leader is idle. Follower poll interval is 50ms; sleep ~10
    // intervals so several no-op polls definitely fire.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cache_after = std::fs::read(&follower_cache_path).expect("read data.cache after");
    let mtime_after = std::fs::metadata(&follower_cache_path)
        .expect("metadata after")
        .modified()
        .expect("mtime after");

    assert_eq!(
        cache_before.len(),
        cache_after.len(),
        "data.cache size changed during no-op poll window: before={} after={}",
        cache_before.len(),
        cache_after.len()
    );
    assert!(
        cache_before == cache_after,
        "data.cache bytes changed during no-op poll window"
    );
    assert_eq!(
        mtime_before, mtime_after,
        "data.cache mtime advanced during no-op poll window"
    );
}

/// Concurrent follower reads must not observe a torn `data.cache`
/// while a manifest swap is rewriting it. Drives a tight INSERT
/// loop on the leader (each commit triggers a fresh follower
/// materialize) against 8 parallel readers; row count must be
/// monotonically non-decreasing and rows must keep shape.
#[tokio::test(flavor = "multi_thread")]
async fn follower_reads_remain_consistent_during_concurrent_materialize() {
    let _guard = FAILOVER_TEST_LOCK.lock().await;
    let leader_tmp = TempDir::new().expect("leader tmp");
    let follower_tmp = TempDir::new().expect("follower tmp");

    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store_impl = Arc::new(MemManifestStore::new());
    let manifest_store: Arc<dyn ManifestStore> = manifest_store_impl.clone();
    let tiered_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());

    let leader = build_singlewriter_node(
        leader_tmp.path(),
        "failover",
        turbodb::Durability::Checkpoint(turbodb::CheckpointConfig::default()),
        lease_store.clone(),
        manifest_store.clone(),
        tiered_storage.clone(),
        walrust_storage.clone(),
        "node-leader",
    )
    .await;
    wait_for_role(&leader.db, Role::Leader, Duration::from_secs(3))
        .await
        .expect("leader role");

    let follower = build_singlewriter_node(
        follower_tmp.path(),
        "failover",
        turbodb::Durability::Checkpoint(turbodb::CheckpointConfig::default()),
        lease_store,
        manifest_store,
        tiered_storage,
        walrust_storage,
        "node-follower",
    )
    .await;
    wait_for_role(&follower.db, Role::Follower, Duration::from_secs(3))
        .await
        .expect("follower role");

    // Seed at least one row so reader tasks can run before the
    // first concurrent INSERT lands.
    leader
        .db
        .execute_async(
            "INSERT INTO t (id, val) VALUES (?1, ?2)",
            &[SqlValue::Integer(0), SqlValue::Text("seed".to_string())],
        )
        .await
        .expect("seed row");
    wait_for_count(&follower.db, 1, Duration::from_secs(5))
        .await
        .expect("follower seed catch-up");

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let reader = |id: u32| {
        let stop = stop.clone();
        let follower_db = &follower.db;
        async move {
            let mut last_count: i64 = 0;
            let mut iterations: u64 = 0;
            while !stop.load(Ordering::SeqCst) {
                let rows = follower_db
                    .query_values_fresh("SELECT COUNT(*) FROM t", &[])
                    .await
                    .unwrap_or_else(|e| panic!("reader {id} count failed: {e}"));
                let count = match rows.first().and_then(|r| r.first()) {
                    Some(SqlValue::Integer(n)) => *n,
                    other => panic!("reader {id}: unexpected count shape: {:?}", other),
                };
                assert!(
                    count >= last_count,
                    "reader {id}: row count regressed {} -> {} \
                     (torn read of pre-truncate manifest)",
                    last_count,
                    count
                );
                last_count = count;

                let id_rows = follower_db
                    .query_values_fresh("SELECT id, val FROM t ORDER BY id LIMIT 1", &[])
                    .await
                    .unwrap_or_else(|e| panic!("reader {id} id query failed: {e}"));
                if let Some(row) = id_rows.first() {
                    match (row.first(), row.get(1)) {
                        (Some(SqlValue::Integer(_)), Some(SqlValue::Text(_))) => {}
                        other => panic!("reader {id}: unexpected row shape: {:?}", other),
                    }
                }
                iterations += 1;
                tokio::task::yield_now().await;
            }
            iterations
        }
    };

    // 200 INSERTs at 8ms apart drives the manifest republish chain
    // tight enough to surface both torn-xRead and
    // missing-page-group races on broken code.
    let writer = {
        let stop = stop.clone();
        let leader_db = &leader.db;
        async move {
            for i in 1..=200i64 {
                leader_db
                    .execute_async(
                        "INSERT INTO t (id, val) VALUES (?1, ?2)",
                        &[
                            SqlValue::Integer(i),
                            SqlValue::Text(format!("row-{}", i)),
                        ],
                    )
                    .await
                    .expect("leader concurrent insert");
                tokio::time::sleep(Duration::from_millis(8)).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            stop.store(true, Ordering::SeqCst);
        }
    };

    let (r0, r1, r2, r3, r4, r5, r6, r7, _w) = tokio::join!(
        reader(0),
        reader(1),
        reader(2),
        reader(3),
        reader(4),
        reader(5),
        reader(6),
        reader(7),
        writer
    );
    let total_iterations = r0 + r1 + r2 + r3 + r4 + r5 + r6 + r7;
    assert!(
        total_iterations > 100,
        "readers ran too few iterations to be a meaningful test: {}",
        total_iterations
    );

    // Final correctness: follower converges to the leader's count.
    wait_for_count(&follower.db, 201, Duration::from_secs(10))
        .await
        .expect("follower converges after concurrent torture");
}

/// After failover + a post-promotion write, a fresh third node
/// joining from the published manifest must see both pre-promotion
/// and post-promotion rows. Asserts the post-promotion manifest is
/// actually a usable base (fresh page-group keys, advanced walrust
/// cursor) before spinning the third node up.
#[tokio::test(flavor = "multi_thread")]
async fn singlewriter_promotion_publishes_usable_base() {
    let _guard = FAILOVER_TEST_LOCK.lock().await;
    let leader_tmp = TempDir::new().expect("leader tmp");
    let follower_tmp = TempDir::new().expect("follower tmp");
    let third_tmp = TempDir::new().expect("third tmp");

    // Tight checkpoint thresholds so post-promotion writes embed
    // into the base — defaults won't fire in a unit-test window.
    let durability = turbodb::Durability::Continuous {
        checkpoint: turbodb::CheckpointConfig {
            interval: Duration::from_millis(100),
            commit_count: 1,
            wal_bytes: 1,
        },
        replication_interval: Duration::from_millis(50),
    };

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
    wait_for_role(&leader.db, Role::Leader, Duration::from_secs(3))
        .await
        .expect("leader role");

    let follower = build_singlewriter_node(
        follower_tmp.path(),
        "failover",
        durability,
        lease_store.clone(),
        manifest_store.clone(),
        tiered_storage.clone(),
        walrust_storage.clone(),
        "node-follower",
    )
    .await;
    wait_for_role(&follower.db, Role::Follower, Duration::from_secs(3))
        .await
        .expect("follower role");

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
        .expect("leader before-crash insert");
    wait_for_count(&follower.db, 1, Duration::from_secs(5))
        .await
        .expect("follower catch up before promotion");
    wait_for_value(&follower.db, 1, "before-crash", Duration::from_secs(5))
        .await
        .expect("follower sees before-crash row");

    // Pre-promotion manifest baseline.
    let pre_promotion_manifest_bytes = manifest_store
        .get("test/failover/_manifest")
        .await
        .expect("manifest_store get pre-promotion")
        .expect("pre-promotion manifest exists")
        .payload;
    let (pre_manifest, pre_walrust) =
        turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&pre_promotion_manifest_bytes)
            .expect("decode pre-promotion manifest");
    let pre_walrust_cursor = pre_walrust.as_ref().map(|(seq, _)| *seq);
    let pre_keys: std::collections::HashSet<String> =
        pre_manifest.page_group_keys.iter().cloned().collect();

    // Kill leader; follower promotes.
    leader
        .db
        .coordinator()
        .expect("leader coordinator")
        .abort_tasks_for_test()
        .await;
    wait_for_role(&follower.db, Role::Leader, Duration::from_secs(12))
        .await
        .expect("follower promotes to leader");
    wait_for_value(&follower.db, 1, "before-crash", Duration::from_secs(5))
        .await
        .expect("promoted leader still sees before-crash");

    // Multiple post-promotion writes give the checkpoint trigger
    // room to embed WAL frames into the base.
    for i in 2..=6i64 {
        follower
            .db
            .execute_async(
                "INSERT INTO t (id, val) VALUES (?1, ?2)",
                &[
                    SqlValue::Integer(i),
                    SqlValue::Text(format!("after-promotion-{}", i)),
                ],
            )
            .await
            .expect("promoted leader after-promotion insert");
    }
    wait_for_count(&follower.db, 6, Duration::from_secs(5))
        .await
        .expect("promoted leader sees all post-promotion rows");

    // Drain the publish chain so the manifest_store reflects the
    // post-promotion base. With commit_count=1, every commit
    // triggers a turbolite checkpoint, so a generous sleep here
    // is enough.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Wait for both the promotion publish AND the post-INSERT
    // publish to land — version + cursor must both advance.
    let deadline = Instant::now() + Duration::from_secs(8);
    let post_promotion_manifest_bytes = loop {
        let bytes = manifest_store
            .get("test/failover/_manifest")
            .await
            .expect("manifest_store get post-promotion")
            .expect("post-promotion manifest exists")
            .payload;
        let (m, walrust_now) =
            turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&bytes)
                .expect("decode post-promotion manifest");
        let cursor_now = walrust_now.as_ref().map(|(seq, _)| *seq);
        let cursor_advanced = match (pre_walrust_cursor, cursor_now) {
            (Some(pre), Some(now)) => now > pre,
            _ => true,
        };
        if m.version > pre_manifest.version && cursor_advanced {
            break bytes;
        }
        if Instant::now() >= deadline {
            panic!(
                "post-promotion manifest did not advance past pre-promotion v{} (cursor pre={:?} now={:?}) within timeout",
                pre_manifest.version, pre_walrust_cursor, cursor_now
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let (post_manifest, post_walrust) =
        turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&post_promotion_manifest_bytes)
            .expect("decode post-promotion manifest (final)");

    // The post-promotion manifest must be a real usable base:
    // version advanced, page coverage at least as wide, fresh
    // page-group keys uploaded, walrust cursor advanced.
    assert!(
        post_manifest.version > pre_manifest.version,
        "post-promotion manifest version did not advance: {} -> {}",
        pre_manifest.version,
        post_manifest.version
    );
    assert!(
        post_manifest.page_count >= pre_manifest.page_count,
        "post-promotion page_count regressed: {} -> {}",
        pre_manifest.page_count,
        post_manifest.page_count
    );
    let new_keys: Vec<&String> = post_manifest
        .page_group_keys
        .iter()
        .filter(|k| !k.is_empty() && !pre_keys.contains(*k))
        .collect();
    assert!(
        !new_keys.is_empty(),
        "post-promotion manifest carries no new page-group keys; \
         flush_dirty_groups did not upload anything new. \
         pre_keys={:?}, post_keys={:?}",
        pre_keys,
        post_manifest.page_group_keys
    );
    let post_walrust_cursor = post_walrust.as_ref().map(|(seq, _)| *seq);
    assert!(
        post_walrust_cursor.is_some(),
        "post-promotion manifest dropped its walrust cursor"
    );
    if let (Some(pre), Some(post)) = (pre_walrust_cursor, post_walrust_cursor) {
        assert!(
            post > pre,
            "post-promotion walrust cursor did not advance: {} -> {}",
            pre,
            post
        );
    }

    // Fresh third node sharing the same storage — joins as
    // Follower and must catch up to both rows.
    let third = build_singlewriter_node(
        third_tmp.path(),
        "failover",
        durability,
        lease_store,
        manifest_store,
        tiered_storage,
        walrust_storage,
        "node-third",
    )
    .await;
    wait_for_role(&third.db, Role::Follower, Duration::from_secs(5))
        .await
        .expect("third node attains follower role");
    if let Err(e) = wait_for_count(&third.db, 6, Duration::from_secs(20)).await {
        let third_local = local_vfs_state(&third.vfs);
        let third_files = dir_snapshot(third_tmp.path());
        let third_vfs = vfs_snapshot(third_tmp.path(), "failover", &third.vfs_name);
        let third_raw = raw_cache_snapshot(&third_tmp.path().join("data.cache"));
        let promoted_local = local_vfs_state(&follower.vfs);
        let walrust_keys = walrust_storage_impl.keys().await;
        panic!(
            "third node count: {}; \
             pre_walrust_cursor={:?} post_walrust_cursor={:?} \
             pre_keys={:?} post_keys={:?} \
             walrust_keys={:?} \
             promoted_leader: {}; \
             third: {}; {}; {}; {}",
            e,
            pre_walrust_cursor,
            post_walrust_cursor,
            pre_keys,
            post_manifest.page_group_keys,
            walrust_keys,
            promoted_local,
            third_local,
            third_files,
            third_vfs,
            third_raw
        );
    }
    wait_for_value(&third.db, 1, "before-crash", Duration::from_secs(5))
        .await
        .expect("third node sees pre-promotion row");
    wait_for_value(&third.db, 2, "after-promotion-2", Duration::from_secs(5))
        .await
        .expect("third node sees first post-promotion row");
    wait_for_value(&third.db, 6, "after-promotion-6", Duration::from_secs(5))
        .await
        .expect("third node sees last after-promotion row");

    assert_no_restore_artifacts(third_tmp.path(), "third-fresh-follower");
}

/// Failover when the follower has already replayed every walrust
/// frame the leader uploaded. The promoted leader's first publish
/// must still flush its accumulated replay state into a fresh
/// page-group base — proven by giving the third node EMPTY walrust
/// storage so it can only see pre-crash data via materialize.
#[tokio::test(flavor = "multi_thread")]
async fn singlewriter_promotion_publishes_already_replayed_base() {
    let _guard = FAILOVER_TEST_LOCK.lock().await;
    let leader_tmp = TempDir::new().expect("leader tmp");
    let follower_tmp = TempDir::new().expect("follower tmp");
    let third_tmp = TempDir::new().expect("third tmp");

    let durability = turbodb::Durability::Continuous {
        checkpoint: turbodb::CheckpointConfig {
            interval: Duration::from_millis(100),
            commit_count: 1,
            wal_bytes: 1,
        },
        replication_interval: Duration::from_millis(50),
    };

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
    wait_for_role(&leader.db, Role::Leader, Duration::from_secs(3))
        .await
        .expect("leader role");

    let follower = build_singlewriter_node(
        follower_tmp.path(),
        "failover",
        durability,
        lease_store.clone(),
        manifest_store.clone(),
        tiered_storage.clone(),
        walrust_storage.clone(),
        "node-follower",
    )
    .await;
    wait_for_role(&follower.db, Role::Follower, Duration::from_secs(3))
        .await
        .expect("follower role");

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
        .expect("leader before-crash insert");

    // Wait for the follower to see the row through normal
    // steady-state polling, BEFORE killing the leader. This is
    // the "already caught up" precondition.
    wait_for_count(&follower.db, 1, Duration::from_secs(5))
        .await
        .expect("follower catches up while leader is alive");
    wait_for_value(&follower.db, 1, "before-crash", Duration::from_secs(5))
        .await
        .expect("follower sees before-crash row");

    // Extra drain to ensure walrust frames + manifest publish are
    // fully settled. After this, the next replay cycle on the
    // follower's poll should be a true no-op (zero frames > cursor).
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Pre-promotion manifest baseline. We diff post against pre
    // and require at least one fresh page-group key — without
    // that the third node could be getting `before-crash` only
    // via walrust replay, not via the promotion's published base.
    let pre_promotion_manifest_bytes = manifest_store
        .get("test/failover/_manifest")
        .await
        .expect("manifest_store get pre-promotion")
        .expect("pre-promotion manifest exists")
        .payload;
    let (pre_manifest, pre_walrust) =
        turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&pre_promotion_manifest_bytes)
            .expect("decode pre-promotion manifest");
    let pre_walrust_cursor = pre_walrust.as_ref().map(|(seq, _)| *seq);
    let pre_keys: std::collections::HashSet<String> =
        pre_manifest.page_group_keys.iter().cloned().collect();
    let pre_envelope_version = manifest_store
        .meta("test/failover/_manifest")
        .await
        .expect("manifest_store meta pre-promotion")
        .map(|m| m.version)
        .unwrap_or(0);

    // Kill the leader. The follower's promotion-time replay will
    // find zero new walrust changesets — it's already current.
    leader
        .db
        .coordinator()
        .expect("leader coordinator")
        .abort_tasks_for_test()
        .await;
    wait_for_role(&follower.db, Role::Leader, Duration::from_secs(12))
        .await
        .expect("follower promotes despite zero replay backlog");
    wait_for_value(&follower.db, 1, "before-crash", Duration::from_secs(5))
        .await
        .expect("promoted leader still reads before-crash after a no-op promotion replay");

    // Wait for the promotion publish: at least one fresh
    // page-group key AND a bumped envelope version. Same-keys is
    // the negative signal — accumulated state never reached the
    // store.
    let deadline = Instant::now() + Duration::from_secs(8);
    let post_promotion_manifest_bytes = loop {
        let bytes = manifest_store
            .get("test/failover/_manifest")
            .await
            .expect("manifest_store get post-promotion")
            .expect("manifest exists post-promotion")
            .payload;
        let (m, _) = turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&bytes)
            .expect("decode post-promotion manifest");
        let post_keys: std::collections::HashSet<&String> =
            m.page_group_keys.iter().collect();
        let has_new_key = post_keys
            .iter()
            .any(|k| !k.is_empty() && !pre_keys.contains(*k));
        let envelope_version = manifest_store
            .meta("test/failover/_manifest")
            .await
            .expect("manifest_store meta post-promotion")
            .map(|m| m.version)
            .unwrap_or(0);
        if has_new_key && envelope_version > pre_envelope_version {
            break bytes;
        }
        if Instant::now() >= deadline {
            panic!(
                "promoted follower did not publish a fresh page-group base \
                 within timeout; pre_keys={:?} post_keys={:?} \
                 pre_envelope={} post_envelope={}",
                pre_keys, m.page_group_keys, pre_envelope_version, envelope_version
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Manifest-inspection assertions (pre vs post).
    let (post_manifest, post_walrust) =
        turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&post_promotion_manifest_bytes)
            .expect("decode post-promotion manifest (final)");
    let post_walrust_cursor = post_walrust.as_ref().map(|(seq, _)| *seq);
    let new_keys: Vec<&String> = post_manifest
        .page_group_keys
        .iter()
        .filter(|k| !k.is_empty() && !pre_keys.contains(*k))
        .collect();
    assert!(
        !new_keys.is_empty(),
        "post-promotion manifest must carry at least one fresh page-group key \
         (the flush of accumulated replay state). pre_keys={:?} post_keys={:?}",
        pre_keys, post_manifest.page_group_keys
    );
    assert!(
        post_walrust_cursor.is_some(),
        "post-promotion manifest dropped its walrust cursor"
    );

    // Empty walrust storage for the third: if `before-crash`
    // reaches it, the row had to come from materialize over the
    // post-promotion page-group base, not from a walrust frame.
    let third_walrust_storage_impl = Arc::new(InMemoryStorage::new());
    let third_walrust_storage: Arc<dyn hadb_storage::StorageBackend> =
        third_walrust_storage_impl.clone();
    let third = build_singlewriter_node(
        third_tmp.path(),
        "failover",
        durability,
        lease_store,
        manifest_store.clone(),
        tiered_storage,
        third_walrust_storage,
        "node-third",
    )
    .await;
    wait_for_role(&third.db, Role::Follower, Duration::from_secs(5))
        .await
        .expect("third node attains follower role");
    wait_for_value(&third.db, 1, "before-crash", Duration::from_secs(10))
        .await
        .unwrap_or_else(|e| {
            let third_walrust_keys = futures_lite_block_on(third_walrust_storage_impl.keys());
            panic!(
                "third node did not see row 1 from materialize alone: {}; \
                 pre_keys={:?} post_keys={:?} new_keys_in_post={:?} \
                 pre_walrust_cursor={:?} post_walrust_cursor={:?} \
                 third_walrust_keys={:?}",
                e,
                pre_keys,
                post_manifest.page_group_keys,
                new_keys,
                pre_walrust_cursor,
                post_walrust_cursor,
                third_walrust_keys,
            )
        });

    // Sanity-check the no-walrust-replay precondition.
    let third_walrust_keys = third_walrust_storage_impl.keys().await;
    assert!(
        third_walrust_keys.is_empty(),
        "third node's walrust storage is unexpectedly populated: {:?}",
        third_walrust_keys
    );

    assert_no_restore_artifacts(third_tmp.path(), "third-already-caught-up");
}

/// Run a future to completion from sync code inside an async test.
fn futures_lite_block_on<F: std::future::Future>(fut: F) -> F::Output {
    let runtime = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| runtime.block_on(fut))
}

/// Retry-safety regression: when materialize fails because a
/// page-group object is missing (leader publish raced upload, or
/// version churn re-keyed the object), the follower must NOT leave
/// VFS state half-applied. Reads taken AFTER the failed apply must
/// see the previous (still-valid) snapshot, not a mix of the new
/// manifest plus stale cache bytes.
///
/// Earlier shapes of this code path called `set_manifest_bytes`
/// BEFORE `materialize_to_file`; a NotFound from materialize would
/// then leave the VFS in a half-state: new shared_manifest, evicted
/// page groups, bumped page_count, persisted local manifest, but a
/// `data.cache` that either still held v_old's bytes or was
/// truncated to zeros. Reads against that mix surface as "file is
/// not a database", count regressions, or schema disappearance.
///
/// This test pauses storage `get()` to simulate the missing-group
/// race, drives the leader through a write that bumps the manifest,
/// then asserts the follower's reads keep returning the
/// pre-paused-write snapshot for the duration of the pause and
/// converge to the new state once storage unpauses.
#[tokio::test(flavor = "multi_thread")]
async fn follower_apply_is_atomic_under_missing_page_group_race() {
    let _guard = FAILOVER_TEST_LOCK.lock().await;
    let leader_tmp = TempDir::new().expect("leader tmp");
    let follower_tmp = TempDir::new().expect("follower tmp");

    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store_impl = Arc::new(MemManifestStore::new());
    let manifest_store: Arc<dyn ManifestStore> = manifest_store_impl.clone();
    let inner_storage = Arc::new(InMemoryStorage::new());
    let pausable = Arc::new(common::PausableStorage::new(inner_storage.clone()));
    let pausable_dyn: Arc<dyn hadb_storage::StorageBackend> = pausable.clone();
    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());

    let leader = build_singlewriter_node(
        leader_tmp.path(),
        "failover",
        turbodb::Durability::Checkpoint(turbodb::CheckpointConfig::default()),
        lease_store.clone(),
        manifest_store.clone(),
        pausable_dyn.clone(),
        walrust_storage.clone(),
        "node-leader",
    )
    .await;
    wait_for_role(&leader.db, Role::Leader, Duration::from_secs(3))
        .await
        .expect("leader role");

    let follower = build_singlewriter_node(
        follower_tmp.path(),
        "failover",
        turbodb::Durability::Checkpoint(turbodb::CheckpointConfig::default()),
        lease_store,
        manifest_store,
        pausable_dyn,
        walrust_storage,
        "node-follower",
    )
    .await;
    wait_for_role(&follower.db, Role::Follower, Duration::from_secs(3))
        .await
        .expect("follower role");

    // Seed: enough rows to grow the table beyond a single page, so
    // a v_old → v_new manifest swap actually touches different
    // page-group structure (page_count and group_pages diverge).
    // Without growth, v_old and v_new might decode the same bytes
    // identically and a half-apply wouldn't be observable.
    for i in 1..=5i64 {
        leader
            .db
            .execute_async(
                "INSERT INTO t (id, val) VALUES (?1, ?2)",
                &[
                    SqlValue::Integer(i),
                    SqlValue::Text(format!("seed-{}", i)),
                ],
            )
            .await
            .expect("seed insert");
    }
    wait_for_count(&follower.db, 5, Duration::from_secs(5))
        .await
        .expect("follower seed catch-up");

    // Wait one extra poll interval so any in-flight apply settles.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Snapshot the follower's pre-pause VFS state. These are the
    // load-bearing assertions: if `set_manifest_bytes` runs before
    // a failing materialize, `manifest().version` advances and
    // `data.cache` either stays at old bytes (mismatched with the
    // new manifest) or gets truncated to zeros — either is the
    // half-apply this test is a regression for.
    let cache_path = follower.vfs.cache_file_path();
    let pre_manifest_version = follower.vfs.manifest().version;
    let pre_page_count = follower.vfs.manifest().page_count;
    let pre_cache_bytes = std::fs::read(&cache_path).expect("read cache pre-pause");
    let pre_cache_mtime = std::fs::metadata(&cache_path)
        .expect("metadata pre-pause")
        .modified()
        .expect("mtime pre-pause");

    // Pause storage so the next manifest-driven materialize sees
    // missing page-group objects (`get` returns Ok(None)). The
    // leader's writes still go through (put delegates).
    pausable.pause();

    // Leader writes more rows during the pause. Each commit bumps
    // the manifest. The follower's apply attempts will fetch
    // page-group objects → paused `get` returns None → NotFound →
    // TransientRetry, with the materialize-before-commit ordering
    // ensuring no VFS mutation along the way.
    for i in 6..=15i64 {
        leader
            .db
            .execute_async(
                "INSERT INTO t (id, val) VALUES (?1, ?2)",
                &[
                    SqlValue::Integer(i),
                    SqlValue::Text(format!("post-pause-{}", i)),
                ],
            )
            .await
            .expect("paused insert");
        // Yield between inserts so the follower's poll loop gets
        // scheduled and runs poll_manifest_store against the latest
        // payload while storage is still paused. Without this,
        // back-to-back leader writes can starve the follower's
        // 50ms-interval task on a busy single-process runtime.
        tokio::time::sleep(Duration::from_millis(60)).await;
    }

    // Wait several follower poll intervals so failed-apply attempts
    // have a chance to half-apply if the ordering is wrong.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // VFS state must be byte-identical to pre-pause: no advance of
    // shared_manifest version, no change in page_count, no rewrite
    // of `data.cache`.
    let mid_manifest_version = follower.vfs.manifest().version;
    let mid_page_count = follower.vfs.manifest().page_count;
    let mid_cache_bytes = std::fs::read(&cache_path).expect("read cache during pause");
    let mid_cache_mtime = std::fs::metadata(&cache_path)
        .expect("metadata during pause")
        .modified()
        .expect("mtime during pause");

    assert_eq!(
        mid_manifest_version, pre_manifest_version,
        "shared_manifest.version advanced during failed apply: {} -> {} \
         (half-apply: set_manifest_bytes ran before materialize NotFound)",
        pre_manifest_version, mid_manifest_version
    );
    assert_eq!(
        mid_page_count, pre_page_count,
        "page_count changed during failed apply: {} -> {}",
        pre_page_count, mid_page_count
    );
    assert_eq!(
        mid_cache_bytes.len(),
        pre_cache_bytes.len(),
        "data.cache size changed during failed apply"
    );
    assert!(
        pre_cache_bytes == mid_cache_bytes,
        "data.cache bytes changed during failed apply (half-apply: \
         materialize truncated/wrote before NotFound bailed)"
    );
    assert_eq!(
        pre_cache_mtime, mid_cache_mtime,
        "data.cache mtime advanced during failed apply"
    );

    // Reads during the paused window must still succeed and return
    // the pre-pause snapshot.
    let rows = follower
        .db
        .query_values_fresh("SELECT COUNT(*) FROM t", &[])
        .await
        .expect("paused-window count read should not error");
    let count = match rows.first().and_then(|r| r.first()) {
        Some(SqlValue::Integer(n)) => *n,
        other => panic!("unexpected count shape during paused window: {:?}", other),
    };
    assert_eq!(
        count, 5,
        "follower read returned {} while storage was paused; \
         apply must not have mutated VFS state",
        count
    );

    // Unpause — the follower's next poll should retry, succeed,
    // and converge to the leader's row count.
    pausable.unpause();
    wait_for_count(&follower.db, 15, Duration::from_secs(5))
        .await
        .expect("follower converges after storage unpauses");
}
