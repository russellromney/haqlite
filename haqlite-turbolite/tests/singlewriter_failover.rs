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

/// B25 regression: a manifest poll that finds the same version
/// the follower has already applied must not rewrite `data.cache`.
/// Without the version-skip guard, every poll interval would
/// re-`set_manifest_bytes` + `materialize_to_file` + sync, briefly
/// clobbering whatever bytes are at offsets a concurrent
/// follower-read shortcut might be looking at.
///
/// Uses checkpoint durability because the skip is gated on
/// `walrust.is_none()` — continuous manifests carry an embed
/// cursor, not an upload head, so the no-op-skip optimization
/// can't be safely applied while walrust frames are flowing.
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
        "data.cache bytes changed during no-op poll window (B25 regression: \
         apply_manifest_payload ran for an unchanged manifest)"
    );
    assert_eq!(
        mtime_before, mtime_after,
        "data.cache mtime advanced during no-op poll window (B25 regression)"
    );
}

/// B26 regression: concurrent follower reads must never observe a
/// torn `data.cache` while `materialize_to_file` is rewriting it.
///
/// Without the apply-side fence (replay_gate.write held around
/// materialize) a reader's `xRead` can race the truncate+rewrite
/// and surface as SQLITE_CORRUPT, missing schema, regressed row
/// counts, or other torn reads. With the fence and follower reads
/// routed through the VFS, every read either completes before
/// truncate or blocks until the cache is consistent again.
///
/// Drives a leader through a tight INSERT loop (each INSERT bumps
/// the manifest in checkpoint mode and the follower's poll triggers
/// a fresh `materialize_to_file`) while parallel reader tasks hammer
/// `query_values_fresh` against the follower. The follower's
/// observed row count must be monotonically non-decreasing (with
/// the schema column shape preserved) and no read may error.
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
                    "reader {id}: row count regressed {} -> {} (B26 regression: \
                     torn read of pre-truncate manifest)",
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

    // Aggressive write cadence (8ms between INSERTs over 200
    // iterations = ~1.6s) drives the leader's page-group upload
    // and manifest republish flow into a tight race. Two distinct
    // failure modes used to surface here:
    //   - Torn xRead during materialize_to_file's `File::create`
    //     truncate window (B27 narrow-gate bug, fixed by holding
    //     the VFS replay-gate write across the full apply).
    //   - "missing page-group object" when the leader publishes
    //     manifest vN naming a key whose object hasn't uploaded
    //     yet, or has been re-keyed under vN+1 churn (fixed by
    //     pre-flighting all fetches before truncating data.cache
    //     and treating the residual NotFound as a transient retry).
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
