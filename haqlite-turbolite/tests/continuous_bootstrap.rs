mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use common::InMemoryStorage;
use hadb::InMemoryLeaseStore;
use hadb_storage::CasResult;
use hadb_storage::StorageBackend;
use haqlite_turbolite::{Builder, HaMode};
use std::sync::Mutex;
use tempfile::TempDir;
use turbodb::{Manifest, ManifestMeta, ManifestStore};
use turbodb_manifest_mem::MemManifestStore;
use turbolite::tiered::{CacheConfig, SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Default)]
struct BootstrapConflictStore {
    state: Mutex<BootstrapConflictState>,
}

#[derive(Default)]
struct BootstrapConflictState {
    manifest: Option<Manifest>,
    first_create_conflict_done: bool,
}

#[derive(Default)]
struct BootstrapConflictInvisibleMetaStore {
    state: Mutex<BootstrapConflictInvisibleMetaState>,
}

#[derive(Default)]
struct BootstrapConflictInvisibleMetaState {
    manifest: Option<Manifest>,
    first_create_conflict_done: bool,
    hide_meta_reads: u32,
}

#[async_trait]
impl ManifestStore for BootstrapConflictStore {
    async fn get(&self, _key: &str) -> Result<Option<Manifest>> {
        Ok(self.state.lock().expect("lock").manifest.clone())
    }

    async fn put(
        &self,
        _key: &str,
        manifest: &Manifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let mut state = self.state.lock().expect("lock");

        if expected_version.is_none() && !state.first_create_conflict_done {
            let mut existing = manifest.clone();
            existing.version = 1;
            state.manifest = Some(existing);
            state.first_create_conflict_done = true;
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }

        let next_version = expected_version.unwrap_or(0) + 1;
        let mut stored = manifest.clone();
        stored.version = next_version;
        state.manifest = Some(stored);
        Ok(CasResult {
            success: true,
            etag: Some(next_version.to_string()),
        })
    }

    async fn meta(&self, _key: &str) -> Result<Option<ManifestMeta>> {
        Ok(self
            .state
            .lock()
            .expect("lock")
            .manifest
            .as_ref()
            .map(ManifestMeta::from))
    }
}

#[async_trait]
impl ManifestStore for BootstrapConflictInvisibleMetaStore {
    async fn get(&self, _key: &str) -> Result<Option<Manifest>> {
        Ok(self.state.lock().expect("lock").manifest.clone())
    }

    async fn put(
        &self,
        _key: &str,
        manifest: &Manifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let mut state = self.state.lock().expect("lock");

        if expected_version.is_none() && !state.first_create_conflict_done {
            let mut existing = manifest.clone();
            existing.version = 1;
            state.manifest = Some(existing);
            state.first_create_conflict_done = true;
            state.hide_meta_reads = 1;
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }

        let next_version = expected_version.unwrap_or(0) + 1;
        let mut stored = manifest.clone();
        stored.version = next_version;
        state.manifest = Some(stored);
        Ok(CasResult {
            success: true,
            etag: Some(next_version.to_string()),
        })
    }

    async fn meta(&self, _key: &str) -> Result<Option<ManifestMeta>> {
        let mut state = self.state.lock().expect("lock");
        if state.hide_meta_reads > 0 {
            state.hide_meta_reads -= 1;
            return Ok(None);
        }
        Ok(state.manifest.as_ref().map(ManifestMeta::from))
    }
}

fn make_remote_vfs(
    cache_dir: &std::path::Path,
    storage: Arc<dyn StorageBackend>,
) -> (SharedTurboliteVfs, String) {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    let vfs_name = format!("continuous_bootstrap_{}", n);
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
        .expect("create VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register VFS");
    (shared_vfs, vfs_name)
}

async fn assert_manifest_page_groups_exist(
    vfs: &SharedTurboliteVfs,
    manifest: &Manifest,
    storage: &InMemoryStorage,
) {
    let _walrust = vfs
        .set_manifest_bytes(&manifest.payload)
        .expect("decode hybrid payload");
    let decoded = vfs.manifest();
    let keys = storage.keys().await;

    for key in decoded.page_group_keys.iter().filter(|k| !k.is_empty()) {
        assert!(
            keys.iter().any(|existing| existing == key),
            "manifest references missing page-group object {key}; storage keys = {:?}",
            keys
        );
    }
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS bootstrap_test (
    id INTEGER PRIMARY KEY,
    value TEXT NOT NULL
);";

#[tokio::test(flavor = "multi_thread")]
async fn continuous_open_publishes_hybrid_manifest_for_fresh_database() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("bootstrap.db");

    let walrust_storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let tiered_storage_impl = Arc::new(InMemoryStorage::new());
    let tiered_storage: Arc<dyn StorageBackend> = tiered_storage_impl.clone();
    let manifest_store = Arc::new(MemManifestStore::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let (vfs, vfs_name) = make_remote_vfs(tmp.path(), tiered_storage);
    let mut db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .durability(turbodb::Durability::Continuous {
            checkpoint: Default::default(),
            replication_interval: Duration::from_millis(50),
        })
        .lease_store(lease_store)
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(walrust_storage)
        .turbolite_vfs(vfs.clone(), &vfs_name)
        .instance_id("writer-1")
        .forwarding_port(19301)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open continuous mode");

    let manifest = manifest_store
        .get("test/bootstrap/_manifest")
        .await
        .expect("fetch manifest")
        .expect("fresh continuous bootstrap should publish manifest");
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.writer_id, "writer-1");

    let walrust = vfs
        .set_manifest_bytes(&manifest.payload)
        .expect("decode hybrid payload")
        .expect("continuous payload must include walrust cursor");
    assert_eq!(walrust, (0, "test/".to_string()));
    assert_manifest_page_groups_exist(&vfs, &manifest, &tiered_storage_impl).await;
    db.close().await.expect("close haqlite");
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_open_retries_first_manifest_create_race() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("bootstrap_conflict.db");

    let walrust_storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let tiered_storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let manifest_store = Arc::new(BootstrapConflictStore::default());
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let (vfs, vfs_name) = make_remote_vfs(tmp.path(), tiered_storage);
    let mut db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .durability(turbodb::Durability::Continuous {
            checkpoint: Default::default(),
            replication_interval: Duration::from_millis(50),
        })
        .lease_store(lease_store)
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(walrust_storage)
        .turbolite_vfs(vfs.clone(), &vfs_name)
        .instance_id("writer-1")
        .forwarding_port(19302)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open continuous mode despite first-manifest create race");

    let manifest = manifest_store
        .get("test/bootstrap_conflict/_manifest")
        .await
        .expect("fetch manifest")
        .expect("bootstrap retry should still publish manifest");
    assert_eq!(manifest.version, 2);

    let walrust = vfs
        .set_manifest_bytes(&manifest.payload)
        .expect("decode hybrid payload")
        .expect("continuous payload must include walrust cursor");
    assert_eq!(walrust, (0, "test/".to_string()));
    db.close().await.expect("close haqlite");
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_open_accepts_same_writer_manifest_when_meta_lags() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("bootstrap_meta_lag.db");

    let walrust_storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let tiered_storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let manifest_store = Arc::new(BootstrapConflictInvisibleMetaStore::default());
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let (vfs, vfs_name) = make_remote_vfs(tmp.path(), tiered_storage);
    let mut db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .durability(turbodb::Durability::Continuous {
            checkpoint: Default::default(),
            replication_interval: Duration::from_millis(50),
        })
        .lease_store(lease_store)
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(walrust_storage)
        .turbolite_vfs(vfs.clone(), &vfs_name)
        .instance_id("writer-1")
        .forwarding_port(19303)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open continuous mode when manifest create winner is only visible via get()");

    let manifest = manifest_store
        .get("test/bootstrap_meta_lag/_manifest")
        .await
        .expect("fetch manifest")
        .expect("same-writer winner should leave a manifest behind");
    assert_eq!(manifest.version, 1);

    let walrust = vfs
        .set_manifest_bytes(&manifest.payload)
        .expect("decode hybrid payload")
        .expect("continuous payload must include walrust cursor");
    assert_eq!(walrust, (0, "test/".to_string()));
    db.close().await.expect("close haqlite");
}

/// Regression: a fresh tenant in turbolite-VFS mode must bootstrap even
/// when the caller passes an empty schema.
///
/// The cinch engine's redis tenant create path (`pool/redis.rs::create`)
/// calls `builder.open(db_path, "")` for a brand-new tenant. Without
/// a schema to execute, ensure_schema's opener-call doesn't push any
/// pages through the VFS, and the local OS path at `db_path` never
/// gets a file. ensure_base_manifest then calls
/// `vfs.import_sqlite_file(&db_path)` and ENOENTs out, the open fails,
/// the orchestrator-side claim is leaked, and Phase Skerry watchdog
/// fires (last_kind=EmptyQueue / total_flush_attempts grows / zero
/// successes — the in-vivo fingerprint we observed).
///
/// The fix lives in haqlite-turbolite::Replicator::ensure_base_manifest:
/// when there is no remote manifest AND no local file, seed a minimal
/// valid SQLite at the local path before import so the import has a
/// real header to read.
#[tokio::test(flavor = "multi_thread")]
async fn continuous_open_publishes_hybrid_manifest_for_fresh_database_with_empty_schema() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("empty_schema_bootstrap.db");

    let walrust_storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let tiered_storage_impl = Arc::new(InMemoryStorage::new());
    let tiered_storage: Arc<dyn StorageBackend> = tiered_storage_impl.clone();
    let manifest_store = Arc::new(MemManifestStore::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let (vfs, vfs_name) = make_remote_vfs(tmp.path(), tiered_storage);
    let mut db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .durability(turbodb::Durability::Continuous {
            checkpoint: Default::default(),
            replication_interval: Duration::from_millis(50),
        })
        .lease_store(lease_store)
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(walrust_storage)
        .turbolite_vfs(vfs.clone(), &vfs_name)
        .instance_id("writer-empty-schema")
        .forwarding_port(19310)
        .open(db_path.to_str().expect("path"), "")
        .await
        .expect("fresh empty-schema bootstrap must succeed (cinch redis-create path)");

    let manifest = manifest_store
        .get("test/empty_schema_bootstrap/_manifest")
        .await
        .expect("fetch manifest")
        .expect("empty-schema fresh bootstrap must publish manifest");
    assert!(
        manifest.version >= 1,
        "manifest version must advance past zero for fresh tenants: {}",
        manifest.version
    );
    assert_eq!(manifest.writer_id, "writer-empty-schema");

    let walrust = vfs
        .set_manifest_bytes(&manifest.payload)
        .expect("decode hybrid payload")
        .expect("continuous payload must include walrust cursor");
    assert_eq!(walrust, (0, "test/".to_string()));
    assert_manifest_page_groups_exist(&vfs, &manifest, &tiered_storage_impl).await;
    db.close().await.expect("close haqlite");
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_first_write_publishes_manifest_that_only_references_existing_page_groups() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("first_write.db");

    let walrust_storage_impl = Arc::new(InMemoryStorage::new());
    let walrust_storage: Arc<dyn StorageBackend> = walrust_storage_impl.clone();
    let tiered_storage_impl = Arc::new(InMemoryStorage::new());
    let tiered_storage: Arc<dyn StorageBackend> = tiered_storage_impl.clone();
    let manifest_store = Arc::new(MemManifestStore::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let (vfs, vfs_name) = make_remote_vfs(tmp.path(), tiered_storage);
    let mut db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .durability(turbodb::Durability::Continuous {
            checkpoint: Default::default(),
            replication_interval: Duration::from_millis(50),
        })
        .lease_store(lease_store)
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(walrust_storage)
        .turbolite_vfs(vfs.clone(), &vfs_name)
        .instance_id("writer-first-write")
        .forwarding_port(19311)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open continuous mode");
    assert!(
        vfs.cache_file_path().exists(),
        "turbolite cache file should exist immediately after open: {}",
        vfs.cache_file_path().display()
    );

    db.execute_async(
        "INSERT INTO bootstrap_test (id, value) VALUES (?1, ?2)",
        &[
            haqlite::SqlValue::Integer(1),
            haqlite::SqlValue::Text("first-write".to_string()),
        ],
    )
    .await
    .expect("first write");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let manifest = loop {
        let manifest = manifest_store
            .get("test/first_write/_manifest")
            .await
            .expect("fetch manifest during first-write poll");
        if let Some(manifest) = manifest {
            let walrust = vfs
                .set_manifest_bytes(&manifest.payload)
                .expect("decode hybrid payload during first-write poll")
                .expect("continuous payload must carry walrust cursor during first-write poll");
            if manifest.version > 1 || walrust.0 > 0 {
                break manifest;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "first write should publish a fresh hybrid manifest envelope"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_manifest_page_groups_exist(&vfs, &manifest, &tiered_storage_impl).await;

    let walrust = vfs
        .set_manifest_bytes(&manifest.payload)
        .expect("decode hybrid payload after first write")
        .expect("continuous payload must carry walrust cursor after first write");
    assert_eq!(
        walrust,
        (0, "test/".to_string()),
        "first write must not move the base cursor until Turbolite checkpoints the base"
    );

    let walrust_keys = walrust_storage_impl.keys().await;
    assert!(
        !walrust_keys.is_empty(),
        "first write should also publish walrust state; keys = {:?}",
        walrust_keys
    );
    db.close().await.expect("close haqlite");
}
