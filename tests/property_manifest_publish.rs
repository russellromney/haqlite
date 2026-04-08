//! Property-based tests for manifest publish CAS semantics.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use proptest::prelude::*;
use tempfile::TempDir;

use common::InMemoryStorage;
use haqlite::{HaMode, HaQLite, InMemoryLeaseStore, InMemoryManifestStore, ManifestStore, SqlValue};
use turbolite::tiered::{SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);
fn make_local_vfs(cache_dir: &std::path::Path) -> (SharedTurboliteVfs, String) {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    let vfs_name = format!("pmp_{}", n);
    let config = TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        pages_per_group: 4,
        sub_pages_per_frame: 2,
        eager_index_load: false,
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("create VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register VFS");
    (shared_vfs, vfs_name)
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v TEXT);";

proptest! {
    #[test]
    fn manifest_cas_under_concurrent_writes(
        num_writers in 1..4usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let tmp_dir = TempDir::new().unwrap();
            let storage = Arc::new(InMemoryStorage::new());
            let lease_store = Arc::new(InMemoryLeaseStore::new());
            let manifest_store = Arc::new(InMemoryManifestStore::new());

            let mut dbs = Vec::new();
            for i in 0..num_writers {
                let tmp = TempDir::new().unwrap();
                let (vfs, vfs_name) = make_local_vfs(tmp.path());
                let db_path = tmp.path().join("shared.db");
                let mut db = HaQLite::builder("test-bucket")
                    .prefix("test/")
                    .mode(HaMode::Shared)
                    .durability(haqlite::Durability::Synchronous)
                    .lease_store(lease_store.clone())
                    .manifest_store(manifest_store.clone())
                    .walrust_storage(storage.clone())
                    .turbolite_vfs(vfs, &vfs_name)
                    .instance_id(&format!("writer-{}", i))
                    .manifest_poll_interval(Duration::from_millis(50))
                    .write_timeout(Duration::from_secs(3))
                    .open(db_path.to_str().unwrap(), SCHEMA)
                    .await
                    .expect("open shared");
                dbs.push((Arc::new(db), tmp));
            }

            let success_count = Arc::new(AtomicU64::new(0));
            let mut handles = Vec::new();

            for (idx, (db, _tmp)) in dbs.iter().enumerate() {
                let db = db.clone();
                let success_count = success_count.clone();
                let key = idx as i64;
                let handle = tokio::spawn(async move {
                    let result = db.execute(
                        "INSERT INTO kv (k, v) VALUES (?1, ?2)",
                        &[
                            SqlValue::Integer(key),
                            SqlValue::Text(format!("val-{}", key)),
                        ],
                    ).await;
                    if result.is_ok() {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }
                    result
                });
                handles.push(handle);
            }

            for handle in handles {
                let _ = handle.await;
            }

            let successes = success_count.load(Ordering::SeqCst);

            let meta = manifest_store.meta("test/_manifest").await.unwrap();
            let manifest_version = meta.map(|m| m.version).unwrap_or(0);

            (successes, manifest_version)
        });

        let (successes, manifest_version) = result;
        if successes > 0 {
            assert!(manifest_version >= 1,
                "manifest version should be >= 1 when writes succeed");
        }
        assert!(manifest_version <= successes,
            "manifest version ({}) should not exceed successful writes ({})", manifest_version, successes);
    }
}
