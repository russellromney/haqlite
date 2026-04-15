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
    // Single-writer only (local VFS), so only 1 possible value. 5 cases
    // for variance in the proptest seed, not 256.
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(5))]
    /// Test Shared mode write path: lease acquire, execute, release.
    ///
    /// Uses local-only VFS (no S3), so only single-writer is valid.
    /// Multi-writer coordination needs shared storage (S3 or HTTP) so
    /// writers can see each other's manifests via xSync. That requires
    /// /v1/sync/pages endpoints and will be tested as an integration
    /// test against a running storage gateway.
    #[test]
    fn shared_mode_lease_write_release(
        num_writers in 1..2usize, // single writer only (local VFS)
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
                    let result = db.execute_async(
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

            // Check turbolite VFS manifest version from the last writer that succeeded.
            // Each successful write bumps the VFS manifest version via xSync.
            let max_vfs_version = dbs.iter()
                .map(|(db, _)| db.connection().unwrap().lock()
                    .query_row("SELECT COUNT(*) FROM kv", [], |r: &rusqlite::Row| r.get::<_, i64>(0))
                    .unwrap_or(0) as u64)
                .max()
                .unwrap_or(0);

            (successes, max_vfs_version)
        });

        let (successes, row_count) = result;
        // Serialized writes: at least one must succeed, and the row count
        // must match successes (no lost writes, no duplicates).
        assert!(successes >= 1,
            "at least one writer should succeed");
        assert_eq!(row_count, successes,
            "row count ({}) should equal successful writes ({})", row_count, successes);
    }
}
