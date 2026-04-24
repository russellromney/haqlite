//! Tests for pluggable ManifestStore in HaQLiteBuilder.
//!
//! Verifies that:
//! 1. Dedicated mode passes manifest_store to Coordinator (not None)
//! 2. Dedicated mode without manifest_store still works (backward compat)
//! 3. Shared mode manifest is published on write
//! 4. Shared mode sequential writes increment manifest version
//! 5. Two shared-mode writers see each other's data via manifest

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use common::InMemoryStorage;
use hadb::{InMemoryLeaseStore, LeaseStore};
use haqlite::{HaQLite, SqlValue};
use haqlite_turbolite::{Builder, Mode};
use turbodb::ManifestStore;
use turbodb_manifest_mem::MemManifestStore;
use turbolite::tiered::{CacheConfig, SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);
fn make_local_vfs(cache_dir: &std::path::Path) -> (SharedTurboliteVfs, String) {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    let vfs_name = format!("tms_{}", n);
    let config = TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        cache: CacheConfig {
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("create VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register VFS");
    (shared_vfs, vfs_name)
}

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET").expect("TIERED_TEST_BUCKET required")
}

fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL").ok()
}

#[cfg(feature = "turbolite-cloud")]
fn make_s3_vfs(cache_dir: &std::path::Path, s3_prefix: &str) -> (SharedTurboliteVfs, String) {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    let vfs_name = format!("tms_s3_{}", n);
    let config = TurboliteConfig {
        bucket: test_bucket(),
        prefix: s3_prefix.to_string(),
        cache_dir: cache_dir.to_path_buf(),
        endpoint_url: endpoint_url(),
        region: Some(std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string())),
        pages_per_group: 4,
        sub_pages_per_frame: 2,
        eager_index_load: false,
        runtime_handle: Some(tokio::runtime::Handle::current()),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("create S3-backed VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register VFS");
    (shared_vfs, vfs_name)
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS manifest_test (
    id INTEGER PRIMARY KEY,
    value TEXT NOT NULL
);";

// ============================================================================
// Dedicated mode
// ============================================================================

#[tokio::test]
async fn dedicated_mode_with_manifest_store() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("dedicated_manifest.db");
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let mut db = Builder::new("test-bucket")
        .prefix("test/").mode(Mode::Writer)
        .lease_store(lease_store)
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(storage)
        .instance_id("test-node")
        .forwarding_port(19201)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open dedicated mode with manifest store");

    db.execute_async(
        "INSERT INTO manifest_test (id, value) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("hello".into())],
    )
    .await
    .expect("insert");

    let val: String = db
        .query_row("SELECT value FROM manifest_test WHERE id = 1", &[], |r| {
            r.get(0)
        })
        .expect("select");
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn dedicated_mode_without_manifest_store_still_works() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("dedicated_no_manifest.db");
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    let mut db = Builder::new("test-bucket")
        .prefix("test/").mode(Mode::Writer)
        .lease_store(lease_store)
        .walrust_storage(storage)
        .instance_id("test-node")
        .forwarding_port(19202)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open dedicated mode without manifest store");

    db.execute_async(
        "INSERT INTO manifest_test (id, value) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("works".into())],
    )
    .await
    .expect("insert");

    let val: String = db
        .query_row("SELECT value FROM manifest_test WHERE id = 1", &[], |r| {
            r.get(0)
        })
        .expect("select");
    assert_eq!(val, "works");
}

// ============================================================================
// Shared mode: manifest_store required and actually used
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn shared_mode_manifest_published_on_write() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("shared_manifest.db");
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let (vfs, vfs_name) = make_local_vfs(tmp.path());
    let mut db = Builder::new("test-bucket")
        .prefix("test/").mode(Mode::MultiWriter).durability(turbodb::Durability::Cloud)
        .lease_store(lease_store)
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(storage)
        .turbolite_vfs(vfs, &vfs_name)
        .instance_id("writer-1")
        .write_timeout(Duration::from_secs(5))
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open shared mode");

    // Before write: no manifest
    let meta = manifest_store
        .meta("test/shared_manifest/_manifest")
        .await
        .expect("meta");
    assert!(meta.is_none(), "no manifest before first write");

    // Write triggers manifest publish
    db.execute_async(
        "INSERT INTO manifest_test (id, value) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("shared".into())],
    )
    .await
    .expect("insert");

    // After write: manifest exists with correct writer_id
    let meta = manifest_store
        .meta("test/shared_manifest/_manifest")
        .await
        .expect("meta")
        .expect("manifest should exist after write");
    assert_eq!(meta.version, 1);
    assert_eq!(meta.writer_id, "writer-1");
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_mode_sequential_writes_increment_manifest_version() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("shared_seq.db");
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let (vfs, vfs_name) = make_local_vfs(tmp.path());
    let mut db = Builder::new("test-bucket")
        .prefix("test/").mode(Mode::MultiWriter).durability(turbodb::Durability::Cloud)
        .lease_store(lease_store)
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(storage)
        .turbolite_vfs(vfs, &vfs_name)
        .instance_id("writer-1")
        .write_timeout(Duration::from_secs(5))
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open");

    for i in 0..3 {
        db.execute_async(
            "INSERT INTO manifest_test (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("v{}", i))],
        )
        .await
        .expect("insert");
    }

    let meta = manifest_store
        .meta("test/shared_seq/_manifest")
        .await
        .expect("meta")
        .expect("manifest should exist");
    assert_eq!(meta.version, 3, "3 writes should produce version 3");
}

// ============================================================================
// Regression: two writers see each other's data via manifest catch-up
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "turbolite-cloud")]
async fn shared_mode_two_writers_see_each_others_data() {
    let tmp1 = TempDir::new().expect("temp dir 1");
    let tmp2 = TempDir::new().expect("temp dir 2");
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let s3_prefix = format!("test/two_writers/{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).expect("time").as_nanos());
    let (vfs1, vfs_name1) = make_s3_vfs(tmp1.path(), &s3_prefix);
    let db1 = Builder::new(&test_bucket())
        .prefix("test/").mode(Mode::MultiWriter).durability(turbodb::Durability::Cloud)
        .lease_store(lease_store.clone())
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(storage.clone())
        .turbolite_vfs(vfs1, &vfs_name1)
        .instance_id("writer-1")
        .write_timeout(Duration::from_secs(5))
        .open(
            tmp1.path().join("two_writers.db").to_str().expect("path"),
            SCHEMA,
        )
        .await
        .expect("open db1");

    // Writer 1 writes
    db1.execute_async(
        "INSERT INTO manifest_test (id, value) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("from_writer_1".into())],
    )
    .await
    .expect("insert from writer 1");

    // Writer 2 opens and writes (should catch up from manifest first)
    let (vfs2, vfs_name2) = make_s3_vfs(tmp2.path(), &s3_prefix);
    let mut db2 = Builder::new(&test_bucket())
        .prefix("test/").mode(Mode::MultiWriter).durability(turbodb::Durability::Cloud)
        .lease_store(lease_store.clone())
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(storage.clone())
        .turbolite_vfs(vfs2, &vfs_name2)
        .instance_id("writer-2")
        .write_timeout(Duration::from_secs(5))
        .open(
            tmp2.path().join("two_writers.db").to_str().expect("path"),
            SCHEMA,
        )
        .await
        .expect("open db2");

    db2.execute_async(
        "INSERT INTO manifest_test (id, value) VALUES (?1, ?2)",
        &[SqlValue::Integer(2), SqlValue::Text("from_writer_2".into())],
    )
    .await
    .expect("insert from writer 2");

    // Writer 2 should see both rows (caught up from writer 1's manifest)
    let count: i64 = db2
        .query_row("SELECT COUNT(*) FROM manifest_test", &[], |r| r.get(0))
        .expect("count");
    assert_eq!(count, 2, "writer 2 should see both rows after catch-up");

    // Manifest should be at version 2
    let meta = manifest_store
        .meta("test/two_writers/_manifest")
        .await
        .expect("meta")
        .expect("manifest");
    assert_eq!(meta.version, 2);
    assert_eq!(meta.writer_id, "writer-2");
}
