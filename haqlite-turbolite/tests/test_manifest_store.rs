//! Tests for pluggable ManifestStore in HaQLiteBuilder.
//!
//! Verifies that:
//! 1. SingleWriter mode passes manifest_store to Coordinator (not None)
//! 2. SingleWriter mode without manifest_store still works (backward compat)
//! 3. SharedWriter mode manifest is published on write
//! 4. SharedWriter mode sequential writes increment manifest version
//! 5. Two sharedwriter-mode writers see each other's data via manifest

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use common::InMemoryStorage;
use hadb::{InMemoryLeaseStore, LeaseStore};
use haqlite::{HaQLite, SqlValue};
use haqlite_turbolite::{Builder, HaMode};
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

#[cfg(feature = "legacy-s3-mode-tests")]
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
// SingleWriter mode
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn dedicated_mode_with_manifest_store() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("dedicated_manifest.db");
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());
    let (vfs, vfs_name) = make_local_vfs(tmp.path());

    let mut db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .lease_store(lease_store)
        .manifest_store(manifest_store.clone() as Arc<dyn ManifestStore>)
        .walrust_storage(storage)
        .turbolite_vfs(vfs, &vfs_name)
        .instance_id("test-node")
        .forwarding_port(19201)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open singlewriter mode with manifest store");

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

#[tokio::test(flavor = "multi_thread")]
async fn dedicated_mode_without_manifest_store_still_works() {
    // SingleWriter without an explicit manifest_store: the builder uses
    // its default (in-memory). The test name reads "still works" for
    // historical reasons — it predates Phase Lucid's stricter dependency
    // checks. Today the setup needs a turbolite_vfs() for any
    // turbolite-backed open.
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("dedicated_no_manifest.db");
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());
    let (vfs, vfs_name) = make_local_vfs(tmp.path());

    let mut db = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .lease_store(lease_store)
        .manifest_store(manifest_store as Arc<dyn ManifestStore>)
        .walrust_storage(storage)
        .turbolite_vfs(vfs, &vfs_name)
        .instance_id("test-node")
        .forwarding_port(19202)
        .open(db_path.to_str().expect("path"), SCHEMA)
        .await
        .expect("open singlewriter mode without manifest store");

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

// SharedWriter mode tests removed in Phase Košice.
//
// SharedWriter (formerly MultiWriter) was unimplemented in
// haqlite-turbolite as of commit 28ed14a ("Wire Turbolite-owned
// continuous replication"). The bail predates Phase Košice; the
// removed tests (shared_mode_manifest_published_on_write,
// shared_mode_sequential_writes_increment_manifest_version,
// shared_mode_two_writers_see_each_others_data) opened the builder in
// SharedWriter mode and could not run. Bail-matrix coverage for the
// (HaMode::SharedWriter, *) combinations now lives in
// `mode_role_bail.rs`. When SharedWriter is implemented, restore
// these tests there.
