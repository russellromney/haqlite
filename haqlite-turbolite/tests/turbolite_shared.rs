//! Phase Zenith-e: Turbolite + Shared mode integration tests.
//!
//! Uses turbolite local storage (no cloud/S3 needed).
//! Tests the per-write lease cycle with turbolite as the storage engine.

mod common;

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use hadb::InMemoryLeaseStore;
use haqlite::{HaQLite, SqlValue};
use haqlite_turbolite::{Builder, Mode};
use turbodb::ManifestStore;
use turbodb_manifest_mem::MemManifestStore;
use turbolite::tiered::{
    CacheConfig, CompressionConfig, SharedTurboliteVfs, TurboliteConfig, TurboliteVfs,
};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT);";

/// Build a Shared mode HaQLite backed by turbolite (local storage).
async fn build_turbolite_shared(
    cache_dir: &std::path::Path,
    db_name: &str,
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
            pages_per_group: 4, // small for testing
            sub_pages_per_frame: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("create turbolite VFS");
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    let vfs_for_register = shared_vfs.clone();

    turbolite::tiered::register_shared(vfs_name, vfs_for_register).expect("register turbolite VFS");

    let db_path = cache_dir.join(format!("{}.db", db_name));
    Builder::new()
        .prefix("test/")
        .mode(Mode::MultiWriter)
        .durability(turbodb::Durability::Cloud)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .turbolite_vfs(shared_vfs, vfs_name)
        .instance_id(instance_id)
        .manifest_poll_interval(Duration::from_millis(50))
        .write_timeout(Duration::from_secs(2))
        .open(db_path.to_str().expect("valid path"), SCHEMA)
        .await
        .expect("open turbolite shared mode")
}

// ============================================================================
// Happy path
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_shared_single_write_read() {
    let tmp = TempDir::new().expect("temp dir");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());
    let vfs_name = format!("tl_single_{}", std::process::id());

    let mut db = build_turbolite_shared(
        tmp.path(),
        "test",
        lease_store,
        manifest_store.clone(),
        "node-1",
        &vfs_name,
    )
    .await;

    // Write
    let rows = db
        .execute_async(
            "INSERT INTO t (id, val) VALUES (?1, ?2)",
            &[SqlValue::Integer(1), SqlValue::Text("hello".into())],
        )
        .await
        .expect("insert");
    assert_eq!(rows, 1);

    // Read
    let val: String = db
        .query_row("SELECT val FROM t WHERE id = 1", &[], |r| r.get(0))
        .expect("select");
    assert_eq!(val, "hello");
}

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_shared_manifest_published_after_write() {
    let tmp = TempDir::new().expect("temp dir");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());
    let vfs_name = format!("tl_manifest_{}", std::process::id());

    let mut db = build_turbolite_shared(
        tmp.path(),
        "test",
        lease_store,
        manifest_store.clone(),
        "node-1",
        &vfs_name,
    )
    .await;

    db.execute_async(
        "INSERT INTO t (id, val) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("world".into())],
    )
    .await
    .expect("insert");

    // Manifest should exist
    let meta = manifest_store
        .meta("test/test/_manifest")
        .await
        .expect("meta call")
        .expect("manifest should exist");
    assert_eq!(meta.version, 1);

    // Payload should be non-empty turbolite wire bytes (the envelope
    // is opaque here; decoding belongs to turbolite).
    let manifest = manifest_store
        .get("test/test/_manifest")
        .await
        .expect("get call")
        .expect("manifest should exist");
    assert!(
        !manifest.payload.is_empty(),
        "turbolite payload should be non-empty"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_shared_sequential_writes_increment_manifest() {
    let tmp = TempDir::new().expect("temp dir");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());
    let vfs_name = format!("tl_seq_{}", std::process::id());

    let mut db = build_turbolite_shared(
        tmp.path(),
        "test",
        lease_store,
        manifest_store.clone(),
        "node-1",
        &vfs_name,
    )
    .await;

    for i in 0..3 {
        db.execute_async(
            "INSERT INTO t (id, val) VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("v{}", i))],
        )
        .await
        .expect("insert");
    }

    let meta = manifest_store
        .meta("test/test/_manifest")
        .await
        .expect("meta call")
        .expect("manifest should exist");
    assert_eq!(meta.version, 3, "3 writes should produce manifest v3");

    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0))
        .expect("count");
    assert_eq!(count, 3);
}

// ============================================================================
// Manifest content verification
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_shared_manifest_has_turbolite_fields() {
    // After Phase Turbogenesis-b haqlite holds only opaque bytes.
    // Verify the round-trip lands turbolite's view with the expected
    // page_size / pages_per_group by decoding through the VFS itself —
    // the only thing that knows the wire format.
    let tmp = TempDir::new().expect("temp dir");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());
    let vfs_name = format!("tl_fields_{}", std::process::id());

    let mut db = build_turbolite_shared(
        tmp.path(),
        "test",
        lease_store,
        manifest_store.clone(),
        "node-1",
        &vfs_name,
    )
    .await;

    db.execute_async(
        "INSERT INTO t (id, val) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("data".into())],
    )
    .await
    .expect("insert");

    let manifest = manifest_store
        .get("test/test/_manifest")
        .await
        .expect("get")
        .expect("manifest");

    // Feed the payload back through a fresh VFS and read its manifest().
    let fresh_cache = tmp.path().join("fresh_cache");
    std::fs::create_dir_all(&fresh_cache).expect("cache dir");
    let fresh_config = TurboliteConfig {
        cache_dir: fresh_cache.clone(),
        cache: CacheConfig {
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let rt_handle = tokio::runtime::Handle::current();
    let backend: Arc<dyn hadb_storage::StorageBackend> =
        Arc::new(hadb_storage_local::LocalStorage::new(&fresh_cache));
    let fresh_vfs = TurboliteVfs::with_backend(fresh_config, backend, rt_handle).expect("vfs");
    let walrust = fresh_vfs
        .set_manifest_bytes(&manifest.payload)
        .expect("set_manifest_bytes");
    assert!(
        walrust.is_none(),
        "pure turbolite payload has no walrust fields"
    );
    let tl = fresh_vfs.manifest();
    assert!(tl.page_size > 0, "page_size should be > 0");
    assert_eq!(tl.pages_per_group, 4, "should match config");
}

// ============================================================================
// Data-fidelity regression: epoch + change_counter survive round-trip
// ============================================================================
//
// Pre-Turbogenesis-b, haqlite's converter layer from turbolite's
// `Manifest` → hadb's `Backend::Turbolite` silently dropped the
// `epoch` and `change_counter` fields (they had no home on the
// Backend variant). This regression guard drives the full payload
// path and asserts those fields survive.

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_manifest_bytes_round_trip_preserves_epoch_and_change_counter() {
    use std::collections::HashMap;
    use turbolite::tiered::{
        CacheConfig, GroupingStrategy, Manifest as TlManifest, TurboliteConfig, TurboliteVfs,
    };

    let tmp = TempDir::new().expect("temp dir");
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(&cache_dir).expect("cache dir");
    let config = TurboliteConfig {
        cache_dir: cache_dir.clone(),
        cache: CacheConfig {
            pages_per_group: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    let rt_handle = tokio::runtime::Handle::current();
    let backend: Arc<dyn hadb_storage::StorageBackend> =
        Arc::new(hadb_storage_local::LocalStorage::new(&cache_dir));
    let vfs = TurboliteVfs::with_backend(config, backend, rt_handle).expect("vfs");

    // Seed the VFS with a manifest whose epoch + change_counter are
    // non-zero so we can tell whether they round-trip.
    let mut seed = TlManifest {
        version: 1,
        change_counter: 4242,
        page_count: 16,
        page_size: 4096,
        pages_per_group: 4,
        sub_pages_per_frame: 0,
        strategy: GroupingStrategy::Positional,
        page_group_keys: vec!["pg/0_v1".into(), "pg/1_v1".into()],
        frame_tables: Vec::new(),
        group_pages: Vec::new(),
        btrees: HashMap::new(),
        interior_chunk_keys: HashMap::new(),
        index_chunk_keys: HashMap::new(),
        subframe_overrides: Vec::new(),
        page_index: HashMap::new(),
        btree_groups: HashMap::new(),
        page_to_tree_name: HashMap::new(),
        tree_name_to_groups: HashMap::new(),
        group_to_tree_name: HashMap::new(),
        db_header: None,
        epoch: 9,
    };
    seed.detect_and_normalize_strategy();
    vfs.set_manifest(seed);

    // Round-trip through the wire: bytes → fresh VFS → manifest().
    let bytes = vfs.manifest_bytes().expect("manifest_bytes");
    let tmp_b = TempDir::new().expect("temp dir b");
    let cache_b = tmp_b.path().join("cache");
    std::fs::create_dir_all(&cache_b).expect("cache b");
    let config_b = TurboliteConfig {
        cache_dir: cache_b.clone(),
        cache: CacheConfig {
            pages_per_group: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    let rt_handle_b = tokio::runtime::Handle::current();
    let backend_b: Arc<dyn hadb_storage::StorageBackend> =
        Arc::new(hadb_storage_local::LocalStorage::new(&cache_b));
    let vfs_b = TurboliteVfs::with_backend(config_b, backend_b, rt_handle_b).expect("vfs b");
    let walrust = vfs_b
        .set_manifest_bytes(&bytes)
        .expect("set_manifest_bytes");
    assert!(walrust.is_none());
    let got = vfs_b.manifest();
    assert_eq!(got.epoch, 9, "epoch must survive manifest round-trip");
    assert_eq!(
        got.change_counter, 4242,
        "change_counter must survive manifest round-trip"
    );
}

// ============================================================================
// Edge cases
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_shared_empty_table_read() {
    let tmp = TempDir::new().expect("temp dir");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());
    let vfs_name = format!("tl_empty_{}", std::process::id());

    let mut db = build_turbolite_shared(
        tmp.path(),
        "test",
        lease_store,
        manifest_store,
        "node-1",
        &vfs_name,
    )
    .await;

    // Schema is deferred to first write. Do a no-op write to create the table.
    db.execute("INSERT INTO t VALUES (1, 'init')", &[])
        .expect("init write");
    db.execute("DELETE FROM t WHERE id = 1", &[])
        .expect("cleanup");

    // Read from empty table
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM t", &[], |r| r.get(0))
        .expect("count");
    assert_eq!(count, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_shared_manifest_writer_id_set() {
    let tmp = TempDir::new().expect("temp dir");
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());
    let vfs_name = format!("tl_writer_{}", std::process::id());

    let mut db = build_turbolite_shared(
        tmp.path(),
        "test",
        lease_store,
        manifest_store.clone(),
        "my-instance-42",
        &vfs_name,
    )
    .await;

    db.execute_async("INSERT INTO t (id, val) VALUES (1, 'x')", &[])
        .await
        .expect("insert");

    let manifest = manifest_store
        .get("test/test/_manifest")
        .await
        .expect("get")
        .expect("manifest");
    assert_eq!(manifest.writer_id, "my-instance-42");
}
