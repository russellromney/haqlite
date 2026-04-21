//! Phase Zenith-e: Turbolite + Shared mode integration tests.
//!
//! Uses turbolite local storage (no cloud/S3 needed).
//! Tests the per-write lease cycle with turbolite as the storage engine.


mod common;

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use hadb::InMemoryLeaseStore;
use haqlite::{HaMode, HaQLite, ManifestStore, SqlValue};
use turbodb::Backend;
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
        compression: CompressionConfig { level: 1, ..Default::default() },
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

    turbolite::tiered::register_shared(vfs_name, vfs_for_register)
        .expect("register turbolite VFS");

    let db_path = cache_dir.join(format!("{}.db", db_name));
    HaQLite::builder("unused-bucket")
        .prefix("test/")
        .mode(HaMode::Shared)
        .durability(haqlite::Durability::Synchronous)
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

    // Manifest should be Turbolite variant
    let manifest = manifest_store
        .get("test/test/_manifest")
        .await
        .expect("get call")
        .expect("manifest should exist");
    assert!(
        matches!(manifest.storage, Backend::Turbolite { .. }),
        "expected Turbolite storage variant, got {:?}",
        manifest.storage
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

    match &manifest.storage {
        Backend::Turbolite {
            page_size,
            pages_per_group,
            ..
        } => {
            // SQLite default page size is 4096
            assert!(*page_size > 0, "page_size should be > 0");
            assert_eq!(*pages_per_group, 4, "should match config");
        }
        other => panic!("expected Turbolite variant, got {:?}", other),
    }
}

// ============================================================================
// Conversion round-trip
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_manifest_conversion_roundtrip() {
    use haqlite::turbolite_replicator::{backend_to_turbolite, turbolite_to_backend};
    use std::collections::HashMap;
    use turbolite::tiered::{
        BTreeManifestEntry, FrameEntry, GroupingStrategy, Manifest, SubframeOverride,
    };

    let tl = Manifest {
        version: 1,
        change_counter: 10,
        page_count: 50,
        page_size: 4096,
        pages_per_group: 4,
        sub_pages_per_frame: 2,
        strategy: GroupingStrategy::BTreeAware,
        page_group_keys: vec!["pg/0_v1".into()],
        frame_tables: vec![vec![FrameEntry {
            offset: 0,
            len: 4096,
        }]],
        group_pages: vec![vec![0, 1, 2, 3]],
        btrees: HashMap::from([(
            0,
            BTreeManifestEntry {
                name: "t".into(),
                obj_type: "table".into(),
                group_ids: vec![0],
            },
        )]),
        interior_chunk_keys: HashMap::from([(0, "ic/0".into())]),
        index_chunk_keys: HashMap::new(),
        subframe_overrides: vec![HashMap::from([(
            0,
            SubframeOverride {
                key: "sf/0".into(),
                entry: FrameEntry {
                    offset: 0,
                    len: 512,
                },
            },
        )])],
        page_index: HashMap::new(),
        btree_groups: HashMap::new(),
        page_to_tree_name: HashMap::new(),
        tree_name_to_groups: HashMap::new(),
        group_to_tree_name: HashMap::new(),
        db_header: None,
        epoch: 0,
    };

    let backend = turbolite_to_backend(&tl);
    let back = backend_to_turbolite(&backend);

    assert_eq!(back.page_count, tl.page_count);
    assert_eq!(back.page_size, tl.page_size);
    assert_eq!(back.pages_per_group, tl.pages_per_group);
    assert_eq!(back.sub_pages_per_frame, tl.sub_pages_per_frame);
    assert_eq!(back.page_group_keys, tl.page_group_keys);
    assert_eq!(back.frame_tables.len(), tl.frame_tables.len());
    assert_eq!(back.group_pages, tl.group_pages);
    assert_eq!(back.btrees.len(), tl.btrees.len());
    assert_eq!(back.interior_chunk_keys.len(), tl.interior_chunk_keys.len());
    assert_eq!(
        back.subframe_overrides.len(),
        tl.subframe_overrides.len()
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
    db.execute("INSERT INTO t VALUES (1, 'init')", &[]).expect("init write");
    db.execute("DELETE FROM t WHERE id = 1", &[]).expect("cleanup");

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

    db.execute_async(
        "INSERT INTO t (id, val) VALUES (1, 'x')",
        &[],
    )
    .await
    .expect("insert");

    let manifest = manifest_store
        .get("test/test/_manifest")
        .await
        .expect("get")
        .expect("manifest");
    assert_eq!(manifest.writer_id, "my-instance-42");
}
