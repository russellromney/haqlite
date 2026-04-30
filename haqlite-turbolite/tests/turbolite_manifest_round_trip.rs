//! Phase Turbogenesis-b regression guard: turbolite manifest bytes
//! round-trip preserves `epoch` and `change_counter`.
//!
//! Pre-Turbogenesis-b, haqlite's converter layer from turbolite's
//! `Manifest` → hadb's `Backend::Turbolite` silently dropped the
//! `epoch` and `change_counter` fields (they had no home on the
//! Backend variant). This test drives the full payload path and
//! asserts those fields survive.
//!
//! Spun out of the deleted `turbolite_shared.rs` (Phase Košice). All
//! the SharedWriter-dependent tests in that file targeted
//! functionality that has been unimplemented in haqlite-turbolite
//! since commit 28ed14a; this regression guard does not touch
//! HaMode at all and stays.

use std::collections::HashMap;
use std::sync::Arc;

use tempfile::TempDir;
use turbolite::tiered::{
    CacheConfig, GroupingStrategy, Manifest as TlManifest, TurboliteConfig, TurboliteVfs,
};

#[tokio::test(flavor = "multi_thread")]
async fn turbolite_manifest_bytes_round_trip_preserves_epoch_and_change_counter() {
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
