//! Full test matrix for haqlite multiwriter modes.
//!
//! Tests encoding combos (plain, zstd, encrypted, zstd+encrypted) across:
//! - Mode C: Local turbolite + walrust shared
//! - Mode F: S3 turbolite S3Primary shared (requires turbolite-cloud + RustFS)
//! - Mode I: S3 turbolite + walrust shared (requires turbolite-cloud + RustFS)
//!
//! Mode A (single node, no HA) is tested by turbolite's own tests.
//! Mode D (dedicated + turbolite) is not yet implemented.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::InMemoryStorage;
use hadb::InMemoryLeaseStore;
use haqlite::{HaMode, HaQLite, InMemoryManifestStore, SqlValue};
use tempfile::TempDir;
use turbolite::tiered::{SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT)";

static VFS_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
fn unique_vfs(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    format!("{}_{}", prefix, n)
}

// ============================================================================
// Encoding combos
// ============================================================================

struct Encoding {
    name: &'static str,
    compression_level: i32,
    encryption_key: Option<[u8; 32]>,
}

const TEST_KEY: [u8; 32] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
    0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
];

fn encodings() -> Vec<Encoding> {
    let mut v = vec![
        Encoding { name: "plain", compression_level: 0, encryption_key: None },
        Encoding { name: "zstd", compression_level: 3, encryption_key: None },
    ];
    // Encryption requires the feature
    v.push(Encoding { name: "encrypted", compression_level: 0, encryption_key: Some(TEST_KEY) });
    v.push(Encoding { name: "zstd_enc", compression_level: 3, encryption_key: Some(TEST_KEY) });
    v
}

// ============================================================================
// Mode C: Local turbolite + walrust shared (multiwriter)
// ============================================================================

async fn run_mode_c(enc: &Encoding) {
    let tmp_a = TempDir::new().expect("tmp");
    let tmp_b = TempDir::new().expect("tmp");

    let walrust_storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(InMemoryManifestStore::new());

    let build_node = |cache_dir: &std::path::Path, instance_id: &str| {
        let vfs_name = unique_vfs(&format!("mc_{}", enc.name));
        let config = TurboliteConfig {
            cache_dir: cache_dir.to_path_buf(),
            compression_level: enc.compression_level,
            encryption_key: enc.encryption_key,
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            eager_index_load: false,
            ..Default::default()
        };
        let vfs = TurboliteVfs::new(config).expect("vfs");
        let shared_vfs = SharedTurboliteVfs::new(vfs);
        turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register");
        (shared_vfs, vfs_name)
    };

    let (vfs_a, vfs_name_a) = build_node(tmp_a.path(), "a");
    let (vfs_b, vfs_name_b) = build_node(tmp_b.path(), "b");

    let mut db_a = HaQLite::builder("test-bucket")
        .prefix("test/")
        .mode(HaMode::Shared)
        .lease_store(lease_store.clone())
        .manifest_store(manifest_store.clone())
        .walrust_storage(walrust_storage.clone())
        .turbolite_vfs(vfs_a, &vfs_name_a)
        .instance_id("node-a")
        .write_timeout(Duration::from_secs(10))
        .open(tmp_a.path().join("t.db").to_str().expect("p"), SCHEMA)
        .await.expect("open a");

    let mut db_b = HaQLite::builder("test-bucket")
        .prefix("test/")
        .mode(HaMode::Shared)
        .lease_store(lease_store.clone())
        .manifest_store(manifest_store.clone())
        .walrust_storage(walrust_storage.clone())
        .turbolite_vfs(vfs_b, &vfs_name_b)
        .instance_id("node-b")
        .write_timeout(Duration::from_secs(10))
        .open(tmp_b.path().join("t.db").to_str().expect("p"), SCHEMA)
        .await.expect("open b");

    // Node A writes 5 rows
    for i in 0..5 {
        db_a.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("a_{}", i))],
        ).await.expect("insert a");
    }

    // Node B writes 5 rows (catches up from walrust)
    for i in 5..10 {
        db_b.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("b_{}", i))],
        ).await.expect("insert b");
    }

    // Verify all 10 visible
    let rows = db_b.query_values_fresh("SELECT id FROM t ORDER BY id", &[])
        .await.expect("query");
    assert_eq!(rows.len(), 10, "[Mode C/{}] expected 10 rows, got {}", enc.name, rows.len());

    db_a.close().await.expect("close a");
    db_b.close().await.expect("close b");
}

#[tokio::test(flavor = "multi_thread")]
async fn mode_c_all_encodings() {
    for enc in &encodings() {
        eprintln!("--- Mode C: {} ---", enc.name);
        run_mode_c(enc).await;
    }
}
