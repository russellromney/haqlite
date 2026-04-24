//! Full test matrix for haqlite multiwriter modes.
//!
//! Tests encoding combos (plain, zstd, encrypted, zstd+encrypted) across:
//! - Mode Sync: S3 turbolite S3Primary + Synchronous durability (requires turbolite-cloud)
//!
//! Mode A (single node, no HA) is tested by turbolite's own tests.

#![cfg(feature = "turbolite-cloud")]

use std::sync::Arc;
use std::time::Duration;

use hadb::InMemoryLeaseStore;
use haqlite::{HaQLite, SqlValue};
use haqlite_turbolite::{Builder, Mode};
use turbodb_manifest_mem::MemManifestStore;
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
// S3 helpers
// ============================================================================

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET")
        .expect("TIERED_TEST_BUCKET required")
}

fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL").ok()
}

fn unique_prefix(name: &str) -> String {
    format!(
        "test/mode_matrix/{}/{}",
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    )
}

// ============================================================================
// Mode Sync: S3Primary + Synchronous durability (multiwriter)
// ============================================================================

async fn run_mode_sync(enc: &Encoding) {
    let tmp_a = TempDir::new().expect("tmp");
    let tmp_b = TempDir::new().expect("tmp");

    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemManifestStore::new());

    let shared_prefix = unique_prefix(enc.name);

    let build_node = |cache_dir: &std::path::Path, instance_id: &str| {
        let vfs_name = unique_vfs(&format!("ms_{}", enc.name));
        let config = TurboliteConfig {
            bucket: test_bucket(),
            prefix: shared_prefix.clone(),
            cache_dir: cache_dir.to_path_buf(),
            endpoint_url: endpoint_url(),
            region: Some("auto".to_string()),
            compression_level: enc.compression_level,
            encryption_key: enc.encryption_key,
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            eager_index_load: false,
            runtime_handle: Some(tokio::runtime::Handle::current()),
            ..Default::default()
        };
        let vfs = TurboliteVfs::new(config).expect("vfs");
        let shared_vfs = SharedTurboliteVfs::new(vfs);
        turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register");
        (shared_vfs, vfs_name)
    };

    // Create and open nodes sequentially so each VFS sees the latest S3 state.
    // VFS fetches the S3 manifest at creation time; creating both upfront means
    // the second VFS would have a stale view of S3.
    let (vfs_a, vfs_name_a) = build_node(tmp_a.path(), "a");
    let mut db_a = Builder::new("unused-bucket")
        .prefix("test/").mode(Mode::MultiWriter).durability(turbodb::Durability::Cloud)
        .lease_store(lease_store.clone())
        .manifest_store(manifest_store.clone())
        .turbolite_vfs(vfs_a, &vfs_name_a)
        .instance_id("node-a")
        .write_timeout(Duration::from_secs(10))
        .open(tmp_a.path().join("t.db").to_str().expect("p"), SCHEMA)
        .await.expect("open a");

    let (vfs_b, vfs_name_b) = build_node(tmp_b.path(), "b");
    let mut db_b = Builder::new("unused-bucket")
        .prefix("test/").mode(Mode::MultiWriter).durability(turbodb::Durability::Cloud)
        .lease_store(lease_store.clone())
        .manifest_store(manifest_store.clone())
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

    // Node B writes 5 rows (catches up via S3 manifest)
    for i in 5..10 {
        db_b.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("b_{}", i))],
        ).await.expect("insert b");
    }

    // Verify all 10 visible
    let rows = db_b.query_values_fresh("SELECT id FROM t ORDER BY id", &[])
        .await.expect("query");
    assert_eq!(rows.len(), 10, "[Mode Sync/{}] expected 10 rows, got {}", enc.name, rows.len());

    db_a.close().await.expect("close a");
    db_b.close().await.expect("close b");
}

#[tokio::test(flavor = "multi_thread")]
async fn mode_sync_all_encodings() {
    for enc in &encodings() {
        eprintln!("--- Mode Sync: {} ---", enc.name);
        run_mode_sync(enc).await;
    }
}
