//! Tests for haqlite::ops — CLI operations (list, verify, compact, snapshot, replicate).
//!
//! Uses an in-memory storage backend to avoid S3 dependencies.

mod common;

use std::path::Path;

use anyhow::Result;
use async_trait::async_trait;

use common::InMemoryStorage;
use haqlite::ops::{self, VerifyFileResult};

// ============================================================================
// Helpers
// ============================================================================

/// Create a minimal SQLite database at the given path with a table.
fn create_test_db(path: &Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT);
         INSERT INTO t (val) VALUES ('hello');",
    )
    .unwrap();
}

/// Encode a snapshot LTX file from a database, returning the raw bytes.
fn encode_snapshot(db_path: &Path, txid: u64) -> Vec<u8> {
    let page_size = {
        let conn = rusqlite::Connection::open(db_path).unwrap();
        let ps: u32 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .unwrap();
        ps
    };
    let mut buf = Vec::new();
    walrust::ltx::encode_snapshot(&mut buf, db_path, page_size, txid).unwrap();
    buf
}

// ============================================================================
// discover_ltx_files
// ============================================================================

#[tokio::test]
async fn test_discover_empty_bucket() {
    let storage = InMemoryStorage::new();
    let files = ops::discover_ltx_files(&storage, "","mydb").await.unwrap();
    assert!(files.is_empty());
}

#[tokio::test]
async fn test_discover_finds_incrementals() {
    let storage = InMemoryStorage::new();
    // Generation 0 = incrementals
    storage
        .insert("mydb/0000/0000000000000001-0000000000000001.ltx", vec![1])
        .await;
    storage
        .insert("mydb/0000/0000000000000002-0000000000000003.ltx", vec![2])
        .await;

    let files = ops::discover_ltx_files(&storage, "","mydb").await.unwrap();
    assert_eq!(files.len(), 2);
    assert!(!files[0].is_snapshot);
    assert_eq!(files[0].min_txid, 1);
    assert_eq!(files[0].max_txid, 1);
    assert_eq!(files[0].generation, 0);
    assert!(!files[1].is_snapshot);
    assert_eq!(files[1].min_txid, 2);
    assert_eq!(files[1].max_txid, 3);
}

#[tokio::test]
async fn test_discover_finds_snapshots() {
    let storage = InMemoryStorage::new();
    // Generation 1 = snapshot
    storage
        .insert("mydb/0001/0000000000000001-0000000000000005.ltx", vec![1])
        .await;

    let files = ops::discover_ltx_files(&storage, "","mydb").await.unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].is_snapshot);
    assert_eq!(files[0].min_txid, 1);
    assert_eq!(files[0].max_txid, 5);
    assert_eq!(files[0].generation, 1);
}

#[tokio::test]
async fn test_discover_ignores_non_ltx_files() {
    let storage = InMemoryStorage::new();
    storage.insert("mydb/0000/manifest.json", vec![]).await;
    storage.insert("mydb/0000/state.json", vec![]).await;
    storage
        .insert("mydb/0000/0000000000000001-0000000000000001.ltx", vec![1])
        .await;

    let files = ops::discover_ltx_files(&storage, "","mydb").await.unwrap();
    assert_eq!(files.len(), 1);
}

#[tokio::test]
async fn test_discover_ignores_other_databases() {
    let storage = InMemoryStorage::new();
    storage
        .insert("mydb/0000/0000000000000001-0000000000000001.ltx", vec![1])
        .await;
    storage
        .insert("other/0000/0000000000000001-0000000000000001.ltx", vec![1])
        .await;

    let files = ops::discover_ltx_files(&storage, "","mydb").await.unwrap();
    assert_eq!(files.len(), 1);
}

#[tokio::test]
async fn test_discover_sorted_by_min_txid() {
    let storage = InMemoryStorage::new();
    storage
        .insert("db/0000/0000000000000005-0000000000000005.ltx", vec![1])
        .await;
    storage
        .insert("db/0000/0000000000000001-0000000000000001.ltx", vec![1])
        .await;
    storage
        .insert("db/0000/0000000000000003-0000000000000003.ltx", vec![1])
        .await;

    let files = ops::discover_ltx_files(&storage, "","db").await.unwrap();
    assert_eq!(files.len(), 3);
    assert_eq!(files[0].min_txid, 1);
    assert_eq!(files[1].min_txid, 3);
    assert_eq!(files[2].min_txid, 5);
}

// ============================================================================
// discover_databases
// ============================================================================

#[tokio::test]
async fn test_discover_databases_empty() {
    let storage = InMemoryStorage::new();
    let dbs = ops::discover_databases(&storage, "").await.unwrap();
    assert!(dbs.is_empty());
}

#[tokio::test]
async fn test_discover_databases_multiple() {
    let storage = InMemoryStorage::new();
    storage
        .insert("alpha/0000/0000000000000001-0000000000000001.ltx", vec![1])
        .await;
    storage
        .insert("beta/0001/0000000000000001-0000000000000001.ltx", vec![1])
        .await;
    storage
        .insert("gamma/0000/0000000000000001-0000000000000001.ltx", vec![1])
        .await;

    let dbs = ops::discover_databases(&storage, "").await.unwrap();
    assert_eq!(dbs, vec!["alpha", "beta", "gamma"]);
}

// ============================================================================
// list_databases
// ============================================================================

#[tokio::test]
async fn test_list_empty_bucket() {
    let storage = InMemoryStorage::new();
    let dbs = ops::list_databases(&storage, "").await.unwrap();
    assert!(dbs.is_empty());
}

#[tokio::test]
async fn test_list_with_snapshot_and_incrementals() {
    let storage = InMemoryStorage::new();
    // Snapshot at gen 1
    storage
        .insert("mydb/0001/0000000000000001-0000000000000005.ltx", vec![1])
        .await;
    // Two incrementals at gen 0
    storage
        .insert("mydb/0000/0000000000000006-0000000000000006.ltx", vec![1])
        .await;
    storage
        .insert("mydb/0000/0000000000000007-0000000000000007.ltx", vec![1])
        .await;

    let dbs = ops::list_databases(&storage, "").await.unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(dbs[0].name, "mydb");
    assert_eq!(dbs[0].max_txid, 7);
    assert_eq!(dbs[0].incremental_count, 2);
    assert!(dbs[0].latest_snapshot.is_some());
    let snap = dbs[0].latest_snapshot.as_ref().unwrap();
    assert_eq!(snap.generation, 1);
    assert_eq!(snap.max_txid, 5);
}

#[tokio::test]
async fn test_list_no_snapshot() {
    let storage = InMemoryStorage::new();
    storage
        .insert("mydb/0000/0000000000000001-0000000000000001.ltx", vec![1])
        .await;

    let dbs = ops::list_databases(&storage, "").await.unwrap();
    assert_eq!(dbs.len(), 1);
    assert!(dbs[0].latest_snapshot.is_none());
}

// ============================================================================
// verify_database
// ============================================================================

#[tokio::test]
async fn test_verify_empty_db_errors() {
    let storage = InMemoryStorage::new();
    let err = ops::verify_database(&storage, "", "mydb").await.unwrap_err();
    assert!(err.to_string().contains("No LTX files found"));
}

#[tokio::test]
async fn test_verify_no_snapshot_errors() {
    let storage = InMemoryStorage::new();
    // Only incrementals, no snapshot
    storage
        .insert("mydb/0000/0000000000000001-0000000000000001.ltx", vec![1])
        .await;

    let err = ops::verify_database(&storage, "", "mydb").await.unwrap_err();
    assert!(err.to_string().contains("No snapshot found"));
}

#[tokio::test]
async fn test_verify_valid_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);

    let ltx_data = encode_snapshot(&db_path, 1);

    let storage = InMemoryStorage::new();
    storage
        .insert("test/0001/0000000000000001-0000000000000001.ltx", ltx_data)
        .await;

    let result = ops::verify_database(&storage, "", "test").await.unwrap();
    assert_eq!(result.total_files, 1);
    assert_eq!(result.verified_count, 1);
    assert!(result.is_valid());
}

#[tokio::test]
async fn test_verify_corrupt_file_detected() {
    let storage = InMemoryStorage::new();
    // Corrupt data that doesn't parse as LTX
    storage
        .insert(
            "mydb/0001/0000000000000001-0000000000000001.ltx",
            vec![0xFF, 0xFF, 0xFF, 0xFF],
        )
        .await;

    let result = ops::verify_database(&storage, "", "mydb").await.unwrap();
    assert!(!result.is_valid());
    assert_eq!(result.verified_count, 0);
    match &result.file_results[0].1 {
        VerifyFileResult::ChecksumFailed(_) => {}
        other => panic!("Expected ChecksumFailed, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_verify_detects_txid_gap() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);

    let snapshot_data = encode_snapshot(&db_path, 1);

    let storage = InMemoryStorage::new();
    // Snapshot
    storage
        .insert(
            "test/0001/0000000000000001-0000000000000001.ltx",
            snapshot_data.clone(),
        )
        .await;
    // Incremental at TXID 2-2
    storage
        .insert(
            "test/0000/0000000000000002-0000000000000002.ltx",
            snapshot_data.clone(),
        )
        .await;
    // Incremental at TXID 5-5 (gap: missing 3-4)
    storage
        .insert(
            "test/0000/0000000000000005-0000000000000005.ltx",
            snapshot_data,
        )
        .await;

    let result = ops::verify_database(&storage, "", "test").await.unwrap();
    assert!(!result.is_valid());
    assert_eq!(result.continuity_issues.len(), 1);
    assert!(result.continuity_issues[0].contains("expected 3"));
}

// ============================================================================
// compact
// ============================================================================

#[tokio::test]
async fn test_compact_no_snapshots() {
    let storage = InMemoryStorage::new();
    let plan = ops::plan_compact(&storage, "", "mydb", 5).await.unwrap();
    assert!(plan.keep_snapshots.is_empty());
    assert!(plan.delete_snapshots.is_empty());
}

#[tokio::test]
async fn test_compact_fewer_than_keep() {
    let storage = InMemoryStorage::new();
    storage
        .insert("mydb/0001/0000000000000001-0000000000000001.ltx", vec![1])
        .await;
    storage
        .insert("mydb/0002/0000000000000001-0000000000000005.ltx", vec![1])
        .await;

    // Keep 5, only 2 exist
    let plan = ops::plan_compact(&storage, "", "mydb", 5).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 2);
    assert!(plan.delete_snapshots.is_empty());
}

#[tokio::test]
async fn test_compact_deletes_oldest() {
    let storage = InMemoryStorage::new();
    // 5 snapshots with increasing TXIDs
    for i in 1..=5u64 {
        let key = format!(
            "mydb/{:04x}/0000000000000001-{:016x}.ltx",
            i, i * 10
        );
        storage.insert(&key, vec![1]).await;
    }

    // Keep 2, delete 3
    let plan = ops::plan_compact(&storage, "", "mydb", 2).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 2);
    assert_eq!(plan.delete_snapshots.len(), 3);

    // Kept snapshots should be the newest (highest TXID)
    assert_eq!(plan.keep_snapshots[0].max_txid, 50);
    assert_eq!(plan.keep_snapshots[1].max_txid, 40);

    // Deleted should be the oldest
    assert_eq!(plan.delete_snapshots[0].max_txid, 30);
    assert_eq!(plan.delete_snapshots[1].max_txid, 20);
    assert_eq!(plan.delete_snapshots[2].max_txid, 10);
}

#[tokio::test]
async fn test_compact_execute_deletes_files() {
    let storage = InMemoryStorage::new();
    for i in 1..=5u64 {
        let key = format!(
            "mydb/{:04x}/0000000000000001-{:016x}.ltx",
            i, i * 10
        );
        storage.insert(&key, vec![1]).await;
    }

    let plan = ops::plan_compact(&storage, "", "mydb", 2).await.unwrap();
    let deleted = ops::execute_compact(&storage, &plan).await.unwrap();
    assert_eq!(deleted, 3);

    // Verify only 2 keys remain
    let remaining = storage.keys().await;
    assert_eq!(remaining.len(), 2);
}

#[tokio::test]
async fn test_compact_ignores_incrementals() {
    let storage = InMemoryStorage::new();
    // Snapshot
    storage
        .insert("mydb/0001/0000000000000001-0000000000000001.ltx", vec![1])
        .await;
    // Incrementals (generation 0) should not be touched
    storage
        .insert("mydb/0000/0000000000000002-0000000000000002.ltx", vec![1])
        .await;
    storage
        .insert("mydb/0000/0000000000000003-0000000000000003.ltx", vec![1])
        .await;

    let plan = ops::plan_compact(&storage, "", "mydb", 1).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 1);
    assert!(plan.delete_snapshots.is_empty());
    // The 2 incrementals should not appear in the plan at all
    assert!(plan.keep_snapshots[0].is_snapshot);
}

// ============================================================================
// snapshot_database
// ============================================================================

#[tokio::test]
async fn test_snapshot_nonexistent_db() {
    let storage = InMemoryStorage::new();
    let err = ops::snapshot_database(&storage, "",Path::new("/nonexistent/test.db"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Database not found"));
}

#[tokio::test]
async fn test_snapshot_creates_ltx_file() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("snap.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();
    let result = ops::snapshot_database(&storage, "",&db_path).await.unwrap();
    // SQLite file change counter is 2 after CREATE TABLE + INSERT (two transactions).
    assert_eq!(result.txid, 2);

    // Verify the LTX file was uploaded
    let keys = storage.keys().await;
    assert_eq!(keys.len(), 1);
    // Key should be: snap/0001/0000000000000001-0000000000000002.ltx
    assert!(keys[0].starts_with("snap/"));
    assert!(keys[0].contains("0001/"));
    assert!(keys[0].ends_with(".ltx"));
}

#[tokio::test]
async fn test_snapshot_increments_txid() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("snap.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();

    // First snapshot (change counter = 2 after CREATE + INSERT)
    let result1 = ops::snapshot_database(&storage, "",&db_path).await.unwrap();
    assert_eq!(result1.txid, 2);

    // Second snapshot: take_snapshot reads the same file change counter (2),
    // but current_txid is now 2, so cc > current_txid fails. The snapshot
    // can only advance if the DB has been modified. Modify it to get txid=3.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('world')", []).unwrap();
    }
    let result2 = ops::snapshot_database(&storage, "",&db_path).await.unwrap();
    assert_eq!(result2.txid, 3);

    let keys = storage.keys().await;
    assert_eq!(keys.len(), 2);
}

// ============================================================================
// replica state
// ============================================================================

#[tokio::test]
async fn test_replica_state_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("replica.db");

    // No state file yet
    assert_eq!(ops::load_replica_state(&db_path), 0);

    // Save state
    ops::save_replica_state(&db_path, 42).unwrap();
    assert_eq!(ops::load_replica_state(&db_path), 42);

    // Update state
    ops::save_replica_state(&db_path, 100).unwrap();
    assert_eq!(ops::load_replica_state(&db_path), 100);
}

#[tokio::test]
async fn test_replica_state_invalid_json() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("replica.db");
    let state_path = db_path.with_extension("db-replica-state");

    std::fs::write(&state_path, "not json").unwrap();
    assert_eq!(ops::load_replica_state(&db_path), 0);
}

#[tokio::test]
async fn test_replica_state_missing_field() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("replica.db");
    let state_path = db_path.with_extension("db-replica-state");

    std::fs::write(&state_path, "{}").unwrap();
    assert_eq!(ops::load_replica_state(&db_path), 0);
}

// ============================================================================
// verify: overlap detection
// ============================================================================

#[tokio::test]
async fn test_verify_detects_txid_overlap() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);

    let snapshot_data = encode_snapshot(&db_path, 1);

    let storage = InMemoryStorage::new();
    // Snapshot
    storage
        .insert(
            "test/0001/0000000000000001-0000000000000001.ltx",
            snapshot_data.clone(),
        )
        .await;
    // Incremental 2-3
    storage
        .insert(
            "test/0000/0000000000000002-0000000000000003.ltx",
            snapshot_data.clone(),
        )
        .await;
    // Overlapping incremental 3-4 (starts at 3, expected 4)
    storage
        .insert(
            "test/0000/0000000000000003-0000000000000004.ltx",
            snapshot_data,
        )
        .await;

    let result = ops::verify_database(&storage, "", "test").await.unwrap();
    assert!(!result.is_valid());
    assert_eq!(result.continuity_issues.len(), 1);
    assert!(result.continuity_issues[0].contains("overlap"));
}

#[tokio::test]
async fn test_verify_no_issues_on_contiguous_incrementals() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);

    let snapshot_data = encode_snapshot(&db_path, 1);

    let storage = InMemoryStorage::new();
    storage
        .insert(
            "test/0001/0000000000000001-0000000000000001.ltx",
            snapshot_data.clone(),
        )
        .await;
    // Contiguous: 2-2, 3-3, 4-4
    storage
        .insert(
            "test/0000/0000000000000002-0000000000000002.ltx",
            snapshot_data.clone(),
        )
        .await;
    storage
        .insert(
            "test/0000/0000000000000003-0000000000000003.ltx",
            snapshot_data.clone(),
        )
        .await;
    storage
        .insert(
            "test/0000/0000000000000004-0000000000000004.ltx",
            snapshot_data,
        )
        .await;

    let result = ops::verify_database(&storage, "", "test").await.unwrap();
    assert!(result.continuity_issues.is_empty());
}

// ============================================================================
// verify: download failures and TXID mismatches
// ============================================================================

#[tokio::test]
async fn test_verify_download_failed() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);
    let snapshot_data = encode_snapshot(&db_path, 1);

    // Create a storage that has the key in list_objects but fails on download.
    // We'll use a wrapper that selectively fails downloads.
    let storage = FailingDownloadStorage::new(snapshot_data);

    let result = ops::verify_database(&storage, "", "test").await.unwrap();
    assert!(!result.is_valid());
    assert_eq!(result.file_results.len(), 2);
    // First file should succeed (the snapshot)
    match &result.file_results[0].1 {
        VerifyFileResult::Ok { .. } => {}
        other => panic!("Expected Ok, got: {:?}", other),
    }
    // Second file should be DownloadFailed
    match &result.file_results[1].1 {
        VerifyFileResult::DownloadFailed(msg) => {
            assert!(msg.contains("simulated download failure"));
        }
        other => panic!("Expected DownloadFailed, got: {:?}", other),
    }
}

/// Storage that lists two files but fails to download the second one.
struct FailingDownloadStorage {
    snapshot_data: Vec<u8>,
}

impl FailingDownloadStorage {
    fn new(snapshot_data: Vec<u8>) -> Self {
        Self { snapshot_data }
    }
}

#[async_trait]
impl walrust::StorageBackend for FailingDownloadStorage {
    fn bucket_name(&self) -> &str {
        "test-bucket"
    }
    async fn upload_bytes(&self, _key: &str, _data: Vec<u8>) -> Result<()> {
        Ok(())
    }
    async fn upload_bytes_with_checksum(&self, _key: &str, _data: Vec<u8>, _checksum: &str) -> Result<()> {
        Ok(())
    }
    async fn upload_file(&self, _key: &str, _path: &Path) -> Result<()> {
        Ok(())
    }
    async fn upload_file_with_checksum(&self, _key: &str, _path: &Path, _checksum: &str) -> Result<()> {
        Ok(())
    }
    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        // Succeed for the snapshot, fail for the incremental
        if key.contains("/0001/") {
            Ok(self.snapshot_data.clone())
        } else {
            Err(anyhow::anyhow!("simulated download failure"))
        }
    }
    async fn download_file(&self, _key: &str, _path: &Path) -> Result<()> {
        Ok(())
    }
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let keys = vec![
            "test/0001/0000000000000001-0000000000000001.ltx".to_string(),
            "test/0000/0000000000000002-0000000000000002.ltx".to_string(),
        ];
        Ok(keys.into_iter().filter(|k| k.starts_with(prefix)).collect())
    }
    async fn list_objects_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>> {
        let all = self.list_objects(prefix).await?;
        Ok(all.into_iter().filter(|k| k.as_str() > start_after).collect())
    }
    async fn exists(&self, _key: &str) -> Result<bool> {
        Ok(true)
    }
    async fn get_checksum(&self, _key: &str) -> Result<Option<String>> {
        Ok(None)
    }
    async fn delete_object(&self, _key: &str) -> Result<()> {
        Ok(())
    }
    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        Ok(keys.len())
    }
}

// ============================================================================
// compact: stale incremental cleanup
// ============================================================================

#[tokio::test]
async fn test_compact_identifies_stale_incrementals() {
    let storage = InMemoryStorage::new();
    // 2 snapshots: gen1 at TXID 10, gen2 at TXID 20
    storage
        .insert("mydb/0001/0000000000000001-000000000000000a.ltx", vec![1])
        .await;
    storage
        .insert("mydb/0002/0000000000000001-0000000000000014.ltx", vec![1])
        .await;
    // Incrementals: some before oldest kept snapshot, some after
    storage
        .insert("mydb/0000/0000000000000001-0000000000000005.ltx", vec![1])
        .await; // max_txid=5, stale (< snapshot min_txid=1... but wait, the oldest kept snapshot min_txid=1)
    storage
        .insert("mydb/0000/0000000000000015-0000000000000015.ltx", vec![1])
        .await; // max_txid=21, not stale

    // Keep 1 (keeps gen2, TXID 20), deletes gen1
    let plan = ops::plan_compact(&storage, "", "mydb", 1).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 1);
    assert_eq!(plan.keep_snapshots[0].max_txid, 20);
    assert_eq!(plan.delete_snapshots.len(), 1);
    assert_eq!(plan.delete_snapshots[0].max_txid, 10);

    // Stale incrementals: those with max_txid < oldest kept snapshot's min_txid (1)
    // The incremental at TXID 1-5 has max_txid=5, and the kept snapshot has min_txid=1
    // So 5 >= 1, meaning it's NOT stale
    assert_eq!(plan.delete_stale_incrementals.len(), 0);
}

#[tokio::test]
async fn test_compact_stale_incrementals_below_kept_snapshot() {
    let storage = InMemoryStorage::new();
    // Snapshot gen1 at TXID 10-20 (keeps)
    storage
        .insert("mydb/0001/000000000000000a-0000000000000014.ltx", vec![1])
        .await;
    // Old incrementals before the snapshot's min_txid
    storage
        .insert("mydb/0000/0000000000000001-0000000000000003.ltx", vec![1])
        .await; // max_txid=3 < 10
    storage
        .insert("mydb/0000/0000000000000004-0000000000000006.ltx", vec![1])
        .await; // max_txid=6 < 10
    storage
        .insert("mydb/0000/0000000000000007-0000000000000009.ltx", vec![1])
        .await; // max_txid=9 < 10
    // Incrementals at/after the snapshot
    storage
        .insert("mydb/0000/0000000000000015-0000000000000016.ltx", vec![1])
        .await; // max_txid=22, not stale

    let plan = ops::plan_compact(&storage, "", "mydb", 1).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 1);
    assert!(plan.delete_snapshots.is_empty()); // only 1 snapshot, keeping 1
    // 3 stale incrementals (max_txid 3,6,9 all < snapshot min_txid 10)
    assert_eq!(plan.delete_stale_incrementals.len(), 3);
}

#[tokio::test]
async fn test_compact_execute_deletes_stale_incrementals() {
    let storage = InMemoryStorage::new();
    // Snapshot gen1 at TXID 10-20
    storage
        .insert("mydb/0001/000000000000000a-0000000000000014.ltx", vec![1])
        .await;
    // Stale incrementals
    storage
        .insert("mydb/0000/0000000000000001-0000000000000003.ltx", vec![1])
        .await;
    storage
        .insert("mydb/0000/0000000000000004-0000000000000006.ltx", vec![1])
        .await;

    let plan = ops::plan_compact(&storage, "", "mydb", 1).await.unwrap();
    assert_eq!(plan.delete_stale_incrementals.len(), 2);

    let deleted = ops::execute_compact(&storage, &plan).await.unwrap();
    assert_eq!(deleted, 2);

    // Only the snapshot remains
    let remaining = storage.keys().await;
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].contains("/0001/"));
}

#[tokio::test]
async fn test_compact_keep_zero_deletes_all_snapshots() {
    let storage = InMemoryStorage::new();
    storage
        .insert("mydb/0001/0000000000000001-0000000000000005.ltx", vec![1])
        .await;
    storage
        .insert("mydb/0002/0000000000000001-000000000000000a.ltx", vec![1])
        .await;
    storage
        .insert("mydb/0000/000000000000000b-000000000000000c.ltx", vec![1])
        .await;

    let plan = ops::plan_compact(&storage, "", "mydb", 0).await.unwrap();
    assert!(plan.keep_snapshots.is_empty());
    assert_eq!(plan.delete_snapshots.len(), 2);
    // With no kept snapshots, oldest_kept_min_txid = u64::MAX, so all incrementals are stale
    assert_eq!(plan.delete_stale_incrementals.len(), 1);
}

// ============================================================================
// verify: is_valid() edge cases
// ============================================================================

#[tokio::test]
async fn test_verify_is_valid_with_only_continuity_issues() {
    // Manually construct a VerifyResult to test is_valid logic
    let result = ops::VerifyResult {
        total_files: 2,
        verified_count: 2,
        total_size: 100,
        file_results: vec![
            ("a.ltx".to_string(), VerifyFileResult::Ok { txid_count: 1, size_bytes: 50 }),
            ("b.ltx".to_string(), VerifyFileResult::Ok { txid_count: 1, size_bytes: 50 }),
        ],
        continuity_issues: vec!["TXID gap: expected 2, got 5".to_string()],
    };
    // All files OK but continuity issue means not valid
    assert!(!result.is_valid());
}

// ============================================================================
// Integration: snapshot + verify roundtrip
// ============================================================================

#[tokio::test]
async fn test_snapshot_then_verify() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("roundtrip.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();
    ops::snapshot_database(&storage, "",&db_path).await.unwrap();

    // Verify the snapshot we just created
    let result = ops::verify_database(&storage, "", "roundtrip").await.unwrap();
    assert!(result.is_valid());
    assert_eq!(result.verified_count, 1);
    assert_eq!(result.total_files, 1);
}

#[tokio::test]
async fn test_snapshot_list_verify_compact_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("full.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();

    // Take 3 snapshots (each needs a new write to advance the change counter)
    for i in 0..3 {
        ops::snapshot_database(&storage, "",&db_path).await.unwrap();
        // Write between snapshots so the file change counter advances
        if i < 2 {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute("INSERT INTO t (val) VALUES (?1)", [&format!("row-{i}")])
                .unwrap();
        }
    }

    // List: should show 1 database with 3 snapshots
    let dbs = ops::list_databases(&storage, "").await.unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(dbs[0].name, "full");
    assert!(dbs[0].latest_snapshot.is_some());

    // Verify: all 3 snapshots should be valid
    let result = ops::verify_database(&storage, "", "full").await.unwrap();
    assert!(result.is_valid());
    assert_eq!(result.verified_count, 3);

    // Compact: keep 1, plan deletes 2
    let plan = ops::plan_compact(&storage, "", "full", 1).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 1);
    assert_eq!(plan.delete_snapshots.len(), 2);

    // Execute compact
    let deleted = ops::execute_compact(&storage, &plan).await.unwrap();
    assert_eq!(deleted, 2);

    // Verify remaining is still valid
    let result = ops::verify_database(&storage, "", "full").await.unwrap();
    assert!(result.is_valid());
    assert_eq!(result.verified_count, 1);
}

// ============================================================================
// Prefix-specific tests
// ============================================================================

#[tokio::test]
async fn test_discover_ltx_files_with_prefix() {
    let storage = InMemoryStorage::new();
    storage
        .insert(
            "haqlite/mydb/0000/0000000000000001-0000000000000001.ltx",
            vec![1],
        )
        .await;
    storage
        .insert(
            "haqlite/mydb/0001/0000000000000001-0000000000000005.ltx",
            vec![2],
        )
        .await;

    let files = ops::discover_ltx_files(&storage, "haqlite/", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].min_txid, 1);
    assert_eq!(files[0].max_txid, 1);
    assert!(!files[0].is_snapshot);
    assert_eq!(files[1].min_txid, 1);
    assert_eq!(files[1].max_txid, 5);
    assert!(files[1].is_snapshot);
}

#[tokio::test]
async fn test_discover_ltx_files_prefix_isolation() {
    let storage = InMemoryStorage::new();
    // Files under haqlite/ prefix
    storage
        .insert(
            "haqlite/mydb/0000/0000000000000001-0000000000000001.ltx",
            vec![1],
        )
        .await;
    // Files under other/ prefix
    storage
        .insert(
            "other/mydb/0000/0000000000000002-0000000000000002.ltx",
            vec![2],
        )
        .await;

    // With haqlite/ prefix, only see haqlite files
    let files = ops::discover_ltx_files(&storage, "haqlite/", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].min_txid, 1);

    // With other/ prefix, only see other files
    let files = ops::discover_ltx_files(&storage, "other/", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].min_txid, 2);

    // With no prefix, see nothing (files are all under prefixes)
    let files = ops::discover_ltx_files(&storage, "", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 0);
}

#[tokio::test]
async fn test_discover_databases_with_prefix() {
    let storage = InMemoryStorage::new();
    storage
        .insert(
            "haqlite/alpha/0000/0000000000000001-0000000000000001.ltx",
            vec![1],
        )
        .await;
    storage
        .insert(
            "haqlite/beta/0001/0000000000000001-0000000000000001.ltx",
            vec![1],
        )
        .await;

    let dbs = ops::discover_databases(&storage, "haqlite/").await.unwrap();
    assert_eq!(dbs, vec!["alpha", "beta"]);
}

#[tokio::test]
async fn test_discover_databases_ignores_other_prefixes() {
    let storage = InMemoryStorage::new();
    // haqlite prefix
    storage
        .insert(
            "haqlite/alpha/0000/0000000000000001-0000000000000001.ltx",
            vec![1],
        )
        .await;
    // other prefix
    storage
        .insert(
            "other/gamma/0000/0000000000000001-0000000000000001.ltx",
            vec![1],
        )
        .await;

    let dbs = ops::discover_databases(&storage, "haqlite/").await.unwrap();
    assert_eq!(dbs, vec!["alpha"]);
}

#[tokio::test]
async fn test_list_databases_with_prefix() {
    let storage = InMemoryStorage::new();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("mydb.db");
    create_test_db(&db_path);
    let snapshot_bytes = encode_snapshot(&db_path, 1);

    storage
        .insert(
            "haqlite/mydb/0001/0000000000000001-0000000000000001.ltx",
            snapshot_bytes,
        )
        .await;

    let dbs = ops::list_databases(&storage, "haqlite/").await.unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(dbs[0].name, "mydb");
    assert_eq!(dbs[0].max_txid, 1);
}

#[tokio::test]
async fn test_snapshot_with_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("prefixed.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();
    let result = ops::snapshot_database(&storage, "haqlite/", &db_path)
        .await
        .unwrap();
    assert_eq!(result.txid, 2); // CREATE TABLE + INSERT = change counter 2
    assert_eq!(result.db_name, "prefixed");

    // Verify the key is under the prefix
    let keys = storage.keys().await;
    assert_eq!(keys.len(), 1);
    assert!(
        keys[0].starts_with("haqlite/prefixed/0001/"),
        "expected key under haqlite/ prefix, got: {}",
        keys[0]
    );
    assert!(keys[0].ends_with(".ltx"));
}

#[tokio::test]
async fn test_snapshot_returns_db_name() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("my_database.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();
    let result = ops::snapshot_database(&storage, "", &db_path)
        .await
        .unwrap();
    assert_eq!(result.db_name, "my_database");
    assert_eq!(result.txid, 2); // CREATE TABLE + INSERT = change counter 2
}

#[tokio::test]
async fn test_normalize_prefix_via_discover() {
    let storage = InMemoryStorage::new();
    storage
        .insert(
            "haqlite/mydb/0000/0000000000000001-0000000000000001.ltx",
            vec![1],
        )
        .await;

    // Without trailing slash - should still work (normalize_prefix adds it)
    let files = ops::discover_ltx_files(&storage, "haqlite", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 1);

    // With trailing slash - also works
    let files = ops::discover_ltx_files(&storage, "haqlite/", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 1);

    // Empty prefix - different namespace, finds nothing
    let files = ops::discover_ltx_files(&storage, "", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 0);
}

#[tokio::test]
async fn test_verify_with_prefix() {
    let storage = InMemoryStorage::new();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    create_test_db(&db_path);
    let snapshot_bytes = encode_snapshot(&db_path, 1);

    storage
        .insert(
            "prod/test/0001/0000000000000001-0000000000000001.ltx",
            snapshot_bytes,
        )
        .await;

    let result = ops::verify_database(&storage, "prod/", "test")
        .await
        .unwrap();
    assert!(result.is_valid());
    assert_eq!(result.verified_count, 1);
    assert_eq!(result.total_files, 1);
}
