//! Tests for haqlite::ops -- CLI operations (list, verify, compact, snapshot, replicate).
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

/// Encode a snapshot HADBP changeset from a database, returning the raw bytes.
fn encode_snapshot(db_path: &Path, seq: u64) -> Vec<u8> {
    let page_size = {
        let conn = rusqlite::Connection::open(db_path).unwrap();
        let ps: u32 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .unwrap();
        ps
    };
    walrust::ltx::encode_snapshot(db_path, page_size, seq, 0).unwrap()
}

// ============================================================================
// discover_ltx_files (now parses .hadbp format)
// ============================================================================

#[tokio::test]
async fn test_discover_empty_bucket() {
    let storage = InMemoryStorage::new();
    let files = ops::discover_ltx_files(&storage, "", "mydb").await.unwrap();
    assert!(files.is_empty());
}

#[tokio::test]
async fn test_discover_finds_incrementals() {
    let storage = InMemoryStorage::new();
    // Generation 0 = incrementals
    storage
        .insert("mydb/0000/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("mydb/0000/0000000000000002.hadbp", vec![2])
        .await;

    let files = ops::discover_ltx_files(&storage, "", "mydb").await.unwrap();
    assert_eq!(files.len(), 2);
    assert!(!files[0].is_snapshot);
    assert_eq!(files[0].seq, 1);
    assert_eq!(files[0].generation, 0);
    assert!(!files[1].is_snapshot);
    assert_eq!(files[1].seq, 2);
}

#[tokio::test]
async fn test_discover_finds_snapshots() {
    let storage = InMemoryStorage::new();
    // Generation 1 = snapshot
    storage
        .insert("mydb/0001/0000000000000005.hadbp", vec![1])
        .await;

    let files = ops::discover_ltx_files(&storage, "", "mydb").await.unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].is_snapshot);
    assert_eq!(files[0].seq, 5);
    assert_eq!(files[0].generation, 1);
}

#[tokio::test]
async fn test_discover_ignores_non_hadbp_files() {
    let storage = InMemoryStorage::new();
    storage.insert("mydb/0000/manifest.json", vec![]).await;
    storage.insert("mydb/0000/state.json", vec![]).await;
    storage
        .insert("mydb/0000/0000000000000001.hadbp", vec![1])
        .await;

    let files = ops::discover_ltx_files(&storage, "", "mydb").await.unwrap();
    assert_eq!(files.len(), 1);
}

#[tokio::test]
async fn test_discover_ignores_other_databases() {
    let storage = InMemoryStorage::new();
    storage
        .insert("mydb/0000/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("other/0000/0000000000000001.hadbp", vec![1])
        .await;

    let files = ops::discover_ltx_files(&storage, "", "mydb").await.unwrap();
    assert_eq!(files.len(), 1);
}

#[tokio::test]
async fn test_discover_sorted_by_seq() {
    let storage = InMemoryStorage::new();
    storage
        .insert("db/0000/0000000000000005.hadbp", vec![1])
        .await;
    storage
        .insert("db/0000/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("db/0000/0000000000000003.hadbp", vec![1])
        .await;

    let files = ops::discover_ltx_files(&storage, "", "db").await.unwrap();
    assert_eq!(files.len(), 3);
    assert_eq!(files[0].seq, 1);
    assert_eq!(files[1].seq, 3);
    assert_eq!(files[2].seq, 5);
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
        .insert("alpha/0000/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("beta/0001/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("gamma/0000/0000000000000001.hadbp", vec![1])
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
    // Snapshot at gen 1, seq 5
    storage
        .insert("mydb/0001/0000000000000005.hadbp", vec![1])
        .await;
    // Two incrementals at gen 0
    storage
        .insert("mydb/0000/0000000000000006.hadbp", vec![1])
        .await;
    storage
        .insert("mydb/0000/0000000000000007.hadbp", vec![1])
        .await;

    let dbs = ops::list_databases(&storage, "").await.unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(dbs[0].name, "mydb");
    assert_eq!(dbs[0].max_seq, 7);
    assert_eq!(dbs[0].incremental_count, 2);
    assert!(dbs[0].latest_snapshot.is_some());
    let snap = dbs[0].latest_snapshot.as_ref().unwrap();
    assert_eq!(snap.generation, 1);
    assert_eq!(snap.seq, 5);
}

#[tokio::test]
async fn test_list_no_snapshot() {
    let storage = InMemoryStorage::new();
    storage
        .insert("mydb/0000/0000000000000001.hadbp", vec![1])
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
    assert!(err.to_string().contains("No changeset files found"));
}

#[tokio::test]
async fn test_verify_no_snapshot_errors() {
    let storage = InMemoryStorage::new();
    // Only incrementals, no snapshot
    storage
        .insert("mydb/0000/0000000000000001.hadbp", vec![1])
        .await;

    let err = ops::verify_database(&storage, "", "mydb").await.unwrap_err();
    assert!(err.to_string().contains("No snapshot found"));
}

#[tokio::test]
async fn test_verify_valid_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);

    let hadbp_data = encode_snapshot(&db_path, 1);

    let storage = InMemoryStorage::new();
    storage
        .insert("test/0001/0000000000000001.hadbp", hadbp_data)
        .await;

    let result = ops::verify_database(&storage, "", "test").await.unwrap();
    assert_eq!(result.total_files, 1);
    assert_eq!(result.verified_count, 1);
    assert!(result.is_valid());
}

#[tokio::test]
async fn test_verify_corrupt_file_detected() {
    let storage = InMemoryStorage::new();
    // Corrupt data that doesn't parse as HADBP
    storage
        .insert(
            "mydb/0001/0000000000000001.hadbp",
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
async fn test_verify_detects_seq_gap() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);

    let snapshot_data = encode_snapshot(&db_path, 1);

    let storage = InMemoryStorage::new();
    // Snapshot at seq 1
    storage
        .insert("test/0001/0000000000000001.hadbp", snapshot_data.clone())
        .await;
    // Incremental at seq 2
    let incr2 = encode_snapshot(&db_path, 2);
    storage
        .insert("test/0000/0000000000000002.hadbp", incr2)
        .await;
    // Incremental at seq 5 (gap: missing 3-4)
    let incr5 = encode_snapshot(&db_path, 5);
    storage
        .insert("test/0000/0000000000000005.hadbp", incr5)
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
        .insert("mydb/0001/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("mydb/0002/0000000000000005.hadbp", vec![1])
        .await;

    // Keep 5, only 2 exist
    let plan = ops::plan_compact(&storage, "", "mydb", 5).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 2);
    assert!(plan.delete_snapshots.is_empty());
}

#[tokio::test]
async fn test_compact_deletes_oldest() {
    let storage = InMemoryStorage::new();
    // 5 snapshots with increasing seqs
    for i in 1..=5u64 {
        let key = format!("mydb/{:04x}/{:016x}.hadbp", i, i * 10);
        storage.insert(&key, vec![1]).await;
    }

    // Keep 2, delete 3
    let plan = ops::plan_compact(&storage, "", "mydb", 2).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 2);
    assert_eq!(plan.delete_snapshots.len(), 3);

    // Kept snapshots should be the newest (highest seq)
    assert_eq!(plan.keep_snapshots[0].seq, 50);
    assert_eq!(plan.keep_snapshots[1].seq, 40);

    // Deleted should be the oldest
    assert_eq!(plan.delete_snapshots[0].seq, 30);
    assert_eq!(plan.delete_snapshots[1].seq, 20);
    assert_eq!(plan.delete_snapshots[2].seq, 10);
}

#[tokio::test]
async fn test_compact_execute_deletes_files() {
    let storage = InMemoryStorage::new();
    for i in 1..=5u64 {
        let key = format!("mydb/{:04x}/{:016x}.hadbp", i, i * 10);
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
        .insert("mydb/0001/0000000000000001.hadbp", vec![1])
        .await;
    // Incrementals (generation 0) should not be touched
    storage
        .insert("mydb/0000/0000000000000002.hadbp", vec![1])
        .await;
    storage
        .insert("mydb/0000/0000000000000003.hadbp", vec![1])
        .await;

    let plan = ops::plan_compact(&storage, "", "mydb", 1).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 1);
    assert!(plan.delete_snapshots.is_empty());
    assert!(plan.keep_snapshots[0].is_snapshot);
}

// ============================================================================
// snapshot_database
// ============================================================================

#[tokio::test]
async fn test_snapshot_nonexistent_db() {
    let storage = InMemoryStorage::new();
    let err = ops::snapshot_database(&storage, "", Path::new("/nonexistent/test.db"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Database not found"));
}

#[tokio::test]
async fn test_snapshot_creates_hadbp_file() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("snap.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();
    let result = ops::snapshot_database(&storage, "", &db_path).await.unwrap();
    // seq starts at 0 + 1 = 1
    assert_eq!(result.seq, 1);

    // Verify the HADBP file was uploaded
    let keys = storage.keys().await;
    assert_eq!(keys.len(), 1);
    assert!(keys[0].starts_with("snap/"));
    assert!(keys[0].contains("0001/"));
    assert!(keys[0].ends_with(".hadbp"));
}

#[tokio::test]
async fn test_snapshot_increments_seq() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("snap.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();

    // First snapshot
    let result1 = ops::snapshot_database(&storage, "", &db_path).await.unwrap();
    assert_eq!(result1.seq, 1);

    // Second snapshot after modifying the DB
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('world')", [])
            .unwrap();
    }
    let result2 = ops::snapshot_database(&storage, "", &db_path).await.unwrap();
    assert_eq!(result2.seq, 2);

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

/// Seq-based keys are unique in S3, so duplicate seqs are impossible by construction.
/// This test verifies that inserting the same seq twice overwrites (S3 semantics)
/// and doesn't create a duplicate.
#[tokio::test]
async fn test_seq_keys_prevent_duplicates() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();
    // Insert seq 1 twice -- second write overwrites the first (S3 semantics)
    storage
        .insert("mydb/0000/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("mydb/0000/0000000000000001.hadbp", vec![2])
        .await;

    let files = ops::discover_ltx_files(&storage, "", "mydb").await.unwrap();
    assert_eq!(files.len(), 1, "duplicate seq should not create two entries");
}

#[tokio::test]
async fn test_verify_no_issues_on_contiguous_incrementals() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);

    let storage = InMemoryStorage::new();
    // Snapshot at seq 1
    let snap = encode_snapshot(&db_path, 1);
    storage
        .insert("test/0001/0000000000000001.hadbp", snap)
        .await;
    // Contiguous: 2, 3, 4
    for seq in 2..=4 {
        let data = encode_snapshot(&db_path, seq);
        storage
            .insert(
                &format!("test/0000/{:016x}.hadbp", seq),
                data,
            )
            .await;
    }

    let result = ops::verify_database(&storage, "", "test").await.unwrap();
    assert!(result.continuity_issues.is_empty());
}

// ============================================================================
// verify: download failures
// ============================================================================

#[tokio::test]
async fn test_verify_download_failed() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    create_test_db(&db_path);
    let snapshot_data = encode_snapshot(&db_path, 1);

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
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if key.contains("/0001/") {
            Ok(Some(self.snapshot_data.clone()))
        } else {
            Err(anyhow::anyhow!("simulated download failure"))
        }
    }
    async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
        Ok(())
    }
    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }
    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let keys = vec![
            "test/0001/0000000000000001.hadbp".to_string(),
            "test/0000/0000000000000002.hadbp".to_string(),
        ];
        Ok(keys
            .into_iter()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
            .collect())
    }
    async fn exists(&self, _key: &str) -> Result<bool> {
        Ok(true)
    }
    async fn put_if_absent(
        &self,
        _key: &str,
        _data: &[u8],
    ) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult { success: true, etag: Some("test".into()) })
    }
    async fn put_if_match(
        &self,
        _key: &str,
        _data: &[u8],
        _etag: &str,
    ) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult { success: true, etag: Some("test".into()) })
    }
}

// ============================================================================
// compact: stale incremental cleanup
// ============================================================================

#[tokio::test]
async fn test_compact_identifies_stale_incrementals() {
    let storage = InMemoryStorage::new();
    // 2 snapshots: gen1 at seq 10, gen2 at seq 20
    storage
        .insert("mydb/0001/000000000000000a.hadbp", vec![1])
        .await;
    storage
        .insert("mydb/0002/0000000000000014.hadbp", vec![1])
        .await;
    // Incrementals: some before oldest kept snapshot, some after
    storage
        .insert("mydb/0000/0000000000000005.hadbp", vec![1])
        .await; // seq 5
    storage
        .insert("mydb/0000/0000000000000015.hadbp", vec![1])
        .await; // seq 21, not stale

    // Keep 1 (keeps gen2, seq 20), deletes gen1
    let plan = ops::plan_compact(&storage, "", "mydb", 1).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 1);
    assert_eq!(plan.keep_snapshots[0].seq, 20);
    assert_eq!(plan.delete_snapshots.len(), 1);
    assert_eq!(plan.delete_snapshots[0].seq, 10);

    // Stale incrementals: seq 5 < kept snapshot seq 20
    assert_eq!(plan.delete_stale_incrementals.len(), 1);
    assert_eq!(plan.delete_stale_incrementals[0].seq, 5);
}

#[tokio::test]
async fn test_compact_stale_incrementals_below_kept_snapshot() {
    let storage = InMemoryStorage::new();
    // Snapshot gen1 at seq 20 (keeps)
    storage
        .insert("mydb/0001/0000000000000014.hadbp", vec![1])
        .await;
    // Old incrementals before the snapshot's seq
    storage
        .insert("mydb/0000/0000000000000003.hadbp", vec![1])
        .await; // seq 3
    storage
        .insert("mydb/0000/0000000000000006.hadbp", vec![1])
        .await; // seq 6
    storage
        .insert("mydb/0000/0000000000000009.hadbp", vec![1])
        .await; // seq 9
    // Incrementals at/after the snapshot
    storage
        .insert("mydb/0000/0000000000000016.hadbp", vec![1])
        .await; // seq 22, not stale

    let plan = ops::plan_compact(&storage, "", "mydb", 1).await.unwrap();
    assert_eq!(plan.keep_snapshots.len(), 1);
    assert!(plan.delete_snapshots.is_empty());
    // 3 stale incrementals (seq 3, 6, 9 all < snapshot seq 20)
    assert_eq!(plan.delete_stale_incrementals.len(), 3);
}

#[tokio::test]
async fn test_compact_execute_deletes_stale_incrementals() {
    let storage = InMemoryStorage::new();
    // Snapshot gen1 at seq 20
    storage
        .insert("mydb/0001/0000000000000014.hadbp", vec![1])
        .await;
    // Stale incrementals
    storage
        .insert("mydb/0000/0000000000000003.hadbp", vec![1])
        .await;
    storage
        .insert("mydb/0000/0000000000000006.hadbp", vec![1])
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
        .insert("mydb/0001/0000000000000005.hadbp", vec![1])
        .await;
    storage
        .insert("mydb/0002/000000000000000a.hadbp", vec![1])
        .await;
    storage
        .insert("mydb/0000/000000000000000c.hadbp", vec![1])
        .await;

    let plan = ops::plan_compact(&storage, "", "mydb", 0).await.unwrap();
    assert!(plan.keep_snapshots.is_empty());
    assert_eq!(plan.delete_snapshots.len(), 2);
    // With no kept snapshots, oldest_kept_seq = u64::MAX, so all incrementals are stale
    assert_eq!(plan.delete_stale_incrementals.len(), 1);
}

// ============================================================================
// verify: is_valid() edge cases
// ============================================================================

#[tokio::test]
async fn test_verify_is_valid_with_only_continuity_issues() {
    let result = ops::VerifyResult {
        total_files: 2,
        verified_count: 2,
        total_size: 100,
        file_results: vec![
            (
                "a.hadbp".to_string(),
                VerifyFileResult::Ok {
                    seq: 1,
                    size_bytes: 50,
                },
            ),
            (
                "b.hadbp".to_string(),
                VerifyFileResult::Ok {
                    seq: 2,
                    size_bytes: 50,
                },
            ),
        ],
        continuity_issues: vec!["Seq gap: expected 2, got 5".to_string()],
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
    ops::snapshot_database(&storage, "", &db_path).await.unwrap();

    // Verify the snapshot we just created
    let result = ops::verify_database(&storage, "", "roundtrip")
        .await
        .unwrap();
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
        ops::snapshot_database(&storage, "", &db_path).await.unwrap();
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
async fn test_discover_files_with_prefix() {
    let storage = InMemoryStorage::new();
    storage
        .insert("haqlite/mydb/0000/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("haqlite/mydb/0001/0000000000000005.hadbp", vec![2])
        .await;

    let files = ops::discover_ltx_files(&storage, "haqlite/", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].seq, 1);
    assert!(!files[0].is_snapshot);
    assert_eq!(files[1].seq, 5);
    assert!(files[1].is_snapshot);
}

#[tokio::test]
async fn test_discover_files_prefix_isolation() {
    let storage = InMemoryStorage::new();
    storage
        .insert("haqlite/mydb/0000/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("other/mydb/0000/0000000000000002.hadbp", vec![2])
        .await;

    let files = ops::discover_ltx_files(&storage, "haqlite/", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].seq, 1);

    let files = ops::discover_ltx_files(&storage, "other/", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].seq, 2);

    let files = ops::discover_ltx_files(&storage, "", "mydb")
        .await
        .unwrap();
    assert_eq!(files.len(), 0);
}

#[tokio::test]
async fn test_discover_databases_with_prefix() {
    let storage = InMemoryStorage::new();
    storage
        .insert("haqlite/alpha/0000/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("haqlite/beta/0001/0000000000000001.hadbp", vec![1])
        .await;

    let dbs = ops::discover_databases(&storage, "haqlite/").await.unwrap();
    assert_eq!(dbs, vec!["alpha", "beta"]);
}

#[tokio::test]
async fn test_discover_databases_ignores_other_prefixes() {
    let storage = InMemoryStorage::new();
    storage
        .insert("haqlite/alpha/0000/0000000000000001.hadbp", vec![1])
        .await;
    storage
        .insert("other/gamma/0000/0000000000000001.hadbp", vec![1])
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
        .insert("haqlite/mydb/0001/0000000000000001.hadbp", snapshot_bytes)
        .await;

    let dbs = ops::list_databases(&storage, "haqlite/").await.unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(dbs[0].name, "mydb");
    assert_eq!(dbs[0].max_seq, 1);
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
    assert_eq!(result.seq, 1);
    assert_eq!(result.db_name, "prefixed");

    let keys = storage.keys().await;
    assert_eq!(keys.len(), 1);
    assert!(
        keys[0].starts_with("haqlite/prefixed/0001/"),
        "expected key under haqlite/ prefix, got: {}",
        keys[0]
    );
    assert!(keys[0].ends_with(".hadbp"));
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
    assert_eq!(result.seq, 1);
}

#[tokio::test]
async fn test_normalize_prefix_via_discover() {
    let storage = InMemoryStorage::new();
    storage
        .insert("haqlite/mydb/0000/0000000000000001.hadbp", vec![1])
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
        .insert("prod/test/0001/0000000000000001.hadbp", snapshot_bytes)
        .await;

    let result = ops::verify_database(&storage, "prod/", "test")
        .await
        .unwrap();
    assert!(result.is_valid());
    assert_eq!(result.verified_count, 1);
    assert_eq!(result.total_files, 1);
}
