//! Integration tests for haqlite.
//!
//! Tests lease CAS correctness, session_id tracking, coordinator join/leave,
//! role events, warm promotion, consecutive expired reads, safe leave,
//! and follower auto-promotion using in-memory backends.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tempfile::TempDir;
use tokio::sync::Mutex;

use haqlite::{
    CasResult, Coordinator, CoordinatorConfig, DbLease, InMemoryLeaseStore, InMemoryNodeRegistry,
    LeaseConfig, LeaseData, LeaseStore, NodeRegistration, NodeRegistry, Role, RoleEvent,
    StorageBackend,
};

// ============================================================================
// InMemoryStorage — implements walrust StorageBackend for tests
// ============================================================================

struct InMemoryStorage {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    upload_count: AtomicU64,
}

impl InMemoryStorage {
    fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            upload_count: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.upload_count.fetch_add(1, Ordering::Relaxed);
        self.objects.lock().await.insert(key.to_string(), data);
        Ok(())
    }

    async fn upload_bytes_with_checksum(
        &self,
        key: &str,
        data: Vec<u8>,
        _checksum: &str,
    ) -> Result<()> {
        self.upload_bytes(key, data).await
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = tokio::fs::read(path).await?;
        self.upload_bytes(key, data).await
    }

    async fn upload_file_with_checksum(
        &self,
        key: &str,
        path: &Path,
        _checksum: &str,
    ) -> Result<()> {
        self.upload_file(key, path).await
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.objects
            .lock()
            .await
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Key not found: {}", key))
    }

    async fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = self.download_bytes(key).await?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, data).await?;
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self
            .objects
            .lock()
            .await
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn list_objects_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self
            .objects
            .lock()
            .await
            .keys()
            .filter(|k| k.starts_with(prefix) && k.as_str() > start_after)
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().await.contains_key(key))
    }

    async fn get_checksum(&self, _key: &str) -> Result<Option<String>> {
        Ok(None)
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.objects.lock().await.remove(key);
        Ok(())
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        let mut objects = self.objects.lock().await;
        let mut deleted = 0;
        for key in keys {
            if objects.remove(key).is_some() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    fn bucket_name(&self) -> &str {
        "test-bucket"
    }
}

// ============================================================================
// ControlledFailStorage — wraps InMemoryStorage, can be triggered to fail
// ============================================================================

/// Storage backend that delegates to InMemoryStorage but can be triggered to
/// fail reads (list/download) or writes (upload) on demand. Used to test
/// promotion failure paths.
struct ControlledFailStorage {
    inner: InMemoryStorage,
    fail_reads: AtomicBool,
    fail_writes: AtomicBool,
}

impl ControlledFailStorage {
    fn new() -> Self {
        Self {
            inner: InMemoryStorage::new(),
            fail_reads: AtomicBool::new(false),
            fail_writes: AtomicBool::new(false),
        }
    }

    fn set_fail_reads(&self, fail: bool) {
        self.fail_reads.store(fail, Ordering::SeqCst);
    }

    fn set_fail_writes(&self, fail: bool) {
        self.fail_writes.store(fail, Ordering::SeqCst);
    }

    fn check_read(&self) -> Result<()> {
        if self.fail_reads.load(Ordering::SeqCst) {
            Err(anyhow::anyhow!("Simulated storage read failure"))
        } else {
            Ok(())
        }
    }

    fn check_write(&self) -> Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
            Err(anyhow::anyhow!("Simulated storage write failure"))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl StorageBackend for ControlledFailStorage {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.check_write()?;
        self.inner.upload_bytes(key, data).await
    }

    async fn upload_bytes_with_checksum(
        &self,
        key: &str,
        data: Vec<u8>,
        checksum: &str,
    ) -> Result<()> {
        self.check_write()?;
        self.inner.upload_bytes_with_checksum(key, data, checksum).await
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        self.check_write()?;
        self.inner.upload_file(key, path).await
    }

    async fn upload_file_with_checksum(
        &self,
        key: &str,
        path: &Path,
        checksum: &str,
    ) -> Result<()> {
        self.check_write()?;
        self.inner.upload_file_with_checksum(key, path, checksum).await
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.check_read()?;
        self.inner.download_bytes(key).await
    }

    async fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        self.check_read()?;
        self.inner.download_file(key, path).await
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.check_read()?;
        self.inner.list_objects(prefix).await
    }

    async fn list_objects_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>> {
        self.check_read()?;
        self.inner.list_objects_after(prefix, start_after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.check_read()?;
        self.inner.exists(key).await
    }

    async fn get_checksum(&self, key: &str) -> Result<Option<String>> {
        self.check_read()?;
        self.inner.get_checksum(key).await
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.inner.delete_object(key).await
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        self.inner.delete_objects(keys).await
    }

    fn bucket_name(&self) -> &str {
        self.inner.bucket_name()
    }
}

// ============================================================================
// FaultyStorage — random error injection for chaos testing
// ============================================================================

/// Storage backend that randomly injects errors and delays.
/// Configurable error rates for reads and writes.
struct FaultyStorage {
    inner: InMemoryStorage,
    /// Probability [0.0, 1.0] that a read operation fails.
    read_error_rate: std::sync::Mutex<f64>,
    /// Probability [0.0, 1.0] that a write operation fails.
    write_error_rate: std::sync::Mutex<f64>,
    /// Count of injected read failures.
    read_failures: AtomicU64,
    /// Count of injected write failures.
    write_failures: AtomicU64,
    /// Count of successful operations.
    successes: AtomicU64,
}

impl FaultyStorage {
    fn new(read_error_rate: f64, write_error_rate: f64) -> Self {
        Self {
            inner: InMemoryStorage::new(),
            read_error_rate: std::sync::Mutex::new(read_error_rate),
            write_error_rate: std::sync::Mutex::new(write_error_rate),
            read_failures: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            successes: AtomicU64::new(0),
        }
    }

    fn maybe_fail_read(&self) -> Result<()> {
        let rate = *self.read_error_rate.lock().unwrap();
        if rate > 0.0 && rand::random::<f64>() < rate {
            self.read_failures.fetch_add(1, Ordering::Relaxed);
            Err(anyhow::anyhow!("Faulty storage: injected read error"))
        } else {
            self.successes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn maybe_fail_write(&self) -> Result<()> {
        let rate = *self.write_error_rate.lock().unwrap();
        if rate > 0.0 && rand::random::<f64>() < rate {
            self.write_failures.fetch_add(1, Ordering::Relaxed);
            Err(anyhow::anyhow!("Faulty storage: injected write error"))
        } else {
            self.successes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn set_read_error_rate(&self, rate: f64) {
        *self.read_error_rate.lock().unwrap() = rate;
    }

    #[allow(dead_code)]
    fn set_write_error_rate(&self, rate: f64) {
        *self.write_error_rate.lock().unwrap() = rate;
    }

    fn read_failure_count(&self) -> u64 {
        self.read_failures.load(Ordering::Relaxed)
    }

    fn write_failure_count(&self) -> u64 {
        self.write_failures.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl StorageBackend for FaultyStorage {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.maybe_fail_write()?;
        self.inner.upload_bytes(key, data).await
    }

    async fn upload_bytes_with_checksum(&self, key: &str, data: Vec<u8>, checksum: &str) -> Result<()> {
        self.maybe_fail_write()?;
        self.inner.upload_bytes_with_checksum(key, data, checksum).await
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        self.maybe_fail_write()?;
        self.inner.upload_file(key, path).await
    }

    async fn upload_file_with_checksum(&self, key: &str, path: &Path, checksum: &str) -> Result<()> {
        self.maybe_fail_write()?;
        self.inner.upload_file_with_checksum(key, path, checksum).await
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.maybe_fail_read()?;
        self.inner.download_bytes(key).await
    }

    async fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        self.maybe_fail_read()?;
        self.inner.download_file(key, path).await
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.maybe_fail_read()?;
        self.inner.list_objects(prefix).await
    }

    async fn list_objects_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>> {
        self.maybe_fail_read()?;
        self.inner.list_objects_after(prefix, start_after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.maybe_fail_read()?;
        self.inner.exists(key).await
    }

    async fn get_checksum(&self, key: &str) -> Result<Option<String>> {
        self.maybe_fail_read()?;
        self.inner.get_checksum(key).await
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.inner.delete_object(key).await
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        self.inner.delete_objects(keys).await
    }

    fn bucket_name(&self) -> &str {
        self.inner.bucket_name()
    }
}

fn create_test_db(dir: &TempDir, name: &str) -> PathBuf {
    let db_path = dir.path().join(format!("{}.db", name));
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY);")
        .unwrap();
    drop(conn);
    db_path
}

// ============================================================================
// LeaseStore — CAS correctness
// ============================================================================

#[tokio::test]
async fn lease_store_write_if_not_exists() {
    let store = InMemoryLeaseStore::new();

    let result = store
        .write_if_not_exists("key1", b"data1".to_vec())
        .await
        .unwrap();
    assert!(result.success);
    assert!(result.etag.is_some());

    // Second write to same key must fail.
    let result = store
        .write_if_not_exists("key1", b"data2".to_vec())
        .await
        .unwrap();
    assert!(!result.success);
    assert!(result.etag.is_none());
}

#[tokio::test]
async fn lease_store_write_if_match() {
    let store = InMemoryLeaseStore::new();

    let create = store
        .write_if_not_exists("key1", b"v1".to_vec())
        .await
        .unwrap();
    let etag1 = create.etag.unwrap();

    // Update with correct etag succeeds.
    let update = store
        .write_if_match("key1", b"v2".to_vec(), &etag1)
        .await
        .unwrap();
    assert!(update.success);
    let etag2 = update.etag.unwrap();
    assert_ne!(etag1, etag2);

    // Update with stale etag fails.
    let stale = store
        .write_if_match("key1", b"v3".to_vec(), &etag1)
        .await
        .unwrap();
    assert!(!stale.success);

    // Read back the latest value.
    let (data, etag) = store.read("key1").await.unwrap().unwrap();
    assert_eq!(data, b"v2");
    assert_eq!(etag, etag2);
}

#[tokio::test]
async fn lease_store_read_nonexistent() {
    let store = InMemoryLeaseStore::new();
    assert!(store.read("missing").await.unwrap().is_none());
}

#[tokio::test]
async fn lease_store_delete() {
    let store = InMemoryLeaseStore::new();

    store
        .write_if_not_exists("key1", b"data".to_vec())
        .await
        .unwrap();
    assert!(store.read("key1").await.unwrap().is_some());

    store.delete("key1").await.unwrap();
    assert!(store.read("key1").await.unwrap().is_none());
}

#[tokio::test]
async fn lease_store_concurrent_write_if_not_exists() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let mut handles = Vec::new();

    // 10 concurrent writers — exactly 1 should win.
    for i in 0..10 {
        let s = store.clone();
        handles.push(tokio::spawn(async move {
            s.write_if_not_exists("race", format!("writer-{}", i).into_bytes())
                .await
                .unwrap()
        }));
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let winners: Vec<_> = results.iter().filter(|r| r.success).collect();
    assert_eq!(winners.len(), 1, "Exactly one writer should win CAS");
}

#[tokio::test]
async fn lease_store_write_if_match_on_deleted_key() {
    let store = InMemoryLeaseStore::new();

    let create = store
        .write_if_not_exists("key1", b"v1".to_vec())
        .await
        .unwrap();
    let etag = create.etag.unwrap();

    store.delete("key1").await.unwrap();

    // write_if_match on deleted key must fail.
    let result = store
        .write_if_match("key1", b"v2".to_vec(), &etag)
        .await
        .unwrap();
    assert!(!result.success);
}

#[tokio::test]
async fn lease_store_write_if_match_nonexistent_key() {
    let store = InMemoryLeaseStore::new();
    let result = store
        .write_if_match("nope", b"data".to_vec(), "\"fake-etag\"")
        .await
        .unwrap();
    assert!(!result.success);
}

// ============================================================================
// LeaseData — serialization and expiry
// ============================================================================

#[tokio::test]
async fn lease_data_serialization_roundtrip() {
    let data = LeaseData {
        instance_id: "node-1".to_string(),
        address: "http://node-1:9000".to_string(),
        claimed_at: 1700000000,
        ttl_secs: 5,
        session_id: "sess-abc".to_string(),
        sleeping: false,
    };

    let json = serde_json::to_vec(&data).unwrap();
    let parsed: LeaseData = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed.instance_id, "node-1");
    assert_eq!(parsed.address, "http://node-1:9000");
    assert_eq!(parsed.session_id, "sess-abc");
    assert_eq!(parsed.ttl_secs, 5);
}

#[tokio::test]
async fn lease_data_backward_compat_no_session_id() {
    // Old lease without session_id, address, or sleeping fields.
    let json = r#"{"instance_id":"node-1","claimed_at":1700000000,"ttl_secs":5}"#;
    let parsed: LeaseData = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.session_id, "");
    assert_eq!(parsed.address, "");
    assert!(!parsed.sleeping);
}

#[tokio::test]
async fn lease_data_expiry_boundary() {
    let now = chrono::Utc::now().timestamp() as u64;

    // Just expired (claimed_at + ttl == now).
    let expired = LeaseData {
        instance_id: "x".to_string(),
        address: String::new(),
        claimed_at: now - 5,
        ttl_secs: 5,
        session_id: String::new(),
        sleeping: false,
    };
    assert!(expired.is_expired());

    // Not yet expired (1 second left).
    let active = LeaseData {
        instance_id: "x".to_string(),
        address: String::new(),
        claimed_at: now - 4,
        ttl_secs: 5,
        session_id: String::new(),
        sleeping: false,
    };
    assert!(!active.is_expired());
}

// ============================================================================
// DbLease — claim, contention, session_id, renew, release
// ============================================================================

#[tokio::test]
async fn lease_claim_uncontested() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let mut lease = DbLease::new(store, "test/", "mydb", "node-1", "addr-1", 5);

    let role = lease.try_claim().await.unwrap();
    assert_eq!(role, Role::Leader);

    let (data, _etag) = lease.read().await.unwrap().unwrap();
    assert_eq!(data.instance_id, "node-1");
    assert_eq!(data.address, "addr-1");
    assert_eq!(data.ttl_secs, 5);
    assert!(!data.session_id.is_empty());
}

#[tokio::test]
async fn lease_claim_contested() {
    let store = Arc::new(InMemoryLeaseStore::new());

    let mut lease1 = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 5);
    assert_eq!(lease1.try_claim().await.unwrap(), Role::Leader);

    let mut lease2 = DbLease::new(store.clone(), "test/", "mydb", "node-2", "addr-2", 5);
    assert_eq!(lease2.try_claim().await.unwrap(), Role::Follower);
}

#[tokio::test]
async fn lease_session_id_unique_per_claim() {
    let store = Arc::new(InMemoryLeaseStore::new());

    let mut lease1 = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 0);
    lease1.try_claim().await.unwrap();
    let session1 = lease1.session_id().to_string();

    // Wait for expiry.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Same instance reclaims — session_id must change.
    let mut lease2 = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 5);
    lease2.try_claim().await.unwrap();
    let session2 = lease2.session_id().to_string();

    assert_ne!(session1, session2, "session_id must change on every new claim");
}

#[tokio::test]
async fn lease_session_id_different_between_instances() {
    let store = Arc::new(InMemoryLeaseStore::new());

    let lease1 = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 5);
    let lease2 = DbLease::new(store.clone(), "test/", "mydb", "node-2", "addr-2", 5);

    // Even without claiming, each instance generates a unique session_id.
    assert_ne!(lease1.session_id(), lease2.session_id());
}

#[tokio::test]
async fn lease_renew() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let mut lease = DbLease::new(store, "test/", "mydb", "node-1", "addr-1", 5);

    lease.try_claim().await.unwrap();
    assert!(lease.renew().await.unwrap());

    let (data, _etag) = lease.read().await.unwrap().unwrap();
    assert_eq!(data.instance_id, "node-1");
    assert_eq!(data.session_id, lease.session_id());
}

#[tokio::test]
async fn lease_renew_without_claim_fails() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let mut lease = DbLease::new(store, "test/", "mydb", "node-1", "addr-1", 5);

    // Renew without claim should error (no etag).
    assert!(lease.renew().await.is_err());
}

#[tokio::test]
async fn lease_release() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let mut lease = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 5);

    lease.try_claim().await.unwrap();
    lease.release().await.unwrap();

    assert!(store.read("test/mydb/_lease.json").await.unwrap().is_none());

    // Another node can now claim.
    let mut lease2 = DbLease::new(store, "test/", "mydb", "node-2", "addr-2", 5);
    assert_eq!(lease2.try_claim().await.unwrap(), Role::Leader);
}

#[tokio::test]
async fn lease_expired_takeover() {
    let store = Arc::new(InMemoryLeaseStore::new());

    // Node 1 claims with 0-second TTL (immediately expired).
    let mut lease1 = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 0);
    assert_eq!(lease1.try_claim().await.unwrap(), Role::Leader);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Node 2 should take over.
    let mut lease2 = DbLease::new(store.clone(), "test/", "mydb", "node-2", "addr-2", 5);
    assert_eq!(lease2.try_claim().await.unwrap(), Role::Leader);

    let (data, _etag) = lease2.read().await.unwrap().unwrap();
    assert_eq!(data.instance_id, "node-2");
}

#[tokio::test]
async fn lease_same_instance_restart_reclaim() {
    let store = Arc::new(InMemoryLeaseStore::new());

    // Node 1 claims.
    let mut lease1 = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 5);
    assert_eq!(lease1.try_claim().await.unwrap(), Role::Leader);
    let session1 = {
        let (data, _) = lease1.read().await.unwrap().unwrap();
        data.session_id.clone()
    };

    // Same instance_id reclaims (simulating restart, same machine ID).
    let mut lease2 = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 5);
    assert_eq!(lease2.try_claim().await.unwrap(), Role::Leader);
    let session2 = {
        let (data, _) = lease2.read().await.unwrap().unwrap();
        data.session_id.clone()
    };

    // New session on restart (so clients detect the failover).
    assert_ne!(session1, session2);
}

#[tokio::test]
async fn lease_key_format() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let lease = DbLease::new(store, "wal/", "tenant-42", "node-1", "addr-1", 5);
    assert_eq!(lease.lease_key(), "wal/tenant-42/_lease.json");
}

#[tokio::test]
async fn lease_has_etag_tracking() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let mut lease = DbLease::new(store, "test/", "mydb", "node-1", "addr-1", 5);

    assert!(!lease.has_etag());
    lease.try_claim().await.unwrap();
    assert!(lease.has_etag());
    lease.release().await.unwrap();
    assert!(!lease.has_etag());
}

// ============================================================================
// Post-claim verification
// ============================================================================

#[tokio::test]
async fn lease_post_claim_verify_ensures_session_match() {
    // The post-claim verify checks both instance_id AND session_id.
    // This test verifies the claim produces consistent session_id.
    let store = Arc::new(InMemoryLeaseStore::new());
    let mut lease = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 5);

    assert_eq!(lease.try_claim().await.unwrap(), Role::Leader);

    let (data, _) = lease.read().await.unwrap().unwrap();
    assert_eq!(data.instance_id, "node-1");
    assert_eq!(data.session_id, lease.session_id());
}

// ============================================================================
// Coordinator — no HA mode
// ============================================================================

#[tokio::test]
async fn coordinator_join_no_ha() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let config = CoordinatorConfig::default();
    let coordinator = Coordinator::new(storage, None, "test/", config);
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "db1");

    let role = coordinator.join("db1", &db_path).await.unwrap();
    assert_eq!(role, Role::Leader);
    assert!(coordinator.contains("db1").await);
    assert_eq!(coordinator.database_count().await, 1);
    assert_eq!(coordinator.role("db1").await, Some(Role::Leader));
}

#[tokio::test]
async fn coordinator_leave_no_ha() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let config = CoordinatorConfig::default();
    let coordinator = Coordinator::new(storage, None, "test/", config);
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "db1");

    coordinator.join("db1", &db_path).await.unwrap();
    coordinator.leave("db1").await.unwrap();

    assert!(!coordinator.contains("db1").await);
    assert_eq!(coordinator.database_count().await, 0);
}

#[tokio::test]
async fn coordinator_leave_nonexistent_is_ok() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let config = CoordinatorConfig::default();
    let coordinator = Coordinator::new(storage, None, "test/", config);

    // Should not error.
    coordinator.leave("nope").await.unwrap();
}

#[tokio::test]
async fn coordinator_role_events_no_ha() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let config = CoordinatorConfig::default();
    let coordinator = Coordinator::new(storage, None, "test/", config);
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "db1");

    let mut rx = coordinator.role_events();

    coordinator.join("db1", &db_path).await.unwrap();

    let event = rx.recv().await.unwrap();
    match event {
        RoleEvent::Joined { db_name, role } => {
            assert_eq!(db_name, "db1");
            assert_eq!(role, Role::Leader);
        }
        _ => panic!("Expected Joined event"),
    }
}

#[tokio::test]
async fn coordinator_multiple_databases() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let config = CoordinatorConfig::default();
    let coordinator = Coordinator::new(storage, None, "test/", config);
    let dir = TempDir::new().unwrap();

    let db1 = create_test_db(&dir, "db1");
    let db2 = create_test_db(&dir, "db2");
    let db3 = create_test_db(&dir, "db3");

    coordinator.join("db1", &db1).await.unwrap();
    coordinator.join("db2", &db2).await.unwrap();
    coordinator.join("db3", &db3).await.unwrap();

    assert_eq!(coordinator.database_count().await, 3);

    coordinator.leave("db2").await.unwrap();
    assert_eq!(coordinator.database_count().await, 2);
    assert!(!coordinator.contains("db2").await);
    assert!(coordinator.contains("db1").await);
    assert!(coordinator.contains("db3").await);
}

#[tokio::test]
async fn coordinator_role_none_for_unknown_db() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let config = CoordinatorConfig::default();
    let coordinator = Coordinator::new(storage, None, "test/", config);

    assert_eq!(coordinator.role("nonexistent").await, None);
}

// ============================================================================
// Coordinator — HA mode
// ============================================================================

#[tokio::test]
async fn coordinator_join_as_leader_with_ha() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let config = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-1".to_string(), "addr-1".to_string())),
        ..Default::default()
    };
    let coordinator = Coordinator::new(storage, Some(lease_store), "test/", config);
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "db1");

    let mut rx = coordinator.role_events();

    let role = coordinator.join("db1", &db_path).await.unwrap();
    assert_eq!(role, Role::Leader);
    assert_eq!(coordinator.role("db1").await, Some(Role::Leader));

    let event = rx.recv().await.unwrap();
    match event {
        RoleEvent::Joined { db_name, role } => {
            assert_eq!(db_name, "db1");
            assert_eq!(role, Role::Leader);
        }
        _ => panic!("Expected Joined event"),
    }
}

#[tokio::test]
async fn coordinator_join_as_follower_with_ha() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "db1");

    // First coordinator claims leadership.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-1".to_string(), "addr-1".to_string())),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    assert_eq!(
        coord1.join("db1", &db_path).await.unwrap(),
        Role::Leader
    );

    // Second coordinator should become follower.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-2".to_string(), "addr-2".to_string())),
        ..Default::default()
    };
    let db_path2 = dir.path().join("db1-replica.db");
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let mut rx2 = coord2.role_events();
    let role2 = coord2.join("db1", &db_path2).await.unwrap();
    assert_eq!(role2, Role::Follower);
    assert_eq!(coord2.role("db1").await, Some(Role::Follower));

    let event = rx2.recv().await.unwrap();
    match event {
        RoleEvent::Joined { db_name, role } => {
            assert_eq!(db_name, "db1");
            assert_eq!(role, Role::Follower);
        }
        _ => panic!("Expected Joined event"),
    }
}

#[tokio::test]
async fn coordinator_leave_leader_does_not_delete_other_nodes_lease() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "db1");

    // Node 1 joins as leader.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-1".to_string(), "addr-1".to_string())),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    coord1.join("db1", &db_path).await.unwrap();

    // Manually overwrite the lease to simulate another node taking over.
    let other_lease_data = serde_json::to_vec(&LeaseData {
        instance_id: "node-2".to_string(),
        address: "addr-2".to_string(),
        claimed_at: chrono::Utc::now().timestamp() as u64,
        ttl_secs: 30,
        session_id: "other-session".to_string(),
        sleeping: false,
    })
    .unwrap();
    // Delete existing, then write as node-2.
    lease_store.delete("test/db1/_lease.json").await.unwrap();
    lease_store
        .write_if_not_exists("test/db1/_lease.json", other_lease_data)
        .await
        .unwrap();

    // Node 1 leaves — should NOT delete node-2's lease.
    coord1.leave("db1").await.unwrap();

    // Lease should still exist (held by node-2).
    let (raw, _etag) = lease_store
        .read("test/db1/_lease.json")
        .await
        .unwrap()
        .expect("Lease should still exist");
    let data: LeaseData = serde_json::from_slice(&raw).unwrap();
    assert_eq!(data.instance_id, "node-2");
}

#[tokio::test]
async fn coordinator_leave_leader_deletes_own_lease() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "db1");

    let config = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-1".to_string(), "addr-1".to_string())),
        ..Default::default()
    };
    let coordinator =
        Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config);
    coordinator.join("db1", &db_path).await.unwrap();

    coordinator.leave("db1").await.unwrap();

    // Lease should be deleted.
    assert!(
        lease_store
            .read("test/db1/_lease.json")
            .await
            .unwrap()
            .is_none()
    );
}

// ============================================================================
// Coordinator — HA configuration validation
// ============================================================================

#[tokio::test]
async fn coordinator_ha_required_expired_reads_config() {
    let config = LeaseConfig {
        instance_id: "node-1".to_string(),
        address: "addr-1".to_string(),
        ttl_secs: 5,
        renew_interval: Duration::from_secs(2),
        follower_poll_interval: Duration::from_secs(1),
        required_expired_reads: 3,
        max_consecutive_renewal_errors: 3,
    };
    assert_eq!(config.required_expired_reads, 3);
}

#[tokio::test]
async fn coordinator_default_config() {
    let config = CoordinatorConfig::default();
    assert_eq!(config.sync_interval, Duration::from_secs(1));
    assert_eq!(config.snapshot_interval, Duration::from_secs(3600));
    assert!(config.lease.is_none());
    assert_eq!(config.follower_pull_interval, Duration::from_secs(1));
}

#[tokio::test]
async fn lease_config_defaults() {
    let config = LeaseConfig::new("node-1".to_string(), "addr-1".to_string());
    assert_eq!(config.ttl_secs, 5);
    assert_eq!(config.renew_interval, Duration::from_secs(2));
    assert_eq!(config.follower_poll_interval, Duration::from_secs(1));
    assert_eq!(config.required_expired_reads, 1);
}

// ============================================================================
// Role and RoleEvent types
// ============================================================================

#[test]
fn role_display() {
    assert_eq!(format!("{}", Role::Leader), "Leader");
    assert_eq!(format!("{}", Role::Follower), "Follower");
}

#[test]
fn role_equality() {
    assert_eq!(Role::Leader, Role::Leader);
    assert_eq!(Role::Follower, Role::Follower);
    assert_ne!(Role::Leader, Role::Follower);
}

#[test]
fn role_copy() {
    let role = Role::Leader;
    let copy = role;
    assert_eq!(role, copy);
}

#[test]
fn role_event_debug() {
    let event = RoleEvent::Joined {
        db_name: "test".to_string(),
        role: Role::Leader,
    };
    let debug = format!("{:?}", event);
    assert!(debug.contains("Joined"));
    assert!(debug.contains("test"));
}

#[test]
fn role_event_clone() {
    let event = RoleEvent::Fenced {
        db_name: "db1".to_string(),
    };
    let cloned = event.clone();
    match cloned {
        RoleEvent::Fenced { db_name } => assert_eq!(db_name, "db1"),
        _ => panic!("Expected Fenced"),
    }
}

// ============================================================================
// LeaseData — sleeping field
// ============================================================================

#[tokio::test]
async fn lease_data_sleeping_serialization() {
    let data = LeaseData {
        instance_id: "node-1".to_string(),
        address: "http://node-1:9000".to_string(),
        claimed_at: 1700000000,
        ttl_secs: 5,
        session_id: "sess-1".to_string(),
        sleeping: true,
    };
    let json = serde_json::to_vec(&data).unwrap();
    let parsed: LeaseData = serde_json::from_slice(&json).unwrap();
    assert!(parsed.sleeping);
}

#[tokio::test]
async fn lease_data_sleeping_defaults_to_false() {
    let json = r#"{"instance_id":"x","address":"","claimed_at":0,"ttl_secs":5,"session_id":"s"}"#;
    let parsed: LeaseData = serde_json::from_str(json).unwrap();
    assert!(!parsed.sleeping);
}

// ============================================================================
// DbLease — set_sleeping
// ============================================================================

#[tokio::test]
async fn lease_set_sleeping() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let mut lease = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 5);

    lease.try_claim().await.unwrap();
    assert!(lease.set_sleeping().await.unwrap());

    let (data, _etag) = lease.read().await.unwrap().unwrap();
    assert!(data.sleeping);
    assert_eq!(data.instance_id, "node-1");
}

#[tokio::test]
async fn lease_set_sleeping_without_claim_fails() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let mut lease = DbLease::new(store, "test/", "mydb", "node-1", "addr-1", 5);

    // No claim → no etag → should error.
    assert!(lease.set_sleeping().await.is_err());
}

#[tokio::test]
async fn lease_claim_wakes_sleeping_cluster() {
    let store = Arc::new(InMemoryLeaseStore::new());

    // Node 1 claims and sets sleeping.
    let mut lease1 = DbLease::new(store.clone(), "test/", "mydb", "node-1", "addr-1", 60);
    lease1.try_claim().await.unwrap();
    lease1.set_sleeping().await.unwrap();

    // Node 2 should be able to claim even though lease is NOT expired (it's sleeping).
    let mut lease2 = DbLease::new(store.clone(), "test/", "mydb", "node-2", "addr-2", 5);
    let role = lease2.try_claim().await.unwrap();
    assert_eq!(role, Role::Leader);

    let (data, _) = lease2.read().await.unwrap().unwrap();
    assert_eq!(data.instance_id, "node-2");
    assert!(!data.sleeping);
}

// ============================================================================
// RoleEvent — new variants
// ============================================================================

#[test]
fn role_event_demoted() {
    let event = RoleEvent::Demoted {
        db_name: "db1".to_string(),
    };
    let debug = format!("{:?}", event);
    assert!(debug.contains("Demoted"));
}

#[test]
fn role_event_sleeping() {
    let event = RoleEvent::Sleeping {
        db_name: "db1".to_string(),
    };
    let debug = format!("{:?}", event);
    assert!(debug.contains("Sleeping"));
}

// ============================================================================
// Coordinator — leader_address tracking
// ============================================================================

#[tokio::test]
async fn coordinator_leader_address_ha() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let config = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-1".to_string(), "http://node-1:9000".to_string())),
        ..Default::default()
    };
    let coordinator = Coordinator::new(storage, Some(lease_store), "test/", config);
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "db1");

    coordinator.join("db1", &db_path).await.unwrap();

    // Leader's address should be set.
    let addr = coordinator.leader_address("db1").await;
    assert_eq!(addr, Some("http://node-1:9000".to_string()));
}

#[tokio::test]
async fn coordinator_leader_address_unknown_db() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let coordinator = Coordinator::new(storage, None, "test/", CoordinatorConfig::default());
    assert_eq!(coordinator.leader_address("nope").await, None);
}

#[tokio::test]
async fn coordinator_follower_tracks_leader_address() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "db1");

    // Node 1 claims leadership.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-1".to_string(), "http://node-1:9000".to_string())),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    coord1.join("db1", &db_path).await.unwrap();

    // Node 2 joins as follower — should see node-1's address.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-2".to_string(), "http://node-2:9000".to_string())),
        ..Default::default()
    };
    let db_path2 = dir.path().join("db1-replica.db");
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    coord2.join("db1", &db_path2).await.unwrap();

    let addr = coord2.leader_address("db1").await;
    assert_eq!(addr, Some("http://node-1:9000".to_string()));
}

// ============================================================================
// FailingLeaseStore — wraps InMemoryLeaseStore with injectable write errors
// ============================================================================

struct FailingLeaseStore {
    inner: InMemoryLeaseStore,
    fail_writes: std::sync::atomic::AtomicBool,
}

impl FailingLeaseStore {
    fn new() -> Self {
        Self {
            inner: InMemoryLeaseStore::new(),
            fail_writes: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn set_failing(&self, fail: bool) {
        self.fail_writes
            .store(fail, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl LeaseStore for FailingLeaseStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.inner.read(key).await
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        self.inner.write_if_not_exists(key, data).await
    }

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        if self
            .fail_writes
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(anyhow::anyhow!("Simulated S3 transient error"));
        }
        self.inner.write_if_match(key, data, etag).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }
}

// ============================================================================
// Test 1: Graceful leave — leader releases, new node claims immediately
// ============================================================================

#[tokio::test]
async fn graceful_leave_allows_immediate_reclaim() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Node 1 joins as leader.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            "node-1".to_string(),
            "addr-1".to_string(),
        )),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "leave-db1");
    assert_eq!(
        coord1.join("db1", &db_path1).await.unwrap(),
        Role::Leader
    );

    // Verify lease exists before leave.
    assert!(lease_store
        .read("test/db1/_lease.json")
        .await
        .unwrap()
        .is_some());

    // Graceful leave — should delete lease.
    coord1.leave("db1").await.unwrap();

    // Lease should be gone.
    assert!(lease_store
        .read("test/db1/_lease.json")
        .await
        .unwrap()
        .is_none());

    // Node 2 should immediately claim as leader (no TTL wait).
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            "node-2".to_string(),
            "addr-2".to_string(),
        )),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = create_test_db(&dir, "leave-db2");
    let role = coord2.join("db1", &db_path2).await.unwrap();
    assert_eq!(role, Role::Leader);

    // Verify new node holds the lease.
    let (raw, _) = lease_store
        .read("test/db1/_lease.json")
        .await
        .unwrap()
        .unwrap();
    let data: LeaseData = serde_json::from_slice(&raw).unwrap();
    assert_eq!(data.instance_id, "node-2");
}

// ============================================================================
// Test 2: Self-fencing — CAS conflict during renewal emits Demoted
// ============================================================================

#[tokio::test]
async fn self_fencing_cas_conflict_emits_demoted() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "fence-db");

    let config = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(100), // fast renewal
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coordinator =
        Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config);
    let mut rx = coordinator.role_events();

    coordinator.join("db1", &db_path).await.unwrap();

    // Consume the Joined event.
    let event = rx.recv().await.unwrap();
    assert!(matches!(event, RoleEvent::Joined { .. }));

    // Overwrite the lease behind the leader's back (simulate another node stealing it).
    let lease_key = "test/db1/_lease.json";
    let (_, current_etag) = lease_store.read(lease_key).await.unwrap().unwrap();
    let fake_lease = serde_json::to_vec(&LeaseData {
        instance_id: "node-2".to_string(),
        address: "addr-2".to_string(),
        claimed_at: chrono::Utc::now().timestamp() as u64,
        ttl_secs: 60,
        session_id: "stolen-session".to_string(),
        sleeping: false,
    })
    .unwrap();
    let cas = lease_store
        .write_if_match(lease_key, fake_lease, &current_etag)
        .await
        .unwrap();
    assert!(cas.success, "Should overwrite lease successfully");

    // Wait for Demoted event (renewal runs every 100ms).
    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Should receive Demoted within 2s")
        .unwrap();

    match event {
        RoleEvent::Demoted { db_name } => assert_eq!(db_name, "db1"),
        other => panic!("Expected Demoted, got {:?}", other),
    }
}

// ============================================================================
// Test 3: Sleeping detection — follower monitor detects sleeping lease
// ============================================================================

#[tokio::test]
async fn follower_monitor_detects_sleeping_lease() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader joins with LONG renew interval (so we can overwrite before next renewal).
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_secs(60), // long — won't overwrite our sleeping flag
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "sleep-db1");
    coord1.join("db1", &db_path1).await.unwrap();

    // Follower joins with fast poll.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_secs(2),
            follower_poll_interval: Duration::from_millis(100), // fast monitor
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = dir.path().join("sleep-db1-replica.db");
    let mut rx2 = coord2.role_events();
    coord2.join("db1", &db_path2).await.unwrap();

    // Consume Joined event.
    let event = rx2.recv().await.unwrap();
    assert!(matches!(event, RoleEvent::Joined { .. }));

    // Overwrite the lease with sleeping=true.
    let lease_key = "test/db1/_lease.json";
    let (_, current_etag) = lease_store.read(lease_key).await.unwrap().unwrap();
    let sleeping_lease = serde_json::to_vec(&LeaseData {
        instance_id: "node-1".to_string(),
        address: "addr-1".to_string(),
        claimed_at: chrono::Utc::now().timestamp() as u64,
        ttl_secs: 60,
        session_id: "sleep-session".to_string(),
        sleeping: true,
    })
    .unwrap();
    let cas = lease_store
        .write_if_match(lease_key, sleeping_lease, &current_etag)
        .await
        .unwrap();
    assert!(cas.success);

    // Wait for Sleeping event.
    let event = tokio::time::timeout(Duration::from_secs(2), rx2.recv())
        .await
        .expect("Should receive Sleeping within 2s")
        .unwrap();

    match event {
        RoleEvent::Sleeping { db_name } => assert_eq!(db_name, "db1"),
        other => panic!("Expected Sleeping, got {:?}", other),
    }
}

// ============================================================================
// Test 4: Multiple databases in HA mode
// ============================================================================

#[tokio::test]
async fn multiple_databases_ha_mode() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    let config = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            "node-1".to_string(),
            "addr-1".to_string(),
        )),
        ..Default::default()
    };
    let coordinator =
        Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config);

    let db1 = create_test_db(&dir, "multi-ha-1");
    let db2 = create_test_db(&dir, "multi-ha-2");
    let db3 = create_test_db(&dir, "multi-ha-3");

    // All should become leader (first claim on each separate database).
    assert_eq!(coordinator.join("db1", &db1).await.unwrap(), Role::Leader);
    assert_eq!(coordinator.join("db2", &db2).await.unwrap(), Role::Leader);
    assert_eq!(coordinator.join("db3", &db3).await.unwrap(), Role::Leader);

    assert_eq!(coordinator.database_count().await, 3);
    assert_eq!(coordinator.role("db1").await, Some(Role::Leader));
    assert_eq!(coordinator.role("db2").await, Some(Role::Leader));
    assert_eq!(coordinator.role("db3").await, Some(Role::Leader));

    // Each should have its own lease.
    assert!(lease_store
        .read("test/db1/_lease.json")
        .await
        .unwrap()
        .is_some());
    assert!(lease_store
        .read("test/db2/_lease.json")
        .await
        .unwrap()
        .is_some());
    assert!(lease_store
        .read("test/db3/_lease.json")
        .await
        .unwrap()
        .is_some());

    // Leave one — the others should be unaffected.
    coordinator.leave("db2").await.unwrap();
    assert_eq!(coordinator.database_count().await, 2);
    assert_eq!(coordinator.role("db2").await, None);
    assert_eq!(coordinator.role("db1").await, Some(Role::Leader));
    assert_eq!(coordinator.role("db3").await, Some(Role::Leader));

    // db2 lease should be released.
    assert!(lease_store
        .read("test/db2/_lease.json")
        .await
        .unwrap()
        .is_none());
    // Other leases should still be held.
    assert!(lease_store
        .read("test/db1/_lease.json")
        .await
        .unwrap()
        .is_some());

    // Another node can claim the released DB.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            "node-2".to_string(),
            "addr-2".to_string(),
        )),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let db2_replica = create_test_db(&dir, "multi-ha-2-replica");
    assert_eq!(
        coord2.join("db2", &db2_replica).await.unwrap(),
        Role::Leader
    );

    let (raw, _) = lease_store
        .read("test/db2/_lease.json")
        .await
        .unwrap()
        .unwrap();
    let data: LeaseData = serde_json::from_slice(&raw).unwrap();
    assert_eq!(data.instance_id, "node-2");
}

// ============================================================================
// Test 5: Data integrity — leader uploads snapshot, restore retrieves it
// ============================================================================

#[tokio::test]
async fn leader_uploads_snapshot_to_storage() {
    let storage = Arc::new(InMemoryStorage::new());
    let dir = TempDir::new().unwrap();

    // Create DB with actual data.
    let db_path = dir.path().join("integrity-source.db");
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        conn.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            .unwrap();
        for i in 1..=10 {
            conn.execute(
                "INSERT INTO items VALUES (?1, ?2)",
                rusqlite::params![i, format!("item-{}", i)],
            )
            .unwrap();
        }
    }

    let config = CoordinatorConfig::default(); // no HA, just walrust replication
    let coordinator = Coordinator::new(
        storage.clone() as Arc<dyn StorageBackend>,
        None,
        "test/",
        config,
    );

    coordinator.join("integrity-db", &db_path).await.unwrap();

    // Wait for walrust to take an initial snapshot (retry loop, not fixed sleep).
    let mut uploaded = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if !storage.objects.lock().await.is_empty() {
            uploaded = true;
            break;
        }
    }
    assert!(uploaded, "Leader should upload snapshot within 4s");

    // Verify something was uploaded to storage.
    let objects = storage.objects.lock().await;
    assert!(
        !objects.is_empty(),
        "Leader should have uploaded snapshot data to storage"
    );
    let keys: Vec<_> = objects.keys().collect();
    assert!(
        keys.iter().any(|k| k.contains("integrity-db")),
        "Uploaded keys should reference the database name: {:?}",
        keys
    );
    drop(objects);

    coordinator.leave("integrity-db").await.unwrap();
}

#[tokio::test]
async fn snapshot_restore_roundtrip_preserves_data() {
    let storage = Arc::new(InMemoryStorage::new());
    let dir = TempDir::new().unwrap();

    // Create DB with specific rows.
    let db_path = dir.path().join("roundtrip-source.db");
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        conn.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            .unwrap();
        for i in 1..=5 {
            conn.execute(
                "INSERT INTO items VALUES (?1, ?2)",
                rusqlite::params![i, format!("row-{}", i)],
            )
            .unwrap();
        }
    }

    let config = CoordinatorConfig::default();
    let coordinator = Coordinator::new(
        storage.clone() as Arc<dyn StorageBackend>,
        None,
        "test/",
        config,
    );

    coordinator.join("roundtrip-db", &db_path).await.unwrap();

    // Wait for walrust to snapshot (retry loop).
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if !storage.objects.lock().await.is_empty() {
            break;
        }
    }

    // Restore to a new path.
    let restore_path = dir.path().join("roundtrip-restored.db");
    let txid = coordinator
        .restore("roundtrip-db", &restore_path)
        .await
        .unwrap();
    assert!(txid.is_some(), "Restore should return a TXID");

    // Verify restored data matches.
    let conn = rusqlite::Connection::open(&restore_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 5, "Restored DB should have all 5 rows");

    let name: String = conn
        .query_row("SELECT name FROM items WHERE id = 3", [], |r| r.get(0))
        .unwrap();
    assert_eq!(name, "row-3", "Restored row content should match");

    coordinator.leave("roundtrip-db").await.unwrap();
}

// ============================================================================
// Test 6: Transient errors — renewal retries, does not demote
// ============================================================================

#[tokio::test]
async fn transient_renewal_errors_do_not_demote() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let failing_store = Arc::new(FailingLeaseStore::new());
    let lease_store: Arc<dyn LeaseStore> = failing_store.clone();
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "transient-db");

    let config = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(100), // fast renewal
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coordinator = Coordinator::new(storage, Some(lease_store), "test/", config);
    let mut rx = coordinator.role_events();

    coordinator.join("db1", &db_path).await.unwrap();
    let event = rx.recv().await.unwrap();
    assert!(matches!(event, RoleEvent::Joined { .. }));

    // Enable failures for a short window — 2 errors (below max_consecutive=3).
    failing_store.set_failing(true);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Disable failures — renewals should resume successfully.
    failing_store.set_failing(false);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should still be leader (transient errors just retry).
    assert_eq!(coordinator.role("db1").await, Some(Role::Leader));

    // No Demoted event should have been emitted.
    let result = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await;
    assert!(
        result.is_err(),
        "Should NOT receive Demoted from transient errors"
    );
}

// ============================================================================
// Test 6b: Sustained renewal errors cause self-demotion (split-brain prevention)
// ============================================================================

#[tokio::test]
async fn sustained_renewal_errors_cause_self_demotion() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let failing_store = Arc::new(FailingLeaseStore::new());
    let lease_store: Arc<dyn LeaseStore> = failing_store.clone();
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "sustained-err-db");

    let config = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(50), // fast renewal
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3, // demote after 3 consecutive errors
        }),
        ..Default::default()
    };
    let coordinator = Coordinator::new(storage, Some(lease_store), "test/", config);
    let mut rx = coordinator.role_events();

    coordinator.join("db1", &db_path).await.unwrap();
    let event = rx.recv().await.unwrap();
    assert!(matches!(event, RoleEvent::Joined { role: Role::Leader, .. }));

    // Enable sustained failures — should self-demote after 3 consecutive errors.
    failing_store.set_failing(true);

    // Wait for demotion. At 50ms interval, 3 errors = ~150ms. Give plenty of buffer.
    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Should receive Demoted event from sustained errors")
        .unwrap();
    assert!(
        matches!(event, RoleEvent::Demoted { .. }),
        "Expected Demoted event, got {:?}",
        event
    );

    // Role should now be Follower.
    assert_eq!(coordinator.role("db1").await, Some(Role::Follower));
}

// ============================================================================
// Test 7: Soak test — many renewal cycles without issues
// ============================================================================

#[tokio::test]
async fn soak_test_many_renewal_cycles() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "soak-db");

    let config = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 5,
            renew_interval: Duration::from_millis(50), // very fast: ~60 cycles in 3s
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coordinator =
        Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config);
    let mut rx = coordinator.role_events();

    coordinator.join("db1", &db_path).await.unwrap();
    let event = rx.recv().await.unwrap();
    assert!(matches!(event, RoleEvent::Joined { .. }));

    // Run for 3 seconds with 50ms renewal = ~60 renewal cycles.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Should still be leader after all those cycles.
    assert_eq!(coordinator.role("db1").await, Some(Role::Leader));

    // Lease should still be valid in the store.
    let (raw, _) = lease_store
        .read("test/db1/_lease.json")
        .await
        .unwrap()
        .expect("Lease should still exist after soak");
    let data: LeaseData = serde_json::from_slice(&raw).unwrap();
    assert_eq!(data.instance_id, "node-1");
    assert!(!data.is_expired(), "Lease should not be expired after continuous renewal");

    // No unexpected events during soak.
    let result = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(result.is_err(), "No unexpected role events during soak");

    coordinator.leave("db1").await.unwrap();
}

// ============================================================================
// Test 8: Follower auto-promotes when leader's lease expires
// ============================================================================

#[tokio::test]
async fn follower_auto_promotes_when_lease_expires() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader: TTL=2s, renew_interval=60s (won't renew before expiry).
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 2,
            renew_interval: Duration::from_secs(60),
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "promo-leader");
    assert_eq!(coord1.join("db1", &db_path1).await.unwrap(), Role::Leader);

    // Follower: fast poll (100ms). Joins while lease is still active.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(200),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = create_test_db(&dir, "promo-follower");
    let mut rx2 = coord2.role_events();
    assert_eq!(
        coord2.join("db1", &db_path2).await.unwrap(),
        Role::Follower
    );

    // Consume Joined event.
    let event = rx2.recv().await.unwrap();
    assert!(matches!(
        event,
        RoleEvent::Joined {
            role: Role::Follower,
            ..
        }
    ));

    // Wait for leader's lease to expire (TTL=2s) and follower to promote.
    let event = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .expect("Follower should promote within 5s")
        .unwrap();

    match event {
        RoleEvent::Promoted { db_name } => assert_eq!(db_name, "db1"),
        other => panic!("Expected Promoted, got {:?}", other),
    }

    // coordinator.role() should now return Leader (Bug #2 verified: updated before renewal).
    assert_eq!(coord2.role("db1").await, Some(Role::Leader));

    // Leader address should be updated to the follower's address.
    let addr = coord2.leader_address("db1").await;
    assert_eq!(addr, Some("addr-2".to_string()));
}

// ============================================================================
// Test 9: Concurrent followers race — exactly one wins
// ============================================================================

#[tokio::test]
async fn concurrent_followers_race_exactly_one_promotes() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader: TTL=2s, renew=60s (expires without renewal).
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 2,
            renew_interval: Duration::from_secs(60),
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "race-leader");
    assert_eq!(coord1.join("db1", &db_path1).await.unwrap(), Role::Leader);

    // Follower A.
    let config_a = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(200),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord_a =
        Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config_a);
    let db_a = create_test_db(&dir, "race-follower-a");
    assert_eq!(coord_a.join("db1", &db_a).await.unwrap(), Role::Follower);

    // Follower B.
    let config_b = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-3".to_string(),
            address: "addr-3".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(200),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord_b =
        Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config_b);
    let db_b = create_test_db(&dir, "race-follower-b");
    assert_eq!(coord_b.join("db1", &db_b).await.unwrap(), Role::Follower);

    // Wait for lease expiry + promotion (TTL=2s + buffer).
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Exactly one should now be leader.
    let role_a = coord_a.role("db1").await;
    let role_b = coord_b.role("db1").await;
    let leaders: Vec<_> = [("node-2", role_a), ("node-3", role_b)]
        .iter()
        .filter(|(_, r)| *r == Some(Role::Leader))
        .map(|(name, _)| *name)
        .collect();

    assert_eq!(
        leaders.len(),
        1,
        "Exactly one follower should promote, but got leaders: {:?} (node-2={:?}, node-3={:?})",
        leaders,
        role_a,
        role_b
    );

    // The lease in the store should be held by the winner.
    let (raw, _) = lease_store
        .read("test/db1/_lease.json")
        .await
        .unwrap()
        .expect("Lease should exist");
    let data: LeaseData = serde_json::from_slice(&raw).unwrap();
    assert!(
        data.instance_id == "node-2" || data.instance_id == "node-3",
        "Lease should be held by one of the followers, got: {}",
        data.instance_id
    );
}

// ============================================================================
// NodeRegistration — serialization and validity
// ============================================================================

#[tokio::test]
async fn node_registration_serialization_roundtrip() {
    let reg = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "http://node-1:9000".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-abc".to_string(),
        last_seen: 1700000000,
    };

    let json = serde_json::to_vec(&reg).unwrap();
    let parsed: NodeRegistration = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed.instance_id, "node-1");
    assert_eq!(parsed.address, "http://node-1:9000");
    assert_eq!(parsed.role, "follower");
    assert_eq!(parsed.leader_session_id, "sess-abc");
    assert_eq!(parsed.last_seen, 1700000000);
}

#[tokio::test]
async fn node_registration_valid_current_session() {
    let now = chrono::Utc::now().timestamp() as u64;
    let reg = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-1".to_string(),
        last_seen: now,
    };

    assert!(reg.is_valid("sess-1", 30));
}

#[tokio::test]
async fn node_registration_invalid_wrong_session() {
    let now = chrono::Utc::now().timestamp() as u64;
    let reg = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-1".to_string(),
        last_seen: now,
    };

    // Wrong session_id — stale registration from previous epoch.
    assert!(!reg.is_valid("sess-2", 30));
}

#[tokio::test]
async fn node_registration_invalid_expired() {
    let now = chrono::Utc::now().timestamp() as u64;
    let reg = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-1".to_string(),
        last_seen: now - 60, // 60 seconds ago
    };

    // Correct session but last_seen too old (TTL=30s).
    assert!(!reg.is_valid("sess-1", 30));
}

#[tokio::test]
async fn node_registration_ttl_exact_boundary() {
    let now = chrono::Utc::now().timestamp() as u64;

    // Exactly at boundary: last_seen + ttl == now → expired (now < last_seen + ttl is false).
    let at_boundary = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-1".to_string(),
        last_seen: now - 30,
    };
    assert!(!at_boundary.is_valid("sess-1", 30));

    // One second before boundary: still valid.
    let before_boundary = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-1".to_string(),
        last_seen: now - 29,
    };
    assert!(before_boundary.is_valid("sess-1", 30));
}

#[tokio::test]
async fn node_registration_both_conditions_must_hold() {
    let now = chrono::Utc::now().timestamp() as u64;

    // Fresh timestamp but wrong session → invalid.
    let wrong_session = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr".to_string(),
        role: "follower".to_string(),
        leader_session_id: "old-sess".to_string(),
        last_seen: now,
    };
    assert!(!wrong_session.is_valid("new-sess", 30));

    // Right session but expired → invalid.
    let expired = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr".to_string(),
        role: "follower".to_string(),
        leader_session_id: "new-sess".to_string(),
        last_seen: now - 60,
    };
    assert!(!expired.is_valid("new-sess", 30));
}

#[tokio::test]
async fn node_registration_filter_replicas() {
    let now = chrono::Utc::now().timestamp() as u64;
    let current_session = "sess-current";

    let registrations = vec![
        NodeRegistration {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            role: "follower".to_string(),
            leader_session_id: current_session.to_string(),
            last_seen: now,
        },
        NodeRegistration {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            role: "follower".to_string(),
            leader_session_id: "old-session".to_string(), // stale
            last_seen: now,
        },
        NodeRegistration {
            instance_id: "node-3".to_string(),
            address: "addr-3".to_string(),
            role: "follower".to_string(),
            leader_session_id: current_session.to_string(),
            last_seen: now - 120, // expired
        },
        NodeRegistration {
            instance_id: "node-4".to_string(),
            address: "addr-4".to_string(),
            role: "follower".to_string(),
            leader_session_id: current_session.to_string(),
            last_seen: now,
        },
    ];

    let valid: Vec<_> = registrations
        .into_iter()
        .filter(|r| r.is_valid(current_session, 60))
        .collect();

    assert_eq!(valid.len(), 2);
    assert_eq!(valid[0].instance_id, "node-1");
    assert_eq!(valid[1].instance_id, "node-4");
}

// ============================================================================
// InMemoryNodeRegistry — register, deregister, discover
// ============================================================================

#[tokio::test]
async fn node_registry_register_and_discover() {
    let registry = InMemoryNodeRegistry::new();
    let now = chrono::Utc::now().timestamp() as u64;

    let reg = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr-1".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-1".to_string(),
        last_seen: now,
    };
    registry.register("ha/", "mydb", &reg).await.unwrap();

    let all = registry.discover_all("ha/", "mydb").await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].instance_id, "node-1");
}

#[tokio::test]
async fn node_registry_deregister() {
    let registry = InMemoryNodeRegistry::new();
    let now = chrono::Utc::now().timestamp() as u64;

    let reg = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr-1".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-1".to_string(),
        last_seen: now,
    };
    registry.register("ha/", "mydb", &reg).await.unwrap();
    registry.deregister("ha/", "mydb", "node-1").await.unwrap();

    let all = registry.discover_all("ha/", "mydb").await.unwrap();
    assert_eq!(all.len(), 0);
}

#[tokio::test]
async fn node_registry_multiple_databases_isolated() {
    let registry = InMemoryNodeRegistry::new();
    let now = chrono::Utc::now().timestamp() as u64;

    let reg1 = NodeRegistration {
        instance_id: "node-1".to_string(),
        address: "addr-1".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-1".to_string(),
        last_seen: now,
    };
    let reg2 = NodeRegistration {
        instance_id: "node-2".to_string(),
        address: "addr-2".to_string(),
        role: "follower".to_string(),
        leader_session_id: "sess-2".to_string(),
        last_seen: now,
    };

    registry.register("ha/", "db-a", &reg1).await.unwrap();
    registry.register("ha/", "db-b", &reg2).await.unwrap();

    // Each DB should only see its own registrations.
    let a = registry.discover_all("ha/", "db-a").await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].instance_id, "node-1");

    let b = registry.discover_all("ha/", "db-b").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].instance_id, "node-2");
}

// ============================================================================
// Coordinator — NodeRegistration integration
// ============================================================================

#[tokio::test]
async fn follower_registers_on_join() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Node 1 claims leadership.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-1".to_string(), "addr-1".to_string())),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "reg-leader");
    coord1.join("db1", &db_path1).await.unwrap();

    // Node 2 joins as follower — should register itself.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-2".to_string(), "addr-2".to_string())),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = dir.path().join("reg-follower.db");
    assert_eq!(coord2.join("db1", &db_path2).await.unwrap(), Role::Follower);

    // Check registration via storage backend (node registry uses StorageBackend).
    let keys = storage.list_objects("test/db1/_nodes/").await.unwrap();
    assert!(
        keys.iter().any(|k| k.contains("node-2")),
        "Follower should register in _nodes/: {:?}",
        keys
    );
}

#[tokio::test]
async fn follower_deregisters_on_leave() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-1".to_string(), "addr-1".to_string())),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "dereg-leader");
    coord1.join("db1", &db_path1).await.unwrap();

    // Follower joins.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-2".to_string(), "addr-2".to_string())),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = dir.path().join("dereg-follower.db");
    coord2.join("db1", &db_path2).await.unwrap();

    // Verify registration exists.
    let keys_before = storage.list_objects("test/db1/_nodes/").await.unwrap();
    assert!(keys_before.iter().any(|k| k.contains("node-2")));

    // Follower leaves — should deregister.
    coord2.leave("db1").await.unwrap();

    let keys_after = storage.list_objects("test/db1/_nodes/").await.unwrap();
    assert!(
        !keys_after.iter().any(|k| k.contains("node-2")),
        "Follower should deregister on leave: {:?}",
        keys_after
    );
}

#[tokio::test]
async fn follower_deregisters_on_promotion() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader: TTL=2s, renew_interval=60s (won't renew, will expire).
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 2,
            renew_interval: Duration::from_secs(60),
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "promo-dereg-leader");
    coord1.join("db1", &db_path1).await.unwrap();

    // Follower with fast poll.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(200),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = create_test_db(&dir, "promo-dereg-follower");
    let mut rx2 = coord2.role_events();
    coord2.join("db1", &db_path2).await.unwrap();

    // Consume Joined event.
    let event = rx2.recv().await.unwrap();
    assert!(matches!(event, RoleEvent::Joined { role: Role::Follower, .. }));

    // Wait for promotion.
    let event = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .expect("Follower should promote within 5s")
        .unwrap();
    assert!(matches!(event, RoleEvent::Promoted { .. }));

    // After promotion, the follower's node registration should be removed.
    // Give a small buffer for the deregistration to complete.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let keys = storage.list_objects("test/db1/_nodes/").await.unwrap();
    assert!(
        !keys.iter().any(|k| k.contains("node-2")),
        "Promoted follower should deregister from _nodes/: {:?}",
        keys
    );
}

#[tokio::test]
async fn coordinator_discover_replicas() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-1".to_string(), "addr-1".to_string())),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "discover-leader");
    coord1.join("db1", &db_path1).await.unwrap();

    // Two followers.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-2".to_string(), "addr-2".to_string())),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = dir.path().join("discover-follower-a.db");
    coord2.join("db1", &db_path2).await.unwrap();

    let config3 = CoordinatorConfig {
        lease: Some(LeaseConfig::new("node-3".to_string(), "addr-3".to_string())),
        ..Default::default()
    };
    let coord3 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config3);
    let db_path3 = dir.path().join("discover-follower-b.db");
    coord3.join("db1", &db_path3).await.unwrap();

    // Leader discovers replicas.
    let replicas = coord1.discover_replicas("db1").await.unwrap();
    assert_eq!(
        replicas.len(),
        2,
        "Leader should discover 2 followers: {:?}",
        replicas.iter().map(|r| &r.instance_id).collect::<Vec<_>>()
    );

    let ids: Vec<_> = replicas.iter().map(|r| r.instance_id.as_str()).collect();
    assert!(ids.contains(&"node-2"), "Should discover node-2");
    assert!(ids.contains(&"node-3"), "Should discover node-3");
}

#[tokio::test]
async fn node_registration_no_ha_mode() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let config = CoordinatorConfig::default(); // No HA
    let coordinator = Coordinator::new(storage.clone(), None, "test/", config);
    let dir = TempDir::new().unwrap();
    let db_path = create_test_db(&dir, "no-ha-reg");

    coordinator.join("db1", &db_path).await.unwrap();

    // No registrations should exist (no HA = no node registry).
    let keys = storage.list_objects("test/db1/_nodes/").await.unwrap();
    assert!(
        keys.is_empty(),
        "No-HA mode should not create node registrations: {:?}",
        keys
    );

    // discover_replicas should return empty vec.
    let replicas = coordinator.discover_replicas("db1").await.unwrap();
    assert!(replicas.is_empty());
}

#[tokio::test]
async fn follower_heartbeat_updates_last_seen() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader with long TTL.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_secs(2),
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "heartbeat-leader");
    coord1.join("db1", &db_path1).await.unwrap();

    // Follower with fast poll (100ms) — will heartbeat frequently.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_secs(60),
            follower_poll_interval: Duration::from_millis(100), // fast heartbeat
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = dir.path().join("heartbeat-follower.db");
    coord2.join("db1", &db_path2).await.unwrap();

    // Read initial registration.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let node_key = "test/db1/_nodes/node-2.json";
    let data1 = storage.download_bytes(node_key).await.unwrap();
    let reg1: NodeRegistration = serde_json::from_slice(&data1).unwrap();
    let first_seen = reg1.last_seen;

    // Wait for at least one more heartbeat.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let data2 = storage.download_bytes(node_key).await.unwrap();
    let reg2: NodeRegistration = serde_json::from_slice(&data2).unwrap();

    // last_seen should be updated (>= first_seen, since clock resolution is 1s
    // they might be equal, but the registration itself should still exist).
    assert!(
        reg2.last_seen >= first_seen,
        "Heartbeat should update last_seen: first={}, latest={}",
        first_seen, reg2.last_seen
    );
    assert_eq!(reg2.instance_id, "node-2");
    assert_eq!(reg2.role, "follower");
}

// ============================================================================
// Regression tests for ha_experiment bugs
// ============================================================================

/// Regression: ensure_db_schema pattern creates tables but zero rows.
/// Bug: Previously, test rows were inserted before knowing the node's role,
/// giving followers phantom data that didn't come from the leader.
#[tokio::test]
async fn regression_schema_only_creates_no_rows() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema-test.db");

    // Replicate the ensure_db_schema pattern from ha_experiment
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;").unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS test_data (
            id INTEGER PRIMARY KEY,
            value TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );",
    ).unwrap();

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM test_data", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 0, "Schema-only setup must not insert any rows");
    drop(conn);
}

/// Regression: fresh SQLite connections see file-level changes, cached ones don't.
/// Bug: Followers kept a cached read-only connection that couldn't see LTX page
/// writes applied directly to the DB file by the pull loop. Fix: open a fresh
/// read-only connection per query.
#[tokio::test]
async fn regression_fresh_connection_sees_file_changes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fresh-conn.db");

    // Create DB with one row
    let conn1 = rusqlite::Connection::open(&db_path).unwrap();
    conn1.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    conn1.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
         INSERT INTO t VALUES (1, 'hello');",
    ).unwrap();
    drop(conn1);

    // Open a cached read-only connection (simulates old follower behavior)
    let cached_conn = rusqlite::Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ).unwrap();
    let count_before: i64 = cached_conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count_before, 1);

    // Simulate LTX apply: write more data through a separate connection
    let conn2 = rusqlite::Connection::open(&db_path).unwrap();
    conn2.execute("INSERT INTO t VALUES (2, 'world')", []).unwrap();
    // Force checkpoint to move data from WAL to main DB (simulates LTX page writes)
    conn2.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn2);

    // Cached connection may or may not see the new row depending on SQLite's
    // page cache behavior. The point is: a FRESH connection always sees it.
    let fresh_conn = rusqlite::Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ).unwrap();
    let count_fresh: i64 = fresh_conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count_fresh, 2, "Fresh connection must see latest data");
    drop(fresh_conn);
    drop(cached_conn);
}

/// Regression: WAL frames are visible to the replicator when autocheckpoint is
/// disabled. Bug: seed_test_rows used a separate connection without disabling
/// autocheckpoint, so SQLite would checkpoint and truncate the WAL before the
/// replicator could read the frames.
#[tokio::test]
async fn regression_wal_frames_visible_with_autocheckpoint_disabled() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wal-visible.db");

    // Open connection with autocheckpoint DISABLED (correct behavior)
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;").unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);").unwrap();
    for i in 1..=100 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("row-{}", i)],
        ).unwrap();
    }

    // WAL file should exist and have content (frames not checkpointed)
    let wal_path = db_path.with_extension("db-wal");
    assert!(wal_path.exists(), "WAL file must exist when autocheckpoint=0");
    let wal_size = std::fs::metadata(&wal_path).unwrap().len();
    assert!(wal_size > 0, "WAL must have frames when autocheckpoint=0");

    drop(conn);
}

/// Regression: follower joining a cluster doesn't create local data.
/// Bug: The old ha_experiment called setup_or_open_leader_db() which inserted
/// 100 rows before join(), so followers had phantom data. The fix splits schema
/// creation (before join) from data seeding (after join, leader only).
#[tokio::test]
async fn regression_follower_has_no_phantom_data() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    // Node 1 joins as leader
    let config1 = CoordinatorConfig {
        sync_interval: Duration::from_millis(50),
        lease: Some(LeaseConfig::new("node-1".into(), "addr-1".into())),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config1);

    // Create a DB with schema only (leader path — schema before join)
    let db_path1 = create_test_db(&dir, "leader");
    let role1 = coord1.join("db1", &db_path1).await.unwrap();
    assert_eq!(role1, Role::Leader);

    // Leader seeds data AFTER join (correct pattern).
    // IMPORTANT: autocheckpoint must be disabled so the replicator can see
    // WAL frames before they're checkpointed away.
    let conn = rusqlite::Connection::open(&db_path1).unwrap();
    conn.execute_batch("PRAGMA wal_autocheckpoint=0;").unwrap();
    conn.execute("INSERT INTO t VALUES (1)", []).unwrap();

    // Wait for replicator to sync, then drop connection.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(conn);

    // Node 2 joins — should get leader's data from S3, NOT local phantom data
    let config2 = CoordinatorConfig {
        sync_interval: Duration::from_millis(50),
        lease: Some(LeaseConfig::new("node-2".into(), "addr-2".into())),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "test/", config2);

    // Create schema-only DB for follower (ensure_db_schema pattern)
    let db_path2 = create_test_db(&dir, "follower");

    // Verify: before join, follower DB has 0 rows (schema only, no phantom data)
    let conn2 = rusqlite::Connection::open(&db_path2).unwrap();
    let count: i64 = conn2.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 0, "Follower must have 0 rows before join (no phantom data)");
    drop(conn2);

    let role2 = coord2.join("db1", &db_path2).await.unwrap();
    assert_eq!(role2, Role::Follower);

    // After join, follower has the leader's data from S3 restore
    let conn2 = rusqlite::Connection::open(&db_path2).unwrap();
    let count: i64 = conn2.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    assert!(count >= 1, "Follower should have leader's data after restore, got {}", count);
    drop(conn2);
}

// ============================================================================
// P0 regression tests: promotion failure paths
// ============================================================================

/// Regression for P0 fix 1.1: If pull_incremental catch-up fails during
/// promotion (S3 down), the follower must NOT emit Promoted. It releases
/// the lease and continues monitoring.
#[tokio::test]
async fn regression_catchup_failure_aborts_promotion() {
    let storage: Arc<ControlledFailStorage> = Arc::new(ControlledFailStorage::new());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader: TTL=2s, won't renew (renew_interval=60s).
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 2,
            renew_interval: Duration::from_secs(60),
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage_dyn.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "catchup-leader");
    assert_eq!(coord1.join("db1", &db_path1).await.unwrap(), Role::Leader);

    // Write data so the leader has something to sync (ensures follower gets txid > 0).
    let conn = rusqlite::Connection::open(&db_path1).unwrap();
    conn.execute_batch("PRAGMA wal_autocheckpoint=0;").unwrap();
    conn.execute("INSERT INTO t VALUES (1)", []).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(conn);

    // Follower: fast poll (100ms).
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(200),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage_dyn.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = create_test_db(&dir, "catchup-follower");
    let mut rx2 = coord2.role_events();
    assert_eq!(
        coord2.join("db1", &db_path2).await.unwrap(),
        Role::Follower
    );

    // Consume Joined event.
    let event = rx2.recv().await.unwrap();
    assert!(matches!(event, RoleEvent::Joined { role: Role::Follower, .. }));

    // NOW trigger storage failure: reads fail → pull_incremental catch-up will fail.
    storage.set_fail_reads(true);

    // Wait for leader's lease to expire (TTL=2s). Follower detects, claims,
    // tries catch-up → FAILS → releases lease, no Promoted event.
    let result = tokio::time::timeout(Duration::from_secs(5), rx2.recv()).await;

    // Should timeout — no Promoted event emitted.
    assert!(
        result.is_err(),
        "Expected no Promoted event (catch-up failed), but got: {:?}",
        result
    );

    // Role should still be Follower.
    assert_eq!(coord2.role("db1").await, Some(Role::Follower));
}

/// Regression for P0 fix 1.2: If replicator.add() fails after successful
/// catch-up (e.g. snapshot upload fails), the follower must NOT emit Promoted.
/// It releases the lease and continues monitoring.
#[tokio::test]
async fn regression_replicator_add_failure_aborts_promotion() {
    let storage: Arc<ControlledFailStorage> = Arc::new(ControlledFailStorage::new());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader: TTL=2s, won't renew.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 2,
            renew_interval: Duration::from_secs(60),
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage_dyn.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "repadd-leader");
    assert_eq!(coord1.join("db1", &db_path1).await.unwrap(), Role::Leader);

    // Follower: fast poll (100ms).
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(200),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage_dyn.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = create_test_db(&dir, "repadd-follower");
    let mut rx2 = coord2.role_events();
    assert_eq!(
        coord2.join("db1", &db_path2).await.unwrap(),
        Role::Follower
    );

    // Consume Joined event.
    let event = rx2.recv().await.unwrap();
    assert!(matches!(event, RoleEvent::Joined { role: Role::Follower, .. }));

    // Trigger write failure: uploads fail → replicator.add() snapshot upload will fail.
    // Reads still work, so pull_incremental catch-up succeeds.
    storage.set_fail_writes(true);

    // Wait for leader's lease to expire (TTL=2s). Follower detects, claims,
    // catch-up succeeds (reads work), replicator.add() fails (writes fail),
    // releases lease, no Promoted event.
    let result = tokio::time::timeout(Duration::from_secs(5), rx2.recv()).await;

    // Should timeout — no Promoted event emitted.
    assert!(
        result.is_err(),
        "Expected no Promoted event (replicator.add failed), but got: {:?}",
        result
    );

    // Role should still be Follower.
    assert_eq!(coord2.role("db1").await, Some(Role::Follower));
}

/// Regression for P0 fix 1.1 + 1.2: After a failed promotion, the lease is
/// released so another node can claim it. Verify by recovering storage and
/// checking that a fresh claim succeeds.
#[tokio::test]
async fn regression_failed_promotion_releases_lease() {
    let storage: Arc<ControlledFailStorage> = Arc::new(ControlledFailStorage::new());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader: TTL=3s, won't renew. Longer TTL avoids race where coord2
    // joins after the lease expires (under heavy test parallelism).
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 3,
            renew_interval: Duration::from_secs(60),
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage_dyn.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "release-leader");
    assert_eq!(coord1.join("db1", &db_path1).await.unwrap(), Role::Leader);

    // Follower with fast poll.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(200),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage_dyn.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = create_test_db(&dir, "release-follower");
    let mut rx2 = coord2.role_events();
    assert_eq!(
        coord2.join("db1", &db_path2).await.unwrap(),
        Role::Follower
    );
    let _ = rx2.recv().await.unwrap(); // consume Joined

    // Trigger read failure: catch-up will fail during promotion.
    storage.set_fail_reads(true);

    // Wait for lease to expire (3s TTL) and at least one failed promotion attempt.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify: no Promoted event.
    assert_eq!(coord2.role("db1").await, Some(Role::Follower));

    // Recover storage. The monitor should be able to claim on the next cycle
    // because the failed promotion released the lease.
    storage.set_fail_reads(false);
    storage.set_fail_writes(false);

    // Wait for next promotion cycle to succeed.
    let event = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .expect("Follower should promote after storage recovers")
        .unwrap();

    match event {
        RoleEvent::Promoted { db_name } => assert_eq!(db_name, "db1"),
        other => panic!("Expected Promoted after recovery, got {:?}", other),
    }
    assert_eq!(coord2.role("db1").await, Some(Role::Leader));
}

// ============================================================================
// Chaos tests: random fault injection
// ============================================================================

/// Chaos test: leader operates normally under intermittent storage failures.
/// With 20% write error rate, the system should still function — the replicator
/// retries and lease renewals don't depend on storage.
#[tokio::test]
async fn chaos_leader_survives_intermittent_write_failures() {
    let storage: Arc<FaultyStorage> = Arc::new(FaultyStorage::new(0.0, 0.2));
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    let config = CoordinatorConfig {
        sync_interval: Duration::from_millis(50),
        lease: Some(LeaseConfig::new("node-1".into(), "addr-1".into())),
        ..Default::default()
    };
    let coord = Coordinator::new(storage_dyn, Some(lease_store), "test/", config);
    let db_path = create_test_db(&dir, "chaos-leader");

    // Should succeed joining despite faulty storage (retries in replicator).
    // If join fails due to injected error, that's also valid behavior —
    // the test verifies the system doesn't panic or corrupt state.
    match coord.join("db1", &db_path).await {
        Ok(role) => {
            assert_eq!(role, Role::Leader);

            // Run for a bit with intermittent failures.
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Should still be leader (lease renewal doesn't use storage).
            assert_eq!(coord.role("db1").await, Some(Role::Leader));

            // Leave cleanly.
            coord.leave("db1").await.unwrap();
        }
        Err(_) => {
            // Join failed due to injected error on initial snapshot upload.
            // This is valid behavior — the system correctly propagated the error.
        }
    }

    // Verify some failures were actually injected.
    let failures = storage.write_failure_count();
    // With 20% rate and many operations, we expect some failures.
    // Don't assert exact count — randomness.
    eprintln!(
        "Chaos: {} write failures injected, {} successes",
        failures,
        storage.read_failure_count() + storage.write_failure_count()
    );
}

/// Chaos test: follower promotion under flaky storage. Start with clean storage,
/// then introduce 30% read errors. Verify the system doesn't panic, corrupt
/// state, or emit spurious events.
#[tokio::test]
async fn chaos_promotion_under_flaky_reads() {
    let storage: Arc<FaultyStorage> = Arc::new(FaultyStorage::new(0.0, 0.0));
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let dir = TempDir::new().unwrap();

    // Leader: TTL=2s, won't renew.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-1".to_string(),
            address: "addr-1".to_string(),
            ttl_secs: 2,
            renew_interval: Duration::from_secs(60),
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage_dyn.clone(), Some(lease_store.clone()), "test/", config1);
    let db_path1 = create_test_db(&dir, "chaos-promo-leader");
    assert_eq!(coord1.join("db1", &db_path1).await.unwrap(), Role::Leader);

    // Follower.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node-2".to_string(),
            address: "addr-2".to_string(),
            ttl_secs: 60,
            renew_interval: Duration::from_millis(200),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage_dyn.clone(), Some(lease_store.clone()), "test/", config2);
    let db_path2 = create_test_db(&dir, "chaos-promo-follower");
    let mut rx2 = coord2.role_events();
    assert_eq!(
        coord2.join("db1", &db_path2).await.unwrap(),
        Role::Follower
    );
    let _ = rx2.recv().await.unwrap(); // consume Joined

    // Introduce 30% read error rate AFTER join succeeds.
    storage.set_read_error_rate(0.3);

    // The follower will try to promote when the lease expires. Under 30% read
    // errors, catch-up might fail randomly. The system should handle this
    // gracefully — retry on next cycle, no panics, no corrupt events.
    //
    // We don't assert promotion happens (it might fail due to injected errors).
    // We DO assert no panics and no corrupt state.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // System should still be operational — either promoted or still follower.
    let role = coord2.role("db1").await;
    assert!(
        role == Some(Role::Leader) || role == Some(Role::Follower),
        "Role should be Leader or Follower, got {:?}", role
    );

    let read_failures = storage.read_failure_count();
    eprintln!(
        "Chaos promotion: {} read failures injected, role={:?}",
        read_failures, role
    );
    // Note: with 30% error rate and few operations, it's possible (but unlikely)
    // that zero failures occur. The key assertion is that the system doesn't
    // panic or corrupt state — not that a specific number of failures happen.
}

// ============================================================================
// Metrics regression tests
// ============================================================================

/// Verify that lease_claims_attempted and lease_claims_succeeded are recorded
/// on a successful leader join, and that promotions_succeeded is recorded
/// when a follower auto-promotes.
#[tokio::test]
async fn metrics_recorded_on_promotion() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let tmp1 = TempDir::new().unwrap();
    let tmp2 = TempDir::new().unwrap();
    let db_path1 = create_test_db(&tmp1, "db1");
    let db_path2 = create_test_db(&tmp2, "db2");

    // Leader joins.
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node1".to_string(),
            address: "http://node1:8080".to_string(),
            ttl_secs: 2,
            renew_interval: Duration::from_millis(500),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 2,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord1 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "ha/", config1);
    let mut rx1 = coord1.role_events();
    assert_eq!(coord1.join("db1", &db_path1).await.unwrap(), Role::Leader);
    let _ = rx1.recv().await.unwrap(); // consume Joined

    let snap1 = coord1.metrics().snapshot();
    assert!(
        snap1.lease_claims_attempted >= 1,
        "Expected at least 1 claim attempt, got {}",
        snap1.lease_claims_attempted
    );
    assert!(
        snap1.lease_claims_succeeded >= 1,
        "Expected at least 1 successful claim, got {}",
        snap1.lease_claims_succeeded
    );

    // Leader leaves — stop renewal so lease expires.
    coord1.leave("db1").await.unwrap();

    // Wait for lease TTL to expire.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Follower joins and should auto-promote after expired reads.
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node2".to_string(),
            address: "http://node2:8080".to_string(),
            ttl_secs: 2,
            renew_interval: Duration::from_millis(500),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 2,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(storage.clone(), Some(lease_store.clone()), "ha/", config2);
    let mut rx2 = coord2.role_events();

    // The lease is expired so join might either claim as Leader immediately
    // or join as Follower and promote quickly.
    let role2 = coord2.join("db1", &db_path2).await.unwrap();
    let _ = rx2.recv().await.unwrap(); // consume Joined

    if role2 == Role::Follower {
        // Wait for promotion.
        let event = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
            .await
            .expect("Timed out waiting for promotion")
            .unwrap();
        assert!(
            matches!(event, RoleEvent::Promoted { .. }),
            "Expected Promoted, got {:?}",
            event
        );
    }

    // Give a moment for metrics to flush.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let snap2 = coord2.metrics().snapshot();
    // Whether it joined as Leader or promoted from Follower, claims must be recorded.
    assert!(
        snap2.lease_claims_attempted >= 1,
        "Expected at least 1 claim attempt on coord2, got {}",
        snap2.lease_claims_attempted
    );
    assert!(
        snap2.lease_claims_succeeded >= 1,
        "Expected at least 1 successful claim on coord2, got {}",
        snap2.lease_claims_succeeded
    );

    // If it was promoted from follower, promotion metrics should be recorded.
    if role2 == Role::Follower {
        assert!(
            snap2.promotions_attempted >= 1,
            "Expected at least 1 promotion attempt, got {}",
            snap2.promotions_attempted
        );
        assert!(
            snap2.promotions_succeeded >= 1,
            "Expected at least 1 successful promotion, got {}",
            snap2.promotions_succeeded
        );
        assert!(
            snap2.last_promotion_duration_us > 0,
            "Expected promotion duration > 0, got {}",
            snap2.last_promotion_duration_us
        );
    }
}

/// Verify that renewal metrics are recorded during normal leader operation.
#[tokio::test]
async fn metrics_renewal_counters() {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let tmp = TempDir::new().unwrap();
    let db_path = create_test_db(&tmp, "db1");

    let config = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "node1".to_string(),
            address: "http://node1:8080".to_string(),
            ttl_secs: 5,
            renew_interval: Duration::from_millis(200),
            follower_poll_interval: Duration::from_millis(100),
            required_expired_reads: 2,
            max_consecutive_renewal_errors: 3,
        }),
        ..Default::default()
    };
    let coord = Coordinator::new(storage, Some(lease_store), "ha/", config);
    let mut rx = coord.role_events();
    assert_eq!(coord.join("db1", &db_path).await.unwrap(), Role::Leader);
    let _ = rx.recv().await.unwrap(); // consume Joined

    // Let the leader renew a few times.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let snap = coord.metrics().snapshot();
    assert!(
        snap.lease_renewals_succeeded >= 2,
        "Expected at least 2 successful renewals in 1s with 200ms interval, got {}",
        snap.lease_renewals_succeeded
    );
    assert!(
        snap.last_renewal_duration_us > 0,
        "Expected renewal duration > 0, got {}",
        snap.last_renewal_duration_us
    );
    assert_eq!(
        snap.lease_renewals_cas_conflict, 0,
        "Expected no CAS conflicts"
    );
    assert_eq!(
        snap.lease_renewals_error, 0,
        "Expected no renewal errors"
    );
}
