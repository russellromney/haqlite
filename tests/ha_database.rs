//! Integration tests for HaQLite — the high-level HA SQLite API.
//!
//! Uses in-memory storage backends with jittered latency to simulate
//! realistic S3 behavior.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tempfile::TempDir;
use tokio::sync::Mutex;

use haqlite::{
    Coordinator, CoordinatorConfig, HaQLite, HaQLiteClient, InMemoryLeaseStore, LeaseConfig, Role,
    SqliteFollowerBehavior, SqliteReplicator, SqlValue,
};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS test_data (
    id INTEGER PRIMARY KEY,
    value TEXT NOT NULL
);";

// ============================================================================
// InMemoryStorage for walrust::storage::StorageBackend
// ============================================================================

struct WalrustInMemoryStorage {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl WalrustInMemoryStorage {
    fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl walrust::storage::StorageBackend for WalrustInMemoryStorage {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.objects.lock().await.insert(key.to_string(), data);
        Ok(())
    }

    async fn upload_bytes_with_checksum(&self, key: &str, data: Vec<u8>, _checksum: &str) -> Result<()> {
        self.upload_bytes(key, data).await
    }

    async fn upload_file(&self, key: &str, path: &std::path::Path) -> Result<()> {
        let data = tokio::fs::read(path).await?;
        self.upload_bytes(key, data).await
    }

    async fn upload_file_with_checksum(&self, key: &str, path: &std::path::Path, _checksum: &str) -> Result<()> {
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

    async fn download_file(&self, key: &str, path: &std::path::Path) -> Result<()> {
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
// Helper to build a Coordinator with in-memory backends
// ============================================================================

fn build_coordinator(
    walrust_storage: Arc<dyn walrust::storage::StorageBackend>,
    lease_store: Arc<dyn hadb::LeaseStore>,
    instance_id: &str,
    address: &str,
) -> Arc<Coordinator> {
    let config = CoordinatorConfig {
        lease: Some(LeaseConfig::new(instance_id.to_string(), address.to_string())),
        ..Default::default()
    };

    let replication_config = walrust::sync::ReplicationConfig {
        sync_interval: config.sync_interval,
        snapshot_interval: config.snapshot_interval,
        ..Default::default()
    };

    let replicator = Arc::new(SqliteReplicator::new(walrust_storage.clone(), "test/", replication_config));
    let follower_behavior = Arc::new(SqliteFollowerBehavior::new(walrust_storage));

    Coordinator::new(
        replicator,
        Some(lease_store),
        None, // node_registry
        follower_behavior,
        "test/",
        config,
    )
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn single_node_local_mode() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("local.db");

    let db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Execute a write.
    let rows = db
        .execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(1), SqlValue::Text("hello".into())],
        )
        .await
        .unwrap();
    assert_eq!(rows, 1);

    // Query a read.
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    // Query the value.
    let value: String = db
        .query_row("SELECT value FROM test_data WHERE id = 1", &[], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(value, "hello");

    // Role should be Leader in local mode.
    assert_eq!(db.role(), Some(haqlite::Role::Leader));

    // No coordinator in local mode.
    assert!(db.coordinator().is_none());

    db.close().await.unwrap();
}

#[tokio::test]
async fn single_node_execute_and_query() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn walrust::storage::StorageBackend> = Arc::new(WalrustInMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let coordinator = build_coordinator(
        walrust_storage,
        lease_store,
        "node-1",
        "http://localhost:19001",
    );

    let db = HaQLite::from_coordinator(
        coordinator,
        db_path.to_str().unwrap(),
        SCHEMA,
        19001,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    // Should be leader (only node).
    assert_eq!(db.role(), Some(haqlite::Role::Leader));

    // Execute writes.
    for i in 1..=5 {
        db.execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[
                SqlValue::Integer(i),
                SqlValue::Text(format!("row-{}", i)),
            ],
        )
        .await
        .unwrap();
    }

    // Query reads.
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 5);

    // Coordinator should be accessible.
    assert!(db.coordinator().is_some());

    db.close().await.unwrap();
}

#[tokio::test]
async fn two_node_forwarded_write() {
    let tmp = TempDir::new().unwrap();
    // Same filename so both derive db_name = "ha" (same HA group).
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let walrust_storage: Arc<dyn walrust::storage::StorageBackend> = Arc::new(WalrustInMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Start leader.
    let leader_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "leader-node",
        "http://localhost:19010",
    );
    let leader = HaQLite::from_coordinator(
        leader_coordinator,
        leader_path.to_str().unwrap(),
        SCHEMA,
        19010,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(leader.role(), Some(haqlite::Role::Leader));

    // Give leader a moment to start forwarding server.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start follower.
    let follower_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "follower-node",
        "http://localhost:19011",
    );
    let follower = HaQLite::from_coordinator(
        follower_coordinator,
        follower_path.to_str().unwrap(),
        SCHEMA,
        19011,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(follower.role(), Some(haqlite::Role::Follower));

    // Write through leader directly.
    leader
        .execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(1), SqlValue::Text("direct".into())],
        )
        .await
        .unwrap();

    // Write through follower — should forward to leader.
    follower
        .execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(2), SqlValue::Text("forwarded".into())],
        )
        .await
        .unwrap();

    // Leader should see both rows.
    let count: i64 = leader
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);

    leader.close().await.unwrap();
    follower.close().await.unwrap();
}

#[tokio::test]
async fn forwarding_error_no_leader() {
    let tmp = TempDir::new().unwrap();
    let follower_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn walrust::storage::StorageBackend> = Arc::new(WalrustInMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Write a fake lease directly — points to a port where nothing is listening.
    // This avoids starting a real leader whose forwarding server might linger.
    let fake_lease = serde_json::json!({
        "instance_id": "ghost-leader",
        "address": "http://127.0.0.1:1",
        "claimed_at": chrono::Utc::now().timestamp() as u64,
        "ttl_secs": 300,
        "session_id": "fake-session",
        "sleeping": false,
    });
    lease_store
        .write_if_not_exists(
            "test/ha/_lease.json",
            serde_json::to_vec(&fake_lease).unwrap(),
        )
        .await
        .unwrap();

    // Start follower — it sees the active lease and becomes Follower.
    let follower_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "orphan-node",
        "http://localhost:19020",
    );
    let follower = HaQLite::from_coordinator(
        follower_coordinator,
        follower_path.to_str().unwrap(),
        SCHEMA,
        19020,
        Duration::from_secs(1), // short timeout
    )
    .await
    .unwrap();

    assert_eq!(follower.role(), Some(haqlite::Role::Follower));

    // Follower should get an error when trying to forward, not hang.
    let result = follower
        .execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(1), SqlValue::Text("test".into())],
        )
        .await;

    assert!(result.is_err());

    follower.close().await.unwrap();
}

#[tokio::test]
async fn close_is_clean() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("clean.db");

    let db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    db.execute(
        "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("test".into())],
    )
    .await
    .unwrap();

    // Close should succeed without errors or hangs.
    db.close().await.unwrap();

    // DB file should still exist.
    assert!(db_path.exists());

    // Can reopen.
    let db2 = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
    let count: i64 = db2
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    db2.close().await.unwrap();
}

#[tokio::test]
async fn auth_rejects_wrong_secret() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let walrust_storage: Arc<dyn walrust::storage::StorageBackend> = Arc::new(WalrustInMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Leader with secret "correct-token".
    let leader_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "auth-leader",
        "http://localhost:19030",
    );
    let leader = HaQLite::from_coordinator_with_secret(
        leader_coordinator,
        leader_path.to_str().unwrap(),
        SCHEMA,
        19030,
        Duration::from_secs(5),
        Some("correct-token".to_string()),
    )
    .await
    .unwrap();
    assert_eq!(leader.role(), Some(Role::Leader));

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Follower with WRONG secret — should fail to forward writes.
    let follower_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "auth-follower",
        "http://localhost:19031",
    );
    let follower = HaQLite::from_coordinator_with_secret(
        follower_coordinator,
        follower_path.to_str().unwrap(),
        SCHEMA,
        19031,
        Duration::from_secs(1),
        Some("wrong-token".to_string()),
    )
    .await
    .unwrap();
    assert_eq!(follower.role(), Some(Role::Follower));

    // Forwarded write should be rejected by the leader.
    let result = follower
        .execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(1), SqlValue::Text("should-fail".into())],
        )
        .await;
    assert!(result.is_err(), "Expected auth rejection, got Ok");

    // Leader should have no rows (write was rejected).
    let count: i64 = leader
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);

    leader.close().await.unwrap();
    follower.close().await.unwrap();
}

#[tokio::test]
async fn auth_accepts_correct_secret() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let walrust_storage: Arc<dyn walrust::storage::StorageBackend> = Arc::new(WalrustInMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Both nodes with same secret.
    let leader_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "auth-ok-leader",
        "http://localhost:19040",
    );
    let leader = HaQLite::from_coordinator_with_secret(
        leader_coordinator,
        leader_path.to_str().unwrap(),
        SCHEMA,
        19040,
        Duration::from_secs(5),
        Some("shared-secret".to_string()),
    )
    .await
    .unwrap();
    assert_eq!(leader.role(), Some(Role::Leader));

    tokio::time::sleep(Duration::from_millis(100)).await;

    let follower_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "auth-ok-follower",
        "http://localhost:19041",
    );
    let follower = HaQLite::from_coordinator_with_secret(
        follower_coordinator,
        follower_path.to_str().unwrap(),
        SCHEMA,
        19041,
        Duration::from_secs(5),
        Some("shared-secret".to_string()),
    )
    .await
    .unwrap();
    assert_eq!(follower.role(), Some(Role::Follower));

    // Forwarded write should succeed.
    follower
        .execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(1), SqlValue::Text("authed".into())],
        )
        .await
        .unwrap();

    // Leader should have the row.
    let count: i64 = leader
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    leader.close().await.unwrap();
    follower.close().await.unwrap();
}

// ============================================================================
// Read Replica Tests
// ============================================================================

#[tokio::test]
async fn read_replica_local_query() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("replica.db");

    // Create and populate a DB with raw rusqlite.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            rusqlite::params![1, "hello"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            rusqlite::params![2, "world"],
        )
        .unwrap();
    }

    // Read-replica-only client — no leader, no S3.
    let client = HaQLiteClient::read_replica_only(db_path.to_str().unwrap());
    assert!(client.is_read_replica());

    // Query should read from local DB.
    let row = client
        .query_row("SELECT COUNT(*) FROM test_data", &[])
        .await
        .unwrap();
    assert_eq!(row[0].as_integer().unwrap(), 2);

    // Query with params.
    let row = client
        .query_row(
            "SELECT value FROM test_data WHERE id = ?1",
            &[SqlValue::Integer(1)],
        )
        .await
        .unwrap();
    assert_eq!(row[0].as_text().unwrap(), "hello");
}

#[tokio::test]
async fn read_replica_execute_returns_error() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("replica.db");

    // Create empty DB.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
    }

    let client = HaQLiteClient::read_replica_only(db_path.to_str().unwrap());

    // execute() should fail — no leader to forward to.
    let result = client
        .execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(1), SqlValue::Text("nope".into())],
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No leader address"),
        "Expected 'No leader address' error, got: {}",
        err
    );
}

#[tokio::test]
async fn read_replica_sees_external_writes() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("replica.db");

    // Create DB with initial data.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            rusqlite::params![1, "original"],
        )
        .unwrap();
    }

    let client = HaQLiteClient::read_replica_only(db_path.to_str().unwrap());

    // First read — sees 1 row.
    let row = client
        .query_row("SELECT COUNT(*) FROM test_data", &[])
        .await
        .unwrap();
    assert_eq!(row[0].as_integer().unwrap(), 1);

    // External write (simulates walrust applying a WAL delta).
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            rusqlite::params![2, "external"],
        )
        .unwrap();
    }

    // Second read — fresh connection should see the new row.
    let row = client
        .query_row("SELECT COUNT(*) FROM test_data", &[])
        .await
        .unwrap();
    assert_eq!(row[0].as_integer().unwrap(), 2);

    // Verify the new row content.
    let row = client
        .query_row(
            "SELECT value FROM test_data WHERE id = ?1",
            &[SqlValue::Integer(2)],
        )
        .await
        .unwrap();
    assert_eq!(row[0].as_text().unwrap(), "external");
}
