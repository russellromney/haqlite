//! Integration tests for HaQLite — the high-level HA SQLite API.
//!
//! Uses in-memory storage backends with jittered latency to simulate
//! realistic S3 behavior.

mod common;

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use common::InMemoryStorage;
use haqlite::{
    Coordinator, CoordinatorConfig, HaQLite, HaQLiteClient, HaQLiteError, InMemoryLeaseStore,
    LeaseConfig, Role, SqliteFollowerBehavior, SqliteReplicator, SqlValue,
};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS test_data (
    id INTEGER PRIMARY KEY,
    value TEXT NOT NULL
);";

// ============================================================================
// Helper to build a Coordinator with in-memory backends
// ============================================================================

fn build_coordinator(
    walrust_storage: Arc<dyn hadb_storage::StorageBackend>,
    lease_store: Arc<dyn hadb::LeaseStore>,
    instance_id: &str,
    address: &str,
) -> Arc<Coordinator> {
    let config = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            lease_store,
            instance_id.to_string(),
            address.to_string(),
        )),
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
        None, // manifest_store
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

    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Execute a write.
    let rows = db
        .execute_async(
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

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let coordinator = build_coordinator(
        walrust_storage,
        lease_store,
        "node-1",
        "http://localhost:19001",
    );

    let mut db = HaQLite::from_coordinator(
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
        db.execute_async(
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

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Start leader.
    let leader_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "leader-node",
        "http://localhost:19010",
    );
    let mut leader = HaQLite::from_coordinator(
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
    let mut follower = HaQLite::from_coordinator(
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
        .execute_async(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(1), SqlValue::Text("direct".into())],
        )
        .await
        .unwrap();

    // Write through follower — should forward to leader.
    follower
        .execute_async(
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

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
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
            "ha",
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
    let mut follower = HaQLite::from_coordinator(
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
        .execute_async(
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

    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    db.execute_async(
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
    let mut db2 = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
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

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Leader with secret "correct-token".
    let leader_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "auth-leader",
        "http://localhost:19030",
    );
    let mut leader = HaQLite::from_coordinator_with_secret(
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
    let mut follower = HaQLite::from_coordinator_with_secret(
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
        .execute_async(
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

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Both nodes with same secret.
    let leader_coordinator = build_coordinator(
        walrust_storage.clone(),
        lease_store.clone(),
        "auth-ok-leader",
        "http://localhost:19040",
    );
    let mut leader = HaQLite::from_coordinator_with_secret(
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
    let mut follower = HaQLite::from_coordinator_with_secret(
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
        .execute_async(
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

// ============================================================================
// Phase Rampart: Structured Error Tests
// ============================================================================

#[tokio::test]
async fn error_execute_on_follower_with_dead_leader_returns_leader_unavailable() {
    let tmp = TempDir::new().unwrap();
    let follower_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Fake lease pointing to a dead port.
    let fake_lease = serde_json::json!({
        "instance_id": "ghost",
        "address": "http://127.0.0.1:1",
        "claimed_at": chrono::Utc::now().timestamp() as u64,
        "ttl_secs": 300,
        "session_id": "fake",
        "sleeping": false,
    });
    lease_store
        .write_if_not_exists("ha", serde_json::to_vec(&fake_lease).unwrap())
        .await
        .unwrap();

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "orphan", "http://localhost:19050",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, follower_path.to_str().unwrap(), SCHEMA, 19050, Duration::from_millis(500),
    )
    .await
    .unwrap();
    assert_eq!(db.role(), Some(Role::Follower));

    let result = db
        .execute_async("INSERT INTO test_data (id, value) VALUES (1, 'x')", &[])
        .await;

    // Must be a leader connection/server error, not a generic error.
    match result {
        Err(HaQLiteError::LeaderConnectionError(_)) => {}
        Err(HaQLiteError::LeaderServerError { .. }) => {}
        other => panic!("Expected LeaderConnectionError or LeaderServerError, got {:?}", other),
    }

    db.close().await.unwrap();
}

#[tokio::test]
async fn error_query_row_bad_sql_returns_database_error() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("err.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    let result: std::result::Result<i64, HaQLiteError> =
        db.query_row("SELECT * FROM nonexistent_table", &[], |r| r.get(0));

    match result {
        Err(HaQLiteError::DatabaseError(msg)) => {
            assert!(msg.contains("no such table"), "got: {msg}");
        }
        other => panic!("Expected DatabaseError, got {:?}", other),
    }

    db.close().await.unwrap();
}

#[tokio::test]
async fn error_execute_bad_sql_returns_database_error() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("err.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    let result = db
        .execute_async("INSERT INTO nonexistent (x) VALUES (1)", &[])
        .await;

    match result {
        Err(HaQLiteError::DatabaseError(msg)) => {
            assert!(msg.contains("no such table"), "got: {msg}");
        }
        other => panic!("Expected DatabaseError, got {:?}", other),
    }

    db.close().await.unwrap();
}

#[tokio::test]
async fn error_query_values_bad_sql_returns_database_error() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("err.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    let result = db.query_values("INVALID SQL GARBAGE", &[]);

    match result {
        Err(HaQLiteError::DatabaseError(_)) => {}
        other => panic!("Expected DatabaseError, got {:?}", other),
    }

    db.close().await.unwrap();
}

// ============================================================================
// Phase Rampart: Forwarding Retry Tests
// ============================================================================

#[tokio::test]
async fn retry_forwarding_does_not_retry_4xx() {
    // Auth rejection (401) is a 4xx -- should NOT retry.
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let mut leader = HaQLite::from_coordinator_with_secret(
        build_coordinator(walrust_storage.clone(), lease_store.clone(), "l", "http://localhost:19060"),
        leader_dir.join("ha.db").to_str().unwrap(),
        SCHEMA, 19060, Duration::from_secs(5), Some("secret".into()),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut follower = HaQLite::from_coordinator_with_secret(
        build_coordinator(walrust_storage.clone(), lease_store.clone(), "f", "http://localhost:19061"),
        follower_dir.join("ha.db").to_str().unwrap(),
        SCHEMA, 19061, Duration::from_secs(1), Some("wrong-secret".into()),
    )
    .await
    .unwrap();

    let start = std::time::Instant::now();
    let result = follower
        .execute_async("INSERT INTO test_data (id, value) VALUES (1, 'x')", &[])
        .await;
    let elapsed = start.elapsed();

    // Should fail with LeaderClientError (4xx from auth rejection).
    assert!(result.is_err());
    match result {
        Err(HaQLiteError::LeaderClientError { status, .. }) => {
            assert_eq!(status, 401, "expected 401, got {status}");
        }
        other => panic!("Expected LeaderClientError with 401, got {:?}", other),
    }

    // Should NOT have retried (4xx) -- well under 100ms backoff.
    assert!(elapsed < Duration::from_millis(200), "Took {:?}, suggests retry happened", elapsed);

    leader.close().await.unwrap();
    follower.close().await.unwrap();
}

#[tokio::test]
async fn retry_forwarding_succeeds_on_first_attempt() {
    // Happy path: forwarding works on first try.
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let mut leader = HaQLite::from_coordinator(
        build_coordinator(walrust_storage.clone(), lease_store.clone(), "l", "http://localhost:19070"),
        leader_dir.join("ha.db").to_str().unwrap(),
        SCHEMA, 19070, Duration::from_secs(5),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut follower = HaQLite::from_coordinator(
        build_coordinator(walrust_storage.clone(), lease_store.clone(), "f", "http://localhost:19071"),
        follower_dir.join("ha.db").to_str().unwrap(),
        SCHEMA, 19071, Duration::from_secs(5),
    )
    .await
    .unwrap();

    let start = std::time::Instant::now();
    follower
        .execute_async(
            "INSERT INTO test_data (id, value) VALUES (1, 'fast')",
            &[],
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();

    // Should succeed quickly (no retries needed).
    assert!(elapsed < Duration::from_millis(500), "Took {:?}, too slow for single attempt", elapsed);

    let count: i64 = leader.query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0)).unwrap();
    assert_eq!(count, 1);

    leader.close().await.unwrap();
    follower.close().await.unwrap();
}

#[tokio::test]
async fn retry_forwarding_retries_on_connection_error() {
    // Follower points to dead leader -- connection error should be retried,
    // then eventually fail with LeaderUnavailable after all retries exhausted.
    let tmp = TempDir::new().unwrap();
    let follower_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let fake_lease = serde_json::json!({
        "instance_id": "ghost",
        "address": "http://127.0.0.1:1",
        "claimed_at": chrono::Utc::now().timestamp() as u64,
        "ttl_secs": 300,
        "session_id": "fake",
        "sleeping": false,
    });
    lease_store
        .write_if_not_exists("ha", serde_json::to_vec(&fake_lease).unwrap())
        .await
        .unwrap();

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "retry-node", "http://localhost:19080",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, follower_path.to_str().unwrap(), SCHEMA, 19080, Duration::from_millis(200),
    )
    .await
    .unwrap();

    let start = std::time::Instant::now();
    let result = db
        .execute_async("INSERT INTO test_data (id, value) VALUES (1, 'x')", &[])
        .await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    match result {
        Err(HaQLiteError::LeaderConnectionError(msg)) => {
            assert!(msg.contains("forwarding attempts failed"), "got: {msg}");
        }
        other => panic!("Expected LeaderConnectionError, got {:?}", other),
    }

    // Should have retried: 0ms + 100ms + 400ms + 1600ms = ~2100ms minimum.
    assert!(elapsed >= Duration::from_millis(1500), "Took {:?}, suggests no retries", elapsed);

    db.close().await.unwrap();
}

// ============================================================================
// Phase Rampart: Read Semaphore Tests
// ============================================================================

#[tokio::test]
async fn semaphore_does_not_block_leader_reads() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("sem.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Leader reads should work regardless of semaphore state.
    db.execute_async("INSERT INTO test_data (id, value) VALUES (1, 'a')", &[])
        .await
        .unwrap();

    // Read many times (leader doesn't use semaphore).
    for _ in 0..100 {
        let count: i64 = db.query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    db.close().await.unwrap();
}

#[tokio::test]
async fn semaphore_query_values_also_bounded() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("sem.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    db.execute_async("INSERT INTO test_data (id, value) VALUES (1, 'a')", &[])
        .await
        .unwrap();

    let result = db.query_values("SELECT * FROM test_data", &[]);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);

    db.close().await.unwrap();
}

// ============================================================================
// Phase Rampart: Graceful Shutdown Tests
// ============================================================================

#[tokio::test]
async fn close_then_reopen_preserves_data() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("persist.db");

    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
    for i in 1..=10 {
        db.execute_async(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("row-{i}"))],
        )
        .await
        .unwrap();
    }
    db.close().await.unwrap();

    // Reopen and verify all data persisted.
    let mut db2 = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
    let count: i64 = db2.query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0)).unwrap();
    assert_eq!(count, 10);
    db2.close().await.unwrap();
}

#[tokio::test]
async fn close_ha_node_is_clean() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "close-node", "http://localhost:19090",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, db_path.to_str().unwrap(), SCHEMA, 19090, Duration::from_secs(5),
    )
    .await
    .unwrap();

    db.execute_async("INSERT INTO test_data (id, value) VALUES (1, 'a')", &[])
        .await
        .unwrap();

    // Close should succeed, not hang.
    db.close().await.unwrap();

    // DB file should still exist.
    assert!(db_path.exists());
}

// ============================================================================
// Phase Rampart: Edge Cases
// ============================================================================

#[tokio::test]
async fn execute_empty_params() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("edge.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Execute with no params.
    db.execute_async("INSERT INTO test_data (id, value) VALUES (1, 'bare')", &[])
        .await
        .unwrap();

    let val: String = db.query_row("SELECT value FROM test_data WHERE id = 1", &[], |r| r.get(0)).unwrap();
    assert_eq!(val, "bare");

    db.close().await.unwrap();
}

#[tokio::test]
async fn query_values_empty_result() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("edge.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    let rows = db.query_values("SELECT * FROM test_data WHERE id = 999", &[]).unwrap();
    assert!(rows.is_empty());

    db.close().await.unwrap();
}

#[tokio::test]
async fn error_not_leader_when_no_address() {
    let tmp = TempDir::new().unwrap();
    let follower_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Fake lease with empty address.
    let fake_lease = serde_json::json!({
        "instance_id": "no-addr",
        "address": "",
        "claimed_at": chrono::Utc::now().timestamp() as u64,
        "ttl_secs": 300,
        "session_id": "fake",
        "sleeping": false,
    });
    lease_store
        .write_if_not_exists("ha", serde_json::to_vec(&fake_lease).unwrap())
        .await
        .unwrap();

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "no-addr-node", "http://localhost:19100",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, follower_path.to_str().unwrap(), SCHEMA, 19100, Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_eq!(db.role(), Some(Role::Follower));

    let result = db
        .execute_async("INSERT INTO test_data (id, value) VALUES (1, 'x')", &[])
        .await;

    // Should be NotLeader (empty address), not LeaderConnectionError (connection failed).
    match result {
        Err(HaQLiteError::NotLeader) => {}
        other => panic!("Expected NotLeader, got {:?}", other),
    }

    db.close().await.unwrap();
}

#[tokio::test]
async fn handoff_on_local_mode_returns_false() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("local.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    let result = db.handoff().await.unwrap();
    assert!(!result); // local mode has no coordinator

    db.close().await.unwrap();
}

// ============================================================================
// Phase Rampart-e: Follower Readiness Tests
// ============================================================================

#[tokio::test]
async fn leader_is_always_caught_up() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("leader.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    assert!(db.is_caught_up());
    assert_eq!(db.replay_position(), 0);

    db.close().await.unwrap();
}

#[tokio::test]
async fn ha_leader_is_caught_up() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "leader", "http://localhost:19110",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, db_path.to_str().unwrap(), SCHEMA, 19110, Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(db.role(), Some(Role::Leader));

    assert!(db.is_caught_up());

    db.close().await.unwrap();
}

#[tokio::test]
async fn prometheus_contains_readiness_gauges() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "prom-node", "http://localhost:19120",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, db_path.to_str().unwrap(), SCHEMA, 19120, Duration::from_secs(5),
    )
    .await
    .unwrap();

    let prom = db.prometheus_metrics().expect("should have metrics in HA mode");
    assert!(prom.contains("haqlite_follower_caught_up"), "missing caught_up gauge");
    assert!(prom.contains("haqlite_follower_replay_position"), "missing replay_position gauge");
    // hadb-level gauges should also be present
    assert!(prom.contains("hadb_follower_caught_up"), "missing hadb caught_up gauge");

    db.close().await.unwrap();
}

#[tokio::test]
async fn local_mode_no_prometheus_metrics() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("local.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    assert!(db.prometheus_metrics().is_none());

    db.close().await.unwrap();
}

// ============================================================================
// Regression Tests
// ============================================================================

#[tokio::test]
async fn regression_hadb_prometheus_has_nonzero_caught_up_for_leader() {
    // Regression: hadb HaMetrics follower_caught_up gauge was always 0
    // because nobody wrote to it. Now the coordinator sets it on promotion.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "prom-regression", "http://localhost:19130",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, db_path.to_str().unwrap(), SCHEMA, 19130, Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(db.role(), Some(Role::Leader));

    let prom = db.prometheus_metrics().unwrap();
    // The hadb-level gauge should be 1 for a leader (not 0).
    assert!(
        prom.contains("hadb_follower_caught_up 1"),
        "hadb gauge should be 1 for leader, got:\n{}",
        prom
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn regression_close_completes_fully() {
    // Regression: close() aborted tasks but didn't await handles,
    // leaving dangling Arc refs. Now it awaits both handles.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "close-regression", "http://localhost:19140",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, db_path.to_str().unwrap(), SCHEMA, 19140, Duration::from_secs(5),
    )
    .await
    .unwrap();

    db.execute_async("INSERT INTO test_data (id, value) VALUES (1, 'test')", &[])
        .await
        .unwrap();

    // close() should complete without hanging (tasks are awaited, not just aborted).
    let close_result = tokio::time::timeout(
        Duration::from_secs(5),
        db.close(),
    )
    .await;
    assert!(close_result.is_ok(), "close() timed out -- tasks not properly awaited");
    assert!(close_result.unwrap().is_ok());
}

#[tokio::test]
async fn error_query_values_on_nonexistent_table() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("err.db");
    let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    let result = db.query_values("SELECT * FROM nonexistent_table", &[]);
    match result {
        Err(HaQLiteError::DatabaseError(msg)) => {
            assert!(msg.contains("no such table"), "got: {msg}");
        }
        other => panic!("Expected DatabaseError, got {:?}", other),
    }

    db.close().await.unwrap();
}

#[tokio::test]
async fn handoff_ha_leader_succeeds() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "handoff-leader", "http://localhost:19150",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, db_path.to_str().unwrap(), SCHEMA, 19150, Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(db.role(), Some(Role::Leader));

    // Write some data first.
    db.execute_async("INSERT INTO test_data (id, value) VALUES (1, 'before-handoff')", &[])
        .await
        .unwrap();

    // Handoff should succeed (this node is the leader).
    let result = db.handoff().await.unwrap();
    assert!(result, "handoff should return true for leader");

    db.close().await.unwrap();
}

#[tokio::test]
async fn handoff_preserves_data() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha.db");

    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let coordinator = build_coordinator(
        walrust_storage, lease_store, "handoff-data", "http://localhost:19160",
    );
    let mut db = HaQLite::from_coordinator(
        coordinator, db_path.to_str().unwrap(), SCHEMA, 19160, Duration::from_secs(5),
    )
    .await
    .unwrap();

    // Write data, handoff, verify data still readable locally.
    for i in 1..=5 {
        db.execute_async(
            "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("row-{i}"))],
        )
        .await
        .unwrap();
    }

    db.handoff().await.unwrap();

    // Data should still be readable (local reads work regardless of role).
    let count: i64 = db.query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0)).unwrap();
    assert_eq!(count, 5);

    db.close().await.unwrap();
}
