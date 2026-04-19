//! Integration test: verify that lease loss triggers write fencing.
//!
//! Two haqlite nodes share a lease store. Node A is leader. We delete the
//! lease from the store so Node B can claim it. Node A should detect the
//! lease loss, demote itself, and fence its connection (writes blocked,
//! reads still work).

mod common;

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use common::InMemoryStorage;
use haqlite::{
    Coordinator, CoordinatorConfig, HaQLite, InMemoryLeaseStore, LeaseConfig, LeaseStore, Role,
    SqliteFollowerBehavior, SqliteReplicator, SqlValue,
};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT)";

fn build_coordinator(
    storage: Arc<dyn hadb_storage::StorageBackend>,
    lease_store: Arc<dyn hadb::LeaseStore>,
    instance_id: &str,
    address: &str,
    ttl_secs: u64,
    renew_interval: Duration,
    follower_poll: Duration,
) -> Arc<Coordinator> {
    let config = CoordinatorConfig {
        lease: Some(LeaseConfig {
            ttl_secs,
            renew_interval,
            follower_poll_interval: follower_poll,
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 1, // fence fast on renewal failure
            ..LeaseConfig::new(instance_id.to_string(), address.to_string())
        }),
        sync_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let repl_config = walrust::sync::ReplicationConfig {
        sync_interval: config.sync_interval,
        ..Default::default()
    };

    let replicator = Arc::new(SqliteReplicator::new(storage.clone(), "test/", repl_config));
    let follower_behavior = Arc::new(SqliteFollowerBehavior::new(storage));

    Coordinator::new(
        replicator,
        Some(lease_store),
        None,
        None,
        follower_behavior,
        "test/",
        config,
    )
}

#[tokio::test]
async fn leader_fenced_on_lease_loss() {
    let tmp = TempDir::new().expect("tmpdir");
    let leader_dir = tmp.path().join("leader");
    let follower_dir = tmp.path().join("follower");
    std::fs::create_dir_all(&leader_dir).expect("mkdir");
    std::fs::create_dir_all(&follower_dir).expect("mkdir");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease_store = Arc::new(InMemoryLeaseStore::new());

    // Short TTL and fast renewal so lease loss is detected quickly
    let ttl = 2;
    let renew = Duration::from_millis(500);
    let poll = Duration::from_millis(300);

    // Start leader (Node A)
    let leader_coord = build_coordinator(
        storage.clone(),
        lease_store.clone(),
        "node-A",
        "http://localhost:19020",
        ttl,
        renew,
        poll,
    );
    let mut leader = HaQLite::from_coordinator(
        leader_coord,
        leader_dir.join("db.db").to_str().expect("path"),
        SCHEMA,
        19020,
        Duration::from_secs(5),
    )
    .await
    .expect("leader open");
    assert_eq!(leader.role(), Some(Role::Leader));

    // Insert some data while we're leader
    leader
        .execute("INSERT INTO t (val) VALUES (?1)", &[SqlValue::Text("before".into())])
        .expect("insert as leader");

    // Verify connection() works for writes
    {
        let conn = leader.connection().expect("conn");
        let guard = conn.lock();
        guard
            .execute("INSERT INTO t (val) VALUES (?1)", rusqlite::params!["also-before"])
            .expect("direct insert should work as leader");
    }

    // Delete the lease so the coordinator thinks it expired
    // This simulates network partition or another node stealing the lease
    lease_store.delete("test/db/_lease.json").await.expect("delete lease");

    // Wait for the leader to detect lease loss.
    // With renew_interval=500ms and max_consecutive_renewal_errors=1,
    // it should detect within ~1s.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Leader should now be demoted
    assert_eq!(leader.role(), Some(Role::Follower), "leader should be demoted after lease loss");

    // Writes through connection() should be blocked
    {
        let conn = leader.connection().expect("conn should still be available");
        let guard = conn.lock();
        let result = guard.execute("INSERT INTO t (val) VALUES (?1)", rusqlite::params!["after-fence"]);
        assert!(result.is_err(), "writes should be blocked after lease loss");
    }

    // Reads through connection() should still work
    {
        let conn = leader.connection().expect("conn");
        let guard = conn.lock();
        let count: i64 = guard
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .expect("reads should work after lease loss");
        assert_eq!(count, 2); // "before" + "also-before"
    }

    leader.close().await.ok();
}
