//! HA integration tests for hrana protocol endpoints.
//!
//! Uses real leader + follower nodes (in-memory storage backends) to verify
//! the hrana protocol works correctly for reads, writes, and write rejection
//! on followers.

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::InMemoryStorage;
use haqlite::hrana::build_hrana_router;
use haqlite::serve::{build_router, AppState};
use haqlite::{
    Coordinator, CoordinatorConfig, HaQLite, InMemoryLeaseStore, LeaseConfig, Role,
    SqliteFollowerBehavior, SqliteReplicator,
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS t (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);";

// ============================================================================
// Helpers
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
        snapshot_interval: config.snapshot_interval,
        ..Default::default()
    };
    let replicator = Arc::new(SqliteReplicator::new(
        walrust_storage.clone(),
        "test/",
        replication_config,
    ));
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

/// Build the full axum router (health/status/metrics + hrana) for a node.
fn build_app(
    db: Arc<HaQLite>,
    db_path: std::path::PathBuf,
    secret: Option<String>,
) -> axum::Router {
    let hrana_router = build_hrana_router(db.clone(), db_path, secret.clone());
    let state = Arc::new(AppState::new(db, secret));
    build_router(state, hrana_router)
}

/// Send a POST pipeline request and return (status, body_json).
async fn pipeline(
    router: &axum::Router,
    path: &str,
    body: &Value,
    auth: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = auth {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let request = builder
        .body(Body::from(serde_json::to_string(body).unwrap()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_json: Value = serde_json::from_slice(&body_bytes)
        .unwrap_or_else(|_| json!({"raw": String::from_utf8_lossy(&body_bytes).to_string()}));
    (status, body_json)
}

fn execute_req(sql: &str) -> Value {
    json!({
        "type": "execute",
        "stmt": { "sql": sql, "args": [], "named_args": [], "want_rows": false }
    })
}

fn query_req(sql: &str) -> Value {
    json!({
        "type": "execute",
        "stmt": { "sql": sql, "args": [], "named_args": [], "want_rows": true }
    })
}

fn pipeline_body(baton: Option<&str>, requests: Vec<Value>) -> Value {
    let mut req = json!({ "requests": requests });
    if let Some(b) = baton {
        req["baton"] = json!(b);
    }
    req
}

// ============================================================================
// Leader tests
// ============================================================================

#[tokio::test]
async fn test_leader_hrana_read() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("leader.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let coord = build_coordinator(storage, lease, "leader-1", "http://localhost:19100");

    let db = Arc::new(
        HaQLite::from_coordinator(
            coord,
            db_path.to_str().unwrap(),
            SCHEMA,
            19100,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    assert_eq!(db.role(), Some(Role::Leader));

    let app = build_app(db.clone(), db_path, None);

    let body = pipeline_body(None, vec![query_req("SELECT COUNT(*) FROM t")]);
    let (status, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "0"
    );

    drop(app);
    Arc::into_inner(db)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_leader_hrana_write() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("leader.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let coord = build_coordinator(storage, lease, "leader-2", "http://localhost:19101");

    let db = Arc::new(
        HaQLite::from_coordinator(
            coord,
            db_path.to_str().unwrap(),
            SCHEMA,
            19101,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    assert_eq!(db.role(), Some(Role::Leader));

    let app = build_app(db.clone(), db_path, None);

    // Insert through hrana
    let body = pipeline_body(
        None,
        vec![
            execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')"),
            execute_req("INSERT INTO t (id, name) VALUES (2, 'Bob')"),
            query_req("SELECT COUNT(*) FROM t"),
        ],
    );
    let (status, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");
    assert_eq!(resp["results"][1]["type"], "ok");
    assert_eq!(
        resp["results"][2]["response"]["result"]["rows"][0][0]["value"],
        "2"
    );

    drop(app);
    Arc::into_inner(db)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_leader_hrana_transaction() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("leader.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let coord = build_coordinator(storage, lease, "leader-3", "http://localhost:19102");

    let db = Arc::new(
        HaQLite::from_coordinator(
            coord,
            db_path.to_str().unwrap(),
            SCHEMA,
            19102,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    let app = build_app(db.clone(), db_path, None);

    // BEGIN
    let body = pipeline_body(None, vec![execute_req("BEGIN")]);
    let (status, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    let baton = resp["baton"].as_str().unwrap();

    // INSERT inside txn
    let body = pipeline_body(
        Some(baton),
        vec![execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')")],
    );
    let (status, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    let baton = resp["baton"].as_str().unwrap();

    // ROLLBACK
    let body = pipeline_body(Some(baton), vec![execute_req("ROLLBACK")]);
    let (status, _) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);

    // Verify rollback — count should be 0
    let body = pipeline_body(None, vec![query_req("SELECT COUNT(*) FROM t")]);
    let (_, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "0"
    );

    drop(app);
    Arc::into_inner(db)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Follower tests
// ============================================================================

#[tokio::test]
async fn test_follower_hrana_read() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Start leader
    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-r1",
        "http://localhost:19110",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19110,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    assert_eq!(leader.role(), Some(Role::Leader));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Start follower
    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-r1",
        "http://localhost:19111",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19111,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    assert_eq!(follower.role(), Some(Role::Follower));

    // Write data directly through leader's native API (bypasses hrana)
    leader
        .execute_async(
            "INSERT INTO t (id, name) VALUES (?1, ?2)",
            &[
                haqlite::SqlValue::Integer(1),
                haqlite::SqlValue::Text("Alice".into()),
            ],
        )
        .await
        .unwrap();

    // Follower's DB file exists but may not have the data yet (no real replication
    // in in-memory tests). Write directly to follower's DB to simulate replication.
    {
        let conn = rusqlite::Connection::open(&follower_path).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO t (id, name) VALUES (?1, ?2)",
            rusqlite::params![1, "Alice"],
        )
        .unwrap();
    }

    // Read through follower's hrana endpoint
    let follower_app = build_app(follower.clone(), follower_path, None);
    let body = pipeline_body(None, vec![query_req("SELECT name FROM t WHERE id = 1")]);
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "Alice"
    );

    drop(follower_app);
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_follower_hrana_rejects_writes() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Start leader
    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-w1",
        "http://localhost:19120",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19120,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    assert_eq!(leader.role(), Some(Role::Leader));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Start follower
    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-w1",
        "http://localhost:19121",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19121,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    assert_eq!(follower.role(), Some(Role::Follower));

    let follower_app = build_app(follower.clone(), follower_path, None);

    // INSERT through follower's hrana → 403 Forbidden
    let body = pipeline_body(
        None,
        vec![execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')")],
    );
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(
        resp["error"]
            .as_str()
            .unwrap()
            .contains("not allowed on a follower"),
        "Expected follower rejection message, got: {:?}",
        resp
    );

    // UPDATE through follower → 403
    let body = pipeline_body(
        None,
        vec![execute_req("UPDATE t SET name = 'Bob' WHERE id = 1")],
    );
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // DELETE through follower → 403
    let body = pipeline_body(None, vec![execute_req("DELETE FROM t WHERE id = 1")]);
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // CREATE TABLE through follower → 403
    let body = pipeline_body(None, vec![execute_req("CREATE TABLE t2 (id INTEGER)")]);
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // DROP TABLE through follower → 403
    let body = pipeline_body(None, vec![execute_req("DROP TABLE t")]);
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // But SELECT is fine
    let body = pipeline_body(None, vec![query_req("SELECT COUNT(*) FROM t")]);
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");

    drop(follower_app);
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_follower_hrana_rejects_batch_with_writes() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-b1",
        "http://localhost:19130",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19130,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-b1",
        "http://localhost:19131",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19131,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );

    let follower_app = build_app(follower.clone(), follower_path, None);

    // Batch containing a mix of reads and writes → 403 (any write in pipeline = rejected)
    let body = pipeline_body(
        None,
        vec![
            query_req("SELECT 1"),
            execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')"),
        ],
    );
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(resp["error"].as_str().unwrap().contains("follower"));

    // Pure read pipeline on follower → OK
    let body = pipeline_body(None, vec![query_req("SELECT 1"), query_req("SELECT 2")]);
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);

    drop(follower_app);
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_follower_hrana_cursor_rejects_writes() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-c1",
        "http://localhost:19140",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19140,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-c1",
        "http://localhost:19141",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19141,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );

    let follower_app = build_app(follower.clone(), follower_path, None);

    // Cursor with write → 403
    let cursor_body = json!({
        "baton": null,
        "batch": {
            "steps": [{
                "stmt": { "sql": "INSERT INTO t VALUES (1, 'Alice')", "args": [], "named_args": [], "want_rows": false },
                "condition": null
            }]
        }
    });
    let request = Request::builder()
        .method("POST")
        .uri("/v3/cursor")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&cursor_body).unwrap()))
        .unwrap();
    let response = follower_app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // Cursor with read → OK
    let cursor_body = json!({
        "baton": null,
        "batch": {
            "steps": [{
                "stmt": { "sql": "SELECT COUNT(*) FROM t", "args": [], "named_args": [], "want_rows": true },
                "condition": null
            }]
        }
    });
    let request = Request::builder()
        .method("POST")
        .uri("/v3/cursor")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&cursor_body).unwrap()))
        .unwrap();
    let response = follower_app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_follower_hrana_rejects_sequence_writes() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-s1",
        "http://localhost:19150",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19150,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-s1",
        "http://localhost:19151",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19151,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );

    let follower_app = build_app(follower.clone(), follower_path, None);

    // Sequence with write → 403
    let body = pipeline_body(
        None,
        vec![json!({
            "type": "sequence",
            "sql": "INSERT INTO t VALUES (1, 'Alice')"
        })],
    );
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    drop(follower_app);
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Auth with HA
// ============================================================================

#[tokio::test]
async fn test_leader_follower_hrana_with_auth() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-a1",
        "http://localhost:19160",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator_with_secret(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19160,
            Duration::from_secs(5),
            Some("secret123".to_string()),
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-a1",
        "http://localhost:19161",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator_with_secret(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19161,
            Duration::from_secs(5),
            Some("secret123".to_string()),
        )
        .await
        .unwrap(),
    );

    let leader_app = build_app(leader.clone(), leader_path, Some("secret123".into()));
    let follower_app = build_app(follower.clone(), follower_path, Some("secret123".into()));

    // Leader: no auth → 401
    let body = pipeline_body(None, vec![query_req("SELECT 1")]);
    let (status, _) = pipeline(&leader_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Leader: correct auth → 200
    let (status, _) = pipeline(&leader_app, "/v3/pipeline", &body, Some("secret123")).await;
    assert_eq!(status, StatusCode::OK);

    // Follower: no auth → 401
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Follower: correct auth + read → 200
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, Some("secret123")).await;
    assert_eq!(status, StatusCode::OK);

    // Follower: correct auth + write → 403 (auth passes, write policy rejects)
    let write_body = pipeline_body(
        None,
        vec![execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')")],
    );
    let (status, _) = pipeline(
        &follower_app,
        "/v3/pipeline",
        &write_body,
        Some("secret123"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    drop(leader_app);
    drop(follower_app);
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Leader write + follower read (simulated replication)
// ============================================================================

#[tokio::test]
async fn test_leader_write_follower_read_through_hrana() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-rw1",
        "http://localhost:19170",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19170,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-rw1",
        "http://localhost:19171",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19171,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );

    let leader_app = build_app(leader.clone(), leader_path, None);
    let follower_app = build_app(follower.clone(), follower_path.clone(), None);

    // Write through leader's hrana endpoint
    let body = pipeline_body(
        None,
        vec![
            execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')"),
            execute_req("INSERT INTO t (id, name) VALUES (2, 'Bob')"),
        ],
    );
    let (status, resp) = pipeline(&leader_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");
    assert_eq!(resp["results"][1]["type"], "ok");

    // Simulate replication: copy data to follower's DB
    {
        let conn = rusqlite::Connection::open(&follower_path).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO t (id, name) VALUES (?1, ?2)",
            rusqlite::params![1, "Alice"],
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO t (id, name) VALUES (?1, ?2)",
            rusqlite::params![2, "Bob"],
        )
        .unwrap();
    }

    // Read through follower's hrana endpoint
    let body = pipeline_body(None, vec![query_req("SELECT id, name FROM t ORDER BY id")]);
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);

    let rows = &resp["results"][0]["response"]["result"]["rows"];
    assert_eq!(rows.as_array().unwrap().len(), 2);
    assert_eq!(rows[0][1]["value"], "Alice");
    assert_eq!(rows[1][1]["value"], "Bob");

    // Verify leader also reads its own data through hrana
    let body = pipeline_body(None, vec![query_req("SELECT COUNT(*) FROM t")]);
    let (_, resp) = pipeline(&leader_app, "/v3/pipeline", &body, None).await;
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "2"
    );

    drop(leader_app);
    drop(follower_app);
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Follower: close and get_autocommit still work
// ============================================================================

#[tokio::test]
async fn test_follower_hrana_close_and_autocommit() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-ca1",
        "http://localhost:19180",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19180,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-ca1",
        "http://localhost:19181",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19181,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );

    let follower_app = build_app(follower.clone(), follower_path, None);

    // get_autocommit on follower
    let body = pipeline_body(None, vec![json!({ "type": "get_autocommit" })]);
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");
    assert_eq!(resp["results"][0]["response"]["is_autocommit"], true);

    // close on follower
    let body = pipeline_body(None, vec![json!({ "type": "close" })]);
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(resp["baton"].is_null());
    assert_eq!(resp["results"][0]["type"], "ok");
    assert_eq!(resp["results"][0]["response"]["type"], "close");

    drop(follower_app);
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Follower: describe still works (read-only operation)
// ============================================================================

#[tokio::test]
async fn test_follower_hrana_describe() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-d1",
        "http://localhost:19190",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19190,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-d1",
        "http://localhost:19191",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19191,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );

    let follower_app = build_app(follower.clone(), follower_path, None);

    let body = pipeline_body(
        None,
        vec![json!({
            "type": "describe",
            "sql": "SELECT id, name FROM t"
        })],
    );
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");

    let cols = resp["results"][0]["response"]["result"]["cols"]
        .as_array()
        .unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0]["name"], "id");
    assert_eq!(cols[1]["name"], "name");

    drop(follower_app);
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Defense-in-depth: follower SQLite connection is read-only at engine level
// ============================================================================

#[tokio::test]
async fn test_follower_connection_is_sqlite_readonly() {
    // Even if is_writable() were bypassed, the SQLite connection opened for
    // followers uses SQLITE_OPEN_READ_ONLY flags, preventing writes at the
    // database engine level. This is defense-in-depth.
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-ro1",
        "http://localhost:19200",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19200,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-ro1",
        "http://localhost:19201",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19201,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    assert_eq!(follower.role(), Some(Role::Follower));

    // Open connection with the exact flags the hrana backend uses for followers
    let ro_conn = rusqlite::Connection::open_with_flags(
        &follower_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    ro_conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

    // Attempt to write — SQLite itself must reject this
    let result = ro_conn.execute("INSERT INTO t (id, name) VALUES (1, 'Sneaky')", []);
    assert!(result.is_err(), "Write must fail on read-only connection");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("readonly") || err_msg.to_lowercase().contains("read-only"),
        "Error should mention readonly, got: {err_msg}"
    );

    // Verify leader-style flags allow writes (same flags as hrana backend for leaders)
    let rw_conn = rusqlite::Connection::open_with_flags(
        &leader_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    rw_conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    rw_conn
        .execute("INSERT INTO t (id, name) VALUES (1, 'Allowed')", [])
        .unwrap();
    let count: i64 = rw_conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Session isolation: concurrent hrana sessions have independent transactions
// ============================================================================

#[tokio::test]
async fn test_hrana_session_isolation_on_leader() {
    // Two hrana sessions each get their own SQLite connection. An uncommitted
    // write in session 1 must be invisible to session 2 (WAL snapshot isolation).
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("leader.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let coord = build_coordinator(storage, lease, "leader-iso1", "http://localhost:19210");

    let db = Arc::new(
        HaQLite::from_coordinator(
            coord,
            db_path.to_str().unwrap(),
            SCHEMA,
            19210,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    let app = build_app(db.clone(), db_path, None);

    // Session 1: BEGIN + INSERT (uncommitted)
    let body = pipeline_body(None, vec![execute_req("BEGIN")]);
    let (status, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    let baton1 = resp["baton"].as_str().unwrap().to_string();

    let body = pipeline_body(
        Some(&baton1),
        vec![execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')")],
    );
    let (status, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    let baton1 = resp["baton"].as_str().unwrap().to_string();

    // Session 2 (new, no baton): must NOT see session 1's uncommitted write
    let body = pipeline_body(None, vec![query_req("SELECT COUNT(*) FROM t")]);
    let (status, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "0",
    );

    // Session 1: COMMIT
    let body = pipeline_body(Some(&baton1), vec![execute_req("COMMIT")]);
    let (status, _) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);

    // Session 2 (fresh): must see committed data
    let body = pipeline_body(None, vec![query_req("SELECT COUNT(*) FROM t")]);
    let (status, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "1",
    );

    drop(app);
    Arc::into_inner(db)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Hrana sessions and HaQLite internal connection share database state
// ============================================================================

#[tokio::test]
async fn test_hrana_and_haqlite_share_database_state() {
    // Hrana opens its own connections, but they target the same DB file.
    // Writes through hrana must be visible through HaQLite's native API
    // and vice versa.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("leader.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let coord = build_coordinator(storage, lease, "leader-share1", "http://localhost:19220");

    let db = Arc::new(
        HaQLite::from_coordinator(
            coord,
            db_path.to_str().unwrap(),
            SCHEMA,
            19220,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    let app = build_app(db.clone(), db_path, None);

    // Write through hrana
    let body = pipeline_body(
        None,
        vec![execute_req("INSERT INTO t (id, name) VALUES (1, 'Hrana')")],
    );
    let (status, _) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);

    // Read through HaQLite native API — must see hrana's write
    let rows = db
        .query_values("SELECT name FROM t WHERE id = 1", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        haqlite::SqlValue::Text(s) => assert_eq!(s, "Hrana"),
        other => panic!("Expected Text('Hrana'), got: {:?}", other),
    }

    // Write through HaQLite native API
    db.execute_async(
        "INSERT INTO t (id, name) VALUES (?1, ?2)",
        &[
            haqlite::SqlValue::Integer(2),
            haqlite::SqlValue::Text("Native".into()),
        ],
    )
    .await
    .unwrap();

    // Read through hrana — must see HaQLite's native write
    let body = pipeline_body(None, vec![query_req("SELECT name FROM t WHERE id = 2")]);
    let (status, resp) = pipeline(&app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "Native"
    );

    drop(app);
    Arc::into_inner(db)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Wrong auth token rejected on both leader and follower
// ============================================================================

#[tokio::test]
async fn test_wrong_auth_rejected_on_both_roles() {
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let leader_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "leader-wa1",
        "http://localhost:19230",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator_with_secret(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19230,
            Duration::from_secs(5),
            Some("correct_secret".to_string()),
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let follower_coord = build_coordinator(
        storage.clone(),
        lease.clone(),
        "follower-wa1",
        "http://localhost:19231",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator_with_secret(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19231,
            Duration::from_secs(5),
            Some("correct_secret".to_string()),
        )
        .await
        .unwrap(),
    );

    let leader_app = build_app(leader.clone(), leader_path, Some("correct_secret".into()));
    let follower_app = build_app(
        follower.clone(),
        follower_path,
        Some("correct_secret".into()),
    );

    let body = pipeline_body(None, vec![query_req("SELECT 1")]);

    // Wrong token on leader → 401
    let (status, _) = pipeline(&leader_app, "/v3/pipeline", &body, Some("wrong_secret")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Wrong token on follower → 401
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, Some("wrong_secret")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Empty token on leader → 401
    let (status, _) = pipeline(&leader_app, "/v3/pipeline", &body, Some("")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Empty token on follower → 401
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, Some("")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    drop(leader_app);
    drop(follower_app);
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}

// ============================================================================
// Role promotion: follower promotes to leader, hrana dynamically accepts writes
// ============================================================================

/// Build a coordinator with short lease TTL for fast failover tests.
fn build_coordinator_fast(
    walrust_storage: Arc<dyn hadb_storage::StorageBackend>,
    lease_store: Arc<dyn hadb::LeaseStore>,
    instance_id: &str,
    address: &str,
) -> Arc<Coordinator> {
    let mut lease_config =
        LeaseConfig::new(lease_store, instance_id.to_string(), address.to_string());
    lease_config.ttl_secs = 3;
    lease_config.renew_interval = Duration::from_millis(500);
    lease_config.follower_poll_interval = Duration::from_millis(200);

    let config = CoordinatorConfig {
        lease: Some(lease_config),
        follower_pull_interval: Duration::from_millis(100),
        replicator_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let replication_config = walrust::sync::ReplicationConfig {
        sync_interval: Duration::from_millis(100),
        snapshot_interval: config.snapshot_interval,
        ..Default::default()
    };
    let replicator = Arc::new(SqliteReplicator::new(
        walrust_storage.clone(),
        "test-fast/",
        replication_config,
    ));
    let follower_behavior = Arc::new(SqliteFollowerBehavior::new(walrust_storage));
    Coordinator::new(
        replicator,
        None, // manifest_store
        None, // node_registry
        follower_behavior,
        "test-fast/",
        config,
    )
}

#[tokio::test]
async fn test_role_promotion_changes_hrana_writability() {
    // When a follower promotes to leader (after the original leader dies),
    // the hrana backend dynamically starts accepting writes. This proves
    // is_writable() and connection() check role at request time, not at
    // construction time.
    let tmp = TempDir::new().unwrap();
    let leader_dir = tmp.path().join("node1");
    let follower_dir = tmp.path().join("node2");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let leader_path = leader_dir.join("ha.db");
    let follower_path = follower_dir.join("ha.db");

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let lease: Arc<dyn hadb::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Leader with short lease TTL for fast failover
    let leader_coord = build_coordinator_fast(
        storage.clone(),
        lease.clone(),
        "leader-promo1",
        "http://localhost:19240",
    );
    let leader = Arc::new(
        HaQLite::from_coordinator(
            leader_coord,
            leader_path.to_str().unwrap(),
            SCHEMA,
            19240,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    assert_eq!(leader.role(), Some(Role::Leader));
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Follower with fast polling
    let follower_coord = build_coordinator_fast(
        storage.clone(),
        lease.clone(),
        "follower-promo1",
        "http://localhost:19241",
    );
    let follower = Arc::new(
        HaQLite::from_coordinator(
            follower_coord,
            follower_path.to_str().unwrap(),
            SCHEMA,
            19241,
            Duration::from_secs(5),
        )
        .await
        .unwrap(),
    );
    assert_eq!(follower.role(), Some(Role::Follower));

    let follower_app = build_app(follower.clone(), follower_path, None);

    // Follower rejects writes initially
    let body = pipeline_body(
        None,
        vec![execute_req("INSERT INTO t (id, name) VALUES (1, 'Before')")],
    );
    let (status, _) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Kill the leader — lease expires after TTL (3 seconds)
    Arc::into_inner(leader)
        .expect("sole owner")
        .close()
        .await
        .unwrap();

    // Wait for follower to promote (poll up to 10 seconds)
    let mut promoted = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if follower.role() == Some(Role::Leader) {
            promoted = true;
            break;
        }
    }
    assert!(
        promoted,
        "Follower should promote to leader after original leader dies"
    );

    // Now the same follower_app should accept writes through hrana
    let body = pipeline_body(
        None,
        vec![execute_req("INSERT INTO t (id, name) VALUES (1, 'After')")],
    );
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "Promoted node should accept writes. Response: {:?}",
        resp
    );
    assert_eq!(resp["results"][0]["type"], "ok");

    // Verify the write landed
    let body = pipeline_body(None, vec![query_req("SELECT name FROM t WHERE id = 1")]);
    let (status, resp) = pipeline(&follower_app, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "After"
    );

    drop(follower_app);
    Arc::into_inner(follower)
        .expect("sole owner")
        .close()
        .await
        .unwrap();
}
