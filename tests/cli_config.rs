//! Tests for haqlite CLI config loading.

use hadb_cli::config::{load_config, SharedConfig};
use haqlite::cli_config::{HaqliteConfig, ServeConfig};

#[test]
fn test_full_haqlite_config() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("haqlite.toml");
    std::fs::write(
        &config_path,
        r#"
[s3]
bucket = "my-bucket"
endpoint = "https://fly.storage.tigris.dev"

[lease]
ttl_secs = 10
renew_interval_ms = 3000
poll_interval_ms = 5000

[serve]
db_path = "/data/my.db"
schema = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);"
port = 9090
forwarding_port = 19090
prefix = "myapp/"
secret = "s3cr3t"
sync_interval_ms = 500
follower_pull_ms = 2000
"#,
    )
    .unwrap();

    let (shared, product): (SharedConfig, HaqliteConfig) =
        load_config(Some(config_path.as_path()), "haqlite.toml").unwrap();

    // Shared config
    assert_eq!(shared.s3.bucket, "my-bucket");
    assert_eq!(
        shared.s3.endpoint.as_deref(),
        Some("https://fly.storage.tigris.dev")
    );
    assert_eq!(shared.lease.ttl_secs, 10);
    assert_eq!(shared.lease.renew_interval_ms, 3000);
    assert_eq!(shared.lease.poll_interval_ms, 5000);

    // Product config
    let serve = product.serve.unwrap();
    assert_eq!(serve.db_path.to_str().unwrap(), "/data/my.db");
    assert_eq!(
        serve.schema.as_deref(),
        Some("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
    );
    assert_eq!(serve.port, 9090);
    assert_eq!(serve.forwarding_port, 19090);
    assert_eq!(serve.prefix, "myapp/");
    assert_eq!(serve.secret.as_deref(), Some("s3cr3t"));
    assert_eq!(serve.sync_interval_ms, 500);
    assert_eq!(serve.follower_pull_ms, 2000);
}

#[test]
fn test_minimal_config_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("haqlite.toml");
    std::fs::write(
        &config_path,
        r#"
[s3]
bucket = "test-bucket"

[serve]
db_path = "/tmp/test.db"
"#,
    )
    .unwrap();

    let (shared, product): (SharedConfig, HaqliteConfig) =
        load_config(Some(config_path.as_path()), "haqlite.toml").unwrap();

    // Shared defaults
    assert_eq!(shared.lease.ttl_secs, 5);
    assert_eq!(shared.lease.renew_interval_ms, 2000);
    assert_eq!(shared.lease.poll_interval_ms, 3000);

    // Product defaults
    let serve = product.serve.unwrap();
    assert_eq!(serve.port, 8080);
    assert_eq!(serve.forwarding_port, 18080);
    assert_eq!(serve.prefix, "haqlite/");
    assert!(serve.secret.is_none());
    assert!(serve.schema.is_none());
    assert_eq!(serve.sync_interval_ms, 1000);
    assert_eq!(serve.follower_pull_ms, 1000);
}

#[test]
fn test_no_config_file_returns_defaults() {
    let (shared, product): (SharedConfig, HaqliteConfig) =
        load_config(None, "haqlite.toml").unwrap();

    assert_eq!(shared.s3.bucket, "");
    assert!(product.serve.is_none());
}

#[test]
fn test_config_without_serve_section() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("haqlite.toml");
    std::fs::write(
        &config_path,
        r#"
[s3]
bucket = "my-bucket"
"#,
    )
    .unwrap();

    let (_shared, product): (SharedConfig, HaqliteConfig) =
        load_config(Some(config_path.as_path()), "haqlite.toml").unwrap();

    assert!(product.serve.is_none());
}

#[tokio::test]
async fn test_query_values_method() {
    use haqlite::SqlValue;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let schema = "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT);";
    let db = haqlite::HaQLite::local(db_path.to_str().unwrap(), schema).expect("local should open");

    db.execute_async(
        "INSERT INTO test (id, name) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Text("Alice".to_string())],
    )
    .await
    .expect("insert should work");

    db.execute_async(
        "INSERT INTO test (id, name) VALUES (?1, ?2)",
        &[SqlValue::Integer(2), SqlValue::Text("Bob".to_string())],
    )
    .await
    .expect("insert should work");

    // Single-row result: COUNT(*) returns one row with one column
    let rows = db
        .query_values("SELECT COUNT(*) FROM test", &[])
        .expect("query_values should work");
    assert_eq!(rows.len(), 1, "COUNT should return 1 row");
    assert_eq!(rows[0].len(), 1, "COUNT row should have 1 column");
    match &rows[0][0] {
        SqlValue::Integer(n) => assert_eq!(*n, 2),
        other => panic!("expected Integer(2), got {other:?}"),
    }

    // Single-row result with params
    let rows = db
        .query_values(
            "SELECT name FROM test WHERE id = ?1",
            &[SqlValue::Integer(1)],
        )
        .expect("query_values with params should work");
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        SqlValue::Text(s) => assert_eq!(s, "Alice"),
        other => panic!("expected Text(Alice), got {other:?}"),
    }
}

#[tokio::test]
async fn test_serve_rejects_empty_bucket() {
    let shared = SharedConfig::default(); // bucket is ""
    let serve = ServeConfig {
        db_path: std::path::PathBuf::from("/tmp/test.db"),
        schema: None,
        port: 19999,
        forwarding_port: 29999,
        prefix: "haqlite/".to_string(),
        secret: None,
        sync_interval_ms: 1000,
        follower_pull_ms: 1000,
        mode: "dedicated".to_string(),
    };
    let err = haqlite::serve::run(&shared, &serve).await.unwrap_err();
    assert!(
        err.to_string().contains("bucket is required"),
        "expected bucket validation error, got: {err}"
    );
}

#[tokio::test]
async fn test_serve_auth_rejects_unauthenticated() {
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    let dir = tempfile::tempdir().unwrap();
    let app = test_app(
        &dir.path().join("auth_test.db"),
        Some("my-secret".to_string()),
    );

    let pipeline_body = r#"{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","args":[],"named_args":[],"want_rows":true}}]}"#;

    // POST /v3/pipeline without auth → 401
    let req = Request::builder()
        .method("POST")
        .uri("/v3/pipeline")
        .header("content-type", "application/json")
        .body(Body::from(pipeline_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 401);

    // POST /v3/pipeline with wrong token → 401
    let req = Request::builder()
        .method("POST")
        .uri("/v3/pipeline")
        .header("content-type", "application/json")
        .header("authorization", "Bearer wrong-token")
        .body(Body::from(pipeline_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 401);

    // POST /v3/pipeline with correct token → 200
    let req = Request::builder()
        .method("POST")
        .uri("/v3/pipeline")
        .header("content-type", "application/json")
        .header("authorization", "Bearer my-secret")
        .body(Body::from(pipeline_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);

    // GET /health is always unauthenticated → 200
    let req = Request::builder()
        .method("GET")
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_serve_no_auth_when_no_secret() {
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    let dir = tempfile::tempdir().unwrap();
    let app = test_app(&dir.path().join("noauth_test.db"), None);

    // POST /v3/pipeline without any auth → 200 (no secret configured)
    let req = Request::builder()
        .method("POST")
        .uri("/v3/pipeline")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","args":[],"named_args":[],"want_rows":true}}]}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);
}

// ============================================================================
// query_values edge cases
// ============================================================================

#[tokio::test]
async fn test_query_values_multi_row() {
    use haqlite::SqlValue;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("multi.db");

    let schema = "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT);";
    let db = haqlite::HaQLite::local(db_path.to_str().unwrap(), schema).expect("local should open");

    for i in 1..=5 {
        db.execute_async(
            "INSERT INTO test (id, name) VALUES (?1, ?2)",
            &[SqlValue::Integer(i), SqlValue::Text(format!("user_{i}"))],
        )
        .await
        .unwrap();
    }

    // Multi-row: SELECT all rows
    let rows = db
        .query_values("SELECT id, name FROM test ORDER BY id", &[])
        .expect("multi-row query should work");
    assert_eq!(rows.len(), 5, "should return 5 rows");
    assert_eq!(rows[0].len(), 2, "each row should have 2 columns");
    match &rows[0][0] {
        SqlValue::Integer(n) => assert_eq!(*n, 1),
        other => panic!("expected Integer(1), got {other:?}"),
    }
    match &rows[4][1] {
        SqlValue::Text(s) => assert_eq!(s, "user_5"),
        other => panic!("expected Text(user_5), got {other:?}"),
    }
}

#[tokio::test]
async fn test_query_values_zero_rows() {
    use haqlite::SqlValue;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("empty.db");

    let schema = "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT);";
    let db = haqlite::HaQLite::local(db_path.to_str().unwrap(), schema).expect("local should open");

    // Zero rows: SELECT from empty table with WHERE clause
    let rows = db
        .query_values(
            "SELECT * FROM test WHERE id = ?1",
            &[SqlValue::Integer(999)],
        )
        .expect("zero-row query should return empty vec, not error");
    assert!(rows.is_empty(), "should return 0 rows");
}

#[tokio::test]
async fn test_query_values_null_values() {
    use haqlite::SqlValue;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("null.db");

    let schema = "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT);";
    let db = haqlite::HaQLite::local(db_path.to_str().unwrap(), schema).expect("local should open");

    // Insert row with NULL name
    db.execute_async(
        "INSERT INTO test (id, name) VALUES (?1, ?2)",
        &[SqlValue::Integer(1), SqlValue::Null],
    )
    .await
    .unwrap();

    let rows = db
        .query_values("SELECT id, name FROM test WHERE id = 1", &[])
        .expect("null query should work");
    assert_eq!(rows.len(), 1);
    assert!(matches!(&rows[0][1], SqlValue::Null));
}

#[tokio::test]
async fn test_query_values_all_types() {
    use haqlite::SqlValue;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("types.db");

    let schema = "CREATE TABLE IF NOT EXISTS types (i INTEGER, r REAL, t TEXT, b BLOB);";
    let db = haqlite::HaQLite::local(db_path.to_str().unwrap(), schema).expect("local should open");

    db.execute_async(
        "INSERT INTO types (i, r, t, b) VALUES (?1, ?2, ?3, ?4)",
        &[
            SqlValue::Integer(42),
            SqlValue::Real(3.15),
            SqlValue::Text("hello".to_string()),
            SqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        ],
    )
    .await
    .unwrap();

    let rows = db
        .query_values("SELECT i, r, t, b FROM types", &[])
        .expect("all-types query should work");
    assert_eq!(rows.len(), 1);
    assert!(matches!(&rows[0][0], SqlValue::Integer(42)));
    match &rows[0][1] {
        SqlValue::Real(f) => assert!((f - 3.15).abs() < 0.001),
        other => panic!("expected Real(3.14), got {other:?}"),
    }
    assert!(matches!(&rows[0][2], SqlValue::Text(s) if s == "hello"));
    assert!(matches!(&rows[0][3], SqlValue::Blob(b) if b == &[0xDE, 0xAD, 0xBE, 0xEF]));
}

#[tokio::test]
async fn test_query_values_invalid_sql() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("badsql.db");

    let schema = "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY);";
    let db = haqlite::HaQLite::local(db_path.to_str().unwrap(), schema).expect("local should open");

    let result = db.query_values("NOT VALID SQL AT ALL", &[]);
    assert!(result.is_err(), "invalid SQL should return error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("query prepare failed"),
        "expected prepare error, got: {err}"
    );
}

// ============================================================================
// HTTP endpoint tests: /status, /metrics, /query, malformed JSON
// ============================================================================

/// Helper: create a test app with schema and optional secret
fn test_app(db_path: &std::path::Path, secret: Option<String>) -> axum::Router {
    let schema = "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT);";
    let db = haqlite::HaQLite::local(db_path.to_str().unwrap(), schema).expect("local should open");
    let db = std::sync::Arc::new(db);
    let hrana_router =
        haqlite::hrana::build_hrana_router(db.clone(), db_path.to_path_buf(), secret.clone());
    let state = std::sync::Arc::new(haqlite::serve::AppState::new(db, secret));
    haqlite::serve::build_router(state, hrana_router)
}

#[tokio::test]
async fn test_get_status_endpoint() {
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    let dir = tempfile::tempdir().unwrap();
    let app = test_app(&dir.path().join("status.db"), None);

    let req = Request::builder()
        .method("GET")
        .uri("/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
    // Local mode has no coordinator, role defaults to Follower
    assert!(json["role"].is_string());
}

#[tokio::test]
async fn test_get_metrics_endpoint() {
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    let dir = tempfile::tempdir().unwrap();
    let app = test_app(&dir.path().join("metrics.db"), None);

    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Local mode has no coordinator → empty metrics
    assert!(json.is_object());
}

#[tokio::test]
async fn test_status_requires_auth_when_secret_set() {
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    let dir = tempfile::tempdir().unwrap();
    let app = test_app(
        &dir.path().join("auth_status.db"),
        Some("my-secret".to_string()),
    );

    // GET /status without auth → 401
    let req = Request::builder()
        .method("GET")
        .uri("/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 401);

    // GET /status with auth → 200
    let req = Request::builder()
        .method("GET")
        .uri("/status")
        .header("authorization", "Bearer my-secret")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);

    // GET /metrics without auth → 401
    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_unknown_endpoint_returns_404() {
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    let dir = tempfile::tempdir().unwrap();
    let app = test_app(&dir.path().join("404.db"), None);

    let req = Request::builder()
        .method("GET")
        .uri("/nonexistent")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 404);
}
