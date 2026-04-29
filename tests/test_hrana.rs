//! Integration tests for haqlite's hrana protocol endpoints.
//!
//! Uses HaQLite::local() for a single-node leader, builds the full axum
//! router, and drives requests through tower::ServiceExt.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use haqlite::database::HaQLite;
use haqlite::hrana::build_hrana_router;
use haqlite::serve::{build_router, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;

// ============================================================================
// Helpers
// ============================================================================

/// Create a local HaQLite instance with a temp database and return the router.
fn setup() -> (axum::Router, Arc<HaQLite>, TempDir) {
    setup_with_secret(None)
}

fn setup_with_secret(secret: Option<String>) -> (axum::Router, Arc<HaQLite>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();

    let db = Arc::new(
        HaQLite::local(
            db_path_str,
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, name TEXT);",
        )
        .unwrap(),
    );

    let hrana_router = build_hrana_router(db.clone(), db_path, secret.clone());
    let state = Arc::new(AppState::new(db.clone(), secret));
    let router = build_router(state, hrana_router);

    (router, db, tmp)
}

/// Build a pipeline request JSON with the given statements.
fn pipeline_request(baton: Option<&str>, requests: Vec<Value>) -> Value {
    let mut req = json!({ "requests": requests });
    if let Some(b) = baton {
        req["baton"] = json!(b);
    }
    req
}

/// Build an execute stream request.
fn execute_req(sql: &str) -> Value {
    json!({
        "type": "execute",
        "stmt": {
            "sql": sql,
            "args": [],
            "named_args": [],
            "want_rows": false
        }
    })
}

/// Build an execute stream request that wants rows.
fn query_req(sql: &str) -> Value {
    json!({
        "type": "execute",
        "stmt": {
            "sql": sql,
            "args": [],
            "named_args": [],
            "want_rows": true
        }
    })
}

/// Build a close stream request.
fn close_req() -> Value {
    json!({ "type": "close" })
}

/// Send a POST request to the router and return (status, body_json).
async fn post_json(
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

/// Send a GET request.
async fn get(router: &axum::Router, path: &str) -> StatusCode {
    let request = Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap();

    router.clone().oneshot(request).await.unwrap().status()
}

// ============================================================================
// Health / Status / Metrics (still work after hrana integration)
// ============================================================================

#[tokio::test]
async fn test_health_endpoint() {
    let (router, _, _tmp) = setup();
    assert_eq!(get(&router, "/health").await, StatusCode::OK);
}

#[tokio::test]
async fn test_status_endpoint_no_auth() {
    let (router, _, _tmp) = setup();
    let request = Request::builder()
        .method("GET")
        .uri("/status")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_status_endpoint_with_auth() {
    let (router, _, _tmp) = setup_with_secret(Some("my-secret".into()));
    let request = Request::builder()
        .method("GET")
        .uri("/status")
        .header("authorization", "Bearer my-secret")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_status_endpoint_wrong_auth() {
    let (router, _, _tmp) = setup_with_secret(Some("my-secret".into()));
    let request = Request::builder()
        .method("GET")
        .uri("/status")
        .header("authorization", "Bearer wrong")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// ============================================================================
// Pipeline — basic operations
// ============================================================================

#[tokio::test]
async fn test_pipeline_v2_select() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(None, vec![query_req("SELECT 1 as val")]);
    let (status, resp) = post_json(&router, "/v2/pipeline", &body, None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(resp["baton"].is_string());
    assert_eq!(resp["results"][0]["type"], "ok");
    let result = &resp["results"][0]["response"]["result"];
    assert_eq!(result["rows"][0][0]["type"], "integer");
    assert_eq!(result["rows"][0][0]["value"], "1");
}

#[tokio::test]
async fn test_pipeline_v3_select() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(None, vec![query_req("SELECT 42 as val")]);
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "42"
    );
}

#[tokio::test]
async fn test_pipeline_insert_and_query() {
    let (router, _, _tmp) = setup();

    // Insert
    let body = pipeline_request(
        None,
        vec![execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')")],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");

    // Query
    let body = pipeline_request(None, vec![query_req("SELECT name FROM t WHERE id = 1")]);
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    let rows = &resp["results"][0]["response"]["result"]["rows"];
    assert_eq!(rows[0][0]["type"], "text");
    assert_eq!(rows[0][0]["value"], "Alice");
}

#[tokio::test]
async fn test_pipeline_multiple_requests() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(
        None,
        vec![
            execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')"),
            execute_req("INSERT INTO t (id, name) VALUES (2, 'Bob')"),
            query_req("SELECT COUNT(*) FROM t"),
        ],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"].as_array().unwrap().len(), 3);
    // Count should be 2
    assert_eq!(
        resp["results"][2]["response"]["result"]["rows"][0][0]["value"],
        "2"
    );
}

// ============================================================================
// Pipeline — baton session continuity
// ============================================================================

#[tokio::test]
async fn test_pipeline_baton_transaction() {
    let (router, _, _tmp) = setup();

    // Start a transaction
    let body = pipeline_request(
        None,
        vec![json!({
            "type": "execute",
            "stmt": { "sql": "BEGIN", "args": [], "named_args": [], "want_rows": false }
        })],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    let baton = resp["baton"].as_str().unwrap();

    // Insert within the transaction
    let body = pipeline_request(
        Some(baton),
        vec![execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')")],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    let baton = resp["baton"].as_str().unwrap();

    // Commit
    let body = pipeline_request(
        Some(baton),
        vec![
            json!({
                "type": "execute",
                "stmt": { "sql": "COMMIT", "args": [], "named_args": [], "want_rows": false }
            }),
            close_req(),
        ],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(resp["baton"].is_null());

    // Verify data persisted via new session
    let body = pipeline_request(None, vec![query_req("SELECT name FROM t WHERE id = 1")]);
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "Alice"
    );
}

#[tokio::test]
async fn test_pipeline_expired_baton_rejected() {
    let (router, _, _tmp) = setup();

    // Use a fake baton
    let body = pipeline_request(
        Some("nonexistent_baton_that_does_not_exist_at_all_1234567890ab"),
        vec![query_req("SELECT 1")],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(resp["error"].as_str().unwrap().contains("expired"));
}

#[tokio::test]
async fn test_pipeline_old_baton_invalid_after_use() {
    let (router, _, _tmp) = setup();

    // Get a baton
    let body = pipeline_request(None, vec![query_req("SELECT 1")]);
    let (_, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    let baton1 = resp["baton"].as_str().unwrap().to_string();

    // Use it — get a new baton
    let body = pipeline_request(Some(&baton1), vec![query_req("SELECT 2")]);
    let (status, _) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);

    // Old baton should be rejected
    let body = pipeline_request(Some(&baton1), vec![query_req("SELECT 3")]);
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(resp["error"].as_str().unwrap().contains("expired"));
}

// ============================================================================
// Pipeline — auth
// ============================================================================

#[tokio::test]
async fn test_pipeline_no_auth_when_no_secret() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(None, vec![query_req("SELECT 1")]);
    let (status, _) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn test_pipeline_auth_required_when_secret_set() {
    let (router, _, _tmp) = setup_with_secret(Some("test-secret".into()));
    let body = pipeline_request(None, vec![query_req("SELECT 1")]);

    // No auth → 401
    let (status, _) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Wrong auth → 401
    let (status, _) = post_json(&router, "/v3/pipeline", &body, Some("wrong")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Correct auth → 200
    let (status, _) = post_json(&router, "/v3/pipeline", &body, Some("test-secret")).await;
    assert_eq!(status, StatusCode::OK);
}

// ============================================================================
// Pipeline — error handling
// ============================================================================

#[tokio::test]
async fn test_pipeline_sql_error_returned_in_result() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(None, vec![query_req("SELECT * FROM nonexistent_table")]);
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;

    // Pipeline succeeds, but the individual result is an error
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "error");
    assert!(resp["results"][0]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("nonexistent_table"));
}

#[tokio::test]
async fn test_pipeline_invalid_sql_error() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(None, vec![execute_req("THIS IS NOT SQL")]);
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "error");
}

// ============================================================================
// Pipeline — close and get_autocommit
// ============================================================================

#[tokio::test]
async fn test_pipeline_close() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(None, vec![close_req()]);
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(resp["baton"].is_null(), "baton should be null after close");
    assert_eq!(resp["results"][0]["type"], "ok");
    assert_eq!(resp["results"][0]["response"]["type"], "close");
}

#[tokio::test]
async fn test_pipeline_get_autocommit() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(None, vec![json!({ "type": "get_autocommit" })]);
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");
    assert_eq!(
        resp["results"][0]["response"]["is_autocommit"], true,
        "Fresh connection should be in autocommit mode"
    );
}

// ============================================================================
// Pipeline — batch with conditions
// ============================================================================

#[tokio::test]
async fn test_pipeline_batch() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(
        None,
        vec![json!({
            "type": "batch",
            "batch": {
                "steps": [
                    {
                        "stmt": { "sql": "INSERT INTO t (id, name) VALUES (1, 'Alice')", "args": [], "named_args": [], "want_rows": false },
                        "condition": null
                    },
                    {
                        "stmt": { "sql": "INSERT INTO t (id, name) VALUES (2, 'Bob')", "args": [], "named_args": [], "want_rows": false },
                        "condition": { "type": "ok", "step": 0 }
                    },
                    {
                        "stmt": { "sql": "SELECT COUNT(*) FROM t", "args": [], "named_args": [], "want_rows": true },
                        "condition": { "type": "ok", "step": 1 }
                    }
                ]
            }
        })],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");

    let batch_result = &resp["results"][0]["response"]["result"];
    // All 3 steps should have results
    assert_eq!(batch_result["step_results"].as_array().unwrap().len(), 3);
    // Count should be 2
    let count_result = &batch_result["step_results"][2];
    assert_eq!(count_result["rows"][0][0]["value"], "2");
}

// ============================================================================
// Pipeline — describe
// ============================================================================

#[tokio::test]
async fn test_pipeline_describe() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(
        None,
        vec![json!({
            "type": "describe",
            "sql": "SELECT id, name FROM t"
        })],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");

    let result = &resp["results"][0]["response"]["result"];
    let cols = result["cols"].as_array().unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0]["name"], "id");
    assert_eq!(cols[1]["name"], "name");
}

// ============================================================================
// Pipeline — sequence
// ============================================================================

#[tokio::test]
async fn test_pipeline_sequence() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(
        None,
        vec![json!({
            "type": "sequence",
            "sql": "INSERT INTO t (id, name) VALUES (1, 'Alice')"
        })],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"][0]["type"], "ok");
    assert_eq!(resp["results"][0]["response"]["type"], "sequence");

    // Verify the insert worked
    let body = pipeline_request(None, vec![query_req("SELECT name FROM t WHERE id = 1")]);
    let (_, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(
        resp["results"][0]["response"]["result"]["rows"][0][0]["value"],
        "Alice"
    );
}

// ============================================================================
// Cursor
// ============================================================================

#[tokio::test]
async fn test_cursor_basic() {
    let (router, _, _tmp) = setup();

    // Insert some data first
    let body = pipeline_request(
        None,
        vec![
            execute_req("INSERT INTO t (id, name) VALUES (1, 'Alice')"),
            execute_req("INSERT INTO t (id, name) VALUES (2, 'Bob')"),
            close_req(),
        ],
    );
    post_json(&router, "/v3/pipeline", &body, None).await;

    // Cursor query
    let cursor_body = json!({
        "baton": null,
        "batch": {
            "steps": [
                {
                    "stmt": { "sql": "SELECT id, name FROM t ORDER BY id", "args": [], "named_args": [], "want_rows": true },
                    "condition": null
                }
            ]
        }
    });

    let request = Request::builder()
        .method("POST")
        .uri("/v3/cursor")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&cursor_body).unwrap()))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    // NDJSON: first line is header with baton, then step_begin, rows, step_end
    let lines: Vec<&str> = body_str.trim().split('\n').collect();
    assert!(
        lines.len() >= 4,
        "Expected at least 4 NDJSON lines, got {}",
        lines.len()
    );

    // Header has baton
    let header: Value = serde_json::from_str(lines[0]).unwrap();
    assert!(header["baton"].is_string());

    // step_begin
    let step_begin: Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(step_begin["type"], "step_begin");

    // Two rows
    let row1: Value = serde_json::from_str(lines[2]).unwrap();
    assert_eq!(row1["type"], "row");
    let row2: Value = serde_json::from_str(lines[3]).unwrap();
    assert_eq!(row2["type"], "row");

    // step_end
    let step_end: Value = serde_json::from_str(lines[4]).unwrap();
    assert_eq!(step_end["type"], "step_end");
}

#[tokio::test]
async fn test_cursor_auth_required() {
    let (router, _, _tmp) = setup_with_secret(Some("secret".into()));
    let cursor_body = json!({
        "baton": null,
        "batch": {
            "steps": [{
                "stmt": { "sql": "SELECT 1", "args": [], "named_args": [], "want_rows": true },
                "condition": null
            }]
        }
    });

    // No auth
    let request = Request::builder()
        .method("POST")
        .uri("/v3/cursor")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&cursor_body).unwrap()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // With auth
    let request = Request::builder()
        .method("POST")
        .uri("/v3/cursor")
        .header("content-type", "application/json")
        .header("authorization", "Bearer secret")
        .body(Body::from(serde_json::to_string(&cursor_body).unwrap()))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// Pipeline — args and named_args
// ============================================================================

#[tokio::test]
async fn test_pipeline_with_positional_args() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(
        None,
        vec![
            json!({
                "type": "execute",
                "stmt": {
                    "sql": "INSERT INTO t (id, name) VALUES (?1, ?2)",
                    "args": [
                        { "type": "integer", "value": "1" },
                        { "type": "text", "value": "Alice" }
                    ],
                    "named_args": [],
                    "want_rows": false
                }
            }),
            query_req("SELECT name FROM t WHERE id = 1"),
        ],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["results"][1]["response"]["result"]["rows"][0][0]["value"],
        "Alice"
    );
}

#[tokio::test]
async fn test_pipeline_with_named_args() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(
        None,
        vec![
            json!({
                "type": "execute",
                "stmt": {
                    "sql": "INSERT INTO t (id, name) VALUES (:id, :name)",
                    "args": [],
                    "named_args": [
                        { "name": "id", "value": { "type": "integer", "value": "1" } },
                        { "name": "name", "value": { "type": "text", "value": "Alice" } }
                    ],
                    "want_rows": false
                }
            }),
            query_req("SELECT name FROM t WHERE id = 1"),
        ],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["results"][1]["response"]["result"]["rows"][0][0]["value"],
        "Alice"
    );
}

// ============================================================================
// Pipeline — value types
// ============================================================================

#[tokio::test]
async fn test_pipeline_null_value() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(
        None,
        vec![
            execute_req("INSERT INTO t (id, name) VALUES (1, NULL)"),
            query_req("SELECT name FROM t WHERE id = 1"),
        ],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["results"][1]["response"]["result"]["rows"][0][0]["type"],
        "null"
    );
}

#[tokio::test]
async fn test_pipeline_blob_value() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(
        None,
        vec![json!({
            "type": "execute",
            "stmt": {
                "sql": "SELECT X'DEADBEEF' as blob_val",
                "args": [],
                "named_args": [],
                "want_rows": true
            }
        })],
    );
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    let val = &resp["results"][0]["response"]["result"]["rows"][0][0];
    assert_eq!(val["type"], "blob");
    // base64 of 0xDEADBEEF = "3q2+7w=="
    assert_eq!(val["base64"], "3q2+7w==");
}

// ============================================================================
// Pipeline — empty
// ============================================================================

#[tokio::test]
async fn test_pipeline_empty_requests() {
    let (router, _, _tmp) = setup();
    let body = pipeline_request(None, vec![]);
    let (status, resp) = post_json(&router, "/v3/pipeline", &body, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["results"].as_array().unwrap().len(), 0);
}

// ============================================================================
// Old /execute and /query endpoints are gone
// ============================================================================

#[tokio::test]
async fn test_old_execute_endpoint_404() {
    let (router, _, _tmp) = setup();
    let request = Request::builder()
        .method("POST")
        .uri("/execute")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"sql":"SELECT 1","params":[]}"#))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_old_query_endpoint_404() {
    let (router, _, _tmp) = setup();
    let request = Request::builder()
        .method("POST")
        .uri("/query")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"sql":"SELECT 1","params":[]}"#))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
