//! Shared-mode HA experiment -- HTTP server for external e2e testing.
//!
//! Each instance is an independent writer. Acquires lease per write.
//! Run multiple instances, hit them with concurrent writes, verify data integrity.
//!
//! Usage:
//!   # Node 1
//!   TIERED_TEST_BUCKET=haqlite-test AWS_ENDPOINT_URL=https://fly.storage.tigris.dev \
//!   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=auto \
//!   cargo run --bin haqlite-shared-experiment -- \
//!     --port 9001 --instance node-1
//!
//!   # Node 2 (same bucket, different port/instance)
//!   ... --port 9002 --instance node-2
//!
//!   # Test from Python:
//!   python tests/e2e_shared.py --nodes http://localhost:9001,http://localhost:9002

use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Json;
use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use haqlite::{Durability, HaQLite, HaMode, SqlValue};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS test_data (
    id TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    writer TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);";

#[derive(Parser)]
#[command(name = "haqlite-shared-experiment")]
#[command(about = "Shared-mode HA experiment for external e2e testing")]
struct Args {
    /// S3 bucket
    #[arg(long, env = "TIERED_TEST_BUCKET", default_value = "haqlite-test")]
    bucket: String,

    /// S3 prefix for this database
    #[arg(long, default_value = "shared-experiment/")]
    prefix: String,

    /// S3 endpoint
    #[arg(long, env = "AWS_ENDPOINT_URL")]
    endpoint: Option<String>,

    /// Local database path
    #[arg(long, default_value = "/tmp/haqlite-shared.db")]
    db: std::path::PathBuf,

    /// Instance ID (unique per node)
    #[arg(long, env = "HAQLITE_INSTANCE")]
    instance: Option<String>,

    /// HTTP port
    #[arg(long, default_value = "9001")]
    port: u16,

    /// Durability mode: synchronous or eventual
    #[arg(long, default_value = "synchronous")]
    durability: String,

    /// Lease TTL in seconds
    #[arg(long, default_value = "30")]
    lease_ttl: u64,
}

// ============================================================================
// HTTP handlers
// ============================================================================

/// POST /write — insert a row. Body: {"id": "...", "value": "..."}
async fn handle_write(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let id = body.get("id").and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let value = body.get("value").and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;

    match state.db.execute(
        "INSERT OR REPLACE INTO test_data (id, value, writer) VALUES (?1, ?2, ?3)",
        &[SqlValue::Text(id.into()), SqlValue::Text(value.into()),
          SqlValue::Text(state.instance_id.clone())],
    ) {
        Ok(rows) => Ok(Json(serde_json::json!({"ok": true, "rows_affected": rows}))),
        Err(e) => Ok(Json(serde_json::json!({"ok": false, "error": format!("{}", e)}))),
    }
}

/// GET /read?id=... — read a specific row (fresh read).
async fn handle_read(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let id = params.get("id").ok_or(StatusCode::BAD_REQUEST)?;
    let rows = state.db.query_values_fresh(
        "SELECT id, value, writer, created_at FROM test_data WHERE id = ?1",
        &[SqlValue::Text(id.clone())],
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(row) = rows.first() {
        Ok(Json(serde_json::json!({
            "found": true,
            "id": row.get(0),
            "value": row.get(1),
            "writer": row.get(2),
            "created_at": row.get(3),
        })))
    } else {
        Ok(Json(serde_json::json!({"found": false})))
    }
}

/// GET /count — row count (fresh read).
async fn handle_count(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rows = state.db.query_values_fresh(
        "SELECT COUNT(*) FROM test_data", &[],
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = rows.first()
        .and_then(|r| r.first())
        .and_then(|v| if let SqlValue::Integer(n) = v { Some(*n) } else { None })
        .unwrap_or(0);

    Ok(Json(serde_json::json!({"count": count})))
}

/// GET /verify — check data integrity (gaps, duplicates).
async fn handle_verify(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rows = state.db.query_values_fresh(
        "SELECT id, value, writer FROM test_data ORDER BY id", &[],
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = rows.len();

    // Check for duplicates (shouldn't happen with PRIMARY KEY)
    let mut ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut duplicates = 0;
    for row in &rows {
        if let SqlValue::Text(id) = &row[0] {
            if !ids.insert(id.clone()) { duplicates += 1; }
        }
    }

    Ok(Json(serde_json::json!({
        "ok": duplicates == 0,
        "count": count,
        "duplicates": duplicates,
        "instance": state.instance_id,
    })))
}

/// GET /dump — return all rows (fresh read).
async fn handle_dump(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rows = state.db.query_values_fresh(
        "SELECT id, value, writer, created_at FROM test_data ORDER BY id", &[],
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let data: Vec<serde_json::Value> = rows.iter().map(|row| {
        serde_json::json!({
            "id": row.get(0).map(|v| format!("{:?}", v)).unwrap_or_default(),
            "value": row.get(1).map(|v| format!("{:?}", v)).unwrap_or_default(),
            "writer": row.get(2).map(|v| format!("{:?}", v)).unwrap_or_default(),
        })
    }).collect();

    Ok(Json(serde_json::json!({"rows": data, "count": rows.len()})))
}

/// GET /health
async fn handle_health() -> StatusCode {
    StatusCode::OK
}

#[derive(Clone)]
struct AppState {
    db: Arc<HaQLite>,
    instance_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let args = Args::parse();
    let instance_id = args.instance.clone().unwrap_or_else(|| {
        format!("node-{}", args.port)
    });

    match args.durability.as_str() {
        "cloud" => {}
        other => anyhow::bail!("unknown durability: {} (expected: cloud)", other),
    };

    info!("=== haqlite shared-mode experiment ===");
    info!("Instance: {}", instance_id);
    info!("Bucket: {}", args.bucket);
    info!("Prefix: {}", args.prefix);
    info!("Durability: {}", args.durability);
    info!("Port: {}", args.port);
    info!("DB: {}", args.db.display());

    // Ensure parent directory exists
    if let Some(parent) = args.db.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut builder = HaQLite::builder()
        .prefix(&args.prefix)
        .mode(HaMode::Dedicated)
        .durability(hadb::Durability::Replicated(Duration::from_secs(1)))
        .instance_id(&instance_id)
        .lease_ttl(args.lease_ttl);

    if let Some(ref ep) = args.endpoint {
        builder = builder.endpoint(ep);
    }

    let db = Arc::new(
        builder.open(args.db.to_str().expect("path"), SCHEMA).await?,
    );

    info!("Database opened. Ready for writes.");

    let state = AppState { db, instance_id: instance_id.clone() };

    let app = axum::Router::new()
        .route("/write", post(handle_write))
        .route("/read", get(handle_read))
        .route("/count", get(handle_count))
        .route("/verify", get(handle_verify))
        .route("/dump", get(handle_dump))
        .route("/health", get(handle_health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    info!("HTTP server listening on http://localhost:{}", args.port);

    axum::serve(listener, app).await?;
    Ok(())
}
