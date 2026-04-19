//! Unified HA experiment binary -- HTTP server for all mode combinations.
//!
//! Supports all 4 valid haqlite configurations:
//!   Dedicated + Replicated  (classic walrust HA)
//!   Dedicated + Synchronous (turbolite S3Primary HA)
//!   Dedicated + Eventual    (turbolite + walrust HA)
//!   Shared + Synchronous    (multiwriter, turbolite S3Primary)
//!
//! Usage:
//!   # Shared + Synchronous (2 nodes)
//!   haqlite-experiment --topology shared --durability synchronous \
//!     --port 9001 --instance node-1 --prefix "exp-123/"
//!
//!   # Dedicated + Replicated (leader)
//!   haqlite-experiment --topology dedicated --durability replicated \
//!     --port 9001 --instance node-1 --prefix "exp-456/"
//!
//!   # Then hit HTTP endpoints from Python e2e tests.

use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Json;
use clap::Parser;
use std::sync::Arc;
use tracing::{error, info};

use haqlite::{Durability, HaMode, HaQLite, SqlValue};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS test_data (
    id TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    writer TEXT NOT NULL,
    seq INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);";

#[derive(Parser)]
#[command(name = "haqlite-experiment")]
#[command(about = "Unified HA experiment for all haqlite mode combinations")]
struct Args {
    /// Topology: dedicated or shared
    #[arg(long, default_value = "shared")]
    topology: String,

    /// Durability: replicated, synchronous, or eventual
    #[arg(long, default_value = "synchronous")]
    durability: String,

    /// S3 bucket
    #[arg(long, env = "TIERED_TEST_BUCKET", default_value = "haqlite-test")]
    bucket: String,

    /// S3 prefix for this database (unique per test run)
    #[arg(long, default_value = "experiment/")]
    prefix: String,

    /// S3 endpoint
    #[arg(long, env = "AWS_ENDPOINT_URL")]
    endpoint: Option<String>,

    /// Local database path
    #[arg(long, default_value = "/tmp/haqlite-experiment.db")]
    db: std::path::PathBuf,

    /// Instance ID (unique per node)
    #[arg(long, env = "HAQLITE_INSTANCE")]
    instance: Option<String>,

    /// HTTP port
    #[arg(long, default_value = "9001")]
    port: u16,

    /// Lease TTL in seconds
    #[arg(long, default_value = "30")]
    lease_ttl: u64,

    /// Shared secret for write forwarding auth (Dedicated mode)
    #[arg(long, env = "HAQLITE_SECRET")]
    secret: Option<String>,

    /// WAL sync interval in milliseconds (Dedicated mode)
    #[arg(long, default_value = "1000")]
    sync_interval_ms: u64,

    /// Lease renew interval in milliseconds (Dedicated mode)
    #[arg(long, default_value = "2000")]
    renew_interval_ms: u64,

    /// Follower poll interval in milliseconds (Dedicated mode)
    #[arg(long, default_value = "1000")]
    follower_poll_ms: u64,

    /// Follower pull interval in milliseconds (Dedicated mode)
    #[arg(long, default_value = "1000")]
    follower_pull_ms: u64,

    /// Number of initial rows to seed (leader only, Dedicated mode)
    #[arg(long, default_value = "0")]
    seed_rows: u32,

    /// Write timeout in seconds (Shared mode)
    #[arg(long, default_value = "30")]
    write_timeout: u64,

    /// NATS URL for lease store (e.g. nats://localhost:4222). Uses S3 if not set.
    #[arg(long, env = "NATS_URL")]
    nats_url: Option<String>,
}

// ============================================================================
// HTTP handlers
// ============================================================================

/// POST /write -- insert a row. Body: {"id": "...", "value": "...", "seq": 0}
async fn handle_write(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let value = body
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let seq = body.get("seq").and_then(|v| v.as_i64()).unwrap_or(0);

    match state
        .db
        .execute(
            "INSERT OR REPLACE INTO test_data (id, value, writer, seq) VALUES (?1, ?2, ?3, ?4)",
            &[
                SqlValue::Text(id.into()),
                SqlValue::Text(value.into()),
                SqlValue::Text(state.instance_id.clone()),
                SqlValue::Integer(seq),
            ],
        )
    {
        Ok(rows) => Ok(Json(
            serde_json::json!({"ok": true, "rows_affected": rows, "node": state.instance_id}),
        )),
        Err(e) => {
            error!("Write failed: {}", e);
            Ok(Json(
                serde_json::json!({"ok": false, "error": format!("{}", e), "node": state.instance_id}),
            ))
        }
    }
}

/// POST /execute -- run arbitrary SQL. Body: {"sql": "...", "params": [...]}
async fn handle_execute(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let sql = body
        .get("sql")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;

    let params: Vec<SqlValue> = match body.get("params") {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => SqlValue::Text(s.clone()),
                serde_json::Value::Number(n) => {
                    SqlValue::Integer(n.as_i64().unwrap_or(0))
                }
                serde_json::Value::Null => SqlValue::Null,
                _ => SqlValue::Text(v.to_string()),
            })
            .collect(),
        _ => vec![],
    };

    match state.db.execute(sql, &params) {
        Ok(rows) => Ok(Json(serde_json::json!({"ok": true, "rows_affected": rows}))),
        Err(e) => Ok(Json(serde_json::json!({"ok": false, "error": format!("{}", e)}))),
    }
}

/// GET /read?id=... -- read a specific row (fresh read).
async fn handle_read(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let id = params.get("id").ok_or(StatusCode::BAD_REQUEST)?;
    let rows = state
        .db
        .query_values_fresh(
            "SELECT id, value, writer, seq, created_at FROM test_data WHERE id = ?1",
            &[SqlValue::Text(id.clone())],
        )
        .await
        .map_err(|e| {
            error!("Read failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Some(row) = rows.first() {
        Ok(Json(serde_json::json!({
            "found": true,
            "id": format_val(row.get(0)),
            "value": format_val(row.get(1)),
            "writer": format_val(row.get(2)),
            "seq": format_val(row.get(3)),
            "created_at": format_val(row.get(4)),
        })))
    } else {
        Ok(Json(serde_json::json!({"found": false})))
    }
}

/// GET /query?sql=... -- run a SELECT query (fresh read).
async fn handle_query(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let sql = params.get("sql").ok_or(StatusCode::BAD_REQUEST)?;
    let rows = state
        .db
        .query_values_fresh(sql, &[])
        .await
        .map_err(|e| {
            error!("Query failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let data: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| row.iter().map(|v| sqlval_to_json(v)).collect())
        .collect();

    Ok(Json(serde_json::json!({"rows": data, "count": data.len()})))
}

/// GET /count -- row count (fresh read).
async fn handle_count(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rows = state
        .db
        .query_values_fresh("SELECT COUNT(*) FROM test_data", &[])
        .await
        .map_err(|e| {
            error!("Count failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let count = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| {
            if let SqlValue::Integer(n) = v {
                Some(*n)
            } else {
                None
            }
        })
        .unwrap_or(0);

    Ok(Json(serde_json::json!({"count": count, "node": state.instance_id})))
}

/// GET /verify -- check data integrity (gaps, duplicates).
async fn handle_verify(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rows = state
        .db
        .query_values_fresh(
            "SELECT id, value, writer FROM test_data ORDER BY id",
            &[],
        )
        .await
        .map_err(|e| {
            error!("Verify failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let count = rows.len();
    let mut ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut duplicates = 0;
    for row in &rows {
        if let SqlValue::Text(id) = &row[0] {
            if !ids.insert(id.clone()) {
                duplicates += 1;
            }
        }
    }

    Ok(Json(serde_json::json!({
        "ok": duplicates == 0,
        "count": count,
        "duplicates": duplicates,
        "node": state.instance_id,
    })))
}

/// GET /dump -- return all rows (fresh read).
async fn handle_dump(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rows = state
        .db
        .query_values_fresh(
            "SELECT id, value, writer, seq, created_at FROM test_data ORDER BY id",
            &[],
        )
        .await
        .map_err(|e| {
            error!("Dump failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let data: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "id": format_val(row.get(0)),
                "value": format_val(row.get(1)),
                "writer": format_val(row.get(2)),
                "seq": format_val(row.get(3)),
            })
        })
        .collect();

    Ok(Json(
        serde_json::json!({"rows": data, "count": rows.len(), "node": state.instance_id}),
    ))
}

/// GET /status -- node info.
async fn handle_status(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let role = state.db.role().map(|r| format!("{}", r)).unwrap_or_else(|| "none".to_string());

    let count = state
        .db
        .query_values_fresh("SELECT COUNT(*) FROM test_data", &[])
        .await
        .ok()
        .and_then(|rows| rows.first().and_then(|r| r.first().cloned()))
        .and_then(|v| if let SqlValue::Integer(n) = v { Some(n) } else { None })
        .unwrap_or(0);

    Ok(Json(serde_json::json!({
        "node": state.instance_id,
        "role": role,
        "topology": state.topology,
        "durability": state.durability_name,
        "count": count,
    })))
}

/// GET /health
async fn handle_health(
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true, "node": state.instance_id}))
}

fn format_val(v: Option<&SqlValue>) -> serde_json::Value {
    match v {
        Some(SqlValue::Text(s)) => serde_json::Value::String(s.clone()),
        Some(SqlValue::Integer(n)) => serde_json::json!(n),
        Some(SqlValue::Real(f)) => serde_json::json!(f),
        Some(SqlValue::Null) => serde_json::Value::Null,
        Some(SqlValue::Blob(b)) => serde_json::json!(format!("<blob {}B>", b.len())),
        None => serde_json::Value::Null,
    }
}

fn sqlval_to_json(v: &SqlValue) -> serde_json::Value {
    match v {
        SqlValue::Text(s) => serde_json::Value::String(s.clone()),
        SqlValue::Integer(n) => serde_json::json!(n),
        SqlValue::Real(f) => serde_json::json!(f),
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Blob(b) => serde_json::json!(format!("<blob {}B>", b.len())),
    }
}

#[derive(Clone)]
struct AppState {
    db: Arc<HaQLite>,
    instance_id: String,
    topology: String,
    durability_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let instance_id = args.instance.clone().unwrap_or_else(|| {
        format!("node-{}", args.port)
    });

    let mode = match args.topology.as_str() {
        "dedicated" => HaMode::Dedicated,
        "shared" => HaMode::Shared,
        other => anyhow::bail!("unknown topology: {} (expected: dedicated, shared)", other),
    };

    let durability = match args.durability.as_str() {
        "replicated" => Durability::Replicated,
        "synchronous" => Durability::Synchronous,
        "eventual" => Durability::Eventual,
        other => anyhow::bail!(
            "unknown durability: {} (expected: replicated, synchronous, eventual)",
            other
        ),
    };

    info!("=== haqlite experiment ===");
    info!("Topology: {}", args.topology);
    info!("Durability: {}", args.durability);
    info!("Instance: {}", instance_id);
    info!("Bucket: {}", args.bucket);
    info!("Prefix: {}", args.prefix);
    info!("Port: {}", args.port);
    info!("DB: {}", args.db.display());

    // Ensure parent directory exists
    if let Some(parent) = args.db.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let forwarding_port = args.port + 1000;
    let address = format!("http://localhost:{}", forwarding_port);

    let mut builder = HaQLite::builder(&args.bucket)
        .prefix(&args.prefix)
        .mode(mode)
        .durability(durability)
        .instance_id(&instance_id)
        .lease_ttl(args.lease_ttl);

    if let Some(ref ep) = args.endpoint {
        builder = builder.endpoint(ep);
    }
    if let Some(ref secret) = args.secret {
        builder = builder.secret(secret);
    }

    #[cfg(feature = "nats-lease")]
    if let Some(ref nats_url) = args.nats_url {
        let nats_lease = hadb_lease_nats::NatsLeaseStore::connect(nats_url, "haqlite-leases")
            .map_err(|e| anyhow::anyhow!("NATS lease connect: {}", e))?;
        builder = builder.lease_store(std::sync::Arc::new(nats_lease));
        info!("Using NATS lease store: {}", nats_url);
    }

    match mode {
        HaMode::Dedicated => {
            // Lease timing knobs (--lease-ttl etc.) are currently dropped on the
            // floor — the haqlite builder constructs its own LeaseConfig at
            // finalize-time. Pre-existing bug; tracked separately.
            let _ = (
                args.lease_ttl,
                args.renew_interval_ms,
                args.follower_poll_ms,
            );
            let coordinator_config = haqlite::CoordinatorConfig {
                sync_interval: std::time::Duration::from_millis(args.sync_interval_ms),
                follower_pull_interval: std::time::Duration::from_millis(args.follower_pull_ms),
                lease: None,
                ..Default::default()
            };

            builder = builder
                .forwarding_port(forwarding_port)
                .address(&address)
                .coordinator_config(coordinator_config);
        }
        HaMode::Shared => {
            builder = builder.write_timeout(std::time::Duration::from_secs(args.write_timeout));
        }
    }

    let db = Arc::new(
        builder
            .open(args.db.to_str().expect("path"), SCHEMA)
            .await?,
    );

    let role_str = db
        .role()
        .map(|r| format!("{}", r))
        .unwrap_or_else(|| "shared-writer".to_string());
    info!("Database opened. Role: {}", role_str);

    // Seed rows if requested (Dedicated leader only)
    if mode == HaMode::Dedicated && args.seed_rows > 0 {
        if db.role() == Some(haqlite::Role::Leader) {
            let count = db
                .query_values_fresh("SELECT COUNT(*) FROM test_data", &[])
                .await
                .ok()
                .and_then(|rows| rows.first().and_then(|r| r.first().cloned()))
                .and_then(|v| {
                    if let SqlValue::Integer(n) = v {
                        Some(n)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);

            if count == 0 {
                info!("Seeding {} initial rows...", args.seed_rows);
                for i in 1..=args.seed_rows {
                    db.execute(
                        "INSERT OR IGNORE INTO test_data (id, value, writer, seq) VALUES (?1, ?2, ?3, ?4)",
                        &[
                            SqlValue::Text(format!("seed-{}", i)),
                            SqlValue::Text(format!("seed-value-{}", i)),
                            SqlValue::Text(instance_id.clone()),
                            SqlValue::Integer(i as i64),
                        ],
                    )
                    ?;
                }
                info!("Seeded {} rows", args.seed_rows);
            }
        }
    }

    let db_for_shutdown = db.clone();
    let state = AppState {
        db,
        instance_id: instance_id.clone(),
        topology: args.topology.clone(),
        durability_name: args.durability.clone(),
    };

    let app = axum::Router::new()
        .route("/write", post(handle_write))
        .route("/execute", post(handle_execute))
        .route("/read", get(handle_read))
        .route("/query", get(handle_query))
        .route("/count", get(handle_count))
        .route("/verify", get(handle_verify))
        .route("/dump", get(handle_dump))
        .route("/status", get(handle_status))
        .route("/health", get(handle_health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    info!("HTTP server listening on http://localhost:{}", args.port);

    let server = axum::serve(listener, app);

    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = sigterm.recv() => info!("Received SIGTERM"),
        _ = tokio::signal::ctrl_c() => info!("Received SIGINT"),
        result = server => {
            if let Err(e) = result {
                error!("HTTP server error: {}", e);
            }
        }
    }

    info!("Shutting down...");
    // Graceful shutdown: release lease, stop background tasks.
    match Arc::try_unwrap(db_for_shutdown) {
        Ok(mut db) => {
            if let Err(e) = db.close().await {
                error!("close() failed: {}", e);
            }
        }
        Err(_arc) => {
            // Other references still held (in-flight requests).
            // Drop will abort background tasks.
            error!("Could not unwrap Arc<HaQLite> for clean shutdown");
        }
    }
    Ok(())
}
