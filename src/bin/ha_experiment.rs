//! haqlite HA experiment — now powered by HaQLite.
//!
//! Proves the HA mechanism works end-to-end:
//!   1. HaQLite handles leader election, WAL replication, write forwarding
//!   2. Kill leader → follower auto-promotes → catches up → starts replicating
//!   3. Writer connects via HTTP — writes forward automatically to leader
//!
//! Usage:
//!   # Terminal 1 — becomes leader
//!   haqlite-ha-experiment --bucket my-bucket --prefix ha-test/ \
//!     --db /tmp/node1/ha.db --instance node1 --port 9001
//!
//!   # Terminal 2 — becomes follower
//!   haqlite-ha-experiment --bucket my-bucket --prefix ha-test/ \
//!     --db /tmp/node2/ha.db --instance node2 --port 9002
//!
//!   # Writer — discovers leader via S3 and sends writes
//!   haqlite-ha-writer --bucket my-bucket --prefix ha-test/ --db-name ha

use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Json;
use clap::Parser;
use std::sync::Arc;
use tracing::{error, info};

use haqlite::{CoordinatorConfig, HaQLite, Role, SqlValue};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS test_data (
    id INTEGER PRIMARY KEY,
    value TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);";

#[derive(Parser)]
#[command(name = "haqlite-ha-experiment")]
#[command(about = "HA SQLite experiment using HaQLite")]
struct Args {
    /// S3 bucket for leader election and WAL storage
    #[arg(long, env = "HA_BUCKET")]
    bucket: String,

    /// S3 prefix (e.g., "ha-test/")
    #[arg(long, env = "HA_PREFIX", default_value = "ha-test/")]
    prefix: String,

    /// S3 endpoint (for Tigris/MinIO)
    #[arg(long, env = "HA_S3_ENDPOINT")]
    endpoint: Option<String>,

    /// Local database path
    #[arg(long, env = "HA_DB_PATH", default_value = "/tmp/haqlite-ha-node.db")]
    db: std::path::PathBuf,

    /// Instance ID (unique per node)
    #[arg(long, env = "HA_INSTANCE_ID")]
    instance: Option<String>,

    /// HTTP port for the app server
    #[arg(long, default_value = "9001")]
    port: u16,

    /// How many test rows to write (leader only, first time)
    #[arg(long, default_value = "100")]
    test_rows: u32,

    /// WAL sync interval in milliseconds
    #[arg(long, default_value = "1000")]
    sync_interval_ms: u64,

    /// Lease TTL in seconds
    #[arg(long, default_value = "5")]
    lease_ttl: u64,

    /// Lease renew interval in milliseconds
    #[arg(long, default_value = "2000")]
    renew_interval_ms: u64,

    /// Follower poll interval in milliseconds (how often followers check for leader death)
    #[arg(long, default_value = "1000")]
    follower_poll_ms: u64,

    /// Follower pull interval in milliseconds (how often followers pull new LTX files)
    #[arg(long, default_value = "1000")]
    follower_pull_ms: u64,

    /// Shared secret for authenticating forwarding requests
    #[arg(long, env = "HAQLITE_SECRET")]
    secret: Option<String>,
}

// ============================================================================
// HTTP handlers
// ============================================================================

/// POST /write — insert a row via HaQLite. Forwarding is transparent.
async fn handle_write(
    State(db): State<Arc<HaQLite>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Get next ID.
    let id: i64 = db
        .query_row("SELECT COALESCE(MAX(id), 0) + 1 FROM test_data", &[], |r| {
            r.get(0)
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    db.execute(
        "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
        &[SqlValue::Integer(id), SqlValue::Text(format!("row-{}", id))],
    )
    .map_err(|e| {
        error!("Write failed: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({"id": id, "count": count})))
}

/// GET /count — return current row count.
async fn handle_count(
    State(db): State<Arc<HaQLite>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"count": count})))
}

/// GET /status — role and row count.
async fn handle_status(
    State(db): State<Arc<HaQLite>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let role = db.role().unwrap_or(Role::Follower);
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .unwrap_or(0);

    Ok(Json(serde_json::json!({
        "role": format!("{}", role),
        "row_count": count,
    })))
}

/// GET /metrics — haqlite HA metrics snapshot.
async fn handle_metrics(State(db): State<Arc<HaQLite>>) -> Json<serde_json::Value> {
    match db.coordinator() {
        Some(c) => {
            let snap = c.metrics().snapshot();
            Json(serde_json::to_value(snap).unwrap_or_default())
        }
        None => Json(serde_json::json!({})),
    }
}

/// GET /replicas — discover registered followers.
async fn handle_replicas(
    State(db): State<Arc<HaQLite>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let coordinator = db.coordinator().ok_or(StatusCode::NOT_FOUND)?;
    let db_name = "ha-db"; // matches the file stem from --db path
    let replicas = coordinator
        .discover_replicas(db_name)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let nodes: Vec<serde_json::Value> = replicas
        .iter()
        .map(|r| {
            serde_json::json!({
                "instance_id": r.instance_id,
                "address": r.address,
                "role": r.role,
                "last_seen": r.last_seen,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "replicas": nodes })))
}

/// GET /verify — check data integrity.
async fn handle_verify(
    State(db): State<Arc<HaQLite>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if count == 0 {
        return Ok(Json(serde_json::json!({
            "ok": true, "count": 0, "gaps": [], "duplicates": 0,
        })));
    }

    let min_id: i64 = db
        .query_row("SELECT MIN(id) FROM test_data", &[], |r| r.get(0))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let max_id: i64 = db
        .query_row("SELECT MAX(id) FROM test_data", &[], |r| r.get(0))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let gap_count: i64 = db
        .query_row(
            "WITH RECURSIVE seq(n) AS (
                SELECT ?1
                UNION ALL
                SELECT n + 1 FROM seq WHERE n < ?2
            )
            SELECT COUNT(*) FROM seq LEFT JOIN test_data ON seq.n = test_data.id
            WHERE test_data.id IS NULL",
            &[&min_id as &dyn rusqlite::types::ToSql, &max_id],
            |r| r.get(0),
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let dup_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM (SELECT id, COUNT(*) as c FROM test_data GROUP BY id HAVING c > 1)",
            &[],
            |r| r.get(0),
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let ok = gap_count == 0 && dup_count == 0;

    Ok(Json(serde_json::json!({
        "ok": ok,
        "count": count,
        "min_id": min_id,
        "max_id": max_id,
        "expected_count": max_id - min_id + 1,
        "gap_count": gap_count,
        "duplicates": dup_count,
    })))
}

/// GET /health
async fn handle_health() -> StatusCode {
    StatusCode::OK
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let forwarding_port = args.port + 1000;

    info!("=== haqlite HA experiment ===");
    info!("Bucket: {}", args.bucket);
    info!("Prefix: {}", args.prefix);
    info!("DB path: {}", args.db.display());
    info!("App HTTP: http://localhost:{}", args.port);
    info!("Forwarding: http://localhost:{}", forwarding_port);

    info!("Sync interval: {}ms", args.sync_interval_ms);
    info!("Lease TTL: {}s", args.lease_ttl);
    info!("Renew interval: {}ms", args.renew_interval_ms);
    info!("Follower poll: {}ms", args.follower_poll_ms);
    info!("Follower pull: {}ms", args.follower_pull_ms);

    // Build coordinator config from CLI args.
    let instance_id = args.instance.clone().unwrap_or_else(|| {
        std::env::var("FLY_MACHINE_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
    });
    let address = format!("http://localhost:{}", forwarding_port);

    // Lease timing knobs (--lease-ttl, --renew-interval-ms,
    // --follower-poll-ms) flow through the singlewriter builder setters below;
    // the HaQLite builder patches the lease store + instance id + address
    // into the LeaseConfig at finalize-time without clobbering timing.
    let coordinator_config = CoordinatorConfig {
        durability: hadb::Durability::Replicated(std::time::Duration::from_millis(
            args.sync_interval_ms,
        )),
        follower_pull_interval: std::time::Duration::from_millis(args.follower_pull_ms),
        lease: None,
        ..Default::default()
    };

    // Build HaQLite.
    let mut builder = HaQLite::builder()
        .prefix(&args.prefix)
        .forwarding_port(forwarding_port)
        .instance_id(&instance_id)
        .address(&address)
        .coordinator_config(coordinator_config)
        .lease_ttl(args.lease_ttl)
        .lease_renew_interval(std::time::Duration::from_millis(args.renew_interval_ms))
        .lease_follower_poll_interval(std::time::Duration::from_millis(args.follower_poll_ms));

    if let Some(ref endpoint) = args.endpoint {
        builder = builder.endpoint(endpoint);
    }
    if let Some(ref secret) = args.secret {
        builder = builder.secret(secret);
    }

    let db = Arc::new(builder.open(args.db.to_str().unwrap(), SCHEMA).await?);

    let role = db.role().unwrap_or(Role::Follower);
    info!("*** THIS INSTANCE IS THE {} ***", role);

    // Seed test rows if leader and DB is empty.
    if role == Role::Leader && args.test_rows > 0 {
        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
            .unwrap_or(0);
        if count == 0 {
            info!("Writing {} initial test rows...", args.test_rows);
            for i in 1..=args.test_rows {
                db.execute(
                    "INSERT OR IGNORE INTO test_data (id, value) VALUES (?1, ?2)",
                    &[
                        SqlValue::Integer(i as i64),
                        SqlValue::Text(format!("row-{}", i)),
                    ],
                )?;
            }
            let count: i64 = db
                .query_row("SELECT COUNT(*) FROM test_data", &[], |r| r.get(0))
                .unwrap_or(0);
            info!("Database has {} rows", count);
        } else {
            info!("Database already has {} rows", count);
        }
    }

    // App HTTP server — no write_proxy needed, HaQLite handles forwarding.
    let app = axum::Router::new()
        .route("/write", post(handle_write))
        .route("/count", get(handle_count))
        .route("/replicas", get(handle_replicas))
        .route("/status", get(handle_status))
        .route("/metrics", get(handle_metrics))
        .route("/verify", get(handle_verify))
        .route("/health", get(handle_health))
        .with_state(db.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    info!("HTTP server listening on port {}", args.port);

    let server = axum::serve(listener, app);

    // Wait for shutdown signal.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
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

    // HaQLite handles cleanup: close connection, leave cluster, abort tasks.
    // We need to unwrap the Arc first.
    match Arc::try_unwrap(db) {
        Ok(mut db) => {
            if let Err(e) = db.close().await {
                error!("Failed to close HaQLite: {}", e);
            }
        }
        Err(_) => {
            error!("Could not unwrap Arc<HaQLite> for clean shutdown");
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    info!("Goodbye.");
    Ok(())
}
