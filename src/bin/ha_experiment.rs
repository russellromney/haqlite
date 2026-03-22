//! haqlite HA experiment
//!
//! Proves the HA mechanism works end-to-end, identically to walrust's ha_experiment
//! but using haqlite's Coordinator instead of raw LeaderElection:
//!
//! 1. Coordinator handles leader election via S3 CAS leases (LeaseStore)
//! 2. Leader streams WAL to S3 via walrust Replicator (inside Coordinator)
//! 3. Followers continuously pull LTX from S3 (inside Coordinator)
//! 4. Kill leader → Coordinator auto-promotes follower → catches up → starts replicating
//! 5. Writer connects via HTTP — writes fail immediately when leader dies
//!
//! Usage:
//!   # Terminal 1 — becomes leader, writes data, streams WAL
//!   haqlite-ha-experiment --bucket my-bucket --prefix ha-test/ \
//!     --db /tmp/ha-node1.db --instance node1 --port 9001
//!
//!   # Terminal 2 — becomes follower, pulls LTX as warm replica
//!   haqlite-ha-experiment --bucket my-bucket --prefix ha-test/ \
//!     --db /tmp/ha-node2.db --instance node2 --port 9002
//!
//!   # Writer — discovers leader via S3 and sends writes
//!   haqlite-ha-writer --bucket my-bucket --prefix ha-test/
//!
//!   # Kill terminal 1 (Ctrl-C) — terminal 2 promotes, writer reconnects

use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Json;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info};

use haqlite::{
    Coordinator, CoordinatorConfig, LeaseConfig, Role, RoleEvent,
    S3Backend, S3LeaseStore, StorageBackend,
};

const DB_NAME: &str = "ha-db";

#[derive(Parser)]
#[command(name = "haqlite-ha-experiment")]
#[command(about = "Standalone HA SQLite experiment using haqlite Coordinator")]
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
    db: PathBuf,

    /// Instance ID (unique per node)
    #[arg(long, env = "HA_INSTANCE_ID")]
    instance: Option<String>,

    /// HTTP port for write endpoint
    #[arg(long, default_value = "9001")]
    port: u16,

    /// WAL sync interval in seconds
    #[arg(long, default_value = "1")]
    sync_interval: u64,

    /// How many test rows to write (leader only, first time)
    #[arg(long, default_value = "100")]
    test_rows: u32,
}

// ============================================================================
// Shared state
// ============================================================================

/// Shared state accessible from HTTP handlers and role event listener.
struct AppState {
    db_path: PathBuf,
    /// SQLite connection — set when leader (read-write), None when follower.
    /// Followers open fresh connections per query to see LTX updates.
    conn: RwLock<Option<Arc<Mutex<rusqlite::Connection>>>>,
    coordinator: Arc<Coordinator>,
    /// Leader's HTTP address — used by follower for write proxying.
    leader_address: Arc<RwLock<String>>,
    /// HTTP client for proxying requests to leader.
    http_client: reqwest::Client,
}

impl AppState {
    /// Open a fresh read-only connection to see the latest DB state.
    /// Used by followers — each query gets a new connection so LTX page
    /// writes applied by the pull loop are visible.
    fn open_read_only(&self) -> Result<rusqlite::Connection, StatusCode> {
        rusqlite::Connection::open_with_flags(
            &self.db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
    }
}

// ============================================================================
// HTTP handlers
// ============================================================================

/// POST /write — insert a row. Returns {"id": N, "count": M}
async fn handle_write(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let conn_guard = state.conn.read().await;
    let conn = conn_guard.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let conn = conn.lock().await;

    let id: i64 = conn
        .query_row("SELECT COALESCE(MAX(id), 0) + 1 FROM test_data", [], |r| r.get(0))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    conn.execute(
        "INSERT INTO test_data (id, value) VALUES (?1, ?2)",
        rusqlite::params![id, format!("row-{}", id)],
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM test_data", [], |r| r.get(0))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({"id": id, "count": count})))
}

/// GET /count — return current row count.
/// Leaders use the cached read-write connection.
/// Followers open a fresh read-only connection each time so LTX updates are visible.
async fn handle_count(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let role = state.coordinator.role(DB_NAME).await.unwrap_or(Role::Follower);

    let count: i64 = if role == Role::Leader {
        // Leader: use cached read-write connection.
        let conn_guard = state.conn.read().await;
        let conn = conn_guard.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        let conn = conn.lock().await;
        conn.query_row("SELECT COUNT(*) FROM test_data", [], |r| r.get(0))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        // Follower: open fresh connection to see latest LTX-applied state.
        let conn = state.open_read_only()?;
        conn.query_row("SELECT COUNT(*) FROM test_data", [], |r| r.get(0))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };

    Ok(Json(serde_json::json!({"count": count})))
}

/// GET /replicas — discover registered followers for this database
async fn handle_replicas(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let replicas = state
        .coordinator
        .discover_replicas(DB_NAME)
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

/// GET /status — role, instance, row count, metrics snapshot
async fn handle_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let role = state.coordinator.role(DB_NAME).await.unwrap_or(Role::Follower);

    let count: i64 = if role == Role::Leader {
        let conn_guard = state.conn.read().await;
        match conn_guard.as_ref() {
            Some(conn) => {
                let conn = conn.lock().await;
                conn.query_row("SELECT COUNT(*) FROM test_data", [], |r| r.get(0))
                    .unwrap_or(0)
            }
            None => 0,
        }
    } else {
        match state.open_read_only() {
            Ok(conn) => conn
                .query_row("SELECT COUNT(*) FROM test_data", [], |r| r.get(0))
                .unwrap_or(0),
            Err(_) => 0,
        }
    };

    Ok(Json(serde_json::json!({
        "role": format!("{}", role),
        "row_count": count,
    })))
}

/// GET /metrics — haqlite HA metrics snapshot
async fn handle_metrics(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let snap = state.coordinator.metrics().snapshot();
    Json(serde_json::to_value(snap).unwrap_or_default())
}

/// GET /verify — check data integrity (no gaps in write sequence)
///
/// Scans test_data IDs and reports:
/// - total rows, min/max ID, any gaps in the sequence
/// - duplicate detection
async fn handle_verify(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let role = state.coordinator.role(DB_NAME).await.unwrap_or(Role::Follower);

    let result = if role == Role::Leader {
        let conn_guard = state.conn.read().await;
        let conn = conn_guard.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        let conn = conn.lock().await;
        verify_data_integrity(&conn)
    } else {
        let conn = state.open_read_only()?;
        verify_data_integrity(&conn)
    };

    result.map(Json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn verify_data_integrity(conn: &rusqlite::Connection) -> Result<serde_json::Value, rusqlite::Error> {
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM test_data", [], |r| r.get(0))?;

    if count == 0 {
        return Ok(serde_json::json!({
            "ok": true,
            "count": 0,
            "gaps": [],
            "duplicates": 0,
        }));
    }

    let min_id: i64 = conn.query_row("SELECT MIN(id) FROM test_data", [], |r| r.get(0))?;
    let max_id: i64 = conn.query_row("SELECT MAX(id) FROM test_data", [], |r| r.get(0))?;

    // Find gaps: IDs in [min..max] that are missing.
    // Uses a CTE to generate the expected range, then LEFT JOIN.
    let gap_count: i64 = conn.query_row(
        "WITH RECURSIVE seq(n) AS (
            SELECT ?1
            UNION ALL
            SELECT n + 1 FROM seq WHERE n < ?2
        )
        SELECT COUNT(*) FROM seq LEFT JOIN test_data ON seq.n = test_data.id
        WHERE test_data.id IS NULL",
        rusqlite::params![min_id, max_id],
        |r| r.get(0),
    )?;

    // Sample up to 10 gap IDs for diagnostics.
    let mut stmt = conn.prepare(
        "WITH RECURSIVE seq(n) AS (
            SELECT ?1
            UNION ALL
            SELECT n + 1 FROM seq WHERE n < ?2
        )
        SELECT seq.n FROM seq LEFT JOIN test_data ON seq.n = test_data.id
        WHERE test_data.id IS NULL
        LIMIT 10",
    )?;
    let gaps: Vec<i64> = stmt
        .query_map(rusqlite::params![min_id, max_id], |r| r.get(0))?
        .filter_map(|r| r.ok())
        .collect();

    // Check for duplicates (shouldn't happen with PRIMARY KEY, but verify).
    let dup_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM (SELECT id, COUNT(*) as c FROM test_data GROUP BY id HAVING c > 1)",
        [],
        |r| r.get(0),
    )?;

    let ok = gap_count == 0 && dup_count == 0;

    Ok(serde_json::json!({
        "ok": ok,
        "count": count,
        "min_id": min_id,
        "max_id": max_id,
        "expected_count": max_id - min_id + 1,
        "gap_count": gap_count,
        "gaps_sample": gaps,
        "duplicates": dup_count,
    }))
}

/// GET /health — always 200
async fn handle_health() -> StatusCode {
    StatusCode::OK
}

/// Middleware: if this node is a follower, proxy writes to the leader.
/// Reads (GET) are served locally from the follower's replica database.
async fn write_proxy(
    State(state): State<Arc<AppState>>,
    request: axum::http::Request<axum::body::Body>,
    next: middleware::Next,
) -> axum::response::Response {
    let is_write = matches!(
        *request.method(),
        axum::http::Method::POST | axum::http::Method::PUT
            | axum::http::Method::DELETE | axum::http::Method::PATCH
    );

    let role = state.coordinator.role(DB_NAME).await.unwrap_or(Role::Follower);
    if !is_write || role == Role::Leader {
        return next.run(request).await;
    }

    let leader_addr = state.leader_address.read().await.clone();
    if leader_addr.is_empty() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    // Read the request body before forwarding.
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let url = format!("{}{}", leader_addr, path);
    let body_bytes = match axum::body::to_bytes(request.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    match state.http_client.request(method, &url).body(body_bytes).send().await {
        Ok(resp) => {
            let status = resp.status();
            let headers = resp.headers().clone();
            let body = resp.bytes().await.unwrap_or_default();
            let mut response = (status, body).into_response();
            if let Some(ct) = headers.get("content-type") {
                response.headers_mut().insert("content-type", ct.clone());
            }
            response
        }
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
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
    let instance_id = args.instance.unwrap_or_else(|| format!("node-{}", std::process::id()));
    let address = format!("http://localhost:{}", args.port);

    info!("=== haqlite HA experiment ===");
    info!("Instance: {}", instance_id);
    info!("Bucket: {}", args.bucket);
    info!("Prefix: {}", args.prefix);
    info!("DB path: {}", args.db.display());
    info!("HTTP: {}", address);

    // Build S3 storage backend (for WAL data — used by walrust Replicator inside Coordinator)
    let storage: Arc<dyn StorageBackend> = Arc::new(
        S3Backend::from_env(args.bucket.clone(), args.endpoint.as_deref()).await?,
    );

    // Build S3 lease store (for CAS leases — used by haqlite for election)
    let s3_config = if let Some(ref endpoint) = args.endpoint {
        aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .load()
            .await
    } else {
        aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await
    };
    let s3_client = aws_sdk_s3::Client::new(&s3_config);
    let lease_store: Arc<dyn haqlite::LeaseStore> = Arc::new(
        S3LeaseStore::new(s3_client, args.bucket.clone()),
    );

    // Build Coordinator config
    let config = CoordinatorConfig {
        sync_interval: std::time::Duration::from_secs(args.sync_interval),
        lease: Some(LeaseConfig::new(instance_id.clone(), address.clone())),
        ..Default::default()
    };

    let coordinator = Coordinator::new(
        storage.clone(),
        Some(lease_store),
        &args.prefix,
        config,
    );

    // Subscribe to role events BEFORE join (so we don't miss the Joined event)
    let mut role_rx = coordinator.role_events();

    // Create DB with schema only — NO test rows yet.
    // We don't know our role yet. Inserting data before join() would give
    // followers phantom rows that didn't come from the leader.
    ensure_db_schema(&args.db)?;

    // Join the HA cluster — Coordinator handles everything:
    // - Leader: claims lease, starts Replicator sync, spawns renewal loop
    // - Follower: restores from S3, spawns pull loop, spawns lease monitor for auto-promote
    let initial_role = coordinator.join(DB_NAME, &args.db).await?;

    let leader_address = Arc::new(RwLock::new(
        if initial_role == Role::Leader {
            address.clone()
        } else {
            coordinator.leader_address(DB_NAME).await.unwrap_or_default()
        },
    ));

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    let state = Arc::new(AppState {
        db_path: args.db.clone(),
        conn: RwLock::new(None),
        coordinator: coordinator.clone(),
        leader_address: leader_address.clone(),
        http_client,
    });

    match initial_role {
        Role::Leader => {
            // Open the leader's main connection FIRST (autocheckpoint disabled so
            // the replicator can see WAL frames), then seed through it.
            let conn = rusqlite::Connection::open(&args.db)?;
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")?;

            // Seed test rows through this connection — WAL frames stay visible
            // to the replicator because autocheckpoint is disabled.
            let count: i64 = conn.query_row("SELECT COUNT(*) FROM test_data", [], |r| r.get(0))?;
            if count == 0 && args.test_rows > 0 {
                info!("Writing {} initial test rows...", args.test_rows);
                for i in 1..=args.test_rows {
                    conn.execute(
                        "INSERT OR IGNORE INTO test_data (id, value) VALUES (?1, ?2)",
                        rusqlite::params![i, format!("row-{}", i)],
                    )?;
                }
                let count: i64 = conn.query_row("SELECT COUNT(*) FROM test_data", [], |r| r.get(0))?;
                info!("Database has {} rows", count);
            } else {
                info!("Database already has {} rows", count);
            }

            *state.conn.write().await = Some(Arc::new(Mutex::new(conn)));
            info!("*** THIS INSTANCE IS THE LEADER ***");
        }
        Role::Follower => {
            let addr = leader_address.read().await.clone();
            info!("*** THIS INSTANCE IS A FOLLOWER (leader: {}) ***", addr);
        }
    }

    // Spawn role event listener — handles Promoted, Demoted, Sleeping
    {
        let event_state = state.clone();
        let event_leader_addr = leader_address.clone();
        let event_address = address.clone();
        tokio::spawn(async move {
            loop {
                match role_rx.recv().await {
                    Ok(RoleEvent::Promoted { db_name }) => {
                        info!("=== PROMOTED to leader for '{}' ===", db_name);
                        *event_leader_addr.write().await = event_address.clone();

                        // Open new read-write connection BEFORE closing the old one
                        // to minimize the unavailability window.
                        match rusqlite::Connection::open(&event_state.db_path) {
                            Ok(conn) => {
                                if let Err(e) = conn.execute_batch(
                                    "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;"
                                ) {
                                    error!("PRAGMA failed on promotion: {}", e);
                                    continue;
                                }
                                match conn.query_row(
                                    "SELECT COUNT(*) FROM test_data", [], |r| r.get::<_, i64>(0)
                                ) {
                                    Ok(count) => info!("Leader DB open after promotion: {} rows", count),
                                    Err(e) => error!("Failed to count rows after promotion: {}", e),
                                }
                                // Atomic swap: old connection is dropped when replaced.
                                *event_state.conn.write().await = Some(Arc::new(Mutex::new(conn)));
                            }
                            Err(e) => {
                                error!("Failed to open DB on promotion: {}", e);
                            }
                        }
                    }
                    Ok(RoleEvent::Demoted { db_name }) => {
                        error!("=== DEMOTED from leader for '{}' ===", db_name);
                        // Close read-write connection immediately — followers use
                        // fresh per-query connections, no cached conn needed.
                        *event_state.conn.write().await = None;
                    }
                    Ok(RoleEvent::Fenced { db_name }) => {
                        error!("=== FENCED for '{}' — stopping ===", db_name);
                        *event_state.conn.write().await = None;
                    }
                    Ok(RoleEvent::Sleeping { db_name }) => {
                        info!("=== SLEEPING signal for '{}' — shutting down ===", db_name);
                        *event_state.conn.write().await = None;
                    }
                    Ok(RoleEvent::Joined { db_name, role }) => {
                        info!("Joined '{}' as {}", db_name, role);
                    }
                    Err(e) => {
                        error!("Role event channel closed: {}", e);
                        break;
                    }
                }
            }
        });
    }

    // HTTP server — proxy middleware forwards writes to leader when this node is follower
    let app = axum::Router::new()
        .route("/write", post(handle_write))
        .route("/count", get(handle_count))
        .route("/replicas", get(handle_replicas))
        .route("/status", get(handle_status))
        .route("/metrics", get(handle_metrics))
        .route("/verify", get(handle_verify))
        .route("/health", get(handle_health))
        .layer(middleware::from_fn_with_state(state.clone(), write_proxy))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    info!("HTTP server listening on port {}", args.port);

    let server = axum::serve(listener, app);

    // Wait for shutdown signal
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

    // Close connection (rejects new writes)
    *state.conn.write().await = None;

    // Leave the cluster — Coordinator handles:
    // - Leader: final WAL sync via replicator.remove(), lease release
    // - Follower: stops pull loop + monitor
    if let Err(e) = coordinator.leave(DB_NAME).await {
        error!("Failed to leave cluster: {}", e);
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    info!("Goodbye.");
    Ok(())
}

// ============================================================================
// DB setup — schema only (safe for any role)
// ============================================================================

/// Create the DB file with schema but NO data.
/// Called before join() so the file exists for the leader path (replicator.add
/// needs the file). For followers, restore_internal() overwrites the file.
fn ensure_db_schema(db_path: &std::path::Path) -> Result<()> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS test_data (
            id INTEGER PRIMARY KEY,
            value TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );",
    )?;
    drop(conn);
    Ok(())
}

