//! `haqlite serve` — production HA SQLite server.
//!
//! Combines HaQLite (leader election + WAL replication) with an HTTP API
//! for SQL operations. Replaces the experiment binary with a config-driven
//! production server.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::Json;
use tracing::{error, info};

use hadb::{CoordinatorConfig, Role};
use hadb_cli::SharedConfig;

use crate::cli_config::ServeConfig;
use crate::database::{HaMode, HaQLite};

/// Shared state for HTTP handlers.
pub struct AppState {
    db: Arc<HaQLite>,
    /// Bearer token for API auth. None = no auth required.
    secret: Option<String>,
}

impl AppState {
    /// Create a new AppState. Public for testing.
    pub fn new(db: Arc<HaQLite>, secret: Option<String>) -> Self {
        Self { db, secret }
    }
}

/// Build the axum Router with all routes. Public for testing.
///
/// Merges the hrana protocol router (v2/v3 pipeline + cursor) with
/// the health/status/metrics endpoints.
pub fn build_router(state: Arc<AppState>, hrana_router: axum::Router) -> axum::Router {
    axum::Router::new()
        .route("/health", get(handle_health))
        .route("/status", get(handle_status))
        .route("/metrics", get(handle_metrics))
        .with_state(state)
        .merge(hrana_router)
}

/// Run the haqlite serve command.
pub async fn run(shared: &SharedConfig, serve: &ServeConfig) -> Result<()> {
    if shared.s3.bucket.is_empty() {
        anyhow::bail!("S3 bucket is required (set [s3] bucket in config or HADB_BUCKET env var)");
    }

    let db_path = &serve.db_path;
    let schema = serve.schema.as_deref().unwrap_or("");

    // Build coordinator config from shared + serve config.
    let instance_id = std::env::var("FLY_MACHINE_ID")
        .or_else(|_| std::env::var("HADB_INSTANCE_ID"))
        .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

    let address = format!("http://0.0.0.0:{}", serve.forwarding_port);

    // The HaQLite builder patches the lease store / instance id / address
    // into whatever `LeaseConfig` we provide here; it preserves our timing
    // policy. Lease timing knobs flow through the singlewriter builder
    // setters below (`.lease_ttl()`, `.lease_renew_interval()`,
    // `.lease_follower_poll_interval()`), so the coordinator config here
    // just carries non-lease fields.
    let coordinator_config = CoordinatorConfig {
        durability: hadb::Durability::Replicated(Duration::from_millis(serve.sync_interval_ms)),
        follower_pull_interval: Duration::from_millis(serve.follower_pull_ms),
        lease: None,
        ..Default::default()
    };

    // Parse mode from env or config. Reject invalid values (no silent fallbacks).
    let mode_str = std::env::var("HAQLITE_MODE")
        .unwrap_or_else(|_| serve.mode.clone())
        .to_lowercase();
    let ha_mode = match mode_str.as_str() {
        "singlewriter" | "dedicated" => HaMode::SingleWriter,
        "sharedwriter" | "shared" => HaMode::SharedWriter,
        other => anyhow::bail!(
            "invalid HAQLITE_MODE '{other}' (expected 'singlewriter' or 'sharedwriter')"
        ),
    };

    // Build HaQLite.
    let mut builder = HaQLite::builder()
        .mode(ha_mode)
        .prefix(&serve.prefix)
        .forwarding_port(serve.forwarding_port)
        .instance_id(&instance_id)
        .address(&address)
        .coordinator_config(coordinator_config)
        .lease_ttl(shared.lease.ttl_secs)
        .lease_renew_interval(shared.lease.renew_interval())
        .lease_follower_poll_interval(shared.lease.poll_interval());

    if let Some(ref endpoint) = shared.s3.endpoint {
        builder = builder.endpoint(endpoint);
    }
    if let Some(ref secret) = serve.secret {
        builder = builder.secret(secret);
    }

    // If NATS URL is set, try to use NATS for leases (faster than S3).
    // Falls back to S3 leases if NATS connection fails.
    let mut lease_store_configured = false;
    #[cfg(feature = "nats-lease")]
    if let Ok(nats_url) = std::env::var("WAL_LEASE_NATS_URL") {
        match hadb_lease_nats::NatsLeaseStore::connect(&nats_url, "hadb-leases").await {
            Ok(store) => {
                info!(url = %nats_url, "using NATS lease store");
                builder = builder.lease_store(std::sync::Arc::new(store));
                lease_store_configured = true;
            }
            Err(e) => {
                error!(url = %nats_url, error = %e, "NATS lease store connection failed, falling back to S3 leases");
            }
        }
    }

    // Phase Lucid: builder no longer reads HAQLITE_LEASE_URL implicitly.
    // Default to S3 lease store using the CLI's bucket/endpoint.
    if !lease_store_configured {
        let mut s3_cfg = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(ep) = shared.s3.endpoint.as_deref() {
            s3_cfg = s3_cfg.endpoint_url(ep);
        }
        let cfg = s3_cfg.load().await;
        let mut s3b = aws_sdk_s3::config::Builder::from(&cfg).force_path_style(true);
        if let Some(ep) = shared.s3.endpoint.as_deref() {
            s3b = s3b.endpoint_url(ep);
        }
        let client = aws_sdk_s3::Client::from_conf(s3b.build());
        let lease: std::sync::Arc<dyn hadb::LeaseStore> =
            std::sync::Arc::new(crate::S3LeaseStore::new(client, shared.s3.bucket.clone()));
        info!(
            "Phase Lucid: using S3 lease store from CLI args (bucket={})",
            shared.s3.bucket
        );
        builder = builder.lease_store(lease);
    }

    // Phase Lucid: explicit walrust storage. Builder no longer
    // falls back to S3Storage::from_env for either.
    let walrust_storage = std::sync::Arc::new(
        hadb_storage_s3::S3Storage::from_env(
            shared.s3.bucket.clone(),
            shared.s3.endpoint.as_deref(),
        )
        .await?,
    );
    builder = builder.walrust_storage(walrust_storage);

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| anyhow!("db_path is not valid UTF-8"))?;

    let db = Arc::new(builder.open(db_path_str, schema).await?);

    let role = db.role().unwrap_or(Role::Follower);
    info!(
        instance = %instance_id,
        role = %role,
        db = %db_path.display(),
        port = serve.port,
        forwarding_port = serve.forwarding_port,
        "haqlite server started"
    );

    let state = Arc::new(AppState {
        db: db.clone(),
        secret: serve.secret.clone(),
    });

    // Build hrana protocol router (libSQL HTTP API).
    let hrana_router =
        crate::hrana::build_hrana_router(db.clone(), serve.db_path.clone(), serve.secret.clone());

    // HTTP API server.
    // /health is unauthenticated (load balancer probes).
    // /status and /metrics require auth when a secret is configured.
    // /v2/pipeline, /v3/pipeline, /v3/cursor use hrana's own auth.
    let app = build_router(state, hrana_router);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", serve.port)).await?;
    info!(port = serve.port, "HTTP API listening");

    let server = axum::serve(listener, app);

    // Wait for shutdown.
    tokio::select! {
        _ = hadb_cli::shutdown_signal() => {}
        result = server => {
            if let Err(e) = result {
                error!("HTTP server error: {e}");
            }
        }
    }

    info!("shutting down");

    match Arc::try_unwrap(db) {
        Ok(mut db) => {
            if let Err(e) = db.close().await {
                error!("failed to close haqlite: {e}");
            }
        }
        Err(_) => {
            error!("could not unwrap Arc<HaQLite> for clean shutdown");
        }
    }

    info!("goodbye");
    Ok(())
}

// ============================================================================
// Auth
// ============================================================================

/// Check bearer token auth. Returns Ok(()) if no secret configured or token matches.
fn check_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let secret = match &state.secret {
        Some(s) => s,
        None => return Ok(()),
    };

    let header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| error_response(StatusCode::UNAUTHORIZED, "missing Authorization header"))?;

    let token = header.strip_prefix("Bearer ").ok_or_else(|| {
        error_response(
            StatusCode::UNAUTHORIZED,
            "invalid Authorization format (expected 'Bearer <token>')",
        )
    })?;

    if token != secret {
        return Err(error_response(StatusCode::UNAUTHORIZED, "invalid token"));
    }

    Ok(())
}

/// Build a JSON error response.
fn error_response(status: StatusCode, message: &str) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({ "error": message })))
}

// ============================================================================
// HTTP handlers
// ============================================================================

/// GET /health — liveness check (no auth).
async fn handle_health() -> StatusCode {
    StatusCode::OK
}

/// GET /status — role and basic info.
async fn handle_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;
    let role = state.db.role().unwrap_or(Role::Follower);
    Ok(Json(serde_json::json!({
        "role": format!("{role}"),
        "status": "ok",
    })))
}

/// GET /metrics — HA coordination metrics snapshot.
async fn handle_metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;
    match state.db.coordinator() {
        Some(c) => {
            let snap = c.metrics().snapshot();
            let value = serde_json::to_value(snap).map_err(|e| {
                error!("metrics serialization failed: {e}");
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "metrics serialization failed",
                )
            })?;
            Ok(Json(value))
        }
        None => Ok(Json(serde_json::json!({}))),
    }
}
