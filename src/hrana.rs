//! Hrana protocol integration for haqlite.
//!
//! Provides libSQL HTTP API compatibility via the hrana-server crate.
//! Endpoints: POST /v2/pipeline, POST /v3/pipeline, POST /v3/cursor.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Json;

use hadb::Role;
use hrana_server::{
    handle_cursor, handle_pipeline, ConnectionInfo, CursorRequest, CursorResponse, HranaBackend,
    HranaConfig, HranaError, PipelineError, PipelineRequest, PipelineResponse, SessionManager,
};

use crate::database::HaQLite;

/// Shared state for hrana HTTP handlers.
pub struct HranaState {
    backend: HaqliteHranaBackend,
    sessions: SessionManager,
    config: HranaConfig,
}

/// Hrana backend implementation for haqlite.
///
/// Single-database per process. Each hrana session gets its own SQLite connection:
/// - Leaders: read-write connection (WAL mode)
/// - Followers: read-only connection (sees latest walrust LTX state)
///
/// Write forwarding is not supported through hrana — followers reject writes
/// via `is_writable()`. Use haqlite's native `execute()` for write forwarding.
pub struct HaqliteHranaBackend {
    db_path: PathBuf,
    db: Arc<HaQLite>,
    secret: Option<String>,
}

impl HranaBackend for HaqliteHranaBackend {
    fn authenticate(&self, token: &str) -> Result<String, HranaError> {
        match &self.secret {
            None => Ok("default".to_string()),
            Some(secret) => {
                if token == secret {
                    Ok("default".to_string())
                } else {
                    Err(HranaError {
                        message: "Invalid authentication token".to_string(),
                        code: "AUTH_FAILED".to_string(),
                    })
                }
            }
        }
    }

    fn connection(&self, _database_id: &str) -> Result<ConnectionInfo, HranaError> {
        let role = self.db.role();
        let flags = match role {
            Some(Role::Leader) | None => {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            }
            Some(Role::Follower) => {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            }
        };

        let conn = rusqlite::Connection::open_with_flags(&self.db_path, flags).map_err(|e| {
            HranaError {
                message: format!("Failed to open connection: {e}"),
                code: "INTERNAL".to_string(),
            }
        })?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .map_err(|e| HranaError {
                message: format!("PRAGMA journal_mode failed: {e}"),
                code: "INTERNAL".to_string(),
            })?;

        Ok(ConnectionInfo {
            conn: Arc::new(Mutex::new(conn)),
            query_timer: None,
        })
    }

    fn is_writable(&self, _database_id: &str) -> bool {
        matches!(self.db.role(), Some(Role::Leader) | None)
    }
}

/// Build the axum router for hrana protocol endpoints.
///
/// Returns a `Router<()>` (state already applied) suitable for merging
/// into the main application router.
pub fn build_hrana_router(
    db: Arc<HaQLite>,
    db_path: PathBuf,
    secret: Option<String>,
) -> axum::Router {
    let config = HranaConfig::default();
    let state = Arc::new(HranaState {
        backend: HaqliteHranaBackend {
            db_path,
            db,
            secret,
        },
        sessions: SessionManager::new(&config),
        config,
    });

    axum::Router::new()
        .route("/v2/pipeline", post(handle_hrana_pipeline))
        .route("/v3/pipeline", post(handle_hrana_pipeline))
        .route("/v3/cursor", post(handle_hrana_cursor))
        .with_state(state)
}

// ============================================================================
// Axum handlers
// ============================================================================

async fn handle_hrana_pipeline(
    State(state): State<Arc<HranaState>>,
    headers: HeaderMap,
    Json(request): Json<PipelineRequest>,
) -> Result<Json<PipelineResponse>, (StatusCode, Json<serde_json::Value>)> {
    let token = extract_bearer_token(&headers);
    match handle_pipeline(
        &state.backend,
        &state.sessions,
        &state.config,
        token,
        request,
    ) {
        Ok(response) => Ok(Json(response)),
        Err(e) => Err(map_pipeline_error(e)),
    }
}

async fn handle_hrana_cursor(
    State(state): State<Arc<HranaState>>,
    headers: HeaderMap,
    Json(request): Json<CursorRequest>,
) -> Result<
    (
        StatusCode,
        [(axum::http::header::HeaderName, &'static str); 1],
        String,
    ),
    (StatusCode, Json<serde_json::Value>),
> {
    let token = extract_bearer_token(&headers);
    match handle_cursor(
        &state.backend,
        &state.sessions,
        &state.config,
        token,
        request,
    ) {
        Ok(CursorResponse { ndjson }) => Ok((
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
            ndjson,
        )),
        Err(e) => Err(map_pipeline_error(e)),
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn extract_bearer_token(headers: &HeaderMap) -> &str {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("")
}

fn map_pipeline_error(err: PipelineError) -> (StatusCode, Json<serde_json::Value>) {
    let (status, message) = match &err {
        PipelineError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
        PipelineError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
        PipelineError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        PipelineError::ReadOnly(msg) => (StatusCode::FORBIDDEN, msg.clone()),
    };
    (status, Json(serde_json::json!({ "error": message })))
}
