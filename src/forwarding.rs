//! Write and query forwarding types and handlers for inter-node communication.
//!
//! Followers and clients forward SQL to the leader's internal HTTP server.
//! The leader executes locally and returns the result.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::database::HaQLiteInner;
use hadb::Role;

/// SQLite value for serialization across the wire.
///
/// Used in `execute()` params — covers all SQLite types.
/// For reads (`query_row`), use rusqlite's `&[&dyn ToSql]` directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqlValue {
    /// Convert to rusqlite's Value type for local execution.
    pub fn to_rusqlite(&self) -> rusqlite::types::Value {
        match self {
            SqlValue::Null => rusqlite::types::Value::Null,
            SqlValue::Integer(i) => rusqlite::types::Value::Integer(*i),
            SqlValue::Real(f) => rusqlite::types::Value::Real(*f),
            SqlValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
            SqlValue::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
        }
    }

    /// Convert from rusqlite's Value type.
    pub fn from_rusqlite(val: rusqlite::types::Value) -> Self {
        match val {
            rusqlite::types::Value::Null => SqlValue::Null,
            rusqlite::types::Value::Integer(n) => SqlValue::Integer(n),
            rusqlite::types::Value::Real(f) => SqlValue::Real(f),
            rusqlite::types::Value::Text(s) => SqlValue::Text(s),
            rusqlite::types::Value::Blob(b) => SqlValue::Blob(b),
        }
    }
}

/// Request body for forwarded execute calls.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ForwardedExecute {
    pub sql: String,
    pub params: Vec<SqlValue>,
}

/// Response body for forwarded execute calls.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ExecuteResult {
    pub rows_affected: u64,
}

/// Shared state for the forwarding HTTP server.
pub(crate) struct ForwardingState {
    pub inner: Arc<HaQLiteInner>,
}

/// Check the `Authorization: Bearer <token>` header against the configured secret.
/// Returns Ok(()) if no secret is configured or if the token matches.
fn check_auth(state: &ForwardingState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let secret = match &state.inner.secret {
        Some(s) => s,
        None => return Ok(()), // no auth configured
    };
    let header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let token = header.strip_prefix("Bearer ").ok_or(StatusCode::UNAUTHORIZED)?;
    if token != secret {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

/// Handler for `POST /haqlite/execute` — receives forwarded writes from followers.
pub(crate) async fn handle_forwarded_execute(
    State(state): State<Arc<ForwardingState>>,
    headers: HeaderMap,
    Json(req): Json<ForwardedExecute>,
) -> Result<Json<ExecuteResult>, StatusCode> {
    check_auth(&state, &headers)?;

    // Only the leader should execute writes.
    let role = state.inner.current_role();
    if role != Some(Role::Leader) {
        return Err(StatusCode::MISDIRECTED_REQUEST);
    }

    let conn_arc = state
        .inner
        .get_conn()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let conn = conn_arc.lock().unwrap();

    let params: Vec<rusqlite::types::Value> = req.params.iter().map(|p| p.to_rusqlite()).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    let rows_affected = conn
        .execute(&req.sql, param_refs.as_slice())
        .map_err(|e| {
            tracing::error!("Forwarded execute failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(ExecuteResult {
        rows_affected: rows_affected as u64,
    }))
}

/// Request body for forwarded query calls.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ForwardedQuery {
    pub sql: String,
    pub params: Vec<SqlValue>,
}

/// Response body for forwarded query calls — returns all matching rows.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

/// Handler for `POST /haqlite/query` — receives forwarded reads from clients.
pub(crate) async fn handle_forwarded_query(
    State(state): State<Arc<ForwardingState>>,
    headers: HeaderMap,
    Json(req): Json<ForwardedQuery>,
) -> Result<Json<QueryResult>, StatusCode> {
    check_auth(&state, &headers)?;

    let conn_arc = state
        .inner
        .get_conn()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let conn = conn_arc.lock().unwrap();

    let params: Vec<rusqlite::types::Value> = req.params.iter().map(|p| p.to_rusqlite()).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    let mut stmt = conn.prepare(&req.sql).map_err(|e| {
        tracing::error!("Forwarded query prepare failed: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let column_count = stmt.column_count();
    let columns: Vec<String> = (0..column_count)
        .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
        .collect();

    let mut rows_iter = stmt.query(param_refs.as_slice()).map_err(|e| {
        tracing::error!("Forwarded query failed: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let mut rows = Vec::new();
    while let Some(row) = rows_iter.next().map_err(|e| {
        tracing::error!("Forwarded query row iteration failed: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })? {
        let mut vals = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let val: rusqlite::types::Value = row.get(i).map_err(|e| {
                tracing::error!("Forwarded query column read failed: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
            vals.push(SqlValue::from_rusqlite(val));
        }
        rows.push(vals);
    }

    Ok(Json(QueryResult { columns, rows }))
}
