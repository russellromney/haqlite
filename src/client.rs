//! HaQLiteClient: SQLite-specific HA client wrapping hadb's generic HaClient.
//!
//! Adds read-replica support (local SQLite reads) and typed SQL operations
//! on top of HaClient's leader discovery and HTTP forwarding.
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! use haqlite::{HaQLiteClient, SqlValue};
//!
//! // Full client: discovers leader, forwards writes, queries leader
//! let client = HaQLiteClient::new("my-bucket")
//!     .db_name("mydb")
//!     .connect()
//!     .await?;
//! client.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Alice".into())]).await?;
//! let row = client.query_row("SELECT COUNT(*) FROM users", &[]).await?;
//!
//! // Read-replica client: reads from local follower DB, no leader needed
//! let reader = HaQLiteClient::read_replica_only("/data/mydb.db");
//! let row = reader.query_row("SELECT COUNT(*) FROM users", &[]).await?;
//! # Ok(())
//! # }
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use hadb::{HaClient, LeaseStore};
use hadb_lease_s3::S3LeaseStore;

use crate::forwarding::{self, SqlValue};

const DEFAULT_PREFIX: &str = "haqlite/";
const DEFAULT_DB_NAME: &str = "db";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Builder for creating a client connection to an HaQLite cluster.
pub struct HaQLiteClientBuilder {
    bucket: String,
    prefix: String,
    endpoint: Option<String>,
    db_name: String,
    timeout: Duration,
    secret: Option<String>,
    read_replica_path: Option<PathBuf>,
}

impl HaQLiteClientBuilder {
    fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: DEFAULT_PREFIX.to_string(),
            endpoint: None,
            db_name: DEFAULT_DB_NAME.to_string(),
            timeout: DEFAULT_TIMEOUT,
            secret: None,
            read_replica_path: None,
        }
    }

    /// S3 key prefix. Default: "haqlite/". Must match the server's prefix.
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    /// S3 endpoint URL (for Tigris, MinIO, R2, etc).
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(endpoint.to_string());
        self
    }

    /// Database name to connect to. Default: "db".
    /// Must match the file stem of the server's db_path.
    pub fn db_name(mut self, name: &str) -> Self {
        self.db_name = name.to_string();
        self
    }

    /// Request timeout. Default: 5s.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Shared secret for authenticating with the forwarding server.
    /// Must match the secret configured on the HaQLite server.
    pub fn secret(mut self, secret: &str) -> Self {
        self.secret = Some(secret.to_string());
        self
    }

    /// Enable read-replica mode: `query_row()` reads from this local DB path
    /// instead of forwarding to the leader. `execute()` still forwards writes.
    ///
    /// The local DB must be a follower that's being replicated via walrust.
    /// Each read opens a fresh connection (no pooling) so external WAL
    /// applies are always visible.
    pub fn read_replica(mut self, db_path: &str) -> Self {
        self.read_replica_path = Some(PathBuf::from(db_path));
        self
    }

    /// Connect to the HaQLite cluster by discovering the leader from S3.
    pub async fn connect(self) -> Result<HaQLiteClient> {
        // Build S3 client for lease reading.
        let s3_config = match &self.endpoint {
            Some(endpoint) => {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .endpoint_url(endpoint)
                    .load()
                    .await
            }
            None => {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .load()
                    .await
            }
        };
        let s3_client = aws_sdk_s3::Client::new(&s3_config);
        let lease_store: Arc<dyn LeaseStore> =
            Arc::new(S3LeaseStore::new(s3_client, self.bucket.clone()));

        let mut ha_builder = HaClient::builder()
            .lease_store(lease_store)
            .prefix(&self.prefix)
            .db_name(&self.db_name)
            .timeout(self.timeout);

        if let Some(ref secret) = self.secret {
            ha_builder = ha_builder.secret(secret);
        }

        let ha_client = ha_builder.connect().await?;

        Ok(HaQLiteClient {
            ha_client,
            read_replica_path: self.read_replica_path,
        })
    }
}

/// SQLite-specific HA client.
///
/// Wraps hadb's generic `HaClient` for leader discovery and HTTP forwarding,
/// adding typed SQL operations and optional read-replica support.
///
/// In read-replica mode, `query_row()` reads from a local follower DB
/// (fresh connection per call — no pooling), while `execute()` still
/// forwards writes to the leader.
pub struct HaQLiteClient {
    ha_client: HaClient,
    read_replica_path: Option<PathBuf>,
}

impl HaQLiteClient {
    /// Start building a client connection. Only the S3 bucket is required.
    pub fn new(bucket: &str) -> HaQLiteClientBuilder {
        HaQLiteClientBuilder::new(bucket)
    }

    /// Create a read-replica-only client that reads from a local DB file.
    ///
    /// No leader discovery or S3 access. `execute()` returns an error.
    /// `query_row()` opens a fresh read-only connection per call.
    pub fn read_replica_only(db_path: &str) -> Self {
        Self {
            ha_client: HaClient::disconnected(),
            read_replica_path: Some(PathBuf::from(db_path)),
        }
    }

    /// Returns true if this client reads from a local replica DB.
    pub fn is_read_replica(&self) -> bool {
        self.read_replica_path.is_some()
    }

    /// Execute a write statement on the leader. Returns rows affected.
    ///
    /// Always forwards to the leader, even in read-replica mode.
    /// Returns an error if no leader is available (e.g. `read_replica_only`).
    pub async fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        let body = forwarding::ForwardedExecute {
            sql: sql.to_string(),
            params: params.to_vec(),
        };

        let result: forwarding::ExecuteResult = self
            .ha_client
            .forward("/haqlite/execute", &body)
            .await?;

        Ok(result.rows_affected)
    }

    /// Query and return all matching rows.
    ///
    /// If a read-replica path is configured, reads from the local DB
    /// (fresh connection per call). Otherwise forwards to the leader.
    pub async fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<Vec<SqlValue>>> {
        if let Some(ref db_path) = self.read_replica_path {
            return self.query_local(db_path, sql, params);
        }

        let body = forwarding::ForwardedQuery {
            sql: sql.to_string(),
            params: params.to_vec(),
        };

        let result: forwarding::QueryResult = self
            .ha_client
            .forward("/haqlite/query", &body)
            .await?;

        Ok(result.rows)
    }

    /// Query a single row. Returns column values as SqlValues.
    ///
    /// Returns an error if the query returns zero rows.
    pub async fn query_row(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlValue>> {
        let rows = self.query(sql, params).await?;
        rows.into_iter()
            .next()
            .ok_or_else(|| anyhow!("query returned no rows"))
    }

    /// Get the current leader address (cached — may be stale).
    pub fn leader_address(&self) -> String {
        self.ha_client.leader_address()
    }

    /// Force re-discovery of the leader from S3.
    pub async fn refresh_leader(&self) -> Result<()> {
        self.ha_client.refresh_leader().await
    }

    // ========================================================================
    // Internal
    // ========================================================================

    /// Read from the local follower DB with a fresh connection.
    ///
    /// Opens a new read-only rusqlite::Connection per call — no pooling.
    /// This ensures external WAL applies (by walrust) are always visible.
    fn query_local(
        &self,
        db_path: &PathBuf,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<Vec<Vec<SqlValue>>> {
        let conn = rusqlite::Connection::open_with_flags(
            db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| anyhow!("Failed to open read-replica DB: {}", e))?;

        let rusqlite_params: Vec<rusqlite::types::Value> =
            params.iter().map(|p| p.to_rusqlite()).collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            rusqlite_params
                .iter()
                .map(|p| p as &dyn rusqlite::types::ToSql)
                .collect();

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| anyhow!("Failed to prepare query: {}", e))?;

        let column_count = stmt.column_count();
        let mut rows_iter = stmt
            .query(param_refs.as_slice())
            .map_err(|e| anyhow!("Query failed: {}", e))?;
        let mut rows = Vec::new();
        while let Some(row) = rows_iter
            .next()
            .map_err(|e| anyhow!("Row iteration failed: {}", e))?
        {
            let mut vals = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let val: rusqlite::types::Value = row
                    .get(i)
                    .map_err(|e| anyhow!("Column {i} read failed: {}", e))?;
                vals.push(SqlValue::from_rusqlite(val));
            }
            rows.push(vals);
        }

        Ok(rows)
    }
}

impl SqlValue {
    /// Extract an integer value, or None if not an Integer.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            SqlValue::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Extract a text value, or None if not a Text.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            SqlValue::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Extract a real value, or None if not a Real.
    pub fn as_real(&self) -> Option<f64> {
        match self {
            SqlValue::Real(f) => Some(*f),
            _ => None,
        }
    }

    /// Returns true if this value is Null.
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }
}
