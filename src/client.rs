//! HaQLiteClient: stateless client that discovers the leader from S3 and forwards
//! reads and writes over HTTP. No local SQLite, no cluster join.
//!
//! ```ignore
//! let client = HaQLiteClient::new("my-bucket").await?;
//!
//! client.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Alice".into())]).await?;
//! let row = client.query_row("SELECT COUNT(*) FROM users", &[]).await?;
//! let count = row[0].as_integer().unwrap();
//! ```

use std::sync::RwLock;
use std::time::Duration;

use anyhow::{anyhow, Result};
use hadb::{DbLease, LeaseStore};
use hadb_s3::S3LeaseStore;

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
        let lease_store: std::sync::Arc<dyn LeaseStore> =
            std::sync::Arc::new(S3LeaseStore::new(s3_client, self.bucket.clone()));

        let http_client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()?;

        // Discover the leader.
        let leader_addr = discover_leader(&lease_store, &self.prefix, &self.db_name).await?;

        Ok(HaQLiteClient {
            lease_store,
            prefix: self.prefix,
            db_name: self.db_name,
            http_client,
            leader_address: RwLock::new(leader_addr),
            secret: self.secret,
        })
    }
}

/// Stateless client for an HaQLite cluster.
///
/// Discovers the leader from S3 and forwards reads/writes over HTTP.
/// On connection failure, re-discovers the leader automatically.
pub struct HaQLiteClient {
    lease_store: std::sync::Arc<dyn LeaseStore>,
    prefix: String,
    db_name: String,
    http_client: reqwest::Client,
    leader_address: RwLock<String>,
    secret: Option<String>,
}

impl HaQLiteClient {
    /// Start building a client connection. Only the S3 bucket is required.
    pub fn new(bucket: &str) -> HaQLiteClientBuilder {
        HaQLiteClientBuilder::new(bucket)
    }

    /// Execute a write statement on the leader. Returns rows affected.
    pub async fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        let body = forwarding::ForwardedExecute {
            sql: sql.to_string(),
            params: params.to_vec(),
        };

        let result = self
            .post_with_retry("/haqlite/execute", &body)
            .await?;

        let exec_result: forwarding::ExecuteResult = serde_json::from_value(result)?;
        Ok(exec_result.rows_affected)
    }

    /// Query a single row from the leader. Returns column values as SqlValues.
    pub async fn query_row(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlValue>> {
        let body = forwarding::ForwardedQuery {
            sql: sql.to_string(),
            params: params.to_vec(),
        };

        let result = self
            .post_with_retry("/haqlite/query", &body)
            .await?;

        let query_result: forwarding::QueryResult = serde_json::from_value(result)?;
        Ok(query_result.values)
    }

    /// Get the current leader address (cached — may be stale).
    pub fn leader_address(&self) -> String {
        self.leader_address.read().unwrap().clone()
    }

    /// Force re-discovery of the leader from S3.
    pub async fn refresh_leader(&self) -> Result<()> {
        let addr = discover_leader(&self.lease_store, &self.prefix, &self.db_name).await?;
        *self.leader_address.write().unwrap() = addr;
        Ok(())
    }

    // ========================================================================
    // Internal
    // ========================================================================

    /// POST to the leader with automatic retry on connection failure.
    /// On failure, re-discovers the leader from S3 and retries once.
    async fn post_with_retry<T: serde::Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<serde_json::Value> {
        let addr = self.leader_address.read().unwrap().clone();
        let url = format!("{}{}", addr, path);

        match self.do_post(&url, body).await {
            Ok(val) => Ok(val),
            Err(first_err) => {
                // Re-discover leader and retry once.
                tracing::warn!(
                    "Request to {} failed ({}), re-discovering leader...",
                    url,
                    first_err
                );
                match discover_leader(&self.lease_store, &self.prefix, &self.db_name).await {
                    Ok(new_addr) => {
                        *self.leader_address.write().unwrap() = new_addr.clone();
                        let retry_url = format!("{}{}", new_addr, path);
                        self.do_post(&retry_url, body).await
                    }
                    Err(discover_err) => {
                        Err(anyhow!(
                            "Request failed ({}) and leader re-discovery also failed ({})",
                            first_err,
                            discover_err
                        ))
                    }
                }
            }
        }
    }

    async fn do_post<T: serde::Serialize>(
        &self,
        url: &str,
        body: &T,
    ) -> Result<serde_json::Value> {
        let mut req = self.http_client.post(url).json(body);
        if let Some(ref secret) = self.secret {
            req = req.bearer_auth(secret);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| anyhow!("Request to {} failed: {}", url, e))?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Leader returned error: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ));
        }

        resp.json()
            .await
            .map_err(|e| anyhow!("Failed to parse response: {}", e))
    }
}

/// Discover the leader's forwarding address by reading the lease from S3.
async fn discover_leader(
    lease_store: &std::sync::Arc<dyn LeaseStore>,
    prefix: &str,
    db_name: &str,
) -> Result<String> {
    let lease = DbLease::new(
        lease_store.clone(),
        prefix,
        db_name,
        "client", // dummy — we never claim
        "client",
        5,
    );

    match lease.read().await? {
        Some((data, _etag)) => {
            if data.sleeping {
                Err(anyhow!("Cluster is sleeping — no active leader"))
            } else if data.is_expired() {
                Err(anyhow!("Leader lease is expired — no active leader"))
            } else if data.address.is_empty() {
                Err(anyhow!("Leader lease has no address"))
            } else {
                Ok(data.address)
            }
        }
        None => Err(anyhow!("No lease found — cluster not initialized")),
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
