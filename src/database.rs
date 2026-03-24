//! HaQLite: dead-simple embedded HA SQLite.
//!
//! ```ignore
//! let db = HaQLite::builder("my-bucket")
//!     .open("/data/my.db", "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
//!     .await?;
//!
//! db.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Alice".into())]).await?;
//! let count: i64 = db.query_row("SELECT COUNT(*) FROM users", &[], |r| r.get(0))?;
//! ```

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::routing::post;
use hadb::{CoordinatorConfig, Coordinator, LeaseConfig, Role, RoleEvent};
use hadb_lease_s3::S3LeaseStore;

use crate::follower_behavior::SqliteFollowerBehavior;
use crate::forwarding::{self, ForwardingState, SqlValue};
use crate::replicator::SqliteReplicator;

const DEFAULT_PREFIX: &str = "haqlite/";
const DEFAULT_FORWARDING_PORT: u16 = 18080;
const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

const ROLE_LEADER: u8 = 0;
const ROLE_FOLLOWER: u8 = 1;

/// Builder for creating an HA SQLite instance.
///
/// Only the bucket is required — everything else has sensible defaults.
///
/// ```ignore
/// let db = HaQLite::builder("my-bucket")
///     .prefix("myapp/")
///     .forwarding_port(19000)
///     .open("/data/my.db", "CREATE TABLE IF NOT EXISTS ...")
///     .await?;
/// ```
pub struct HaQLiteBuilder {
    bucket: String,
    prefix: String,
    endpoint: Option<String>,
    instance_id: Option<String>,
    address: Option<String>,
    forwarding_port: u16,
    forward_timeout: Duration,
    coordinator_config: Option<CoordinatorConfig>,
    secret: Option<String>,
}

impl HaQLiteBuilder {
    fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: DEFAULT_PREFIX.to_string(),
            endpoint: None,
            instance_id: None,
            address: None,
            forwarding_port: DEFAULT_FORWARDING_PORT,
            forward_timeout: DEFAULT_FORWARD_TIMEOUT,
            coordinator_config: None,
            secret: None,
        }
    }

    /// S3 key prefix for all haqlite data. Default: "haqlite/".
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    /// S3 endpoint URL (for Tigris, MinIO, R2, etc).
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(endpoint.to_string());
        self
    }

    /// Unique instance ID for this node. Default: FLY_MACHINE_ID env or UUID.
    pub fn instance_id(mut self, id: &str) -> Self {
        self.instance_id = Some(id.to_string());
        self
    }

    /// Network address for this node (how other nodes reach the forwarding server).
    /// Default: auto-detected from Fly internal DNS or hostname.
    pub fn address(mut self, addr: &str) -> Self {
        self.address = Some(addr.to_string());
        self
    }

    /// Port for the internal write-forwarding HTTP server. Default: 18080.
    pub fn forwarding_port(mut self, port: u16) -> Self {
        self.forwarding_port = port;
        self
    }

    /// Timeout for forwarded write requests. Default: 5s.
    pub fn forward_timeout(mut self, timeout: Duration) -> Self {
        self.forward_timeout = timeout;
        self
    }

    /// Override the coordinator config (lease timing, sync interval, etc).
    pub fn coordinator_config(mut self, config: CoordinatorConfig) -> Self {
        self.coordinator_config = Some(config);
        self
    }

    /// Shared secret for authenticating forwarding requests.
    /// When set, the forwarding server rejects requests without a matching
    /// `Authorization: Bearer <secret>` header.
    pub fn secret(mut self, secret: &str) -> Self {
        self.secret = Some(secret.to_string());
        self
    }


    /// Open the database and join the HA cluster.
    ///
    /// `schema` is run once on first open (e.g. CREATE TABLE IF NOT EXISTS ...).
    pub async fn open(self, db_path: &str, schema: &str) -> Result<HaQLite> {
        let db_path = PathBuf::from(db_path);
        let db_name = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("db")
            .to_string();

        // Auto-detect instance_id and address.
        let instance_id = self.instance_id.unwrap_or_else(|| {
            std::env::var("FLY_MACHINE_ID")
                .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
        });
        let address = self.address.unwrap_or_else(|| {
            detect_address(&instance_id, self.forwarding_port)
        });

        // Build AWS S3 config and client.
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

        // Build walrust S3 storage backend (for SQLite WAL operations).
        let walrust_storage: Arc<dyn walrust::StorageBackend> = Arc::new(
            walrust::S3Backend::from_env(self.bucket.clone(), self.endpoint.as_deref()).await?,
        );

        // Build S3 lease store (for CAS leases).
        let lease_store = Arc::new(S3LeaseStore::new(s3_client, self.bucket.clone()));

        // Build coordinator config.
        let mut config = self.coordinator_config.unwrap_or_default();
        config.lease = Some(LeaseConfig::new(instance_id.clone(), address.clone()));

        // Build SQLite-specific components.
        let replication_config = walrust::ReplicationConfig {
            sync_interval: config.sync_interval,
            snapshot_interval: config.snapshot_interval,
            ..Default::default()
        };
        let replicator = Arc::new(SqliteReplicator::new(
            walrust_storage.clone(),
            &self.prefix,
            replication_config,
        ));
        let follower_behavior = Arc::new(SqliteFollowerBehavior::new(walrust_storage));

        // Build hadb Coordinator.
        let coordinator = Coordinator::new(
            replicator,
            Some(lease_store as Arc<dyn hadb::LeaseStore>),
            None, // node_registry
            follower_behavior,
            &self.prefix,
            config,
        );

        open_with_coordinator(
            coordinator,
            db_path,
            &db_name,
            schema,
            &address,
            self.forwarding_port,
            self.forward_timeout,
            self.secret,
        )
        .await
    }
}

/// HA SQLite database — transparent write forwarding, local reads, automatic failover.
///
/// Create via `HaQLite::builder("bucket").open(path, schema).await?`
/// or `HaQLite::local(path, schema)?` for single-node mode.
pub struct HaQLite {
    inner: Arc<HaQLiteInner>,
    _fwd_handle: tokio::task::JoinHandle<()>,
    _role_handle: tokio::task::JoinHandle<()>,
}

/// Internal state shared between HaQLite, forwarding handler, and role listener.
///
/// Uses std::sync primitives so `query_row()` works without async runtime.
pub(crate) struct HaQLiteInner {
    pub(crate) coordinator: Option<Arc<Coordinator>>,
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    /// Cached role — updated atomically by the role event listener.
    role: AtomicU8,
    /// Read-write connection when leader, None when follower.
    pub(crate) conn: RwLock<Option<Arc<Mutex<rusqlite::Connection>>>>,
    /// Leader's forwarding address (read from S3 lease, updated on role change).
    leader_address: RwLock<String>,
    pub(crate) http_client: reqwest::Client,
    /// Shared secret for authenticating forwarding requests.
    pub(crate) secret: Option<String>,
}

impl HaQLiteInner {
    /// Get current role (lock-free atomic read).
    pub(crate) fn current_role(&self) -> Option<Role> {
        match self.role.load(Ordering::SeqCst) {
            ROLE_LEADER => Some(Role::Leader),
            ROLE_FOLLOWER => Some(Role::Follower),
            _ => None,
        }
    }

    fn set_role(&self, role: Role) {
        self.role.store(
            match role {
                Role::Leader => ROLE_LEADER,
                Role::Follower => ROLE_FOLLOWER,
            },
            Ordering::SeqCst,
        );
    }

    fn leader_addr(&self) -> String {
        self.leader_address.read().unwrap().clone()
    }

    fn set_leader_addr(&self, addr: String) {
        *self.leader_address.write().unwrap() = addr;
    }

    fn set_conn(&self, conn: Option<Arc<Mutex<rusqlite::Connection>>>) {
        *self.conn.write().unwrap() = conn;
    }

    pub(crate) fn get_conn(&self) -> Option<Arc<Mutex<rusqlite::Connection>>> {
        self.conn.read().unwrap().clone()
    }
}

impl HaQLite {
    /// Start building an HA SQLite instance. Only the S3 bucket is required.
    pub fn builder(bucket: &str) -> HaQLiteBuilder {
        HaQLiteBuilder::new(bucket)
    }

    /// Open a local-only SQLite database (no HA, no S3).
    ///
    /// Same `execute()`/`query_row()` API. Useful for development and testing.
    pub fn local(db_path: &str, schema: &str) -> Result<HaQLite> {
        let db_path = PathBuf::from(db_path);
        ensure_schema(&db_path, schema)?;

        let conn = open_leader_connection(&db_path)?;

        let inner = Arc::new(HaQLiteInner {
            coordinator: None,
            db_name: db_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("db")
                .to_string(),
            db_path,
            role: AtomicU8::new(ROLE_LEADER),
            conn: RwLock::new(Some(Arc::new(Mutex::new(conn)))),
            leader_address: RwLock::new(String::new()),
            http_client: reqwest::Client::new(),
            secret: None,
        });

        // No forwarding server or role listener in local mode.
        let fwd_handle = tokio::spawn(async {});
        let role_handle = tokio::spawn(async {});

        Ok(HaQLite {
            inner,
            _fwd_handle: fwd_handle,
            _role_handle: role_handle,
        })
    }

    /// Create an HaQLite instance from a pre-built Coordinator.
    ///
    /// For tests and advanced use cases where you build the Coordinator yourself.
    /// Handles schema creation, join, connection lifecycle, and forwarding server.
    pub async fn from_coordinator(
        coordinator: Arc<Coordinator>,
        db_path: &str,
        schema: &str,
        forwarding_port: u16,
        forward_timeout: Duration,
    ) -> Result<HaQLite> {
        Self::from_coordinator_with_secret(
            coordinator, db_path, schema, forwarding_port, forward_timeout, None,
        )
        .await
    }

    /// Like `from_coordinator`, but with an optional shared secret for auth.
    pub async fn from_coordinator_with_secret(
        coordinator: Arc<Coordinator>,
        db_path: &str,
        schema: &str,
        forwarding_port: u16,
        forward_timeout: Duration,
        secret: Option<String>,
    ) -> Result<HaQLite> {
        let db_path = PathBuf::from(db_path);
        let db_name = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("db")
            .to_string();
        let address = format!("http://localhost:{}", forwarding_port);

        open_with_coordinator(
            coordinator,
            db_path,
            &db_name,
            schema,
            &address,
            forwarding_port,
            forward_timeout,
            secret,
        )
        .await
    }

    /// Execute a write statement. Returns rows affected.
    ///
    /// On the leader: executes locally.
    /// On a follower: forwards to the leader via HTTP.
    pub async fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        let role = self.inner.current_role();

        match role {
            Some(Role::Leader) | None => self.execute_local(sql, params),
            Some(Role::Follower) => self.execute_forwarded(sql, params).await,
        }
    }

    /// Query a single row. Always executes locally.
    ///
    /// On the leader: uses the persistent rw connection.
    /// On a follower: opens a fresh read-only connection (sees LTX updates).
    pub fn query_row<T, F>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let role = self.inner.current_role();

        match role {
            Some(Role::Leader) | None => {
                let conn_arc = self
                    .inner
                    .get_conn()
                    .ok_or_else(|| anyhow!("No connection available"))?;
                let conn = conn_arc.lock().unwrap();
                conn.query_row(sql, params, f)
                    .map_err(|e| anyhow!("query_row failed: {}", e))
            }
            Some(Role::Follower) => {
                // Followers must open fresh connections each time because walrust
                // applies LTX files externally — pooled connections hold stale snapshots.
                let conn = rusqlite::Connection::open_with_flags(
                    &self.inner.db_path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(|e| anyhow!("Failed to open read-only connection: {}", e))?;
                conn.query_row(sql, params, f)
                    .map_err(|e| anyhow!("query_row failed: {}", e))
            }
        }
    }

    /// Query and return rows as Vec<Vec<SqlValue>>. Always executes locally.
    ///
    /// Returns all matching rows. Each row is a Vec of column values.
    /// Returns an empty Vec if no rows match (not an error).
    pub fn query_values(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<Vec<SqlValue>>> {
        let rusqlite_params: Vec<rusqlite::types::Value> =
            params.iter().map(|p| p.to_rusqlite()).collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            rusqlite_params
                .iter()
                .map(|p| p as &dyn rusqlite::types::ToSql)
                .collect();

        let query_with = |conn: &rusqlite::Connection| -> Result<Vec<Vec<SqlValue>>> {
            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| anyhow!("query prepare failed: {e}"))?;
            let column_count = stmt.column_count();
            let mut rows_iter = stmt
                .query(param_refs.as_slice())
                .map_err(|e| anyhow!("query failed: {e}"))?;
            let mut rows = Vec::new();
            while let Some(row) = rows_iter
                .next()
                .map_err(|e| anyhow!("row iteration failed: {e}"))?
            {
                let mut vals = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let val: rusqlite::types::Value = row
                        .get(i)
                        .map_err(|e| anyhow!("column {i} read failed: {e}"))?;
                    vals.push(SqlValue::from_rusqlite(val));
                }
                rows.push(vals);
            }
            Ok(rows)
        };

        let role = self.inner.current_role();
        match role {
            Some(Role::Leader) | None => {
                let conn_arc = self
                    .inner
                    .get_conn()
                    .ok_or_else(|| anyhow!("No connection available"))?;
                let conn = conn_arc.lock().unwrap();
                query_with(&conn)
            }
            Some(Role::Follower) => {
                let conn = rusqlite::Connection::open_with_flags(
                    &self.inner.db_path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(|e| anyhow!("Failed to open read-only connection: {e}"))?;
                query_with(&conn)
            }
        }
    }

    /// Get the current role of this node.
    pub fn role(&self) -> Option<Role> {
        self.inner.current_role()
    }

    /// Subscribe to role change events.
    pub fn role_events(&self) -> Option<tokio::sync::broadcast::Receiver<RoleEvent>> {
        self.inner.coordinator.as_ref().map(|c| c.role_events())
    }

    /// Access the underlying Coordinator (for metrics, discover_replicas, etc).
    pub fn coordinator(&self) -> Option<&Arc<Coordinator>> {
        self.inner.coordinator.as_ref()
    }

    /// Get metrics in Prometheus exposition format.
    /// Returns None in local mode (no coordinator).
    pub fn prometheus_metrics(&self) -> Option<String> {
        self.inner
            .coordinator
            .as_ref()
            .map(|c| c.metrics().snapshot().to_prometheus())
    }

    /// Graceful leader handoff — release leadership without shutting down.
    ///
    /// The node transitions to follower (can still serve reads while draining).
    /// A follower on another node will see the released lease and promote itself.
    /// Returns `Ok(true)` if handoff succeeded, `Ok(false)` if not the leader.
    pub async fn handoff(&self) -> Result<bool> {
        let coordinator = match &self.inner.coordinator {
            Some(c) => c,
            None => return Ok(false), // local mode — no HA
        };

        let result = coordinator.handoff(&self.inner.db_name).await?;
        if result {
            // Close rw connection — role listener will also catch the Demoted event,
            // but we do it eagerly here.
            self.inner.set_conn(None);
        }
        Ok(result)
    }

    /// Cleanly shut down: close connections, leave cluster, abort background tasks.
    pub async fn close(self) -> Result<()> {
        // Close connection.
        self.inner.set_conn(None);

        // Leave the cluster.
        if let Some(ref coordinator) = self.inner.coordinator {
            coordinator.leave(&self.inner.db_name).await?;
        }

        // Abort background tasks.
        self._fwd_handle.abort();
        self._role_handle.abort();

        Ok(())
    }

    // ========================================================================
    // Internal
    // ========================================================================

    fn execute_local(&self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        let conn_arc = self
            .inner
            .get_conn()
            .ok_or_else(|| anyhow!("No write connection available (not leader?)"))?;
        let conn = conn_arc.lock().unwrap();

        let values: Vec<rusqlite::types::Value> = params.iter().map(|p| p.to_rusqlite()).collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            values.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

        let rows = conn
            .execute(sql, param_refs.as_slice())
            .map_err(|e| anyhow!("execute failed: {}", e))?;

        Ok(rows as u64)
    }

    async fn execute_forwarded(&self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        let leader_addr = self.inner.leader_addr();
        if leader_addr.is_empty() {
            return Err(anyhow!("No leader address available — cannot forward write"));
        }

        let url = format!("{}/haqlite/execute", leader_addr);
        let body = forwarding::ForwardedExecute {
            sql: sql.to_string(),
            params: params.to_vec(),
        };

        let mut req = self.inner.http_client.post(&url).json(&body);
        if let Some(ref secret) = self.inner.secret {
            req = req.bearer_auth(secret);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| anyhow!("Failed to forward write to leader: {}", e))?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Leader returned error: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ));
        }

        let result: forwarding::ExecuteResult = resp
            .json()
            .await
            .map_err(|e| anyhow!("Failed to parse leader response: {}", e))?;

        Ok(result.rows_affected)
    }
}

// ============================================================================
// Shared open logic for builder and from_coordinator
// ============================================================================

async fn open_with_coordinator(
    coordinator: Arc<Coordinator>,
    db_path: PathBuf,
    db_name: &str,
    schema: &str,
    address: &str,
    forwarding_port: u16,
    forward_timeout: Duration,
    secret: Option<String>,
) -> Result<HaQLite> {
    ensure_schema(&db_path, schema)?;

    // Subscribe to role events BEFORE join.
    let role_rx = coordinator.role_events();

    // Join the HA cluster.
    let initial_role = coordinator.join(db_name, &db_path).await?;

    let leader_addr = if initial_role == Role::Leader {
        address.to_string()
    } else {
        coordinator
            .leader_address(db_name)
            .await
            .unwrap_or_default()
    };

    let http_client = reqwest::Client::builder()
        .timeout(forward_timeout)
        .build()?;

    let inner = Arc::new(HaQLiteInner {
        coordinator: Some(coordinator),
        db_name: db_name.to_string(),
        db_path: db_path.clone(),
        role: AtomicU8::new(match initial_role {
            Role::Leader => ROLE_LEADER,
            Role::Follower => ROLE_FOLLOWER,
        }),
        conn: RwLock::new(None),
        leader_address: RwLock::new(leader_addr),
        http_client,
        secret: secret.clone(),
    });

    // If leader, open rw connection.
    if initial_role == Role::Leader {
        let conn = open_leader_connection(&db_path)?;
        inner.set_conn(Some(Arc::new(Mutex::new(conn))));
    }

    // Spawn forwarding server.
    let fwd_state = Arc::new(ForwardingState {
        inner: inner.clone(),
    });
    let fwd_app = axum::Router::new()
        .route(
            "/haqlite/execute",
            post(forwarding::handle_forwarded_execute),
        )
        .route(
            "/haqlite/query",
            post(forwarding::handle_forwarded_query),
        )
        .with_state(fwd_state);
    let fwd_listener =
        tokio::net::TcpListener::bind(format!("0.0.0.0:{}", forwarding_port)).await?;
    let fwd_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(fwd_listener, fwd_app).await {
            tracing::error!("Forwarding server error: {}", e);
        }
    });

    // Spawn role event listener.
    let role_inner = inner.clone();
    let role_address = address.to_string();
    let role_handle = tokio::spawn(async move {
        run_role_listener(role_rx, role_inner, role_address).await;
    });

    Ok(HaQLite {
        inner,
        _fwd_handle: fwd_handle,
        _role_handle: role_handle,
    })
}

// ============================================================================
// Role event listener
// ============================================================================

async fn run_role_listener(
    mut role_rx: tokio::sync::broadcast::Receiver<RoleEvent>,
    inner: Arc<HaQLiteInner>,
    self_address: String,
) {
    loop {
        match role_rx.recv().await {
            Ok(RoleEvent::Promoted { db_name }) => {
                tracing::info!("HaQLite: promoted to leader for '{}'", db_name);
                inner.set_leader_addr(self_address.clone());
                inner.set_role(Role::Leader);

                match open_leader_connection(&inner.db_path) {
                    Ok(conn) => {
                        inner.set_conn(Some(Arc::new(Mutex::new(conn))));
                    }
                    Err(e) => {
                        tracing::error!(
                            "HaQLite: failed to open connection on promotion: {}",
                            e
                        );
                    }
                }
            }
            Ok(RoleEvent::Demoted { db_name }) => {
                tracing::error!("HaQLite: demoted from leader for '{}'", db_name);
                inner.set_role(Role::Follower);
                inner.set_conn(None);
            }
            Ok(RoleEvent::Fenced { db_name }) => {
                tracing::error!("HaQLite: fenced for '{}' — stopping writes", db_name);
                inner.set_role(Role::Follower);
                inner.set_conn(None);
            }
            Ok(RoleEvent::Sleeping { db_name }) => {
                tracing::info!("HaQLite: sleeping signal for '{}'", db_name);
                inner.set_conn(None);
            }
            Ok(RoleEvent::Joined { .. }) => {}
            Err(_) => {
                tracing::error!("HaQLite: role event channel closed");
                break;
            }
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Create the DB file with schema (WAL mode, autocheckpoint=0).
fn ensure_schema(db_path: &Path, schema: &str) -> Result<()> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")?;
    conn.execute_batch(schema)?;
    drop(conn);
    Ok(())
}

/// Open a read-write connection with WAL mode and autocheckpoint disabled.
fn open_leader_connection(db_path: &Path) -> Result<rusqlite::Connection> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")?;
    Ok(conn)
}

/// Auto-detect this node's network address for the forwarding server.
fn detect_address(instance_id: &str, port: u16) -> String {
    // On Fly: use internal DNS.
    if let Ok(app_name) = std::env::var("FLY_APP_NAME") {
        return format!(
            "http://{}.vm.{}.internal:{}",
            instance_id, app_name, port
        );
    }

    // Fallback: hostname.
    let hostname = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "localhost".to_string());

    format!("http://{}:{}", hostname, port)
}
