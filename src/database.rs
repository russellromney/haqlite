//! HaQLite: dead-simple embedded HA SQLite.
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! use haqlite::{HaQLite, SqlValue};
//!
//! let db = HaQLite::builder("my-bucket")
//!     .open("/data/my.db", "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
//!     .await?;
//!
//! db.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Alice".into())]).await?;
//! let count: i64 = db.query_row("SELECT COUNT(*) FROM users", &[], |r| r.get(0))?;
//! # Ok(())
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::Result;
use axum::routing::post;
use hadb::{CoordinatorConfig, Coordinator, JoinResult, LeaseConfig, Replicator, Role, RoleEvent};
use hadb_lease_s3::S3LeaseStore;

use crate::error::HaQLiteError;
use crate::follower_behavior::SqliteFollowerBehavior;
use crate::forwarding::{self, ForwardingState, SqlValue};
use crate::replicator::SqliteReplicator;

const DEFAULT_PREFIX: &str = "haqlite/";
const DEFAULT_FORWARDING_PORT: u16 = 18080;
const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

const ROLE_LEADER: u8 = 0;
const ROLE_FOLLOWER: u8 = 1;

/// Coordination mode for HaQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HaMode {
    /// Persistent leader. Continuous sync. Write forwarding.
    #[default]
    Dedicated,
    /// Ephemeral leader. Lease per write. No forwarding.
    /// For Lambda, Fly scale-to-zero, and other ephemeral compute.
    Shared,
}

/// Builder for creating an HA SQLite instance.
///
/// Only the bucket is required — everything else has sensible defaults.
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// use haqlite::HaQLite;
///
/// let db = HaQLite::builder("my-bucket")
///     .prefix("myapp/")
///     .forwarding_port(19000)
///     .open("/data/my.db", "CREATE TABLE IF NOT EXISTS ...")
///     .await?;
/// # Ok(())
/// # }
/// ```
const DEFAULT_READ_CONCURRENCY: usize = 32;

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
    read_concurrency: usize,
    lease_store: Option<Arc<dyn hadb::LeaseStore>>,
    mode: HaMode,
    manifest_store: Option<Arc<dyn hadb::ManifestStore>>,
    manifest_poll_interval: Option<Duration>,
    write_timeout: Option<Duration>,
    walrust_storage: Option<Arc<dyn walrust::StorageBackend>>,
    lease_ttl: Option<u64>,
    #[cfg(feature = "turbolite")]
    turbolite_vfs: Option<(turbolite::tiered::SharedTurboliteVfs, String)>,
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
            read_concurrency: DEFAULT_READ_CONCURRENCY,
            lease_store: None,
            mode: HaMode::Dedicated,
            manifest_store: None,
            manifest_poll_interval: None,
            write_timeout: None,
            walrust_storage: None,
            lease_ttl: None,
            #[cfg(feature = "turbolite")]
            turbolite_vfs: None,
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

    /// Maximum concurrent follower reads. Default: 32.
    pub fn read_concurrency(mut self, n: usize) -> Self {
        self.read_concurrency = n;
        self
    }

    /// Use a custom LeaseStore instead of the default S3LeaseStore.
    ///
    /// Works with any `LeaseStore` implementation: NATS, Redis, etcd, etc.
    /// When set, the builder skips S3LeaseStore construction in `open()`.
    pub fn lease_store(mut self, store: Arc<dyn hadb::LeaseStore>) -> Self {
        self.lease_store = Some(store);
        self
    }

    /// Set the coordination mode. Default: `HaMode::Dedicated`.
    pub fn mode(mut self, mode: HaMode) -> Self {
        self.mode = mode;
        self
    }

    /// Use a ManifestStore (required for `HaMode::Shared`).
    pub fn manifest_store(mut self, store: Arc<dyn hadb::ManifestStore>) -> Self {
        self.manifest_store = Some(store);
        self
    }

    /// Manifest polling interval for Shared mode. Default: 1s.
    pub fn manifest_poll_interval(mut self, interval: Duration) -> Self {
        self.manifest_poll_interval = Some(interval);
        self
    }

    /// Write timeout for lease acquisition in Shared mode. Default: 5s.
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = Some(timeout);
        self
    }

    /// Lease TTL in seconds for Shared mode. Default: 5.
    /// Lower values make lease expiration faster (useful for testing).
    pub fn lease_ttl(mut self, ttl_secs: u64) -> Self {
        self.lease_ttl = Some(ttl_secs);
        self
    }

    /// Use a custom walrust StorageBackend instead of building from S3 env.
    /// For testing with in-memory storage.
    pub fn walrust_storage(mut self, storage: Arc<dyn walrust::StorageBackend>) -> Self {
        self.walrust_storage = Some(storage);
        self
    }

    /// Use a turbolite VFS as the storage engine (Shared mode only).
    ///
    /// `vfs` is the TurboliteVfs (must already be registered with SQLite).
    /// `vfs_name` is the name used in the VFS URI (e.g. "mydb").
    #[cfg(feature = "turbolite")]
    pub fn turbolite_vfs(mut self, vfs: turbolite::tiered::SharedTurboliteVfs, vfs_name: &str) -> Self {
        self.turbolite_vfs = Some((vfs, vfs_name.to_string()));
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

        // Build walrust storage backend (injected for tests, S3 by default).
        let walrust_storage: Arc<dyn walrust::StorageBackend> = match self.walrust_storage {
            Some(storage) => storage,
            None => Arc::new(
                walrust::S3Backend::from_env(self.bucket.clone(), self.endpoint.as_deref()).await?,
            ),
        };

        // Use custom lease store if provided, otherwise build S3 lease store.
        // Only construct the S3 client when we actually need it for S3LeaseStore.
        let lease_store: Arc<dyn hadb::LeaseStore> = match self.lease_store {
            Some(store) => store,
            None => {
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
                Arc::new(S3LeaseStore::new(s3_client, self.bucket.clone()))
            }
        };

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

        match self.mode {
            HaMode::Dedicated => {
                let follower_behavior = Arc::new(SqliteFollowerBehavior::new(walrust_storage));

                // Build hadb Coordinator.
                let coordinator = Coordinator::new(
                    replicator,
                    Some(lease_store),
                    self.manifest_store.clone(),
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
                    self.read_concurrency,
                )
                .await
            }
            HaMode::Shared => {
                let manifest_store = self.manifest_store
                    .expect("Shared mode requires manifest_store");
                let poll_interval = self.manifest_poll_interval
                    .unwrap_or(Duration::from_secs(1));
                let write_timeout = self.write_timeout
                    .unwrap_or(Duration::from_secs(5));

                #[cfg(feature = "turbolite")]
                if let Some((vfs, vfs_name)) = self.turbolite_vfs {
                    return open_shared_turbolite(
                        lease_store, manifest_store, vfs, &vfs_name,
                        db_path, &db_name, schema, &self.prefix, &instance_id,
                        poll_interval, write_timeout, self.read_concurrency,
                    ).await;
                }

                let lease_ttl = self.lease_ttl.unwrap_or(5);
                open_shared(
                    lease_store,
                    manifest_store,
                    replicator,
                    walrust_storage,
                    db_path,
                    &db_name,
                    schema,
                    &self.prefix,
                    &instance_id,
                    poll_interval,
                    write_timeout,
                    self.read_concurrency,
                    lease_ttl,
                )
                .await
            }
        }
    }
}

/// HA SQLite database — transparent write forwarding, local reads, automatic failover.
///
/// Create via `HaQLite::builder("bucket").open(path, schema).await?`
/// or `HaQLite::local(path, schema)?` for single-node mode.
/// HA SQLite database with transparent write forwarding, local reads, and automatic failover.
///
/// **You MUST call [`.close().await`](HaQLite::close) before dropping.** If dropped without
/// close(), background tasks are aborted and the lease is not cleanly released, which may
/// block other nodes from acquiring the lease until TTL expires.
///
/// Create via `HaQLite::builder("bucket").open(path, schema).await?`
/// or `HaQLite::local(path, schema)?` for single-node mode.
pub struct HaQLite {
    inner: Arc<HaQLiteInner>,
    _fwd_handle: tokio::task::JoinHandle<()>,
    _role_handle: tokio::task::JoinHandle<()>,
    closed: bool,
}

impl Drop for HaQLite {
    fn drop(&mut self) {
        if !self.closed {
            // Abort background tasks so they don't leak.
            // Note: this does NOT cleanly release the lease. Call close().await for that.
            self._fwd_handle.abort();
            self._role_handle.abort();
            self.inner.read_semaphore.close();
            tracing::warn!(
                "HaQLite dropped without close() for '{}'. Background tasks aborted, lease not cleanly released. \
                 Call .close().await before dropping for clean shutdown.",
                self.inner.db_name,
            );
        }
    }
}

/// Internal state shared between HaQLite, forwarding handler, and role listener.
///
/// Uses std::sync primitives so `query_row()` works without async runtime.
pub(crate) struct HaQLiteInner {
    pub(crate) coordinator: Option<Arc<Coordinator>>,
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    /// Cached role -- updated atomically by the role event listener.
    role: AtomicU8,
    /// Read-write connection when leader, None when follower.
    pub(crate) conn: RwLock<Option<Arc<Mutex<rusqlite::Connection>>>>,
    /// Leader's forwarding address (read from S3 lease, updated on role change).
    leader_address: RwLock<String>,
    pub(crate) http_client: reqwest::Client,
    /// Shared secret for authenticating forwarding requests.
    pub(crate) secret: Option<String>,
    /// Limits concurrent follower reads. Closed on shutdown.
    read_semaphore: tokio::sync::Semaphore,
    /// Whether the follower has caught up with the leader's WAL.
    follower_caught_up: Arc<AtomicBool>,
    /// Current follower replay position (TXID).
    follower_replay_position: Arc<AtomicU64>,
    // --- Phase Crest: Shared mode fields ---
    mode: HaMode,
    /// Direct lease store access (Shared mode, bypasses Coordinator).
    shared_lease_store: Option<Arc<dyn hadb::LeaseStore>>,
    /// Direct manifest store access (Shared mode).
    shared_manifest_store: Option<Arc<dyn hadb::ManifestStore>>,
    /// Direct replicator access (Shared mode).
    shared_replicator: Option<Arc<SqliteReplicator>>,
    /// Walrust storage backend for pull_incremental.
    shared_walrust_storage: Option<Arc<dyn walrust::StorageBackend>>,
    /// S3 prefix for lease/manifest keys.
    shared_prefix: String,
    /// Instance ID for this node.
    shared_instance_id: String,
    /// Serializes all writes in Shared mode.
    write_mutex: tokio::sync::Mutex<()>,
    /// Cached manifest version for freshness checks.
    cached_manifest_version: AtomicU64,
    /// Write timeout for lease acquisition.
    write_timeout: Duration,
    /// Lease TTL for shared mode leases.
    lease_ttl: u64,
    /// Turbolite VFS for Shared mode turbolite path.
    #[cfg(feature = "turbolite")]
    shared_turbolite_vfs: Option<turbolite::tiered::SharedTurboliteVfs>,
    /// VFS name for reopening turbolite connections.
    #[cfg(feature = "turbolite")]
    shared_turbolite_vfs_name: Option<String>,
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

    fn leader_addr(&self) -> std::result::Result<String, HaQLiteError> {
        self.leader_address.read()
            .map(|g| g.clone())
            .map_err(|_| HaQLiteError::DatabaseError("leader_address lock poisoned".into()))
    }

    fn set_leader_addr(&self, addr: String) {
        *self.leader_address.write()
            .expect("leader_address write lock poisoned") = addr;
    }

    fn set_conn(&self, conn: Option<Arc<Mutex<rusqlite::Connection>>>) {
        *self.conn.write()
            .expect("conn write lock poisoned") = conn;
    }

    pub(crate) fn get_conn(&self) -> std::result::Result<Option<Arc<Mutex<rusqlite::Connection>>>, HaQLiteError> {
        self.conn.read()
            .map(|g| g.clone())
            .map_err(|_| HaQLiteError::DatabaseError("conn lock poisoned".into()))
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
            read_semaphore: tokio::sync::Semaphore::new(DEFAULT_READ_CONCURRENCY),
            follower_caught_up: Arc::new(AtomicBool::new(true)),
            follower_replay_position: Arc::new(AtomicU64::new(0)),
            mode: HaMode::Dedicated,
            shared_lease_store: None,
            shared_manifest_store: None,
            shared_replicator: None,
            shared_walrust_storage: None,
            shared_prefix: String::new(),
            shared_instance_id: String::new(),
            write_mutex: tokio::sync::Mutex::new(()),
            cached_manifest_version: AtomicU64::new(0),
            write_timeout: DEFAULT_FORWARD_TIMEOUT,
            lease_ttl: 5,
            #[cfg(feature = "turbolite")]
            shared_turbolite_vfs: None,
            #[cfg(feature = "turbolite")]
            shared_turbolite_vfs_name: None,
        });

        // No forwarding server or role listener in local mode.
        let fwd_handle = tokio::spawn(async {});
        let role_handle = tokio::spawn(async {});

        Ok(HaQLite {
            inner,
            _fwd_handle: fwd_handle,
            _role_handle: role_handle,
            closed: false,
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
            DEFAULT_READ_CONCURRENCY,
        )
        .await
    }

    /// Execute a write statement. Returns rows affected.
    ///
    /// On the leader: executes locally.
    /// On a follower: forwards to the leader via HTTP.
    pub async fn execute(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
        match self.inner.mode {
            HaMode::Shared => self.execute_shared(sql, params).await,
            HaMode::Dedicated => {
                let role = self.inner.current_role();
                match role {
                    Some(Role::Leader) | None => self.execute_local(sql, params),
                    Some(Role::Follower) => self.execute_forwarded(sql, params).await,
                }
            }
        }
    }

    /// Query a single row from local state. Does NOT catch up from manifest.
    ///
    /// **In Shared mode, this may return stale data.** If another node has written
    /// since this node last caught up, `query_row_local` will not see those writes.
    /// Use [`query_row_fresh`] for consistency in Shared mode.
    ///
    /// In Dedicated mode: leader reads from persistent connection, follower opens
    /// a fresh read-only connection (sees walrust LTX updates).
    pub fn query_row_local<T, F>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
        f: F,
    ) -> std::result::Result<T, HaQLiteError>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let role = self.inner.current_role();

        match role {
            Some(Role::Leader) | None => {
                let conn_arc = self
                    .inner
                    .get_conn()?
                    .ok_or(HaQLiteError::DatabaseError("No connection available".into()))?;
                let conn = conn_arc.lock()
                    .map_err(|_| HaQLiteError::DatabaseError("connection lock poisoned".into()))?;
                conn.query_row(sql, params, f)
                    .map_err(|e| HaQLiteError::DatabaseError(format!("query_row failed: {e}")))
            }
            Some(Role::Follower) => {
                // Bound concurrent follower reads via semaphore.
                let _permit = self.inner.read_semaphore.try_acquire()
                    .map_err(|e| match e {
                        tokio::sync::TryAcquireError::Closed => HaQLiteError::EngineClosed,
                        tokio::sync::TryAcquireError::NoPermits => HaQLiteError::DatabaseError(
                            "Too many concurrent reads".into(),
                        ),
                    })?;
                // Followers must open fresh connections each time because walrust
                // applies LTX files externally -- pooled connections hold stale snapshots.
                let conn = rusqlite::Connection::open_with_flags(
                    &self.inner.db_path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(|e| HaQLiteError::DatabaseError(format!("Failed to open read-only connection: {e}")))?;
                conn.query_row(sql, params, f)
                    .map_err(|e| HaQLiteError::DatabaseError(format!("query_row failed: {e}")))
            }
        }
    }

    /// Deprecated: use [`query_row_local`] (stale reads ok) or [`query_row_fresh`] (consistency required).
    #[deprecated(note = "use query_row_local (stale reads ok) or query_row_fresh (consistency required)")]
    pub fn query_row<T, F>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
        f: F,
    ) -> std::result::Result<T, HaQLiteError>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.query_row_local(sql, params, f)
    }

    /// Query rows from local state. Does NOT catch up from manifest.
    ///
    /// **In Shared mode, this may return stale data.** Use [`query_values_fresh`] for consistency.
    ///
    /// Returns all matching rows. Each row is a Vec of column values.
    /// Returns an empty Vec if no rows match (not an error).
    pub fn query_values_local(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<Vec<Vec<SqlValue>>, HaQLiteError> {
        let rusqlite_params: Vec<rusqlite::types::Value> =
            params.iter().map(|p| p.to_rusqlite()).collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            rusqlite_params
                .iter()
                .map(|p| p as &dyn rusqlite::types::ToSql)
                .collect();

        let query_with = |conn: &rusqlite::Connection| -> std::result::Result<Vec<Vec<SqlValue>>, HaQLiteError> {
            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| HaQLiteError::DatabaseError(format!("query prepare failed: {e}")))?;
            let column_count = stmt.column_count();
            let mut rows_iter = stmt
                .query(param_refs.as_slice())
                .map_err(|e| HaQLiteError::DatabaseError(format!("query failed: {e}")))?;
            let mut rows = Vec::new();
            while let Some(row) = rows_iter
                .next()
                .map_err(|e| HaQLiteError::DatabaseError(format!("row iteration failed: {e}")))?
            {
                let mut vals = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let val: rusqlite::types::Value = row
                        .get(i)
                        .map_err(|e| HaQLiteError::DatabaseError(format!("column {i} read failed: {e}")))?;
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
                    .get_conn()?
                    .ok_or(HaQLiteError::DatabaseError("No connection available".into()))?;
                let conn = conn_arc.lock()
                    .map_err(|_| HaQLiteError::DatabaseError("connection lock poisoned".into()))?;
                query_with(&conn)
            }
            Some(Role::Follower) => {
                let _permit = self.inner.read_semaphore.try_acquire()
                    .map_err(|e| match e {
                        tokio::sync::TryAcquireError::Closed => HaQLiteError::EngineClosed,
                        tokio::sync::TryAcquireError::NoPermits => HaQLiteError::DatabaseError(
                            "Too many concurrent reads".into(),
                        ),
                    })?;
                let conn = rusqlite::Connection::open_with_flags(
                    &self.inner.db_path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(|e| HaQLiteError::DatabaseError(format!("Failed to open read-only connection: {e}")))?;
                query_with(&conn)
            }
        }
    }

    /// Deprecated: use [`query_values_local`] (stale reads ok) or [`query_values_fresh`] (consistency required).
    #[deprecated(note = "use query_values_local (stale reads ok) or query_values_fresh (consistency required)")]
    pub fn query_values(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<Vec<Vec<SqlValue>>, HaQLiteError> {
        self.query_values_local(sql, params)
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

    /// Whether the follower has caught up with the leader's WAL.
    /// Always true for leaders and local mode.
    pub fn is_caught_up(&self) -> bool {
        match self.inner.current_role() {
            Some(Role::Leader) | None => true,
            Some(Role::Follower) => self.inner.follower_caught_up.load(Ordering::SeqCst),
        }
    }

    /// Current follower replay position (TXID). Returns 0 for leaders/local mode.
    pub fn replay_position(&self) -> u64 {
        self.inner.follower_replay_position.load(Ordering::SeqCst)
    }

    /// Get metrics in Prometheus exposition format.
    /// Returns None in local mode (no coordinator).
    pub fn prometheus_metrics(&self) -> Option<String> {
        self.inner
            .coordinator
            .as_ref()
            .map(|c| {
                let mut output = c.metrics().snapshot().to_prometheus();
                let caught_up = if self.is_caught_up() { 1 } else { 0 };
                let position = self.replay_position();
                output.push_str(&format!(
                    "\n# HELP haqlite_follower_caught_up Whether follower is caught up (1=yes, 0=no)\n\
                     # TYPE haqlite_follower_caught_up gauge\n\
                     haqlite_follower_caught_up {}\n\
                     # HELP haqlite_follower_replay_position Current follower replay TXID\n\
                     # TYPE haqlite_follower_replay_position gauge\n\
                     haqlite_follower_replay_position {}\n",
                    caught_up, position,
                ));
                output
            })
    }

    /// Graceful leader handoff — release leadership without shutting down.
    ///
    /// The node transitions to follower (can still serve reads while draining).
    /// A follower on another node will see the released lease and promote itself.
    /// Returns `Ok(true)` if handoff succeeded, `Ok(false)` if not the leader.
    pub async fn handoff(&self) -> std::result::Result<bool, HaQLiteError> {
        let coordinator = match &self.inner.coordinator {
            Some(c) => c,
            None => return Ok(false), // local mode -- no HA
        };

        let result = coordinator
            .handoff(&self.inner.db_name)
            .await
            .map_err(|e| HaQLiteError::CoordinatorError(e.to_string()))?;
        if result {
            // Update cached role immediately -- don't wait for role listener
            // to process the Demoted event.
            self.inner.set_role(Role::Follower);
            // Close rw connection -- role listener will also catch the Demoted event,
            // but we do it eagerly here.
            self.inner.set_conn(None);
        }
        Ok(result)
    }

    /// Cleanly shut down: drain in-flight operations, leave cluster, abort background tasks.
    ///
    /// Must be called before dropping. If not called, Drop will abort tasks and log a warning,
    /// but the lease will not be cleanly released.
    pub async fn close(&mut self) -> std::result::Result<(), HaQLiteError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        // 1. Close read semaphore -- new reads get EngineClosed immediately.
        self.inner.read_semaphore.close();

        // 2. Close connection (drains in-flight writes via the Mutex).
        self.inner.set_conn(None);

        // 3. Leave the cluster (releases lease).
        if let Some(ref coordinator) = self.inner.coordinator {
            coordinator
                .leave(&self.inner.db_name)
                .await
                .map_err(|e| HaQLiteError::CoordinatorError(e.to_string()))?;
        }

        // 4. Abort background tasks.
        self._fwd_handle.abort();
        self._role_handle.abort();

        Ok(())
    }

    /// Read with freshness guarantee: checks manifest and catches up before querying.
    /// In Dedicated mode, this is equivalent to `query_row_local()`.
    pub async fn query_row_fresh<T, F>(
        &self, sql: &str, params: &[&dyn rusqlite::types::ToSql], f: F,
    ) -> std::result::Result<T, HaQLiteError>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.ensure_fresh().await?;
        self.query_row_local(sql, params, f)
    }

    /// Read with freshness guarantee (multi-row version).
    pub async fn query_values_fresh(
        &self, sql: &str, params: &[SqlValue],
    ) -> std::result::Result<Vec<Vec<SqlValue>>, HaQLiteError> {
        self.ensure_fresh().await?;
        self.query_values_local(sql, params)
    }

    /// Check manifest freshness and catch up if stale.
    async fn ensure_fresh(&self) -> std::result::Result<(), HaQLiteError> {
        if self.inner.mode != HaMode::Shared {
            return Ok(());
        }
        let manifest_store = match &self.inner.shared_manifest_store {
            Some(ms) => ms,
            None => return Ok(()),
        };
        let manifest_key = format!("{}{}/_manifest", self.inner.shared_prefix, self.inner.db_name);
        let cached = self.inner.cached_manifest_version.load(Ordering::SeqCst);
        match manifest_store.meta(&manifest_key).await {
            Ok(Some(meta)) if meta.version > cached => {
                // Turbolite path: apply manifest to VFS
                #[cfg(feature = "turbolite")]
                if let Some(ref vfs) = self.inner.shared_turbolite_vfs {
                    if let Ok(Some(ha_manifest)) = manifest_store.get(&manifest_key).await {
                        if let hadb::StorageManifest::Turbolite { .. } = &ha_manifest.storage {
                            let tl_manifest = crate::turbolite_replicator::ha_storage_to_turbolite(&ha_manifest.storage);
                            vfs.set_manifest(tl_manifest);
                            self.inner.cached_manifest_version.store(meta.version, Ordering::SeqCst);
                        }
                    }
                    return Ok(());
                }

                // Walrust path: restore from S3
                if let Some(ref rep) = self.inner.shared_replicator {
                    rep.restore(&self.inner.db_name, &self.inner.db_path).await
                        .map_err(|e| HaQLiteError::ReplicationError(
                            format!("ensure_fresh catch-up failed for '{}': {}", self.inner.db_name, e)
                        ))?;
                    self.inner.cached_manifest_version.store(meta.version, Ordering::SeqCst);
                    // Re-register replicator after restore so sync() uses restored state
                    let _ = rep.inner().remove(&self.inner.db_name).await;
                    if let Err(e) = rep.add(&self.inner.db_name, &self.inner.db_path).await {
                        return Err(HaQLiteError::ReplicationError(
                            format!("re-register after ensure_fresh restore failed for '{}': {}", self.inner.db_name, e)
                        ));
                    }
                    // Reopen connection to see restored data
                    let new_conn = open_leader_connection(&self.inner.db_path)
                        .map_err(|e| HaQLiteError::DatabaseError(
                            format!("reopen after ensure_fresh restore failed for '{}': {}", self.inner.db_name, e)
                        ))?;
                    self.inner.set_conn(Some(Arc::new(Mutex::new(new_conn))));
                }
            }
            _ => {}
        }
        Ok(())
    }

    // ========================================================================
    // Internal
    // ========================================================================

    async fn execute_shared(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
        // Dispatch to turbolite path if turbolite VFS is configured.
        #[cfg(feature = "turbolite")]
        if self.inner.shared_turbolite_vfs.is_some() {
            return self.execute_shared_turbolite(sql, params).await;
        }

        let lease_store = self.inner.shared_lease_store.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_lease_store required in Shared mode".into()))?;
        let manifest_store = self.inner.shared_manifest_store.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_manifest_store required in Shared mode".into()))?;
        let replicator = self.inner.shared_replicator.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_replicator required in Shared mode".into()))?;
        let walrust_storage = self.inner.shared_walrust_storage.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_walrust_storage required in Shared mode".into()))?;

        // 1. Serialize writes on this node
        let _write_guard = self.inner.write_mutex.lock().await;

        // 2. Acquire lease with retry + backoff
        let lease_key = format!("{}{}/_lease", self.inner.shared_prefix, self.inner.db_name);
        let lease_data = serde_json::to_vec(&serde_json::json!({
            "instance_id": self.inner.shared_instance_id,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default().as_millis() as u64,
            "ttl_secs": self.inner.lease_ttl,
        })).unwrap_or_default();

        let mut lease_etag: Option<String> = None;
        let deadline = tokio::time::Instant::now() + self.inner.write_timeout;
        let mut attempt = 0u32;
        loop {
            // First attempt: try to create the lease (no existing lease)
            // Subsequent attempts: only try write_if_match if we confirmed lease is expired
            let result = match &lease_etag {
                None => lease_store.write_if_not_exists(&lease_key, lease_data.clone()).await,
                Some(etag) => lease_store.write_if_match(&lease_key, lease_data.clone(), etag).await,
            };
            match result {
                Ok(cas) if cas.success => {
                    lease_etag = cas.etag;
                    break;
                }
                Ok(_cas) => {
                    // Lease held by another node.
                    if tokio::time::Instant::now() >= deadline {
                        return Err(HaQLiteError::LeaseContention(
                            format!("could not acquire lease for '{}' within {:?}", self.inner.db_name, self.inner.write_timeout),
                        ));
                    }
                    // Read current lease to check TTL expiry
                    match lease_store.read(&lease_key).await {
                        Ok(Some((data, etag))) => {
                            // Parse lease data to check TTL
                            let expired = if let Ok(lease_json) = serde_json::from_slice::<serde_json::Value>(&data) {
                                let lease_ts = lease_json.get("timestamp")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);
                                let lease_ttl = lease_json.get("ttl_secs")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(5);
                                let now_ms = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default().as_millis() as u64;
                                now_ms > lease_ts + (lease_ttl * 1000)
                            } else {
                                // Can't parse lease data; treat as expired (stale/corrupt)
                                true
                            };

                            if expired {
                                // Lease expired: attempt takeover via CAS on next iteration
                                lease_etag = Some(etag);
                            } else {
                                // Lease still valid: wait, don't try takeover yet
                                lease_etag = None;
                            }
                        }
                        Ok(None) => {
                            // Lease was deleted between our check; retry create
                            lease_etag = None;
                        }
                        Err(_) => {
                            lease_etag = None;
                        }
                    }
                    let backoff = Duration::from_millis(100 * 2u64.pow(attempt.min(4)));
                    tokio::time::sleep(backoff.min(Duration::from_secs(2))).await;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(HaQLiteError::CoordinatorError(format!("lease acquire failed: {}", e)));
                }
            }
        }

        // 3-6. Critical section: catch-up, execute, sync, publish.
        // Wrap in a closure so lease is always released even on error.
        let manifest_key = format!("{}{}/_manifest", self.inner.shared_prefix, self.inner.db_name);
        let cached_version = self.inner.cached_manifest_version.load(Ordering::SeqCst);

        let result: std::result::Result<u64, HaQLiteError> = async {
            // 3. Catch up from manifest
            match manifest_store.meta(&manifest_key).await {
                Ok(Some(meta)) if meta.version > cached_version => {
                    // Restore from S3 (full snapshot + incrementals from all writers)
                    replicator.restore(&self.inner.db_name, &self.inner.db_path).await
                        .map_err(|e| HaQLiteError::ReplicationError(
                            format!("shared mode catch-up failed for '{}', refusing stale write: {}", self.inner.db_name, e)
                        ))?;
                    self.inner.cached_manifest_version.store(meta.version, Ordering::SeqCst);
                    // Re-register with replicator so sync() uses the restored state
                    let _ = replicator.inner().remove(&self.inner.db_name).await;
                    if let Err(e) = replicator.add(&self.inner.db_name, &self.inner.db_path).await {
                        return Err(HaQLiteError::ReplicationError(
                            format!("re-register after restore failed for '{}': {}", self.inner.db_name, e)
                        ));
                    }
                    // Reopen connection after restore
                    let new_conn = open_leader_connection(&self.inner.db_path)
                        .map_err(|e| HaQLiteError::DatabaseError(
                            format!("reopen after catch-up for '{}': {}", self.inner.db_name, e)
                        ))?;
                    self.inner.set_conn(Some(Arc::new(Mutex::new(new_conn))));
                }
                _ => {} // already up to date or no manifest
            }

            // Re-read cached_version after catch-up (catch-up may have updated the atomic)
            let cached_version = self.inner.cached_manifest_version.load(Ordering::SeqCst);

            // 4. Execute SQL locally
            let rows = self.execute_local(sql, params)?;

            // 5. Checkpoint: flush WAL to S3
            replicator.sync(&self.inner.db_name).await
                .map_err(|e| HaQLiteError::ReplicationError(format!("checkpoint failed: {}", e)))?;

            // 5b. Re-validate lease after sync. If the lease expired during
            // the (potentially slow) sync, another node may have acquired it
            // and published a new manifest. Proceeding would overwrite their
            // work or leave our write unreachable.
            match lease_store.read(&lease_key).await {
                Ok(Some((data, _etag))) => {
                    if let Ok(lease_json) = serde_json::from_slice::<serde_json::Value>(&data) {
                        let holder = lease_json.get("instance_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if holder != self.inner.shared_instance_id {
                            // Lease was taken by another node during sync
                            return Err(HaQLiteError::LeaseContention(
                                format!(
                                    "lease lost during sync for '{}': held by '{}', we are '{}'",
                                    self.inner.db_name, holder, self.inner.shared_instance_id,
                                ),
                            ));
                        }
                    }
                }
                Ok(None) => {
                    // Lease deleted (expired and cleaned up)
                    return Err(HaQLiteError::LeaseContention(
                        format!("lease disappeared during sync for '{}'", self.inner.db_name),
                    ));
                }
                Err(e) => {
                    // Can't verify lease; fail safe
                    return Err(HaQLiteError::LeaseContention(
                        format!("lease re-validation failed for '{}': {}", self.inner.db_name, e),
                    ));
                }
            }

            // 6. Publish manifest
            let current_seq = replicator.inner().current_seq(&self.inner.db_name).await.unwrap_or(0);
            let changeset_prefix = format!("{}{}/", self.inner.shared_prefix, self.inner.db_name);
            let new_manifest = hadb::HaManifest {
                version: 0, // store assigns version
                writer_id: self.inner.shared_instance_id.clone(),
                lease_epoch: 0,
                timestamp_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default().as_millis() as u64,
                storage: hadb::StorageManifest::Walrust {
                    txid: current_seq,
                    changeset_prefix: changeset_prefix.clone(),
                    latest_changeset_key: format!("{}0000/{:016x}.hadbp", changeset_prefix, current_seq),
                    snapshot_key: None,
                    snapshot_txid: None,
                },
            };
            let expected_version = if cached_version > 0 { Some(cached_version) } else { None };
            match manifest_store.put(&manifest_key, &new_manifest, expected_version).await {
                Ok(cas) if cas.success => {
                    let new_version = cached_version + 1;
                    self.inner.cached_manifest_version.store(new_version, Ordering::SeqCst);
                }
                Ok(_) => {
                    // CAS failed: another writer published between our lease check and
                    // manifest publish. Our write is in S3 but not discoverable.
                    if let Ok(Some(meta)) = manifest_store.meta(&manifest_key).await {
                        self.inner.cached_manifest_version.store(meta.version, Ordering::SeqCst);
                    }
                    return Err(HaQLiteError::ReplicationError(
                        format!("manifest publish CAS failed for '{}': write committed locally but not published", self.inner.db_name),
                    ));
                }
                Err(e) => {
                    return Err(HaQLiteError::ReplicationError(
                        format!("manifest publish failed for '{}': {}", self.inner.db_name, e),
                    ));
                }
            }

            Ok(rows)
        }.await;

        // 7. Always release lease, even on error
        if let Err(e) = lease_store.delete(&lease_key).await {
            tracing::warn!("lease release failed for '{}': {}", self.inner.db_name, e);
        }

        result
    }

    /// Turbolite path for execute_shared: lease -> catch-up -> write -> publish -> release.
    #[cfg(feature = "turbolite")]
    async fn execute_shared_turbolite(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
        let lease_store = self.inner.shared_lease_store.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_lease_store required in Shared mode".into()))?;
        let manifest_store = self.inner.shared_manifest_store.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_manifest_store required in Shared mode".into()))?;
        let vfs = self.inner.shared_turbolite_vfs.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_turbolite_vfs required for turbolite path".into()))?;

        // 1. Serialize writes on this node
        let _write_guard = self.inner.write_mutex.lock().await;

        // 2. Acquire lease with retry + backoff (same logic as walrust path)
        let lease_key = format!("{}{}/_lease", self.inner.shared_prefix, self.inner.db_name);
        let lease_data = serde_json::to_vec(&serde_json::json!({
            "instance_id": self.inner.shared_instance_id,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default().as_millis() as u64,
            "ttl_secs": self.inner.lease_ttl,
        })).unwrap_or_default();

        let mut lease_etag: Option<String> = None;
        let deadline = tokio::time::Instant::now() + self.inner.write_timeout;
        let mut attempt = 0u32;
        loop {
            let result = match &lease_etag {
                None => lease_store.write_if_not_exists(&lease_key, lease_data.clone()).await,
                Some(etag) => lease_store.write_if_match(&lease_key, lease_data.clone(), etag).await,
            };
            match result {
                Ok(cas) if cas.success => {
                    lease_etag = cas.etag;
                    break;
                }
                Ok(_cas) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(HaQLiteError::LeaseContention(
                            format!("could not acquire lease for '{}' within {:?}", self.inner.db_name, self.inner.write_timeout),
                        ));
                    }
                    match lease_store.read(&lease_key).await {
                        Ok(Some((data, etag))) => {
                            let expired = if let Ok(lease_json) = serde_json::from_slice::<serde_json::Value>(&data) {
                                let lease_ts = lease_json.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
                                let lease_ttl = lease_json.get("ttl_secs").and_then(|v| v.as_u64()).unwrap_or(5);
                                let now_ms = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default().as_millis() as u64;
                                now_ms > lease_ts + (lease_ttl * 1000)
                            } else {
                                true
                            };
                            if expired {
                                lease_etag = Some(etag);
                            } else {
                                lease_etag = None;
                            }
                        }
                        Ok(None) => { lease_etag = None; }
                        Err(_) => { lease_etag = None; }
                    }
                    let backoff = Duration::from_millis(100 * 2u64.pow(attempt.min(4)));
                    tokio::time::sleep(backoff.min(Duration::from_secs(2))).await;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(HaQLiteError::CoordinatorError(format!("lease acquire failed: {}", e)));
                }
            }
        }

        // 3-5. Critical section: catch-up, execute, publish.
        // Wrap in a closure so lease is always released even on error.
        let manifest_key = format!("{}{}/_manifest", self.inner.shared_prefix, self.inner.db_name);
        let cached_version = self.inner.cached_manifest_version.load(Ordering::SeqCst);

        let result: std::result::Result<u64, HaQLiteError> = async {
            // 3. Catch up from manifest (apply turbolite manifest to VFS)
            match manifest_store.meta(&manifest_key).await {
                Ok(Some(meta)) if meta.version > cached_version => {
                    if let Ok(Some(ha_manifest)) = manifest_store.get(&manifest_key).await {
                        if let hadb::StorageManifest::Turbolite { .. } = &ha_manifest.storage {
                            let tl_manifest = crate::turbolite_replicator::ha_storage_to_turbolite(&ha_manifest.storage);
                            vfs.set_manifest(tl_manifest);
                            self.inner.cached_manifest_version.store(meta.version, Ordering::SeqCst);
                        }
                    }
                }
                _ => {} // already up to date or no manifest
            }

            // Re-read cached_version after catch-up
            let cached_version = self.inner.cached_manifest_version.load(Ordering::SeqCst);

            // 4. Execute SQL locally
            let rows = self.execute_local(sql, params)?;

            // 5. Publish manifest with turbolite state
            let tl_manifest = vfs.manifest();
            let storage = crate::turbolite_replicator::turbolite_to_ha_storage(&tl_manifest);
            let new_manifest = hadb::HaManifest {
                version: 0, // store assigns version
                writer_id: self.inner.shared_instance_id.clone(),
                lease_epoch: 0,
                timestamp_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default().as_millis() as u64,
                storage,
            };
            // 5b. Re-validate lease before publishing (turbolite path).
            match lease_store.read(&lease_key).await {
                Ok(Some((data, _etag))) => {
                    if let Ok(lease_json) = serde_json::from_slice::<serde_json::Value>(&data) {
                        let holder = lease_json.get("instance_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if holder != self.inner.shared_instance_id {
                            return Err(HaQLiteError::LeaseContention(
                                format!(
                                    "lease lost during write for '{}': held by '{}', we are '{}'",
                                    self.inner.db_name, holder, self.inner.shared_instance_id,
                                ),
                            ));
                        }
                    }
                }
                Ok(None) => {
                    return Err(HaQLiteError::LeaseContention(
                        format!("lease disappeared during write for '{}'", self.inner.db_name),
                    ));
                }
                Err(e) => {
                    return Err(HaQLiteError::LeaseContention(
                        format!("lease re-validation failed for '{}': {}", self.inner.db_name, e),
                    ));
                }
            }

            let expected_version = if cached_version > 0 { Some(cached_version) } else { None };
            match manifest_store.put(&manifest_key, &new_manifest, expected_version).await {
                Ok(cas) if cas.success => {
                    let new_version = cached_version + 1;
                    self.inner.cached_manifest_version.store(new_version, Ordering::SeqCst);
                }
                Ok(_) => {
                    if let Ok(Some(meta)) = manifest_store.meta(&manifest_key).await {
                        self.inner.cached_manifest_version.store(meta.version, Ordering::SeqCst);
                    }
                    return Err(HaQLiteError::ReplicationError(
                        format!("manifest publish CAS failed for '{}': write committed locally but not published", self.inner.db_name),
                    ));
                }
                Err(e) => {
                    return Err(HaQLiteError::ReplicationError(
                        format!("manifest publish failed for '{}': {}", self.inner.db_name, e),
                    ));
                }
            }

            Ok(rows)
        }.await;

        // 6. Always release lease, even on error
        if let Err(e) = lease_store.delete(&lease_key).await {
            tracing::warn!("lease release failed for '{}': {}", self.inner.db_name, e);
        }

        result
    }

    fn execute_local(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
        let conn_arc = self
            .inner
            .get_conn()?
            .ok_or(HaQLiteError::DatabaseError("No write connection available (not leader?)".into()))?;
        let conn = conn_arc.lock()
            .map_err(|_| HaQLiteError::DatabaseError("write connection lock poisoned".into()))?;

        let values: Vec<rusqlite::types::Value> = params.iter().map(|p| p.to_rusqlite()).collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            values.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

        let rows = conn
            .execute(sql, param_refs.as_slice())
            .map_err(|e| HaQLiteError::DatabaseError(format!("execute failed: {e}")))?;

        Ok(rows as u64)
    }

    async fn execute_forwarded(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
        let leader_addr = self.inner.leader_addr()?;
        if leader_addr.is_empty() {
            return Err(HaQLiteError::NotLeader);
        }

        let url = format!("{}/haqlite/execute", leader_addr);
        let body = forwarding::ForwardedExecute {
            sql: sql.to_string(),
            params: params.to_vec(),
        };

        let backoffs = [
            Duration::from_millis(100),
            Duration::from_millis(400),
            Duration::from_millis(1600),
        ];
        let mut last_err = String::new();

        for (attempt, backoff) in std::iter::once(&Duration::ZERO).chain(backoffs.iter()).enumerate() {
            if attempt > 0 {
                tokio::time::sleep(*backoff).await;
            }

            let mut req = self.inner.http_client.post(&url).json(&body);
            if let Some(ref secret) = self.inner.secret {
                req = req.bearer_auth(secret);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    last_err = format!("connection error: {e}");
                    continue; // retry on connection failure
                }
            };

            let status = resp.status();
            if status.is_success() {
                let result: forwarding::ExecuteResult = resp
                    .json()
                    .await
                    .map_err(|e| HaQLiteError::LeaderResponseParseError(
                        format!("failed to parse leader response: {e}")
                    ))?;
                return Ok(result.rows_affected);
            }

            let body_text = resp.text().await.unwrap_or_default();

            // Don't retry 4xx: client errors (bad SQL, auth) won't succeed on retry.
            if status.is_client_error() {
                return Err(HaQLiteError::LeaderClientError {
                    status: status.as_u16(),
                    body: body_text,
                });
            }

            // 5xx: retry with backoff.
            last_err = format!("{status}: {body_text}");
        }

        Err(HaQLiteError::LeaderConnectionError(format!(
            "all {} forwarding attempts failed: {last_err}", backoffs.len() + 1
        )))
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
    read_concurrency: usize,
) -> Result<HaQLite> {
    ensure_schema(&db_path, schema)?;

    // Subscribe to role events BEFORE join.
    let role_rx = coordinator.role_events();

    // Join the HA cluster.
    let JoinResult { role: initial_role, caught_up, position } =
        coordinator.join(db_name, &db_path).await?;

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
        read_semaphore: tokio::sync::Semaphore::new(read_concurrency),
        follower_caught_up: caught_up,
        follower_replay_position: position,
        mode: HaMode::Dedicated,
        shared_lease_store: None,
        shared_manifest_store: None,
        shared_replicator: None,
        shared_walrust_storage: None,
        shared_prefix: String::new(),
        shared_instance_id: String::new(),
        write_mutex: tokio::sync::Mutex::new(()),
        cached_manifest_version: AtomicU64::new(0),
        write_timeout: DEFAULT_FORWARD_TIMEOUT,
        lease_ttl: 5,
        #[cfg(feature = "turbolite")]
        shared_turbolite_vfs: None,
        #[cfg(feature = "turbolite")]
        shared_turbolite_vfs_name: None,
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
        closed: false,
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
            Ok(RoleEvent::ManifestChanged { db_name, version }) => {
                tracing::debug!("manifest changed for '{}' to v{}", db_name, version);
            }
            Err(_) => {
                tracing::error!("HaQLite: role event channel closed");
                break;
            }
        }
    }
}

// ============================================================================
// Shared mode: open + manifest poller
// ============================================================================

#[allow(clippy::too_many_arguments)]
async fn open_shared(
    lease_store: Arc<dyn hadb::LeaseStore>,
    manifest_store: Arc<dyn hadb::ManifestStore>,
    replicator: Arc<SqliteReplicator>,
    walrust_storage: Arc<dyn walrust::StorageBackend>,
    db_path: PathBuf,
    db_name: &str,
    schema: &str,
    prefix: &str,
    instance_id: &str,
    manifest_poll_interval: Duration,
    write_timeout: Duration,
    read_concurrency: usize,
    lease_ttl: u64,
) -> Result<HaQLite> {
    ensure_schema(&db_path, schema)?;

    // Initial catch-up: restore from S3 if data exists.
    // Must happen BEFORE add() because add() takes a snapshot that would
    // overwrite existing data in S3 with this node's (possibly empty) DB.
    let _ = replicator.restore(db_name, &db_path).await;

    // Register database with replicator (required before sync() can flush WAL).
    // This snapshots the current local DB (which now includes restored data).
    replicator.add(db_name, &db_path).await?;

    // Check current manifest version
    let manifest_key = format!("{}{}/_manifest", prefix, db_name);
    let cached_version = match manifest_store.meta(&manifest_key).await? {
        Some(meta) => meta.version,
        None => 0,
    };

    // Open RW connection (every node can write in Shared mode)
    let conn = open_leader_connection(&db_path)?;

    let db_name_owned = db_name.to_string();
    let inner = Arc::new(HaQLiteInner {
        coordinator: None,
        db_name: db_name_owned.clone(),
        db_path: db_path.clone(),
        role: AtomicU8::new(ROLE_LEADER), // Shared mode: always "leader" locally
        conn: RwLock::new(Some(Arc::new(Mutex::new(conn)))),
        leader_address: RwLock::new(String::new()),
        http_client: reqwest::Client::new(),
        secret: None,
        read_semaphore: tokio::sync::Semaphore::new(read_concurrency),
        follower_caught_up: Arc::new(AtomicBool::new(true)),
        follower_replay_position: Arc::new(AtomicU64::new(0)),
        // Shared mode fields
        mode: HaMode::Shared,
        shared_lease_store: Some(lease_store),
        shared_manifest_store: Some(manifest_store),
        shared_replicator: Some(replicator),
        shared_walrust_storage: Some(walrust_storage),
        shared_prefix: prefix.to_string(),
        shared_instance_id: instance_id.to_string(),
        write_mutex: tokio::sync::Mutex::new(()),
        cached_manifest_version: AtomicU64::new(cached_version),
        write_timeout,
        lease_ttl,
        #[cfg(feature = "turbolite")]
        shared_turbolite_vfs: None,
        #[cfg(feature = "turbolite")]
        shared_turbolite_vfs_name: None,
    });

    // Spawn manifest polling background task
    let poller_inner = inner.clone();
    let poller_db_name = db_name_owned.clone();
    let poller_manifest_key = manifest_key.clone();
    let role_handle = tokio::spawn(async move {
        run_manifest_poller(poller_inner, poller_db_name, poller_manifest_key, manifest_poll_interval).await;
    });

    // No forwarding server in Shared mode
    let fwd_handle = tokio::spawn(async {});

    tracing::info!(
        "HaQLite opened in Shared mode: db='{}', manifest_poll={}ms",
        db_name, manifest_poll_interval.as_millis(),
    );

    Ok(HaQLite {
        inner,
        _fwd_handle: fwd_handle,
        _role_handle: role_handle,
        closed: false,
    })
}

async fn run_manifest_poller(
    inner: Arc<HaQLiteInner>,
    db_name: String,
    manifest_key: String,
    poll_interval: Duration,
) {
    let mut interval = tokio::time::interval(poll_interval);
    loop {
        interval.tick().await;
        if let Some(ref ms) = inner.shared_manifest_store {
            match ms.meta(&manifest_key).await {
                Ok(Some(meta)) => {
                    let cached = inner.cached_manifest_version.load(Ordering::SeqCst);
                    if meta.version > cached {
                        // Restore from S3 (full snapshot + all incrementals)
                        if let Some(ref rep) = inner.shared_replicator {
                            match rep.restore(&db_name, &inner.db_path).await {
                                Ok(_) => {
                                    inner.cached_manifest_version.store(meta.version, Ordering::SeqCst);
                                    tracing::debug!(
                                        "manifest poller: caught up '{}' to v{}",
                                        db_name, meta.version,
                                    );
                                }
                                Err(e) => {
                                    tracing::error!("manifest poller: restore failed for '{}': {}", db_name, e);
                                }
                            }
                        }
                    }
                }
                Ok(None) => {} // no manifest yet
                Err(e) => {
                    tracing::error!("manifest poller: meta check failed for '{}': {}", db_name, e);
                }
            }
        }
    }
}

// ============================================================================
// Turbolite shared mode: open + manifest poller
// ============================================================================

#[cfg(feature = "turbolite")]
#[allow(clippy::too_many_arguments)]
async fn open_shared_turbolite(
    lease_store: Arc<dyn hadb::LeaseStore>,
    manifest_store: Arc<dyn hadb::ManifestStore>,
    vfs: turbolite::tiered::SharedTurboliteVfs,
    vfs_name: &str,
    db_path: PathBuf,
    db_name: &str,
    schema: &str,
    prefix: &str,
    instance_id: &str,
    manifest_poll_interval: Duration,
    write_timeout: Duration,
    read_concurrency: usize,
) -> Result<HaQLite> {
    // Initial catch-up from manifest
    let replicator = Arc::new(crate::turbolite_replicator::TurboliteReplicator::new(
        vfs.clone(), manifest_store.clone(), prefix, db_name,
    ));
    let _ = replicator.pull(db_name, &db_path).await;

    // Ensure schema via turbolite VFS connection
    let vfs_uri = format!("file:{}?vfs={}", db_path.display(), vfs_name);
    {
        let conn = rusqlite::Connection::open_with_flags(
            &vfs_uri,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )?;
        conn.execute_batch(schema)?;
    }

    // Open persistent connection via turbolite VFS
    let conn = rusqlite::Connection::open_with_flags(
        &vfs_uri,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )?;

    let manifest_key = format!("{}{}/_manifest", prefix, db_name);
    let cached_version = match manifest_store.meta(&manifest_key).await? {
        Some(meta) => meta.version,
        None => 0,
    };

    let db_name_owned = db_name.to_string();
    let inner = Arc::new(HaQLiteInner {
        coordinator: None,
        db_name: db_name_owned.clone(),
        db_path: db_path.clone(),
        role: AtomicU8::new(ROLE_LEADER),
        conn: RwLock::new(Some(Arc::new(Mutex::new(conn)))),
        leader_address: RwLock::new(String::new()),
        http_client: reqwest::Client::new(),
        secret: None,
        read_semaphore: tokio::sync::Semaphore::new(read_concurrency),
        follower_caught_up: Arc::new(AtomicBool::new(true)),
        follower_replay_position: Arc::new(AtomicU64::new(0)),
        // Shared mode fields
        mode: HaMode::Shared,
        shared_lease_store: Some(lease_store),
        shared_manifest_store: Some(manifest_store.clone()),
        shared_replicator: None, // walrust replicator not used
        shared_walrust_storage: None,
        shared_prefix: prefix.to_string(),
        shared_instance_id: instance_id.to_string(),
        write_mutex: tokio::sync::Mutex::new(()),
        cached_manifest_version: AtomicU64::new(cached_version),
        write_timeout,
        lease_ttl: 5,
        shared_turbolite_vfs: Some(vfs.clone()),
        shared_turbolite_vfs_name: Some(vfs_name.to_string()),
    });

    // Manifest poller (turbolite variant)
    let poller_inner = inner.clone();
    let poller_manifest_key = manifest_key;
    let poller_vfs = vfs.clone();
    let role_handle = tokio::spawn(async move {
        run_manifest_poller_turbolite(
            poller_inner, poller_manifest_key,
            manifest_poll_interval, poller_vfs,
        ).await;
    });

    // No forwarding server in Shared mode
    let fwd_handle = tokio::spawn(async {});

    tracing::info!(
        "HaQLite opened in Shared+Turbolite mode: db='{}', manifest_poll={}ms",
        db_name, manifest_poll_interval.as_millis(),
    );

    Ok(HaQLite {
        inner,
        _fwd_handle: fwd_handle,
        _role_handle: role_handle,
        closed: false,
    })
}

#[cfg(feature = "turbolite")]
async fn run_manifest_poller_turbolite(
    inner: Arc<HaQLiteInner>,
    manifest_key: String,
    poll_interval: Duration,
    vfs: turbolite::tiered::SharedTurboliteVfs,
) {
    let mut interval = tokio::time::interval(poll_interval);
    loop {
        interval.tick().await;
        if let Some(ref ms) = inner.shared_manifest_store {
            match ms.meta(&manifest_key).await {
                Ok(Some(meta)) => {
                    let cached = inner.cached_manifest_version.load(Ordering::SeqCst);
                    if meta.version > cached {
                        match ms.get(&manifest_key).await {
                            Ok(Some(ha_manifest)) => {
                                if let hadb::StorageManifest::Turbolite { .. } = &ha_manifest.storage {
                                    let tl_manifest = crate::turbolite_replicator::ha_storage_to_turbolite(&ha_manifest.storage);
                                    vfs.set_manifest(tl_manifest);
                                    inner.cached_manifest_version.store(meta.version, Ordering::SeqCst);
                                    tracing::debug!(
                                        "turbolite manifest poller: caught up to v{}",
                                        meta.version,
                                    );
                                }
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::error!("turbolite manifest poller: get failed: {}", e);
                            }
                        }
                    }
                }
                Ok(None) => {} // no manifest yet
                Err(e) => {
                    tracing::error!("turbolite manifest poller: meta check failed: {}", e);
                }
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
