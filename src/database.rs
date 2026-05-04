//! HaQLite: dead-simple embedded HA SQLite.
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! use haqlite::{HaQLite, SqlValue};
//!
//! let mut db = HaQLite::builder()
//!     .lease_store(my_lease_store)
//!     .walrust_storage(my_storage)
//!     .open("/data/my.db", "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
//!     .await?;
//!
//! db.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Alice".into())])?;
//! let count: i64 = db.query_row_local("SELECT COUNT(*) FROM users", &[], |r| r.get(0))?;
//! # Ok(())
//! # }
//! ```

use parking_lot::{Mutex, RwLock};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::routing::post;
use hadb::{Coordinator, CoordinatorConfig, JoinResult, LeaseConfig, Role, RoleEvent};
use hadb_lease_s3::S3LeaseStore;

use crate::error::HaQLiteError;
use crate::follower_behavior::SqliteFollowerBehavior;
use crate::forwarding::{self, ForwardingState, SqlValue};

/// Custom authorizer callback for SQLite connections.
///
/// Called by haqlite when applying or updating the connection authorizer.
/// The `fenced` parameter indicates whether the database has lost its lease
/// and writes should be blocked.
///
/// When `fenced=false`, the authorizer should enforce security policy
/// (e.g. block ATTACH, dangerous functions, non-allowlisted PRAGMAs).
/// When `fenced=true`, the authorizer should additionally block all writes.
///
/// The returned closure is installed as the SQLite authorizer via
/// `conn.authorizer()`. It must be `FnMut` + `Send` + `'static`.
pub type AuthorizerFactory = Arc<
    dyn Fn(
            bool,
        ) -> Box<
            dyn FnMut(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization + Send,
        > + Send
        + Sync,
>;
use crate::replicator::SqliteReplicator;

const DEFAULT_PREFIX: &str = "haqlite/";
const DEFAULT_FORWARDING_PORT: u16 = 18080;
const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

const ROLE_LEADER: u8 = 0;
const ROLE_FOLLOWER: u8 = 1;
const ROLE_CLIENT: u8 = 2;
const ROLE_LATENT_WRITER: u8 = 3;

/// How a singlewriter-mode leader accepts forwarded writes from followers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardingMode {
    /// Use haqlite's built-in HTTP forwarding server on the given port.
    BuiltinHttp { port: u16 },
    /// Disable haqlite's built-in forwarding server entirely.
    ///
    /// Use this when a higher-level system already handles leader routing,
    /// redirects, or request replay.
    Disabled,
}

impl ForwardingMode {
    fn port(self) -> Option<u16> {
        match self {
            ForwardingMode::BuiltinHttp { port } => Some(port),
            ForwardingMode::Disabled => None,
        }
    }
}

// Re-export canonical mode/durability types from hadb.
// Shared across haqlite and hakuzu for consistent validation.
pub use hadb::{validate_mode_durability, validate_mode_role, Durability, HaMode};

/// Builder for creating an HA SQLite instance.
///
/// SingleWriter mode requires an explicit lease store and WAL storage.
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// use haqlite::HaQLite;
///
/// let mut db = HaQLite::builder()
///     .lease_store(my_lease_store)
///     .walrust_storage(my_storage)
///     .prefix("myapp/")
///     .forwarding_port(19000)
///     .open("/data/my.db", "CREATE TABLE IF NOT EXISTS ...")
///     .await?;
/// # Ok(())
/// # }
/// ```
const DEFAULT_READ_CONCURRENCY: usize = 32;

pub struct HaQLiteBuilder {
    prefix: String,
    endpoint: Option<String>,
    instance_id: Option<String>,
    address: Option<String>,
    forwarding_mode: ForwardingMode,
    forward_timeout: Duration,
    coordinator_config: Option<CoordinatorConfig>,
    secret: Option<String>,
    read_concurrency: usize,
    lease_store: Option<Arc<dyn hadb::LeaseStore>>,
    mode: HaMode,
    role: Option<Role>,
    durability: Option<hadb::Durability>,
    manifest_poll_interval: Option<Duration>,
    write_timeout: Option<Duration>,
    walrust_storage: Option<Arc<dyn hadb_storage::StorageBackend>>,
    lease_ttl: Option<u64>,
    lease_renew_interval: Option<Duration>,
    lease_follower_poll_interval: Option<Duration>,
    authorizer: Option<AuthorizerFactory>,
}

impl Default for HaQLiteBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HaQLiteBuilder {
    pub fn new() -> Self {
        Self {
            prefix: DEFAULT_PREFIX.to_string(),
            endpoint: None,
            instance_id: None,
            address: None,
            forwarding_mode: ForwardingMode::BuiltinHttp {
                port: DEFAULT_FORWARDING_PORT,
            },
            forward_timeout: DEFAULT_FORWARD_TIMEOUT,
            coordinator_config: None,
            secret: None,
            read_concurrency: DEFAULT_READ_CONCURRENCY,
            lease_store: None,
            mode: HaMode::SingleWriter,
            role: None,
            durability: None,
            manifest_poll_interval: None,
            write_timeout: None,
            walrust_storage: None,
            lease_ttl: None,
            lease_renew_interval: None,
            lease_follower_poll_interval: None,
            authorizer: None,
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

    /// Network address for this node (how other nodes or routing layers reach it).
    /// Default: auto-detected from Fly internal DNS or hostname when built-in
    /// forwarding is enabled; empty when forwarding is disabled.
    pub fn address(mut self, addr: &str) -> Self {
        self.address = Some(addr.to_string());
        self
    }

    /// Port for the internal write-forwarding HTTP server. Default: 18080.
    /// Set to `0` to disable forwarding.
    pub fn forwarding_port(mut self, port: u16) -> Self {
        self.forwarding_mode = if port == 0 {
            ForwardingMode::Disabled
        } else {
            ForwardingMode::BuiltinHttp { port }
        };
        self
    }

    /// Set the forwarding strategy explicitly.
    pub fn forwarding_mode(mut self, mode: ForwardingMode) -> Self {
        self.forwarding_mode = mode;
        self
    }

    /// Disable haqlite's built-in leader forwarding server.
    pub fn disable_forwarding(mut self) -> Self {
        self.forwarding_mode = ForwardingMode::Disabled;
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
    /// Works with any `LeaseStore` implementation: NATS, Redis, etcd, HTTP, etc.
    /// When set, the builder skips S3LeaseStore construction in `open()`.
    pub fn lease_store(mut self, store: Arc<dyn hadb::LeaseStore>) -> Self {
        self.lease_store = Some(store);
        self
    }

    /// Use the Cinch-protocol HTTP LeaseStore (for embedded replicas via
    /// Grabby or the engine's embedded lease API).
    ///
    /// Shorthand for `.lease_store(Arc::new(CinchLeaseStore::new(endpoint, token)))`.
    pub fn lease_endpoint(self, endpoint: &str, token: &str) -> Self {
        self.lease_store(Arc::new(hadb_lease_cinch::CinchLeaseStore::new(
            endpoint, token,
        )))
    }

    /// Set the coordination topology. Default: `HaMode::SingleWriter`.
    pub fn mode(mut self, mode: HaMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set a self-declared role.
    ///
    /// Leave unset for the normal SingleWriter path, where the lease outcome
    /// assigns `Leader` or `Follower`. `Client` and `LatentWriter` are visible
    /// in the type system now, but their base-haqlite implementations are not
    /// wired yet and will fail clearly during `open()`.
    pub fn role(mut self, role: Role) -> Self {
        self.role = Some(role);
        self
    }

    /// Walrust-only path: plain HA SQLite with WAL shipping.
    /// Default `hadb::Durability::Replicated(1s)`.
    pub fn durability(mut self, durability: hadb::Durability) -> Self {
        self.durability = Some(durability);
        self
    }

    /// SharedWriter mode manifest polling interval. Default: 1s.
    /// Manifest polling interval for SharedWriter mode. Default: 1s.
    pub fn manifest_poll_interval(mut self, interval: Duration) -> Self {
        self.manifest_poll_interval = Some(interval);
        self
    }

    /// Write timeout for lease acquisition in SharedWriter mode. Default: 5s.
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = Some(timeout);
        self
    }

    /// Lease TTL in seconds. Applies to both SingleWriter mode (fed into the
    /// Coordinator's `LeaseConfig`) and SharedWriter mode (used for lease acquisition
    /// during writes). Default: 5.
    pub fn lease_ttl(mut self, ttl_secs: u64) -> Self {
        self.lease_ttl = Some(ttl_secs);
        self
    }

    /// Lease renewal interval (SingleWriter mode). How often the leader refreshes
    /// its lease in the backing store. Default: 2s.
    pub fn lease_renew_interval(mut self, interval: Duration) -> Self {
        self.lease_renew_interval = Some(interval);
        self
    }

    /// Follower poll interval (SingleWriter mode). How often followers check the
    /// lease for leader death. Default: 1s.
    pub fn lease_follower_poll_interval(mut self, interval: Duration) -> Self {
        self.lease_follower_poll_interval = Some(interval);
        self
    }

    /// Use a custom storage backend for walrust's WAL/snapshot writes.
    /// Pass any `Arc<dyn hadb_storage::StorageBackend>` (S3, Cinch HTTP,
    /// in-memory, etc.). For tests this is the in-memory backend; for
    /// production it's typically `hadb_storage_s3::S3Storage` or
    /// `hadb_storage_cinch::CinchHttpStorage`.
    pub fn walrust_storage(mut self, storage: Arc<dyn hadb_storage::StorageBackend>) -> Self {
        self.walrust_storage = Some(storage);
        self
    }

    /// Custom authorizer factory for SQLite connections.
    ///
    /// The factory is called with `fenced: bool` and must return a closure
    /// that will be installed as the SQLite authorizer. When `fenced=false`,
    /// enforce your security sandbox (block ATTACH, etc.). When `fenced=true`,
    /// additionally block all write operations.
    ///
    /// If not set, haqlite uses its built-in authorizer (read-only allowlist
    /// when fenced, no authorizer when unfenced).
    pub fn authorizer<F, G>(mut self, factory: F) -> Self
    where
        F: Fn(bool) -> G + Send + Sync + 'static,
        G: FnMut(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization
            + Send
            + 'static,
    {
        self.authorizer = Some(Arc::new(move |fenced| Box::new(factory(fenced))));
        self
    }

    pub fn get_prefix(&self) -> &str {
        &self.prefix
    }
    pub fn get_endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
    pub fn get_instance_id(&self) -> Option<&str> {
        self.instance_id.as_deref()
    }
    pub fn get_address(&self) -> Option<&str> {
        self.address.as_deref()
    }
    pub fn get_forwarding_port(&self) -> u16 {
        self.forwarding_mode.port().unwrap_or(0)
    }
    pub fn get_forwarding_mode(&self) -> ForwardingMode {
        self.forwarding_mode
    }
    pub fn get_forward_timeout(&self) -> Duration {
        self.forward_timeout
    }
    pub fn get_coordinator_config(&self) -> Option<&CoordinatorConfig> {
        self.coordinator_config.as_ref()
    }
    pub fn get_secret(&self) -> Option<&str> {
        self.secret.as_deref()
    }
    pub fn get_read_concurrency(&self) -> usize {
        self.read_concurrency
    }
    pub fn get_lease_store(&self) -> Option<&Arc<dyn hadb::LeaseStore>> {
        self.lease_store.as_ref()
    }
    pub fn get_mode(&self) -> HaMode {
        self.mode
    }
    pub fn get_role(&self) -> Option<Role> {
        self.role
    }
    pub fn get_durability(&self) -> Option<hadb::Durability> {
        self.durability
    }
    pub fn get_manifest_poll_interval(&self) -> Option<Duration> {
        self.manifest_poll_interval
    }
    pub fn get_write_timeout(&self) -> Option<Duration> {
        self.write_timeout
    }
    pub fn get_walrust_storage(&self) -> Option<&Arc<dyn hadb_storage::StorageBackend>> {
        self.walrust_storage.as_ref()
    }
    pub fn get_lease_ttl(&self) -> Option<u64> {
        self.lease_ttl
    }
    pub fn get_lease_renew_interval(&self) -> Option<Duration> {
        self.lease_renew_interval
    }
    pub fn get_lease_follower_poll_interval(&self) -> Option<Duration> {
        self.lease_follower_poll_interval
    }
    pub fn get_authorizer(&self) -> Option<&AuthorizerFactory> {
        self.authorizer.as_ref()
    }

    /// Open the database and join the HA cluster.
    ///
    /// `schema` is run once on first open (e.g. CREATE TABLE IF NOT EXISTS ...).
    pub async fn open(self, db_path: &str, schema: &str) -> Result<HaQLite> {
        let role_for_validation = self.role.unwrap_or(match self.mode {
            HaMode::SingleWriter => Role::Leader,
            HaMode::SharedWriter => Role::LatentWriter,
        });
        validate_mode_role(self.mode, role_for_validation)
            .map_err(|e| anyhow::anyhow!("invalid mode/role combination: {e}"))?;

        match (self.mode, self.role) {
            (HaMode::SharedWriter, Some(Role::Client)) => {
                anyhow::bail!("Client mode not yet implemented in base haqlite")
            }
            (HaMode::SharedWriter, None | Some(Role::LatentWriter)) => {
                anyhow::bail!(
                    "SharedWriter not implemented in base haqlite - would require sync WAL replication before commit ack"
                )
            }
            (
                HaMode::SingleWriter,
                None | Some(Role::Leader) | Some(Role::Follower) | Some(Role::Client),
            ) => {}
            (HaMode::SingleWriter, Some(Role::LatentWriter))
            | (HaMode::SharedWriter, Some(Role::Leader | Role::Follower)) => {
                unreachable!("mode/role validation should reject this combination")
            }
        }

        let db_path = PathBuf::from(db_path);
        let db_name = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("db")
            .to_string();

        let instance_id = self.instance_id.unwrap_or_else(|| {
            std::env::var("FLY_MACHINE_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
        });
        let forwarding_mode = self.forwarding_mode;
        let address = self.address.unwrap_or_else(|| match forwarding_mode {
            ForwardingMode::BuiltinHttp { port } => detect_address(&instance_id, port),
            ForwardingMode::Disabled => String::new(),
        });

        let walrust_durability = self.durability.unwrap_or_else(hadb::Durability::default);

        let walrust_storage = self
            .walrust_storage
            .ok_or_else(|| anyhow::anyhow!("SingleWriter mode requires walrust storage"))?;

        let lease_store: Arc<dyn hadb::LeaseStore> = self.lease_store.ok_or_else(|| {
            anyhow::anyhow!(
                "HaQLiteBuilder requires lease_store(). Pass an Arc<dyn hadb::LeaseStore>, \
                 or call haqlite::env::lease_store_from_env(bucket, endpoint).await?"
            )
        })?;

        let mut config = self.coordinator_config.unwrap_or_default();
        let mut lease_cfg = match config.lease.take() {
            Some(mut existing) => {
                existing.store = lease_store.clone();
                existing.instance_id = instance_id.clone();
                existing.address = address.clone();
                existing
            }
            None => LeaseConfig::new(lease_store.clone(), instance_id.clone(), address.clone()),
        };
        if let Some(ttl) = self.lease_ttl {
            lease_cfg.ttl_secs = ttl;
        }
        if let Some(d) = self.lease_renew_interval {
            lease_cfg.renew_interval = d;
        }
        if let Some(d) = self.lease_follower_poll_interval {
            lease_cfg.follower_poll_interval = d;
        }
        config.lease = Some(lease_cfg);
        config.requested_role = self.role;

        let (sync_interval, skip_snapshot) = match walrust_durability {
            hadb::Durability::Replicated(dur) => (dur, false),
            hadb::Durability::Local => (Duration::from_secs(3600), false),
            hadb::Durability::SyncReplicated => (Duration::from_millis(1), false),
        };

        let replication_config = walrust::ReplicationConfig {
            sync_interval,
            snapshot_interval: config.snapshot_interval,
            autonomous_snapshots: true,
            ..Default::default()
        };
        let replicator = Arc::new(
            SqliteReplicator::new(walrust_storage.clone(), &self.prefix, replication_config)
                .with_skip_snapshot(skip_snapshot),
        );

        let follower_behavior: Arc<dyn hadb::FollowerBehavior> =
            Arc::new(SqliteFollowerBehavior::new(walrust_storage.clone()));

        let coordinator = Coordinator::new(
            replicator,
            None,
            None,
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
            forwarding_mode,
            self.forward_timeout,
            self.secret,
            self.read_concurrency,
            self.authorizer,
            None,
            None,
            None,
            false,
        )
        .await
    }
}

/// HA SQLite database — transparent write forwarding, local reads, automatic failover.
///
/// Create via `HaQLite::builder().<wire-storage>.open(path, schema).await?`
/// or `HaQLite::local(path, schema)?` for single-node mode.
/// HA SQLite database with transparent write forwarding, local reads, and automatic failover.
///
/// **You MUST call [`.close().await`](HaQLite::close) before dropping.** If dropped without
/// close(), background tasks are aborted and the lease is not cleanly released, which may
/// block other nodes from acquiring the lease until TTL expires.
///
/// Create via `HaQLite::builder().<wire-storage>.open(path, schema).await?`
/// or `HaQLite::local(path, schema)?` for single-node mode.
pub struct HaQLite {
    inner: Arc<HaQLiteInner>,
    _role_handle: tokio::task::JoinHandle<()>,
    closed: bool,
}

impl Drop for HaQLite {
    fn drop(&mut self) {
        if !self.closed {
            // Abort background tasks so they don't leak.
            // Note: this does NOT cleanly release the lease. Call close().await for that.
            // Forwarding server is managed by inner (stopped via stop_forwarding_server).
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
pub struct HaQLiteInner {
    pub coordinator: Option<Arc<Coordinator>>,
    pub db_name: String,
    pub db_path: PathBuf,
    /// Cached role -- updated atomically by the role event listener.
    pub role: AtomicU8,
    /// Read-write connection when leader, None when follower.
    /// ArcSwapOption: lock-free reads (every execute/query), rare writes (role change).
    pub conn: arc_swap::ArcSwapOption<Mutex<rusqlite::Connection>>,
    /// Leader's forwarding address (read from S3 lease, updated on role change).
    pub leader_address: RwLock<String>,
    pub http_client: reqwest::Client,
    /// Shared secret for authenticating forwarding requests.
    pub secret: Option<String>,
    /// Limits concurrent follower reads. Closed on shutdown.
    pub read_semaphore: tokio::sync::Semaphore,
    /// Whether the follower has caught up with the leader's WAL.
    pub follower_caught_up: Arc<AtomicBool>,
    /// Current follower replay position (TXID).
    pub follower_replay_position: Arc<AtomicU64>,
    // SharedWriter mode fields
    pub mode: HaMode,
    /// Direct lease store access (SharedWriter mode, bypasses Coordinator).
    pub shared_lease_store: Option<Arc<dyn hadb::LeaseStore>>,
    /// Direct replicator access (SharedWriter mode).
    pub shared_replicator: Option<Arc<SqliteReplicator>>,
    /// Walrust storage backend for pull_incremental.
    pub shared_walrust_storage: Option<Arc<dyn hadb_storage::StorageBackend>>,
    /// S3 prefix for lease/manifest keys.
    pub shared_prefix: String,
    /// Instance ID for this node.
    pub shared_instance_id: String,
    /// Serializes all writes in SharedWriter mode.
    pub write_mutex: tokio::sync::Mutex<()>,
    /// Cached manifest version for freshness checks.
    pub cached_manifest_version: AtomicU64,
    /// Write timeout for lease acquisition.
    pub write_timeout: Duration,
    /// Lease TTL for shared-writer mode leases.
    pub lease_ttl: u64,
    /// Schema SQL to apply on first write (deferred from open for S3Primary compat).
    pub schema_sql: Option<String>,
    /// Whether schema has been applied on this node.
    pub schema_applied: AtomicBool,
    /// How leader write forwarding is handled for this database.
    pub forwarding_mode: ForwardingMode,
    /// Running forwarding server task. Started when leader, stopped on demotion.
    pub fwd_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Custom authorizer factory. If set, used instead of built-in fence/unfence authorizer.
    pub authorizer: Option<AuthorizerFactory>,
    /// Optional lazy connection opener. Used by sibling crates to open
    /// connections via a custom VFS on first use.
    pub connection_opener:
        Option<Arc<dyn Fn() -> Result<rusqlite::Connection, HaQLiteError> + Send + Sync>>,
    /// Optional follower read opener. When set, follower reads use this surface
    /// instead of the normal connection opener.
    pub follower_read_connection_opener:
        Option<Arc<dyn Fn() -> Result<rusqlite::Connection, HaQLiteError> + Send + Sync>>,
    /// Optional flush callback for tiered storage (turbolite). Set by sibling
    /// crates that inject a custom VFS; base haqlite never sets this.
    pub on_flush: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
}

impl HaQLiteInner {
    /// Create a new HaQLiteInner with default values for optional fields.
    /// Primarily for use by sibling crates and advanced use cases.
    pub fn new(
        db_path: PathBuf,
        db_name: String,
        role: AtomicU8,
        conn: arc_swap::ArcSwapOption<Mutex<rusqlite::Connection>>,
        http_client: reqwest::Client,
        mode: HaMode,
        forwarding_mode: ForwardingMode,
    ) -> Self {
        Self {
            coordinator: None,
            db_name,
            db_path,
            role,
            conn,
            leader_address: RwLock::new(String::new()),
            http_client,
            secret: None,
            read_semaphore: tokio::sync::Semaphore::new(DEFAULT_READ_CONCURRENCY),
            follower_caught_up: Arc::new(AtomicBool::new(true)),
            follower_replay_position: Arc::new(AtomicU64::new(0)),
            mode,
            shared_lease_store: None,
            shared_replicator: None,
            shared_walrust_storage: None,
            shared_prefix: String::new(),
            shared_instance_id: String::new(),
            write_mutex: tokio::sync::Mutex::new(()),
            cached_manifest_version: AtomicU64::new(0),
            write_timeout: DEFAULT_FORWARD_TIMEOUT,
            lease_ttl: 5,
            schema_sql: None,
            schema_applied: AtomicBool::new(true),
            forwarding_mode,
            fwd_handle: tokio::sync::Mutex::new(None),
            authorizer: None,
            connection_opener: None,
            follower_read_connection_opener: None,
            on_flush: None,
        }
    }

    /// Ensure a connection is available, using the lazy opener if set.
    /// For sibling crates and advanced use cases.
    pub fn ensure_conn(&self) -> Result<Arc<Mutex<rusqlite::Connection>>, HaQLiteError> {
        if let Some(conn) = self.conn.load_full() {
            return Ok(conn);
        }
        if let Some(ref opener) = self.connection_opener {
            let new_conn = opener().map_err(|e| e)?;
            let arc = Arc::new(Mutex::new(new_conn));
            self.set_conn(Some(arc.clone()));
            return Ok(arc);
        }
        Err(HaQLiteError::DatabaseError(
            "No write connection available (not leader?)".into(),
        ))
    }

    /// Start the write-forwarding HTTP server. Only leaders need this.
    /// Followers send writes to the leader; they don't listen.
    async fn start_forwarding_server(self: &Arc<Self>) {
        let port = match self.forwarding_mode {
            ForwardingMode::BuiltinHttp { port } => port,
            ForwardingMode::Disabled => {
                tracing::debug!(db = %self.db_name, "Forwarding server disabled");
                return;
            }
        };
        let mut handle = self.fwd_handle.lock().await;
        if handle.is_some() {
            return; // Already running
        }
        let fwd_state = Arc::new(ForwardingState {
            inner: self.clone(),
        });
        let fwd_app = axum::Router::new()
            .route(
                "/haqlite/execute",
                axum::routing::post(forwarding::handle_forwarded_execute),
            )
            .route(
                "/haqlite/query",
                axum::routing::post(forwarding::handle_forwarded_query),
            )
            .with_state(fwd_state);
        match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await {
            Ok(listener) => {
                let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
                tracing::debug!(db = %self.db_name, port, "Forwarding server started");
                *handle = Some(tokio::spawn(async move {
                    if let Err(e) = axum::serve(listener, fwd_app).await {
                        tracing::error!("Forwarding server error: {}", e);
                    }
                }));
            }
            Err(e) => {
                tracing::error!(db = %self.db_name, "Failed to bind forwarding server: {}", e);
            }
        }
    }

    /// Stop the write-forwarding server (on demotion to follower).
    async fn stop_forwarding_server(&self) {
        let mut handle = self.fwd_handle.lock().await;
        if let Some(h) = handle.take() {
            h.abort();
            tracing::debug!(db = %self.db_name, "Forwarding server stopped");
        }
    }

    /// Get current role (lock-free atomic read).
    pub(crate) fn current_role(&self) -> Option<Role> {
        match self.role.load(Ordering::SeqCst) {
            ROLE_LEADER => Some(Role::Leader),
            ROLE_FOLLOWER => Some(Role::Follower),
            ROLE_CLIENT => Some(Role::Client),
            ROLE_LATENT_WRITER => Some(Role::LatentWriter),
            _ => None,
        }
    }

    /// Path that walrust uses for file-level operations (snapshot/restore/WAL sync).
    fn set_role(&self, role: Role) {
        self.role.store(
            match role {
                Role::Leader => ROLE_LEADER,
                Role::Follower => ROLE_FOLLOWER,
                Role::Client => ROLE_CLIENT,
                Role::LatentWriter => ROLE_LATENT_WRITER,
            },
            Ordering::SeqCst,
        );
    }

    /// Open a leader connection, using the custom opener if set.
    pub fn try_open_leader_conn(&self) -> Result<rusqlite::Connection, HaQLiteError> {
        if let Some(ref opener) = self.connection_opener {
            return opener();
        }
        open_leader_connection(&self.db_path)
            .map_err(|e| HaQLiteError::DatabaseError(format!("open leader connection: {e}")))
    }

    fn apply_schema_if_needed(&self, conn: &rusqlite::Connection) -> Result<(), HaQLiteError> {
        if self.schema_applied.load(Ordering::SeqCst) {
            return Ok(());
        }

        let Some(schema) = self.schema_sql.as_deref() else {
            self.schema_applied.store(true, Ordering::SeqCst);
            return Ok(());
        };

        if !schema.trim().is_empty() {
            conn.execute_batch(schema).map_err(|e| {
                HaQLiteError::DatabaseError(format!("apply schema after join failed: {e}"))
            })?;
            if let Some(ref callback) = self.on_flush {
                callback().map_err(|e| {
                    HaQLiteError::DatabaseError(format!("flush after schema failed: {e}"))
                })?;
            }
        }

        self.schema_applied.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn leader_addr(&self) -> std::result::Result<String, HaQLiteError> {
        Ok(self.leader_address.read().clone())
    }

    fn set_leader_addr(&self, addr: String) {
        *self.leader_address.write() = addr;
    }

    pub fn set_conn(&self, conn: Option<Arc<Mutex<rusqlite::Connection>>>) {
        self.conn.store(conn);
    }

    /// Block all writes on the connection via SQLite authorizer.
    /// Called on demotion/fencing. The connection stays open for reads.
    fn fence_connection(&self) {
        if let Ok(Some(conn_arc)) = self.get_conn() {
            let conn = conn_arc.lock();
            self.apply_read_only_authorizer(&conn);
            tracing::info!("HaQLite: connection fenced (writes blocked)");
        }
    }

    /// Apply unfenced authorizer (or clear authorizer). Called on promotion.
    fn unfence_connection(&self) {
        if let Ok(Some(conn_arc)) = self.get_conn() {
            let conn = conn_arc.lock();
            if let Some(ref factory) = self.authorizer {
                conn.authorizer(Some(factory(false)));
            } else {
                conn.authorizer(
                    None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>,
                );
            }
            tracing::info!("HaQLite: connection unfenced (writes allowed)");
        }
    }

    /// Apply the custom authorizer (unfenced) to a connection, if one is configured.
    /// Called when a new connection is opened (local mode, promotion, etc.).
    fn apply_initial_authorizer(&self, conn: &rusqlite::Connection) {
        if let Some(ref factory) = self.authorizer {
            conn.authorizer(Some(factory(false)));
        }
    }

    /// Apply a read-only authorizer to a connection.
    ///
    /// Used both for fencing the persistent leader connection on demotion and
    /// for ephemeral follower read connections opened through a custom VFS.
    fn apply_read_only_authorizer(&self, conn: &rusqlite::Connection) {
        if let Some(ref factory) = self.authorizer {
            conn.authorizer(Some(factory(true)));
        } else {
            conn.authorizer(Some(|ctx: rusqlite::hooks::AuthContext<'_>| {
                use rusqlite::hooks::{AuthAction, Authorization};
                match ctx.action {
                    AuthAction::Select
                    | AuthAction::Read { .. }
                    | AuthAction::Function { .. }
                    | AuthAction::Recursive => Authorization::Allow,
                    _ => Authorization::Deny,
                }
            }));
        }
    }

    /// Open a fresh follower read connection.
    ///
    /// Plain haqlite followers read from the on-disk SQLite file, but sibling
    /// crates like haqlite-turbolite install a custom opener that must be used
    /// for *all* connections, including follower reads, so the VFS-backed
    /// database and the query path see the same bytes.
    fn open_follower_read_conn(&self) -> Result<rusqlite::Connection, HaQLiteError> {
        if let Some(ref opener) = self.follower_read_connection_opener {
            let conn = opener()?;
            conn.execute_batch("PRAGMA query_only=ON;").map_err(|e| {
                HaQLiteError::DatabaseError(format!(
                    "failed to enable query_only on follower read connection: {e}"
                ))
            })?;
            self.apply_read_only_authorizer(&conn);
            return Ok(conn);
        }

        if let Some(ref opener) = self.connection_opener {
            let conn = opener()?;
            conn.execute_batch("PRAGMA query_only=ON;").map_err(|e| {
                HaQLiteError::DatabaseError(format!(
                    "failed to enable query_only on follower read connection: {e}"
                ))
            })?;
            self.apply_read_only_authorizer(&conn);
            return Ok(conn);
        }

        rusqlite::Connection::open_with_flags(
            &self.db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| {
            HaQLiteError::DatabaseError(format!("Failed to open read-only connection: {e}"))
        })
    }

    pub(crate) fn get_conn(
        &self,
    ) -> std::result::Result<Option<Arc<Mutex<rusqlite::Connection>>>, HaQLiteError> {
        Ok(self.conn.load_full())
    }
}

impl HaQLite {
    /// Start building an HA SQLite instance. Wire storage explicitly via
    /// `.lease_store(...)` / `.walrust_storage(...)`; bucket-based defaults
    /// no longer ship from the builder. The `haqlite::env` module provides
    /// opt-in env-var helpers that construct S3 stores from
    /// `CINCH_S3_BUCKET` etc., for callers who want the convenience.
    pub fn builder() -> HaQLiteBuilder {
        HaQLiteBuilder::new()
    }

    /// Construct from a pre-built inner and role handle.
    /// For sibling crates and advanced use cases.
    pub fn from_inner(inner: Arc<HaQLiteInner>, role_handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            inner,
            _role_handle: role_handle,
            closed: false,
        }
    }

    /// Flush tiered storage pages if a sibling crate (e.g. haqlite-turbolite)
    /// registered an on_flush callback. No-op otherwise.
    pub fn flush_turbolite(&self) -> Result<()> {
        if let Some(ref callback) = self.inner.on_flush {
            callback()?;
        }
        Ok(())
    }

    /// Open a local-only SQLite database (no HA, no S3).
    ///
    /// Same `execute()`/`query_row()` API. Useful for development and testing.
    pub fn local(db_path: &str, schema: &str) -> Result<HaQLite> {
        Self::local_with_authorizer(db_path, schema, None)
    }

    /// Open a local-only SQLite database with a custom authorizer.
    pub fn local_with_authorizer(
        db_path: &str,
        schema: &str,
        authorizer: Option<AuthorizerFactory>,
    ) -> Result<HaQLite> {
        let db_path = PathBuf::from(db_path);
        // Local mode never uses an opener — discard the (always-None) returned conn.
        let _ = ensure_schema(&db_path, schema, None)?;

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
            conn: arc_swap::ArcSwapOption::new(Some(Arc::new(Mutex::new(conn)))),
            leader_address: RwLock::new(String::new()),
            http_client: reqwest::Client::new(),
            secret: None,
            read_semaphore: tokio::sync::Semaphore::new(DEFAULT_READ_CONCURRENCY),
            follower_caught_up: Arc::new(AtomicBool::new(true)),
            follower_replay_position: Arc::new(AtomicU64::new(0)),
            mode: HaMode::SingleWriter,
            shared_lease_store: None,
            shared_replicator: None,
            shared_walrust_storage: None,
            shared_prefix: String::new(),
            shared_instance_id: String::new(),
            write_mutex: tokio::sync::Mutex::new(()),
            cached_manifest_version: AtomicU64::new(0),
            write_timeout: DEFAULT_FORWARD_TIMEOUT,
            lease_ttl: 5,
            schema_sql: None,
            schema_applied: AtomicBool::new(true),
            authorizer,
            forwarding_mode: ForwardingMode::Disabled,
            fwd_handle: tokio::sync::Mutex::new(None),
            connection_opener: None,
            follower_read_connection_opener: None,
            on_flush: None,
        });

        // Apply custom authorizer (unfenced) on initial connection.
        if let Ok(Some(conn_arc)) = inner.get_conn() {
            let c = conn_arc.lock();
            inner.apply_initial_authorizer(&c);
        }

        // No forwarding server or role listener in local mode.
        let role_handle = tokio::spawn(async {});

        Ok(HaQLite {
            inner,
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
            coordinator,
            db_path,
            schema,
            forwarding_port,
            forward_timeout,
            None,
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
            ForwardingMode::BuiltinHttp {
                port: forwarding_port,
            },
            forward_timeout,
            secret,
            DEFAULT_READ_CONCURRENCY,
            None,  // no custom authorizer for from_coordinator
            None,  // no custom connection opener
            None,  // no custom follower read opener
            None,  // no tiered storage flush
            false, // schema was applied by the plain bootstrap connection
        )
        .await
    }

    /// Execute a write statement. Returns rows affected.
    ///
    /// Execute SQL. Synchronous on the leader (the common path).
    /// On a follower: blocks on HTTP forwarding to the leader.
    /// In shared-writer mode: blocks on lease acquisition + S3 commit.
    pub fn execute(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> std::result::Result<u64, HaQLiteError> {
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
            .iter()
            .map(|p| p as &dyn rusqlite::types::ToSql)
            .collect();
        match self.inner.mode {
            HaMode::SingleWriter => {
                let role = self.inner.current_role();
                match role {
                    Some(Role::Leader) | None => self.execute_local_raw(sql, &param_refs),
                    Some(Role::Follower) => {
                        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
                            HaQLiteError::DatabaseError(
                                "execute forwarding requires tokio runtime".into(),
                            )
                        })?;
                        tokio::task::block_in_place(|| {
                            handle.block_on(self.execute_forwarded(sql, params))
                        })
                    }
                    Some(Role::Client) => Err(HaQLiteError::ConfigurationError(
                        "Client mode not yet implemented in base haqlite".into(),
                    )),
                    Some(Role::LatentWriter) => Err(HaQLiteError::ConfigurationError(
                        "LatentWriter requires SharedWriter mode".into(),
                    )),
                }
            }
            HaMode::SharedWriter => Err(HaQLiteError::ConfigurationError(
                "SharedWriter mode not supported in base haqlite. Use the tiered-storage crate."
                    .into(),
            )),
        }
    }

    /// Async execute for callers already in an async context.
    /// Equivalent to `execute()` but avoids the `block_on` for follower/shared-writer paths.
    ///
    /// The returned future is Send, so it can be used with `tokio::spawn`.
    pub async fn execute_async(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> std::result::Result<u64, HaQLiteError> {
        match self.inner.mode {
            HaMode::SingleWriter => {
                let role = self.inner.current_role();
                match role {
                    Some(Role::Leader) | None => {
                        // param_refs scoped to this synchronous branch only.
                        // Keeping it out of the async scope makes the future Send.
                        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
                            .iter()
                            .map(|p| p as &dyn rusqlite::types::ToSql)
                            .collect();
                        self.execute_local_raw(sql, &param_refs)
                    }
                    Some(Role::Follower) => self.execute_forwarded(sql, params).await,
                    Some(Role::Client) => Err(HaQLiteError::ConfigurationError(
                        "Client mode not yet implemented in base haqlite".into(),
                    )),
                    Some(Role::LatentWriter) => Err(HaQLiteError::ConfigurationError(
                        "LatentWriter requires SharedWriter mode".into(),
                    )),
                }
            }
            HaMode::SharedWriter => Err(HaQLiteError::ConfigurationError(
                "SharedWriter mode not supported in base haqlite. Use the tiered-storage crate."
                    .into(),
            )),
        }
    }

    /// Query a single row from local state. Does NOT catch up from manifest.
    ///
    /// **In SharedWriter mode, this may return stale data.** If another node has written
    /// since this node last caught up, `query_row_local` will not see those writes.
    /// Use [`query_row_fresh`] for consistency in SharedWriter mode.
    ///
    /// In SingleWriter mode: leader reads from persistent connection, follower opens
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
                let conn_arc = self.inner.ensure_conn()?;
                let conn = conn_arc.lock();
                conn.query_row(sql, params, f)
                    .map_err(|e| HaQLiteError::DatabaseError(format!("query_row failed: {e}")))
            }
            Some(Role::Follower) | Some(Role::Client) => {
                // Bound concurrent follower reads via semaphore.
                let _permit = self
                    .inner
                    .read_semaphore
                    .try_acquire()
                    .map_err(|e| match e {
                        tokio::sync::TryAcquireError::Closed => HaQLiteError::EngineClosed,
                        tokio::sync::TryAcquireError::NoPermits => {
                            HaQLiteError::DatabaseError("Too many concurrent reads".into())
                        }
                    })?;
                // Open a fresh follower read connection each time so reads see
                // external replication progress. Sibling crates may route this
                // through a VFS-backed opener instead of the raw db_path.
                let conn = self.inner.open_follower_read_conn()?;
                conn.query_row(sql, params, f)
                    .map_err(|e| HaQLiteError::DatabaseError(format!("query_row failed: {e}")))
            }
            Some(Role::LatentWriter) => Err(HaQLiteError::ConfigurationError(
                "LatentWriter requires SharedWriter mode".into(),
            )),
        }
    }

    /// Deprecated: use [`query_row_local`] (stale reads ok) or [`query_row_fresh`] (consistency required).
    #[deprecated(
        note = "use query_row_local (stale reads ok) or query_row_fresh (consistency required)"
    )]
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
    /// **In SharedWriter mode, this may return stale data.** Use [`query_values_fresh`] for consistency.
    ///
    /// Returns all matching rows. Each row is a Vec of column values.
    /// Returns an empty Vec if no rows match (not an error).
    pub fn query_values_local(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> std::result::Result<Vec<Vec<SqlValue>>, HaQLiteError> {
        // SqlValue implements ToSql directly (zero-copy, no String/Blob clone).
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
            .iter()
            .map(|p| p as &dyn rusqlite::types::ToSql)
            .collect();

        let query_with =
            |conn: &rusqlite::Connection| -> std::result::Result<Vec<Vec<SqlValue>>, HaQLiteError> {
                let mut stmt = conn.prepare(sql).map_err(|e| {
                    HaQLiteError::DatabaseError(format!("query prepare failed: {e}"))
                })?;
                let column_count = stmt.column_count();
                let mut rows_iter = stmt
                    .query(param_refs.as_slice())
                    .map_err(|e| HaQLiteError::DatabaseError(format!("query failed: {e}")))?;
                let mut rows = Vec::new();
                while let Some(row) = rows_iter.next().map_err(|e| {
                    HaQLiteError::DatabaseError(format!("row iteration failed: {e}"))
                })? {
                    let mut vals = Vec::with_capacity(column_count);
                    for i in 0..column_count {
                        let val: rusqlite::types::Value = row.get(i).map_err(|e| {
                            HaQLiteError::DatabaseError(format!("column {i} read failed: {e}"))
                        })?;
                        vals.push(SqlValue::from_rusqlite(val));
                    }
                    rows.push(vals);
                }
                Ok(rows)
            };

        let role = self.inner.current_role();
        match role {
            Some(Role::Leader) | None => {
                let conn_arc = self.inner.ensure_conn()?;
                let conn = conn_arc.lock();
                query_with(&conn)
            }
            Some(Role::Follower) | Some(Role::Client) => {
                let _permit = self
                    .inner
                    .read_semaphore
                    .try_acquire()
                    .map_err(|e| match e {
                        tokio::sync::TryAcquireError::Closed => HaQLiteError::EngineClosed,
                        tokio::sync::TryAcquireError::NoPermits => {
                            HaQLiteError::DatabaseError("Too many concurrent reads".into())
                        }
                    })?;
                let conn = self.inner.open_follower_read_conn()?;
                query_with(&conn)
            }
            Some(Role::LatentWriter) => Err(HaQLiteError::ConfigurationError(
                "LatentWriter requires SharedWriter mode".into(),
            )),
        }
    }

    /// Deprecated: use [`query_values_local`] (stale reads ok) or [`query_values_fresh`] (consistency required).
    #[deprecated(
        note = "use query_values_local (stale reads ok) or query_values_fresh (consistency required)"
    )]
    pub fn query_values(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> std::result::Result<Vec<Vec<SqlValue>>, HaQLiteError> {
        self.query_values_local(sql, params)
    }

    /// Get the current role of this node.
    pub fn role(&self) -> Option<Role> {
        self.inner.current_role()
    }

    /// Get the underlying rusqlite connection.
    ///
    /// This is the primary API for using haqlite. You get the connection,
    /// use it like normal rusqlite (queries, transactions, prepared statements),
    /// and haqlite handles HA (lease, WAL shipping) transparently
    /// at the storage layer.
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = haqlite::HaQLite::builder().open("/data/my.db", "").await?;
    /// let conn = db.connection()?;
    /// let guard = conn.lock();
    /// guard.execute("INSERT INTO users (name) VALUES (?1)", ["Alice"])?;
    /// let count: i64 = guard.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connection(
        &self,
    ) -> std::result::Result<Arc<Mutex<rusqlite::Connection>>, HaQLiteError> {
        self.inner.ensure_conn()
    }

    /// Block all writes on the connection. Reads keep working.
    ///
    /// Called automatically on lease loss (demotion/fencing). Can also be
    /// called explicitly by the engine to fence a database.
    pub fn fence(&self) {
        self.inner.fence_connection();
    }

    /// Allow writes again. Called automatically on promotion.
    pub fn unfence(&self) {
        self.inner.unfence_connection();
    }

    /// Get the database file path.
    pub fn db_path(&self) -> &Path {
        &self.inner.db_path
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
            Some(Role::Follower) | Some(Role::Client) => {
                self.inner.follower_caught_up.load(Ordering::SeqCst)
            }
            Some(Role::LatentWriter) => false,
        }
    }

    /// Current follower replay position (TXID). Returns 0 for leaders/local mode.
    pub fn replay_position(&self) -> u64 {
        self.inner.follower_replay_position.load(Ordering::SeqCst)
    }

    /// Get metrics in Prometheus exposition format.
    /// Returns None in local mode (no coordinator).
    pub fn prometheus_metrics(&self) -> Option<String> {
        self.inner.coordinator.as_ref().map(|c| {
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

        // 2. Stop accepting forwarded writes before the final replication sync.
        // A forwarded write that lands after walrust has measured the WAL but
        // before the connection closes can be acknowledged yet missed by the
        // final changeset.
        self.inner.stop_forwarding_server().await;

        // 3. Wait for any in-flight execute/forwarded-execute using the SQLite
        // connection to finish. The final sync below needs a quiet WAL.
        if let Some(conn_arc) = self.inner.get_conn()? {
            drop(conn_arc.lock());
        }

        // 4. Flush tiered storage pages if this instance can have local writes.
        // Followers only read/apply remote state; asking them to publish on
        // shutdown can call into writer-only replication paths they never
        // registered.
        if matches!(self.inner.current_role(), Some(Role::Leader) | None) {
            self.flush_turbolite()?;
        }

        // 5. Leave the cluster while the SQLite connection is still open.
        // walrust's final sync reads the live WAL during coordinator.leave();
        // closing the last connection first can checkpoint/remove the WAL and
        // turn acknowledged writes into unsynced local-only bytes.
        if let Some(ref coordinator) = self.inner.coordinator {
            coordinator
                .leave(&self.inner.db_name)
                .await
                .map_err(|e| HaQLiteError::CoordinatorError(e.to_string()))?;
        }

        // 6. Now the final sync is done, close the connection.
        self.inner.set_conn(None);

        // 7. Abort background role listener.
        self._role_handle.abort();

        Ok(())
    }

    /// Read with freshness guarantee: checks manifest and catches up before querying.
    /// In SingleWriter mode, this is equivalent to `query_row_local()`.
    pub async fn query_row_fresh<T, F>(
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

    /// Read with freshness guarantee (multi-row version).
    pub async fn query_values_fresh(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> std::result::Result<Vec<Vec<SqlValue>>, HaQLiteError> {
        self.query_values_local(sql, params)
    }

    /// Acquire the distributed write lease with retry + backoff.
    ///
    /// Requires a lease store with atomic CAS (NATS, Redis, real S3).
    /// Tigris S3 does NOT work: its conditional PUTs (If-None-Match, If-Match)
    /// are not atomic for concurrent requests. Two concurrent PUTs both return 200.
    async fn acquire_lease(
        &self,
        lease_store: &dyn hadb::LeaseStore,
        lease_key: &str,
    ) -> std::result::Result<(), HaQLiteError> {
        let deadline = tokio::time::Instant::now() + self.inner.write_timeout;
        let mut attempt = 0u32;

        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(HaQLiteError::LeaseContention(format!(
                    "could not acquire lease for '{}' within {:?}",
                    self.inner.db_name, self.inner.write_timeout
                )));
            }

            let lease_data = serde_json::to_vec(&serde_json::json!({
                "instance_id": self.inner.shared_instance_id,
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default().as_millis() as u64,
                "ttl_secs": self.inner.lease_ttl,
            }))
            .unwrap_or_default();

            match lease_store.read(lease_key).await {
                Ok(None) => {
                    // No lease exists. Create it.
                    let result = lease_store.write_if_not_exists(lease_key, lease_data).await;
                    match result {
                        Ok(cas) if cas.success => return Ok(()),
                        Ok(_) => {} // Someone else created it. Retry.
                        Err(e) => {
                            return Err(HaQLiteError::CoordinatorError(format!(
                                "lease write failed: {}",
                                e
                            )))
                        }
                    }
                }
                Ok(Some((data, etag))) => {
                    let is_ours = serde_json::from_slice::<serde_json::Value>(&data)
                        .map(|j| {
                            j.get("instance_id").and_then(|v| v.as_str()).unwrap_or("")
                                == self.inner.shared_instance_id
                        })
                        .unwrap_or(false);

                    if is_ours {
                        return Ok(());
                    }

                    let expired = serde_json::from_slice::<serde_json::Value>(&data)
                        .map(|j| {
                            let ts = j.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
                            let ttl = j.get("ttl_secs").and_then(|v| v.as_u64()).unwrap_or(5);
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            now > ts + (ttl * 1000)
                        })
                        .unwrap_or(true);

                    if expired {
                        // CAS replace the expired lease.
                        let result = lease_store
                            .write_if_match(lease_key, lease_data, &etag)
                            .await;
                        match result {
                            Ok(cas) if cas.success => return Ok(()),
                            Ok(_) => {} // CAS conflict. Retry.
                            Err(e) => {
                                return Err(HaQLiteError::CoordinatorError(format!(
                                    "lease CAS failed: {}",
                                    e
                                )))
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(HaQLiteError::CoordinatorError(format!(
                        "lease read failed: {}",
                        e
                    )))
                }
            }

            let backoff = Duration::from_millis(50 * 2u64.pow(attempt.min(4)));
            tokio::time::sleep(backoff.min(Duration::from_secs(2))).await;
            attempt += 1;
        }
    }

    fn execute_local_raw(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> std::result::Result<u64, HaQLiteError> {
        let conn_arc = self.inner.ensure_conn()?;
        let rows = {
            let conn = conn_arc.lock();
            conn.execute(sql, params)
                .map_err(|e| HaQLiteError::DatabaseError(format!("execute failed: {e}")))?
        };
        self.flush_turbolite()?;
        Ok(rows as u64)
    }

    fn execute_local(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> std::result::Result<u64, HaQLiteError> {
        // SqlValue implements ToSql directly (zero-copy, no String/Blob clone).
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
            .iter()
            .map(|p| p as &dyn rusqlite::types::ToSql)
            .collect();
        self.execute_local_raw(sql, &param_refs)
    }

    /// Refresh leader address from the Coordinator (which tracks lease changes).
    async fn refresh_leader_addr(&self) -> std::result::Result<String, HaQLiteError> {
        if let Some(ref coord) = self.inner.coordinator {
            if let Some(addr) = coord.leader_address(&self.inner.db_name).await {
                if !addr.is_empty() {
                    self.inner.set_leader_addr(addr);
                }
            }
        }
        self.inner.leader_addr()
    }

    async fn execute_forwarded(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> std::result::Result<u64, HaQLiteError> {
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

        for (attempt, backoff) in std::iter::once(&Duration::ZERO)
            .chain(backoffs.iter())
            .enumerate()
        {
            if attempt > 0 {
                tokio::time::sleep(*backoff).await;
            }

            // Re-read leader address each attempt: the Coordinator updates it
            // when a new leader is elected, but our cached copy may be stale.
            let leader_addr = self.refresh_leader_addr().await?;
            if leader_addr.is_empty() {
                last_err = "no leader address available".to_string();
                continue;
            }
            let url = format!("{}/haqlite/execute", leader_addr);

            let mut req = self.inner.http_client.post(&url).json(&body);
            if let Some(ref secret) = self.inner.secret {
                req = req.bearer_auth(secret);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    last_err = format!("connection error: {e}");
                    continue;
                }
            };

            let status = resp.status();
            if status.is_success() {
                let result: forwarding::ExecuteResult = resp.json().await.map_err(|e| {
                    HaQLiteError::LeaderResponseParseError(format!(
                        "failed to parse leader response: {e}"
                    ))
                })?;
                tracing::debug!(
                    db = %self.inner.db_name,
                    leader_addr = %leader_addr,
                    rows_affected = result.rows_affected,
                    "forwarded execute acknowledged by leader"
                );
                return Ok(result.rows_affected);
            }

            let body_text = resp.text().await.unwrap_or_default();

            // 421 Misdirected Request: stale leader address or mid-promotion.
            // Refresh leader address and retry.
            if status.as_u16() == 421 {
                last_err = format!("421 misdirected (stale leader addr): {body_text}");
                continue;
            }

            // Don't retry other 4xx: client errors (bad SQL, auth) won't succeed on retry.
            if status.is_client_error() {
                return Err(HaQLiteError::LeaderClientError {
                    status: status.as_u16(),
                    body: body_text,
                });
            }

            // 5xx: retry with backoff.
            last_err = format!("{status}: {body_text}");
        }

        // If we never had a leader address, this is NotLeader, not a connection error.
        if last_err == "no leader address available" {
            Err(HaQLiteError::NotLeader)
        } else {
            Err(HaQLiteError::LeaderConnectionError(format!(
                "all {} forwarding attempts failed: {last_err}",
                backoffs.len() + 1
            )))
        }
    }
}

// ============================================================================
// Shared open logic for builder and from_coordinator
// ============================================================================

pub async fn open_with_coordinator(
    coordinator: Arc<Coordinator>,
    db_path: PathBuf,
    db_name: &str,
    schema: &str,
    address: &str,
    forwarding_mode: ForwardingMode,
    forward_timeout: Duration,
    secret: Option<String>,
    read_concurrency: usize,
    authorizer: Option<AuthorizerFactory>,
    connection_opener: Option<
        Arc<dyn Fn() -> Result<rusqlite::Connection, HaQLiteError> + Send + Sync>,
    >,
    follower_read_connection_opener: Option<
        Arc<dyn Fn() -> Result<rusqlite::Connection, HaQLiteError> + Send + Sync>,
    >,
    on_flush: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
    schema_already_applied_after_join: bool,
) -> Result<HaQLite> {
    let custom_connection_opener = connection_opener.is_some();
    let bootstrap_conn = if custom_connection_opener {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        None
    } else {
        ensure_schema(&db_path, schema, None)?
    };

    // Drop the bootstrap connection BEFORE coordinator.join. For
    // custom VFS-backed openers we also skip pre-join schema writes:
    // the join path acquires the HA lease/fence, and only then may
    // turbolite safely flush schema pages to remote storage.
    drop(bootstrap_conn);

    // Subscribe to role events BEFORE join.
    let role_rx = coordinator.role_events();

    // Join the HA cluster.
    let JoinResult {
        role: initial_role,
        caught_up,
        position,
    } = coordinator.join(db_name, &db_path).await?;

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
            Role::Client => ROLE_CLIENT,
            Role::LatentWriter => unreachable!("Coordinator join only yields valid HA roles"),
        }),
        conn: arc_swap::ArcSwapOption::new(None),
        leader_address: RwLock::new(leader_addr),
        http_client,
        secret: secret.clone(),
        read_semaphore: tokio::sync::Semaphore::new(read_concurrency),
        follower_caught_up: caught_up,
        follower_replay_position: position,
        mode: HaMode::SingleWriter,
        shared_lease_store: None,
        shared_replicator: None,
        shared_walrust_storage: None,
        shared_prefix: String::new(),
        shared_instance_id: String::new(),
        write_mutex: tokio::sync::Mutex::new(()),
        cached_manifest_version: AtomicU64::new(0),
        write_timeout: DEFAULT_FORWARD_TIMEOUT,
        lease_ttl: 5,
        schema_sql: (custom_connection_opener && !schema_already_applied_after_join)
            .then(|| schema.to_string()),
        schema_applied: AtomicBool::new(
            !custom_connection_opener || schema_already_applied_after_join,
        ),
        authorizer,
        forwarding_mode,
        fwd_handle: tokio::sync::Mutex::new(None),
        connection_opener,
        follower_read_connection_opener,
        on_flush,
    });

    // bootstrap_conn was dropped above. For leader, reopen via the
    // connection_opener now that the role is known. Followers open
    // their own rw conn on promotion.
    if initial_role == Role::Leader {
        match inner.try_open_leader_conn() {
            Ok(conn) => {
                inner.apply_initial_authorizer(&conn);
                inner.apply_schema_if_needed(&conn)?;
                inner.set_conn(Some(Arc::new(Mutex::new(conn))));
            }
            Err(e) => {
                tracing::error!("HaQLite: failed to open initial leader connection: {}", e);
            }
        }
    }

    // Start forwarding server only if we're the leader.
    // On promotion, the role listener will start it.
    if initial_role == Role::Leader {
        inner.start_forwarding_server().await;
    }

    // Spawn role event listener.
    let role_inner = inner.clone();
    let role_address = address.to_string();
    let role_handle = tokio::spawn(async move {
        run_role_listener(role_rx, role_inner, role_address).await;
    });

    Ok(HaQLite {
        inner,
        _role_handle: role_handle,
        closed: false,
    })
}

// ============================================================================
// Role event listener
// ============================================================================

pub async fn run_role_listener(
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
                inner.start_forwarding_server().await;

                if inner.get_conn().ok().flatten().is_some() {
                    // Connection already exists (was fenced). Unfence it.
                    inner.unfence_connection();
                    if let Ok(Some(conn_arc)) = inner.get_conn() {
                        let conn = conn_arc.lock();
                        if let Err(e) = inner.apply_schema_if_needed(&conn) {
                            tracing::error!(
                                "HaQLite: failed to apply schema on promotion for '{}': {}",
                                db_name,
                                e
                            );
                        }
                    }
                } else {
                    // No connection (first promotion or after sleep). Open one.
                    match inner.try_open_leader_conn() {
                        Ok(conn) => {
                            inner.apply_initial_authorizer(&conn);
                            if let Err(e) = inner.apply_schema_if_needed(&conn) {
                                tracing::error!(
                                    "HaQLite: failed to apply schema on promotion for '{}': {}",
                                    db_name,
                                    e
                                );
                            }
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
            }
            Ok(RoleEvent::Demoted { db_name }) => {
                tracing::error!("HaQLite: demoted from leader for '{}'", db_name);
                inner.stop_forwarding_server().await;
                inner.set_role(Role::Follower);
                inner.fence_connection();
            }
            Ok(RoleEvent::Fenced { db_name }) => {
                tracing::error!("HaQLite: fenced for '{}' -- stopping writes", db_name);
                inner.stop_forwarding_server().await;
                inner.set_role(Role::Follower);
                inner.fence_connection();
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
// SharedWriter mode: open + manifest poller
// ============================================================================

#[allow(clippy::too_many_arguments)]
// ============================================================================
// Helpers
// ============================================================================

/// Create the DB file with schema (WAL mode, autocheckpoint=0).
///
/// When a `connection_opener` is supplied (haqlite-turbolite path), the
/// schema must land *through* the registered turbolite VFS so the
/// VFS-backed connection sees it. Earlier versions opened via plain
/// `rusqlite::Connection::open(db_path)`, which writes to a regular
/// file at `db_path` that the VFS-backed connection never reads from —
/// causing `no such table: t` for tiering modes that don't import the
/// disk file at first publish (Checkpoint and Cloud).
///
/// Returns the opened connection when a `connection_opener` was used so
/// the caller can re-use it as the persistent leader connection. Some
/// VFS impls (notably turbolite in Cloud mode) hold per-file locks that
/// don't release across an open/close gap, so opening a second time via
/// the opener would fail with `database is locked`. Returning the
/// connection lets the caller skip that second open entirely.
fn ensure_schema(
    db_path: &Path,
    schema: &str,
    connection_opener: Option<
        &Arc<dyn Fn() -> Result<rusqlite::Connection, HaQLiteError> + Send + Sync>,
    >,
) -> Result<Option<rusqlite::Connection>> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match connection_opener {
        Some(opener) => {
            let conn = opener().map_err(|e| anyhow::anyhow!("ensure_schema opener: {e}"))?;
            if !schema.trim().is_empty() {
                conn.execute_batch(schema)?;
            }
            Ok(Some(conn))
        }
        None => {
            let conn = rusqlite::Connection::open(db_path)?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; \
                 PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000;",
            )?;
            if !schema.trim().is_empty() {
                conn.execute_batch(schema)?;
            }
            drop(conn);
            Ok(None)
        }
    }
}

/// Open a read-write connection with WAL mode and autocheckpoint disabled.
fn open_leader_connection(db_path: &Path) -> Result<rusqlite::Connection> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000;")?;
    Ok(conn)
}

/// Parse a URL into base (scheme + host + port + path) and query params.
struct ParsedUrl {
    base: String,
    host: String,
    params: std::collections::HashMap<String, String>,
}

fn parse_url_params(url: &str) -> ParsedUrl {
    let (base, query) = match url.find('?') {
        Some(i) => (&url[..i], &url[i + 1..]),
        None => (url, ""),
    };

    // Extract host from base: strip scheme, take up to first /
    let host = base
        .split("://")
        .last()
        .unwrap_or(base)
        .split('/')
        .next()
        .unwrap_or(base)
        .split(':')
        .next()
        .unwrap_or(base)
        .to_string();

    let params: std::collections::HashMap<String, String> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|kv| {
            let mut parts = kv.splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(k), Some(v)) => Some((k.to_string(), v.to_string())),
                _ => None,
            }
        })
        .collect();

    ParsedUrl {
        base: base.to_string(),
        host,
        params,
    }
}

#[cfg(test)]
mod send_tests {
    use super::*;

    /// Compile-time proof that execute_async returns a Send future.
    /// This test doesn't run anything; it just verifies the type constraint.
    /// If param_refs leaks across an await point, this fails to compile.
    #[allow(dead_code)]
    fn execute_async_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("db");
        let db = HaQLite::local(path.to_str().expect("path"), "").expect("open");
        let params = vec![SqlValue::Integer(1)];
        let fut = db.execute_async("SELECT 1", &params);
        assert_send(&fut);
    }
}

#[cfg(test)]
mod url_parsing_tests {
    use super::*;

    #[test]
    fn parse_http_with_token() {
        let parsed = parse_url_params("http://proxy:8080?token=mytoken");
        assert_eq!(parsed.base, "http://proxy:8080");
        assert_eq!(parsed.host, "proxy");
        assert_eq!(parsed.params.get("token").unwrap(), "mytoken");
    }

    #[test]
    fn parse_https_with_token() {
        let parsed = parse_url_params("https://proxy.example.com?token=abc123");
        assert_eq!(parsed.base, "https://proxy.example.com");
        assert_eq!(parsed.host, "proxy.example.com");
        assert_eq!(parsed.params.get("token").unwrap(), "abc123");
    }

    #[test]
    fn parse_nats_with_bucket() {
        let parsed = parse_url_params("nats://localhost:4222?bucket=leases");
        assert_eq!(parsed.base, "nats://localhost:4222");
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.params.get("bucket").unwrap(), "leases");
    }

    #[test]
    fn parse_no_query_params() {
        let parsed = parse_url_params("http://proxy:8080");
        assert_eq!(parsed.base, "http://proxy:8080");
        assert_eq!(parsed.host, "proxy");
        assert!(parsed.params.is_empty());
    }

    #[test]
    fn parse_multiple_params() {
        let parsed = parse_url_params("http://proxy:8080?token=abc&timeout=30");
        assert_eq!(parsed.params.get("token").unwrap(), "abc");
        assert_eq!(parsed.params.get("timeout").unwrap(), "30");
    }

    #[test]
    fn parse_s3_bucket_with_endpoint() {
        // Used in resolve_lease_store_from_env for s3://bucket?endpoint=...
        let url = "s3://my-bucket?endpoint=https://fly.storage.tigris.dev";
        // The function is called with url[5..] for s3:// prefix stripping
        let parsed = parse_url_params(&url[5..]);
        assert_eq!(parsed.host, "my-bucket");
        assert_eq!(
            parsed.params.get("endpoint").unwrap(),
            "https://fly.storage.tigris.dev"
        );
    }

    #[test]
    fn parse_value_with_equals_sign() {
        // Endpoint URLs contain = in query params of their own
        let parsed = parse_url_params("http://proxy:8080?token=abc=def");
        // splitn(2, '=') means only split on first '='
        assert_eq!(parsed.params.get("token").unwrap(), "abc=def");
    }

    #[test]
    fn parse_empty_string() {
        let parsed = parse_url_params("");
        assert_eq!(parsed.base, "");
        assert_eq!(parsed.host, "");
        assert!(parsed.params.is_empty());
    }

    #[test]
    fn parse_url_with_path() {
        let parsed = parse_url_params("http://proxy:8080/v1/lease?token=abc");
        assert_eq!(parsed.base, "http://proxy:8080/v1/lease");
        assert_eq!(parsed.host, "proxy");
        assert_eq!(parsed.params.get("token").unwrap(), "abc");
    }

    #[test]
    fn parse_bare_host() {
        let parsed = parse_url_params("my-bucket");
        assert_eq!(parsed.base, "my-bucket");
        assert_eq!(parsed.host, "my-bucket");
        assert!(parsed.params.is_empty());
    }
}

#[cfg(test)]
mod forwarding_mode_tests {
    use super::*;

    #[test]
    fn forwarding_port_zero_disables_forwarding() {
        let builder = HaQLite::builder().forwarding_port(0);
        assert_eq!(builder.get_forwarding_mode(), ForwardingMode::Disabled);
        assert_eq!(builder.get_forwarding_port(), 0);
    }

    #[test]
    fn disable_forwarding_sets_disabled_mode() {
        let builder = HaQLite::builder().disable_forwarding();
        assert_eq!(builder.get_forwarding_mode(), ForwardingMode::Disabled);
        assert_eq!(builder.get_forwarding_port(), 0);
    }

    #[test]
    fn explicit_builtin_forwarding_keeps_port() {
        let builder =
            HaQLite::builder().forwarding_mode(ForwardingMode::BuiltinHttp { port: 19080 });
        assert_eq!(
            builder.get_forwarding_mode(),
            ForwardingMode::BuiltinHttp { port: 19080 }
        );
        assert_eq!(builder.get_forwarding_port(), 19080);
    }
}

/// Auto-detect this node's network address for the forwarding server.
fn detect_address(instance_id: &str, port: u16) -> String {
    // On Fly: use internal DNS.
    if let Ok(app_name) = std::env::var("FLY_APP_NAME") {
        return format!("http://{}.vm.{}.internal:{}", instance_id, app_name, port);
    }

    // Fallback: hostname.
    let hostname = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "localhost".to_string());

    format!("http://{}:{}", hostname, port)
}
