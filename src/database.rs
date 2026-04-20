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
//! db.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Alice".into())])?;
//! let count: i64 = db.query_row("SELECT COUNT(*) FROM users", &[], |r| r.get(0))?;
//! # Ok(())
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use std::time::Duration;

use anyhow::Result;
use axum::routing::post;
use hadb::{CoordinatorConfig, Coordinator, JoinResult, LeaseConfig, Replicator, Role, RoleEvent};
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
    dyn Fn(bool) -> Box<dyn FnMut(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization + Send>
        + Send
        + Sync,
>;
use crate::replicator::SqliteReplicator;

const DEFAULT_PREFIX: &str = "haqlite/";
const DEFAULT_FORWARDING_PORT: u16 = 18080;
const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

const ROLE_LEADER: u8 = 0;
const ROLE_FOLLOWER: u8 = 1;

// Re-export canonical mode/durability types from hadb.
// Shared across haqlite and hakuzu for consistent validation.
pub use hadb::{Durability, HaMode, validate_mode_durability};

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
    durability: Durability,
    manifest_store: Option<Arc<dyn hadb::ManifestStore>>,
    manifest_poll_interval: Option<Duration>,
    write_timeout: Option<Duration>,
    walrust_storage: Option<Arc<dyn hadb_storage::StorageBackend>>,
    lease_ttl: Option<u64>,
    lease_renew_interval: Option<Duration>,
    lease_follower_poll_interval: Option<Duration>,
    turbolite_vfs: Option<(turbolite::tiered::SharedTurboliteVfs, String)>,
    /// HTTP endpoint + token for turbolite page storage through a proxy.
    /// The proxy handles S3 tiering and fence enforcement.
    /// Haqlite adds the fence token (from the Coordinator lease) internally.
    turbolite_http: Option<(String, String)>,
    /// Pre-constructed turbolite page storage backend. Mutually exclusive
    /// with `turbolite_http` and `turbolite_vfs`. Phase Lucid: callers that
    /// don't want the Cinch HTTP wiring (e.g. direct S3) construct the
    /// backend themselves and pass it here.
    turbolite_storage: Option<Arc<dyn hadb_storage::StorageBackend>>,
    authorizer: Option<AuthorizerFactory>,
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
            durability: Durability::Replicated,
            manifest_store: None,
            manifest_poll_interval: None,
            write_timeout: None,
            walrust_storage: None,
            lease_ttl: None,
            lease_renew_interval: None,
            lease_follower_poll_interval: None,
            turbolite_vfs: None,
            turbolite_http: None,
            turbolite_storage: None,
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
        self.lease_store(Arc::new(
            hadb_lease_cinch::CinchLeaseStore::new(endpoint, token),
        ))
    }

    /// Set the coordination topology. Default: `HaMode::Dedicated`.
    pub fn mode(mut self, mode: HaMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set the durability mode. Default: `Durability::Replicated`.
    ///
    /// - `Replicated`: plain SQLite + walrust WAL shipping (Dedicated only)
    /// - `Synchronous`: turbolite S3Primary, every write to S3
    /// - `Eventual`: turbolite S3 + walrust WAL shipping between checkpoints
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Use a ManifestStore (required for `HaMode::Shared`).
    pub fn manifest_store(mut self, store: Arc<dyn hadb::ManifestStore>) -> Self {
        self.manifest_store = Some(store);
        self
    }

    /// Use an HTTP-based ManifestStore (for embedded replicas via a proxy).
    ///
    /// Shorthand for `.manifest_store(Arc::new(HttpManifestStore::new(endpoint, token)))`.
    pub fn manifest_endpoint(self, endpoint: &str, token: &str) -> Self {
        self.manifest_store(Arc::new(
            hadb_manifest_http::HttpManifestStore::new(endpoint, token),
        ))
    }

    /// Route turbolite page storage through an HTTP proxy.
    ///
    /// The proxy handles S3 tiering and fence enforcement. Haqlite adds the
    /// fence token (from the Coordinator lease) to the turbolite StorageBackend
    /// internally, since it's not available until the Coordinator is created.
    ///
    /// For lease, manifest, and walrust storage, use the dedicated builder
    /// methods: `.lease_endpoint()`, `.manifest_endpoint()`, `.walrust_storage()`.
    pub fn turbolite_http(mut self, endpoint: &str, token: &str) -> Self {
        self.turbolite_http = Some((endpoint.to_string(), token.to_string()));
        self
    }

    /// Use a pre-constructed page storage backend for turbolite (Synchronous
    /// durability). Caller is responsible for constructing the backend
    /// (`hadb_storage_s3::S3Storage`, `hadb_storage_cinch::CinchHttpStorage`,
    /// etc.) and any sub-prefix wiring.
    ///
    /// Mutually exclusive with `turbolite_http()` and `turbolite_vfs()`.
    /// Phase Lucid: replaces the implicit `S3Storage::from_env(bucket, endpoint)`
    /// fallback that fired when no other turbolite config was set.
    pub fn turbolite_storage(mut self, storage: Arc<dyn hadb_storage::StorageBackend>) -> Self {
        self.turbolite_storage = Some(storage);
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

    /// Lease TTL in seconds. Applies to both Dedicated mode (fed into the
    /// Coordinator's `LeaseConfig`) and Shared mode (used for lease acquisition
    /// during writes). Default: 5.
    pub fn lease_ttl(mut self, ttl_secs: u64) -> Self {
        self.lease_ttl = Some(ttl_secs);
        self
    }

    /// Lease renewal interval (Dedicated mode). How often the leader refreshes
    /// its lease in the backing store. Default: 2s.
    pub fn lease_renew_interval(mut self, interval: Duration) -> Self {
        self.lease_renew_interval = Some(interval);
        self
    }

    /// Follower poll interval (Dedicated mode). How often followers check the
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

    /// Use a turbolite VFS as the storage engine (Shared mode only).
    ///
    /// `vfs` is the TurboliteVfs (must already be registered with SQLite).
    /// `vfs_name` is the name used in the VFS URI (e.g. "mydb").
    pub fn turbolite_vfs(mut self, vfs: turbolite::tiered::SharedTurboliteVfs, vfs_name: &str) -> Self {
        self.turbolite_vfs = Some((vfs, vfs_name.to_string()));
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
        G: FnMut(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization + Send + 'static,
    {
        self.authorizer = Some(Arc::new(move |fenced| Box::new(factory(fenced))));
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

        // Validate topology + durability combination.
        // Shared mode only supports Synchronous durability. Multiple concurrent
        // writers need every write to go through S3 so each writer always sees
        // the latest state. Eventual consistency means writers operate on stale
        // data, silently losing updates.
        if self.mode == HaMode::Shared && self.durability != Durability::Synchronous {
            return Err(anyhow::anyhow!(
                "Shared topology only supports Synchronous durability. \
                 Multiple writers require every write to be durable to S3 so \
                 each writer always sees the latest state."
            ));
        }

        // Shared fence: the `AtomicFenceWriter` half goes to `DbLease`
        // (via the coordinator config), the `AtomicFence` reader half is
        // cloned into every storage adapter that performs fenced writes.
        let (atomic_fence, atomic_fence_writer) = hadb_lease::AtomicFence::new();
        let fence_writer = Arc::new(atomic_fence_writer);

        let walrust_storage_opt: Option<Arc<dyn hadb_storage::StorageBackend>> = match self.walrust_storage {
            Some(storage) => Some(storage),
            None => {
                if self.mode == HaMode::Dedicated {
                    if let Some((ref ep, ref tok)) = self.turbolite_http {
                        // Cinch HTTP mode: WAL segments flow through /v1/sync/wal
                        // on the Cinch lease/sync server (Grabby or engine-embedded).
                        // Storage adapter reads the fence on every write and refuses
                        // when no lease is held. (Convenience for the explicit
                        // turbolite_http() builder method.)
                        let fence: Arc<dyn hadb_lease::FenceSource> =
                            Arc::new(atomic_fence.clone());
                        Some(Arc::new(
                            hadb_storage_cinch::CinchHttpStorage::new(ep, tok, "wal")
                                .with_fence(fence),
                        ))
                    } else {
                        // Phase Lucid: no implicit S3-from-env fallback. Caller
                        // must pass walrust_storage() explicitly when not using
                        // turbolite_http().
                        return Err(anyhow::anyhow!(
                            "HaMode::Dedicated requires walrust storage. Either call \
                             HaQLiteBuilder::walrust_storage(...) with an explicit backend, \
                             or use HaQLiteBuilder::turbolite_http(endpoint, token) to wire \
                             both walrust + turbolite via the Cinch HTTP path."
                        ));
                    }
                } else {
                    None
                }
            }
        };

        // Phase Lucid: lease store must be explicit. Use haqlite::env::lease_store_from_env
        // to opt into env-var resolution.
        let lease_store: Arc<dyn hadb::LeaseStore> = self.lease_store.ok_or_else(|| {
            anyhow::anyhow!(
                "HaQLiteBuilder requires lease_store(). Pass an Arc<dyn hadb::LeaseStore>, \
                 or call haqlite::env::lease_store_from_env(bucket, endpoint).await?"
            )
        })?;

        // Build coordinator config. The lease store now lives inside
        // `LeaseConfig` (Phase Fjord) — store + policy travel together.
        //
        // Phase Driftwood: preserve caller-provided `config.lease` timing
        // policy, but patch in the wiring (store, instance_id, address)
        // from the builder setters. Previously this overwrote the whole
        // LeaseConfig, silently dropping CLI timing knobs. Builder setters
        // (`.lease_ttl()`, `.lease_renew_interval()`,
        // `.lease_follower_poll_interval()`) take precedence when set.
        let mut config = self.coordinator_config.unwrap_or_default();
        let mut lease_cfg = match config.lease.take() {
            Some(mut existing) => {
                existing.store = lease_store.clone();
                existing.instance_id = instance_id.clone();
                existing.address = address.clone();
                existing
            }
            None => LeaseConfig::new(
                lease_store.clone(),
                instance_id.clone(),
                address.clone(),
            ),
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
        // HTTP-backed storage path: the lease store is `CinchLeaseStore`
        // (or a compatible token-scoped backend). Phase Fjord moved the
        // "writer"-key default into `CinchLeaseStore::key_for`, so there's
        // no override to set here — only the fence wiring.
        if self.turbolite_http.is_some() {
            config.fence_writer = Some(fence_writer.clone());
        }

        match self.mode {
            HaMode::Dedicated => {
                let walrust_storage = walrust_storage_opt
                    .ok_or_else(|| anyhow::anyhow!("Dedicated mode requires walrust storage"))?;
                let replication_config = walrust::ReplicationConfig {
                    sync_interval: config.sync_interval,
                    snapshot_interval: config.snapshot_interval,
                    autonomous_snapshots: true,
                    ..Default::default()
                };
                let replicator = Arc::new(
                    SqliteReplicator::new(walrust_storage.clone(), &self.prefix, replication_config)
                        .with_skip_snapshot(self.durability == Durability::Synchronous)
                );

                // For Synchronous durability: create ONE turbolite VFS shared by
                // both the follower (for manifest polling) and the leader/inner
                // (for reads, writes, and manifest publishing). S3Primary uses
                // journal_mode=OFF, so walrust has no WAL to ship.
                let (follower_behavior, tl_state, manifest_wakeup) = if self.durability == Durability::Synchronous {
                    // Phase Lucid: manifest store must be explicit for Synchronous mode.
                    let ms: Arc<dyn hadb::ManifestStore> = self.manifest_store.clone().ok_or_else(|| {
                        anyhow::anyhow!(
                            "Durability::Synchronous + HaMode::Dedicated requires manifest_store(). \
                             Pass an Arc<dyn hadb::ManifestStore>, or call \
                             haqlite::env::manifest_store_from_env(bucket, endpoint).await?"
                        )
                    })?;
                    let vfs_name = format!("haqlite_ded_sync_{}", uuid::Uuid::new_v4());
                    // Per-database cache dir so turbolite page caches don't collide.
                    let cache_dir = db_path.parent()
                        .unwrap_or_else(|| std::path::Path::new("/tmp"))
                        .join(format!(".tl_cache_{}", db_name));
                    std::fs::create_dir_all(&cache_dir)?;
                    let tl_config = turbolite::tiered::TurboliteConfig {
                        cache_dir,
                        sync_mode: turbolite::tiered::SyncMode::Durable,
                        // Disable inline GC: old page group versions must survive
                        // until explicit gc() runs (which checks snapshot manifests).
                        cache: turbolite::tiered::CacheConfig {
                            gc_enabled: false,
                            ..Default::default()
                        },
                        ..Default::default()
                    };
                    let rt_handle = tokio::runtime::Handle::current();
                    let vfs = if let Some((ref ep, ref tok)) = self.turbolite_http {
                        // HTTP mode: page storage speaks the Cinch /v1/sync/pages
                        // contract. Reader half of the shared AtomicFence is cloned
                        // in so fenced writes refuse when no lease is held.
                        let fence: Arc<dyn hadb_lease::FenceSource> =
                            Arc::new(atomic_fence.clone());
                        let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(
                            hadb_storage_cinch::CinchHttpStorage::new(ep, tok, "pages")
                                .with_fence(fence),
                        );
                        turbolite::tiered::TurboliteVfs::with_backend(tl_config, storage, rt_handle)
                            .map_err(|e| anyhow::anyhow!("turbolite VFS (HTTP) for Dedicated+Sync: {e}"))?
                    } else if let Some(ref storage) = self.turbolite_storage {
                        // Phase Lucid: caller-supplied backend.
                        turbolite::tiered::TurboliteVfs::with_backend(tl_config, storage.clone(), rt_handle)
                            .map_err(|e| anyhow::anyhow!("turbolite VFS (caller-supplied) for Dedicated+Sync: {e}"))?
                    } else {
                        // Phase Lucid: no implicit S3-from-env fallback.
                        return Err(anyhow::anyhow!(
                            "Durability::Synchronous requires turbolite page storage. \
                             Either call HaQLiteBuilder::turbolite_http(endpoint, token), \
                             HaQLiteBuilder::turbolite_storage(backend), or \
                             HaQLiteBuilder::turbolite_vfs(vfs, name)."
                        ));
                    };
                    let shared_vfs = turbolite::tiered::SharedTurboliteVfs::new(vfs);
                    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone())
                        .map_err(|e| anyhow::anyhow!("register VFS: {}", e))?;

                    let manifest_wakeup = Arc::new(tokio::sync::Notify::new());
                    let fb: Arc<dyn hadb::FollowerBehavior> = Arc::new(
                        SqliteFollowerBehavior::new(walrust_storage.clone())
                            .with_turbolite_catchup(shared_vfs.clone())
                            .with_wakeup(manifest_wakeup.clone())
                    );
                    let ts = Some(DedicatedTurboliteState {
                        manifest_store: ms,
                        vfs: shared_vfs,
                        vfs_name,
                        prefix: self.prefix.clone(),
                    });
                    (fb, ts, Some(manifest_wakeup))
                } else {
                    let fb: Arc<dyn hadb::FollowerBehavior> = Arc::new(
                        SqliteFollowerBehavior::new(walrust_storage.clone())
                    );
                    (fb, None, None)
                };

                // Build hadb Coordinator. Lease store is in `config.lease`
                // already; Coordinator no longer takes it as a separate arg.
                let coordinator = Coordinator::new(
                    replicator,
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
                    tl_state,
                    self.authorizer,
                    manifest_wakeup,
                )
                .await
            }
            HaMode::Shared => {
                let poll_interval = self.manifest_poll_interval
                    .unwrap_or(Duration::from_secs(1));
                let write_timeout = self.write_timeout
                    .unwrap_or(Duration::from_secs(5));
                let lease_ttl = self.lease_ttl.unwrap_or(5);

                // Phase Lucid: manifest store must be explicit for Shared mode.
                let manifest_store: Arc<dyn hadb::ManifestStore> = self.manifest_store.ok_or_else(|| {
                    anyhow::anyhow!(
                        "HaMode::Shared requires manifest_store(). Pass an Arc<dyn hadb::ManifestStore>, \
                         or call haqlite::env::manifest_store_from_env(bucket, endpoint).await?"
                    )
                })?;

                // Auto-create turbolite VFS if not provided.
                // Synchronous = S3Primary, Eventual = default (Durable).
                let (vfs, vfs_name) = match self.turbolite_vfs {
                    Some(v) => v,
                    None => {
                        let sync_mode = match self.durability {
                            Durability::Synchronous => turbolite::tiered::SyncMode::RemotePrimary,
                            _ => turbolite::tiered::SyncMode::default(),
                        };
                        let vfs_name = format!("haqlite_auto_{}", uuid::Uuid::new_v4());
                        let cache_dir = db_path.parent()
                            .unwrap_or_else(|| std::path::Path::new("/tmp"))
                            .join(format!(".tl_cache_{}", db_name));
                        std::fs::create_dir_all(&cache_dir)?;
                        let config = turbolite::tiered::TurboliteConfig {
                            cache_dir,
                            sync_mode,
                            cache: turbolite::tiered::CacheConfig {
                                gc_enabled: false,
                                ..Default::default()
                            },
                            ..Default::default()
                        };
                        let rt_handle = tokio::runtime::Handle::current();
                        let vfs = if let Some((ref ep, ref tok)) = self.turbolite_http {
                            let fence: Arc<dyn hadb_lease::FenceSource> =
                                Arc::new(atomic_fence.clone());
                            let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(
                                hadb_storage_cinch::CinchHttpStorage::new(ep, tok, "pages")
                                    .with_fence(fence),
                            );
                            turbolite::tiered::TurboliteVfs::with_backend(config, storage, rt_handle)
                                .map_err(|e| anyhow::anyhow!("auto-create turbolite VFS (HTTP): {e}"))?
                        } else if let Some(ref storage) = self.turbolite_storage {
                            // Phase Lucid: caller-supplied backend.
                            turbolite::tiered::TurboliteVfs::with_backend(config, storage.clone(), rt_handle)
                                .map_err(|e| anyhow::anyhow!("auto-create turbolite VFS (caller-supplied): {e}"))?
                        } else {
                            // Phase Lucid: no implicit S3-from-env fallback.
                            return Err(anyhow::anyhow!(
                                "HaMode::Shared requires turbolite page storage. Either call \
                                 HaQLiteBuilder::turbolite_http(endpoint, token), \
                                 HaQLiteBuilder::turbolite_storage(backend), or \
                                 HaQLiteBuilder::turbolite_vfs(vfs, name)."
                            ));
                        };
                        let shared = turbolite::tiered::SharedTurboliteVfs::new(vfs);
                        turbolite::tiered::register_shared(&vfs_name, shared.clone())
                            .map_err(|e| anyhow::anyhow!("register VFS: {}", e))?;
                        (shared, vfs_name)
                    }
                };

                open_shared_turbolite(
                    lease_store, manifest_store, vfs, &vfs_name,
                    db_path, &db_name, schema, &self.prefix, &instance_id,
                    poll_interval, write_timeout, self.read_concurrency,
                    lease_ttl,
                ).await
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
pub(crate) struct HaQLiteInner {
    pub(crate) coordinator: Option<Arc<Coordinator>>,
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    /// Cached role -- updated atomically by the role event listener.
    role: AtomicU8,
    /// Read-write connection when leader, None when follower.
    /// ArcSwapOption: lock-free reads (every execute/query), rare writes (role change).
    pub(crate) conn: arc_swap::ArcSwapOption<Mutex<rusqlite::Connection>>,
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
    // Shared mode fields
    mode: HaMode,
    /// Direct lease store access (Shared mode, bypasses Coordinator).
    shared_lease_store: Option<Arc<dyn hadb::LeaseStore>>,
    /// Direct manifest store access (Shared mode).
    shared_manifest_store: Option<Arc<dyn hadb::ManifestStore>>,
    /// Direct replicator access (Shared mode).
    shared_replicator: Option<Arc<SqliteReplicator>>,
    /// Walrust storage backend for pull_incremental.
    shared_walrust_storage: Option<Arc<dyn hadb_storage::StorageBackend>>,
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
    shared_turbolite_vfs: Option<turbolite::tiered::SharedTurboliteVfs>,
    /// VFS name for reopening turbolite connections.
    shared_turbolite_vfs_name: Option<String>,
    /// Schema SQL to apply on first write (deferred from open for S3Primary compat).
    schema_sql: Option<String>,
    /// Whether schema has been applied on this node.
    schema_applied: AtomicBool,
    /// Port for the write-forwarding HTTP server.
    forwarding_port: u16,
    /// Running forwarding server task. Started when leader, stopped on demotion.
    fwd_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Custom authorizer factory. If set, used instead of built-in fence/unfence authorizer.
    authorizer: Option<AuthorizerFactory>,
}

impl HaQLiteInner {
    /// Start the write-forwarding HTTP server. Only leaders need this.
    /// Followers send writes to the leader; they don't listen.
    async fn start_forwarding_server(self: &Arc<Self>) {
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
        match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.forwarding_port)).await {
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
            _ => None,
        }
    }

    /// Path that walrust uses for file-level operations (snapshot/restore/WAL sync).
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
        Ok(self.leader_address.read().clone())
    }

    fn set_leader_addr(&self, addr: String) {
        *self.leader_address.write() = addr;
    }

    fn set_conn(&self, conn: Option<Arc<Mutex<rusqlite::Connection>>>) {
        self.conn.store(conn);
    }

    /// Block all writes on the connection via SQLite authorizer.
    /// Called on demotion/fencing. The connection stays open for reads.
    fn fence_connection(&self) {
        if let Ok(Some(conn_arc)) = self.get_conn() {
            let conn = conn_arc.lock();
                if let Some(ref factory) = self.authorizer {
                    conn.authorizer(Some(factory(true)));
                } else {
                    // Default: read-only allowlist.
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
                conn.authorizer(None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>);
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

    pub(crate) fn get_conn(&self) -> std::result::Result<Option<Arc<Mutex<rusqlite::Connection>>>, HaQLiteError> {
        Ok(self.conn.load_full())
    }

    /// Open a turbolite VFS connection if one doesn't exist yet.
    /// Used for lazy connection creation in Synchronous durability (S3Primary) where
    /// we can't open the connection during open_shared_turbolite because
    /// it would trigger unwanted S3Primary xSync uploads.
    fn ensure_turbolite_conn(&self) -> std::result::Result<Arc<Mutex<rusqlite::Connection>>, HaQLiteError> {
        if let Some(conn) = self.get_conn()? {
            return Ok(conn);
        }

        let vfs_name = self.shared_turbolite_vfs_name.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("VFS name required for turbolite conn".into()))?;
        // Use just the filename for the VFS URI. Turbolite's xOpen joins it with
        // the cache_dir, so passing a full path would create a broken nested path.
        let db_filename = self.db_path.file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_else(|| self.db_name.clone());
        let vfs_uri = format!("file:{}?vfs={}", db_filename, vfs_name);
        let new_conn = rusqlite::Connection::open_with_flags(
            &vfs_uri,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).map_err(|e| HaQLiteError::DatabaseError(
            format!("lazy turbolite conn open for '{}': {}", self.db_name, e)
        ))?;

        // S3Primary VFS defaults to journal_mode=OFF which prevents SQLite from
        // calling xSync on commit (data never reaches S3). Switch to DELETE so
        // xSync fires every commit.
        let current_mode: String = new_conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap_or_else(|_| "unknown".to_string());
        if current_mode == "off" {
            // S3Primary: use DELETE mode so xSync fires on every commit.
            // synchronous=FULL ensures data is flushed before xSync uploads to S3.
            new_conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA cache_size=-64000;")
                .map_err(|e| HaQLiteError::DatabaseError(format!("journal pragma DELETE: {}", e)))?;
        } else if current_mode != "wal" && current_mode != "delete" && current_mode != "memory" {
            // WAL mode (Replicated/Eventual): NORMAL is safe because walrust provides durability.
            new_conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000;")
                .map_err(|e| HaQLiteError::DatabaseError(format!("journal pragma WAL: {}", e)))?;
        } else if current_mode == "delete" {
            // Already in DELETE mode (S3Primary reopened): ensure FULL sync.
            new_conn.execute_batch("PRAGMA synchronous=FULL; PRAGMA cache_size=-64000;")
                .map_err(|e| HaQLiteError::DatabaseError(format!("pragma sync FULL: {}", e)))?;
        }

        let arc = Arc::new(Mutex::new(new_conn));
        self.set_conn(Some(arc.clone()));
        Ok(arc)
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
        Self::local_with_authorizer(db_path, schema, None)
    }

    /// Open a local-only SQLite database with a custom authorizer.
    pub fn local_with_authorizer(
        db_path: &str,
        schema: &str,
        authorizer: Option<AuthorizerFactory>,
    ) -> Result<HaQLite> {
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
            conn: arc_swap::ArcSwapOption::new(Some(Arc::new(Mutex::new(conn)))),
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
            shared_turbolite_vfs: None,
            shared_turbolite_vfs_name: None,
            schema_sql: None,
            schema_applied: AtomicBool::new(true),
            authorizer,
            forwarding_port: 0,
            fwd_handle: tokio::sync::Mutex::new(None),
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
            None, // no turbolite state for from_coordinator
            None, // no custom authorizer for from_coordinator
            None, // no manifest wakeup for from_coordinator
        )
        .await
    }

    /// Execute a write statement. Returns rows affected.
    ///
    /// Execute SQL. Synchronous on the leader (the common path).
    /// On a follower: blocks on HTTP forwarding to the leader.
    /// In shared mode: blocks on lease acquisition + S3 commit.
    pub fn execute(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
        match self.inner.mode {
            HaMode::Shared => {
                // Shared mode requires async (lease acquisition). Use the tokio handle.
                // block_in_place moves the task off the async worker thread so block_on
                // doesn't panic. Safe from both async and spawn_blocking contexts.
                let handle = tokio::runtime::Handle::try_current()
                    .map_err(|_| HaQLiteError::DatabaseError("execute in Shared mode requires tokio runtime".into()))?;
                tokio::task::block_in_place(|| handle.block_on(self.execute_shared(sql, params)))
            }
            HaMode::Dedicated => {
                let role = self.inner.current_role();
                match role {
                    Some(Role::Leader) | None => {
                        self.execute_local_raw(sql, &param_refs)
                    }
                    Some(Role::Follower) => {
                        let handle = tokio::runtime::Handle::try_current()
                            .map_err(|_| HaQLiteError::DatabaseError("execute forwarding requires tokio runtime".into()))?;
                        tokio::task::block_in_place(|| handle.block_on(self.execute_forwarded(sql, params)))
                    }
                }
            }
        }
    }

    /// Async execute for callers already in an async context.
    /// Equivalent to `execute()` but avoids the `block_on` for follower/shared paths.
    ///
    /// The returned future is Send, so it can be used with `tokio::spawn`.
    pub async fn execute_async(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
        match self.inner.mode {
            HaMode::Shared => self.execute_shared(sql, params).await,
            HaMode::Dedicated => {
                let role = self.inner.current_role();
                match role {
                    Some(Role::Leader) | None => {
                        // param_refs scoped to this synchronous branch only.
                        // Keeping it out of the async scope makes the future Send.
                        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                            params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                        self.execute_local_raw(sql, &param_refs)
                    }
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
                let conn_arc = if self.inner.shared_turbolite_vfs.is_some() {
                    self.inner.ensure_turbolite_conn()?
                } else {
                    self.inner.get_conn()?
                        .ok_or(HaQLiteError::DatabaseError("No connection available".into()))?
                };
                let conn = conn_arc.lock();
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
                // For turbolite (Dedicated+Synchronous): read through VFS.
                // For walrust: open fresh plain SQLite connections (walrust applies
                // LTX files externally, pooled connections hold stale snapshots).
                let conn = if let Some(ref vfs_name) = self.inner.shared_turbolite_vfs_name {
                    let db_filename = self.inner.db_path.file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_else(|| self.inner.db_name.clone());
                    let vfs_uri = format!("file:{}?vfs={}", db_filename, vfs_name);
                    rusqlite::Connection::open_with_flags(
                        &vfs_uri,
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                            | rusqlite::OpenFlags::SQLITE_OPEN_URI
                            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                    )
                    .map_err(|e| HaQLiteError::DatabaseError(format!("Failed to open turbolite follower connection: {e}")))?
                } else {
                    rusqlite::Connection::open_with_flags(
                        &self.inner.db_path,
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                    )
                    .map_err(|e| HaQLiteError::DatabaseError(format!("Failed to open read-only connection: {e}")))?
                };
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
        // SqlValue implements ToSql directly (zero-copy, no String/Blob clone).
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

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
                // Use ensure_turbolite_conn for lazy connection open (Synchronous durability),
                // fall back to get_conn for non-turbolite paths.
                let conn_arc = if self.inner.shared_turbolite_vfs.is_some() {
                    self.inner.ensure_turbolite_conn()?
                } else {
                    self.inner.get_conn()?
                        .ok_or(HaQLiteError::DatabaseError("No connection available".into()))?
                };
                let conn = conn_arc.lock();
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
                let conn = if let Some(ref vfs_name) = self.inner.shared_turbolite_vfs_name {
                    let db_filename = self.inner.db_path.file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_else(|| self.inner.db_name.clone());
                    let vfs_uri = format!("file:{}?vfs={}", db_filename, vfs_name);
                    rusqlite::Connection::open_with_flags(
                        &vfs_uri,
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                            | rusqlite::OpenFlags::SQLITE_OPEN_URI
                            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                    )
                    .map_err(|e| HaQLiteError::DatabaseError(format!("Failed to open turbolite follower connection: {e}")))?
                } else {
                    rusqlite::Connection::open_with_flags(
                        &self.inner.db_path,
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                    )
                    .map_err(|e| HaQLiteError::DatabaseError(format!("Failed to open read-only connection: {e}")))?
                };
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

    /// Get the underlying rusqlite connection.
    ///
    /// This is the primary API for using haqlite. You get the connection,
    /// use it like normal rusqlite (queries, transactions, prepared statements),
    /// and haqlite handles HA (lease, WAL shipping, turbolite tiering) transparently
    /// at the storage layer.
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = haqlite::HaQLite::builder("bucket").open("/data/my.db", "").await?;
    /// let conn = db.connection()?;
    /// let guard = conn.lock();
    /// guard.execute("INSERT INTO users (name) VALUES (?1)", ["Alice"])?;
    /// let count: i64 = guard.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connection(&self) -> std::result::Result<Arc<Mutex<rusqlite::Connection>>, HaQLiteError> {
        if self.inner.shared_turbolite_vfs.is_some() {
            self.inner.ensure_turbolite_conn()
        } else {
            self.inner.get_conn()?
                .ok_or(HaQLiteError::DatabaseError("No connection available".into()))
        }
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

    /// Flush turbolite staging logs to storage (S3 or HTTP storage API).
    /// Call after WAL checkpoint to ensure pages are durably stored.
    /// No-op if turbolite VFS is not active.
    pub fn flush_turbolite(&self) -> std::io::Result<()> {
        if let Some(ref vfs) = self.inner.shared_turbolite_vfs {
            vfs.vfs().flush_to_storage()?;
        }
        Ok(())
    }

    /// Get the turbolite VFS name (if using Synchronous durability).
    /// Callers can open their own read-only connections on this VFS for lock-free reads.
    pub fn vfs_name(&self) -> Option<&str> {
        self.inner.shared_turbolite_vfs_name.as_deref()
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

        // 3. Flush turbolite staging logs to remote storage BEFORE releasing the
        //    lease. The lease fences writes, so once it's gone, pending pages
        //    cannot be uploaded. Must happen after connection close (no more
        //    writes) and before coordinator.leave() (still holds the lease).
        if let Some(ref vfs) = self.inner.shared_turbolite_vfs {
            if let Err(e) = vfs.vfs().flush_to_storage() {
                tracing::error!("turbolite flush on close failed: {}", e);
            }
        }

        // 4. Leave the cluster (releases lease).
        if let Some(ref coordinator) = self.inner.coordinator {
            coordinator
                .leave(&self.inner.db_name)
                .await
                .map_err(|e| HaQLiteError::CoordinatorError(e.to_string()))?;
        }

        // 5. Stop forwarding server and abort background tasks.
        self.inner.stop_forwarding_server().await;
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
        let vfs = match &self.inner.shared_turbolite_vfs {
            Some(vfs) => vfs,
            None => return Ok(()),
        };

        // Catch up from haqlite's manifest store (the source of truth for Shared mode).
        // Writers publish here in execute_shared step 4.
        let manifest_store = match &self.inner.shared_manifest_store {
            Some(ms) => ms,
            None => return Ok(()), // no manifest store = local-only, already fresh
        };
        let manifest_key = format!("{}{}/_manifest", self.inner.shared_prefix, self.inner.db_name);
        let current_version = self.inner.cached_manifest_version.load(Ordering::SeqCst);

        match manifest_store.get(&manifest_key).await {
            Ok(Some(ha_manifest)) if ha_manifest.version > current_version => {
                // Apply the turbolite manifest from the store.
                let tl_manifest = crate::turbolite_replicator::ha_storage_to_turbolite(&ha_manifest.storage);
                vfs.set_manifest(tl_manifest);
                self.inner.cached_manifest_version.store(ha_manifest.version, Ordering::SeqCst);
                self.inner.set_conn(None); // reopen with new manifest
            }
            Ok(_) => {} // no manifest or same version
            Err(e) => {
                return Err(HaQLiteError::ReplicationError(
                    format!("manifest store fetch failed: {}", e)));
            }
        }
        Ok(())
    }

    // ========================================================================
    // Internal
    // ========================================================================

    async fn execute_shared(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
        let lease_store = self.inner.shared_lease_store.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_lease_store required".into()))?;
        let manifest_store = self.inner.shared_manifest_store.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_manifest_store required".into()))?;
        let vfs = self.inner.shared_turbolite_vfs.as_ref()
            .ok_or(HaQLiteError::ConfigurationError("shared_turbolite_vfs required".into()))?;

        let _write_guard = self.inner.write_mutex.lock().await;
        let lease_key = format!("{}{}/_lease", self.inner.shared_prefix, self.inner.db_name);
        let manifest_key = format!("{}{}/_manifest", self.inner.shared_prefix, self.inner.db_name);

        // 1. Acquire lease
        self.acquire_lease(lease_store.as_ref(), &lease_key).await?;

        // 2-5. Critical section (lease always released, even on error)
        let result: std::result::Result<u64, HaQLiteError> = async {
            // 2. Catch up from haqlite's manifest store (the single source of truth).
            // Writers publish here in step 4. Reading from the store (not turbolite S3)
            // ensures we see the latest manifest even if turbolite's S3 is eventually consistent.
            {
                let cached = self.inner.cached_manifest_version.load(Ordering::SeqCst);
                match manifest_store.get(&manifest_key).await {
                    Ok(Some(ha_manifest)) if ha_manifest.version > cached => {
                        let tl_manifest = crate::turbolite_replicator::ha_storage_to_turbolite(&ha_manifest.storage);
                        vfs.set_manifest(tl_manifest);
                        self.inner.cached_manifest_version.store(ha_manifest.version, Ordering::SeqCst);
                        self.inner.set_conn(None);
                    }
                    Ok(_) => {} // no manifest or same version
                    Err(e) => return Err(HaQLiteError::ReplicationError(
                        format!("manifest store catch-up failed: {}", e))),
                }
            }

            // 3. Write: ensure schema + execute SQL (all synchronous, no .await)
            {
                let conn = self.inner.ensure_turbolite_conn()?;
                if !self.inner.schema_applied.load(Ordering::SeqCst) {
                    if let Some(ref schema) = self.inner.schema_sql {
                        let c = conn.lock();
                        c.execute_batch(schema)
                            .map_err(|e| HaQLiteError::DatabaseError(format!("schema: {}", e)))?;
                    }
                    self.inner.schema_applied.store(true, Ordering::SeqCst);
                }
            }
            let rows = self.execute_local(sql, params)?;

            // 4. Publish turbolite manifest to haqlite's manifest store.
            // We hold the lease, so we're the only writer. Use the current
            // version from the store for CAS (not our cached version, which
            // may be stale if another writer published between our reads).
            {
                let tl_manifest = vfs.manifest();
                let ha_manifest = hadb::HaManifest {
                    version: 0, // assigned by store
                    writer_id: self.inner.shared_instance_id.clone(),
                    lease_epoch: 0,
                    timestamp_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                    storage: crate::turbolite_replicator::turbolite_to_ha_storage(&tl_manifest),
                };
                // Fetch current version from store for correct CAS.
                let current_version = match manifest_store.meta(&manifest_key).await {
                    Ok(Some(meta)) => Some(meta.version),
                    Ok(None) => None,
                    Err(e) => return Err(HaQLiteError::ReplicationError(
                        format!("manifest meta fetch failed: {}", e))),
                };
                match manifest_store.put(
                    &manifest_key,
                    &ha_manifest,
                    current_version,
                ).await {
                    Ok(cas) if cas.success => {
                        let new_version = current_version.map(|v| v + 1).unwrap_or(1);
                        self.inner.cached_manifest_version.store(new_version, Ordering::SeqCst);
                    }
                    Ok(_) => {
                        return Err(HaQLiteError::ReplicationError(
                            "manifest CAS conflict (concurrent writer despite holding lease)".into()));
                    }
                    Err(e) => {
                        return Err(HaQLiteError::ReplicationError(
                            format!("manifest publish failed: {}", e)));
                    }
                }
            }

            Ok(rows)
        }.await;

        // Always release lease
        let _ = lease_store.delete(&lease_key).await;

        result
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
                return Err(HaQLiteError::LeaseContention(
                    format!("could not acquire lease for '{}' within {:?}",
                        self.inner.db_name, self.inner.write_timeout)));
            }

            let lease_data = serde_json::to_vec(&serde_json::json!({
                "instance_id": self.inner.shared_instance_id,
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default().as_millis() as u64,
                "ttl_secs": self.inner.lease_ttl,
            })).unwrap_or_default();

            match lease_store.read(lease_key).await {
                Ok(None) => {
                    // No lease exists. Create it.
                    let result = lease_store.write_if_not_exists(lease_key, lease_data).await;
                    match result {
                        Ok(cas) if cas.success => return Ok(()),
                        Ok(_) => {} // Someone else created it. Retry.
                        Err(e) => return Err(HaQLiteError::CoordinatorError(
                            format!("lease write failed: {}", e))),
                    }
                }
                Ok(Some((data, etag))) => {
                    let is_ours = serde_json::from_slice::<serde_json::Value>(&data)
                        .map(|j| {
                            j.get("instance_id").and_then(|v| v.as_str()).unwrap_or("") ==
                                self.inner.shared_instance_id
                        }).unwrap_or(false);

                    if is_ours {
                        return Ok(());
                    }

                    let expired = serde_json::from_slice::<serde_json::Value>(&data)
                        .map(|j| {
                            let ts = j.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
                            let ttl = j.get("ttl_secs").and_then(|v| v.as_u64()).unwrap_or(5);
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default().as_millis() as u64;
                            now > ts + (ttl * 1000)
                        }).unwrap_or(true);

                    if expired {
                        // CAS replace the expired lease.
                        let result = lease_store.write_if_match(lease_key, lease_data, &etag).await;
                        match result {
                            Ok(cas) if cas.success => return Ok(()),
                            Ok(_) => {} // CAS conflict. Retry.
                            Err(e) => return Err(HaQLiteError::CoordinatorError(
                                format!("lease CAS failed: {}", e))),
                        }
                    }
                }
                Err(e) => return Err(HaQLiteError::CoordinatorError(
                    format!("lease read failed: {}", e))),
            }

            let backoff = Duration::from_millis(50 * 2u64.pow(attempt.min(4)));
            tokio::time::sleep(backoff.min(Duration::from_secs(2))).await;
            attempt += 1;
        }
    }

    fn execute_local_raw(&self, sql: &str, params: &[&dyn rusqlite::types::ToSql]) -> std::result::Result<u64, HaQLiteError> {
        // Fast path: load Guard (no Arc clone), lock Mutex directly.
        let guard = if self.inner.shared_turbolite_vfs.is_some() {
            // Turbolite path needs ensure_turbolite_conn which may create the connection.
            drop(self.inner.conn.load()); // drop guard before potential store
            let conn_arc = self.inner.ensure_turbolite_conn()?;
            let conn = conn_arc.lock();
            let rows = conn.execute(sql, params)
                .map_err(|e| HaQLiteError::DatabaseError(format!("execute failed: {e}")))?;
            return Ok(rows as u64);
        } else {
            self.inner.conn.load()
        };
        let conn_arc = guard.as_ref()
            .ok_or(HaQLiteError::DatabaseError("No write connection available (not leader?)".into()))?;
        let conn = conn_arc.lock();
        let rows = conn.execute(sql, params)
            .map_err(|e| HaQLiteError::DatabaseError(format!("execute failed: {e}")))?;
        Ok(rows as u64)
    }

    fn execute_local(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
        // SqlValue implements ToSql directly (zero-copy, no String/Blob clone).
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
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

    async fn execute_forwarded(&self, sql: &str, params: &[SqlValue]) -> std::result::Result<u64, HaQLiteError> {
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
                let result: forwarding::ExecuteResult = resp
                    .json()
                    .await
                    .map_err(|e| HaQLiteError::LeaderResponseParseError(
                        format!("failed to parse leader response: {e}")
                    ))?;
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
                "all {} forwarding attempts failed: {last_err}", backoffs.len() + 1
            )))
        }
    }
}

// ============================================================================
// Shared open logic for builder and from_coordinator
// ============================================================================

/// Optional turbolite state for Dedicated+Synchronous mode.
/// When present, the leader publishes manifests and the promoted leader
/// opens turbolite VFS connections instead of plain SQLite.
struct DedicatedTurboliteState {
    manifest_store: Arc<dyn hadb::ManifestStore>,
    vfs: turbolite::tiered::SharedTurboliteVfs,
    vfs_name: String,
    prefix: String,
}

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
    turbolite_state: Option<DedicatedTurboliteState>,
    authorizer: Option<AuthorizerFactory>,
    manifest_wakeup: Option<Arc<tokio::sync::Notify>>,
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

    let (tl_manifest_store, tl_vfs, tl_vfs_name, tl_prefix) = match turbolite_state {
        Some(ts) => (Some(ts.manifest_store), Some(ts.vfs), Some(ts.vfs_name), ts.prefix),
        None => (None, None, None, String::new()),
    };

    let inner = Arc::new(HaQLiteInner {
        coordinator: Some(coordinator),
        db_name: db_name.to_string(),
        db_path: db_path.clone(),
        role: AtomicU8::new(match initial_role {
            Role::Leader => ROLE_LEADER,
            Role::Follower => ROLE_FOLLOWER,
        }),
        conn: arc_swap::ArcSwapOption::new(None),
        leader_address: RwLock::new(leader_addr),
        http_client,
        secret: secret.clone(),
        read_semaphore: tokio::sync::Semaphore::new(read_concurrency),
        follower_caught_up: caught_up,
        follower_replay_position: position,
        mode: HaMode::Dedicated,
        shared_lease_store: None,
        shared_manifest_store: tl_manifest_store,
        shared_replicator: None,
        shared_walrust_storage: None,
        shared_prefix: tl_prefix,
        shared_instance_id: String::new(),
        write_mutex: tokio::sync::Mutex::new(()),
        cached_manifest_version: AtomicU64::new(0),
        write_timeout: DEFAULT_FORWARD_TIMEOUT,
        lease_ttl: 5,
        shared_turbolite_vfs: tl_vfs,
        shared_turbolite_vfs_name: tl_vfs_name,
        schema_sql: None,
        schema_applied: AtomicBool::new(true),
        authorizer,
        forwarding_port,
        fwd_handle: tokio::sync::Mutex::new(None),
    });

    // If leader, open rw connection.
    if initial_role == Role::Leader {
        if inner.shared_turbolite_vfs.is_some() {
            // Dedicated+Synchronous: open through turbolite VFS.
            // Schema is applied via ensure_turbolite_conn + execute_batch.
            inner.ensure_turbolite_conn()?;
            // Apply schema through turbolite connection.
            let conn_arc = inner.get_conn()?
                .expect("ensure_turbolite_conn should have set conn");
            let conn = conn_arc.lock();
            conn.execute_batch(schema)
                .map_err(|e| anyhow::anyhow!("schema via turbolite: {}", e))?;
            inner.apply_initial_authorizer(&conn);
        } else {
            let conn = open_leader_connection(&db_path)?;
            inner.apply_initial_authorizer(&conn);
            inner.set_conn(Some(Arc::new(Mutex::new(conn))));
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
        run_role_listener(role_rx, role_inner, role_address, manifest_wakeup).await;
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

async fn run_role_listener(
    mut role_rx: tokio::sync::broadcast::Receiver<RoleEvent>,
    inner: Arc<HaQLiteInner>,
    self_address: String,
    manifest_wakeup: Option<Arc<tokio::sync::Notify>>,
) {
    loop {
        match role_rx.recv().await {
            Ok(RoleEvent::Promoted { db_name }) => {
                tracing::info!("HaQLite: promoted to leader for '{}'", db_name);
                inner.set_leader_addr(self_address.clone());
                inner.set_role(Role::Leader);
                inner.start_forwarding_server().await;

                if inner.shared_turbolite_vfs.is_some() {
                    // Dedicated+Synchronous: open through turbolite VFS.
                    // catchup_on_promotion already applied the latest manifest.
                    inner.set_conn(None);
                    match inner.ensure_turbolite_conn() {
                        Ok(_) => {
                            if let Ok(Some(conn_arc)) = inner.get_conn() {
                                let c = conn_arc.lock();
                                inner.apply_initial_authorizer(&c);
                            }
                            tracing::info!("HaQLite: opened turbolite connection on promotion");
                        }
                        Err(e) => {
                            tracing::error!(
                                "HaQLite: failed to open turbolite conn on promotion: {}",
                                e
                            );
                        }
                    }
                } else if inner.get_conn().ok().flatten().is_some() {
                    // Connection already exists (was fenced). Unfence it.
                    inner.unfence_connection();
                } else {
                    // No connection (first promotion or after sleep). Open one.
                    match open_leader_connection(&inner.db_path) {
                        Ok(conn) => {
                            inner.apply_initial_authorizer(&conn);
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
                if let Some(ref notify) = manifest_wakeup {
                    tracing::info!(
                        "HaQLite: manifest changed for '{}' v{}, waking follower loop",
                        db_name, version
                    );
                    notify.notify_one();
                } else {
                    tracing::debug!("manifest changed for '{}' to v{}", db_name, version);
                }
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
async fn run_manifest_poller(
    inner: Arc<HaQLiteInner>,
    _db_name: String,
    manifest_key: String,
    poll_interval: Duration,
) {
    // The manifest poller's purpose is read-freshness for query_values_fresh.
    // It does NOT do walrust restore or update cached_manifest_version.
    // Those operations must happen under the write mutex in execute_shared /
    // ensure_fresh to avoid races (stale pager cache, checksum chain breaks).
    //
    // The poller only monitors for version changes. ensure_fresh checks
    // cached_version vs manifest version and does the actual catch-up.
    let mut interval = tokio::time::interval(poll_interval);
    loop {
        interval.tick().await;
        // Just keep the interval ticking. The actual work happens in
        // ensure_fresh (for reads) and execute_shared (for writes).
        // This task exists so HaQLite can be extended with push-based
        // notifications in the future.
        if inner.shared_manifest_store.is_none() {
            return; // No manifest store, nothing to poll
        }
        // Lightweight check: just see if there's a newer version.
        // Don't take any action -- let the read/write paths handle it.
        if let Some(ref ms) = inner.shared_manifest_store {
            let _ = ms.meta(&manifest_key).await;
        }
    }
}

// ============================================================================
// Turbolite shared mode: open + manifest poller
// ============================================================================

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
    lease_ttl: u64,
) -> Result<HaQLite> {
    let vfs_uri = format!("file:{}?vfs={}", db_path.display(), vfs_name);

    // Initialize the VFS manifest from S3/local. READ_ONLY so no pages are
    // written to the cache. Schema is deferred to first execute_shared().
    {
        let _ = rusqlite::Connection::open_with_flags(
            &vfs_uri,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).ok(); // may fail on fresh DB, that's fine
    }

    // Defer connection open to first use. Opening a read-write connection via
    // turbolite VFS in S3Primary mode triggers xSync which uploads dirty
    // subframes to S3. This causes duplicate version commits that overwrite
    // data from other nodes. The connection will be lazily opened by
    // ensure_turbolite_conn() on first execute/query.
    let conn: Option<Arc<Mutex<rusqlite::Connection>>> = None;

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
        conn: arc_swap::ArcSwapOption::new(conn),
        leader_address: RwLock::new(String::new()),
        http_client: reqwest::Client::new(),
        secret: None,
        read_semaphore: tokio::sync::Semaphore::new(read_concurrency),
        follower_caught_up: Arc::new(AtomicBool::new(true)),
        follower_replay_position: Arc::new(AtomicU64::new(0)),
        // Shared mode fields -- walrust optional (Synchronous durability: no walrust)
        mode: HaMode::Shared,
        shared_lease_store: Some(lease_store),
        shared_manifest_store: Some(manifest_store.clone()),
        shared_replicator: None,
        shared_walrust_storage: None,
        shared_prefix: prefix.to_string(),
        shared_instance_id: instance_id.to_string(),
        write_mutex: tokio::sync::Mutex::new(()),
        cached_manifest_version: AtomicU64::new(cached_version),
        write_timeout,
        lease_ttl,
        shared_turbolite_vfs: Some(vfs.clone()),
        shared_turbolite_vfs_name: Some(vfs_name.to_string()),
        schema_sql: Some(schema.to_string()),
        schema_applied: AtomicBool::new(false),
        authorizer: None, // Shared mode doesn't support custom authorizer (no fencing)
        forwarding_port: 0, // Shared mode: no forwarding server
        fwd_handle: tokio::sync::Mutex::new(None),
    });

    // Use standard manifest poller (walrust-based, not turbolite-based)
    let poller_inner = inner.clone();
    let poller_db_name = db_name_owned.clone();
    let poller_manifest_key = manifest_key;
    let role_handle = tokio::spawn(async move {
        run_manifest_poller(poller_inner, poller_db_name, poller_manifest_key, manifest_poll_interval).await;
    });

    tracing::info!(
        "HaQLite opened in Shared+Synchronous mode: db='{}', manifest_poll={}ms",
        db_name, manifest_poll_interval.as_millis(),
    );

    Ok(HaQLite {
        inner,
        _role_handle: role_handle,
        closed: false,
    })
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
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000;")?;
    conn.execute_batch(schema)?;
    drop(conn);
    Ok(())
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
