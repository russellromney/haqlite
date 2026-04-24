use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use haqlite::{HaQLite, HaQLiteBuilder, HaQLiteError, SqlValue, AuthorizerFactory};
use hadb::{Role, LeaseStore};
use turbodb::ManifestStore;
use turbolite::tiered::SharedTurboliteVfs;

/// Coordination topology for tiered HA SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Single writer with lease-per-database (production default).
    /// Maps to haqlite's Dedicated mode.
    Writer,
    /// Multiple writers with per-write lease acquisition (experimental).
    /// Maps to haqlite's Shared mode. Requires Cloud durability.
    MultiWriter,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Writer => write!(f, "Writer"),
            Mode::MultiWriter => write!(f, "MultiWriter"),
        }
    }
}

/// Builder for tiered HA SQLite with turbolite VFS.
///
/// Wraps haqlite's walrust HA layer and injects turbolite for page-level
/// S3 tiering. Two modes: Writer (Dedicated, default) and MultiWriter
/// (Shared, experimental).
pub struct Builder {
    inner: HaQLiteBuilder,
    mode: Mode,
    turbolite_durability: turbodb::Durability,
    turbolite_http: Option<(String, String)>,
    turbolite_storage: Option<Arc<dyn hadb_storage::StorageBackend>>,
    turbolite_vfs: Option<(SharedTurboliteVfs, String)>,
    manifest_store: Option<Arc<dyn ManifestStore>>,
    rollback_token: Option<String>,
    rollback_url: Option<String>,
}

impl Builder {
    pub fn new(bucket: &str) -> Self {
        Self {
            inner: HaQLiteBuilder::new(bucket),
            mode: Mode::Writer,
            turbolite_durability: turbodb::Durability::default(),
            turbolite_http: None,
            turbolite_storage: None,
            turbolite_vfs: None,
            manifest_store: None,
            rollback_token: None,
            rollback_url: None,
        }
    }

    pub fn prefix(mut self, prefix: &str) -> Self {
        self.inner = self.inner.prefix(prefix);
        self
    }

    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.inner = self.inner.endpoint(endpoint);
        self
    }

    pub fn instance_id(mut self, id: &str) -> Self {
        self.inner = self.inner.instance_id(id);
        self
    }

    pub fn address(mut self, addr: &str) -> Self {
        self.inner = self.inner.address(addr);
        self
    }

    pub fn forwarding_port(mut self, port: u16) -> Self {
        self.inner = self.inner.forwarding_port(port);
        self
    }

    pub fn forward_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.forward_timeout(timeout);
        self
    }

    pub fn secret(mut self, secret: &str) -> Self {
        self.inner = self.inner.secret(secret);
        self
    }

    pub fn read_concurrency(mut self, n: usize) -> Self {
        self.inner = self.inner.read_concurrency(n);
        self
    }

    pub fn manifest_poll_interval(mut self, interval: Duration) -> Self {
        self.inner = self.inner.manifest_poll_interval(interval);
        self
    }

    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.write_timeout(timeout);
        self
    }

    pub fn lease_store(mut self, store: Arc<dyn LeaseStore>) -> Self {
        self.inner = self.inner.lease_store(store);
        self
    }

    pub fn lease_endpoint(self, endpoint: &str, token: &str) -> Self {
        let store = Arc::new(
            hadb_lease_cinch::CinchLeaseStore::new(endpoint, token),
        );
        self.lease_store(store)
    }

    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub fn durability(mut self, durability: turbodb::Durability) -> Self {
        self.turbolite_durability = durability;
        self
    }

    pub fn turbolite_http(mut self, endpoint: &str, token: &str) -> Self {
        self.turbolite_http = Some((endpoint.to_string(), token.to_string()));
        self
    }

    pub fn turbolite_storage(mut self, storage: Arc<dyn hadb_storage::StorageBackend>) -> Self {
        self.turbolite_storage = Some(storage);
        self
    }

    pub fn turbolite_vfs(mut self, vfs: SharedTurboliteVfs, vfs_name: &str) -> Self {
        self.turbolite_vfs = Some((vfs, vfs_name.to_string()));
        self
    }

    pub fn manifest_store(mut self, store: Arc<dyn ManifestStore>) -> Self {
        self.manifest_store = Some(store);
        self
    }

    pub fn manifest_endpoint(self, endpoint: &str, token: &str) -> Self {
        let store = Arc::new(
            turbodb_manifest_cinch::CinchManifestStore::new(endpoint, token),
        );
        self.manifest_store(store)
    }

    pub fn walrust_storage(mut self, storage: Arc<dyn hadb_storage::StorageBackend>) -> Self {
        self.inner = self.inner.walrust_storage(storage);
        self
    }

    pub fn authorizer<F, G>(mut self, factory: F) -> Self
    where
        F: Fn(bool) -> G + Send + Sync + 'static,
        G: FnMut(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization + Send + 'static,
    {
        self.inner = self.inner.authorizer(factory);
        self
    }

    pub fn with_rollback_detection(mut self, token: &str) -> Self {
        self.rollback_token = Some(token.to_string());
        self
    }

    pub fn with_rollback_url(mut self, url: &str) -> Self {
        self.rollback_url = Some(url.to_string());
        self
    }

    pub async fn open(self, db_path: &str, schema: &str) -> Result<HaQLite> {
        if self.mode == Mode::MultiWriter && !self.turbolite_durability.is_cloud() {
            return Err(anyhow::anyhow!(
                "MultiWriter requires Cloud durability"
            ));
        }

        match self.mode {
            Mode::Writer => self.open_writer(db_path, schema).await,
            Mode::MultiWriter => {
                anyhow::bail!("MultiWriter mode not yet implemented in haqlite-turbolite")
            }
        }
    }

    async fn open_writer(self, db_path: &str, schema: &str) -> Result<HaQLite> {
        let db_path = std::path::PathBuf::from(db_path);
        let db_name = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("db")
            .to_string();

        let instance_id = self.inner.get_instance_id().map(|s| s.to_string()).unwrap_or_else(|| {
            std::env::var("FLY_MACHINE_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
        });
        let address = self.inner.get_address().map(|s| s.to_string()).unwrap_or_else(|| {
            detect_address(&instance_id, self.inner.get_forwarding_port())
        });

        // Build walrust storage.
        let walrust_storage: Arc<dyn hadb_storage::StorageBackend> =
            if let Some(storage) = self.inner.get_walrust_storage().cloned() {
                storage
            } else if let Some((ref ep, ref tok)) = self.turbolite_http {
                let (fence, _) = haqlite::AtomicFence::new();
                Arc::new(
                    hadb_storage_cinch::CinchHttpStorage::new(ep, tok, "wal")
                        .with_fence(Arc::new(fence)),
                )
            } else {
                anyhow::bail!("Writer mode requires walrust storage. Pass .walrust_storage() or .turbolite_http()")
            };

        // Resolve sync_interval and skip_snapshot from turbodb durability.
        let (sync_interval, skip_snapshot) = match self.turbolite_durability {
            turbodb::Durability::Checkpoint { .. } => (Duration::from_secs(3600), false),
            turbodb::Durability::Continuous { replication_interval, .. } => (replication_interval, false),
            turbodb::Durability::Cloud => (Duration::from_millis(1), true),
        };

        let snapshot_interval = self.inner.get_coordinator_config()
            .map(|c| c.snapshot_interval)
            .unwrap_or(Duration::from_secs(3600));
        let replication_config = walrust::ReplicationConfig {
            sync_interval,
            snapshot_interval,
            autonomous_snapshots: true,
            ..Default::default()
        };
        let replicator = Arc::new(
            haqlite::SqliteReplicator::new(walrust_storage.clone(), self.inner.get_prefix(), replication_config)
                .with_skip_snapshot(skip_snapshot)
        );

        // Build turbolite VFS.
        let manifest_store: Arc<dyn turbodb::ManifestStore> = self.manifest_store
            .ok_or_else(|| anyhow::anyhow!("Turbolite-backed Writer mode requires manifest_store()"))?;

        let (shared_vfs, vfs_name) = if let Some((ref vfs, ref name)) = self.turbolite_vfs {
            (vfs.clone(), name.clone())
        } else {
            let vfs_name = format!("haqlite_ded_sync_{}", uuid::Uuid::new_v4());
            let cache_dir = db_path.parent()
                .unwrap_or_else(|| std::path::Path::new("/tmp"))
                .join(format!(".tl_cache_{}", db_name));
            std::fs::create_dir_all(&cache_dir)?;
            let tl_config = turbolite::tiered::TurboliteConfig {
                cache_dir,
                cache: turbolite::tiered::CacheConfig {
                    gc_enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            };
            let rt_handle = tokio::runtime::Handle::current();
            let vfs = if let Some((ref ep, ref tok)) = self.turbolite_http {
                let (fence, _) = haqlite::AtomicFence::new();
                let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(
                    hadb_storage_cinch::CinchHttpStorage::new(ep, tok, "pages")
                        .with_fence(Arc::new(fence)),
                );
                turbolite::tiered::TurboliteVfs::with_backend(tl_config, storage, rt_handle)
                    .map_err(|e| anyhow::anyhow!("turbolite VFS (HTTP): {e}"))?
            } else if let Some(ref storage) = self.turbolite_storage {
                turbolite::tiered::TurboliteVfs::with_backend(tl_config, storage.clone(), rt_handle)
                    .map_err(|e| anyhow::anyhow!("turbolite VFS (caller-supplied): {e}"))?
            } else {
                anyhow::bail!("Turbolite requires .turbolite_http(), .turbolite_storage(), or .turbolite_vfs()")
            };
            let shared_vfs = turbolite::tiered::SharedTurboliteVfs::new(vfs);
            turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone())
                .map_err(|e| anyhow::anyhow!("register VFS: {e}"))?;
            (shared_vfs, vfs_name)
        };

        let manifest_wakeup = Arc::new(tokio::sync::Notify::new());
        let follower_behavior: Arc<dyn hadb::FollowerBehavior> = Arc::new(
            crate::TurboliteFollowerBehavior::new(shared_vfs.clone())
                .with_wakeup(manifest_wakeup)
        );

        // Build lease store and coordinator config.
        let lease_store: Arc<dyn hadb::LeaseStore> = self.inner.get_lease_store()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("lease_store() required"))?;

        let mut config = self.inner.get_coordinator_config().cloned().unwrap_or_default();
        let mut lease_cfg = match config.lease.take() {
            Some(mut existing) => {
                existing.store = lease_store.clone();
                existing.instance_id = instance_id.clone();
                existing.address = address.clone();
                existing
            }
            None => haqlite::LeaseConfig::new(lease_store.clone(), instance_id.clone(), address.clone()),
        };
        if let Some(ttl) = self.inner.get_lease_ttl() {
            lease_cfg.ttl_secs = ttl;
        }
        if let Some(d) = self.inner.get_lease_renew_interval() {
            lease_cfg.renew_interval = d;
        }
        if let Some(d) = self.inner.get_lease_follower_poll_interval() {
            lease_cfg.follower_poll_interval = d;
        }
        config.lease = Some(lease_cfg);
        if self.turbolite_http.is_some() {
            let fence_writer = Arc::new(haqlite::AtomicFence::new().1);
            config.fence_writer = Some(fence_writer);
        }

        let coordinator = haqlite::Coordinator::new(
            replicator,
            Some(manifest_store.clone()),
            None,
            follower_behavior,
            self.inner.get_prefix(),
            config,
        );

        // Build connection opener for turbolite VFS.
        let vfs_name_for_opener = vfs_name.clone();
        let db_name_for_opener = db_name.clone();
        let db_filename = db_path.file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_else(|| db_name.clone());
        let is_cloud = self.turbolite_durability.is_cloud();
        let connection_opener: Arc<dyn Fn() -> Result<rusqlite::Connection, haqlite::HaQLiteError> + Send + Sync> =
            Arc::new(move || {
                let vfs_uri = format!("file:{}?vfs={}", db_filename, vfs_name_for_opener);
                let conn = rusqlite::Connection::open_with_flags(
                    &vfs_uri,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                        | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                        | rusqlite::OpenFlags::SQLITE_OPEN_URI,
                ).map_err(|e| haqlite::HaQLiteError::DatabaseError(
                    format!("turbolite conn open for '{}': {}", db_name_for_opener, e)
                ))?;
                // Apply pragmas based on durability.
                if is_cloud {
                    conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA cache_size=-64000;")
                        .map_err(|e| haqlite::HaQLiteError::DatabaseError(format!("journal pragma: {e}")))?;
                } else {
                    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000;")
                        .map_err(|e| haqlite::HaQLiteError::DatabaseError(format!("journal pragma: {e}")))?;
                }
                Ok(conn)
            });

        let shared_vfs_for_flush = shared_vfs.clone();
        let on_flush: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>> = Some(Arc::new(move || {
            shared_vfs_for_flush.flush_to_storage()
                .map_err(|e| anyhow::anyhow!("turbolite flush: {e}"))
        }));

        haqlite::database::open_with_coordinator(
            coordinator,
            db_path,
            &db_name,
            schema,
            &address,
            self.inner.get_forwarding_port(),
            self.inner.get_forward_timeout(),
            self.inner.get_secret().map(|s| s.to_string()),
            self.inner.get_read_concurrency(),
            self.inner.get_authorizer().cloned(),
            Some(connection_opener),
            on_flush,
        )
        .await
        .map_err(|e| e.into())
    }
}

fn detect_address(instance_id: &str, forwarding_port: u16) -> String {
    if let Ok(addr) = std::env::var("FLY_PRIVATE_IP") {
        format!("http://{}:{}", addr, forwarding_port)
    } else {
        format!("http://{}:{}", instance_id, forwarding_port)
    }
}
