use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::Result;

use hadb::{HaMode, LeaseStore, NoOpReplicator, Replicator, Role};
use haqlite::{ForwardingMode, HaQLite, HaQLiteBuilder};
use turbodb::ManifestStore;
use turbolite::tiered::SharedTurboliteVfs;

#[derive(Debug, Clone)]
enum CinchHttpConfig {
    Public {
        endpoint: String,
        token: String,
    },
    Internal {
        endpoint: String,
        database_id: String,
    },
}

impl CinchHttpConfig {
    fn endpoint(&self) -> &str {
        match self {
            Self::Public { endpoint, .. } | Self::Internal { endpoint, .. } => endpoint,
        }
    }

    fn manifest_store(&self) -> turbodb_manifest_cinch::CinchManifestStore {
        match self {
            Self::Public { endpoint, token } => {
                turbodb_manifest_cinch::CinchManifestStore::new(endpoint, token)
            }
            Self::Internal {
                endpoint,
                database_id,
            } => turbodb_manifest_cinch::CinchManifestStore::new_internal(endpoint, database_id),
        }
    }

    fn storage(&self, prefix: &str) -> hadb_storage_cinch::CinchHttpStorage {
        match self {
            Self::Public { endpoint, token } => {
                hadb_storage_cinch::CinchHttpStorage::new(endpoint, token, prefix)
            }
            Self::Internal {
                endpoint,
                database_id,
            } => hadb_storage_cinch::CinchHttpStorage::new_internal(endpoint, database_id, prefix),
        }
    }

    fn identity_key(&self) -> String {
        match self {
            Self::Public { endpoint, token } => format!("public:{endpoint}:{token}"),
            Self::Internal {
                endpoint,
                database_id,
            } => format!("internal:{endpoint}:{database_id}"),
        }
    }
}

type SharedFence = (Arc<haqlite::AtomicFence>, Arc<haqlite::AtomicFenceWriter>);

#[derive(Clone)]
struct AutoVfsRegistration {
    shared_vfs: SharedTurboliteVfs,
    vfs_name: String,
    shared_fence: Option<SharedFence>,
}

static AUTO_VFS_REGISTRY: OnceLock<Mutex<HashMap<String, AutoVfsRegistration>>> = OnceLock::new();

fn auto_vfs_registry() -> &'static Mutex<HashMap<String, AutoVfsRegistration>> {
    AUTO_VFS_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn stable_vfs_name(key: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    format!("haqlite_ded_sync_{:016x}", hasher.finish())
}

fn new_shared_fence() -> SharedFence {
    let (fence, writer) = haqlite::AtomicFence::new();
    (Arc::new(fence), Arc::new(writer))
}

fn auto_vfs_key(
    cache_dir: &std::path::Path,
    db_filename: &str,
    prefix: &str,
    instance_id: &str,
    storage_identity: &str,
    durability: &turbodb::Durability,
) -> String {
    format!(
        "cache={}|db={}|prefix={}|instance={}|storage={}|durability={:?}",
        cache_dir.display(),
        db_filename,
        prefix,
        instance_id,
        storage_identity,
        durability
    )
}

fn get_or_register_auto_vfs<F>(key: String, create: F) -> Result<AutoVfsRegistration>
where
    F: FnOnce() -> Result<(SharedTurboliteVfs, Option<SharedFence>)>,
{
    let mut registry = auto_vfs_registry()
        .lock()
        .expect("auto VFS registry mutex poisoned");
    if let Some(existing) = registry.get(&key) {
        return Ok(existing.clone());
    }

    let vfs_name = stable_vfs_name(&key);
    let (shared_vfs, shared_fence) = create()?;
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone())
        .map_err(|e| anyhow::anyhow!("register VFS: {e}"))?;

    let registration = AutoVfsRegistration {
        shared_vfs,
        vfs_name,
        shared_fence,
    };
    registry.insert(key, registration.clone());
    Ok(registration)
}

#[cfg(test)]
fn auto_vfs_registry_len() -> usize {
    auto_vfs_registry()
        .lock()
        .expect("auto VFS registry mutex poisoned")
        .len()
}

/// Builder for tiered HA SQLite with turbolite VFS.
///
/// Wraps haqlite's walrust HA layer and injects turbolite for page-level
/// S3 tiering. Single-writer with lease-protected coordination today;
/// [`hadb::HaMode`] is the canonical topology axis.
pub struct Builder {
    inner: HaQLiteBuilder,
    mode: HaMode,
    role: Option<Role>,
    turbolite_durability: turbodb::Durability,
    turbolite_http: Option<CinchHttpConfig>,
    manifest_http: Option<CinchHttpConfig>,
    turbolite_storage: Option<Arc<dyn hadb_storage::StorageBackend>>,
    turbolite_vfs: Option<(SharedTurboliteVfs, String)>,
    manifest_store: Option<Arc<dyn ManifestStore>>,
    /// Optional replicator override. When `None`, the open path picks a
    /// default based on `turbolite_durability`: walrust-backed
    /// `SqliteReplicator` for `Continuous`/`Checkpoint`, `NoOpReplicator`
    /// for `Cloud`. Setting this explicitly lets a consumer plug in any
    /// `hadb::Replicator` impl (test harnesses, alternative WAL shippers).
    replicator: Option<Arc<dyn Replicator>>,
    rollback_database_id: Option<String>,
    rollback_token: Option<String>,
    rollback_url: Option<String>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Self {
            inner: HaQLiteBuilder::new(),
            mode: HaMode::SingleWriter,
            role: None,
            turbolite_durability: turbodb::Durability::default(),
            turbolite_http: None,
            manifest_http: None,
            turbolite_storage: None,
            turbolite_vfs: None,
            manifest_store: None,
            replicator: None,
            rollback_database_id: None,
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

    pub fn forwarding_mode(mut self, mode: ForwardingMode) -> Self {
        self.inner = self.inner.forwarding_mode(mode);
        self
    }

    pub fn disable_forwarding(mut self) -> Self {
        self.inner = self.inner.disable_forwarding();
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

    pub fn lease_ttl(mut self, ttl_secs: u64) -> Self {
        self.inner = self.inner.lease_ttl(ttl_secs);
        self
    }

    pub fn lease_renew_interval(mut self, interval: Duration) -> Self {
        self.inner = self.inner.lease_renew_interval(interval);
        self
    }

    pub fn lease_follower_poll_interval(mut self, interval: Duration) -> Self {
        self.inner = self.inner.lease_follower_poll_interval(interval);
        self
    }

    pub fn lease_store(mut self, store: Arc<dyn LeaseStore>) -> Self {
        self.inner = self.inner.lease_store(store);
        self
    }

    pub fn lease_endpoint(self, endpoint: &str, token: &str) -> Self {
        let store = Arc::new(hadb_lease_cinch::CinchLeaseStore::new(endpoint, token));
        self.lease_store(store)
    }

    pub fn mode(mut self, mode: HaMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn role(mut self, role: Role) -> Self {
        self.role = Some(role);
        self
    }

    pub fn durability(mut self, durability: turbodb::Durability) -> Self {
        self.turbolite_durability = durability;
        self
    }

    pub fn turbolite_http(mut self, endpoint: &str, token: &str) -> Self {
        self.turbolite_http = Some(CinchHttpConfig::Public {
            endpoint: endpoint.to_string(),
            token: token.to_string(),
        });
        self
    }

    /// Use grabby's internal NoAuth sync API for turbolite page/WAL storage.
    /// Requests carry `database_id` as a query parameter instead of Bearer auth.
    pub fn turbolite_http_internal(mut self, endpoint: &str, database_id: &str) -> Self {
        self.turbolite_http = Some(CinchHttpConfig::Internal {
            endpoint: endpoint.to_string(),
            database_id: database_id.to_string(),
        });
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
        self.manifest_http = None;
        self.manifest_store = Some(store);
        self
    }

    pub fn manifest_endpoint(mut self, endpoint: &str, token: &str) -> Self {
        self.manifest_http = Some(CinchHttpConfig::Public {
            endpoint: endpoint.to_string(),
            token: token.to_string(),
        });
        self.manifest_store = None;
        self
    }

    /// Use grabby's internal NoAuth manifest API for system DB manifests.
    /// Requests carry `database_id` as a query parameter instead of Bearer auth.
    pub fn manifest_endpoint_internal(mut self, endpoint: &str, database_id: &str) -> Self {
        self.manifest_http = Some(CinchHttpConfig::Internal {
            endpoint: endpoint.to_string(),
            database_id: database_id.to_string(),
        });
        self.manifest_store = None;
        self
    }

    pub fn walrust_storage(mut self, storage: Arc<dyn hadb_storage::StorageBackend>) -> Self {
        self.inner = self.inner.walrust_storage(storage);
        self
    }

    /// Inject a custom `hadb::Replicator`. Overrides the durability-driven
    /// default (walrust `SqliteReplicator` for Continuous/Checkpoint,
    /// `NoOpReplicator` for Cloud).
    ///
    /// Useful for tests, alternative WAL shippers, or consumers that want
    /// haqlite-turbolite without walrust at all.
    pub fn replicator(mut self, replicator: Arc<dyn Replicator>) -> Self {
        self.replicator = Some(replicator);
        self
    }

    pub fn authorizer<F, G>(mut self, factory: F) -> Self
    where
        F: Fn(bool) -> G + Send + Sync + 'static,
        G: FnMut(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization
            + Send
            + 'static,
    {
        self.inner = self.inner.authorizer(factory);
        self
    }

    /// Enable Phase Strata rollback detection. Before turbolite VFS opens,
    /// the builder GETs the remote turbolite manifest from the storage URL
    /// (taken from `.turbolite_http()`), compares its `epoch` to the local
    /// cached manifest, and wipes the local cache + SQLite stub on mismatch.
    /// `database_id` is used to compute the cache directory path
    /// (`<db_path.parent>/.tl_cache_<database_id>/`).
    ///
    /// Requires `.turbolite_http(endpoint, _)` to also be set.
    pub fn with_rollback_detection(mut self, database_id: &str, token: &str) -> Self {
        self.rollback_database_id = Some(database_id.to_string());
        self.rollback_token = Some(token.to_string());
        self
    }

    pub fn with_rollback_url(mut self, url: &str) -> Self {
        self.rollback_url = Some(url.to_string());
        self
    }

    pub async fn open(self, db_path: &str, schema: &str) -> Result<HaQLite> {
        let role_for_validation = self.role.unwrap_or(match self.mode {
            HaMode::SingleWriter => Role::Leader,
            HaMode::SharedWriter => Role::LatentWriter,
        });
        hadb::validate_mode_role(self.mode, role_for_validation)
            .map_err(|e| anyhow::anyhow!("invalid mode/role combination: {e}"))?;

        match (self.mode, self.role) {
            (
                HaMode::SingleWriter,
                None | Some(Role::Leader) | Some(Role::Follower) | Some(Role::Client),
            ) => self.open_writer(db_path, schema).await,
            (HaMode::SharedWriter, Some(Role::Client)) => {
                anyhow::bail!("Client mode not yet implemented in haqlite-turbolite")
            }
            (HaMode::SharedWriter, None | Some(Role::LatentWriter)) => {
                anyhow::bail!("SharedWriter mode not yet implemented in haqlite-turbolite")
            }
            (HaMode::SingleWriter, Some(Role::LatentWriter))
            | (HaMode::SharedWriter, Some(Role::Leader | Role::Follower)) => {
                unreachable!("mode/role validation should reject this combination")
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

        // Phase Strata: detect manifest epoch mismatch (fork/rollback) and wipe
        // the local cache + SQLite stub before turbolite opens. Requires
        // turbolite_http to be set so we have a storage URL to GET the remote
        // manifest from.
        if let (Some(ref database_id), Some(ref token)) = (
            self.rollback_database_id.as_ref(),
            self.rollback_token.as_ref(),
        ) {
            let storage_url = match self.turbolite_http {
                Some(ref cfg) => cfg.endpoint().to_string(),
                None => {
                    anyhow::bail!("with_rollback_detection requires .turbolite_http() to be set")
                }
            };
            let detector = crate::RollbackDetector::new(&storage_url, token);
            detector
                .check_and_repair(database_id, &db_path)
                .await
                .map_err(|e| anyhow::anyhow!("rollback detection: {e}"))?;
        }

        let instance_id = self
            .inner
            .get_instance_id()
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                std::env::var("FLY_MACHINE_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
            });
        let forwarding_mode = self.inner.get_forwarding_mode();
        let address = self
            .inner
            .get_address()
            .map(|s| s.to_string())
            .unwrap_or_else(|| match forwarding_mode {
                ForwardingMode::BuiltinHttp { port } => detect_address(&instance_id, port),
                ForwardingMode::Disabled => String::new(),
            });

        // Validate required coordinator inputs before constructing/registering
        // the process-lifetime SQLite VFS. Failed tests and startup retries
        // should not open a Turbolite cache file just to discover that the
        // builder is missing its lease store.
        let lease_store: Arc<dyn hadb::LeaseStore> = self
            .inner
            .get_lease_store()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("lease_store() required"))?;

        // Build or reuse the process-lifetime SQLite VFS. sqlite-vfs does not
        // unregister today, so retrying after a post-registration open error
        // must not create a fresh auto VFS each time.
        let (shared_vfs, vfs_name, shared_fence) = if let Some((ref vfs, ref name)) =
            self.turbolite_vfs
        {
            let shared_fence = self.turbolite_http.as_ref().map(|_| new_shared_fence());
            (vfs.clone(), name.clone(), shared_fence)
        } else {
            let cache_dir = db_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("/tmp"))
                .join(format!(".tl_cache_{}", db_name));
            std::fs::create_dir_all(&cache_dir)?;
            let db_filename = db_path
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_else(|| db_name.clone());
            let tl_config = turbolite::tiered::TurboliteConfig {
                cache_dir: cache_dir.clone(),
                cache: turbolite::tiered::CacheConfig {
                    gc_enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            };
            let rt_handle = tokio::runtime::Handle::current();
            if let Some(ref cfg) = self.turbolite_http {
                let key = auto_vfs_key(
                    &cache_dir,
                    &db_filename,
                    self.inner.get_prefix(),
                    &instance_id,
                    &cfg.identity_key(),
                    &self.turbolite_durability,
                );
                let registration = get_or_register_auto_vfs(key, || {
                    let shared_fence = new_shared_fence();
                    let storage: Arc<dyn hadb_storage::StorageBackend> =
                        Arc::new(cfg.storage("pages").with_fence(shared_fence.0.clone()));
                    let vfs = turbolite::tiered::TurboliteVfs::with_backend(
                        tl_config, storage, rt_handle,
                    )
                    .map_err(|e| anyhow::anyhow!("turbolite VFS (HTTP): {e}"))?;
                    Ok((
                        turbolite::tiered::SharedTurboliteVfs::new(vfs),
                        Some(shared_fence),
                    ))
                })?;
                (
                    registration.shared_vfs,
                    registration.vfs_name,
                    registration.shared_fence,
                )
            } else if let Some(ref storage) = self.turbolite_storage {
                let key = auto_vfs_key(
                    &cache_dir,
                    &db_filename,
                    self.inner.get_prefix(),
                    &instance_id,
                    &format!("caller-supplied:{:p}", Arc::as_ptr(storage)),
                    &self.turbolite_durability,
                );
                let registration = get_or_register_auto_vfs(key, || {
                    let vfs = turbolite::tiered::TurboliteVfs::with_backend(
                        tl_config,
                        storage.clone(),
                        rt_handle,
                    )
                    .map_err(|e| anyhow::anyhow!("turbolite VFS (caller-supplied): {e}"))?;
                    Ok((turbolite::tiered::SharedTurboliteVfs::new(vfs), None))
                })?;
                (
                    registration.shared_vfs,
                    registration.vfs_name,
                    registration.shared_fence,
                )
            } else {
                anyhow::bail!("Turbolite requires .turbolite_http(), .turbolite_storage(), or .turbolite_vfs()")
            }
        };

        let manifest_store: Arc<dyn turbodb::ManifestStore> =
            if let Some(store) = self.manifest_store.clone() {
                store
            } else if let Some(ref cfg) = self.manifest_http {
                let mut store = cfg.manifest_store();
                if let Some((fence, _)) = &shared_fence {
                    store = store.with_fence(fence.clone());
                }
                Arc::new(store)
            } else {
                anyhow::bail!("Turbolite-backed SingleWriter mode requires manifest_store()");
            };

        let walrust_storage: Option<Arc<dyn hadb_storage::StorageBackend>> =
            match self.turbolite_durability {
                turbodb::Durability::Continuous { .. } => {
                    let storage: Arc<dyn hadb_storage::StorageBackend> =
                        if let Some(storage) = self.inner.get_walrust_storage().cloned() {
                            storage
                        } else if let Some(ref cfg) = self.turbolite_http {
                            let fence = shared_fence
                                .as_ref()
                                .map(|(fence, _)| fence.clone())
                                .expect("turbolite_http implies shared fence");
                            Arc::new(cfg.storage("wal").with_fence(fence))
                        } else {
                            anyhow::bail!(
                                "Continuous durability requires walrust storage. \
                                 Pass .walrust_storage(), .turbolite_http(), or .replicator()."
                            );
                        };
                    Some(storage)
                }
                turbodb::Durability::Checkpoint { .. } | turbodb::Durability::Cloud => None,
            };
        let walrust_prefix =
            if self.inner.get_walrust_storage().is_none() && self.turbolite_http.is_some() {
                // Cinch's HTTP sync API already scopes WAL objects by Bearer token
                // or internal `database_id`; adding the builder prefix again would
                // make walrust paths look like `/v1/sync/wal/{database_id}/...`.
                ""
            } else {
                self.inner.get_prefix()
            };

        // Pick replicator. Three sources, in priority order:
        // 1. Caller-supplied via `.replicator(...)` (test harnesses, alt WAL shippers).
        // 2. `Cloud` durability → `NoOpReplicator`.
        // 3. `Checkpoint` / `Continuous` → Turbolite-owned base state, with
        //    optional walrust delta shipping in `Continuous`.
        let replicator: Arc<dyn Replicator> = if let Some(r) = self.replicator.clone() {
            r
        } else {
            match self.turbolite_durability {
                turbodb::Durability::Cloud => Arc::new(NoOpReplicator::new()),
                turbodb::Durability::Checkpoint { .. } => Arc::new(
                    crate::TurboliteReplicator::new(
                        shared_vfs.clone(),
                        manifest_store.clone(),
                        self.inner.get_prefix(),
                        &db_name,
                    )
                    .with_writer_id(instance_id.clone()),
                ),
                turbodb::Durability::Continuous {
                    replication_interval,
                    ..
                } => {
                    let snapshot_interval = self
                        .inner
                        .get_coordinator_config()
                        .map(|c| c.snapshot_interval)
                        .unwrap_or(Duration::from_secs(3600));
                    let delta_replicator =
                        Arc::new(haqlite::ExternalSnapshotSqliteReplicator::new(
                            walrust_storage
                                .as_ref()
                                .expect("continuous durability validated walrust storage")
                                .clone(),
                            walrust_prefix,
                            walrust::ReplicationConfig {
                                sync_interval: replication_interval,
                                snapshot_interval,
                                autonomous_snapshots: false,
                                ..Default::default()
                            },
                            Arc::new(turbolite::tiered::TurboliteSnapshotSource::new(
                                shared_vfs.clone(),
                            )),
                        )?);
                    Arc::new(crate::TurboliteWalReplicator::new(
                        shared_vfs.clone(),
                        manifest_store.clone(),
                        self.inner.get_prefix(),
                        &db_name,
                        instance_id.clone(),
                        walrust_prefix.to_string(),
                        walrust_storage
                            .as_ref()
                            .expect("continuous durability validated walrust storage")
                            .clone(),
                        delta_replicator,
                    ))
                }
            }
        };

        let manifest_wakeup = Arc::new(tokio::sync::Notify::new());
        let follower_behavior: Arc<dyn hadb::FollowerBehavior> = {
            let mut behavior = crate::TurboliteFollowerBehavior::new(shared_vfs.clone())
                .with_wakeup(manifest_wakeup);
            match self.turbolite_durability {
                turbodb::Durability::Cloud => {}
                turbodb::Durability::Checkpoint { .. } => {
                    behavior = behavior.with_manifest_store(
                        manifest_store.clone(),
                        format!("{}{}/_manifest", self.inner.get_prefix(), db_name),
                    );
                }
                turbodb::Durability::Continuous { .. } => {
                    behavior = behavior
                        .with_manifest_store(
                            manifest_store.clone(),
                            format!("{}{}/_manifest", self.inner.get_prefix(), db_name),
                        )
                        .with_walrust(
                            walrust_storage
                                .as_ref()
                                .expect("continuous durability validated walrust storage")
                                .clone(),
                            walrust_prefix.to_string(),
                        );
                }
            }
            Arc::new(behavior)
        };

        // Build coordinator config.
        let mut config = self
            .inner
            .get_coordinator_config()
            .cloned()
            .unwrap_or_default();
        let mut lease_cfg = match config.lease.take() {
            Some(mut existing) => {
                existing.store = lease_store.clone();
                existing.instance_id = instance_id.clone();
                existing.address = address.clone();
                existing
            }
            None => {
                haqlite::LeaseConfig::new(lease_store.clone(), instance_id.clone(), address.clone())
            }
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
        config.requested_role = self.role;
        if let Some((_, fence_writer)) = shared_fence {
            config.fence_writer = Some(fence_writer);
        }

        let replicator_for_flush = replicator.clone();

        // Build connection opener for turbolite VFS.
        let vfs_name_for_opener = vfs_name.clone();
        let db_name_for_opener = db_name.clone();
        let db_filename = db_path
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_else(|| db_name.clone());
        let db_filename_for_opener = db_filename.clone();
        let is_cloud = self.turbolite_durability.is_cloud();
        let is_continuous = matches!(
            self.turbolite_durability,
            turbodb::Durability::Continuous { .. }
        );
        let connection_opener: Arc<
            dyn Fn() -> Result<rusqlite::Connection, haqlite::HaQLiteError> + Send + Sync,
        > = Arc::new(move || {
            let vfs_uri = format!(
                "file:{}?vfs={}",
                db_filename_for_opener, vfs_name_for_opener
            );
            let conn = rusqlite::Connection::open_with_flags(
                &vfs_uri,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI,
            )
            .map_err(|e| {
                haqlite::HaQLiteError::DatabaseError(format!(
                    "turbolite conn open for '{}': {}",
                    db_name_for_opener, e
                ))
            })?;
            // Continuous mode needs WAL (walrust ships the live WAL).
            // Checkpoint/cloud need DELETE so commits land in the VFS
            // main-db cache where turbolite's checkpoint flush picks
            // them up — pinned explicitly because SQLite defaults to
            // WAL on a reopened turbolite-backed DB.
            if !is_continuous || is_cloud {
                conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA cache_size=-64000;")
                    .map_err(|e| haqlite::HaQLiteError::DatabaseError(format!("pragma: {e}")))?;
            } else {
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000;")
                        .map_err(|e| haqlite::HaQLiteError::DatabaseError(format!("journal pragma: {e}")))?;
            }
            Ok(conn)
        });

        // Follower reads go through the VFS so xLock(SHARED) takes
        // the replay gate, blocking torn reads against an in-flight
        // materialize.
        let vfs_name_for_follower = vfs_name.clone();
        let db_filename_for_follower = db_filename.clone();
        let db_name_for_follower = db_name.clone();
        let follower_read_connection_opener: Arc<
            dyn Fn() -> Result<rusqlite::Connection, haqlite::HaQLiteError> + Send + Sync,
        > = Arc::new(move || {
            let vfs_uri = format!(
                "file:{}?vfs={}",
                db_filename_for_follower, vfs_name_for_follower
            );
            rusqlite::Connection::open_with_flags(
                &vfs_uri,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI,
            )
            .map_err(|e| {
                haqlite::HaQLiteError::DatabaseError(format!(
                    "turbolite follower VFS open for '{}': {}",
                    db_name_for_follower, e
                ))
            })
        });

        let db_name_for_flush = db_name.clone();
        let flush_runtime = tokio::runtime::Handle::current();
        let flush_mutex = Arc::new(Mutex::new(()));
        let on_flush: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>> =
            Some(Arc::new(move || {
                let _guard = flush_mutex.lock().map_err(|_| {
                    anyhow::anyhow!("haqlite-turbolite sync/publish mutex poisoned")
                })?;
                let replicator = replicator_for_flush.clone();
                let db_name = db_name_for_flush.clone();
                let handle = flush_runtime.clone();
                std::thread::spawn(move || {
                    handle.block_on(async move { replicator.sync(&db_name).await })
                })
                .join()
                .map_err(|_| anyhow::anyhow!("haqlite-turbolite sync/publish thread panicked"))?
                .map_err(|e| anyhow::anyhow!("haqlite-turbolite sync/publish failed: {e}"))
            }));

        let schema_for_leader_setup = schema.to_string();
        let opener_for_leader_setup = connection_opener.clone();
        config.before_leader_add = Some(Arc::new(move || {
            let conn = opener_for_leader_setup()
                .map_err(|e| anyhow::anyhow!("turbolite leader setup open failed: {e}"))?;
            if !schema_for_leader_setup.trim().is_empty() {
                conn.execute_batch(&schema_for_leader_setup)
                    .map_err(|e| anyhow::anyhow!("turbolite leader setup schema failed: {e}"))?;
            }
            Ok(())
        }));

        let coordinator = haqlite::Coordinator::new(
            replicator,
            None,
            None,
            follower_behavior,
            self.inner.get_prefix(),
            config,
        );

        haqlite::database::open_with_coordinator(
            coordinator,
            db_path,
            &db_name,
            schema,
            &address,
            forwarding_mode,
            self.inner.get_forward_timeout(),
            self.inner.get_secret().map(|s| s.to_string()),
            self.inner.get_read_concurrency(),
            self.inner.get_authorizer().cloned(),
            Some(connection_opener),
            Some(follower_read_connection_opener),
            on_flush,
            true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use hadb_storage::StorageBackend;

    #[tokio::test(flavor = "multi_thread")]
    async fn auto_vfs_registration_skips_preflight_failures() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let db_path = tmp.path().join("retry.db");
        let storage: Arc<dyn StorageBackend> = Arc::new(hadb_storage_local::LocalStorage::new(
            tmp.path().join("remote"),
        ));
        let manifest_store: Arc<dyn ManifestStore> =
            Arc::new(turbodb_manifest_mem::MemManifestStore::new());
        let before = auto_vfs_registry_len();

        for _ in 0..3 {
            let result = Builder::new()
                .prefix("retry/")
                .mode(HaMode::SingleWriter)
                .durability(turbodb::Durability::Cloud)
                .turbolite_storage(storage.clone())
                .manifest_store(manifest_store.clone())
                .instance_id("retry-node")
                .disable_forwarding()
                .open(db_path.to_str().expect("path"), "")
                .await;

            let err = match result {
                Ok(_) => panic!("open should fail without lease_store"),
                Err(err) => err,
            };
            assert!(
                err.to_string().contains("lease_store() required"),
                "unexpected error: {err:#}"
            );
        }

        assert_eq!(
            auto_vfs_registry_len(),
            before,
            "preflight failures must not register a process-lifetime VFS"
        );
    }
}
