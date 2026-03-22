use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use anyhow::Result;
use tokio::sync::{broadcast, watch, RwLock};

use walrust_core::storage::StorageBackend;
use walrust_core::sync::ReplicationConfig;
use walrust_core::Replicator;

use crate::follower;
use crate::lease::DbLease;
use crate::lease_store::LeaseStore;
use crate::metrics::HaMetrics;
use crate::node_registry::{NodeRegistration, NodeRegistry, S3NodeRegistry};
use crate::types::{CoordinatorConfig, Role, RoleEvent};

const ROLE_LEADER: u8 = 0;
const ROLE_FOLLOWER: u8 = 1;

/// Atomic wrapper around Role for lock-free reads.
/// Shared via Arc between the coordinator map and background tasks.
pub(crate) struct AtomicRole(AtomicU8);

impl AtomicRole {
    fn new(role: Role) -> Self {
        Self(AtomicU8::new(match role {
            Role::Leader => ROLE_LEADER,
            Role::Follower => ROLE_FOLLOWER,
        }))
    }

    pub(crate) fn load(&self) -> Role {
        match self.0.load(Ordering::SeqCst) {
            ROLE_LEADER => Role::Leader,
            _ => Role::Follower,
        }
    }

    pub(crate) fn store(&self, role: Role) {
        self.0.store(
            match role {
                Role::Leader => ROLE_LEADER,
                Role::Follower => ROLE_FOLLOWER,
            },
            Ordering::SeqCst,
        );
    }
}

/// Per-database entry in the coordinator.
struct DbEntry {
    /// Shared between coordinator map and background tasks (monitor updates on promotion).
    role: Arc<AtomicRole>,
    /// Current leader's address for this database (for write proxying).
    leader_address: Arc<RwLock<String>>,
    cancel_tx: watch::Sender<bool>,
    /// Separate channel for stopping the follower pull loop (followers only).
    /// On leave(), we send on BOTH cancel_tx and follower_stop_tx.
    /// On promotion, the monitor sends on follower_stop_tx only — cancel_tx is unaffected.
    follower_stop_tx: Option<watch::Sender<bool>>,
}

/// HA coordinator for SQLite databases.
///
/// Wraps walrust's Replicator with lease-based role assignment.
/// Call `join()` to add a database — it claims or follows automatically.
/// Call `leave()` to cleanly remove a database (final sync + lease release).
///
/// When `lease_store` is None, all databases are Leaders (single-node mode).
pub struct Coordinator {
    replicator: Arc<Replicator>,
    storage: Arc<dyn StorageBackend>,
    lease_store: Option<Arc<dyn LeaseStore>>,
    node_registry: Option<Arc<dyn NodeRegistry>>,
    config: CoordinatorConfig,
    prefix: String,
    databases: RwLock<HashMap<String, DbEntry>>,
    role_tx: broadcast::Sender<RoleEvent>,
    metrics: Arc<HaMetrics>,
}

impl Coordinator {
    /// Create a new Coordinator.
    ///
    /// `storage` is the walrust S3 backend for WAL data.
    /// `lease_store` is for CAS lease operations (None = no HA, always Leader).
    /// `prefix` is the S3 key prefix for all databases (e.g. "wal/" or "ha/").
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        lease_store: Option<Arc<dyn LeaseStore>>,
        prefix: &str,
        config: CoordinatorConfig,
    ) -> Arc<Self> {
        let replication_config = ReplicationConfig {
            sync_interval: config.sync_interval,
            snapshot_interval: config.snapshot_interval,
            ..Default::default()
        };

        let replicator = Replicator::new(storage.clone(), prefix, replication_config);

        let (role_tx, _) = broadcast::channel(64);

        // Auto-create node registry when HA is enabled (lease_store present).
        let node_registry: Option<Arc<dyn NodeRegistry>> = if lease_store.is_some() {
            Some(Arc::new(S3NodeRegistry::new(storage.clone())))
        } else {
            None
        };

        Arc::new(Self {
            replicator,
            storage,
            lease_store,
            node_registry,
            config,
            prefix: prefix.to_string(),
            databases: RwLock::new(HashMap::new()),
            role_tx,
            metrics: Arc::new(HaMetrics::new()),
        })
    }

    /// Join a database to the HA cluster.
    ///
    /// If leases are enabled: claims the lease → Leader, or follows → Follower.
    /// If leases are disabled: starts leader sync → always Leader.
    ///
    /// Returns the initial role.
    pub async fn join(&self, name: &str, db_path: &Path) -> Result<Role> {
        let lease_store = match &self.lease_store {
            Some(ls) => ls.clone(),
            None => {
                // No HA — always leader. Just use walrust directly.
                tokio::time::timeout(self.config.replicator_timeout, self.replicator.add(name, db_path))
                    .await
                    .map_err(|_| anyhow::anyhow!(
                        "Coordinator: replicator.add('{}') timed out after {:?}",
                        name, self.config.replicator_timeout
                    ))??;
                let (cancel_tx, _) = watch::channel(false);
                self.databases.write().await.insert(
                    name.to_string(),
                    DbEntry {
                        role: Arc::new(AtomicRole::new(Role::Leader)),
                        leader_address: Arc::new(RwLock::new(String::new())),
                        cancel_tx,
                        follower_stop_tx: None,
                    },
                );
                let _ = self.role_tx.send(RoleEvent::Joined {
                    db_name: name.to_string(),
                    role: Role::Leader,
                });
                tracing::info!("Coordinator: '{}' joined as Leader (no HA)", name);
                return Ok(Role::Leader);
            }
        };

        // HA mode: try to claim lease.
        let lease_config = self
            .config
            .lease
            .as_ref()
            .expect("lease config must be present when lease_store is Some");

        let mut lease = DbLease::new(
            lease_store,
            &self.prefix,
            name,
            &lease_config.instance_id,
            &lease_config.address,
            lease_config.ttl_secs,
        );

        self.metrics.inc(&self.metrics.lease_claims_attempted);
        let role = lease.try_claim().await?;
        match role {
            Role::Leader => self.metrics.inc(&self.metrics.lease_claims_succeeded),
            Role::Follower => self.metrics.inc(&self.metrics.lease_claims_failed),
        }
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let shared_role = Arc::new(AtomicRole::new(role));

        let leader_addr = Arc::new(RwLock::new(
            if role == Role::Leader {
                lease_config.address.clone()
            } else {
                // Read the current leader's address from the lease.
                match DbLease::new(
                    self.lease_store.as_ref().unwrap().clone(),
                    &self.prefix, name,
                    &lease_config.instance_id, &lease_config.address, lease_config.ttl_secs,
                ).read().await {
                    Ok(Some((data, _))) => data.address,
                    _ => String::new(),
                }
            },
        ));

        match role {
            Role::Leader => {
                // Start leader sync (snapshot + WAL push).
                tokio::time::timeout(self.config.replicator_timeout, self.replicator.add(name, db_path))
                    .await
                    .map_err(|_| anyhow::anyhow!(
                        "Coordinator: replicator.add('{}') timed out after {:?}",
                        name, self.config.replicator_timeout
                    ))??;

                // Spawn lease renewal loop.
                let replicator = self.replicator.clone();
                let db_name = name.to_string();
                let role_tx = self.role_tx.clone();
                let renew_interval = lease_config.renew_interval;
                let max_errors = lease_config.max_consecutive_renewal_errors;
                let replicator_timeout = self.config.replicator_timeout;
                let role_ref = shared_role.clone();
                let metrics = self.metrics.clone();
                let mut renewal_cancel_rx = cancel_rx.clone();
                tokio::spawn(async move {
                    let demoted = follower::run_leader_renewal_standalone(
                        lease,
                        &replicator,
                        &db_name,
                        &role_tx,
                        renew_interval,
                        &mut renewal_cancel_rx,
                        max_errors,
                        replicator_timeout,
                        metrics,
                    )
                    .await;
                    if demoted {
                        role_ref.store(Role::Follower);
                    }
                });

                self.databases.write().await.insert(
                    name.to_string(),
                    DbEntry {
                        role: shared_role,
                        leader_address: leader_addr,
                        cancel_tx,
                        follower_stop_tx: None,
                    },
                );

                let _ = self.role_tx.send(RoleEvent::Joined {
                    db_name: name.to_string(),
                    role: Role::Leader,
                });
                tracing::info!("Coordinator: '{}' joined as Leader", name);
                Ok(Role::Leader)
            }
            Role::Follower => {
                // Restore from S3 to get a base snapshot.
                let txid = match self.restore_internal(name, db_path).await? {
                    Some(txid) => txid,
                    None => 0,
                };

                // Register as follower for read replica discovery.
                if let Some(ref registry) = self.node_registry {
                    // Read the leader's session_id from the lease.
                    if let Ok(Some((leader_data, _))) = lease.read().await {
                        let reg = NodeRegistration {
                            instance_id: lease_config.instance_id.clone(),
                            address: lease_config.address.clone(),
                            role: "follower".to_string(),
                            leader_session_id: leader_data.session_id,
                            last_seen: chrono::Utc::now().timestamp() as u64,
                        };
                        if let Err(e) = registry.register(&self.prefix, name, &reg).await {
                            tracing::error!(
                                "Coordinator: failed to register follower '{}': {}", name, e
                            );
                        }
                    }
                }

                // Shared TXID for warm promotion: follower loop writes, monitor reads.
                let follower_txid = Arc::new(std::sync::atomic::AtomicU64::new(txid));

                // Separate channel for the follower pull loop.
                // The monitor sends on follower_stop_tx to stop the pull loop on promotion.
                // This is a DIFFERENT channel from cancel_tx, so the monitor's cancel_rx
                // is unaffected — no borrow_and_update hack needed.
                let (follower_stop_tx, follower_stop_rx) = watch::channel(false);

                // Spawn follower pull loop — listens to follower_stop_rx.
                {
                    let storage = self.storage.clone();
                    let prefix = self.prefix.clone();
                    let db_name = name.to_string();
                    let db_path_buf = db_path.to_path_buf();
                    let pull_interval = self.config.follower_pull_interval;
                    let txid_out = follower_txid.clone();
                    let metrics = self.metrics.clone();
                    tokio::spawn(async move {
                        let _ = follower::run_follower_loop(
                            storage,
                            prefix,
                            db_name,
                            db_path_buf,
                            txid,
                            pull_interval,
                            txid_out,
                            follower_stop_rx,
                            metrics,
                        )
                        .await;
                    });
                }

                // Spawn lease monitor — sends on follower_stop_tx, listens to cancel_rx.
                {
                    let monitor_cancel_rx = cancel_rx.clone();
                    let replicator = self.replicator.clone();
                    let storage = self.storage.clone();
                    let prefix = self.prefix.clone();
                    let db_name = name.to_string();
                    let db_path_buf = db_path.to_path_buf();
                    let role_tx = self.role_tx.clone();
                    let poll_interval = lease_config.follower_poll_interval;
                    let renew_interval = lease_config.renew_interval;
                    let required_expired_reads = lease_config.required_expired_reads;
                    let max_errors = lease_config.max_consecutive_renewal_errors;
                    let role_ref = shared_role.clone();
                    let txid_ref = follower_txid.clone();
                    let addr_ref = leader_addr.clone();
                    let self_address = lease_config.address.clone();

                    let monitor_lease = DbLease::new(
                        self.lease_store.as_ref().unwrap().clone(),
                        &self.prefix,
                        name,
                        &lease_config.instance_id,
                        &lease_config.address,
                        lease_config.ttl_secs,
                    );

                    let replicator_timeout = self.config.replicator_timeout;
                    let monitor_follower_stop_tx = follower_stop_tx.clone();
                    let monitor_node_registry = self.node_registry.clone();
                    let metrics = self.metrics.clone();
                    tokio::spawn(async move {
                        let _result = follower::run_lease_monitor(
                            monitor_lease,
                            replicator,
                            storage,
                            prefix,
                            db_name,
                            db_path_buf,
                            txid_ref,
                            monitor_follower_stop_tx,
                            role_tx,
                            poll_interval,
                            renew_interval,
                            required_expired_reads,
                            addr_ref,
                            role_ref,
                            self_address,
                            max_errors,
                            replicator_timeout,
                            monitor_cancel_rx,
                            monitor_node_registry,
                            metrics,
                        )
                        .await;
                    });
                }

                self.databases.write().await.insert(
                    name.to_string(),
                    DbEntry {
                        role: shared_role,
                        leader_address: leader_addr,
                        cancel_tx,
                        follower_stop_tx: Some(follower_stop_tx),
                    },
                );

                let _ = self.role_tx.send(RoleEvent::Joined {
                    db_name: name.to_string(),
                    role: Role::Follower,
                });
                tracing::info!("Coordinator: '{}' joined as Follower", name);
                Ok(Role::Follower)
            }
        }
    }

    /// Leave the HA cluster for a database.
    ///
    /// If Leader: final WAL sync + lease release.
    /// If Follower: stop pull loop + monitor.
    pub async fn leave(&self, name: &str) -> Result<()> {
        let entry = self.databases.write().await.remove(name);
        let entry = match entry {
            Some(e) => e,
            None => return Ok(()),
        };

        // Cancel all background loops for this database.
        let _ = entry.cancel_tx.send(true);
        if let Some(ref stop_tx) = entry.follower_stop_tx {
            let _ = stop_tx.send(true);
        }

        let role = entry.role.load();

        match role {
            Role::Leader => {
                // Final WAL sync (best-effort, don't block leave on timeout).
                if tokio::time::timeout(self.config.replicator_timeout, self.replicator.remove(name))
                    .await
                    .is_err()
                {
                    tracing::error!(
                        "Coordinator: replicator.remove('{}') timed out after {:?}, continuing leave",
                        name, self.config.replicator_timeout
                    );
                }

                // Release lease — read first to verify we still hold it before deleting.
                if let Some(ls) = &self.lease_store {
                    if let Some(lease_config) = &self.config.lease {
                        let lease = DbLease::new(
                            ls.clone(),
                            &self.prefix,
                            name,
                            &lease_config.instance_id,
                            &lease_config.address,
                            lease_config.ttl_secs,
                        );
                        match lease.read().await {
                            Ok(Some((data, _))) if data.instance_id == lease_config.instance_id => {
                                if let Err(e) = ls.delete(lease.lease_key()).await {
                                    tracing::error!(
                                        "Coordinator: failed to release lease for '{}': {}",
                                        name, e
                                    );
                                }
                            }
                            Ok(Some((data, _))) => {
                                tracing::info!(
                                    "Coordinator: lease for '{}' held by {} (not us), skipping release",
                                    name, data.instance_id
                                );
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::error!(
                                    "Coordinator: failed to read lease for '{}' during leave: {}",
                                    name, e
                                );
                            }
                        }
                    }
                }
                tracing::info!("Coordinator: '{}' left (was Leader)", name);
            }
            Role::Follower => {
                // Deregister from node registry.
                if let (Some(ref registry), Some(ref lease_config)) =
                    (&self.node_registry, &self.config.lease)
                {
                    if let Err(e) = registry
                        .deregister(&self.prefix, name, &lease_config.instance_id)
                        .await
                    {
                        tracing::error!(
                            "Coordinator: failed to deregister follower '{}': {}",
                            name, e
                        );
                    }
                }
                tracing::info!("Coordinator: '{}' left (was Follower)", name);
            }
        }

        Ok(())
    }

    /// Get the current role of a database.
    pub async fn role(&self, name: &str) -> Option<Role> {
        self.databases
            .read()
            .await
            .get(name)
            .map(|e| e.role.load())
    }

    /// Get the current leader's address for a database.
    pub async fn leader_address(&self, name: &str) -> Option<String> {
        let addr = self
            .databases
            .read()
            .await
            .get(name)
            .map(|e| e.leader_address.clone());
        match addr {
            Some(a) => Some(a.read().await.clone()),
            None => None,
        }
    }

    /// Subscribe to role change events.
    pub fn role_events(&self) -> broadcast::Receiver<RoleEvent> {
        self.role_tx.subscribe()
    }

    /// Get shared metrics reference for this coordinator.
    pub fn metrics(&self) -> &Arc<HaMetrics> {
        &self.metrics
    }

    /// Restore a database from S3.
    pub async fn restore(&self, name: &str, output_path: &Path) -> Result<Option<u64>> {
        self.restore_internal(name, output_path).await
    }

    /// Discover registered replicas for a database, filtered to valid ones
    /// (matching current leader session and within TTL).
    pub async fn discover_replicas(&self, name: &str) -> Result<Vec<NodeRegistration>> {
        let registry = match &self.node_registry {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };
        let lease_config = match &self.config.lease {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        let all = registry.discover_all(&self.prefix, name).await?;

        // Get current leader's session_id to filter stale registrations.
        let current_session_id = match &self.lease_store {
            Some(ls) => {
                let lease = DbLease::new(
                    ls.clone(),
                    &self.prefix,
                    name,
                    &lease_config.instance_id,
                    &lease_config.address,
                    lease_config.ttl_secs,
                );
                match lease.read().await {
                    Ok(Some((data, _))) => data.session_id,
                    _ => return Ok(Vec::new()),
                }
            }
            None => return Ok(Vec::new()),
        };

        Ok(all
            .into_iter()
            .filter(|r| r.is_valid(&current_session_id, lease_config.ttl_secs * 6))
            .collect())
    }

    /// Check if a specific database is in the coordinator.
    pub async fn contains(&self, name: &str) -> bool {
        self.databases.read().await.contains_key(name)
    }

    /// Number of databases currently managed.
    pub async fn database_count(&self) -> usize {
        self.databases.read().await.len()
    }

    // ========================================================================
    // Internal
    // ========================================================================

    async fn restore_internal(&self, name: &str, output_path: &Path) -> Result<Option<u64>> {
        self.replicator.restore(name, output_path).await
    }
}
