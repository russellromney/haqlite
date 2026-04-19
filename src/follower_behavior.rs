//! SQLite-specific follower behavior implementation.
//!
//! Supports two replication paths:
//! - WAL-based (walrust): for Replicated and Eventual durability
//! - Manifest-based (turbolite): for Synchronous durability (S3Primary, no WAL)

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

use hadb::{FollowerBehavior, HaMetrics, Replicator};
use hadb_storage::StorageBackend;

/// SQLite-specific follower behavior.
///
/// For WAL-based durability (Replicated/Eventual), uses walrust pull_incremental.
/// For turbolite S3Primary (Synchronous), polls turbolite's S3 manifest directly
/// and applies via set_manifest. No WAL exists in S3Primary mode.
pub struct SqliteFollowerBehavior {
    /// walrust storage backend for WAL-based replication.
    walrust_storage: Arc<dyn StorageBackend>,
    /// Optional turbolite VFS for manifest-based catch-up (Synchronous durability).
    /// When set, the follower polls turbolite's S3 manifest instead of walrust WAL.
    turbolite_vfs: Option<turbolite::tiered::SharedTurboliteVfs>,
    /// Optional wakeup signal for fast-path manifest catch-up.
    /// When the coordinator receives a ManifestChanged event, it calls notify_one()
    /// to immediately wake the follower loop instead of waiting for poll interval.
    wakeup: Option<Arc<tokio::sync::Notify>>,
}

impl SqliteFollowerBehavior {
    pub fn new(walrust_storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            walrust_storage,
            turbolite_vfs: None,
            wakeup: None,
        }
    }

    /// Enable turbolite manifest-based catch-up for Synchronous durability.
    /// The VFS must point to the same S3 bucket/prefix as the leader's turbolite.
    pub fn with_turbolite_catchup(
        mut self,
        vfs: turbolite::tiered::SharedTurboliteVfs,
    ) -> Self {
        self.turbolite_vfs = Some(vfs);
        self
    }

    /// Enable fast-path wakeup on ManifestChanged events.
    /// The Notify is shared with the role listener which calls notify_one()
    /// when the coordinator receives ManifestChanged.
    pub fn with_wakeup(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.wakeup = Some(notify);
        self
    }

    fn uses_turbolite_catchup(&self) -> bool {
        self.turbolite_vfs.is_some()
    }
}

#[async_trait]
impl FollowerBehavior for SqliteFollowerBehavior {
    async fn run_follower_loop(
        &self,
        _replicator: Arc<dyn Replicator>,
        prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        poll_interval: Duration,
        position: Arc<AtomicU64>,
        caught_up: Arc<AtomicBool>,
        mut cancel_rx: watch::Receiver<bool>,
        metrics: Arc<HaMetrics>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(poll_interval);

        if self.uses_turbolite_catchup() {
            // Turbolite manifest-based catch-up for S3Primary (no WAL).
            // Polls turbolite's own S3 manifest directly. Position tracks
            // the turbolite manifest version.
            let vfs = self.turbolite_vfs.as_ref()
                .expect("turbolite_vfs required for turbolite catchup");

            // Macro to avoid duplicating the manifest fetch + apply logic.
            // Called from both interval.tick() and notify.notified() branches.
            macro_rules! fetch_manifest {
                () => {{
                    // Check cancel before doing S3 work. The cancel_rx is set
                    // during promotion, and we must not call set_manifest after
                    // the leader starts writing (it would evict dirty pages).
                    if *cancel_rx.borrow() {
                        return Ok(());
                    }
                    let current_version = position.load(Ordering::SeqCst);
                    let vfs_clone = vfs.clone();
                    let fetch_result = tokio::task::spawn_blocking(move || {
                        vfs_clone.fetch_and_apply_remote_manifest()
                    }).await;

                    match fetch_result {
                        Ok(Ok(Some(new_version))) if new_version > current_version => {
                            tracing::debug!(
                                "Follower '{}': turbolite manifest v{} -> v{}",
                                db_name, current_version, new_version,
                            );
                            position.store(new_version, Ordering::SeqCst);
                            metrics.follower_replay_position.store(new_version, Ordering::Relaxed);
                            metrics.inc(&metrics.follower_pulls_succeeded);
                            caught_up.store(true, Ordering::SeqCst);
                            metrics.follower_caught_up.store(1, Ordering::Relaxed);
                        }
                        Ok(Ok(_)) => {
                            caught_up.store(true, Ordering::SeqCst);
                            metrics.follower_caught_up.store(1, Ordering::Relaxed);
                            metrics.inc(&metrics.follower_pulls_no_new_data);
                        }
                        Ok(Err(e)) => {
                            tracing::error!("Follower '{}': turbolite manifest fetch failed: {}", db_name, e);
                            metrics.inc(&metrics.follower_pulls_failed);
                        }
                        Err(e) => {
                            tracing::error!("Follower '{}': turbolite manifest task panicked: {}", db_name, e);
                            metrics.inc(&metrics.follower_pulls_failed);
                        }
                    }
                }};
            }

            loop {
                // Wait for poll interval, wakeup signal, or cancellation.
                // ManifestChanged wakeup gives sub-millisecond follower catch-up
                // instead of waiting for the full poll interval.
                let do_poll = if let Some(ref notify) = self.wakeup {
                    tokio::select! {
                        _ = interval.tick() => true,
                        _ = notify.notified() => true,
                        _ = cancel_rx.changed() => false,
                    }
                } else {
                    tokio::select! {
                        _ = interval.tick() => true,
                        _ = cancel_rx.changed() => false,
                    }
                };

                if !do_poll {
                    return Ok(());
                }

                fetch_manifest!();
            }
        } else {
            // WAL-based catch-up via walrust (Replicated/Eventual durability).
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let current_seq = position.load(Ordering::SeqCst);
                        match walrust::sync::pull_incremental(
                            self.walrust_storage.as_ref(),
                            prefix,
                            db_name,
                            db_path,
                            current_seq,
                        ).await {
                            Ok(new_seq) => {
                                if new_seq > current_seq {
                                    tracing::debug!(
                                        "Follower '{}': pulled seq {} -> {}",
                                        db_name, current_seq, new_seq
                                    );
                                    position.store(new_seq, Ordering::SeqCst);
                                    metrics.follower_replay_position.store(new_seq, Ordering::Relaxed);
                                    metrics.inc(&metrics.follower_pulls_succeeded);
                                    caught_up.store(true, Ordering::SeqCst);
                                    metrics.follower_caught_up.store(1, Ordering::Relaxed);
                                } else {
                                    caught_up.store(true, Ordering::SeqCst);
                                    metrics.follower_caught_up.store(1, Ordering::Relaxed);
                                    metrics.inc(&metrics.follower_pulls_no_new_data);
                                }
                            }
                            Err(e) => {
                                tracing::error!("Follower '{}': pull failed: {}", db_name, e);
                                metrics.inc(&metrics.follower_pulls_failed);
                            }
                        }
                    }
                    _ = cancel_rx.changed() => {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn catchup_on_promotion(
        &self,
        prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        position: u64,
    ) -> Result<()> {
        if self.uses_turbolite_catchup() {
            // For turbolite S3Primary: fetch latest S3 manifest before becoming leader.
            let vfs = self.turbolite_vfs.as_ref()
                .expect("turbolite_vfs required");
            let vfs_clone = vfs.clone();
            let result = tokio::task::spawn_blocking(move || {
                vfs_clone.fetch_and_apply_remote_manifest()
            }).await.map_err(|e| anyhow::anyhow!("manifest fetch task panicked: {}", e))?;

            match result {
                Ok(Some(_version)) => Ok(()),
                Ok(None) => {
                    // No manifest in S3 is normal for a fresh cluster.
                    Ok(())
                }
                Err(e) => {
                    Err(anyhow::anyhow!(
                        "turbolite manifest fetch failed on promotion for '{}': {}",
                        db_name, e,
                    ))
                }
            }
        } else {
            // WAL-based catch-up on promotion.
            let new_seq = walrust::sync::pull_incremental(
                self.walrust_storage.as_ref(),
                prefix,
                db_name,
                db_path,
                position,
            )
            .await?;
            tracing::info!(
                "SqliteFollowerBehavior '{}': caught up seq {} -> {}",
                db_name, position, new_seq
            );
            Ok(())
        }
    }
}
