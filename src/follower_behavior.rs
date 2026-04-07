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
use walrust::StorageBackend as WalrustStorageBackend;

/// SQLite-specific follower behavior.
///
/// For WAL-based durability (Replicated/Eventual), uses walrust pull_incremental.
/// For turbolite S3Primary (Synchronous), polls turbolite's S3 manifest directly
/// and applies via set_manifest. No WAL exists in S3Primary mode.
pub struct SqliteFollowerBehavior {
    /// walrust storage backend for WAL-based replication.
    walrust_storage: Arc<dyn WalrustStorageBackend>,
    /// Optional turbolite VFS for manifest-based catch-up (Synchronous durability).
    /// When set, the follower polls turbolite's S3 manifest instead of walrust WAL.
    turbolite_vfs: Option<turbolite::tiered::SharedTurboliteVfs>,
}

impl SqliteFollowerBehavior {
    pub fn new(walrust_storage: Arc<dyn WalrustStorageBackend>) -> Self {
        Self {
            walrust_storage,
            turbolite_vfs: None,
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

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let current_version = position.load(Ordering::SeqCst);
                        // fetch_and_apply_s3_manifest is blocking (S3 I/O).
                        // Run in spawn_blocking to avoid blocking the tokio runtime.
                        let vfs_clone = vfs.clone();
                        let fetch_result = tokio::task::spawn_blocking(move || {
                            vfs_clone.fetch_and_apply_s3_manifest()
                        }).await;

                        match fetch_result {
                            Ok(Ok(Some(new_version))) if new_version > current_version => {
                                caught_up.store(false, Ordering::SeqCst);
                                metrics.follower_caught_up.store(0, Ordering::Relaxed);
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
                                // No new manifest or same version.
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
                    }
                    _ = cancel_rx.changed() => {
                        let v = position.load(Ordering::SeqCst);
                        tracing::info!("Follower '{}': cancelled at turbolite manifest v{}", db_name, v);
                        return Ok(());
                    }
                }
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
                                    caught_up.store(false, Ordering::SeqCst);
                                    metrics.follower_caught_up.store(0, Ordering::Relaxed);
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
                        let current_seq = position.load(Ordering::SeqCst);
                        tracing::info!("Follower '{}': cancelled at seq {}", db_name, current_seq);
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
                vfs_clone.fetch_and_apply_s3_manifest()
            }).await.map_err(|e| anyhow::anyhow!("manifest fetch task panicked: {}", e))?;

            match result {
                Ok(Some(version)) => {
                    tracing::info!(
                        "SqliteFollowerBehavior '{}': turbolite catchup to v{} on promotion",
                        db_name, version,
                    );
                }
                Ok(None) => {
                    tracing::info!(
                        "SqliteFollowerBehavior '{}': no turbolite manifest in S3 (empty DB)",
                        db_name,
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "SqliteFollowerBehavior '{}': turbolite manifest fetch on promotion failed: {}",
                        db_name, e,
                    );
                }
            }
            Ok(())
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
