//! SQLite-specific follower behavior implementation.

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
/// Tracks sequence numbers for incremental WAL replication using walrust.
pub struct SqliteFollowerBehavior {
    /// walrust storage backend for SQLite WAL operations.
    walrust_storage: Arc<dyn WalrustStorageBackend>,
}

impl SqliteFollowerBehavior {
    pub fn new(walrust_storage: Arc<dyn WalrustStorageBackend>) -> Self {
        Self { walrust_storage }
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
                                // New data downloaded -- not caught up until replay completes.
                                caught_up.store(false, Ordering::SeqCst);
                                metrics.follower_caught_up.store(0, Ordering::Relaxed);
                                tracing::debug!(
                                    "Follower '{}': pulled seq {} -> {}",
                                    db_name, current_seq, new_seq
                                );
                                position.store(new_seq, Ordering::SeqCst);
                                metrics.follower_replay_position.store(new_seq, Ordering::Relaxed);
                                metrics.inc(&metrics.follower_pulls_succeeded);
                                // Replay succeeded (pull_incremental applies the WAL).
                                caught_up.store(true, Ordering::SeqCst);
                                metrics.follower_caught_up.store(1, Ordering::Relaxed);
                            } else {
                                // Empty poll -- no new data means we are caught up.
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

    async fn catchup_on_promotion(
        &self,
        prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        position: u64,
    ) -> Result<()> {
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
