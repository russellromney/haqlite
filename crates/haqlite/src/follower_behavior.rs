//! SQLite-specific follower behavior implementation.
//!
//! WAL-based (walrust): for Replicated and Eventual durability

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
pub struct SqliteFollowerBehavior {
    /// walrust storage backend for WAL-based replication.
    walrust_storage: Arc<dyn StorageBackend>,
    /// Optional wakeup signal for fast-path manifest catch-up.
    /// When the coordinator receives a ManifestChanged event, it calls notify_one()
    /// to immediately wake the follower loop instead of waiting for poll interval.
    wakeup: Option<Arc<tokio::sync::Notify>>,
}

impl SqliteFollowerBehavior {
    pub fn new(walrust_storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            walrust_storage,
            wakeup: None,
        }
    }

    /// Enable fast-path wakeup on ManifestChanged events.
    /// The Notify is shared with the role listener which calls notify_one()
    /// when the coordinator receives ManifestChanged.
    pub fn with_wakeup(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.wakeup = Some(notify);
        self
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
