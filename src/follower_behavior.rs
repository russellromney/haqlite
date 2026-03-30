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
/// Tracks TXIDs for incremental WAL replication using walrust.
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
                    let current_txid = position.load(Ordering::SeqCst);
                    match walrust::sync::pull_incremental(
                        self.walrust_storage.as_ref(),
                        prefix,
                        db_name,
                        db_path,
                        current_txid,
                    ).await {
                        Ok(new_txid) => {
                            if new_txid > current_txid {
                                // New data downloaded -- not caught up until replay completes.
                                caught_up.store(false, Ordering::SeqCst);
                                tracing::debug!(
                                    "Follower '{}': pulled TXID {} -> {}",
                                    db_name, current_txid, new_txid
                                );
                                position.store(new_txid, Ordering::SeqCst);
                                caught_up.store(true, Ordering::SeqCst);
                                metrics.inc(&metrics.follower_pulls_succeeded);
                                // Replay succeeded (pull_incremental applies the WAL).
                                caught_up.store(true, Ordering::SeqCst);
                            } else {
                                // Empty poll -- no new data means we are caught up.
                                caught_up.store(true, Ordering::SeqCst);
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
                    let current_txid = position.load(Ordering::SeqCst);
                    tracing::info!("Follower '{}': cancelled at TXID {}", db_name, current_txid);
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
        let new_txid = walrust::sync::pull_incremental(
            self.walrust_storage.as_ref(),
            prefix,
            db_name,
            db_path,
            position,
        )
        .await?;
        tracing::info!(
            "SqliteFollowerBehavior '{}': caught up TXID {} → {}",
            db_name, position, new_txid
        );
        Ok(())
    }
}
