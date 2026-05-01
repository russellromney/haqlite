use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::watch;

use hadb::{FollowerBehavior, HaMetrics, Replicator};
use hadb_storage::StorageBackend;
use turbodb::ManifestStore;

use crate::replay_sink::HaqliteTurboliteReplaySink;

pub struct TurboliteFollowerBehavior {
    vfs: turbolite::tiered::SharedTurboliteVfs,
    manifest_store: Option<Arc<dyn ManifestStore>>,
    manifest_key: Option<String>,
    walrust_storage: Option<Arc<dyn StorageBackend>>,
    walrust_prefix: Option<String>,
    wakeup: Option<Arc<tokio::sync::Notify>>,
}

impl TurboliteFollowerBehavior {
    pub fn new(vfs: turbolite::tiered::SharedTurboliteVfs) -> Self {
        Self {
            vfs,
            manifest_store: None,
            manifest_key: None,
            walrust_storage: None,
            walrust_prefix: None,
            wakeup: None,
        }
    }

    pub fn with_manifest_store(
        mut self,
        store: Arc<dyn ManifestStore>,
        manifest_key: String,
    ) -> Self {
        self.manifest_store = Some(store);
        self.manifest_key = Some(manifest_key);
        self
    }

    pub fn with_walrust(
        mut self,
        storage: Arc<dyn StorageBackend>,
        walrust_prefix: String,
    ) -> Self {
        self.walrust_storage = Some(storage);
        self.walrust_prefix = Some(walrust_prefix);
        self
    }

    pub fn with_wakeup(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.wakeup = Some(notify);
        self
    }

    fn requires_hybrid_payload(&self) -> bool {
        self.walrust_storage.is_some() || self.walrust_prefix.is_some()
    }

    /// Apply a published hybrid manifest. Updates Turbolite's
    /// in-memory manifest, then if the payload carries a walrust
    /// cursor, replays delta pages from `current cursor → latest
    /// uploaded` straight into Turbolite's tiered cache via
    /// `walrust::sync::pull_incremental_into_sink` and
    /// `TurboliteVfs::begin_replay`. No temp SQLite restore file.
    /// No materialize_to_file. No PRAGMA dance. No double-import.
    /// `apply_page` writes go into staging on the `ReplayHandle`;
    /// `finalize` (driven by the walrust pull) installs them under
    /// the replay gate so in-flight readers see either the
    /// pre-replay snapshot or the post-replay snapshot, never a
    /// cross-page mix.
    async fn apply_manifest_payload(
        &self,
        db_name: &str,
        _db_path: &Path,
        payload: &[u8],
    ) -> Result<u64> {
        let walrust = self
            .vfs
            .set_manifest_bytes(payload)
            .map_err(|e| anyhow!("turbolite set_manifest_bytes failed: {}", e))?;

        if self.requires_hybrid_payload() && walrust.is_none() {
            return Err(anyhow!(
                "continuous manifest for '{}' must carry walrust replay cursor",
                db_name
            ));
        }

        if let Some((walrust_seq, _changeset_prefix)) = walrust {
            let storage = self.walrust_storage.as_ref().ok_or_else(|| {
                anyhow!("continuous follower '{}' missing walrust storage", db_name)
            })?;
            let prefix = self.walrust_prefix.as_ref().ok_or_else(|| {
                anyhow!("continuous follower '{}' missing walrust prefix", db_name)
            })?;

            let handle = self
                .vfs
                .begin_replay()
                .map_err(|e| anyhow!("turbolite begin_replay failed: {}", e))?;
            let mut sink = HaqliteTurboliteReplaySink::new(handle);
            let final_seq = walrust::sync::pull_incremental_into_sink(
                storage.as_ref(),
                prefix,
                db_name,
                &mut sink,
                walrust_seq,
            )
            .await?;
            tracing::debug!(
                "Follower '{}': replayed walrust {} -> {} via direct page sink",
                db_name,
                walrust_seq,
                final_seq,
            );
        }

        Ok(self.vfs.manifest().version)
    }

    async fn poll_manifest_store(
        &self,
        db_name: &str,
        db_path: &Path,
    ) -> Result<Option<u64>> {
        let Some(store) = &self.manifest_store else {
            return Ok(None);
        };
        let key = self
            .manifest_key
            .as_ref()
            .ok_or_else(|| anyhow!("manifest_store configured without manifest_key"))?;

        let Some(manifest) = store.get(key).await? else {
            return Ok(None);
        };
        let version = self
            .apply_manifest_payload(db_name, db_path, &manifest.payload)
            .await?;
        Ok(Some(version))
    }
}

#[async_trait]
impl FollowerBehavior for TurboliteFollowerBehavior {
    async fn run_follower_loop(
        &self,
        _replicator: Arc<dyn Replicator>,
        _prefix: &str,
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

            if !do_poll || *cancel_rx.borrow() {
                return Ok(());
            }

            let current_version = position.load(Ordering::SeqCst);

            if self.manifest_store.is_some() {
                match self.poll_manifest_store(db_name, db_path).await {
                    Ok(Some(new_version)) if new_version > current_version => {
                        tracing::debug!(
                            "Follower '{}': manifest v{} -> v{}",
                            db_name,
                            current_version,
                            new_version,
                        );
                        position.store(new_version, Ordering::SeqCst);
                        metrics
                            .follower_replay_position
                            .store(new_version, Ordering::Relaxed);
                        metrics.inc(&metrics.follower_pulls_succeeded);
                        caught_up.store(true, Ordering::SeqCst);
                        metrics.follower_caught_up.store(1, Ordering::Relaxed);
                    }
                    Ok(Some(_)) | Ok(None) => {
                        caught_up.store(true, Ordering::SeqCst);
                        metrics.follower_caught_up.store(1, Ordering::Relaxed);
                        metrics.inc(&metrics.follower_pulls_no_new_data);
                    }
                    Err(e) => {
                        tracing::error!(
                            "Follower '{}': manifest-store catch-up failed: {}",
                            db_name,
                            e
                        );
                        metrics.inc(&metrics.follower_pulls_failed);
                    }
                }
                continue;
            }

            let vfs_clone = self.vfs.clone();
            let fetch_result =
                tokio::task::spawn_blocking(move || vfs_clone.fetch_and_apply_remote_manifest())
                    .await;

            match fetch_result {
                Ok(Ok(Some(new_version))) if new_version > current_version => {
                    tracing::debug!(
                        "Follower '{}': turbolite manifest v{} -> v{}",
                        db_name,
                        current_version,
                        new_version,
                    );
                    position.store(new_version, Ordering::SeqCst);
                    metrics
                        .follower_replay_position
                        .store(new_version, Ordering::Relaxed);
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
                    tracing::error!(
                        "Follower '{}': turbolite manifest fetch failed: {}",
                        db_name,
                        e
                    );
                    metrics.inc(&metrics.follower_pulls_failed);
                }
                Err(e) => {
                    tracing::error!(
                        "Follower '{}': turbolite manifest task panicked: {}",
                        db_name,
                        e
                    );
                    metrics.inc(&metrics.follower_pulls_failed);
                }
            }
        }
    }

    async fn catchup_on_promotion(
        &self,
        _prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        _position: u64,
    ) -> Result<()> {
        if self.manifest_store.is_some() {
            let _ = self.poll_manifest_store(db_name, db_path).await?;
            return Ok(());
        }

        let vfs_clone = self.vfs.clone();
        let result =
            tokio::task::spawn_blocking(move || vfs_clone.fetch_and_apply_remote_manifest())
                .await
                .map_err(|e| anyhow!("manifest fetch task panicked: {}", e))?;

        match result {
            Ok(Some(_version)) => Ok(()),
            Ok(None) => Ok(()),
            Err(e) => Err(anyhow!("manifest fetch failed: {}", e)),
        }
    }
}
