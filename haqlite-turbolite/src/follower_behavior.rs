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

use crate::replay_sink::{
    apply_prepared_page_replay, prepare_page_replay, HaqliteTurboliteReplaySink,
};

enum ApplyOutcome {
    Applied(u64),
    Noop,
    TransientRetry(String),
}

pub struct TurboliteFollowerBehavior {
    vfs: turbolite::tiered::SharedTurboliteVfs,
    manifest_store: Option<Arc<dyn ManifestStore>>,
    manifest_key: Option<String>,
    walrust_storage: Option<Arc<dyn StorageBackend>>,
    walrust_prefix: Option<String>,
    wakeup: Option<Arc<tokio::sync::Notify>>,
    replay_base_seq: Option<Arc<AtomicU64>>,
    replay_base_pending_publish: Option<Arc<AtomicBool>>,
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
            replay_base_seq: None,
            replay_base_pending_publish: None,
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

    pub fn with_replay_base_tracking(
        mut self,
        seq: Arc<AtomicU64>,
        pending_publish: Arc<AtomicBool>,
    ) -> Self {
        self.replay_base_seq = Some(seq);
        self.replay_base_pending_publish = Some(pending_publish);
        self
    }

    fn requires_delta_replay(&self) -> bool {
        self.walrust_storage.is_some() || self.walrust_prefix.is_some()
    }

    /// Apply a published base manifest: materialize the Turbolite base,
    /// then, for Continuous mode, list and replay walrust delta pages on top.
    /// The whole sequence runs
    /// under one VFS replay-gate write so VFS-routed reads either
    /// complete before the gate is taken or block at xLock until
    /// apply finishes.
    ///
    /// In Continuous mode the follower position is a delta seq, not a
    /// Turbolite manifest version. A same-base poll can still have new
    /// delta objects, so Continuous never no-ops solely because the base
    /// manifest version is unchanged.
    async fn apply_manifest_payload(
        &self,
        db_name: &str,
        _db_path: &Path,
        payload: &[u8],
        current_version: u64,
    ) -> Result<ApplyOutcome> {
        let decoded_manifest = turbolite::tiered::TurboliteVfs::decode_manifest_bytes(payload)
            .map_err(|e| anyhow!("turbolite decode_manifest_bytes failed: {}", e))?;
        let continuous = self.requires_delta_replay();
        if !continuous && decoded_manifest.version <= current_version {
            return Ok(ApplyOutcome::Noop);
        }

        // A missing page-group during materialize is a transient
        // upload race; surface it as TransientRetry so the next
        // poll retries without advancing position.
        let vfs = self.vfs.clone();
        let cache_path = self.vfs.cache_file_path();
        let gate = self.vfs.replay_gate();
        let payload_owned = payload.to_vec();
        let db_name_owned = db_name.to_string();
        let replay_base_seq = self.replay_base_seq.clone();
        let replay_base_pending_publish = self.replay_base_pending_publish.clone();

        // Materialize before set_manifest_bytes: pre-flighting
        // fetches BEFORE the only-mutation step keeps a missing-group
        // transient from leaving the VFS half-applied.
        //
        // Continuous mode tracks follower position as a walrust seq,
        // not a Turbolite manifest version. Once this process has the
        // base manifest installed, a same-base poll should only pull
        // WAL objects after the caller's current seq; rematerializing
        // the old base every poll creates a base/replay churn window.
        let local_manifest = self.vfs.manifest();
        let materialize_base = decoded_manifest.version > local_manifest.version;
        let replay_start_seq = if continuous {
            if materialize_base {
                decoded_manifest.change_counter
            } else {
                current_version.max(local_manifest.change_counter)
            }
        } else {
            0
        };
        let prepared_replay = if continuous {
            let storage = self
                .walrust_storage
                .as_ref()
                .ok_or_else(|| {
                    anyhow!("continuous follower '{}' missing walrust storage", db_name)
                })?
                .clone();
            let prefix = self
                .walrust_prefix
                .as_ref()
                .ok_or_else(|| anyhow!("continuous follower '{}' missing walrust prefix", db_name))?
                .clone();
            Some(prepare_page_replay(storage.as_ref(), &prefix, db_name, replay_start_seq).await?)
        } else {
            None
        };
        let decoded_manifest_for_materialize = decoded_manifest.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<ApplyOutcome> {
            let _gate = loop {
                if let Some(guard) = gate.try_write() {
                    break guard;
                }
                std::thread::sleep(Duration::from_millis(1));
            };

            if materialize_base {
                match vfs
                    .shared_state()
                    .materialize_manifest_to_file(&decoded_manifest_for_materialize, &cache_path)
                {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(ApplyOutcome::TransientRetry(format!(
                            "missing page-group during materialize for '{}': {}",
                            db_name_owned, e
                        )));
                    }
                    Err(e) => {
                        return Err(anyhow!("turbolite materialize failed: {}", e));
                    }
                }

                vfs.set_manifest_bytes(&payload_owned)
                    .map_err(|e| anyhow!("turbolite set_manifest_bytes failed: {}", e))?;

                let page_count = vfs.manifest().page_count;
                vfs.sync_after_external_restore(page_count);
            }

            if continuous {
                let handle = vfs
                    .begin_replay()
                    .map_err(|e| anyhow!("turbolite begin_replay failed: {}", e))?;
                let mut sink = HaqliteTurboliteReplaySink::new_under_external_write(handle);
                let final_seq = apply_prepared_page_replay(
                    &mut sink,
                    prepared_replay.expect("decoded walrust requires prepared replay"),
                )?;
                tracing::debug!(
                    "Follower '{}': replayed walrust {} -> {} via direct page sink",
                    db_name_owned,
                    replay_start_seq,
                    final_seq,
                );
                if final_seq > replay_start_seq {
                    if let (Some(seq), Some(pending)) =
                        (replay_base_seq, replay_base_pending_publish)
                    {
                        seq.store(final_seq, Ordering::Release);
                        pending.store(true, Ordering::Release);
                    }
                }
                return Ok(ApplyOutcome::Applied(final_seq));
            }

            Ok(ApplyOutcome::Applied(vfs.manifest().version))
        })
        .await
        .map_err(|e| anyhow!("turbolite apply task panicked: {}", e))??;

        Ok(result)
    }

    async fn poll_manifest_store(
        &self,
        db_name: &str,
        db_path: &Path,
        current_version: u64,
    ) -> Result<Option<ApplyOutcome>> {
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
        let outcome = self
            .apply_manifest_payload(db_name, db_path, &manifest.payload, current_version)
            .await?;
        Ok(Some(outcome))
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
                match self
                    .poll_manifest_store(db_name, db_path, current_version)
                    .await
                {
                    Ok(Some(ApplyOutcome::Applied(new_version)))
                        if new_version > current_version =>
                    {
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
                    Ok(Some(ApplyOutcome::TransientRetry(msg))) => {
                        tracing::warn!(
                            "Follower '{}': apply transient ({}); will retry on next poll",
                            db_name,
                            msg
                        );
                        caught_up.store(false, Ordering::SeqCst);
                        metrics.follower_caught_up.store(0, Ordering::Relaxed);
                        metrics.inc(&metrics.follower_pulls_failed);
                    }
                    Ok(Some(ApplyOutcome::Applied(_)))
                    | Ok(Some(ApplyOutcome::Noop))
                    | Ok(None) => {
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
                        caught_up.store(false, Ordering::SeqCst);
                        metrics.follower_caught_up.store(0, Ordering::Relaxed);
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
        position: u64,
    ) -> Result<()> {
        if self.manifest_store.is_some() {
            match self.poll_manifest_store(db_name, db_path, position).await? {
                Some(ApplyOutcome::TransientRetry(msg)) => {
                    return Err(anyhow!(
                        "promotion catch-up for '{}' hit transient apply failure: {}",
                        db_name,
                        msg
                    ));
                }
                Some(ApplyOutcome::Applied(_)) | Some(ApplyOutcome::Noop) | None => return Ok(()),
            }
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
