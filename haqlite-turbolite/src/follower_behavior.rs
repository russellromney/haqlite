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

use crate::phase4_chain::FollowerCursor;
use crate::replay_sink::{
    apply_prepared_page_replay, prepare_page_replay, prepare_phase4_replay,
    HaqliteTurboliteReplaySink,
};

enum ApplyOutcome {
    /// Applied a prefix and advanced to `position`. `caught_up` is true only
    /// when the applied seq reached the discovered chain head; a verified-prefix
    /// apply that stopped at a chain break (gap/fork) is NOT caught up and must
    /// not advertise fresh reads (F3).
    Applied {
        position: u64,
        caught_up: bool,
    },
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
    replay_base_pending_publish: Option<Arc<AtomicBool>>,
    replay_base_seq: Option<Arc<AtomicU64>>,
    /// Phase 004 leadership-term epoch, shared with the replicator.
    /// Reset to 0 on promotion so the replicator re-latches the new
    /// term's lease revision from the fence on its next publish.
    term_epoch: Option<Arc<AtomicU64>>,
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
            replay_base_pending_publish: None,
            replay_base_seq: None,
            term_epoch: None,
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
        pending_publish: Arc<AtomicBool>,
        replay_seq: Arc<AtomicU64>,
    ) -> Self {
        self.replay_base_pending_publish = Some(pending_publish);
        self.replay_base_seq = Some(replay_seq);
        self
    }

    /// Share the phase-004 term-epoch cell with the replicator. Reset
    /// to 0 on promotion so the replicator re-latches the new term.
    pub fn with_term_epoch(mut self, term_epoch: Arc<AtomicU64>) -> Self {
        self.term_epoch = Some(term_epoch);
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

        // Phase 004: when the base manifest carries a populated cursor
        // anchor, the writer is on the fenced TLM_DELTA contract — use
        // the phase-4 follower path. An empty anchor means a pre-cursor
        // base (phase-3 publisher), so fall back to the .hadbp path.
        // This gate keeps the shipped phase-3 failover behavior intact
        // until the writer starts populating the cursor (step 7).
        if continuous && !decoded_manifest.cursor.base_object_checksum.is_empty() {
            return self
                .apply_manifest_payload_phase4(db_name, decoded_manifest, payload)
                .await;
        }

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
        let replay_base_pending_publish = self.replay_base_pending_publish.clone();
        let replay_base_seq = self.replay_base_seq.clone();

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
        // Continuous readers use `change_counter` as the durable delta
        // replay floor. A same-version base with a higher cursor still
        // covers more committed deltas, so adopt it before listing WAL
        // objects or the follower can chase an already-checkpointed seq.
        let materialize_base = decoded_manifest.version > local_manifest.version
            || (continuous && decoded_manifest.change_counter > local_manifest.change_counter);
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
                let base_checksum = walrust::ltx::compute_checksum_from_file(&cache_path)
                    .map_err(|e| anyhow!("walrust base checksum failed: {}", e))?;
                prepared_replay
                    .as_ref()
                    .expect("decoded walrust requires prepared replay")
                    .validate_base_checksum(base_checksum)?;
                let handle = vfs
                    .begin_replay_after(replay_start_seq)
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
                    if let Some(pending) = replay_base_pending_publish {
                        pending.store(true, Ordering::Release);
                    }
                    if let Some(seq) = replay_base_seq {
                        seq.fetch_max(final_seq, Ordering::AcqRel);
                    }
                }
                // prepare_page_replay applies every discovered contiguous
                // changeset up to the listed head and fails closed on a gap or
                // chain break, so a successful apply means we reached the head.
                return Ok(ApplyOutcome::Applied {
                    position: final_seq,
                    caught_up: true,
                });
            }

            Ok(ApplyOutcome::Applied {
                position: vfs.manifest().version,
                caught_up: true,
            })
        })
        .await
        .map_err(|e| anyhow!("turbolite apply task panicked: {}", e))??;

        Ok(result)
    }

    /// Phase 004 follower apply path (fenced TLM_DELTA contract).
    ///
    /// Working-cursor model: the follower's replay position lives in the
    /// turbolite VFS local manifest's `cursor` field and **advances** as
    /// deltas are applied. We adopt the published base's cursor (and
    /// re-materialize the base) only when the base advances the floor
    /// beyond our working position — a higher epoch (promotion), a
    /// higher folded-in seq (new checkpoint), or a higher manifest
    /// version. Otherwise we keep advancing the local working cursor and
    /// list deltas after it, so a same-base poll picks up new deltas
    /// without re-materializing.
    async fn apply_manifest_payload_phase4(
        &self,
        db_name: &str,
        decoded_manifest: turbolite::tiered::Manifest,
        payload: &[u8],
    ) -> Result<ApplyOutcome> {
        let storage = self
            .walrust_storage
            .as_ref()
            .ok_or_else(|| anyhow!("phase4 follower '{}' missing walrust storage", db_name))?
            .clone();
        let prefix = self
            .walrust_prefix
            .as_ref()
            .ok_or_else(|| anyhow!("phase4 follower '{}' missing walrust prefix", db_name))?
            .clone();

        let base_cursor = decoded_manifest.cursor.clone();
        let base_writer_id = decoded_manifest.writer_id.clone();
        let local = self.vfs.manifest();
        let local_cursor = local.cursor.clone();

        // Adopt the published base (re-materialize) only when it moves the
        // floor past our working position. A higher manifest version alone
        // must NOT trigger adoption if it would rewind the applied-seq floor
        // backward (re-materializing an older image and re-applying deltas
        // already folded in); guard the version clause on a non-decreasing
        // seq. Same-epoch republish keeps the same writer_id by the
        // strictly-monotonic-epoch lease invariant.
        let adopt_base = base_cursor.epoch > local_cursor.epoch
            || base_cursor.last_applied_seq > local_cursor.last_applied_seq
            || (decoded_manifest.version > local.version
                && base_cursor.last_applied_seq >= local_cursor.last_applied_seq);
        let working = if adopt_base {
            base_cursor.clone()
        } else {
            local_cursor.clone()
        };
        let materialize_base = adopt_base;

        let follower_cursor = FollowerCursor {
            last_applied_seq: working.last_applied_seq,
            base_object_checksum: working.base_object_checksum.clone(),
            epoch: working.epoch,
            writer_id: base_writer_id,
        };
        let replay_start_seq = working.last_applied_seq;
        let working_epoch = working.epoch;

        // Storage I/O (list + fetch + verify) before the blocking apply.
        let prepared = prepare_phase4_replay(storage.as_ref(), &prefix, db_name, &follower_cursor)
            .await
            .map_err(|e| anyhow!("phase4 prepare replay for '{}': {}", db_name, e))?;

        // A chain break is not an error — the verified prefix still
        // applies — but a gap/fork means the follower has NOT fully
        // caught up this poll. Surface it so ops can see a stuck chain.
        let break_reason = prepared.break_reason.clone();
        let chain_ok = matches!(break_reason, crate::phase4_chain::ChainBreak::Ok);
        if !chain_ok {
            tracing::warn!(
                "Follower '{}': phase4 chain stopped early at {:?}; applying verified prefix and retrying next poll",
                db_name,
                break_reason,
            );
        }

        let vfs = self.vfs.clone();
        let cache_path = self.vfs.cache_file_path();
        let gate = self.vfs.replay_gate();
        let payload_owned = payload.to_vec();
        let db_name_owned = db_name.to_string();
        let replay_base_pending_publish = self.replay_base_pending_publish.clone();
        let replay_base_seq = self.replay_base_seq.clone();
        let decoded_for_materialize = decoded_manifest.clone();

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
                    .materialize_manifest_to_file(&decoded_for_materialize, &cache_path)
                {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(ApplyOutcome::TransientRetry(format!(
                            "missing page-group during phase4 materialize for '{}': {}",
                            db_name_owned, e
                        )));
                    }
                    Err(e) => return Err(anyhow!("turbolite phase4 materialize failed: {}", e)),
                }
                vfs.set_manifest_bytes(&payload_owned)
                    .map_err(|e| anyhow!("turbolite phase4 set_manifest_bytes failed: {}", e))?;
                let page_count = vfs.manifest().page_count;
                vfs.sync_after_external_restore(page_count);
            }

            let new_anchor = prepared.new_anchor.clone();
            let new_last_applied_seq = prepared.new_last_applied_seq;

            let handle = vfs
                .begin_replay_after(replay_start_seq)
                .map_err(|e| anyhow!("turbolite phase4 begin_replay failed: {}", e))?;
            let mut sink = HaqliteTurboliteReplaySink::new_under_external_write(handle);
            let final_seq = apply_prepared_page_replay(&mut sink, prepared.prepared)?;

            // Advance the persisted working cursor to the last applied
            // delta's envelope checksum (or leave it unchanged if nothing
            // applied). This is the load-bearing FollowerCursor invariant.
            if new_last_applied_seq > replay_start_seq {
                vfs.update_replay_cursor(turbolite::tiered::ReplayCursor {
                    last_applied_seq: new_last_applied_seq,
                    base_object_checksum: new_anchor,
                    epoch: working_epoch,
                })
                .map_err(|e| anyhow!("turbolite phase4 update_replay_cursor failed: {}", e))?;

                if let Some(pending) = replay_base_pending_publish {
                    pending.store(true, Ordering::Release);
                }
                if let Some(seq) = replay_base_seq {
                    seq.fetch_max(new_last_applied_seq, Ordering::AcqRel);
                }
            }

            tracing::debug!(
                "Follower '{}': phase4 replayed {} -> {} (epoch {})",
                db_name_owned,
                replay_start_seq,
                final_seq,
                working_epoch,
            );
            // Caught up only if the chain verified to the head this poll. A
            // gap/fork applied only a verified prefix, so more is expected and
            // we must not advertise caught-up off a short prefix (F3).
            Ok(ApplyOutcome::Applied {
                position: new_last_applied_seq,
                caught_up: chain_ok,
            })
        })
        .await
        .map_err(|e| anyhow!("turbolite phase4 apply task panicked: {}", e))??;

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
                    Ok(Some(ApplyOutcome::Applied {
                        position: new_version,
                        caught_up: is_caught_up,
                    })) if new_version > current_version => {
                        tracing::debug!(
                            "Follower '{}': manifest v{} -> v{} (caught_up={})",
                            db_name,
                            current_version,
                            new_version,
                            is_caught_up,
                        );
                        position.store(new_version, Ordering::SeqCst);
                        metrics
                            .follower_replay_position
                            .store(new_version, Ordering::Relaxed);
                        metrics.inc(&metrics.follower_pulls_succeeded);
                        // Only advertise caught-up when the applied seq reached
                        // the chain head. A verified-prefix apply that stopped
                        // at a gap/fork advanced position but is NOT caught up
                        // and must not serve fresh reads (F3).
                        caught_up.store(is_caught_up, Ordering::SeqCst);
                        metrics
                            .follower_caught_up
                            .store(u64::from(is_caught_up), Ordering::Relaxed);
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
                    // Same-position apply: did the poll reach the head? A
                    // not-caught-up apply at the same position means the chain
                    // is still broken/short — keep caught_up false (F3).
                    Ok(Some(ApplyOutcome::Applied {
                        caught_up: false, ..
                    })) => {
                        caught_up.store(false, Ordering::SeqCst);
                        metrics.follower_caught_up.store(0, Ordering::Relaxed);
                        metrics.inc(&metrics.follower_pulls_no_new_data);
                    }
                    Ok(Some(ApplyOutcome::Applied { .. }))
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
        // Phase 004: a new leadership term begins. Reset the shared
        // term epoch so the replicator re-latches the new (higher)
        // lease revision from the fence on its next publish — fencing
        // a base published at the prior epoch.
        if let Some(te) = &self.term_epoch {
            te.store(0, Ordering::Release);
        }
        if self.manifest_store.is_some() {
            match self.poll_manifest_store(db_name, db_path, position).await? {
                Some(ApplyOutcome::TransientRetry(msg)) => {
                    return Err(anyhow!(
                        "promotion catch-up for '{}' hit transient apply failure: {}",
                        db_name,
                        msg
                    ));
                }
                Some(ApplyOutcome::Applied { .. }) | Some(ApplyOutcome::Noop) | None => {
                    return Ok(())
                }
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
