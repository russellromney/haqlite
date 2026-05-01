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

enum ApplyOutcome {
    Applied(u64),
    TransientRetry(String),
}

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
    /// in-memory manifest, materializes the manifest's pages into
    /// the live cache file, then if the payload carries a walrust
    /// cursor, replays delta pages from `current cursor → latest
    /// uploaded` via `walrust::sync::pull_incremental_into_sink` +
    /// `TurboliteVfs::begin_replay`. No temp SQLite restore file.
    /// `apply_page` writes go into staging on the `ReplayHandle`;
    /// `finalize` (driven by the walrust pull) installs them.
    ///
    /// `current_version` is the follower's last-applied manifest
    /// version. We decode the published manifest's version FIRST
    /// (a pure read, no state mutation) and skip the entire apply
    /// when the manifest is a pure-checkpoint payload at the same
    /// or older version. Without this guard, a routine no-new-data
    /// poll would still rewrite `data.cache` via
    /// `materialize_to_file` and re-run replay, briefly clobbering
    /// pages a follower-read shortcut might be observing right now.
    ///
    /// The skip is gated on `walrust.is_none()` — a continuous
    /// manifest can be republished at the same version while new
    /// walrust frames continue to upload (the manifest's `walrust`
    /// field carries the embed cursor, not the upload head). If we
    /// skipped a same-version continuous manifest after pulling 0
    /// frames on a tight first attempt, we'd never re-pull and the
    /// follower would stall indefinitely on storage that races the
    /// publish. Continuous followers stay on the original
    /// always-apply path; the no-op-skip optimization is a
    /// checkpoint-only concern today.
    async fn apply_manifest_payload(
        &self,
        db_name: &str,
        _db_path: &Path,
        payload: &[u8],
        current_version: u64,
    ) -> Result<u64> {
        // Decode without mutating local state so we can early-exit
        // on a pure-checkpoint no-op poll. The skip is gated on
        // `walrust.is_none()` because a continuous manifest can be
        // republished at the same version while new walrust frames
        // upload (the manifest's walrust field is the embed cursor,
        // not the upload head); skipping a same-version continuous
        // payload after a tight no-frames-yet first attempt would
        // latch the follower stale.
        let (decoded_manifest, decoded_walrust) =
            turbolite::tiered::TurboliteVfs::decode_manifest_bytes(payload)
                .map_err(|e| anyhow!("turbolite decode_manifest_bytes failed: {}", e))?;
        if decoded_walrust.is_none() && decoded_manifest.version <= current_version {
            return Ok(current_version);
        }

        if self.requires_hybrid_payload() && decoded_walrust.is_none() {
            return Err(anyhow!(
                "continuous manifest for '{}' must carry walrust replay cursor",
                db_name
            ));
        }

        // The full apply runs inside one `spawn_blocking` so the
        // VFS replay-gate write lock can be held across:
        //   1. `set_manifest_bytes` — adopts the new manifest in
        //      VFS in-memory state.
        //   2. `materialize_to_file` — rewrites `data.cache` from
        //      the manifest's page-group objects.
        //   3. `sync_after_external_restore` — clears mem cache,
        //      marks all pages present, persists bitmap.
        //   4. `walrust::sync::pull_incremental_into_sink` followed
        //      by `ReplayHandle::finalize_assuming_external_write` —
        //      layers walrust delta pages on top of the materialized
        //      base.
        //
        // Holding the gate over the whole sequence prevents two
        // distinct reader-visible inconsistencies:
        //   - Torn bytes mid-`materialize_to_file` (the file is
        //     truncated then re-written page-by-page).
        //   - Logical tear between materialize and replay (manifest
        //     swap done, base materialized, walrust deltas not yet
        //     applied — VFS reads would see a snapshot the leader
        //     never published).
        //
        // VFS-routed follower reads take the read half on
        // `xLock(SHARED)`, so they either complete before the gate
        // is taken or block at xLock until apply finishes.
        //
        // Replay finalize would normally take the same write lock
        // itself; we use `finalize_assuming_external_write` (via
        // `HaqliteTurboliteReplaySink::new_under_external_write`) to
        // skip the inner take. parking_lot's `RwLock` is not
        // re-entrant, so without that variant the second take would
        // deadlock.
        //
        // walrust pull is async (storage I/O). To run it under the
        // sync write guard we use the captured tokio runtime handle
        // to `block_on` the pull future — same pattern as the
        // sync-API shims documented in the cinch-cloud `CLAUDE.md`
        // ("don't reach for block_on" exceptions). The gate is
        // released the instant the closure returns.
        //
        // A missing page-group object during materialize is a
        // transient race against the leader's publish ordering:
        // the manifest can name a page-group key that the leader's
        // background uploader hasn't pushed yet (or has already
        // re-keyed under a higher version's churn). We surface
        // that as `io::ErrorKind::NotFound` from
        // `materialize_to_file` and translate it here into a
        // no-op-skip return: don't bump the position, don't error
        // the follower; the next poll re-fetches and retries.
        let runtime = tokio::runtime::Handle::current();
        let vfs = self.vfs.clone();
        let cache_path = self.vfs.cache_file_path();
        let gate = self.vfs.replay_gate();
        let payload_owned = payload.to_vec();
        let db_name_owned = db_name.to_string();
        let walrust_storage = self.walrust_storage.clone();
        let walrust_prefix = self.walrust_prefix.clone();

        // Decode the candidate manifest WITHOUT mutating VFS state
        // so we can materialize from it and only commit
        // (`set_manifest_bytes`) once the page-group fetches have
        // all succeeded. Order is critical for missing-group
        // retry safety:
        //   1. Decode (pure read).
        //   2. Materialize from the decoded manifest (preflights
        //      fetches in memory; on NotFound returns BEFORE
        //      truncating `data.cache`).
        //   3. set_manifest_bytes (now the only mutation; commits
        //      the new manifest, evicts changed page groups,
        //      persists local manifest).
        //   4. sync_after_external_restore (clear mem cache, mark
        //      all pages present, persist bitmap).
        //   5. Replay walrust frames if continuous.
        //
        // If step 2 returns NotFound, steps 3–5 don't run. VFS
        // state stays exactly as it was: shared_manifest unchanged,
        // page bitmap unchanged, db_header unchanged, evictions
        // not performed, data.cache untouched. Reads after the
        // gate releases see the previous (still-valid) snapshot;
        // the next manifest poll retries.
        let decoded_manifest_for_materialize = decoded_manifest.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<ApplyOutcome> {
            let _gate = gate.write();

            // Materialize the decoded manifest WITHOUT yet committing
            // it to the VFS. Pre-flight semantics: on missing-group
            // / fetch failure, returns before File::create touches
            // `data.cache`.
            match vfs
                .shared_state()
                .materialize_manifest_to_file(&decoded_manifest_for_materialize, &cache_path)
            {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Transient: leader publish raced the page-group
                    // upload. VFS state unchanged because we haven't
                    // called set_manifest_bytes yet. Caller skips
                    // advancing position so the next poll retries.
                    return Ok(ApplyOutcome::TransientRetry(format!(
                        "missing page-group during materialize for '{}': {}",
                        db_name_owned, e
                    )));
                }
                Err(e) => {
                    return Err(anyhow!("turbolite materialize failed: {}", e));
                }
            }

            // Materialize succeeded. Now safe to commit the new
            // manifest to live VFS state.
            let walrust = vfs
                .set_manifest_bytes(&payload_owned)
                .map_err(|e| anyhow!("turbolite set_manifest_bytes failed: {}", e))?;

            let page_count = vfs.manifest().page_count;
            vfs.sync_after_external_restore(page_count);

            if let Some((walrust_seq, _changeset_prefix)) = walrust {
                let storage = walrust_storage.ok_or_else(|| {
                    anyhow!(
                        "continuous follower '{}' missing walrust storage",
                        db_name_owned
                    )
                })?;
                let prefix = walrust_prefix.ok_or_else(|| {
                    anyhow!(
                        "continuous follower '{}' missing walrust prefix",
                        db_name_owned
                    )
                })?;

                let handle = vfs
                    .begin_replay()
                    .map_err(|e| anyhow!("turbolite begin_replay failed: {}", e))?;
                let mut sink = HaqliteTurboliteReplaySink::new_under_external_write(handle);
                let final_seq = runtime
                    .block_on(walrust::sync::pull_incremental_into_sink(
                        storage.as_ref(),
                        &prefix,
                        &db_name_owned,
                        &mut sink,
                        walrust_seq,
                    ))?;
                tracing::debug!(
                    "Follower '{}': replayed walrust {} -> {} via direct page sink",
                    db_name_owned,
                    walrust_seq,
                    final_seq,
                );
            }

            Ok(ApplyOutcome::Applied(vfs.manifest().version))
        })
        .await
        .map_err(|e| anyhow!("turbolite apply task panicked: {}", e))??;

        match result {
            ApplyOutcome::Applied(v) => Ok(v),
            ApplyOutcome::TransientRetry(msg) => {
                tracing::warn!("Follower '{}': apply transient ({}); will retry on next poll", db_name, msg);
                Ok(current_version)
            }
        }
    }

    async fn poll_manifest_store(
        &self,
        db_name: &str,
        db_path: &Path,
        current_version: u64,
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
            .apply_manifest_payload(db_name, db_path, &manifest.payload, current_version)
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
                match self
                    .poll_manifest_store(db_name, db_path, current_version)
                    .await
                {
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
        position: u64,
    ) -> Result<()> {
        if self.manifest_store.is_some() {
            let _ = self.poll_manifest_store(db_name, db_path, position).await?;
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
