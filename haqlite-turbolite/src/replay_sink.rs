//! `walrust_core::PageReplaySink` adapter that drives a Turbolite
//! `ReplayHandle` directly.
//!
//! Routes decoded HADBP physical pages from
//! `walrust::sync::pull_incremental_into_sink` into Turbolite's
//! tiered cache without staging through a temporary SQLite file.
//! `pull_incremental_into_sink` calls `begin` once, then per
//! discovered changeset calls `apply_page` for each page and
//! `commit_changeset(seq)`, then exactly one of `finalize` or
//! `abort` per invocation. The adapter owns the `ReplayHandle` in
//! an `Option` so the consume-by-value `finalize` and `abort`
//! methods on the handle can be reached from inside the trait's
//! `&mut self` methods.

use anyhow::{anyhow, Result};
use turbolite::tiered::{FinalizeReport, ReplayHandle};
use walrust::PageReplaySink;

/// Captured outcome of the most recent `finalize`. Used by the
/// promotion path to confirm an install actually happened (test
/// hooks, telemetry); production publish reads accumulated VFS
/// state directly via `TurboliteVfs::publish_replayed_base`.
#[derive(Default)]
pub(crate) struct FinalizeOutcome {
    pub last_finalize: Option<FinalizeReport>,
}

/// All callers (`apply_manifest_payload`, `restore_from_manifest`)
/// run inside a `spawn_blocking` that already holds
/// `TurboliteVfs::replay_gate().write()` around the full
/// set_manifest_bytes + materialize + sync + replay window. Replay
/// finalize therefore goes through
/// `ReplayHandle::finalize_assuming_external_write` so it doesn't
/// recursively re-take the gate (parking_lot's `RwLock` is not
/// re-entrant; a second take would deadlock).
pub(crate) struct HaqliteTurboliteReplaySink {
    handle: Option<ReplayHandle>,
    outcome: FinalizeOutcome,
}

impl HaqliteTurboliteReplaySink {
    pub(crate) fn new_under_external_write(handle: ReplayHandle) -> Self {
        Self {
            handle: Some(handle),
            outcome: FinalizeOutcome::default(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_outcome(self) -> FinalizeOutcome {
        self.outcome
    }
}

impl PageReplaySink for HaqliteTurboliteReplaySink {
    fn begin(&mut self) -> Result<()> {
        // Handle is constructed via `vfs.begin_replay()`, which
        // already armed the cycle. Nothing to do per-cycle here;
        // the trait's `begin` exists for sinks that allocate state
        // lazily.
        if self.handle.is_none() {
            return Err(anyhow!("HaqliteTurboliteReplaySink: begin called after consume"));
        }
        Ok(())
    }

    fn apply_page(&mut self, sqlite_page_id: u32, data: &[u8]) -> Result<()> {
        let handle = self
            .handle
            .as_mut()
            .ok_or_else(|| anyhow!("HaqliteTurboliteReplaySink: apply_page called after consume"))?;
        handle
            .apply_page(sqlite_page_id, data)
            .map_err(|e| anyhow!("turbolite ReplayHandle::apply_page failed: {}", e))
    }

    fn commit_changeset(&mut self, seq: u64) -> Result<()> {
        let handle = self.handle.as_mut().ok_or_else(|| {
            anyhow!("HaqliteTurboliteReplaySink: commit_changeset called after consume")
        })?;
        handle
            .commit_changeset(seq)
            .map_err(|e| anyhow!("turbolite ReplayHandle::commit_changeset failed: {}", e))
    }

    fn finalize(&mut self) -> Result<()> {
        let handle = self
            .handle
            .take()
            .ok_or_else(|| anyhow!("HaqliteTurboliteReplaySink: finalize called twice"))?;
        let report = handle
            .finalize_assuming_external_write()
            .map_err(|e| anyhow!("turbolite ReplayHandle::finalize failed: {}", e))?;
        self.outcome.last_finalize = Some(report);
        Ok(())
    }

    fn abort(&mut self) -> Result<()> {
        // walrust's driver calls abort on lifecycle failure (including
        // a failed `begin`/`finalize`). Tolerate the post-consume
        // case (finalize already took the handle and we're now being
        // asked to abort because finalize itself returned Err).
        if let Some(handle) = self.handle.take() {
            handle
                .abort()
                .map_err(|e| anyhow!("turbolite ReplayHandle::abort failed: {}", e))?;
        }
        Ok(())
    }
}
