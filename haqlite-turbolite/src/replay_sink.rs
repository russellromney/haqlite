//! `walrust::PageReplaySink` adapter for a turbolite `ReplayHandle`.

use anyhow::{anyhow, Result};
use turbolite::tiered::{FinalizeReport, ReplayHandle};
use walrust::PageReplaySink;

#[derive(Default)]
pub(crate) struct FinalizeOutcome {
    pub last_finalize: Option<FinalizeReport>,
}

/// All callers run under a held VFS replay-gate write, so finalize
/// goes through `finalize_assuming_external_write` to avoid a
/// reentrant take on parking_lot's RwLock.
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
        // Tolerate post-consume aborts: walrust's driver calls
        // abort() if finalize() returned Err.
        if let Some(handle) = self.handle.take() {
            handle
                .abort()
                .map_err(|e| anyhow!("turbolite ReplayHandle::abort failed: {}", e))?;
        }
        Ok(())
    }
}
