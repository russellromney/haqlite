//! `walrust::PageReplaySink` adapter for a turbolite `ReplayHandle`.

use anyhow::{anyhow, Result};
use hadb_storage::StorageBackend;
use turbolite::tiered::{FinalizeReport, ReplayHandle};
use walrust::hadb_changeset::storage::ChangesetKind;
use walrust::hadb_changeset::{physical, storage as cs_storage};
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

pub(crate) struct PreparedPageReplay {
    current_seq: u64,
    changesets: Vec<(u64, physical::PhysicalChangeset)>,
}

pub(crate) async fn prepare_page_replay(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    current_seq: u64,
) -> Result<PreparedPageReplay> {
    let files = cs_storage::discover_after(
        storage,
        prefix,
        db_name,
        current_seq,
        ChangesetKind::Physical,
    )
    .await?;
    let mut changesets = Vec::with_capacity(files.len());
    for file in files {
        let data = storage
            .get(&file.key)
            .await?
            .ok_or_else(|| anyhow!("changeset disappeared while preparing replay: {}", file.key))?;
        let changeset = physical::decode(&data)
            .map_err(|e| anyhow!("failed to decode changeset at {}: {}", file.key, e))?;
        changesets.push((file.seq, changeset));
    }
    Ok(PreparedPageReplay {
        current_seq,
        changesets,
    })
}

pub(crate) fn apply_prepared_page_replay(
    sink: &mut dyn PageReplaySink,
    prepared: PreparedPageReplay,
) -> Result<u64> {
    if let Err(begin_err) = sink.begin() {
        try_abort(sink, &begin_err);
        return Err(begin_err);
    }

    let mut applied_seq = prepared.current_seq;
    let result = (|| {
        for (seq, changeset) in prepared.changesets {
            for page in &changeset.pages {
                let sqlite_page_id: u32 = page
                    .page_id
                    .to_u64()
                    .try_into()
                    .map_err(|_| anyhow!("page_id {} exceeds u32", page.page_id.to_u64()))?;
                sink.apply_page(sqlite_page_id, &page.data)?;
            }
            sink.commit_changeset(seq)?;
            applied_seq = seq;
        }
        Ok(applied_seq)
    })();

    match result {
        Ok(seq) => {
            if let Err(finalize_err) = sink.finalize() {
                try_abort(sink, &finalize_err);
                return Err(finalize_err);
            }
            Ok(seq)
        }
        Err(e) => {
            try_abort(sink, &e);
            Err(e)
        }
    }
}

fn try_abort(sink: &mut dyn PageReplaySink, primary: &anyhow::Error) {
    if let Err(abort_err) = sink.abort() {
        tracing::error!(
            "PageReplaySink::abort failed after primary error '{}': {}",
            primary,
            abort_err
        );
    }
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
            return Err(anyhow!(
                "HaqliteTurboliteReplaySink: begin called after consume"
            ));
        }
        Ok(())
    }

    fn apply_page(&mut self, sqlite_page_id: u32, data: &[u8]) -> Result<()> {
        let handle = self.handle.as_mut().ok_or_else(|| {
            anyhow!("HaqliteTurboliteReplaySink: apply_page called after consume")
        })?;
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
