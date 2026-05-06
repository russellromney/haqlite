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

#[derive(Debug)]
pub(crate) struct PreparedPageReplay {
    current_seq: u64,
    changesets: Vec<(u64, physical::PhysicalChangeset)>,
    target_page_count: Option<u64>,
    base_file_checksum_required: bool,
}

impl PreparedPageReplay {
    pub(crate) fn validate_base_checksum(&self, expected_prev_checksum: u64) -> Result<()> {
        if !self.base_file_checksum_required {
            return Ok(());
        }
        let Some((seq, changeset)) = self.changesets.first() else {
            return Ok(());
        };
        if changeset.header.prev_checksum != expected_prev_checksum {
            return Err(anyhow!(
                "first changeset checksum chain break at seq {}: expected base {:016x}, found {:016x}",
                seq,
                expected_prev_checksum,
                changeset.header.prev_checksum
            ));
        }
        Ok(())
    }
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
    let mut expected_seq = current_seq + 1;
    let previous_key = (current_seq > 0).then(|| {
        cs_storage::format_key(
            prefix,
            db_name,
            cs_storage::GENERATION_INCREMENTAL,
            current_seq,
            ChangesetKind::Physical,
        )
    });
    let mut expected_prev_checksum: Option<u64> = match previous_key {
        Some(key) => match storage.get(&key).await? {
            Some(data) => Some(
                physical::decode(&data)
                    .map_err(|e| anyhow!("failed to decode previous changeset at {}: {}", key, e))?
                    .checksum,
            ),
            None => None,
        },
        None => None,
    };
    let base_file_checksum_required = expected_prev_checksum.is_none();
    let mut target_page_count = None;
    for file in files {
        let data = storage
            .get(&file.key)
            .await?
            .ok_or_else(|| anyhow!("changeset disappeared while preparing replay: {}", file.key))?;
        let changeset = physical::decode(&data)
            .map_err(|e| anyhow!("failed to decode changeset at {}: {}", file.key, e))?;
        if file.seq != expected_seq {
            return Err(anyhow!(
                "non-contiguous changeset sequence for {}: expected {}, found {} at {}",
                db_name,
                expected_seq,
                file.seq,
                file.key
            ));
        }
        if changeset.header.seq != file.seq {
            return Err(anyhow!(
                "changeset header seq mismatch at {}: key seq {}, header seq {}",
                file.key,
                file.seq,
                changeset.header.seq
            ));
        }
        if let Some(expected_prev) = expected_prev_checksum {
            if changeset.header.prev_checksum != expected_prev {
                return Err(anyhow!(
                    "changeset checksum chain break at {}: expected prev {:016x}, found {:016x}",
                    file.key,
                    expected_prev,
                    changeset.header.prev_checksum
                ));
            }
        }
        for page in &changeset.pages {
            if page.page_id.to_u64() == 1 {
                if let Some(page_count) = sqlite_header_page_count(&page.data) {
                    target_page_count = Some(page_count);
                }
            }
        }
        expected_prev_checksum = Some(changeset.checksum);
        expected_seq += 1;
        changesets.push((file.seq, changeset));
    }
    Ok(PreparedPageReplay {
        current_seq,
        changesets,
        target_page_count,
        base_file_checksum_required,
    })
}

pub(crate) fn apply_prepared_page_replay(
    sink: &mut HaqliteTurboliteReplaySink,
    prepared: PreparedPageReplay,
) -> Result<u64> {
    if let Err(begin_err) = sink.begin() {
        try_abort(sink, &begin_err);
        return Err(begin_err);
    }

    let mut applied_seq = prepared.current_seq;
    let result = (|| {
        if let Some(page_count) = prepared.target_page_count {
            sink.set_target_page_count(page_count)?;
        }
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

fn sqlite_header_page_count(data: &[u8]) -> Option<u64> {
    if data.len() < 32 || data.get(0..16) != Some(b"SQLite format 3\0") {
        return None;
    }
    let page_count = u32::from_be_bytes([data[28], data[29], data[30], data[31]]);
    (page_count > 0).then_some(page_count as u64)
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

    pub(crate) fn set_target_page_count(&mut self, page_count: u64) -> Result<()> {
        let handle = self.handle.as_mut().ok_or_else(|| {
            anyhow!("HaqliteTurboliteReplaySink: set_target_page_count called after consume")
        })?;
        handle.set_target_page_count(page_count).map_err(|e| {
            anyhow!(
                "turbolite ReplayHandle::set_target_page_count failed: {}",
                e
            )
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hadb_storage::CasResult;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use walrust::hadb_changeset::physical::{PageEntry, PageId, PageIdSize, PhysicalChangeset};

    #[derive(Clone, Default)]
    struct MemoryStorage {
        objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    }

    impl MemoryStorage {
        fn put_sync(&self, key: String, data: Vec<u8>) {
            self.objects
                .lock()
                .expect("memory storage lock")
                .insert(key, data);
        }
    }

    #[async_trait]
    impl StorageBackend for MemoryStorage {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self
                .objects
                .lock()
                .expect("memory storage lock")
                .get(key)
                .cloned())
        }

        async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.put_sync(key.to_string(), data.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.objects
                .lock()
                .expect("memory storage lock")
                .remove(key);
            Ok(())
        }

        async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
            let after = after.unwrap_or("");
            Ok(self
                .objects
                .lock()
                .expect("memory storage lock")
                .keys()
                .filter(|key| key.starts_with(prefix) && key.as_str() > after)
                .cloned()
                .collect())
        }

        async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
            let mut objects = self.objects.lock().expect("memory storage lock");
            if objects.contains_key(key) {
                return Ok(CasResult {
                    success: false,
                    etag: None,
                });
            }
            objects.insert(key.to_string(), data.to_vec());
            Ok(CasResult {
                success: true,
                etag: Some("1".to_string()),
            })
        }

        async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
            self.put(key, data).await?;
            Ok(CasResult {
                success: true,
                etag: Some("1".to_string()),
            })
        }
    }

    fn page(page_id: u32, fill: u8) -> Vec<u8> {
        let mut data = vec![fill; 128];
        if page_id == 1 {
            data[..16].copy_from_slice(b"SQLite format 3\0");
            data[28..32].copy_from_slice(&7u32.to_be_bytes());
        }
        data
    }

    fn changeset(seq: u64, prev_checksum: u64, page_id: u32, fill: u8) -> PhysicalChangeset {
        PhysicalChangeset::new(
            seq,
            prev_checksum,
            PageIdSize::U32,
            128,
            vec![PageEntry {
                page_id: PageId::U32(page_id),
                data: page(page_id, fill),
            }],
        )
    }

    fn put_changeset(storage: &MemoryStorage, key_seq: u64, changeset: &PhysicalChangeset) {
        let key = cs_storage::format_key(
            "prefix/",
            "db",
            cs_storage::GENERATION_INCREMENTAL,
            key_seq,
            ChangesetKind::Physical,
        );
        storage.put_sync(key, physical::encode(changeset));
    }

    #[tokio::test]
    async fn prepare_page_replay_rejects_changeset_gap() {
        let storage = MemoryStorage::default();
        let c2 = changeset(2, 0, 1, 0x22);
        put_changeset(&storage, 2, &c2);

        let err = prepare_page_replay(&storage, "prefix/", "db", 0)
            .await
            .expect_err("missing seq 1 must fail closed");

        assert!(err
            .to_string()
            .contains("non-contiguous changeset sequence"));
    }

    #[tokio::test]
    async fn prepare_page_replay_rejects_key_header_seq_mismatch() {
        let storage = MemoryStorage::default();
        let c2 = changeset(2, 0, 1, 0x22);
        put_changeset(&storage, 1, &c2);

        let err = prepare_page_replay(&storage, "prefix/", "db", 0)
            .await
            .expect_err("key/header sequence mismatch must fail closed");

        assert!(err.to_string().contains("changeset header seq mismatch"));
    }

    #[tokio::test]
    async fn prepare_page_replay_rejects_internal_checksum_chain_break() {
        let storage = MemoryStorage::default();
        let c1 = changeset(1, 0, 1, 0x11);
        let c2 = changeset(2, c1.checksum.wrapping_add(1), 2, 0x22);
        put_changeset(&storage, 1, &c1);
        put_changeset(&storage, 2, &c2);

        let err = prepare_page_replay(&storage, "prefix/", "db", 0)
            .await
            .expect_err("wrong-chain seq 2 must fail closed");

        assert!(err.to_string().contains("changeset checksum chain break"));
    }

    #[tokio::test]
    async fn prepared_page_replay_validates_base_checksum_and_page_count() {
        let storage = MemoryStorage::default();
        let c1 = changeset(1, 0xAA55, 1, 0x11);
        put_changeset(&storage, 1, &c1);

        let prepared = prepare_page_replay(&storage, "prefix/", "db", 0)
            .await
            .expect("valid replay prepares");

        assert_eq!(prepared.target_page_count, Some(7));
        prepared
            .validate_base_checksum(0xAA55)
            .expect("matching base checksum validates");
        let err = prepared
            .validate_base_checksum(0x55AA)
            .expect_err("wrong base checksum must fail closed");
        assert!(err
            .to_string()
            .contains("first changeset checksum chain break"));
    }

    #[tokio::test]
    async fn prepared_page_replay_uses_previous_changeset_checksum_after_base() {
        let storage = MemoryStorage::default();
        let c3 = changeset(3, 0xAA55, 1, 0x33);
        let c4 = changeset(4, c3.checksum, 2, 0x44);
        put_changeset(&storage, 3, &c3);
        put_changeset(&storage, 4, &c4);

        let prepared = prepare_page_replay(&storage, "prefix/", "db", 3)
            .await
            .expect("valid replay prepares from previous changeset");

        prepared
            .validate_base_checksum(0x55AA)
            .expect("whole-file checksum is not used after a previous changeset exists");
    }

    #[tokio::test]
    async fn prepare_page_replay_rejects_previous_changeset_chain_break() {
        let storage = MemoryStorage::default();
        let c3 = changeset(3, 0xAA55, 1, 0x33);
        let c4 = changeset(4, c3.checksum.wrapping_add(1), 2, 0x44);
        put_changeset(&storage, 3, &c3);
        put_changeset(&storage, 4, &c4);

        let err = prepare_page_replay(&storage, "prefix/", "db", 3)
            .await
            .expect_err("wrong-chain seq 4 must fail closed");

        assert!(err.to_string().contains("changeset checksum chain break"));
    }
}
