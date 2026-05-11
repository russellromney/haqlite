use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;

use hadb::Replicator;
use hadb_storage::StorageBackend;
use turbodb::ManifestStore;
use turbolite::tiered::SharedTurboliteVfs;

use crate::replay_sink::{
    apply_prepared_page_replay, prepare_page_replay, HaqliteTurboliteReplaySink,
};

fn manifest_key(prefix: &str, db_name: &str) -> String {
    format!("{}{}/_manifest", prefix, db_name)
}

/// Create a minimal valid SQLite file at `path` so
/// `vfs.import_sqlite_file` has a real on-disk header to read for a
/// fresh-tenant bootstrap. Idempotent — does nothing if `path` already
/// exists.
///
/// Why this is necessary: turbolite-backed connections write through
/// the VFS, not the literal OS path. For a brand-new tenant whose
/// connection_opener hasn't yet triggered an xSync that publishes a
/// remote manifest (empty schema with no commit, slow HTTP storage,
/// etc.), `ensure_base_manifest`'s import branch hits ENOENT on the
/// nonexistent local file. Seeding a tiny SQLite here lets the
/// import succeed; turbolite reads the empty db, builds an empty
/// manifest at version 1, and the writer takes over from there.
///
/// `PRAGMA user_version = 1; PRAGMA user_version = 0;` is the cheapest
/// way to bump SQLite's file change counter past zero, which
/// `import::import_sqlite_file` asserts is non-zero.
///
/// Public so callers and tests can exercise the seed step directly.
pub fn seed_local_sqlite_for_import(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = rusqlite::Connection::open(path).map_err(io_err)?;
    conn.execute_batch("PRAGMA user_version = 1; PRAGMA user_version = 0;")
        .map_err(io_err)?;
    drop(conn);
    Ok(())
}

fn io_err<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
}

fn is_not_found_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|e| e.kind() == std::io::ErrorKind::NotFound)
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod seed_tests {
    use super::*;
    use tempfile::TempDir;

    /// `seed_local_sqlite_for_import` creates a real, valid SQLite file
    /// at `path` even when the parent directory doesn't exist yet — the
    /// engine's `./data/.tl_cache_<id>` parent is created by the
    /// haqlite-turbolite Builder, but `db_path = ./data/<id>.db`
    /// itself has no creator unless we make one here.
    #[test]
    fn seed_creates_file_and_parent_dir() {
        let tmp = TempDir::new().expect("tmp");
        let nested = tmp.path().join("never").join("existed");
        let path = nested.join("seed.db");
        assert!(!nested.exists());
        assert!(!path.exists());

        seed_local_sqlite_for_import(&path).expect("seed");

        assert!(nested.exists(), "parent dir must be created");
        assert!(path.exists(), "seed file must exist after call");
    }

    /// The seeded file must be a valid SQLite file with file change
    /// counter > 0 — that's the precondition `import_sqlite_file`
    /// asserts. SQLite encodes the change counter at offset 24..28
    /// (big-endian). PRAGMA user_version=1 then =0 commits twice,
    /// bumping the counter to 2.
    #[test]
    fn seed_bumps_file_change_counter_past_zero() {
        use std::io::Read;
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("seed.db");
        seed_local_sqlite_for_import(&path).expect("seed");

        let mut header = [0u8; 100];
        let mut f = std::fs::File::open(&path).expect("open");
        f.read_exact(&mut header).expect("read header");
        let counter = u32::from_be_bytes([header[24], header[25], header[26], header[27]]);
        assert!(
            counter > 0,
            "file change counter must be > 0 after seed (import_sqlite_file asserts > 0); got {}",
            counter
        );
    }

    /// Idempotent: re-seeding an existing file is a no-op (doesn't
    /// truncate, doesn't error). Important because ensure_base_manifest
    /// may be called more than once over the lifetime of a tenant.
    #[test]
    fn seed_is_idempotent() {
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("seed.db");
        seed_local_sqlite_for_import(&path).expect("first seed");

        // Write a sentinel byte right after the SQLite header so we
        // can detect if the second seed truncated.
        {
            use std::io::Seek;
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open rw");
            f.seek(std::io::SeekFrom::End(0)).expect("seek end");
            f.write_all(b"sentinel").expect("write sentinel");
        }
        let len_before = std::fs::metadata(&path).expect("meta").len();

        seed_local_sqlite_for_import(&path).expect("second seed (idempotent)");
        let len_after = std::fs::metadata(&path).expect("meta").len();
        assert_eq!(
            len_before, len_after,
            "seed must not truncate or rewrite an existing file"
        );
    }

    /// The seeded file must round-trip through `import_sqlite_file`
    /// — that's the call site that prompted the seed in the first
    /// place. We don't run the full turbolite import here (it needs a
    /// backend), but we do verify SQLite can re-open the file and
    /// answer a simple query, which is the SQLite-level precondition
    /// import depends on.
    #[test]
    fn seeded_file_is_a_valid_sqlite_database() {
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("seed.db");
        seed_local_sqlite_for_import(&path).expect("seed");

        let conn = rusqlite::Connection::open(&path).expect("reopen seeded db");
        let user_version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("query user_version");
        assert_eq!(user_version, 0);
    }
}

fn should_publish_manifest(vfs: &SharedTurboliteVfs) -> bool {
    let manifest = vfs.manifest();
    manifest.version > 0 || manifest.page_count > 0 || !manifest.page_group_keys.is_empty()
}

fn manifest_envelope(writer_id: &str, payload: Vec<u8>) -> turbodb::Manifest {
    turbodb::Manifest {
        version: 0,
        writer_id: writer_id.to_string(),
        timestamp_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        payload,
    }
}

fn hybrid_manifest_required_message(name: &str) -> String {
    format!(
        "continuous turbolite+walrust database '{}' requires checkpointed base state in ManifestStore before WAL replay",
        name
    )
}

async fn publish_manifest(
    store: &Arc<dyn ManifestStore>,
    key: &str,
    writer_id: &str,
    payload: Vec<u8>,
) -> Result<Vec<u8>> {
    let manifest = manifest_envelope(writer_id, payload);
    let expected = store.meta(key).await?.map(|m| m.version);
    let cas = store.put(key, &manifest, expected).await?;
    if cas.success {
        return Ok(manifest.payload);
    }

    // Fresh bootstrap can race with another first publisher for the same
    // database. Recover by first trying a normal update if the winner becomes
    // visible, then by accepting a visible same-writer winner. A different
    // writer is still a correctness error.
    if expected.is_none() {
        for delay_ms in [0_u64, 10, 25, 50] {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }

            if let Some(retry_expected) = store.meta(key).await?.map(|m| m.version) {
                let retry = store.put(key, &manifest, Some(retry_expected)).await?;
                if retry.success {
                    return Ok(manifest.payload);
                }
            }

            if let Some(current) = store.get(key).await? {
                if current.writer_id == manifest.writer_id {
                    tracing::debug!(
                        key,
                        winner_version = current.version,
                        writer_id = %current.writer_id,
                        "manifest create lost race to same writer; treating existing manifest as authoritative"
                    );
                    return Ok(current.payload);
                }

                return Err(anyhow!(
                    "manifest CAS conflict for '{}' won by different writer '{}' (ours '{}')",
                    key,
                    current.writer_id,
                    manifest.writer_id
                ));
            }
        }
    }

    Err(anyhow!(
        "manifest CAS conflict for '{}' (expected {:?})",
        key,
        expected
    ))
}

pub struct TurboliteReplicator {
    vfs: SharedTurboliteVfs,
    manifest_store: Arc<dyn ManifestStore>,
    manifest_key: String,
    writer_id: String,
}

impl TurboliteReplicator {
    pub fn new(
        vfs: SharedTurboliteVfs,
        manifest_store: Arc<dyn ManifestStore>,
        prefix: &str,
        db_name: &str,
    ) -> Self {
        Self {
            vfs,
            manifest_store,
            manifest_key: manifest_key(prefix, db_name),
            writer_id: String::new(),
        }
    }

    pub fn with_writer_id(mut self, writer_id: String) -> Self {
        self.writer_id = writer_id;
        self
    }

    async fn publish_current_manifest(&self) -> Result<()> {
        if !should_publish_manifest(&self.vfs) {
            return Ok(());
        }
        let payload = self
            .vfs
            .manifest_bytes()
            .map_err(|e| anyhow!("turbolite manifest_bytes failed: {}", e))?;
        let actual_payload = publish_manifest(
            &self.manifest_store,
            &self.manifest_key,
            &self.writer_id,
            payload,
        )
        .await?;
        self.vfs
            .set_manifest_bytes(&actual_payload)
            .map_err(|e| anyhow!("turbolite adopt authoritative manifest failed: {}", e))?;
        Ok(())
    }
}

#[async_trait]
impl Replicator for TurboliteReplicator {
    async fn add(&self, _name: &str, _path: &Path) -> Result<()> {
        Ok(())
    }

    async fn pull(&self, _name: &str, _path: &Path) -> Result<()> {
        if let Some(manifest) = self.manifest_store.get(&self.manifest_key).await? {
            self.vfs
                .set_manifest_bytes(&manifest.payload)
                .map_err(|e| anyhow!("turbolite set_manifest_bytes failed: {}", e))?;
        }
        Ok(())
    }

    async fn remove(&self, _name: &str) -> Result<()> {
        self.publish_current_manifest().await
    }

    async fn sync(&self, _name: &str) -> Result<()> {
        self.publish_current_manifest().await
    }
}

pub struct TurboliteWalReplicator {
    vfs: SharedTurboliteVfs,
    manifest_store: Arc<dyn ManifestStore>,
    manifest_key: String,
    writer_id: String,
    walrust_prefix: String,
    walrust_storage: Arc<dyn StorageBackend>,
    walrust: Arc<haqlite::ExternalSnapshotSqliteReplicator>,
    replay_base_pending_publish: Arc<AtomicBool>,
    replay_base_seq: Arc<AtomicU64>,
    last_published_base_cursor: Mutex<Option<walrust::ExternalBaseCursor>>,
    live_wal_path: Mutex<Option<std::path::PathBuf>>,
}

impl TurboliteWalReplicator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vfs: SharedTurboliteVfs,
        manifest_store: Arc<dyn ManifestStore>,
        prefix: &str,
        db_name: &str,
        writer_id: String,
        walrust_prefix: String,
        walrust_storage: Arc<dyn StorageBackend>,
        walrust: Arc<haqlite::ExternalSnapshotSqliteReplicator>,
        replay_base_pending_publish: Arc<AtomicBool>,
        replay_base_seq: Arc<AtomicU64>,
    ) -> Self {
        Self {
            vfs,
            manifest_store,
            manifest_key: manifest_key(prefix, db_name),
            writer_id,
            walrust_prefix,
            walrust_storage,
            walrust,
            replay_base_pending_publish,
            replay_base_seq,
            last_published_base_cursor: Mutex::new(None),
            live_wal_path: Mutex::new(None),
        }
    }

    pub fn walrust(&self) -> &Arc<haqlite::ExternalSnapshotSqliteReplicator> {
        &self.walrust
    }

    fn live_wal_path(&self, db_path: &Path) -> Result<std::path::PathBuf> {
        let db_name = db_path
            .file_name()
            .ok_or_else(|| anyhow!("invalid turbolite db path '{}'", db_path.display()))?;
        let cache_dir = self
            .vfs
            .cache_file_path()
            .parent()
            .ok_or_else(|| anyhow!("cache_file_path has no parent"))?
            .to_path_buf();
        Ok(cache_dir.join(format!("{}-wal", db_name.to_string_lossy())))
    }

    async fn publish_current_manifest(
        &self,
        name: &str,
        _covered_wal_seq: Option<u64>,
    ) -> Result<()> {
        if !should_publish_manifest(&self.vfs) {
            return Err(anyhow!(
                "{} (turbolite manifest is still empty)",
                hybrid_manifest_required_message(name)
            ));
        }

        let used_pending_replay_base = self.replay_base_pending_publish.load(Ordering::Acquire);
        let replay_base_seq = if used_pending_replay_base {
            let seq = self.replay_base_seq.load(Ordering::Acquire);
            if seq == 0 {
                return Err(anyhow!(
                    "turbolite pending replay-base publish for '{}' has no replay seq",
                    name
                ));
            }
            Some(seq)
        } else {
            None
        };
        // A WAL changeset being durable through seq N is not the same thing as
        // the Turbolite page base containing seq N. Only replayed page bases
        // that are being published now may advance the manifest replay cursor;
        // otherwise new openers must materialize the base and replay the WAL
        // delta chain after it.
        let exact_replay_cursor = replay_base_seq;

        // Route through `publish_replayed_base`. For a leader that
        // checkpointed via the SQLite write path, pending replay
        // state is empty and this is equivalent to encoding the
        // current pure base manifest. For a freshly-promoted follower
        // that has accumulated replay staging logs and dirty
        // groups, this flushes them to remote storage as fresh
        // page-group keys before encoding the hybrid manifest, so
        // a third fresh follower joining from the published
        // manifest sees the replayed bytes — not stale pre-replay
        // page-group keys.
        let payload = self
            .vfs
            .publish_replayed_base()
            .map_err(|e| anyhow!("turbolite publish_replayed_base failed: {}", e))?;
        let payload = if let Some(seq) = exact_replay_cursor {
            self.vfs
                .set_manifest_bytes(&payload)
                .map_err(|e| anyhow!("turbolite adopt replay-base payload failed: {}", e))?;
            self.vfs
                .manifest_bytes_with_exact_replay_cursor(seq)
                .map_err(|e| anyhow!("turbolite encode exact replay cursor failed: {}", e))?
        } else {
            payload
        };

        let actual_payload = publish_manifest(
            &self.manifest_store,
            &self.manifest_key,
            &self.writer_id,
            payload,
        )
        .await?;
        let decoded = turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&actual_payload)
            .map_err(|e| anyhow!("turbolite decode authoritative base failed: {}", e))?;
        self.vfs.set_manifest_bytes(&actual_payload).map_err(|e| {
            anyhow!(
                "turbolite install authoritative base manifest failed: {}",
                e
            )
        })?;
        let published_base_seq = decoded.change_counter;
        let cache_path = self.vfs.cache_file_path();
        let temp_path =
            cache_path.with_extension(format!("published-base-{}.tmp", uuid::Uuid::new_v4()));
        let vfs = self.vfs.clone();
        let decoded_for_checksum = decoded.clone();
        let published_base_checksum = tokio::task::spawn_blocking(move || -> Result<u64> {
            vfs.shared_state()
                .materialize_manifest_to_file(&decoded_for_checksum, &temp_path)
                .context("turbolite materialize published base for checksum failed")?;
            let checksum = walrust::ltx::compute_checksum_from_file(&temp_path)
                .map_err(|e| anyhow!("walrust published base checksum failed: {}", e))?;
            let _ = std::fs::remove_file(&temp_path);
            Ok(checksum)
        })
        .await
        .map_err(|e| anyhow!("turbolite published-base checksum task panicked: {}", e))??;

        if let Ok(mut guard) = self.last_published_base_cursor.lock() {
            *guard = Some(walrust::ExternalBaseCursor {
                seq: published_base_seq,
                checksum: published_base_checksum,
            });
        }

        if used_pending_replay_base {
            self.replay_base_pending_publish
                .store(false, Ordering::Release);
            self.replay_base_seq.store(0, Ordering::Release);
        }

        Ok(())
    }

    async fn ensure_base_manifest(&self, name: &str, path: &Path) -> Result<()> {
        if let Some(manifest) = self.manifest_store.get(&self.manifest_key).await? {
            turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&manifest.payload)
                .map_err(|e| anyhow!("turbolite decode manifest for '{}' failed: {}", name, e))?;
            self.restore_from_manifest(name, path).await?;
            return Ok(());
        }

        if self.vfs.remote_manifest_exists().map_err(|e| {
            anyhow!(
                "turbolite remote manifest_exists failed for '{}': {}",
                name,
                e
            )
        })? {
            let vfs = self.vfs.clone();
            let restored =
                tokio::task::spawn_blocking(move || vfs.fetch_and_apply_remote_manifest())
                    .await
                    .map_err(|e| anyhow!("turbolite fetch manifest task panicked: {}", e))?
                    .map_err(|e| anyhow!("turbolite fetch remote manifest failed: {}", e))?;

            if restored.is_none() {
                return Err(anyhow!(
                    "turbolite backend reported a manifest for '{}', but fetch returned none",
                    name
                ));
            }
        } else {
            // Brand-new tenant: no remote manifest exists yet AND
            // db_path may not exist on disk because turbolite-backed
            // connections write through the VFS, not the OS path.
            // `import_sqlite_file` requires a real file, so seed a
            // minimal SQLite header at db_path first.
            seed_local_sqlite_for_import(path).map_err(|e| {
                anyhow!(
                    "turbolite seed local sqlite for fresh bootstrap of '{}' at {}: {}",
                    name,
                    path.display(),
                    e
                )
            })?;
            let vfs = self.vfs.clone();
            let path_buf = path.to_path_buf();
            tokio::task::spawn_blocking(move || vfs.import_sqlite_file(&path_buf))
                .await
                .map_err(|e| anyhow!("turbolite import task panicked: {}", e))?
                .map_err(|e| anyhow!("turbolite import failed: {}", e))?;
        }

        self.publish_current_manifest(name, None).await
    }

    fn external_base_cursor(&self, cache_path: &Path) -> Result<walrust::ExternalBaseCursor> {
        let manifest = self.vfs.manifest();
        if let Ok(guard) = self.last_published_base_cursor.lock() {
            if let Some(cursor) = *guard {
                if cursor.seq == manifest.change_counter {
                    return Ok(cursor);
                }
            }
        }
        let temp_path =
            cache_path.with_extension(format!("external-base-{}.tmp", uuid::Uuid::new_v4()));
        self.vfs
            .shared_state()
            .materialize_manifest_to_file(&manifest, &temp_path)
            .context("turbolite materialize external base for checksum failed")?;
        let checksum = walrust::ltx::compute_checksum_from_file(&temp_path)
            .map_err(|e| anyhow!("walrust external base checksum failed: {}", e))?;
        let _ = std::fs::remove_file(&temp_path);
        Ok(walrust::ExternalBaseCursor {
            seq: manifest.change_counter,
            checksum,
        })
    }

    async fn add_external_base_with_retry(
        &self,
        name: &str,
        cache_path: &Path,
        wal_path: &Path,
        base: walrust::ExternalBaseCursor,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            match self
                .walrust
                .add_external_base_with_wal_path(name, cache_path, wal_path, base)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e)
                    if e.to_string().contains("missing its changeset object")
                        && Instant::now() < deadline =>
                {
                    tracing::warn!(
                        "TurboliteWalReplicator::add('{}') hit transient missing base changeset on attempt {}: {}; retrying",
                        name,
                        attempts,
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn restore_from_manifest(&self, name: &str, _path: &Path) -> Result<()> {
        let Some(manifest) = self.manifest_store.get(&self.manifest_key).await? else {
            return Err(anyhow!("{}", hybrid_manifest_required_message(name)));
        };

        let payload_owned = manifest.payload.clone();

        let decoded_manifest =
            turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&payload_owned)
                .map_err(|e| anyhow!("turbolite decode_manifest_bytes failed: {}", e))?;
        let walrust_seq = decoded_manifest.change_counter;

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            let prepared_replay = prepare_page_replay(
                self.walrust_storage.as_ref(),
                &self.walrust_prefix,
                name,
                walrust_seq,
            )
            .await?;

            let vfs = self.vfs.clone();
            let cache_path = self.vfs.cache_file_path();
            let gate = self.vfs.replay_gate();
            let payload_for_attempt = payload_owned.clone();
            let decoded_manifest_for_attempt = decoded_manifest.clone();
            let name_owned = name.to_string();
            let replay_base_pending_publish = self.replay_base_pending_publish.clone();
            let replay_base_seq = self.replay_base_seq.clone();
            let attempt = tokio::task::spawn_blocking(move || -> Result<()> {
                let _gate = loop {
                    if let Some(guard) = gate.try_write() {
                        break guard;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                };

                vfs.shared_state()
                    .materialize_manifest_to_file(&decoded_manifest_for_attempt, &cache_path)
                    .context("turbolite materialize failed")?;

                // Materialize succeeded; commit the new manifest.
                vfs.set_manifest_bytes(&payload_for_attempt)
                    .map_err(|e| anyhow!("turbolite set_manifest_bytes failed: {}", e))?;

                let page_count = vfs.manifest().page_count;
                vfs.sync_after_external_restore(page_count);

                let base_checksum = walrust::ltx::compute_checksum_from_file(&cache_path)
                    .map_err(|e| anyhow!("walrust base checksum failed: {}", e))?;
                prepared_replay.validate_base_checksum(base_checksum)?;

                let handle = vfs
                    .begin_replay_after(walrust_seq)
                    .map_err(|e| anyhow!("turbolite begin_replay failed: {}", e))?;
                let mut sink = HaqliteTurboliteReplaySink::new_under_external_write(handle);
                let final_seq = apply_prepared_page_replay(&mut sink, prepared_replay)?;

                tracing::debug!(
                    "TurboliteWalReplicator::pull('{}') replayed walrust {} -> {} via direct page sink",
                    name_owned,
                    walrust_seq,
                    final_seq
                );
                if final_seq > walrust_seq {
                    replay_base_pending_publish.store(true, Ordering::Release);
                    replay_base_seq.fetch_max(final_seq, Ordering::AcqRel);
                }

                Ok(())
            })
            .await
            .map_err(|e| anyhow!("turbolite restore task panicked: {}", e))?;

            match attempt {
                Ok(()) => return Ok(()),
                Err(e) if is_not_found_error(&e) && Instant::now() < deadline => {
                    tracing::warn!(
                        "TurboliteWalReplicator::pull('{}') hit transient missing object on attempt {}: {}; retrying",
                        name,
                        attempts,
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[async_trait]
impl Replicator for TurboliteWalReplicator {
    async fn add(&self, name: &str, path: &Path) -> Result<()> {
        let cache_path = self.vfs.cache_file_path();
        let wal_path = self.live_wal_path(path)?;
        if let Ok(mut guard) = self.live_wal_path.lock() {
            *guard = Some(wal_path.clone());
        }
        self.ensure_base_manifest(name, path).await?;
        let base = self.external_base_cursor(&cache_path)?;
        self.add_external_base_with_retry(name, &cache_path, &wal_path, base)
            .await
    }

    async fn add_continuing(&self, name: &str, path: &Path) -> Result<()> {
        let cache_path = self.vfs.cache_file_path();
        let wal_path = self.live_wal_path(path)?;
        if let Ok(mut guard) = self.live_wal_path.lock() {
            *guard = Some(wal_path.clone());
        }

        // Promotion publish: hadb calls `add_continuing` right
        // after a follower wins the lease. Flush the accumulated
        // replay state now so a fresh follower joining before the
        // next commit still sees the replayed pages in the
        // published base.
        self.publish_current_manifest(name, None).await?;
        let base = self.external_base_cursor(&cache_path)?;

        self.add_external_base_with_retry(name, &cache_path, &wal_path, base)
            .await
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        self.restore_from_manifest(name, path).await
    }

    async fn remove(&self, name: &str) -> Result<()> {
        let frames = self.walrust.inner().flush(name).await?;
        if frames > 0 {
            tracing::info!(
                "TurboliteWalReplicator::remove('{}') flushed {} frames",
                name,
                frames,
            );
        }
        let covered_wal_seq = self.walrust.inner().current_seq(name).await;
        self.publish_current_manifest(name, covered_wal_seq).await?;
        self.walrust.remove(name).await
    }

    async fn sync(&self, name: &str) -> Result<()> {
        let frames = self.walrust.inner().flush(name).await?;
        if frames > 0 {
            tracing::info!(
                "TurboliteWalReplicator::sync('{}') flushed {} frames",
                name,
                frames,
            );
        }
        if self.replay_base_pending_publish.load(Ordering::Acquire) {
            self.publish_current_manifest(name, None).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hadb_storage::CasResult;
    use std::sync::Mutex;
    use turbodb::{Manifest, ManifestMeta};

    #[derive(Default)]
    struct SameWriterCreateConflictStore {
        current: Mutex<Option<Manifest>>,
    }

    #[async_trait]
    impl ManifestStore for SameWriterCreateConflictStore {
        async fn get(&self, _key: &str) -> Result<Option<Manifest>> {
            Ok(self.current.lock().expect("lock").clone())
        }

        async fn put(
            &self,
            _key: &str,
            manifest: &Manifest,
            expected_version: Option<u64>,
        ) -> Result<CasResult> {
            let mut current = self.current.lock().expect("lock");
            if expected_version.is_none() && current.is_some() {
                return Ok(CasResult {
                    success: false,
                    etag: None,
                });
            }

            let mut stored = manifest.clone();
            stored.version = expected_version.unwrap_or(0) + 1;
            *current = Some(stored);
            Ok(CasResult {
                success: true,
                etag: None,
            })
        }

        async fn meta(&self, _key: &str) -> Result<Option<ManifestMeta>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn publish_manifest_returns_authoritative_same_writer_payload_on_create_conflict() {
        let store = Arc::new(SameWriterCreateConflictStore::default());
        let existing = manifest_envelope("writer-a", b"already-published".to_vec());
        *store.current.lock().expect("lock") = Some(existing);

        let actual = publish_manifest(
            &(store as Arc<dyn ManifestStore>),
            "db/_manifest",
            "writer-a",
            b"candidate".to_vec(),
        )
        .await
        .expect("same-writer conflict should be accepted");

        assert_eq!(actual, b"already-published");
    }
}
