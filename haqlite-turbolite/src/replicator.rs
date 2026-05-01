use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use hadb::Replicator;
use hadb_storage::StorageBackend;
use turbodb::ManifestStore;
use turbolite::tiered::SharedTurboliteVfs;

use crate::replay_sink::HaqliteTurboliteReplaySink;

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
) -> Result<()> {
    let manifest = manifest_envelope(writer_id, payload);
    let expected = store.meta(key).await?.map(|m| m.version);
    let cas = store.put(key, &manifest, expected).await?;
    if cas.success {
        return Ok(());
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
                    return Ok(());
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
                    return Ok(());
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
        publish_manifest(
            &self.manifest_store,
            &self.manifest_key,
            &self.writer_id,
            payload,
        )
        .await
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
    ) -> Self {
        Self {
            vfs,
            manifest_store,
            manifest_key: manifest_key(prefix, db_name),
            writer_id,
            walrust_prefix,
            walrust_storage,
            walrust,
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

    async fn current_walrust_seq(&self, name: &str) -> Result<u64> {
        self.walrust
            .inner()
            .current_seq(name)
            .await
            .ok_or_else(|| anyhow!("walrust database '{}' is not registered", name))
    }

    async fn publish_current_manifest(&self, name: &str) -> Result<()> {
        let current_manifest = self.vfs.manifest();
        if !should_publish_manifest(&self.vfs) {
            return Err(anyhow!(
                "{} (turbolite manifest is still empty)",
                hybrid_manifest_required_message(name)
            ));
        }

        // Resolve the cursor that the published manifest must
        // carry. If the base manifest version has not advanced
        // since a previous publish, we must keep the existing
        // hybrid cursor: a follower replaying from this manifest
        // expects WAL frames after that cursor, not after the
        // latest in-memory walrust seq.
        let current_walrust_seq = self.current_walrust_seq(name).await?;
        let walrust_seq =
            if let Some(existing) = self.manifest_store.get(&self.manifest_key).await? {
                match turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&existing.payload) {
                    Ok((published_manifest, Some((published_seq, _))))
                        if published_manifest.version == current_manifest.version =>
                    {
                        published_seq
                    }
                    Ok(_) | Err(_) => current_walrust_seq,
                }
            } else {
                current_walrust_seq
            };

        // Route through `publish_replayed_base`. For a leader that
        // checkpointed via the SQLite write path, pending replay
        // state is empty and this is equivalent to encoding the
        // current hybrid manifest. For a freshly-promoted follower
        // that has accumulated replay staging logs and dirty
        // groups, this flushes them to remote storage as fresh
        // page-group keys before encoding the hybrid manifest, so
        // a third fresh follower joining from the published
        // manifest sees the replayed bytes — not stale pre-replay
        // page-group keys.
        let payload = self
            .vfs
            .publish_replayed_base(walrust_seq, &self.walrust_prefix)
            .map_err(|e| anyhow!("turbolite publish_replayed_base failed: {}", e))?;
        publish_manifest(
            &self.manifest_store,
            &self.manifest_key,
            &self.writer_id,
            payload,
        )
        .await
    }

    async fn ensure_base_manifest(&self, name: &str, path: &Path) -> Result<()> {
        if self.manifest_store.get(&self.manifest_key).await?.is_some() {
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
            // No remote manifest, no published hybrid manifest. The local
            // db_path may legitimately not exist yet — turbolite-backed
            // connections write through the VFS, not the OS path, so a
            // brand-new tenant whose connection_opener didn't trigger an
            // xSync (e.g., empty schema, no committed pragma side-effect
            // visible to a remote HTTP storage backend before
            // ensure_base_manifest runs) leaves us with neither a remote
            // manifest nor a real file at `path`. import_sqlite_file
            // requires the file. Seed a minimal valid SQLite at `path`
            // before import so the bootstrap completes — turbolite then
            // imports a 1-page (header-only) database, which the
            // connection_opener fills in on first write.
            //
            // Symptom this fixes (cinch): redis tenant create returns 500
            // "Failed to create Redis database via HaQLite builder:
            // turbolite import failed: No such file or directory" and the
            // orchestrator-side claim is leaked, tripping Phase Skerry
            // watchdog with last_kind=EmptyQueue.
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

        self.publish_current_manifest(name).await
    }

    async fn restore_from_manifest(&self, name: &str, _path: &Path) -> Result<()> {
        let Some(manifest) = self.manifest_store.get(&self.manifest_key).await? else {
            return Err(anyhow!("{}", hybrid_manifest_required_message(name)));
        };

        // Same full-apply gate pattern as
        // `TurboliteFollowerBehavior::apply_manifest_payload`: take
        // the VFS replay-gate write lock once, hold across
        // set_manifest_bytes + materialize + sync + replay so VFS
        // reads can't observe a partial logical apply, and use
        // `finalize_assuming_external_write` to avoid a recursive
        // lock-take. See that method for the full design rationale.
        let runtime = tokio::runtime::Handle::current();
        let vfs = self.vfs.clone();
        let cache_path = self.vfs.cache_file_path();
        let gate = self.vfs.replay_gate();
        let payload_owned = manifest.payload.clone();
        let name_owned = name.to_string();
        let walrust_storage = self.walrust_storage.clone();
        let walrust_prefix = self.walrust_prefix.clone();

        // Decode the candidate manifest before mutating VFS state.
        // Same materialize-before-commit ordering as
        // `TurboliteFollowerBehavior::apply_manifest_payload`: a
        // missing-group race during materialize must leave the live
        // VFS untouched so the caller can retry without observing a
        // half-applied state. See that method for the full rationale.
        let (decoded_manifest, decoded_walrust) =
            turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&payload_owned)
                .map_err(|e| anyhow!("turbolite decode_manifest_bytes failed: {}", e))?;

        if decoded_walrust.is_none() {
            return Err(anyhow!(
                "continuous manifest for '{}' must carry walrust replay cursor",
                name
            ));
        }

        tokio::task::spawn_blocking(move || -> Result<()> {
            let _gate = gate.write();

            // Materialize from the decoded (not-yet-committed)
            // manifest first so a missing-group transient leaves
            // VFS state fully untouched.
            vfs.shared_state()
                .materialize_manifest_to_file(&decoded_manifest, &cache_path)
                .map_err(|e| anyhow!("turbolite materialize failed: {}", e))?;

            // Materialize succeeded; commit the new manifest.
            let walrust = vfs
                .set_manifest_bytes(&payload_owned)
                .map_err(|e| anyhow!("turbolite set_manifest_bytes failed: {}", e))?;
            let (walrust_seq, _changeset_prefix) = walrust.expect("decoded_walrust was Some");

            let page_count = vfs.manifest().page_count;
            vfs.sync_after_external_restore(page_count);

            let handle = vfs
                .begin_replay()
                .map_err(|e| anyhow!("turbolite begin_replay failed: {}", e))?;
            let mut sink = HaqliteTurboliteReplaySink::new_under_external_write(handle);
            let final_seq = runtime
                .block_on(walrust::sync::pull_incremental_into_sink(
                    walrust_storage.as_ref(),
                    &walrust_prefix,
                    &name_owned,
                    &mut sink,
                    walrust_seq,
                ))?;

            tracing::debug!(
                "TurboliteWalReplicator::pull('{}') replayed walrust {} -> {} via direct page sink",
                name_owned,
                walrust_seq,
                final_seq
            );

            Ok(())
        })
        .await
        .map_err(|e| anyhow!("turbolite restore task panicked: {}", e))??;

        Ok(())
    }
}

#[async_trait]
impl Replicator for TurboliteWalReplicator {
    async fn add(&self, name: &str, path: &Path) -> Result<()> {
        let cache_path = self.vfs.cache_file_path();
        let wal_path = self.live_wal_path(path)?;
        self.walrust
            .add_with_wal_path(name, &cache_path, &wal_path)
            .await?;
        self.ensure_base_manifest(name, path).await
    }

    async fn add_continuing(&self, name: &str, path: &Path) -> Result<()> {
        let cache_path = self.vfs.cache_file_path();
        let wal_path = self.live_wal_path(path)?;
        self.walrust
            .add_without_snapshot_with_wal_path(name, &cache_path, &wal_path)
            .await
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        self.restore_from_manifest(name, path).await
    }

    async fn remove(&self, name: &str) -> Result<()> {
        self.walrust.sync(name).await?;
        self.publish_current_manifest(name).await?;
        self.walrust.remove(name).await
    }

    async fn sync(&self, name: &str) -> Result<()> {
        self.walrust.sync(name).await?;
        self.publish_current_manifest(name).await
    }
}
