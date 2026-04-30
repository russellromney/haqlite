use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use hadb::Replicator;
use hadb_storage::StorageBackend;
use turbodb::ManifestStore;
use turbolite::tiered::SharedTurboliteVfs;

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

fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow!("remove {}: {}", path.display(), e)),
    }
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}{}", path.display(), suffix))
}

fn remove_sqlite_sidecars(path: &Path) -> Result<()> {
    for suffix in ["-wal", "-shm", "-journal"] {
        remove_if_exists(&sqlite_sidecar_path(path, suffix))?;
    }
    Ok(())
}

fn normalize_replayed_sqlite_base(path: &Path) -> Result<()> {
    let conn = rusqlite::Connection::open(path)
        .map_err(|e| anyhow!("open WAL-replayed sqlite base {}: {}", path.display(), e))?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")
        .map_err(|e| {
            anyhow!(
                "normalize WAL-replayed sqlite base {} to rollback journal: {}",
                path.display(),
                e
            )
        })?;
    drop(conn);
    remove_sqlite_sidecars(path)
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

        let current_walrust_seq = self.current_walrust_seq(name).await?;
        let walrust_seq =
            if let Some(existing) = self.manifest_store.get(&self.manifest_key).await? {
                match turbolite::tiered::TurboliteVfs::decode_manifest_bytes(&existing.payload) {
                    Ok((published_manifest, Some((published_seq, _))))
                        if published_manifest.version == current_manifest.version =>
                    {
                        // The hybrid cursor is the walrust seq already included
                        // in this Turbolite base, not the latest uploaded WAL seq.
                        // If the base manifest did not change, followers must keep
                        // replaying WAL after the original base cursor.
                        published_seq
                    }
                    Ok(_) | Err(_) => current_walrust_seq,
                }
            } else {
                current_walrust_seq
            };
        let payload = self
            .vfs
            .manifest_bytes_with_walrust_delta(walrust_seq, &self.walrust_prefix)
            .map_err(|e| anyhow!("turbolite hybrid manifest_bytes failed: {}", e))?;
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

    async fn restore_from_manifest(&self, name: &str, path: &Path) -> Result<()> {
        let Some(manifest) = self.manifest_store.get(&self.manifest_key).await? else {
            return Err(anyhow!("{}", hybrid_manifest_required_message(name)));
        };

        let walrust = self
            .vfs
            .set_manifest_bytes(&manifest.payload)
            .map_err(|e| anyhow!("turbolite set_manifest_bytes failed: {}", e))?;

        let Some((walrust_seq, _changeset_prefix)) = walrust else {
            return Err(anyhow!(
                "continuous manifest for '{}' must carry walrust replay cursor",
                name
            ));
        };

        let cache_path = self.vfs.cache_file_path();
        let restore_path = cache_path.with_extension("restore.db");
        let vfs = self.vfs.clone();
        let restore_path_for_materialize = restore_path.clone();
        let materialized_version = tokio::task::spawn_blocking(move || {
            vfs.shared_state()
                .materialize_to_file(&restore_path_for_materialize)
        })
        .await
        .map_err(|e| anyhow!("turbolite materialize task panicked: {}", e))?
        .map_err(|e| anyhow!("turbolite materialize failed: {}", e))?;

        // Followers bootstrap through a normal SQLite/VFS connection before they
        // ever promote. That can leave behind a local `<db>-wal` / `<db>-shm`
        // pair containing stale pre-restore state. After we materialize the
        // authoritative base into `data.cache` and replay walrust frames onto it,
        // the local sidecars must be cleared or a promoted leader connection can
        // overlay the fresh base with old empty-table frames.
        remove_sqlite_sidecars(path)?;

        let final_seq = walrust::sync::pull_incremental(
            self.walrust_storage.as_ref(),
            &self.walrust_prefix,
            name,
            &restore_path,
            walrust_seq,
        )
        .await?;
        let restore_path_for_normalize = restore_path.clone();
        tokio::task::spawn_blocking(move || {
            normalize_replayed_sqlite_base(&restore_path_for_normalize)
        })
        .await
        .map_err(|e| anyhow!("sqlite base normalize task panicked: {}", e))??;
        let vfs = self.vfs.clone();
        let restore_path_for_import = restore_path.clone();
        tokio::task::spawn_blocking(move || vfs.import_sqlite_file(&restore_path_for_import))
            .await
            .map_err(|e| anyhow!("turbolite import task panicked: {}", e))?
            .map_err(|e| anyhow!("turbolite import after WAL replay failed: {}", e))?;
        self.vfs
            .replace_cache_from_sqlite_file(&restore_path)
            .map_err(|e| anyhow!("turbolite adopt restored cache failed: {}", e))?;
        remove_if_exists(&restore_path)?;
        tracing::debug!(
            "TurboliteWalReplicator::pull('{}') restored base {}, WAL {} -> {}",
            name,
            materialized_version,
            walrust_seq,
            final_seq
        );

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
