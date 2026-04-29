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

    async fn current_walrust_seq(&self, name: &str) -> Result<u64> {
        self.walrust
            .inner()
            .current_seq(name)
            .await
            .ok_or_else(|| anyhow!("walrust database '{}' is not registered", name))
    }

    async fn publish_current_manifest(&self, name: &str) -> Result<()> {
        if !should_publish_manifest(&self.vfs) {
            return Err(anyhow!(
                "{} (turbolite manifest is still empty)",
                hybrid_manifest_required_message(name)
            ));
        }

        let walrust_seq = self.current_walrust_seq(name).await?;
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
            let vfs = self.vfs.clone();
            let path = path.to_path_buf();
            tokio::task::spawn_blocking(move || vfs.import_sqlite_file(&path))
                .await
                .map_err(|e| anyhow!("turbolite import task panicked: {}", e))?
                .map_err(|e| anyhow!("turbolite import failed: {}", e))?;
        }

        self.publish_current_manifest(name).await
    }

    async fn restore_from_manifest(&self, name: &str) -> Result<()> {
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
        let page_count = self.vfs.manifest().page_count;
        let vfs = self.vfs.clone();
        let materialized_version = tokio::task::spawn_blocking(move || {
            vfs.shared_state().materialize_to_file(&cache_path)
        })
        .await
        .map_err(|e| anyhow!("turbolite materialize task panicked: {}", e))?
        .map_err(|e| anyhow!("turbolite materialize failed: {}", e))?;

        self.vfs.sync_after_external_restore(page_count);

        let cache_path = self.vfs.cache_file_path();
        let final_seq = walrust::sync::pull_incremental(
            self.walrust_storage.as_ref(),
            &self.walrust_prefix,
            name,
            &cache_path,
            walrust_seq,
        )
        .await?;
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
        self.walrust.add(name, path).await?;
        self.ensure_base_manifest(name, path).await
    }

    async fn add_continuing(&self, name: &str, path: &Path) -> Result<()> {
        self.walrust.add_continuing(name, path).await
    }

    async fn pull(&self, name: &str, _path: &Path) -> Result<()> {
        self.restore_from_manifest(name).await
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
