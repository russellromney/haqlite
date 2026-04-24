use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use hadb::Replicator;
use turbodb::ManifestStore;
use turbolite::tiered::{SharedTurboliteVfs, TurboliteVfs};

pub struct TurboliteReplicator {
    vfs: SharedTurboliteVfs,
    manifest_store: Arc<dyn ManifestStore>,
    manifest_key: String,
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
            manifest_key: format!("{}{}/_manifest", prefix, db_name),
        }
    }

    pub fn vfs(&self) -> &TurboliteVfs {
        &self.vfs
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
                .map_err(|e| anyhow::anyhow!("turbolite set_manifest_bytes failed: {}", e))?;
        }
        Ok(())
    }

    async fn remove(&self, _name: &str) -> Result<()> {
        Ok(())
    }

    async fn sync(&self, _name: &str) -> Result<()> {
        self.vfs
            .flush_to_storage()
            .map_err(|e| anyhow::anyhow!("turbolite flush_to_storage failed: {}", e))?;
        Ok(())
    }
}
