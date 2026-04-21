//! Turbolite replicator: `hadb::Replicator` over a turbolite VFS.
//!
//! Instead of shipping WAL frames (walrust's job), "turbolite
//! replication" is manifest propagation: publish turbolite's manifest
//! bytes through a `turbodb::ManifestStore`, and followers call
//! `TurboliteVfs::set_manifest_bytes()` on the fetched payload. The
//! VFS handles page-level storage; no frame shipping.
//!
//! Phase Turbogenesis-b: converter layer between turbolite's native
//! `tiered::Manifest` and hadb's old closed `Backend` enum is deleted.
//! Haqlite only passes opaque `Vec<u8>` between the VFS and the store
//! now; turbolite owns its persisted wire format end-to-end.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use hadb::Replicator;
use turbodb::ManifestStore;
use turbolite::tiered::{SharedTurboliteVfs, TurboliteVfs};

/// Replicator that uses turbolite's VFS manifest for state transfer.
///
/// Instead of shipping WAL frames (like walrust), turbolite replication
/// works by synchronizing the page-group manifest. The VFS handles
/// page-level storage, so "replication" is just manifest propagation.
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
        // VFS already registered; nothing to do.
        Ok(())
    }

    async fn pull(&self, _name: &str, _path: &Path) -> Result<()> {
        // Fetch envelope from store, hand opaque payload to the VFS.
        // If the payload was hybrid (turbolite + walrust delta), the
        // walrust fields come back; wiring them to a walrust replicator
        // is the caller's job, not this replicator's.
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
        // Flush pending uploads to storage (S3 or HTTP API).
        // In local mode, sync happens via the VFS xSync callback during checkpoint.
        self.vfs
            .flush_to_storage()
            .map_err(|e| anyhow::anyhow!("turbolite flush_to_storage failed: {}", e))?;
        Ok(())
    }
}
