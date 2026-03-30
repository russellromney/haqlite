//! SQLite replicator implementation using walrust.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use walrust::ReplicationConfig;
use walrust::StorageBackend;

use hadb::Replicator;

/// SQLite replicator wrapping walrust.
///
/// Handles WAL replication via LTX files (SQLite-specific format).
pub struct SqliteReplicator {
    inner: Arc<walrust::Replicator>,
}

impl SqliteReplicator {
    /// Create a new SqliteReplicator.
    ///
    /// `storage` is the walrust S3 backend for WAL data.
    /// `prefix` is the S3 key prefix for all databases (e.g. "wal/" or "ha/").
    /// `config` is the walrust replication configuration.
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        prefix: &str,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            inner: walrust::Replicator::new(storage, prefix, config),
        }
    }

    /// Get a reference to the inner walrust Replicator.
    ///
    /// Useful for operations not exposed by the hadb Replicator trait.
    pub fn inner(&self) -> &Arc<walrust::Replicator> {
        &self.inner
    }

    /// Restore a database from S3, returning the final TXID.
    ///
    /// This is SQLite-specific and not part of the generic Replicator trait.
    pub async fn restore(&self, name: &str, output_path: &Path) -> Result<Option<u64>> {
        self.inner.restore(name, output_path).await
    }
}

#[async_trait]
impl Replicator for SqliteReplicator {
    async fn add(&self, name: &str, path: &Path) -> Result<()> {
        self.inner.add(name, path).await
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        // For SQLite, pull is the same as restore (get initial snapshot)
        self.inner.restore(name, path).await?;
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        self.inner.remove(name).await;
        Ok(())
    }

    async fn sync(&self, name: &str) -> Result<()> {
        let frames = self.inner.flush(name).await?;
        if frames > 0 {
            tracing::info!("SqliteReplicator::sync('{}') flushed {} frames", name, frames);
        }
        Ok(())
    }
}
