//! SQLite replicator implementation using walrust.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use hadb_storage::StorageBackend;
use walrust::{ReplicationConfig, SnapshotOwnership, SnapshotSource};

use hadb::Replicator;

/// SQLite replicator wrapping walrust.
///
/// Handles WAL replication via HADBP changesets (SQLite-specific format).
pub struct SqliteReplicator {
    inner: Arc<walrust::Replicator>,
    /// When true, `add()` skips the initial snapshot upload.
    /// Used for SingleWriter+Synchronous where remote storage handles durability
    /// and the local SQLite file may be empty (data is in S3 page groups).
    skip_snapshot_on_add: bool,
}

impl SqliteReplicator {
    /// Create a new SqliteReplicator.
    ///
    /// `storage` is the walrust S3 backend for WAL data.
    /// `prefix` is the S3 key prefix for all databases (e.g. "wal/" or "ha/").
    /// `config` is the walrust replication configuration.
    pub fn new(storage: Arc<dyn StorageBackend>, prefix: &str, config: ReplicationConfig) -> Self {
        Self {
            inner: walrust::Replicator::new(storage, prefix, config),
            skip_snapshot_on_add: false,
        }
    }

    /// Skip snapshot upload on `add()`. For SingleWriter+Synchronous where
    /// remote storage handles durability and the local SQLite file has no data.
    pub fn with_skip_snapshot(mut self, skip: bool) -> Self {
        self.skip_snapshot_on_add = skip;
        self
    }

    /// Get a reference to the inner walrust Replicator.
    ///
    /// Useful for operations not exposed by the hadb Replicator trait.
    pub fn inner(&self) -> &Arc<walrust::Replicator> {
        &self.inner
    }

    /// Restore a database from S3, returning the final seq.
    ///
    /// This is SQLite-specific and not part of the generic Replicator trait.
    pub async fn restore(&self, name: &str, output_path: &Path) -> Result<Option<u64>> {
        self.inner.restore(name, output_path).await
    }

    /// Register a database without taking a snapshot.
    /// Use after restore() to avoid uploading a redundant snapshot.
    pub async fn add_without_snapshot(&self, name: &str, path: &Path) -> Result<()> {
        self.inner.add_without_snapshot(name, path).await
    }
}

/// SQLite replicator for external-base-state mode.
///
/// Use this when another layer owns checkpointed base state and walrust should
/// only ship / replay WAL deltas after that checkpoint.
pub struct ExternalSnapshotSqliteReplicator {
    inner: Arc<walrust::Replicator>,
    snapshot_source: Arc<dyn SnapshotSource>,
}

impl ExternalSnapshotSqliteReplicator {
    /// Create a new external-base-state SQLite replicator.
    ///
    /// The wrapper forces walrust into external snapshot ownership. The caller
    /// must still pass `autonomous_snapshots = false`; enabling periodic
    /// snapshots in this mode is a real bug and is rejected.
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        prefix: &str,
        mut config: ReplicationConfig,
        snapshot_source: Arc<dyn SnapshotSource>,
    ) -> Result<Self> {
        if config.autonomous_snapshots {
            anyhow::bail!(
                "external-base-state SQLite replication requires autonomous_snapshots = false"
            );
        }
        config.snapshot_ownership = SnapshotOwnership::External;

        Ok(Self {
            inner: walrust::Replicator::try_new(storage, prefix, config)?,
            snapshot_source,
        })
    }

    pub fn inner(&self) -> &Arc<walrust::Replicator> {
        &self.inner
    }
}

#[async_trait]
impl Replicator for SqliteReplicator {
    async fn add(&self, name: &str, path: &Path) -> Result<()> {
        if self.skip_snapshot_on_add {
            self.inner.add_without_snapshot(name, path).await
        } else {
            self.inner.add(name, path).await
        }
    }

    async fn add_continuing(&self, name: &str, path: &Path) -> Result<()> {
        self.inner.add_without_snapshot(name, path).await
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        // Restore from latest snapshot + chained incrementals.
        let restored_seq = self.inner.restore(name, path).await?;
        let seq = restored_seq.unwrap_or(0);

        // Apply any remaining incrementals that restore() skipped due to
        // checksum chain breaks (e.g., changesets from a promoted leader
        // whose chain diverged). pull_incremental applies by page content,
        // not checksum chain, so it can bridge the gap.
        let final_seq = walrust::sync::pull_incremental(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            path,
            seq,
        )
        .await?;

        if final_seq > seq {
            tracing::info!(
                "SqliteReplicator::pull('{}') bridged chain gap: restore seq {} -> pull seq {}",
                name,
                seq,
                final_seq,
            );
        }
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        self.inner.remove(name).await;
        Ok(())
    }

    async fn sync(&self, name: &str) -> Result<()> {
        let frames = self.inner.flush(name).await?;
        if frames > 0 {
            tracing::info!(
                "SqliteReplicator::sync('{}') flushed {} frames",
                name,
                frames
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Replicator for ExternalSnapshotSqliteReplicator {
    async fn add(&self, name: &str, path: &Path) -> Result<()> {
        self.inner.add(name, path).await
    }

    async fn add_continuing(&self, name: &str, path: &Path) -> Result<()> {
        self.inner.add_without_snapshot(name, path).await
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        let restored_seq = walrust::restore_with_snapshot_source(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            path,
            self.snapshot_source.as_ref(),
        )
        .await?;

        let final_seq = walrust::sync::pull_incremental(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            path,
            restored_seq,
        )
        .await?;

        if final_seq > restored_seq {
            tracing::info!(
                "ExternalSnapshotSqliteReplicator::pull('{}') advanced after restore: {} -> {}",
                name,
                restored_seq,
                final_seq,
            );
        }
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        self.inner.remove(name).await;
        Ok(())
    }

    async fn sync(&self, name: &str) -> Result<()> {
        let frames = self.inner.flush(name).await?;
        if frames > 0 {
            tracing::info!(
                "ExternalSnapshotSqliteReplicator::sync('{}') flushed {} frames",
                name,
                frames,
            );
        }
        Ok(())
    }
}
