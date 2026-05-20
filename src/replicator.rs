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
    /// When true, `pull()` is allowed to bridge a chain break by applying
    /// forked changesets by page content (the old behavior). Default false:
    /// a chain break that `restore()` refused is a hard error rather than a
    /// silent fork merge (F5). Only enable with an explicit fork-ack policy.
    allow_fork_bridge: bool,
}

/// Guard against `pull_incremental` silently bridging a forked history (F5).
///
/// `restore()` applies every chain-valid contiguous incremental after the
/// snapshot and stops at the first checksum-chain break. So any changeset
/// object still present after `restored_seq` is one `restore()` deliberately
/// refused — a fork from a prior lineage. `pull_incremental` would then apply
/// it by raw page content, merging the fork. Unless fork-bridging is explicitly
/// acknowledged, that is a hard error.
async fn ensure_no_fork_after_restore(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
    restored_seq: u64,
    allow_fork_bridge: bool,
) -> Result<()> {
    if allow_fork_bridge {
        return Ok(());
    }
    let leftover = walrust::hadb_changeset::storage::discover_after(
        storage,
        prefix,
        name,
        restored_seq,
        walrust::hadb_changeset::storage::ChangesetKind::Physical,
    )
    .await?;
    if let Some(first) = leftover.first() {
        anyhow::bail!(
            "refusing to bridge a chain break for '{}': restore stopped at seq {} but {} changeset(s) remain (next at seq {}); these are a forked lineage restore refused. Enable explicit fork-ack to bridge.",
            name,
            restored_seq,
            leftover.len(),
            first.seq,
        );
    }
    Ok(())
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
            allow_fork_bridge: false,
        }
    }

    /// Skip snapshot upload on `add()`. For SingleWriter+Synchronous where
    /// remote storage handles durability and the local SQLite file has no data.
    pub fn with_skip_snapshot(mut self, skip: bool) -> Self {
        self.skip_snapshot_on_add = skip;
        self
    }

    /// Allow `pull()` to bridge a chain break by applying forked changesets
    /// by page content. Default is false (fork = hard error). Only enable
    /// behind an explicit fork-acknowledgement policy (F5).
    pub fn with_allow_fork_bridge(mut self, allow: bool) -> Self {
        self.allow_fork_bridge = allow;
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

    /// Register a database with an explicit WAL path.
    pub async fn add_with_wal_path(&self, name: &str, path: &Path, wal_path: &Path) -> Result<()> {
        self.inner.add_with_wal_path(name, path, wal_path).await
    }

    /// Register a database without taking a snapshot, with an explicit WAL path.
    pub async fn add_without_snapshot_with_wal_path(
        &self,
        name: &str,
        path: &Path,
        wal_path: &Path,
    ) -> Result<()> {
        self.inner
            .add_without_snapshot_with_wal_path(name, path, wal_path)
            .await
    }

    pub async fn add_external_base_with_wal_path(
        &self,
        name: &str,
        path: &Path,
        wal_path: &Path,
        base: walrust::ExternalBaseCursor,
    ) -> Result<()> {
        self.inner
            .add_external_base_with_wal_path(name, path, wal_path, base)
            .await
    }
}

/// SQLite replicator for external-base-state mode.
///
/// Use this when another layer owns checkpointed base state and walrust should
/// only ship / replay WAL deltas after that checkpoint.
pub struct ExternalSnapshotSqliteReplicator {
    inner: Arc<walrust::Replicator>,
    snapshot_source: Arc<dyn SnapshotSource>,
    /// See `SqliteReplicator::allow_fork_bridge` (F5). Default false.
    allow_fork_bridge: bool,
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
            allow_fork_bridge: false,
        })
    }

    /// Allow `pull()` to bridge a chain break by applying forked changesets
    /// by page content. Default is false (fork = hard error). Only enable
    /// behind an explicit fork-acknowledgement policy (F5).
    pub fn with_allow_fork_bridge(mut self, allow: bool) -> Self {
        self.allow_fork_bridge = allow;
        self
    }

    pub fn inner(&self) -> &Arc<walrust::Replicator> {
        &self.inner
    }

    pub async fn add_with_wal_path(&self, name: &str, path: &Path, wal_path: &Path) -> Result<()> {
        self.inner.add_with_wal_path(name, path, wal_path).await
    }

    pub async fn add_without_snapshot_with_wal_path(
        &self,
        name: &str,
        path: &Path,
        wal_path: &Path,
    ) -> Result<()> {
        self.inner
            .add_without_snapshot_with_wal_path(name, path, wal_path)
            .await
    }

    pub async fn add_external_base_with_wal_path(
        &self,
        name: &str,
        path: &Path,
        wal_path: &Path,
        base: walrust::ExternalBaseCursor,
    ) -> Result<()> {
        self.inner
            .add_external_base_with_wal_path(name, path, wal_path, base)
            .await
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

        // F5: a changeset still present after `restored_seq` is one restore
        // refused for a checksum-chain break (a forked lineage). Bridging it
        // by page content silently merges divergent history, so hard-error
        // unless fork-bridging is explicitly acknowledged.
        ensure_no_fork_after_restore(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            seq,
            self.allow_fork_bridge,
        )
        .await?;

        // Apply any remaining incrementals after the restored seq. With
        // fork-bridging acknowledged this can cross a chain break by page
        // content; otherwise the guard above already failed closed.
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

        // F5: hard-error on a forked tail restore refused, unless fork-bridging
        // is explicitly acknowledged.
        ensure_no_fork_after_restore(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            restored_seq,
            self.allow_fork_bridge,
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
