//! CLI operations for haqlite: list, verify, compact, snapshot, replicate.
//!
//! These are the implementations behind `haqlite list`, `haqlite verify`, etc.
//! They operate on a `hadb_storage::StorageBackend` (S3-compatible object store)
//! and parse the HADBP changeset format from S3 key names.

use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::Utc;
use hadb_storage::StorageBackend;

// ============================================================================
// Helpers
// ============================================================================

/// Normalize a prefix so it either is empty or ends with '/'.
fn normalize_prefix(prefix: &str) -> String {
    if prefix.is_empty() {
        String::new()
    } else if prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    }
}

// ============================================================================
// Changeset discovery
// ============================================================================

/// A changeset file discovered from S3 by parsing HADBP filenames.
#[derive(Debug, Clone)]
pub struct DiscoveredLtx {
    /// Full S3 key
    pub key: String,
    /// Sequence number
    pub seq: u64,
    /// Whether this is a snapshot (generation > 0) vs incremental (generation 0)
    pub is_snapshot: bool,
    /// Generation number (0 = live incremental, 1+ = snapshot)
    pub generation: u64,
}

/// Discover all changeset files for a database by listing S3 and parsing filenames.
///
/// HADBP format: `{prefix}{db_name}/{GGGG}/{seq:016x}.hadbp`
/// Generation 0000 = live incrementals, 0001+ = snapshots.
pub async fn discover_ltx_files(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Vec<DiscoveredLtx>> {
    let prefix = normalize_prefix(prefix);
    let db_prefix = format!("{}{}/", prefix, db_name);
    let keys = storage.list(&db_prefix, None).await?;
    let mut files = Vec::new();
    for key in &keys {
        let relative = match key.strip_prefix(&db_prefix) {
            Some(r) => r,
            None => continue,
        };
        let parts: Vec<&str> = relative.splitn(2, '/').collect();
        if parts.len() != 2 {
            continue;
        }
        let gen = match u64::from_str_radix(parts[0], 16) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let filename = parts[1];
        if !filename.ends_with(".hadbp") {
            continue;
        }
        let stem = &filename[..filename.len() - 6]; // strip ".hadbp"
        let seq = match u64::from_str_radix(stem, 16) {
            Ok(v) => v,
            Err(_) => continue,
        };
        files.push(DiscoveredLtx {
            key: key.clone(),
            seq,
            is_snapshot: gen > 0,
            generation: gen,
        });
    }
    files.sort_by_key(|f| f.seq);
    Ok(files)
}

/// Discover all database names by listing S3 objects under prefix and extracting
/// the first path component after the prefix.
pub async fn discover_databases(storage: &dyn StorageBackend, prefix: &str) -> Result<Vec<String>> {
    let prefix = normalize_prefix(prefix);
    let keys = storage.list(&prefix, None).await?;
    let mut dbs = std::collections::BTreeSet::new();
    for key in &keys {
        if let Some(relative) = key.strip_prefix(&prefix) {
            if let Some(name) = relative.split('/').next() {
                if !name.is_empty() {
                    dbs.insert(name.to_string());
                }
            }
        }
    }
    Ok(dbs.into_iter().collect())
}

// ============================================================================
// list
// ============================================================================

/// Database info returned by list.
#[derive(Debug)]
pub struct DatabaseInfo {
    pub name: String,
    pub max_seq: u64,
    pub incremental_count: usize,
    pub latest_snapshot: Option<SnapshotInfo>,
}

/// Snapshot metadata.
#[derive(Debug)]
pub struct SnapshotInfo {
    pub generation: u64,
    pub seq: u64,
}

/// List all databases in the S3 bucket under the given prefix, returning structured info.
pub async fn list_databases(
    storage: &dyn StorageBackend,
    prefix: &str,
) -> Result<Vec<DatabaseInfo>> {
    let dbs = discover_databases(storage, prefix).await?;
    let mut result = Vec::new();
    for db in dbs {
        let files = discover_ltx_files(storage, prefix, &db).await?;
        let max_seq = files.iter().map(|f| f.seq).max().unwrap_or(0);
        let incremental_count = files.iter().filter(|f| !f.is_snapshot).count();
        let latest_snapshot = files
            .iter()
            .filter(|f| f.is_snapshot)
            .max_by_key(|f| f.seq)
            .map(|s| SnapshotInfo {
                generation: s.generation,
                seq: s.seq,
            });
        result.push(DatabaseInfo {
            name: db,
            max_seq,
            incremental_count,
            latest_snapshot,
        });
    }
    Ok(result)
}

// ============================================================================
// verify
// ============================================================================

/// Result of verifying a single changeset file.
#[derive(Debug)]
pub enum VerifyFileResult {
    Ok { seq: u64, size_bytes: u64 },
    SeqMismatch { expected_seq: u64, header_seq: u64 },
    ChecksumFailed(String),
    DownloadFailed(String),
}

/// Result of verifying a database's backup chain.
#[derive(Debug)]
pub struct VerifyResult {
    pub total_files: usize,
    pub verified_count: usize,
    pub total_size: u64,
    pub file_results: Vec<(String, VerifyFileResult)>,
    pub continuity_issues: Vec<String>,
}

impl VerifyResult {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.file_results
            .iter()
            .all(|(_, r)| matches!(r, VerifyFileResult::Ok { .. }))
            && self.continuity_issues.is_empty()
    }
}

/// Verify integrity of all changeset files for a database.
///
/// Downloads each file, verifies HADBP checksums, and checks seq continuity
/// in the incremental chain.
pub async fn verify_database(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
) -> Result<VerifyResult> {
    let files = discover_ltx_files(storage, prefix, name).await?;
    if files.is_empty() {
        return Err(anyhow!("No changeset files found for database '{}'", name));
    }

    let snapshots = files.iter().filter(|f| f.is_snapshot).count();
    if snapshots == 0 {
        return Err(anyhow!(
            "No snapshot found. Backup incomplete, cannot verify."
        ));
    }

    let mut verified_count = 0usize;
    let mut total_size: u64 = 0;
    let mut file_results = Vec::new();

    for file in &files {
        let filename = file
            .key
            .split('/')
            .next_back()
            .unwrap_or(&file.key)
            .to_string();
        match storage
            .get(&file.key)
            .await
            .and_then(|opt| opt.ok_or_else(|| anyhow::anyhow!("key {} not found", file.key)))
        {
            Ok(data) => match walrust::hadb_changeset::physical::decode(&data) {
                Ok(changeset) => {
                    if changeset.header.seq != file.seq {
                        file_results.push((
                            filename,
                            VerifyFileResult::SeqMismatch {
                                expected_seq: file.seq,
                                header_seq: changeset.header.seq,
                            },
                        ));
                    } else {
                        verified_count += 1;
                        total_size += data.len() as u64;
                        file_results.push((
                            filename,
                            VerifyFileResult::Ok {
                                seq: file.seq,
                                size_bytes: data.len() as u64,
                            },
                        ));
                    }
                }
                Err(e) => {
                    file_results.push((filename, VerifyFileResult::ChecksumFailed(e.to_string())));
                }
            },
            Err(e) => {
                file_results.push((filename, VerifyFileResult::DownloadFailed(e.to_string())));
            }
        }
    }

    // Seq continuity check on incrementals: detect gaps
    let mut sorted_incr: Vec<_> = files.iter().filter(|f| !f.is_snapshot).collect();
    sorted_incr.sort_by_key(|f| f.seq);
    let mut continuity_issues = Vec::new();
    let mut expected_next: Option<u64> = None;
    for file in &sorted_incr {
        if let Some(expected) = expected_next {
            if file.seq > expected {
                continuity_issues.push(format!(
                    "Seq gap: expected {}, got {} (missing {}-{})",
                    expected,
                    file.seq,
                    expected,
                    file.seq - 1
                ));
            } else if file.seq < expected {
                continuity_issues.push(format!(
                    "Seq overlap: expected {}, got {} (duplicate)",
                    expected, file.seq
                ));
            }
        }
        expected_next = Some(file.seq + 1);
    }

    Ok(VerifyResult {
        total_files: files.len(),
        verified_count,
        total_size,
        file_results,
        continuity_issues,
    })
}

// ============================================================================
// compact
// ============================================================================

/// Result of a compaction plan.
#[derive(Debug)]
pub struct CompactPlan {
    pub keep_snapshots: Vec<DiscoveredLtx>,
    pub delete_snapshots: Vec<DiscoveredLtx>,
    pub delete_stale_incrementals: Vec<DiscoveredLtx>,
}

/// Plan compaction: which snapshots to keep and which to delete.
///
/// Also identifies stale incrementals (those older than the oldest kept snapshot)
/// that can safely be removed since they're unrestorable without a base snapshot.
pub async fn plan_compact(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
    keep_count: usize,
) -> Result<CompactPlan> {
    let files = discover_ltx_files(storage, prefix, name).await?;
    let mut snapshots: Vec<_> = files.iter().filter(|f| f.is_snapshot).cloned().collect();
    let incrementals: Vec<_> = files.iter().filter(|f| !f.is_snapshot).cloned().collect();

    if snapshots.is_empty() {
        return Ok(CompactPlan {
            keep_snapshots: Vec::new(),
            delete_snapshots: Vec::new(),
            delete_stale_incrementals: Vec::new(),
        });
    }

    // Sort by seq descending (newest first)
    snapshots.sort_by(|a, b| b.seq.cmp(&a.seq));

    let (keep_snapshots, delete_snapshots) = if snapshots.len() <= keep_count {
        (snapshots, Vec::new())
    } else {
        let delete = snapshots.split_off(keep_count);
        (snapshots, delete)
    };

    // Find stale incrementals: those with seq < oldest kept snapshot's seq.
    let oldest_kept_seq = keep_snapshots.last().map(|s| s.seq).unwrap_or(u64::MAX);
    let delete_stale_incrementals: Vec<_> = incrementals
        .into_iter()
        .filter(|i| i.seq < oldest_kept_seq)
        .collect();

    Ok(CompactPlan {
        keep_snapshots,
        delete_snapshots,
        delete_stale_incrementals,
    })
}

/// Execute compaction: delete old snapshots and stale incrementals from S3.
pub async fn execute_compact(storage: &dyn StorageBackend, plan: &CompactPlan) -> Result<usize> {
    let mut keys: Vec<String> = plan
        .delete_snapshots
        .iter()
        .map(|s| s.key.clone())
        .collect();
    keys.extend(plan.delete_stale_incrementals.iter().map(|s| s.key.clone()));
    if keys.is_empty() {
        return Ok(0);
    }
    storage.delete_many(&keys).await
}

// ============================================================================
// snapshot
// ============================================================================

/// Result of taking a snapshot.
#[derive(Debug)]
pub struct SnapshotResult {
    /// Seq of the newly created snapshot.
    pub seq: u64,
    /// Database name (derived from the file stem).
    pub db_name: String,
}

/// Take an immediate full snapshot of a database to S3.
pub async fn snapshot_database(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_path: &Path,
) -> Result<SnapshotResult> {
    if !db_path.exists() {
        return Err(anyhow!("Database not found: {}", db_path.display()));
    }

    let prefix = normalize_prefix(prefix);
    let mut state = walrust::SyncState::new(db_path.to_path_buf())?;
    let db_name = state.name.clone();

    // Discover current seq from S3 so the snapshot gets the right sequence number
    let files = discover_ltx_files(storage, &prefix, &db_name).await?;
    state.current_seq = files.iter().map(|f| f.seq).max().unwrap_or(0);

    // Initialize checksum from the database file
    state.init_checksum()?;

    walrust::sync::take_snapshot(storage, &prefix, &mut state).await?;

    Ok(SnapshotResult {
        seq: state.current_seq,
        db_name,
    })
}

// ============================================================================
// replicate
// ============================================================================

/// Load replica seq from state file, or 0 if none exists.
///
/// Logs at error level if the state file exists but cannot be parsed,
/// since this indicates corruption (not a normal "first run" scenario).
pub fn load_replica_state(local: &Path) -> u64 {
    let state_path = local.with_extension("db-replica-state");
    if !state_path.exists() {
        return 0;
    }
    let data = match std::fs::read_to_string(&state_path) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(
                "Failed to read replica state file {}: {}",
                state_path.display(),
                e
            );
            return 0;
        }
    };
    match serde_json::from_str::<serde_json::Value>(&data) {
        Ok(state) => {
            // Try new field name first, fall back to old for migration
            state["current_seq"]
                .as_u64()
                .or_else(|| state["current_txid"].as_u64())
                .unwrap_or_else(|| {
                    tracing::error!(
                        "Replica state file {} missing current_seq field",
                        state_path.display()
                    );
                    0
                })
        }
        Err(e) => {
            tracing::error!(
                "Failed to parse replica state file {}: {}",
                state_path.display(),
                e
            );
            0
        }
    }
}

/// Save current replica seq to state file.
pub fn save_replica_state(local: &Path, current_seq: u64) -> Result<()> {
    let state_path = local.with_extension("db-replica-state");
    let state = serde_json::json!({
        "current_seq": current_seq,
        "last_updated": Utc::now().to_rfc3339(),
    });
    std::fs::write(&state_path, serde_json::to_string_pretty(&state)?)?;
    Ok(())
}

/// Run as a read replica: bootstrap from S3 if needed, then poll for incremental updates.
///
/// Polls S3 every `interval` for new changeset files and applies them to the local database.
/// Uses exponential backoff on consecutive failures with escalating log severity.
/// Shuts down gracefully on SIGTERM/SIGINT.
pub async fn replicate_database(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    local: &Path,
    interval: Duration,
) -> Result<()> {
    let prefix = normalize_prefix(prefix);
    let mut current_seq = load_replica_state(local);

    if current_seq == 0 || !local.exists() {
        tracing::info!("Bootstrapping replica from S3...");
        current_seq = walrust::sync::restore(storage, &prefix, db_name, local, None).await?;
        save_replica_state(local, current_seq)?;
    }

    tracing::info!(
        "Replicating '{}' -> {} (starting at seq {})",
        db_name,
        local.display(),
        current_seq
    );

    let mut consecutive_failures: u32 = 0;
    let max_backoff = Duration::from_secs(60);

    let shutdown = hadb_cli::shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("Received shutdown signal, stopping replication.");
                return Ok(());
            }
            result = walrust::sync::pull_incremental(storage, &prefix, db_name, local, current_seq) => {
                match result {
                    Ok(new_seq) => {
                        if consecutive_failures > 0 {
                            tracing::info!("Replication recovered after {} failures", consecutive_failures);
                        }
                        consecutive_failures = 0;

                        if new_seq > current_seq {
                            tracing::info!(
                                "Pulled seq {} -> {}",
                                current_seq,
                                new_seq
                            );
                            current_seq = new_seq;
                            save_replica_state(local, current_seq)?;
                        }
                        tokio::time::sleep(interval).await;
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        let backoff = interval
                            .mul_f64(2.0_f64.powi(consecutive_failures.min(6) as i32))
                            .min(max_backoff);

                        if consecutive_failures <= 3 {
                            tracing::warn!(
                                "Replication poll failed (attempt {}): {}",
                                consecutive_failures,
                                e
                            );
                        } else {
                            tracing::error!(
                                "Replication poll failed (attempt {}, backoff {:?}): {}",
                                consecutive_failures,
                                backoff,
                                e
                            );
                        }

                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }
    }
}
