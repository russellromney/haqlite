//! haqlite CLI — HA SQLite server and management tools.
//!
//! ```text
//! haqlite serve       # Run HA SQLite server
//! haqlite restore     # Restore from S3
//! haqlite list        # List databases in S3
//! haqlite verify      # Verify backup integrity
//! haqlite compact     # Compact snapshots (retention)
//! haqlite replicate   # Run as read replica
//! haqlite snapshot    # Take immediate snapshot
//! haqlite explain     # Show resolved config
//! ```

use std::process::ExitCode;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use hadb_cli::{
    CliBackend, CompactArgs, ListArgs, ReplicateArgs, RestoreArgs, SharedConfig, SnapshotArgs,
    VerifyArgs,
};
use haqlite::cli_config::HaqliteConfig;
use haqlite::ops::{self, VerifyFileResult};

/// Default S3 key prefix for haqlite. Matches the default in ServeConfig.
const DEFAULT_PREFIX: &str = "haqlite/";

/// Resolve the effective prefix: use the CLI arg if non-empty, otherwise the haqlite default.
fn resolve_prefix(s3: &hadb_cli::S3Args) -> &str {
    if s3.prefix.is_empty() {
        DEFAULT_PREFIX
    } else {
        &s3.prefix
    }
}

struct HaqliteBackend;

#[async_trait]
impl CliBackend for HaqliteBackend {
    type Config = HaqliteConfig;

    fn product_name(&self) -> &'static str {
        "haqlite"
    }

    fn config_filename(&self) -> &'static str {
        "haqlite.toml"
    }

    async fn serve(&self, shared: &SharedConfig, product: &HaqliteConfig) -> Result<()> {
        let serve_config = product
            .serve
            .as_ref()
            .ok_or_else(|| anyhow!("missing [serve] section in config"))?;
        haqlite::serve::run(shared, serve_config).await
    }

    async fn restore(&self, _shared: &SharedConfig, args: &RestoreArgs) -> Result<()> {
        let prefix = resolve_prefix(&args.s3);
        let storage = hadb_storage_s3::S3Storage::from_env(
            args.s3.bucket.clone(),
            args.s3.endpoint.as_deref(),
        )
        .await?;
        let txid = walrust::sync::restore(
            &storage,
            prefix,
            &args.name,
            &args.output,
            args.point_in_time.as_deref(),
        )
        .await?;
        println!(
            "Restored '{}' to {} (TXID {})",
            args.name,
            args.output.display(),
            txid
        );
        Ok(())
    }

    async fn list(&self, _shared: &SharedConfig, args: &ListArgs) -> Result<()> {
        let prefix = resolve_prefix(&args.s3);
        let storage = hadb_storage_s3::S3Storage::from_env(
            args.s3.bucket.clone(),
            args.s3.endpoint.as_deref(),
        )
        .await?;
        let dbs = ops::list_databases(&storage, prefix).await?;
        if dbs.is_empty() {
            println!("No databases found.");
            return Ok(());
        }
        println!("Databases:");
        for db in &dbs {
            let snapshot_info = match &db.latest_snapshot {
                Some(s) => format!("snapshot gen {} (seq {})", s.generation, s.seq),
                None => "no snapshot".to_string(),
            };
            println!(
                "  {} (seq: {}, {} incrementals, {})",
                db.name, db.max_seq, db.incremental_count, snapshot_info
            );
        }
        Ok(())
    }

    async fn verify(&self, _shared: &SharedConfig, args: &VerifyArgs) -> Result<()> {
        let prefix = resolve_prefix(&args.s3);
        let storage = hadb_storage_s3::S3Storage::from_env(
            args.s3.bucket.clone(),
            args.s3.endpoint.as_deref(),
        )
        .await?;
        let result = ops::verify_database(&storage, prefix, &args.name).await?;

        println!("Verifying '{}': {} files", args.name, result.total_files);
        println!();

        for (filename, file_result) in &result.file_results {
            match file_result {
                VerifyFileResult::Ok { seq, size_bytes } => {
                    println!(
                        "  OK   {} (seq {}, {:.1} KB)",
                        filename,
                        seq,
                        *size_bytes as f64 / 1024.0
                    );
                }
                VerifyFileResult::SeqMismatch {
                    expected_seq,
                    header_seq,
                } => {
                    println!(
                        "  WARN {} seq mismatch: filename {}, header {}",
                        filename, expected_seq, header_seq
                    );
                }
                VerifyFileResult::ChecksumFailed(e) => {
                    println!("  FAIL {} checksum: {}", filename, e);
                }
                VerifyFileResult::DownloadFailed(e) => {
                    println!("  FAIL {} download: {}", filename, e);
                }
            }
        }

        for issue in &result.continuity_issues {
            println!("  WARN {}", issue);
        }

        println!();
        println!(
            "Verified: {}/{} files ({:.1} KB)",
            result.verified_count,
            result.total_files,
            result.total_size as f64 / 1024.0
        );

        if result.is_valid() {
            println!("All checks passed.");
            Ok(())
        } else {
            let issue_count = result
                .file_results
                .iter()
                .filter(|(_, r)| !matches!(r, VerifyFileResult::Ok { .. }))
                .count()
                + result.continuity_issues.len();
            println!("{} issue(s) found.", issue_count);
            Err(anyhow!("Integrity issues detected"))
        }
    }

    async fn compact(&self, _shared: &SharedConfig, args: &CompactArgs) -> Result<()> {
        let prefix = resolve_prefix(&args.s3);
        let storage = hadb_storage_s3::S3Storage::from_env(
            args.s3.bucket.clone(),
            args.s3.endpoint.as_deref(),
        )
        .await?;
        let plan = ops::plan_compact(&storage, prefix, &args.name, args.keep).await?;

        if plan.keep_snapshots.is_empty() && plan.delete_snapshots.is_empty() {
            println!("No snapshots found for '{}'.", args.name);
            return Ok(());
        }

        let total_deletes = plan.delete_snapshots.len() + plan.delete_stale_incrementals.len();
        if total_deletes == 0 {
            println!(
                "Only {} snapshots exist (keeping {}). Nothing to delete.",
                plan.keep_snapshots.len(),
                args.keep
            );
            return Ok(());
        }

        println!("Compaction plan for '{}':", args.name);
        println!("  Keeping {} snapshots:", plan.keep_snapshots.len());
        for s in &plan.keep_snapshots {
            let filename = s.key.split('/').next_back().unwrap_or(&s.key);
            println!("    {} (gen {}, seq {})", filename, s.generation, s.seq);
        }
        if !plan.delete_snapshots.is_empty() {
            println!("  Deleting {} snapshots:", plan.delete_snapshots.len());
            for s in &plan.delete_snapshots {
                let filename = s.key.split('/').next_back().unwrap_or(&s.key);
                println!("    {} (gen {}, seq {})", filename, s.generation, s.seq);
            }
        }
        if !plan.delete_stale_incrementals.is_empty() {
            println!(
                "  Deleting {} stale incrementals (older than oldest kept snapshot):",
                plan.delete_stale_incrementals.len()
            );
            for s in &plan.delete_stale_incrementals {
                let filename = s.key.split('/').next_back().unwrap_or(&s.key);
                println!("    {} (seq {})", filename, s.seq);
            }
        }

        if !args.force {
            println!();
            println!("Dry-run mode: no files deleted. Use --force to actually delete.");
            return Ok(());
        }

        let deleted = ops::execute_compact(&storage, &plan).await?;
        println!();
        println!("Compaction complete: deleted {} files.", deleted);
        Ok(())
    }

    async fn replicate(&self, _shared: &SharedConfig, args: &ReplicateArgs) -> Result<()> {
        let prefix = resolve_prefix(&args.s3);
        let storage = hadb_storage_s3::S3Storage::from_env(
            args.s3.bucket.clone(),
            args.s3.endpoint.as_deref(),
        )
        .await?;
        println!(
            "Replicating '{}' -> {} (interval: {}s)",
            args.source,
            args.local.display(),
            args.interval
        );
        println!("Press Ctrl+C to stop");
        println!();
        ops::replicate_database(
            &storage,
            prefix,
            &args.source,
            &args.local,
            Duration::from_secs(args.interval),
        )
        .await
    }

    async fn snapshot(&self, _shared: &SharedConfig, args: &SnapshotArgs) -> Result<()> {
        let prefix = resolve_prefix(&args.s3);
        let storage = hadb_storage_s3::S3Storage::from_env(
            args.s3.bucket.clone(),
            args.s3.endpoint.as_deref(),
        )
        .await?;
        let result = ops::snapshot_database(&storage, prefix, &args.database).await?;
        println!(
            "Snapshot uploaded for '{}' (seq {})",
            result.db_name, result.seq
        );
        Ok(())
    }

    async fn explain(&self, shared: &SharedConfig, product: &HaqliteConfig) -> Result<()> {
        println!("=== haqlite Configuration ===\n");
        println!("S3:");
        println!("  Bucket:    {}", shared.s3.bucket);
        println!(
            "  Endpoint:  {}",
            shared.s3.endpoint.as_deref().unwrap_or("(default)")
        );
        println!("\nLease:");
        println!("  TTL:       {}s", shared.lease.ttl_secs);
        println!("  Renew:     {}ms", shared.lease.renew_interval_ms);
        println!("  Poll:      {}ms", shared.lease.poll_interval_ms);
        println!("\nRetention:");
        println!("  Keep:      {} snapshots", shared.retention.keep);
        if let Some(serve) = &product.serve {
            println!("\nServe:");
            println!("  DB path:          {}", serve.db_path.display());
            println!("  Port:             {}", serve.port);
            println!("  Forwarding port:  {}", serve.forwarding_port);
            println!("  Prefix:           {}", serve.prefix);
            println!("  Sync interval:    {}ms", serve.sync_interval_ms);
            println!("  Follower pull:    {}ms", serve.follower_pull_ms);
            if serve.secret.is_some() {
                println!("  Secret:           (set)");
            }
        } else {
            println!("\nServe: (not configured)");
        }
        Ok(())
    }
}

fn main() -> ExitCode {
    hadb_cli::run_cli(HaqliteBackend)
}
