//! haqlite CLI — HA SQLite server and management tools.
//!
//! ```text
//! haqlite serve       # Run HA SQLite server
//! haqlite restore     # Restore from S3
//! haqlite list        # List databases in S3
//! haqlite verify      # Verify backup integrity
//! haqlite compact     # Compact snapshots (GFS retention)
//! haqlite replicate   # Run as read replica
//! haqlite snapshot    # Take immediate snapshot
//! haqlite explain     # Show resolved config
//! ```

use std::process::ExitCode;

use anyhow::Result;
use async_trait::async_trait;

use hadb_cli::{CliBackend, RestoreArgs, SharedConfig};
use haqlite::cli_config::HaqliteConfig;

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
            .ok_or_else(|| anyhow::anyhow!("missing [serve] section in config"))?;
        haqlite::serve::run(shared, serve_config).await
    }

    async fn restore(&self, _shared: &SharedConfig, args: &RestoreArgs) -> Result<()> {
        let storage = walrust::S3Backend::from_env(
            args.s3.bucket.clone(),
            args.s3.endpoint.as_deref(),
        )
        .await?;

        let txid = walrust::sync::restore(
            &storage,
            "",
            &args.name,
            &args.output,
            args.point_in_time.as_deref(),
        )
        .await?;

        tracing::info!("restored '{}' to TXID {txid}", args.name);
        Ok(())
    }
}

fn main() -> ExitCode {
    hadb_cli::run_cli(HaqliteBackend)
}
