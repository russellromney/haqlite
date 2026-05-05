use std::path::Path;

use anyhow::{Context, Result};

/// Detects manifest epoch mismatches between local cache and remote storage.
/// Wipes the local cache when a fork or rollback is detected.
pub struct RollbackDetector {
    client: reqwest::Client,
    storage_url: String,
    token: String,
}

impl RollbackDetector {
    pub fn new(storage_url: &str, token: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client for rollback detection");
        Self::with_client(client, storage_url, token)
    }

    pub fn with_client(client: reqwest::Client, storage_url: &str, token: &str) -> Self {
        Self {
            client,
            storage_url: storage_url.to_string(),
            token: token.to_string(),
        }
    }

    pub async fn check_and_repair(&self, database_id: &str, db_path: &Path) -> Result<bool> {
        let parent = db_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("db_path has no parent: {}", db_path.display()))?;
        let cache_dir = parent.join(format!(".tl_cache_{}", database_id));
        let local_manifest_path = cache_dir.join("manifest.msgpack");

        let local_epoch_opt = match std::fs::read(&local_manifest_path) {
            Ok(bytes) => {
                Some(decode_manifest_epoch(&bytes).context("decode local manifest epoch")?)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!(
                    "read local manifest {}",
                    local_manifest_path.display()
                )))
            }
        };

        let (remote_bytes, remote_epoch) = match self.fetch_remote_manifest().await {
            Ok(Some((bytes, epoch))) => (bytes, epoch),
            Ok(None) => {
                tracing::info!(
                    database_id = %database_id,
                    has_local = local_epoch_opt.is_some(),
                    "remote manifest absent, nothing to align",
                );
                return Ok(false);
            }
            Err(e) => {
                tracing::warn!(
                    database_id = %database_id,
                    error = %e,
                    "remote manifest fetch failed, skipping align",
                );
                return Ok(false);
            }
        };

        match local_epoch_opt {
            Some(local_epoch) if local_epoch == remote_epoch => Ok(false),
            Some(local_epoch) => {
                tracing::warn!(
                    database_id = %database_id,
                    local_epoch,
                    remote_epoch,
                    "manifest epoch mismatch, wiping local cache (fork/rollback detected)",
                );
                remove_path_if_exists(&cache_dir)
                    .with_context(|| format!("wipe cache dir {}", cache_dir.display()))?;
                remove_path_if_exists(db_path)
                    .with_context(|| format!("wipe SQLite stub {}", db_path.display()))?;
                let shm_path = db_path.with_extension("db-shm");
                remove_path_if_exists(&shm_path)
                    .with_context(|| format!("wipe SQLite shm {}", shm_path.display()))?;
                seed_remote_manifest(&cache_dir, &local_manifest_path, &remote_bytes)?;
                tracing::info!(
                    database_id = %database_id,
                    bytes = remote_bytes.len(),
                    "wiped + pre-seeded remote manifest",
                );
                Ok(true)
            }
            None => {
                tracing::info!(
                    database_id = %database_id,
                    remote_epoch,
                    bytes = remote_bytes.len(),
                    "no local manifest; pre-seeding remote so turbolite opens aligned",
                );
                remove_path_if_exists(db_path)
                    .with_context(|| format!("wipe SQLite stub {}", db_path.display()))?;
                let shm_path = db_path.with_extension("db-shm");
                remove_path_if_exists(&shm_path)
                    .with_context(|| format!("wipe SQLite shm {}", shm_path.display()))?;
                seed_remote_manifest(&cache_dir, &local_manifest_path, &remote_bytes)?;
                Ok(true)
            }
        }
    }

    async fn fetch_remote_manifest(&self) -> Result<Option<(Vec<u8>, u64)>> {
        let url = format!(
            "{}/v1/sync/pages/manifest.msgpack",
            self.storage_url.trim_end_matches('/')
        );
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("grabby /v1/sync/pages/manifest.msgpack GET failed")?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "grabby /v1/sync/pages/manifest.msgpack returned {}: {}",
                status,
                text
            );
        }
        let bytes = resp
            .bytes()
            .await
            .context("read grabby manifest body")?
            .to_vec();
        let epoch = decode_manifest_epoch(&bytes)?;
        Ok(Some((bytes, epoch)))
    }
}

fn decode_manifest_epoch(bytes: &[u8]) -> Result<u64> {
    let manifest: turbolite::tiered::Manifest =
        rmp_serde::from_slice(bytes).context("deserialize turbolite manifest")?;
    Ok(manifest.epoch)
}

fn remove_path_if_exists(path: &Path) -> std::io::Result<()> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn seed_remote_manifest(cache_dir: &Path, manifest_path: &Path, bytes: &[u8]) -> Result<()> {
    std::fs::create_dir_all(cache_dir)
        .with_context(|| format!("create cache dir {}", cache_dir.display()))?;
    std::fs::write(manifest_path, bytes)
        .with_context(|| format!("pre-seed remote manifest at {}", manifest_path.display()))?;
    Ok(())
}
