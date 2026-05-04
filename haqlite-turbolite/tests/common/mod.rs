//! Shared test utilities for haqlite integration tests.
//!
//! `InMemoryStorage` implements the byte-level `hadb_storage::StorageBackend`
//! trait (which walrust now consumes). It adds two test-only helpers
//! `insert()` and `keys()` that let tests seed the store and inspect its
//! contents directly; production callers use `put` / `list` through the
//! trait.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use tokio::sync::Mutex;

#[cfg(feature = "legacy-s3-mode-tests")]
static S3_VFS_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

#[cfg(feature = "s3")]
pub fn s3_env_available() -> bool {
    std::env::var("TIERED_TEST_BUCKET").is_ok()
}

#[cfg(feature = "s3")]
pub fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET").expect("TIERED_TEST_BUCKET required")
}

#[cfg(feature = "s3")]
pub fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL_S3"))
        .ok()
}

#[cfg(feature = "s3")]
pub async fn s3_backend(s3_prefix: &str) -> Arc<dyn hadb_storage::StorageBackend> {
    let storage = hadb_storage_s3::S3Storage::from_env(test_bucket(), endpoint_url().as_deref())
        .await
        .expect("create S3 storage")
        .with_prefix(s3_prefix);
    Arc::new(storage)
}

#[cfg(feature = "legacy-s3-mode-tests")]
pub async fn make_s3_vfs(
    cache_dir: &std::path::Path,
    vfs_prefix: &str,
    s3_prefix: &str,
    compression_level: i32,
    encryption_key: Option<[u8; 32]>,
) -> (
    turbolite::tiered::SharedTurboliteVfs,
    String,
    Arc<dyn hadb_storage::StorageBackend>,
) {
    let n = S3_VFS_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let vfs_name = format!("{}_{}", vfs_prefix, n);
    let config = turbolite::tiered::TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        cache: turbolite::tiered::CacheConfig {
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            ..Default::default()
        },
        compression: turbolite::tiered::CompressionConfig {
            level: compression_level,
            ..Default::default()
        },
        encryption: turbolite::tiered::EncryptionConfig {
            key: encryption_key,
        },
        ..Default::default()
    };
    let backend = s3_backend(s3_prefix).await;
    let vfs = turbolite::tiered::TurboliteVfs::with_backend(
        config,
        backend.clone(),
        tokio::runtime::Handle::current(),
    )
    .expect("create S3-backed VFS");
    let shared_vfs = turbolite::tiered::SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).expect("register VFS");
    (shared_vfs, vfs_name, backend)
}

pub struct InMemoryStorage {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
        }
    }

    /// Insert a key/value directly (test setup).
    pub async fn insert(&self, key: &str, data: Vec<u8>) {
        self.objects.lock().await.insert(key.to_string(), data);
    }

    /// Return all keys sorted (test assertions).
    pub async fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.objects.lock().await.keys().cloned().collect();
        keys.sort();
        keys
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().await.get(key).cloned())
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.objects
            .lock()
            .await
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().await.remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let map = self.objects.lock().await;
        let mut keys: Vec<String> = map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().await.contains_key(key))
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut map = self.objects.lock().await;
        if map.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        map.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".into()),
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
        let mut map = self.objects.lock().await;
        if !map.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        map.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".into()),
        })
    }
}

/// Wrapper around `InMemoryStorage` that can simulate page-group
/// objects becoming temporarily unavailable for `get()` (e.g., a
/// leader publish-then-upload race or a churn re-key window).
///
/// `paused == true` causes every `get()` to return `Ok(None)` as if
/// the object weren't there. Other methods delegate normally so the
/// leader's writes (`put`) and the follower's manifest-store reads
/// (which go through `ManifestStore`, not `StorageBackend`) keep
/// flowing.
pub struct PausableStorage {
    inner: Arc<InMemoryStorage>,
    pub paused: Arc<AtomicBool>,
}

impl PausableStorage {
    pub fn new(inner: Arc<InMemoryStorage>) -> Self {
        Self {
            inner,
            paused: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn unpause(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for PausableStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if self.paused.load(Ordering::SeqCst) {
            return Ok(None);
        }
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        if self.paused.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        if self.paused.load(Ordering::SeqCst) {
            return Ok(false);
        }
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.inner.put_if_match(key, data, etag).await
    }
}
