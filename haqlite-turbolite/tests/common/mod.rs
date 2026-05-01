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
