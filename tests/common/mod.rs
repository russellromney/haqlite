//! Shared test utilities for haqlite integration tests.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;

/// In-memory walrust StorageBackend for tests.
///
/// Avoids S3 dependencies. All data stored in a HashMap behind a Mutex.
pub struct InMemoryStorage {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
        }
    }

    /// Insert a key-value pair directly (for test setup).
    pub async fn insert(&self, key: &str, data: Vec<u8>) {
        self.objects.lock().await.insert(key.to_string(), data);
    }

    /// Get all keys sorted (for test assertions).
    pub async fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.objects.lock().await.keys().cloned().collect();
        keys.sort();
        keys
    }
}

#[async_trait]
impl walrust::StorageBackend for InMemoryStorage {
    fn bucket_name(&self) -> &str {
        "test-bucket"
    }

    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.objects.lock().await.insert(key.to_string(), data);
        Ok(())
    }

    async fn upload_bytes_with_checksum(
        &self,
        key: &str,
        data: Vec<u8>,
        _checksum: &str,
    ) -> Result<()> {
        self.upload_bytes(key, data).await
    }

    async fn upload_file(&self, key: &str, path: &std::path::Path) -> Result<()> {
        let data = tokio::fs::read(path).await?;
        self.upload_bytes(key, data).await
    }

    async fn upload_file_with_checksum(
        &self,
        key: &str,
        path: &std::path::Path,
        _checksum: &str,
    ) -> Result<()> {
        self.upload_file(key, path).await
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.objects
            .lock()
            .await
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Key not found: {}", key))
    }

    async fn download_file(&self, key: &str, path: &std::path::Path) -> Result<()> {
        let data = self.download_bytes(key).await?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, data).await?;
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self
            .objects
            .lock()
            .await
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn list_objects_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self
            .objects
            .lock()
            .await
            .keys()
            .filter(|k| k.starts_with(prefix) && k.as_str() > start_after)
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().await.contains_key(key))
    }

    async fn get_checksum(&self, _key: &str) -> Result<Option<String>> {
        Ok(None)
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.objects.lock().await.remove(key);
        Ok(())
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        let mut objects = self.objects.lock().await;
        let mut deleted = 0;
        for key in keys {
            if objects.remove(key).is_some() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}
