use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

/// Result of a CAS (compare-and-swap) write operation.
#[derive(Debug, Clone)]
pub struct CasResult {
    /// true if the write succeeded, false if precondition failed.
    pub success: bool,
    /// New etag if the write succeeded.
    pub etag: Option<String>,
}

/// Trait for CAS lease operations on a key-value store.
///
/// Separate from walrust's `StorageBackend` (which handles WAL data, not leases).
/// haqlite ships `S3LeaseStore` for production and `InMemoryLeaseStore` for tests.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Read a key, returning (data, etag). None if key doesn't exist.
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>>;

    /// Write only if key doesn't exist (create). CAS.
    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult>;

    /// Write only if current etag matches (update). CAS.
    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult>;

    /// Delete a key (best-effort for lease release).
    async fn delete(&self, key: &str) -> Result<()>;
}

// ============================================================================
// S3LeaseStore
// ============================================================================

/// LeaseStore backed by S3 conditional PUTs.
///
/// Uses `if_none_match("*")` for create and `if_match(etag)` for update.
/// Handles 412 PreconditionFailed detection.
pub struct S3LeaseStore {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3LeaseStore {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    /// Check if an S3 error is a 412 PreconditionFailed (CAS conflict).
    ///
    /// Uses proper SDK error metadata instead of fragile string matching.
    /// Checks error code first (most reliable), then HTTP status code as fallback.
    fn is_precondition_failed(
        err: &aws_sdk_s3::error::SdkError<impl ProvideErrorMetadata + std::fmt::Debug>,
    ) -> bool {
        // Check error code from service error metadata.
        if let Some(service_err) = err.as_service_error() {
            if let Some(code) = service_err.code() {
                if code == "PreconditionFailed" || code == "ConditionalRequestConflict" {
                    return true;
                }
            }
        }
        // Fallback: check HTTP status code.
        if let Some(raw) = err.raw_response() {
            if raw.status().as_u16() == 412 {
                return true;
            }
        }
        false
    }

    /// Check if an S3 error is a 404 NoSuchKey (key doesn't exist).
    fn is_not_found(
        err: &aws_sdk_s3::error::SdkError<impl ProvideErrorMetadata + std::fmt::Debug>,
    ) -> bool {
        if let Some(service_err) = err.as_service_error() {
            if let Some(code) = service_err.code() {
                if code == "NoSuchKey" || code == "NotFound" {
                    return true;
                }
            }
        }
        if let Some(raw) = err.raw_response() {
            if raw.status().as_u16() == 404 {
                return true;
            }
        }
        false
    }
}

#[async_trait]
impl LeaseStore for S3LeaseStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(output) => {
                let etag = output
                    .e_tag()
                    .ok_or_else(|| anyhow!("S3 GetObject returned no ETag"))?
                    .to_string();
                let body = output.body.collect().await?.into_bytes().to_vec();
                Ok(Some((body, etag)))
            }
            Err(e) => {
                if Self::is_not_found(&e) {
                    Ok(None)
                } else {
                    Err(anyhow!("S3 GetObject failed: {}", e))
                }
            }
        }
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .if_none_match("*")
            .send()
            .await;

        match result {
            Ok(output) => Ok(CasResult {
                success: true,
                etag: output.e_tag().map(|s| s.to_string()),
            }),
            Err(e) if Self::is_precondition_failed(&e) => Ok(CasResult {
                success: false,
                etag: None,
            }),
            Err(e) => Err(anyhow!("S3 PutObject (if-none-match) failed: {}", e)),
        }
    }

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .if_match(etag)
            .send()
            .await;

        match result {
            Ok(output) => Ok(CasResult {
                success: true,
                etag: output.e_tag().map(|s| s.to_string()),
            }),
            Err(e) if Self::is_precondition_failed(&e) => Ok(CasResult {
                success: false,
                etag: None,
            }),
            Err(e) => Err(anyhow!("S3 PutObject (if-match) failed: {}", e)),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow!("S3 DeleteObject failed: {}", e))?;
        Ok(())
    }
}

// ============================================================================
// InMemoryLeaseStore (for tests)
// ============================================================================

struct Entry {
    data: Vec<u8>,
    etag: String,
}

/// In-memory LeaseStore for tests. Simulates CAS correctly with monotonic etags.
pub struct InMemoryLeaseStore {
    store: Mutex<HashMap<String, Entry>>,
    etag_counter: AtomicU64,
}

impl InMemoryLeaseStore {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            etag_counter: AtomicU64::new(1),
        }
    }

    fn next_etag(&self) -> String {
        let n = self.etag_counter.fetch_add(1, Ordering::SeqCst);
        format!("\"etag-{}\"", n)
    }
}

impl Default for InMemoryLeaseStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LeaseStore for InMemoryLeaseStore {
    async fn read(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let store = self.store.lock().await;
        Ok(store.get(key).map(|e| (e.data.clone(), e.etag.clone())))
    }

    async fn write_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<CasResult> {
        let mut store = self.store.lock().await;
        if store.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        let etag = self.next_etag();
        store.insert(
            key.to_string(),
            Entry {
                data,
                etag: etag.clone(),
            },
        );
        Ok(CasResult {
            success: true,
            etag: Some(etag),
        })
    }

    async fn write_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<CasResult> {
        let mut store = self.store.lock().await;
        match store.get(key) {
            Some(entry) if entry.etag == etag => {
                let new_etag = self.next_etag();
                store.insert(
                    key.to_string(),
                    Entry {
                        data,
                        etag: new_etag.clone(),
                    },
                );
                Ok(CasResult {
                    success: true,
                    etag: Some(new_etag),
                })
            }
            _ => Ok(CasResult {
                success: false,
                etag: None,
            }),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.store.lock().await.remove(key);
        Ok(())
    }
}
