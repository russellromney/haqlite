//! HTTP `PageStorage` adapter for turbolite.
//!
//! This adapter lives in haqlite (not turbolite) because it knows about HA
//! concerns that turbolite shouldn't: fence tokens, Bearer auth, and the
//! specific `/v1/sync/pages/` API contract. turbolite exposes the
//! `PageStorage` trait; we implement it here and plug it in via
//! `TurboliteVfs::new_with_storage`.
//!
//! # Fence-token contract
//!
//! Every write carries a `Fence-Token` header matching the writer's current
//! lease revision. If the shared fence token is 0 (lease not yet acquired,
//! or lost) we refuse to write rather than silently issuing fence-less PUTs
//! that a server might accept and then reject for a former-leader.
//!
//! # Server contract
//!
//! Speaks turbolite-style relative keys (`manifest.msgpack`, `p/d/0_v1`,
//! `p/it/3_v5`, ...) against a server that scopes keys by Bearer token.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::runtime::Handle as TokioHandle;
use turbolite::tiered::{Manifest, PageStorage};

/// HTTP-backed `PageStorage` with fence-token fencing on writes.
pub struct HttpPageStorage {
    client: reqwest::Client,
    endpoint: String,
    token: String,
    runtime: TokioHandle,
    /// Shared fence token: updated by the lease renewal loop. Writes use this
    /// value as the `Fence-Token` header; zero means "no active lease" and
    /// blocks writes.
    fence_token: Arc<AtomicU64>,
    fetch_count: AtomicU64,
    fetch_bytes: AtomicU64,
}

impl HttpPageStorage {
    pub fn new(
        endpoint: &str,
        token: &str,
        runtime: TokioHandle,
        fence_token: Arc<AtomicU64>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            runtime,
            fence_token,
            fetch_count: AtomicU64::new(0),
            fetch_bytes: AtomicU64::new(0),
        }
    }

    fn url(&self, key: &str) -> String {
        format!("{}/v1/sync/pages/{}", self.endpoint, key)
    }

    fn block_on<F: std::future::Future<Output = T>, T>(handle: &TokioHandle, fut: F) -> T {
        match TokioHandle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => handle.block_on(fut),
        }
    }

    fn require_fence_header(&self) -> io::Result<String> {
        let fence = self.fence_token.load(Ordering::SeqCst);
        if fence == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Fence-Token is 0 (no active lease); refusing to write",
            ));
        }
        Ok(fence.to_string())
    }

    async fn get_object_async(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let mut retries = 0u32;
        loop {
            let resp = self
                .client
                .get(&self.url(key))
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HTTP GET {}: {}", key, e)))?;

            match resp.status().as_u16() {
                200 => {
                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                404 => return Ok(None),
                status => {
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("HTTP GET {} returned {} after 3 retries", key, status),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << retries))).await;
                }
            }
        }
    }

    async fn range_get_async(
        &self,
        key: &str,
        start: u64,
        len: u32,
    ) -> io::Result<Option<Vec<u8>>> {
        let range = format!("bytes={}-{}", start, start + len as u64 - 1);
        let mut retries = 0u32;
        loop {
            let resp = self
                .client
                .get(&self.url(key))
                .bearer_auth(&self.token)
                .header("Range", &range)
                .send()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HTTP range GET {}: {}", key, e)))?;

            match resp.status().as_u16() {
                200 | 206 => {
                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                404 => return Ok(None),
                status => {
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "HTTP range GET {} ({}) returned {} after 3 retries",
                                key, range, status
                            ),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << retries))).await;
                }
            }
        }
    }

    async fn put_page_groups_async(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        if groups.is_empty() {
            return Ok(());
        }
        let fence = self.require_fence_header()?;
        let mut handles = Vec::with_capacity(groups.len());
        for (key, data) in groups {
            let client = self.client.clone();
            let url = self.url(key);
            let token = self.token.clone();
            let data = data.clone();
            let fence = fence.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    let req = client
                        .put(&url)
                        .bearer_auth(&token)
                        .header("Fence-Token", fence.as_str())
                        .body(data.clone());
                    let resp = req
                        .send()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    if resp.status().is_success() {
                        return Ok::<_, io::Error>(data.len() as u64);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("HTTP PUT failed after 3 retries: {}", resp.status()),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << retries))).await;
                }
            }));
        }
        for handle in handles {
            match handle.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            }
        }
        Ok(())
    }

    async fn delete_objects_async(&self, keys: &[String]) -> io::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let fence = self.require_fence_header()?;
        let url = format!("{}/v1/sync/pages/_delete", self.endpoint);
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .header("Fence-Token", &fence)
            .json(&serde_json::json!({ "keys": keys.to_vec() }))
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HTTP batch delete: {}", e)))?;
        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("HTTP batch delete returned {}", resp.status()),
            ));
        }
        Ok(())
    }

    async fn get_manifest_async(&self) -> io::Result<Option<Manifest>> {
        // turbolite stores its manifest as a regular object at manifest.msgpack.
        match self.get_object_async("manifest.msgpack").await? {
            Some(bytes) => {
                let mut manifest: Manifest = rmp_serde::from_slice(&bytes).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid manifest msgpack: {}", e),
                    )
                })?;
                manifest.build_page_index();
                Ok(Some(manifest))
            }
            None => {
                // Legacy JSON fallback for databases from before msgpack.
                match self.get_object_async("manifest.json").await? {
                    Some(bytes) => {
                        let mut manifest: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Invalid manifest JSON: {}", e),
                            )
                        })?;
                        manifest.build_page_index();
                        Ok(Some(manifest))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    async fn put_manifest_async(&self, manifest: &Manifest) -> io::Result<()> {
        let data = rmp_serde::to_vec(manifest)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        // put_manifest goes through the same fenced PUT path as page groups.
        self.put_page_groups_async(&[("manifest.msgpack".to_string(), data)])
            .await
    }
}

impl PageStorage for HttpPageStorage {
    fn get_page_group(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        Self::block_on(&self.runtime, self.get_object_async(key))
    }

    fn put_page_groups(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        Self::block_on(&self.runtime, self.put_page_groups_async(groups))
    }

    fn delete_objects(&self, keys: &[String]) -> io::Result<()> {
        Self::block_on(&self.runtime, self.delete_objects_async(keys))
    }

    fn range_get(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        Self::block_on(&self.runtime, self.range_get_async(key, start, len))
    }

    fn get_manifest(&self) -> io::Result<Option<Manifest>> {
        Self::block_on(&self.runtime, self.get_manifest_async())
    }

    fn put_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        Self::block_on(&self.runtime, self.put_manifest_async(manifest))
    }

    fn fetch_count(&self) -> u64 {
        self.fetch_count.load(Ordering::Relaxed)
    }

    fn fetch_bytes(&self) -> u64 {
        self.fetch_bytes.load(Ordering::Relaxed)
    }
}
