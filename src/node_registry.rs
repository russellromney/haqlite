//! Node registration for read replica discovery.
//!
//! Each follower registers itself in S3 at `{prefix}{db_name}/nodes/{instance_id}.json`.
//! Registration includes the current leader's `session_id` — clients use this to
//! filter stale registrations from previous leadership epochs.
//!
//! Node registration uses unconditional PUTs (no CAS needed — each node only writes
//! its own file). Discovery lists all registrations and filters by session + TTL.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use walrust::storage::StorageBackend;

/// Node registration data stored in S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistration {
    pub instance_id: String,
    pub address: String,
    pub role: String,
    pub leader_session_id: String,
    pub last_seen: u64,
}

impl NodeRegistration {
    /// Check if this registration is valid: belongs to the current leader session
    /// and has been seen recently (within `ttl_secs`).
    pub fn is_valid(&self, current_session_id: &str, ttl_secs: u64) -> bool {
        let now = chrono::Utc::now().timestamp() as u64;
        self.leader_session_id == current_session_id && now < self.last_seen + ttl_secs
    }
}

/// Trait for node registration operations.
#[async_trait]
pub trait NodeRegistry: Send + Sync {
    /// Register a node under `{prefix}{db_name}/nodes/{instance_id}.json`.
    async fn register(
        &self,
        prefix: &str,
        db_name: &str,
        registration: &NodeRegistration,
    ) -> Result<()>;

    /// Deregister a node by deleting its registration file.
    async fn deregister(&self, prefix: &str, db_name: &str, instance_id: &str) -> Result<()>;

    /// Discover all registered nodes for a database.
    async fn discover_all(&self, prefix: &str, db_name: &str) -> Result<Vec<NodeRegistration>>;
}

fn node_key(prefix: &str, db_name: &str, instance_id: &str) -> String {
    format!("{}{}/_nodes/{}.json", prefix, db_name, instance_id)
}

fn nodes_prefix(prefix: &str, db_name: &str) -> String {
    format!("{}{}/_nodes/", prefix, db_name)
}

// ============================================================================
// S3 implementation
// ============================================================================

/// S3-backed node registry using `StorageBackend`.
pub struct S3NodeRegistry {
    storage: Arc<dyn StorageBackend>,
}

impl S3NodeRegistry {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl NodeRegistry for S3NodeRegistry {
    async fn register(
        &self,
        prefix: &str,
        db_name: &str,
        registration: &NodeRegistration,
    ) -> Result<()> {
        let key = node_key(prefix, db_name, &registration.instance_id);
        let data = serde_json::to_vec(registration)?;
        self.storage.upload_bytes(&key, data).await
    }

    async fn deregister(&self, prefix: &str, db_name: &str, instance_id: &str) -> Result<()> {
        let key = node_key(prefix, db_name, instance_id);
        self.storage.delete_object(&key).await
    }

    async fn discover_all(&self, prefix: &str, db_name: &str) -> Result<Vec<NodeRegistration>> {
        let list_prefix = nodes_prefix(prefix, db_name);
        let keys = self.storage.list_objects(&list_prefix).await?;
        let mut registrations = Vec::new();
        for key in &keys {
            if !key.ends_with(".json") {
                continue;
            }
            match self.storage.download_bytes(key).await {
                Ok(data) => match serde_json::from_slice::<NodeRegistration>(&data) {
                    Ok(reg) => registrations.push(reg),
                    Err(e) => {
                        tracing::warn!("Failed to parse node registration {}: {}", key, e);
                    }
                },
                Err(e) => {
                    tracing::warn!("Failed to download node registration {}: {}", key, e);
                }
            }
        }
        Ok(registrations)
    }
}

// ============================================================================
// In-memory implementation (for testing)
// ============================================================================

/// In-memory node registry for testing.
pub struct InMemoryNodeRegistry {
    /// Key: "{prefix}{db_name}/nodes/{instance_id}.json"
    store: Mutex<HashMap<String, NodeRegistration>>,
}

impl InMemoryNodeRegistry {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryNodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeRegistry for InMemoryNodeRegistry {
    async fn register(
        &self,
        prefix: &str,
        db_name: &str,
        registration: &NodeRegistration,
    ) -> Result<()> {
        let key = node_key(prefix, db_name, &registration.instance_id);
        self.store.lock().unwrap().insert(key, registration.clone());
        Ok(())
    }

    async fn deregister(&self, prefix: &str, db_name: &str, instance_id: &str) -> Result<()> {
        let key = node_key(prefix, db_name, instance_id);
        self.store.lock().unwrap().remove(&key);
        Ok(())
    }

    async fn discover_all(&self, prefix: &str, db_name: &str) -> Result<Vec<NodeRegistration>> {
        let list_prefix = nodes_prefix(prefix, db_name);
        let store = self.store.lock().unwrap();
        Ok(store
            .iter()
            .filter(|(k, _)| k.starts_with(&list_prefix))
            .map(|(_, v)| v.clone())
            .collect())
    }
}
