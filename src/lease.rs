use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::lease_store::LeaseStore;
use crate::types::Role;

/// Lease data stored in S3 (or in-memory for tests).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseData {
    pub instance_id: String,
    /// Network address of the leader (for client discovery).
    #[serde(default)]
    pub address: String,
    pub claimed_at: u64,
    pub ttl_secs: u64,
    /// Unique per leadership claim. Changes on every promotion, even if the same
    /// instance reclaims. Clients poll for a new session_id after failures.
    #[serde(default)]
    pub session_id: String,
    /// When true, the leader is shutting down gracefully (Fly scale-to-zero).
    /// Followers should call on_sleep and exit rather than promote.
    #[serde(default)]
    pub sleeping: bool,
}

impl LeaseData {
    /// Whether this lease has expired based on current time.
    pub fn is_expired(&self) -> bool {
        let now = chrono::Utc::now().timestamp() as u64;
        now >= self.claimed_at + self.ttl_secs
    }
}

/// Per-database CAS lease manager.
///
/// Handles claim, renew, release for a single database's lease key.
/// Uses post-claim verification with jitter to handle backends (like Tigris)
/// that don't enforce conditional PUTs.
pub struct DbLease {
    store: Arc<dyn LeaseStore>,
    lease_key: String,
    instance_id: String,
    address: String,
    ttl_secs: u64,
    current_etag: Option<String>,
    /// Generated fresh on every new claim (not renewal).
    session_id: String,
}

impl DbLease {
    pub fn new(
        store: Arc<dyn LeaseStore>,
        prefix: &str,
        db_name: &str,
        instance_id: &str,
        address: &str,
        ttl_secs: u64,
    ) -> Self {
        let lease_key = format!("{}{}/_lease.json", prefix, db_name);
        Self {
            store,
            lease_key,
            instance_id: instance_id.to_string(),
            address: address.to_string(),
            ttl_secs,
            current_etag: None,
            session_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    /// Try to claim the lease. Returns Leader if we got it, Follower if someone else holds it.
    pub async fn try_claim(&mut self) -> Result<Role> {
        match self.read().await? {
            None => {
                // No lease exists — try to create one.
                self.try_claim_new().await
            }
            Some((lease, etag)) => {
                if lease.sleeping {
                    // Cluster was sleeping — claim to wake it up.
                    // Matches walrust leader.rs:134-137.
                    self.session_id = uuid::Uuid::new_v4().to_string();
                    self.try_claim_expired(&etag).await
                } else if lease.instance_id == self.instance_id {
                    // We already hold it (e.g. restart). Renew in place.
                    self.current_etag = Some(etag.clone());
                    self.session_id = uuid::Uuid::new_v4().to_string();
                    match self.renew().await? {
                        true => Ok(Role::Leader),
                        false => Ok(Role::Follower),
                    }
                } else if lease.is_expired() {
                    // Expired lease by another instance — try to take over.
                    self.session_id = uuid::Uuid::new_v4().to_string();
                    self.try_claim_expired(&etag).await
                } else {
                    // Active lease by another instance.
                    Ok(Role::Follower)
                }
            }
        }
    }

    /// Renew an existing lease we hold. Returns false if we lost it (412).
    /// Keeps the same session_id (no post-claim verify needed — only leader renews).
    pub async fn renew(&mut self) -> Result<bool> {
        let etag = self
            .current_etag
            .as_ref()
            .ok_or_else(|| anyhow!("No ETag — cannot renew without prior claim"))?
            .clone();

        let body = serde_json::to_vec(&self.make_lease())?;
        let result = self.store.write_if_match(&self.lease_key, body, &etag).await?;

        if result.success {
            self.current_etag = result.etag;
            Ok(true)
        } else {
            tracing::warn!("Lease renewal failed (ETag mismatch) — lost lease");
            self.current_etag = None;
            Ok(false)
        }
    }

    /// Release the lease (best-effort delete).
    pub async fn release(&mut self) -> Result<()> {
        self.store.delete(&self.lease_key).await?;
        self.current_etag = None;
        Ok(())
    }

    /// Read the current lease data + etag. None if no lease exists.
    pub async fn read(&self) -> Result<Option<(LeaseData, String)>> {
        match self.store.read(&self.lease_key).await? {
            Some((data, etag)) => {
                let lease: LeaseData = serde_json::from_slice(&data)?;
                Ok(Some((lease, etag)))
            }
            None => Ok(None),
        }
    }

    /// The lease key used in the store.
    pub fn lease_key(&self) -> &str {
        &self.lease_key
    }

    /// The instance ID of this node.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Current session ID (set on each new claim).
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Whether this lease currently holds a valid etag (i.e. we believe we hold the lease).
    pub fn has_etag(&self) -> bool {
        self.current_etag.is_some()
    }

    // ========================================================================
    // Internal
    // ========================================================================

    /// Try to create a new lease (key doesn't exist).
    async fn try_claim_new(&mut self) -> Result<Role> {
        let body = serde_json::to_vec(&self.make_lease())?;
        let result = self
            .store
            .write_if_not_exists(&self.lease_key, body)
            .await?;

        if result.success {
            self.current_etag = result.etag;
            self.post_claim_verify().await
        } else {
            // Someone else created it between our read and write.
            Ok(Role::Follower)
        }
    }

    /// Try to take over an expired lease (CAS on the old etag).
    async fn try_claim_expired(&mut self, expired_etag: &str) -> Result<Role> {
        let body = serde_json::to_vec(&self.make_lease())?;
        let result = self
            .store
            .write_if_match(&self.lease_key, body, expired_etag)
            .await?;

        if result.success {
            self.current_etag = result.etag;
            self.post_claim_verify().await
        } else {
            // Someone else took over the expired lease first.
            Ok(Role::Follower)
        }
    }

    /// Post-claim verification: sleep with jitter then read back.
    ///
    /// Handles backends like Tigris that don't enforce conditional PUTs.
    /// The jitter ensures concurrent claimants read after all writes complete,
    /// and strong read consistency means both see the same last-writer-wins result.
    async fn post_claim_verify(&mut self) -> Result<Role> {
        let jitter_ms = {
            use rand::Rng;
            rand::thread_rng().gen_range(50..200)
        };
        tokio::time::sleep(Duration::from_millis(jitter_ms)).await;

        match self.read().await? {
            Some((lease, read_etag)) => {
                if lease.instance_id == self.instance_id
                    && lease.session_id == self.session_id
                {
                    self.current_etag = Some(read_etag);
                    Ok(Role::Leader)
                } else {
                    tracing::warn!(
                        "Post-claim verify failed: lease held by {}:{} (not us: {}:{})",
                        lease.instance_id,
                        lease.session_id,
                        self.instance_id,
                        self.session_id,
                    );
                    self.current_etag = None;
                    Ok(Role::Follower)
                }
            }
            None => {
                tracing::warn!("Post-claim verify: lease vanished after write");
                self.current_etag = None;
                Ok(Role::Follower)
            }
        }
    }

    /// Write the lease with `sleeping: true` — signals followers to shut down gracefully.
    /// Only valid when we hold the lease (have an etag).
    /// Matches walrust's `LeaderElection::set_sleeping()`.
    pub async fn set_sleeping(&mut self) -> Result<bool> {
        let etag = self
            .current_etag
            .as_ref()
            .ok_or_else(|| anyhow!("No ETag — cannot set sleeping without prior claim"))?
            .clone();

        let lease = LeaseData {
            instance_id: self.instance_id.clone(),
            address: self.address.clone(),
            claimed_at: chrono::Utc::now().timestamp() as u64,
            ttl_secs: self.ttl_secs,
            session_id: self.session_id.clone(),
            sleeping: true,
        };
        let body = serde_json::to_vec(&lease)?;
        let result = self.store.write_if_match(&self.lease_key, body, &etag).await?;

        if result.success {
            self.current_etag = result.etag;
            Ok(true)
        } else {
            tracing::warn!("set_sleeping CAS conflict — lost lease");
            self.current_etag = None;
            Ok(false)
        }
    }

    fn make_lease(&self) -> LeaseData {
        LeaseData {
            instance_id: self.instance_id.clone(),
            address: self.address.clone(),
            claimed_at: chrono::Utc::now().timestamp() as u64,
            ttl_secs: self.ttl_secs,
            session_id: self.session_id.clone(),
            sleeping: false,
        }
    }
}
