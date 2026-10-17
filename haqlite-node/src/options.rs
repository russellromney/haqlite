use std::{sync::Arc, time::Duration};

use hadb_storage_s3::S3Storage;
use haqlite::{Durability, HaQLite};
use napi::bindgen_prelude::*;
use napi_derive::napi;

/// HA options. AWS credentials and region use the standard AWS provider chain.
#[napi(object)]
pub struct OpenOptions {
    /// S3 bucket for replicated WAL segments and, by default, leases.
    pub bucket: String,
    pub schema: Option<String>,
    /// S3-compatible endpoint, such as MinIO or R2.
    pub endpoint: Option<String>,
    /// HTTP lease service. Omit to store leases in the S3 bucket.
    pub lease_endpoint: Option<String>,
    pub lease_token: Option<String>,
    pub prefix: Option<String>,
    pub instance_id: Option<String>,
    /// Address reachable by other nodes for write forwarding.
    pub address: Option<String>,
    /// Built-in forwarding port (default 18080). Zero disables forwarding.
    pub forwarding_port: Option<f64>,
    pub secret: Option<String>,
    pub sync_interval_ms: Option<f64>,
    pub follower_pull_interval_ms: Option<f64>,
    pub forward_timeout_ms: Option<f64>,
    pub read_concurrency: Option<f64>,
    pub lease_ttl_seconds: Option<f64>,
    pub lease_renew_interval_ms: Option<f64>,
    pub lease_follower_poll_interval_ms: Option<f64>,
}

impl OpenOptions {
    pub async fn open(self, path: &str) -> Result<HaQLite> {
        if self.bucket.trim().is_empty() {
            return Err(invalid("bucket must not be empty"));
        }
        if self.lease_token.is_some() && self.lease_endpoint.is_none() {
            return Err(invalid("leaseToken requires leaseEndpoint"));
        }

        // Validate every numeric setting before loading credentials or contacting S3.
        let port = self
            .forwarding_port
            .map(|n| integer("forwardingPort", n, 0, 65535))
            .transpose()?;
        let concurrency = self
            .read_concurrency
            .map(|n| integer("readConcurrency", n, 1, u32::MAX))
            .transpose()?;
        let ttl = self
            .lease_ttl_seconds
            .map(|n| integer("leaseTtlSeconds", n, 1, u32::MAX))
            .transpose()?;
        let sync = milliseconds("syncIntervalMs", self.sync_interval_ms)?;
        let pull = milliseconds("followerPullIntervalMs", self.follower_pull_interval_ms)?;
        let timeout = milliseconds("forwardTimeoutMs", self.forward_timeout_ms)?;
        let renew = milliseconds("leaseRenewIntervalMs", self.lease_renew_interval_ms)?;
        let poll = milliseconds(
            "leaseFollowerPollIntervalMs",
            self.lease_follower_poll_interval_ms,
        )?;

        let mut builder = HaQLite::builder();
        if let Some(value) = self.prefix {
            builder = builder.prefix(&value);
        }
        if let Some(value) = self.instance_id {
            builder = builder.instance_id(&value);
        }
        if let Some(value) = self.address {
            builder = builder.address(&value);
        }
        if let Some(value) = self.secret {
            builder = builder.secret(&value);
        }
        if let Some(value) = port {
            builder = builder.forwarding_port(value as u16);
        }
        if let Some(value) = concurrency {
            builder = builder.read_concurrency(value as usize);
        }
        if let Some(value) = ttl {
            builder = builder.lease_ttl(value as u64);
        }
        if let Some(value) = sync {
            builder = builder.durability(Durability::Replicated(value));
        }
        if let Some(value) = pull {
            builder = builder.follower_pull_interval(value);
        }
        if let Some(value) = timeout {
            builder = builder.forward_timeout(value);
        }
        if let Some(value) = renew {
            builder = builder.lease_renew_interval(value);
        }
        if let Some(value) = poll {
            builder = builder.lease_follower_poll_interval(value);
        }

        let storage = S3Storage::from_env(self.bucket.clone(), self.endpoint.as_deref())
            .await
            .map_err(super::database_error)?;
        builder = if let Some(endpoint) = self.lease_endpoint {
            builder.lease_endpoint(&endpoint, self.lease_token.as_deref().unwrap_or(""))
        } else {
            builder.lease_store(Arc::new(haqlite::S3LeaseStore::new(
                storage.client().clone(),
                self.bucket,
            )))
        };
        builder
            .walrust_storage(Arc::new(storage))
            .open(path, self.schema.as_deref().unwrap_or(""))
            .await
            .map_err(super::database_error)
    }
}

pub fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || path == ":memory:" {
        return Err(invalid("A file-backed database path is required"));
    }
    Ok(())
}

fn integer(name: &str, value: f64, min: u32, max: u32) -> Result<u32> {
    if !value.is_finite() || value.fract() != 0.0 || value < min as f64 || value > max as f64 {
        return Err(invalid(&format!(
            "{name} must be an integer between {min} and {max}"
        )));
    }
    Ok(value as u32)
}

fn milliseconds(name: &str, value: Option<f64>) -> Result<Option<Duration>> {
    value
        .map(|n| integer(name, n, 1, u32::MAX).map(|n| Duration::from_millis(n as u64)))
        .transpose()
}

fn invalid(message: &str) -> Error {
    Error::new(Status::InvalidArg, message)
}
