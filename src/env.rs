//! Phase Lucid: opt-in env-var resolution for haqlite stores.
//!
//! These helpers used to live inside `HaQLiteBuilder::open()` as silent
//! fallbacks: if the caller did not pass `lease_store()` / `manifest_store()`,
//! the builder would read `HAQLITE_LEASE_URL` / `HAQLITE_MANIFEST_URL` and
//! dispatch by URL scheme. That made the dependency invisible at the call
//! site. Now the builder requires the caller to be explicit; if you want
//! the env-var convenience, call these helpers and pass the result.
//!
//! Typical use (CLI binaries, dev harnesses):
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn demo(bucket: &str, endpoint: Option<&str>) -> anyhow::Result<()> {
//! let lease = haqlite::env::lease_store_from_env(bucket, endpoint).await?;
//! let manifest = haqlite::env::manifest_store_from_env(bucket, endpoint).await?;
//! let _ = haqlite::HaQLite::builder(bucket)
//!     .lease_store(lease)
//!     .manifest_store(manifest);
//! # Ok(()) }
//! ```

use anyhow::Result;
use std::sync::Arc;

use crate::S3LeaseStore;

/// Resolve a lease store from `HAQLITE_LEASE_URL`.
///
/// Supported schemes:
/// - `http://host:port?token=...` → `CinchLeaseStore`
/// - `https://host:port?token=...` → `CinchLeaseStore`
/// - `nats://host:port?bucket=...` → `NatsLeaseStore` (requires `nats-lease` feature)
/// - `s3://bucket?endpoint=...` → `S3LeaseStore`
/// - `s3` (bare) → `S3LeaseStore` using the `bucket` + `endpoint` arguments
///
/// Returns an error explaining what to set if `HAQLITE_LEASE_URL` is unset.
pub async fn lease_store_from_env(
    bucket: &str,
    endpoint: Option<&str>,
) -> Result<Arc<dyn hadb::LeaseStore>> {
    let url = std::env::var("HAQLITE_LEASE_URL").map_err(|_| {
        anyhow::anyhow!(
            "HAQLITE_LEASE_URL not set. Either call HaQLiteBuilder::lease_store() \
             with an explicit store, or set the env var.\n\
             Examples:\n  \
               HAQLITE_LEASE_URL=http://proxy:8080?token=mytoken\n  \
               HAQLITE_LEASE_URL=nats://localhost:4222?bucket=leases\n  \
               HAQLITE_LEASE_URL=s3  (uses the bucket/endpoint passed here)"
        )
    })?;

    if url == "s3" || url.starts_with("s3://") {
        let (s3_bucket, s3_endpoint) = if url == "s3" {
            (bucket.to_string(), endpoint.map(|s| s.to_string()))
        } else {
            let parsed = parse_url_params(&url[5..]);
            (parsed.host, parsed.params.get("endpoint").cloned())
        };
        let base_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        let mut s3_builder = aws_sdk_s3::config::Builder::from(&base_config)
            .force_path_style(true);
        if let Some(ref ep) = s3_endpoint {
            s3_builder = s3_builder.endpoint_url(ep);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_builder.build());
        tracing::info!(
            "haqlite::env: using S3 lease store: bucket={} endpoint={:?}",
            s3_bucket,
            s3_endpoint
        );
        return Ok(Arc::new(S3LeaseStore::new(client, s3_bucket)));
    }

    if url.starts_with("http://") || url.starts_with("https://") {
        let parsed = parse_url_params(&url);
        let token = parsed.params.get("token").cloned().unwrap_or_default();
        tracing::info!("haqlite::env: using Cinch HTTP lease store: {}", parsed.base);
        return Ok(Arc::new(hadb_lease_cinch::CinchLeaseStore::new(
            &parsed.base,
            &token,
        )));
    }

    #[cfg(feature = "nats-lease")]
    if url.starts_with("nats://") {
        let parsed = parse_url_params(&url);
        let nats_bucket = parsed
            .params
            .get("bucket")
            .cloned()
            .unwrap_or_else(|| "haqlite-leases".to_string());
        tracing::info!(
            "haqlite::env: using NATS lease store: {} bucket={}",
            parsed.base,
            nats_bucket
        );
        let store = hadb_lease_nats::NatsLeaseStore::connect(&parsed.base, &nats_bucket).await?;
        return Ok(Arc::new(store));
    }
    #[cfg(not(feature = "nats-lease"))]
    if url.starts_with("nats://") {
        return Err(anyhow::anyhow!(
            "HAQLITE_LEASE_URL uses nats:// but haqlite was compiled without the nats-lease feature"
        ));
    }

    Err(anyhow::anyhow!(
        "Unsupported HAQLITE_LEASE_URL scheme: {}. Supported: http://, https://, nats://, s3://, s3",
        url
    ))
}

/// Resolve a manifest store from `HAQLITE_MANIFEST_URL`.
///
/// Same scheme dispatch as [`lease_store_from_env`].
pub async fn manifest_store_from_env(
    bucket: &str,
    endpoint: Option<&str>,
) -> Result<Arc<dyn hadb::ManifestStore>> {
    let url = std::env::var("HAQLITE_MANIFEST_URL").map_err(|_| {
        anyhow::anyhow!(
            "HAQLITE_MANIFEST_URL not set. Either call HaQLiteBuilder::manifest_store() \
             with an explicit store, or set the env var.\n\
             Examples:\n  \
               HAQLITE_MANIFEST_URL=http://proxy:8080?token=mytoken\n  \
               HAQLITE_MANIFEST_URL=nats://localhost:4222?bucket=manifests\n  \
               HAQLITE_MANIFEST_URL=s3  (uses the bucket/endpoint passed here)"
        )
    })?;

    if url == "s3" || url.starts_with("s3://") {
        #[cfg(feature = "s3-manifest")]
        {
            let (s3_bucket, s3_endpoint) = if url == "s3" {
                (bucket.to_string(), endpoint.map(|s| s.to_string()))
            } else {
                let parsed = parse_url_params(&url[5..]);
                (parsed.host, parsed.params.get("endpoint").cloned())
            };
            let base_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .load()
                .await;
            let mut s3_builder = aws_sdk_s3::config::Builder::from(&base_config)
                .force_path_style(true);
            if let Some(ref ep) = s3_endpoint {
                s3_builder = s3_builder.endpoint_url(ep);
            }
            let client = aws_sdk_s3::Client::from_conf(s3_builder.build());
            tracing::info!(
                "haqlite::env: using S3 manifest store: bucket={} endpoint={:?}",
                s3_bucket,
                s3_endpoint
            );
            return Ok(Arc::new(hadb_manifest_s3::S3ManifestStore::new(
                client, s3_bucket,
            )));
        }
        #[cfg(not(feature = "s3-manifest"))]
        {
            let _ = (bucket, endpoint);
            return Err(anyhow::anyhow!(
                "HAQLITE_MANIFEST_URL uses s3 but haqlite was compiled without the s3-manifest feature"
            ));
        }
    }

    if url.starts_with("http://") || url.starts_with("https://") {
        let parsed = parse_url_params(&url);
        let token = parsed.params.get("token").cloned().unwrap_or_default();
        tracing::info!("haqlite::env: using HTTP manifest store: {}", parsed.base);
        return Ok(Arc::new(hadb_manifest_http::HttpManifestStore::new(
            &parsed.base,
            &token,
        )));
    }

    #[cfg(feature = "nats-manifest")]
    if url.starts_with("nats://") {
        let parsed = parse_url_params(&url);
        let nats_bucket = parsed
            .params
            .get("bucket")
            .cloned()
            .unwrap_or_else(|| "haqlite-manifests".to_string());
        tracing::info!(
            "haqlite::env: using NATS manifest store: {} bucket={}",
            parsed.base,
            nats_bucket
        );
        let store =
            hadb_manifest_nats::NatsManifestStore::connect(&parsed.base, &nats_bucket).await?;
        return Ok(Arc::new(store));
    }
    #[cfg(not(feature = "nats-manifest"))]
    if url.starts_with("nats://") {
        return Err(anyhow::anyhow!(
            "HAQLITE_MANIFEST_URL uses nats:// but haqlite was compiled without the nats-manifest feature"
        ));
    }

    Err(anyhow::anyhow!(
        "Unsupported HAQLITE_MANIFEST_URL scheme: {}. Supported: http://, https://, nats://, s3://, s3",
        url
    ))
}

struct ParsedUrl {
    base: String,
    host: String,
    params: std::collections::HashMap<String, String>,
}

fn parse_url_params(url: &str) -> ParsedUrl {
    let (base, query) = match url.find('?') {
        Some(i) => (&url[..i], &url[i + 1..]),
        None => (url, ""),
    };

    let host = base
        .split("://")
        .last()
        .unwrap_or(base)
        .split('/')
        .next()
        .unwrap_or(base)
        .split(':')
        .next()
        .unwrap_or(base)
        .to_string();

    let mut params = std::collections::HashMap::new();
    for kv in query.split('&').filter(|s| !s.is_empty()) {
        if let Some((k, v)) = kv.split_once('=') {
            params.insert(k.to_string(), v.to_string());
        }
    }

    ParsedUrl {
        base: base.to_string(),
        host,
        params,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unset_lease() {
        std::env::remove_var("HAQLITE_LEASE_URL");
    }

    fn unset_manifest() {
        std::env::remove_var("HAQLITE_MANIFEST_URL");
    }

    /// Helper: extract error message from `Result<Arc<dyn _>, _>` since the
    /// trait objects don't impl Debug (so `unwrap_err()` won't compile).
    fn err_msg<T>(r: Result<T>) -> String {
        match r {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn lease_store_unset_returns_clear_error() {
        unset_lease();
        let err = err_msg(lease_store_from_env("b", None).await);
        assert!(err.contains("HAQLITE_LEASE_URL not set"), "got: {err}");
        assert!(err.contains("HaQLiteBuilder::lease_store()"));
    }

    #[tokio::test]
    async fn manifest_store_unset_returns_clear_error() {
        unset_manifest();
        let err = err_msg(manifest_store_from_env("b", None).await);
        assert!(err.contains("HAQLITE_MANIFEST_URL not set"), "got: {err}");
        assert!(err.contains("HaQLiteBuilder::manifest_store()"));
    }

    #[tokio::test]
    async fn lease_store_unsupported_scheme_errors() {
        std::env::set_var("HAQLITE_LEASE_URL", "ftp://nope");
        let err = err_msg(lease_store_from_env("b", None).await);
        assert!(err.contains("Unsupported HAQLITE_LEASE_URL scheme"), "got: {err}");
        unset_lease();
    }

    #[test]
    fn parse_url_params_basic() {
        let p = parse_url_params("http://example.com:8080/path?token=abc&extra=1");
        assert_eq!(p.host, "example.com");
        assert_eq!(p.params.get("token").map(|s| s.as_str()), Some("abc"));
        assert_eq!(p.params.get("extra").map(|s| s.as_str()), Some("1"));
    }

    #[test]
    fn parse_url_params_no_query() {
        let p = parse_url_params("nats://localhost:4222");
        assert_eq!(p.host, "localhost");
        assert!(p.params.is_empty());
    }
}
