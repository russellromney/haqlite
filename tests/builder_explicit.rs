//! Phase Lucid: builder no longer silently resolves stores from env vars.
//!
//! These tests pin the new contract:
//! - `lease_store()` must be called for any HA mode.
//! - `manifest_store()` must be called for HaMode::Shared (and Dedicated+Sync,
//!   which is exercised by cinch-cloud's e2e tests since constructing an
//!   in-memory walrust `StorageBackend` here would require pulling in
//!   walrust's private test helpers).
//! - turbolite page storage (`turbolite_storage` / `turbolite_http` /
//!   `turbolite_vfs`) must be configured for HaMode::Shared.
//! - The env-var helpers live in `haqlite::env::*` for callers that want
//!   them by name.
//!
//! Each missing piece produces a loud error from `open()`. No silent
//! S3-from-env fallback, no silent HAQLITE_LEASE_URL/HAQLITE_MANIFEST_URL
//! lookup.

use haqlite::{HaMode, HaQLite, InMemoryManifestStore};
use hadb::InMemoryLeaseStore;
use std::sync::Arc;
use tempfile::TempDir;

/// `unwrap_err()` won't compile because `HaQLite` doesn't implement `Debug`.
fn err_msg<T>(r: anyhow::Result<T>) -> String {
    match r {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected open() to fail, got Ok"),
    }
}

/// Strip env vars that the old fallback paths used to consult so a host
/// shell with them set doesn't mask the test.
fn unset_env() {
    std::env::remove_var("HAQLITE_LEASE_URL");
    std::env::remove_var("HAQLITE_MANIFEST_URL");
}

#[tokio::test]
async fn dedicated_without_lease_or_walrust_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let result = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("test-1")
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    // Walrust is checked before lease for Dedicated mode, so the walrust message wins.
    // Either is acceptable proof that the silent fallback is gone.
    assert!(
        err.contains("walrust storage") || err.contains("lease_store()"),
        "expected lease/walrust error, got: {err}"
    );
}

#[tokio::test]
async fn shared_without_lease_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let result = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("test-1")
        .mode(HaMode::Shared)
        .durability(haqlite::Durability::Synchronous)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("lease_store()"),
        "expected lease_store() error, got: {err}"
    );
}

#[tokio::test]
async fn shared_without_manifest_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let lease = Arc::new(InMemoryLeaseStore::new());

    let result = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("test-1")
        .mode(HaMode::Shared)
        .durability(haqlite::Durability::Synchronous)
        .lease_store(lease)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("manifest_store()") && err.contains("Shared"),
        "expected Shared+manifest_store() error, got: {err}"
    );
}

#[tokio::test]
async fn shared_without_turbolite_errors_clearly() {
    unset_env();
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("t.db");
    let db_path_str = db_path.to_str().unwrap();

    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(InMemoryManifestStore::new());

    let result = HaQLite::builder("test-bucket")
        .prefix("p/")
        .instance_id("test-1")
        .mode(HaMode::Shared)
        .durability(haqlite::Durability::Synchronous)
        .lease_store(lease)
        .manifest_store(manifest)
        .open(db_path_str, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;

    let err = err_msg(result);
    assert!(
        err.contains("turbolite_http")
            && err.contains("turbolite_storage")
            && err.contains("turbolite_vfs"),
        "expected error to list all three turbolite options, got: {err}"
    );
}

#[tokio::test]
async fn env_module_is_public_and_returns_clear_errors() {
    // The env helpers are the only blessed way to opt into env-var resolution.
    unset_env();

    let lease_err = match haqlite::env::lease_store_from_env("b", None).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error when HAQLITE_LEASE_URL is unset"),
    };
    assert!(lease_err.contains("HAQLITE_LEASE_URL not set"));

    let manifest_err = match haqlite::env::manifest_store_from_env("b", None).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error when HAQLITE_MANIFEST_URL is unset"),
    };
    assert!(manifest_err.contains("HAQLITE_MANIFEST_URL not set"));
}
