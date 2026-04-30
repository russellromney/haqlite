//! Phase Košice: (HaMode, Role) bail-matrix tests for base haqlite.
//!
//! Verifies the open-time dispatch in `HaQLiteBuilder::open` rejects
//! unimplemented combinations with the planned error strings before any
//! infrastructure work runs (no real lease store, manifest, or storage
//! is needed — the bail fires on the validation/dispatch path).
//!
//! The fully-implemented `SingleWriter + Leader/Follower` combination is
//! covered by the existing `ha_database` and `plain_replicator_regression`
//! tests; that is intentionally out of scope here.

use std::sync::Arc;

use haqlite::{HaQLite, HaMode, InMemoryLeaseStore, Role};
use tempfile::TempDir;

fn db_path(tmp: &TempDir, name: &str) -> String {
    tmp.path().join(name).to_string_lossy().into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn singlewriter_client_bails_with_planned_message() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());

    let err = HaQLite::builder()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .role(Role::Client)
        .lease_store(lease)
        .instance_id("node-1")
        .open(&db_path(&tmp, "client.db"), "")
        .await
        .err()
        .expect("Client must bail");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("Client mode not yet implemented in base haqlite"),
        "unexpected error: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sharedwriter_latentwriter_bails_with_planned_message() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());

    let err = HaQLite::builder()
        .prefix("test/")
        .mode(HaMode::SharedWriter)
        .role(Role::LatentWriter)
        .lease_store(lease)
        .instance_id("node-1")
        .open(&db_path(&tmp, "latent.db"), "")
        .await
        .err()
        .expect("SharedWriter must bail in base haqlite");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("SharedWriter not implemented in base haqlite"),
        "unexpected error: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sharedwriter_default_role_bails_with_planned_message() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());

    // No explicit role — SharedWriter still has no base-haqlite impl.
    let err = HaQLite::builder()
        .prefix("test/")
        .mode(HaMode::SharedWriter)
        .lease_store(lease)
        .instance_id("node-1")
        .open(&db_path(&tmp, "shared_default.db"), "")
        .await
        .err()
        .expect("SharedWriter (default role) must bail");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("SharedWriter not implemented in base haqlite"),
        "unexpected error: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sharedwriter_client_bails_with_planned_message() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());

    let err = HaQLite::builder()
        .prefix("test/")
        .mode(HaMode::SharedWriter)
        .role(Role::Client)
        .lease_store(lease)
        .instance_id("node-1")
        .open(&db_path(&tmp, "shared_client.db"), "")
        .await
        .err()
        .expect("Client must bail under SharedWriter too");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("Client mode not yet implemented in base haqlite"),
        "unexpected error: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn singlewriter_latentwriter_is_rejected_by_validate_mode_role() {
    // The (SingleWriter, LatentWriter) combination is type-system-visible
    // but invalid by the hadb validator. The builder's open() must surface
    // the validator's "LatentWriter requires SharedWriter mode" message.
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());

    let err = HaQLite::builder()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .role(Role::LatentWriter)
        .lease_store(lease)
        .instance_id("node-1")
        .open(&db_path(&tmp, "sw_latent.db"), "")
        .await
        .err()
        .expect("invalid combination");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("LatentWriter requires SharedWriter mode"),
        "unexpected error: {msg}"
    );
}
