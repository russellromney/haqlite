//! Phase Košice: (HaMode, Role) bail-matrix tests for haqlite-turbolite.
//!
//! Verifies the open-time dispatch in `Builder::open` rejects unimplemented
//! combinations with the planned error strings. The dispatch fires before
//! any real lease/storage/replicator work runs, so an InMemoryLeaseStore
//! plus a MemManifestStore plus a dummy turbolite VFS is enough.
//!
//! The fully-implemented `SingleWriter + Leader/Follower` combination is
//! covered by the existing `flotilla_regression`, `mode_matrix`, and
//! `e2e_modes` suites.

mod common;

use std::sync::Arc;
use std::time::Duration;

use hadb::InMemoryLeaseStore;
use haqlite_turbolite::{Builder, HaMode, Role};
use tempfile::TempDir;
use turbodb_manifest_mem::MemManifestStore;
use turbolite::tiered::{SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};

fn dummy_vfs(tmp: &TempDir) -> (SharedTurboliteVfs, String) {
    let cache_dir = tmp.path().join(".tl_cache");
    let cfg = TurboliteConfig {
        cache_dir,
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(cfg).expect("create VFS");
    let shared = SharedTurboliteVfs::new(vfs);
    let name = format!("kosice_{}", uuid::Uuid::new_v4());
    turbolite::tiered::register_shared(&name, shared.clone()).expect("register");
    (shared, name)
}

fn err_msg<T>(r: anyhow::Result<T>) -> String {
    match r {
        Err(e) => format!("{e:#}"),
        Ok(_) => panic!("expected open() to fail, got Ok"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn singlewriter_client_bails_with_planned_message() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let (vfs, vfs_name) = dummy_vfs(&tmp);

    let res = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .role(Role::Client)
        .lease_store(lease)
        .manifest_store(manifest)
        .turbolite_vfs(vfs, &vfs_name)
        .instance_id("node-1")
        .manifest_poll_interval(Duration::from_millis(50))
        .open(tmp.path().join("c.db").to_str().unwrap(), "")
        .await;

    let err = err_msg(res);
    assert!(
        err.contains("Client mode not yet implemented"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sharedwriter_latentwriter_bails_with_planned_message() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let (vfs, vfs_name) = dummy_vfs(&tmp);

    let res = Builder::new()
        .prefix("test/")
        .mode(HaMode::SharedWriter)
        .role(Role::LatentWriter)
        .lease_store(lease)
        .manifest_store(manifest)
        .turbolite_vfs(vfs, &vfs_name)
        .instance_id("node-1")
        .manifest_poll_interval(Duration::from_millis(50))
        .open(tmp.path().join("s.db").to_str().unwrap(), "")
        .await;

    let err = err_msg(res);
    assert!(
        err.contains("SharedWriter mode not yet implemented in haqlite-turbolite"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sharedwriter_default_role_bails_with_planned_message() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let (vfs, vfs_name) = dummy_vfs(&tmp);

    // No explicit role — SharedWriter defaults to LatentWriter for
    // validation, so the SharedWriter-not-implemented bail still fires.
    let res = Builder::new()
        .prefix("test/")
        .mode(HaMode::SharedWriter)
        .lease_store(lease)
        .manifest_store(manifest)
        .turbolite_vfs(vfs, &vfs_name)
        .instance_id("node-1")
        .manifest_poll_interval(Duration::from_millis(50))
        .open(tmp.path().join("sd.db").to_str().unwrap(), "")
        .await;

    let err = err_msg(res);
    assert!(
        err.contains("SharedWriter mode not yet implemented in haqlite-turbolite"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sharedwriter_client_bails_with_planned_message() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let (vfs, vfs_name) = dummy_vfs(&tmp);

    let res = Builder::new()
        .prefix("test/")
        .mode(HaMode::SharedWriter)
        .role(Role::Client)
        .lease_store(lease)
        .manifest_store(manifest)
        .turbolite_vfs(vfs, &vfs_name)
        .instance_id("node-1")
        .manifest_poll_interval(Duration::from_millis(50))
        .open(tmp.path().join("sc.db").to_str().unwrap(), "")
        .await;

    let err = err_msg(res);
    assert!(
        err.contains("Client mode not yet implemented"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn singlewriter_latentwriter_is_rejected_by_validate_mode_role() {
    let tmp = TempDir::new().unwrap();
    let lease = Arc::new(InMemoryLeaseStore::new());
    let manifest = Arc::new(MemManifestStore::new());
    let (vfs, vfs_name) = dummy_vfs(&tmp);

    let res = Builder::new()
        .prefix("test/")
        .mode(HaMode::SingleWriter)
        .role(Role::LatentWriter)
        .lease_store(lease)
        .manifest_store(manifest)
        .turbolite_vfs(vfs, &vfs_name)
        .instance_id("node-1")
        .manifest_poll_interval(Duration::from_millis(50))
        .open(tmp.path().join("swl.db").to_str().unwrap(), "")
        .await;

    let err = err_msg(res);
    assert!(
        err.contains("LatentWriter requires SharedWriter mode"),
        "unexpected error: {err}"
    );
}
