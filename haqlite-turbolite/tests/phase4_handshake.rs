//! Phase 004 writer→follower handshake (in-process, no MinIO).
//!
//! Proves the load-bearing linkage between the writer's base publish
//! and the follower's chain verification, end to end through the public
//! API:
//!
//! 1. Writer publishes a base via
//!    `TurboliteVfs::manifest_bytes_with_phase4_cursor`, which stamps
//!    `cursor.{last_applied_seq,epoch,base_object_checksum}` + writer_id
//!    and returns `(payload, anchor)`.
//! 2. Deltas are shipped as TLM_DELTA envelopes chaining from `anchor`
//!    (the first delta's `prev_checksum`), then each prior envelope.
//! 3. A fresh follower decodes the base payload, reads
//!    `cursor.base_object_checksum`, and verifies the delta chain back
//!    to it.
//!
//! The handshake assertion is `follower.cursor.base_object_checksum ==
//! anchor == first_delta.prev_checksum`. If the writer's anchor
//! computation and the follower's anchor read ever diverge, the chain
//! fails to verify and this test catches it. The full SQLite apply is
//! covered separately (replay_sink prepare_phase4_replay tests +
//! turbolite VFS apply tests); this test isolates the protocol
//! handshake the writer wiring depends on.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use common::InMemoryStorage;
use haqlite_turbolite::phase4_chain::{filter_and_verify, ChainBreak, FollowerCursor};
use turbolite::tiered::{
    CacheConfig, GroupingStrategy, Manifest as TlManifest, TurboliteConfig, TurboliteVfs,
};
use walrust::external_delta::{self, DeltaPayloadV1};
use walrust::hadb_changeset::physical::{PageEntry, PageId, PageIdSize, PhysicalChangeset};

fn seed_manifest() -> TlManifest {
    TlManifest {
        version: 1,
        change_counter: 0,
        page_count: 4,
        page_size: 4096,
        pages_per_group: 4,
        sub_pages_per_frame: 0,
        strategy: GroupingStrategy::Positional,
        page_group_keys: vec!["pg/0_v1".into()],
        frame_tables: Vec::new(),
        group_pages: Vec::new(),
        btrees: HashMap::new(),
        interior_chunk_keys: HashMap::new(),
        index_chunk_keys: HashMap::new(),
        subframe_overrides: Vec::new(),
        page_index: HashMap::new(),
        btree_groups: HashMap::new(),
        page_to_tree_name: HashMap::new(),
        tree_name_to_groups: HashMap::new(),
        group_to_tree_name: HashMap::new(),
        db_header: None,
        discontinuity_stamp: 0,
        cursor: Default::default(),
        writer_id: String::new(),
    }
}

fn build_writer_vfs() -> TurboliteVfs {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(&cache_dir).expect("cache dir");
    // Leak the TempDir so the cache dir outlives the VFS for the test.
    std::mem::forget(tmp);
    let config = TurboliteConfig {
        cache_dir: cache_dir.clone(),
        cache: CacheConfig {
            pages_per_group: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    let rt_handle = tokio::runtime::Handle::current();
    let backend: Arc<dyn hadb_storage::StorageBackend> =
        Arc::new(hadb_storage_local::LocalStorage::new(&cache_dir));
    let mut vfs = TurboliteVfs::with_backend(config, backend, rt_handle).expect("vfs");
    let mut seed = seed_manifest();
    seed.detect_and_normalize_strategy();
    vfs.set_manifest(seed);
    vfs
}

fn changeset(seq: u64, page_id: u32, fill: u8) -> PhysicalChangeset {
    let mut data = vec![fill; 128];
    if page_id == 1 {
        data[..16].copy_from_slice(b"SQLite format 3\0");
        data[28..32].copy_from_slice(&4u32.to_be_bytes());
    }
    PhysicalChangeset::new(
        seq,
        0,
        PageIdSize::U32,
        128,
        vec![PageEntry {
            page_id: PageId::U32(page_id),
            data,
        }],
    )
}

async fn publish_delta(
    storage: &InMemoryStorage,
    seq: u64,
    epoch: u64,
    writer: &str,
    prev: Vec<u8>,
    end_page_count: u64,
    cs: &PhysicalChangeset,
) -> [u8; 32] {
    let payload = DeltaPayloadV1 {
        seq,
        epoch,
        writer_id: writer.to_string(),
        prev_checksum: prev,
        end_page_count,
        ltx_payload: walrust::hadb_changeset::physical::encode(cs),
    };
    walrust::publish_delta_envelope(storage, "wal/", "db", &payload)
        .await
        .expect("publish delta");
    external_delta::checksum(&external_delta::encode(&payload).expect("encode"))
}

/// The core handshake: the writer's published base anchor equals what
/// the follower reads from the base cursor, and the delta chain
/// verifies back to it.
#[tokio::test(flavor = "multi_thread")]
async fn writer_base_anchor_links_to_follower_chain() {
    let writer = build_writer_vfs();
    let epoch = 7u64;
    let writer_id = "writer-A";

    // Writer publishes a base at last_applied_seq=0, stamping the cursor.
    let (base_payload, anchor) = writer
        .manifest_bytes_with_phase4_cursor(0, epoch, writer_id)
        .expect("publish phase4 base");
    assert_eq!(anchor.len(), 32, "anchor is a 32-byte BLAKE3");

    // Ship two TLM_DELTA envelopes chaining from the base anchor.
    let storage = InMemoryStorage::new();
    let c1 = changeset(1, 1, 0x11);
    let ck1 = publish_delta(&storage, 1, epoch, writer_id, anchor.clone(), 4, &c1).await;
    let c2 = changeset(2, 2, 0x22);
    let _ck2 = publish_delta(&storage, 2, epoch, writer_id, ck1.to_vec(), 5, &c2).await;

    // Fresh follower decodes the base and reads the cursor.
    let base = TurboliteVfs::decode_manifest_bytes(&base_payload).expect("decode base");
    assert_eq!(
        base.cursor.base_object_checksum, anchor,
        "HANDSHAKE: follower reads the same anchor the writer published"
    );
    assert_eq!(base.cursor.epoch, epoch);
    assert_eq!(base.writer_id, writer_id);

    let cursor = FollowerCursor {
        last_applied_seq: base.cursor.last_applied_seq,
        base_object_checksum: base.cursor.base_object_checksum.clone(),
        epoch: base.cursor.epoch,
        writer_id: base.writer_id.clone(),
    };

    // The chain verifies from the base anchor through both deltas.
    let deltas =
        walrust::list_delta_envelopes_after(&storage, "wal/", "db", cursor.last_applied_seq)
            .await
            .expect("list deltas");
    let result = filter_and_verify(deltas, &cursor).expect("no equivocation");
    assert_eq!(result.break_reason, ChainBreak::Ok);
    let seqs: Vec<u64> = result.verified.iter().map(|d| d.seq).collect();
    assert_eq!(seqs, vec![1, 2]);
    // First delta chains to the published base anchor.
    assert_eq!(result.verified[0].payload.prev_checksum, anchor);
    // end_page_count of the last delta is what apply would set.
    assert_eq!(result.verified.last().unwrap().payload.end_page_count, 5);
}

/// A follower at a different epoch (e.g. stale, hasn't seen the
/// promotion) filters out all of this writer's deltas — the fence.
#[tokio::test(flavor = "multi_thread")]
async fn follower_at_wrong_epoch_filters_everything() {
    let writer = build_writer_vfs();
    let (_payload, anchor) = writer
        .manifest_bytes_with_phase4_cursor(0, 9, "writer-B")
        .expect("publish base");

    let storage = InMemoryStorage::new();
    let c1 = changeset(1, 1, 0x11);
    publish_delta(&storage, 1, 9, "writer-B", anchor.clone(), 4, &c1).await;

    // Follower cursor at epoch 8 (one behind) — drops the epoch-9 delta.
    let cursor = FollowerCursor {
        last_applied_seq: 0,
        base_object_checksum: anchor,
        epoch: 8,
        writer_id: "writer-B".to_string(),
    };
    let deltas = walrust::list_delta_envelopes_after(&storage, "wal/", "db", 0)
        .await
        .expect("list");
    let result = filter_and_verify(deltas, &cursor).expect("no equivocation");
    assert!(
        result.verified.is_empty(),
        "epoch-9 deltas are fenced from an epoch-8 follower cursor"
    );
}
