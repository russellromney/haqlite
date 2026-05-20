//! Phase 004 object-store reality proof (step 8, MinIO/real S3).
//!
//! The plan emphasizes that the original live-stack bug was on the
//! object-store path, so the phase-4 protocol must be proven against a
//! real S3-compatible store's listing semantics — not just in-memory.
//! These tests gate on `TIERED_TEST_BUCKET` (set when MinIO/Tigris is
//! available) and are skipped otherwise, matching the existing
//! object-storage test convention.
//!
//! Run against MinIO:
//!   TIERED_TEST_BUCKET=phase4-test AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
//!   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//!   AWS_REGION=us-east-1 \
//!   cargo test -p haqlite-turbolite --test phase4_object_store

mod common;

#[cfg(feature = "s3")]
mod object_store {
    use crate::common;
    use haqlite_turbolite::phase4_chain::{filter_and_verify, ChainBreak, FollowerCursor};
    use walrust::external_delta::{self, DeltaPayloadV1};
    use walrust::hadb_changeset::physical::{PageEntry, PageId, PageIdSize, PhysicalChangeset};

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

    async fn publish(
        storage: &dyn hadb_storage::StorageBackend,
        db: &str,
        seq: u64,
        epoch: u64,
        writer: &str,
        prev: Vec<u8>,
        end_pages: u64,
        cs: &PhysicalChangeset,
    ) -> [u8; 32] {
        let payload = DeltaPayloadV1 {
            seq,
            epoch,
            writer_id: writer.to_string(),
            prev_checksum: prev,
            end_page_count: end_pages,
            ltx_payload: walrust::hadb_changeset::physical::encode(cs),
        };
        walrust::publish_delta_envelope(storage, "", db, &payload)
            .await
            .expect("publish delta to object store");
        external_delta::checksum(&external_delta::encode(&payload).expect("encode"))
    }

    /// A fresh follower lists `.tlmd` deltas from a REAL object store
    /// (MinIO/Tigris), filters by (epoch, writer_id), and verifies the
    /// BLAKE3 chain back to the base anchor — proving the protocol
    /// works against real listing semantics, not just in-memory.
    #[tokio::test]
    async fn obj_fresh_follower_lists_and_verifies_chain() {
        if !common::s3_env_available() {
            eprintln!("skipping: TIERED_TEST_BUCKET not set");
            return;
        }
        // Unique prefix per run so reruns don't collide.
        let prefix = format!("phase4-obj/{}/", uuid::Uuid::new_v4());
        let storage = common::s3_backend(&prefix).await;
        let db = "objdb";
        let epoch = 5u64;
        let writer = "writer-A";
        let base_anchor = vec![0xBB; 32];

        // Writer ships a 3-delta chain to the real store.
        let c1 = changeset(1, 1, 0x11);
        let ck1 = publish(
            storage.as_ref(),
            db,
            1,
            epoch,
            writer,
            base_anchor.clone(),
            4,
            &c1,
        )
        .await;
        let c2 = changeset(2, 2, 0x22);
        let ck2 = publish(storage.as_ref(), db, 2, epoch, writer, ck1.to_vec(), 5, &c2).await;
        let c3 = changeset(3, 3, 0x33);
        let _ck3 = publish(storage.as_ref(), db, 3, epoch, writer, ck2.to_vec(), 6, &c3).await;

        // Fresh follower (empty state) lists from the real store + verifies.
        let cursor = FollowerCursor {
            last_applied_seq: 0,
            base_object_checksum: base_anchor.clone(),
            epoch,
            writer_id: writer.to_string(),
        };
        let deltas = walrust::list_delta_envelopes_after(storage.as_ref(), "", db, 0)
            .await
            .expect("list .tlmd from object store");
        let result = filter_and_verify(deltas, &cursor).expect("no equivocation");
        assert_eq!(result.break_reason, ChainBreak::Ok);
        let seqs: Vec<u64> = result.verified.iter().map(|d| d.seq).collect();
        assert_eq!(
            seqs,
            vec![1, 2, 3],
            "real-store listing yields the full chain in order"
        );
        assert_eq!(result.verified[0].payload.prev_checksum, base_anchor);
        assert_eq!(result.verified.last().unwrap().payload.end_page_count, 6);
    }

    /// obj_no_state_json: the phase-4 delta prefix contains only `.tlmd`
    /// objects — no `state.json` sidecar (the cursor lives in the
    /// turbolite manifest, per the plan's blast-radius rule) and no
    /// rogue keys.
    #[tokio::test]
    async fn obj_no_state_json_only_tlmd_objects() {
        if !common::s3_env_available() {
            eprintln!("skipping: TIERED_TEST_BUCKET not set");
            return;
        }
        let prefix = format!("phase4-obj/{}/", uuid::Uuid::new_v4());
        let storage = common::s3_backend(&prefix).await;
        let db = "objdb";

        let c1 = changeset(1, 1, 0x11);
        publish(
            storage.as_ref(),
            db,
            1,
            5,
            "writer-A",
            vec![0xBB; 32],
            4,
            &c1,
        )
        .await;
        let c2 = changeset(2, 2, 0x22);
        let ck1 = external_delta::checksum(
            &external_delta::encode(&DeltaPayloadV1 {
                seq: 1,
                epoch: 5,
                writer_id: "writer-A".into(),
                prev_checksum: vec![0xBB; 32],
                end_page_count: 4,
                ltx_payload: walrust::hadb_changeset::physical::encode(&c1),
            })
            .unwrap(),
        );
        publish(storage.as_ref(), db, 2, 5, "writer-A", ck1.to_vec(), 5, &c2).await;

        // List the whole db prefix (raw) and assert object hygiene.
        let all = storage
            .list(&format!("{}/", db), None)
            .await
            .expect("list db prefix");
        assert!(!all.is_empty(), "expected published objects");
        for key in &all {
            assert!(
                !key.ends_with("state.json"),
                "phase-4 prefix must not contain state.json: {key}"
            );
            assert!(
                key.ends_with(".tlmd"),
                "phase-4 prefix must contain only .tlmd objects, found: {key}"
            );
        }
    }
}
