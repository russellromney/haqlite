//! Phase 004 follower candidate filter + chain verifier.
//!
//! Pure logic (no storage I/O) that turns a raw, ordered list of
//! discovered TLM_DELTA envelopes into the maximal verified prefix a
//! follower may apply. Two stages, matching the plan's replication
//! shape:
//!
//! 1. **Candidate filter** — drop deltas whose `(epoch, writer_id)`
//!    does not match the cursor's epoch + the base's writer_id (stale
//!    writer leftover, sibling-prefix leakage). Then detect
//!    **equivocation**: two surviving candidates at the same `seq`
//!    with different content. Equivocation is fatal — the writer
//!    published two different histories at one seq, and a follower
//!    cannot safely pick one.
//!
//! 2. **Chain verification** — walk forward from
//!    `cursor.base_object_checksum`, taking the longest contiguous
//!    prefix where each delta's `prev_checksum` equals the prior
//!    object's envelope checksum (the base anchor for the first delta,
//!    the prior delta's envelope checksum afterward) and each `seq` is
//!    exactly one greater than the last. Stop at the first break and
//!    report why.
//!
//! The verifier never applies anything — it returns the prefix and a
//! break reason. Atomic apply is a separate step so a verify failure
//! can never leave partial state.

use walrust::DiscoveredDelta;

/// The cursor a follower reads from its current base manifest.
#[derive(Debug, Clone)]
pub struct FollowerCursor {
    /// Seq of the last delta already folded into the base. Followers
    /// only consider deltas with `seq > last_applied_seq`.
    pub last_applied_seq: u64,
    /// BLAKE3 of the published base manifest object — the anchor the
    /// first delta's `prev_checksum` must equal.
    pub base_object_checksum: Vec<u8>,
    /// Lease epoch of the base. Deltas at any other epoch are filtered.
    pub epoch: u64,
    /// writer_id recorded in the base. Deltas from any other writer
    /// are filtered.
    pub writer_id: String,
}

/// Why chain verification stopped. `Ok` means the entire candidate set
/// verified; every other variant names the first break.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainBreak {
    /// The full candidate prefix verified contiguously.
    Ok,
    /// A seq is missing (e.g. N, N+1, N+3 → gap after N+1).
    Gap { expected_seq: u64, found_seq: u64 },
    /// The first delta's `prev_checksum` does not equal the cursor's
    /// `base_object_checksum`.
    BaseMismatch { seq: u64 },
    /// A non-first delta's `prev_checksum` does not equal the prior
    /// delta's envelope checksum.
    Fork { seq: u64 },
}

/// Equivocation: two distinct candidates at the same seq (after the
/// epoch/writer filter) with different envelope checksums. Fatal —
/// surfaced with both keys so an operator can investigate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Equivocation {
    pub seq: u64,
    pub epoch: u64,
    pub writer_id: String,
    pub key_a: String,
    pub key_b: String,
}

/// Outcome of filtering + verifying a candidate set.
#[derive(Debug)]
pub struct VerifiedChain {
    /// The maximal contiguous verified prefix, ascending by seq. Safe
    /// to apply atomically. May be empty (nothing new, or the very
    /// first delta already broke).
    pub verified: Vec<DiscoveredDelta>,
    /// Why verification stopped (Ok if the whole candidate set passed).
    pub break_reason: ChainBreak,
}

/// Run the candidate filter over raw discovered deltas.
///
/// Drops deltas whose epoch != cursor.epoch or writer_id !=
/// cursor.writer_id, and deltas at or below the cursor's
/// `last_applied_seq`. Returns the surviving candidates sorted by seq,
/// or `Err(Equivocation)` if two survivors share a seq with different
/// envelope checksums.
///
/// Garbage at a seq that *also* has a valid candidate (e.g. a
/// wrong-writer object) is silently dropped by the filter and never
/// reaches the equivocation check — only two *valid* (passing the
/// filter) candidates at one seq are equivocation.
pub fn filter_candidates(
    deltas: Vec<DiscoveredDelta>,
    cursor: &FollowerCursor,
) -> Result<Vec<DiscoveredDelta>, Equivocation> {
    let mut survivors: Vec<DiscoveredDelta> = deltas
        .into_iter()
        .filter(|d| d.seq > cursor.last_applied_seq)
        .filter(|d| d.payload.epoch == cursor.epoch)
        .filter(|d| d.payload.writer_id == cursor.writer_id)
        .collect();

    survivors.sort_by_key(|d| d.seq);

    // Equivocation: adjacent survivors with the same seq but different
    // envelope checksum. (Sorted by seq, so duplicates are adjacent.)
    for pair in survivors.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        if a.seq == b.seq && a.envelope_checksum != b.envelope_checksum {
            return Err(Equivocation {
                seq: a.seq,
                epoch: a.payload.epoch,
                writer_id: a.payload.writer_id.clone(),
                key_a: a.key.clone(),
                key_b: b.key.clone(),
            });
        }
    }

    Ok(survivors)
}

/// Verify the BLAKE3 chain over filtered candidates, returning the
/// maximal contiguous prefix.
///
/// Precondition: `candidates` is the output of [`filter_candidates`]
/// (sorted by seq, epoch/writer already matched, no equivocation).
/// Walks from `cursor.base_object_checksum`:
/// - first delta must have `prev_checksum == base_object_checksum` and
///   `seq == last_applied_seq + 1`
/// - each subsequent delta must have `prev_checksum == prior delta's
///   envelope_checksum` and `seq == prior seq + 1`
///
/// Stops at the first break; everything before it is the verified
/// prefix. Duplicate-seq survivors with *identical* checksum (idempotent
/// re-publish that slipped past dedup) collapse to one.
pub fn verify_chain(candidates: Vec<DiscoveredDelta>, cursor: &FollowerCursor) -> VerifiedChain {
    let mut verified: Vec<DiscoveredDelta> = Vec::new();
    let mut expected_seq = cursor.last_applied_seq + 1;
    let mut expected_prev: Vec<u8> = cursor.base_object_checksum.clone();
    let mut break_reason = ChainBreak::Ok;

    let mut iter = candidates.into_iter().peekable();
    while let Some(delta) = iter.next() {
        // Collapse an idempotent duplicate seq with identical checksum.
        if let Some(prev) = verified.last() {
            if delta.seq == prev.seq && delta.envelope_checksum == prev.envelope_checksum {
                continue;
            }
        }

        if delta.seq != expected_seq {
            break_reason = ChainBreak::Gap {
                expected_seq,
                found_seq: delta.seq,
            };
            break;
        }

        if delta.payload.prev_checksum != expected_prev {
            break_reason = if verified.is_empty() {
                ChainBreak::BaseMismatch { seq: delta.seq }
            } else {
                ChainBreak::Fork { seq: delta.seq }
            };
            break;
        }

        expected_prev = delta.envelope_checksum.to_vec();
        expected_seq = delta.seq + 1;
        verified.push(delta);
    }

    VerifiedChain {
        verified,
        break_reason,
    }
}

/// Convenience: filter then verify in one call. Returns `Err` only for
/// equivocation (the one fatal condition); a chain break is reported in
/// the returned `VerifiedChain::break_reason`, not as an error, because
/// a partial verified prefix is still safe to apply.
pub fn filter_and_verify(
    deltas: Vec<DiscoveredDelta>,
    cursor: &FollowerCursor,
) -> Result<VerifiedChain, Equivocation> {
    let candidates = filter_candidates(deltas, cursor)?;
    Ok(verify_chain(candidates, cursor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrust::external_delta::{self, DeltaPayloadV1};

    /// Build a DiscoveredDelta with a chosen prev_checksum. Returns the
    /// delta; its `envelope_checksum` is the real BLAKE3 of its encoded
    /// envelope so chains can be built test-side.
    fn delta(
        seq: u64,
        epoch: u64,
        writer: &str,
        prev: Vec<u8>,
        end_pages: u64,
        ltx: Vec<u8>,
    ) -> DiscoveredDelta {
        let payload = DeltaPayloadV1 {
            seq,
            epoch,
            writer_id: writer.to_string(),
            prev_checksum: prev,
            end_page_count: end_pages,
            ltx_payload: ltx,
        };
        let envelope = external_delta::encode(&payload).expect("encode");
        let envelope_checksum = external_delta::checksum(&envelope);
        DiscoveredDelta {
            key: format!("wal/db/0000/{seq:016x}.tlmd"),
            seq,
            payload,
            envelope_checksum,
        }
    }

    fn cursor(last_seq: u64, base_ck: Vec<u8>, epoch: u64, writer: &str) -> FollowerCursor {
        FollowerCursor {
            last_applied_seq: last_seq,
            base_object_checksum: base_ck,
            epoch,
            writer_id: writer.to_string(),
        }
    }

    /// Build a valid chain of `n` deltas anchored at `base_ck`,
    /// starting at seq `start`. Returns the deltas in order.
    fn valid_chain(start: u64, n: u64, epoch: u64, writer: &str, base_ck: Vec<u8>) -> Vec<DiscoveredDelta> {
        let mut out = Vec::new();
        let mut prev = base_ck;
        for i in 0..n {
            let seq = start + i;
            let d = delta(seq, epoch, writer, prev.clone(), 100 + seq, vec![seq as u8]);
            prev = d.envelope_checksum.to_vec();
            out.push(d);
        }
        out
    }

    #[test]
    fn continuous_follower_candidate_filter_drops_wrong_epoch_and_writer() {
        let base = vec![0xBB; 32];
        let cur = cursor(0, base.clone(), 5, "leader-A");

        let mut deltas = valid_chain(1, 2, 5, "leader-A", base.clone());
        // Stale-epoch object at a fresh seq — dropped.
        deltas.push(delta(3, 4, "leader-A", vec![0; 32], 103, vec![9]));
        // Wrong-writer object — dropped.
        deltas.push(delta(4, 5, "old-leader", vec![0; 32], 104, vec![9]));

        let survivors = filter_candidates(deltas, &cur).expect("no equivocation");
        let seqs: Vec<u64> = survivors.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![1, 2], "only matching epoch+writer survive");
    }

    #[test]
    fn continuous_follower_candidate_filter_respects_cursor_floor() {
        let base = vec![0xBB; 32];
        // Cursor already at seq 5 — deltas <= 5 are not candidates.
        let cur = cursor(5, base.clone(), 5, "leader-A");
        let mut deltas = valid_chain(1, 4, 5, "leader-A", base.clone()); // seq 1..4
        deltas.extend(valid_chain(6, 2, 5, "leader-A", vec![0xCC; 32])); // seq 6,7
        let survivors = filter_candidates(deltas, &cur).expect("no equivocation");
        let seqs: Vec<u64> = survivors.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![6, 7], "deltas at/below the floor are dropped");
    }

    #[test]
    fn continuous_follower_equivocation_fatal() {
        let base = vec![0xBB; 32];
        let cur = cursor(0, base.clone(), 5, "leader-A");

        // Two DIFFERENT valid (epoch+writer match) deltas at seq 1.
        let d1a = delta(1, 5, "leader-A", base.clone(), 101, vec![1]);
        let d1b = delta(1, 5, "leader-A", base.clone(), 101, vec![2]); // diff ltx -> diff checksum
        assert_ne!(d1a.envelope_checksum, d1b.envelope_checksum);

        let err = filter_candidates(vec![d1a.clone(), d1b.clone()], &cur)
            .expect_err("two valid candidates at one seq = equivocation");
        assert_eq!(err.seq, 1);
        assert_eq!(err.epoch, 5);
        // Both offending keys surfaced.
        assert!(err.key_a.contains("0000000000000001"));
        assert!(err.key_b.contains("0000000000000001"));
    }

    #[test]
    fn adv_duplicate_seq_garbage_skipped_is_not_equivocation() {
        let base = vec![0xBB; 32];
        let cur = cursor(0, base.clone(), 5, "leader-A");
        // One valid candidate at seq 1, plus a wrong-writer object at
        // seq 1. The garbage is filtered out *before* equivocation
        // detection, so this is NOT equivocation.
        let valid = delta(1, 5, "leader-A", base.clone(), 101, vec![1]);
        let garbage = delta(1, 5, "other-writer", base.clone(), 101, vec![2]);
        let survivors = filter_candidates(vec![valid, garbage], &cur)
            .expect("garbage filtered, not equivocation");
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].payload.writer_id, "leader-A");
    }

    #[test]
    fn verify_full_valid_chain() {
        let base = vec![0xBB; 32];
        let cur = cursor(0, base.clone(), 5, "leader-A");
        let deltas = valid_chain(1, 5, 5, "leader-A", base.clone());
        let result = filter_and_verify(deltas, &cur).expect("no equivocation");
        assert_eq!(result.break_reason, ChainBreak::Ok);
        let seqs: Vec<u64> = result.verified.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn adv_missing_middle_applies_through_gap_then_stops() {
        let base = vec![0xBB; 32];
        let cur = cursor(0, base.clone(), 5, "leader-A");
        // Build a valid chain of 3, then drop the middle (seq 2).
        let full = valid_chain(1, 3, 5, "leader-A", base.clone());
        let deltas = vec![full[0].clone(), full[2].clone()]; // seq 1 and 3
        let result = filter_and_verify(deltas, &cur).expect("no equivocation");
        // Only seq 1 verifies; gap reported at expected 2, found 3.
        let seqs: Vec<u64> = result.verified.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![1]);
        assert_eq!(
            result.break_reason,
            ChainBreak::Gap {
                expected_seq: 2,
                found_seq: 3
            }
        );
    }

    #[test]
    fn adv_same_seq_wrong_checksum_base_mismatch() {
        let base = vec![0xBB; 32];
        let cur = cursor(0, base.clone(), 5, "leader-A");
        // First delta's prev_checksum does NOT equal the base anchor.
        let bad_first = delta(1, 5, "leader-A", vec![0xFF; 32], 101, vec![1]);
        let result = filter_and_verify(vec![bad_first], &cur).expect("no equivocation");
        assert!(result.verified.is_empty());
        assert_eq!(result.break_reason, ChainBreak::BaseMismatch { seq: 1 });
    }

    #[test]
    fn adv_truncated_chain_applies_exactly_k() {
        let base = vec![0xBB; 32];
        let cur = cursor(0, base.clone(), 5, "leader-A");
        // Valid chain of 2, then a third delta that chains to the WRONG
        // prior (forged prev_checksum) -> fork after seq 2.
        let mut deltas = valid_chain(1, 2, 5, "leader-A", base.clone());
        let forged = delta(3, 5, "leader-A", vec![0x77; 32], 103, vec![3]);
        deltas.push(forged);
        let result = filter_and_verify(deltas, &cur).expect("no equivocation");
        let seqs: Vec<u64> = result.verified.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![1, 2], "applies exactly the verified prefix");
        assert_eq!(result.break_reason, ChainBreak::Fork { seq: 3 });
    }

    #[test]
    fn idempotent_duplicate_seq_identical_checksum_collapses() {
        let base = vec![0xBB; 32];
        let cur = cursor(0, base.clone(), 5, "leader-A");
        let chain = valid_chain(1, 2, 5, "leader-A", base.clone());
        // Duplicate the seq-1 delta verbatim (same checksum) — an
        // idempotent re-publish. Must not be equivocation, must collapse.
        let dup = chain[0].clone();
        let deltas = vec![chain[0].clone(), dup, chain[1].clone()];
        let result = filter_and_verify(deltas, &cur).expect("identical dup is not equivocation");
        let seqs: Vec<u64> = result.verified.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![1, 2]);
        assert_eq!(result.break_reason, ChainBreak::Ok);
    }

    #[test]
    fn same_base_higher_deltas_when_cursor_did_not_advance() {
        // Follower cursor at seq 2; new contiguous deltas 3,4 arrive
        // against the same base. They verify and apply.
        let base = vec![0xBB; 32];
        // Build a 4-long chain; the follower already applied 1,2 so its
        // cursor's base_object_checksum is the *base* and last_applied=2.
        // For the verifier, the anchor for seq 3 is delta-2's envelope
        // checksum, which the cursor would carry forward. Simulate by
        // setting the cursor anchor to delta-2's checksum.
        let chain = valid_chain(1, 4, 5, "leader-A", base.clone());
        let anchor = chain[1].envelope_checksum.to_vec(); // delta seq 2
        let cur = cursor(2, anchor, 5, "leader-A");
        let deltas = vec![chain[2].clone(), chain[3].clone()]; // seq 3,4
        let result = filter_and_verify(deltas, &cur).expect("no equivocation");
        let seqs: Vec<u64> = result.verified.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![3, 4]);
        assert_eq!(result.break_reason, ChainBreak::Ok);
    }
}
