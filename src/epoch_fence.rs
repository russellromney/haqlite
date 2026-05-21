//! Leader-epoch fence for the base SingleWriter replication path (F11).
//!
//! # The gap this closes
//!
//! Base SingleWriter ships HADBP changesets keyed only by `seq`. The on-disk
//! HADBP header carries no leader epoch, so a former leader's changeset can be
//! accepted on key-name `seq` alone. Combined with a lease/TOCTOU race that is
//! a split-brain durability gap: a demoted leader can publish a changeset the
//! follower then applies as if it were authoritative.
//!
//! # The fix
//!
//! Every leader holds a monotonic lease epoch (the lease claim's etag, parsed
//! to `u64` — the same revision `hadb`'s fence uses). The leader stamps that
//! epoch into a per-database marker object next to the changesets. The follower
//! reads the marker on its apply path and rejects a marker whose epoch is
//! strictly below the highest epoch it has already accepted.
//!
//! The comparison is the canonical fencing-token rule: an incoming epoch is
//! accepted only if it is **>=** the highest accepted (`fence_accepts` uses a
//! strict `>` for a *new* token; a follower re-reading the *same* leader's epoch
//! must accept the equal value, so the gate is "reject strictly-lower"). A
//! strictly-lower epoch is a stale former-leader write and is refused.
//!
//! This mirrors `hadb_lease::fence::fence_accepts(current, incoming) ->
//! incoming > current` and keeps the same direction: never weaken to accept a
//! strictly-lower epoch.

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use hadb_storage::StorageBackend;

/// Per-database marker object name holding the current leader epoch.
///
/// Lives at `{prefix}{db_name}/{EPOCH_MARKER}`. The name has no `{GGGG}/`
/// generation segment, so changeset discovery (which only matches
/// `{db}/{generation:04x}/{seq:016x}.{ext}`) never treats it as a changeset.
const EPOCH_MARKER: &str = "_epoch";

/// The canonical fencing-token comparison, kept identical to
/// `hadb_lease::fence::fence_accepts`: a *new* token `incoming` supersedes
/// `current` only if it is strictly greater. A stale or equal token
/// (`incoming <= current`) does not advance the fence.
///
/// Pinned here so the base SingleWriter path enforces the same rule even when
/// `haqlite` is built against a published `hadb` that does not yet re-export
/// `fence_accepts`. Do not weaken to `>=`: that would let a former leader with
/// an equal revision advance the fence.
#[inline]
pub fn fence_accepts(current: u64, incoming: u64) -> bool {
    incoming > current
}

/// Build the storage key for a database's epoch marker.
fn epoch_marker_key(prefix: &str, db_name: &str) -> String {
    format!("{prefix}{db_name}/{EPOCH_MARKER}")
}

/// Stamp the leader epoch into the per-database marker.
///
/// Called on the leader before/around publishing changesets. The marker is
/// only ever advanced: if storage already holds an epoch `>=` ours we leave it
/// alone (a concurrent newer leader must win). This keeps the marker monotonic
/// even if an older leader briefly races a publish.
pub async fn stamp_leader_epoch(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    epoch: u64,
) -> Result<()> {
    let key = epoch_marker_key(prefix, db_name);
    if let Some(existing) = read_epoch_value(storage, &key).await? {
        // Only advance. A strictly-greater stored epoch belongs to a newer
        // leader; never regress the marker under it.
        if !fence_accepts(existing, epoch) {
            return Ok(());
        }
    }
    storage.put(&key, epoch.to_string().as_bytes()).await?;
    Ok(())
}

/// Read the current epoch marker, if any. `None` means no marker exists yet
/// (e.g. a database written before this fence shipped); the follower treats
/// that as "no epoch claim" and does not gate on it.
pub async fn read_leader_epoch(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Option<u64>> {
    let key = epoch_marker_key(prefix, db_name);
    read_epoch_value(storage, &key).await
}

async fn read_epoch_value(storage: &dyn StorageBackend, key: &str) -> Result<Option<u64>> {
    match storage.get(key).await? {
        Some(bytes) => {
            let text = std::str::from_utf8(&bytes)
                .map_err(|e| anyhow::anyhow!("epoch marker {key} is not valid utf-8: {e}"))?;
            let epoch = text
                .trim()
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("epoch marker {key} is not a u64: {e}"))?;
            Ok(Some(epoch))
        }
        None => Ok(None),
    }
}

/// Tracks the highest leader epoch a follower has accepted, and gates incoming
/// changeset batches against it using the same strict fencing rule as `hadb`.
///
/// A follower seeds this from the epoch it restored at and advances it as it
/// applies higher-epoch batches. An incoming epoch strictly below the high-water
/// mark is a former leader's stale write and is rejected (fail closed).
#[derive(Debug, Default)]
pub struct AppliedEpoch {
    highest: AtomicU64,
    /// Whether any epoch has been recorded. Distinguishes "highest == 0 because
    /// nothing applied" from "highest == 0 because epoch 0 was applied".
    seen: std::sync::atomic::AtomicBool,
}

impl AppliedEpoch {
    pub fn new() -> Self {
        Self::default()
    }

    /// The highest epoch accepted so far, or `None` if none recorded yet.
    pub fn highest(&self) -> Option<u64> {
        if self.seen.load(Ordering::SeqCst) {
            Some(self.highest.load(Ordering::SeqCst))
        } else {
            None
        }
    }

    /// Decide whether an incoming epoch may be applied.
    ///
    /// - No epoch recorded yet: accept (the first batch sets the high-water mark).
    /// - `incoming >= highest`: accept (same or newer leader).
    /// - `incoming <  highest`: reject (stale former-leader write).
    pub fn accepts(&self, incoming: u64) -> bool {
        match self.highest() {
            None => true,
            // Accept equal-or-greater. Reject strictly-lower. Equivalent to:
            // `incoming >= highest`, i.e. NOT a strictly-lower stale write.
            Some(highest) => incoming >= highest,
        }
    }

    /// Record an accepted epoch, advancing the high-water mark. Never regresses.
    pub fn record(&self, incoming: u64) {
        match self.highest() {
            Some(highest) if !fence_accepts(highest, incoming) => {}
            _ => self.highest.store(incoming, Ordering::SeqCst),
        }
        self.seen.store(true, Ordering::SeqCst);
    }

    /// Convenience: gate then record. Returns `Ok(())` if accepted (and records
    /// it), or an error describing the stale-epoch rejection.
    pub fn admit(&self, db_name: &str, incoming: u64) -> Result<()> {
        if self.accepts(incoming) {
            self.record(incoming);
            Ok(())
        } else {
            let highest = self.highest().unwrap_or(0);
            anyhow::bail!(
                "refusing changeset for '{db_name}': leader epoch {incoming} is below the \
                 highest applied epoch {highest} (stale former-leader write; split-brain guard)"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_accepts_is_strictly_increasing() {
        // Matches hadb_lease::fence::fence_accepts exactly.
        assert!(fence_accepts(41, 42));
        assert!(!fence_accepts(42, 42)); // equal does not advance
        assert!(!fence_accepts(42, 41)); // strictly-lower rejected
        assert!(!fence_accepts(42, 0));
    }

    #[test]
    fn first_epoch_is_accepted_and_sets_high_water() {
        let applied = AppliedEpoch::new();
        assert_eq!(applied.highest(), None);
        assert!(applied.accepts(7));
        applied.record(7);
        assert_eq!(applied.highest(), Some(7));
    }

    #[test]
    fn older_epoch_changeset_is_rejected() {
        let applied = AppliedEpoch::new();
        applied.admit("db", 5).expect("first epoch accepted");
        // A former leader's changeset stamped with an older epoch is refused.
        let err = applied
            .admit("db", 4)
            .expect_err("stale epoch must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("stale former-leader"), "unexpected: {msg}");
        // High-water mark is unchanged by the rejected write.
        assert_eq!(applied.highest(), Some(5));
    }

    #[test]
    fn current_and_newer_epoch_changesets_are_accepted() {
        let applied = AppliedEpoch::new();
        applied.admit("db", 5).expect("first epoch accepted");
        // Same leader re-publishing at the same epoch: accepted.
        applied.admit("db", 5).expect("equal epoch accepted");
        assert_eq!(applied.highest(), Some(5));
        // A new leader at a higher epoch: accepted and advances the mark.
        applied.admit("db", 9).expect("newer epoch accepted");
        assert_eq!(applied.highest(), Some(9));
        // Now a straggler from epoch 5 is below the mark: rejected.
        applied
            .admit("db", 5)
            .expect_err("epoch below new high-water must be rejected");
        assert_eq!(applied.highest(), Some(9));
    }

    #[test]
    fn record_never_regresses_high_water() {
        let applied = AppliedEpoch::new();
        applied.record(10);
        applied.record(3); // lower; ignored
        assert_eq!(applied.highest(), Some(10));
        applied.record(10); // equal; stays
        assert_eq!(applied.highest(), Some(10));
        applied.record(11); // higher; advances
        assert_eq!(applied.highest(), Some(11));
    }

    /// End-to-end F11: a leader stamps its epoch into the storage marker, then a
    /// follower reads that stamp and gates. An older-epoch (former-leader) stamp
    /// is rejected; the current and a newer leader's stamp are accepted; an
    /// equal epoch from the same writer is accepted.
    #[tokio::test]
    async fn stamped_marker_round_trip_gates_former_leader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = hadb_storage_local::LocalStorage::new(dir.path());
        let prefix = "wal/";
        let db = "app";

        // Leader at epoch 5 publishes: stamp the marker.
        stamp_leader_epoch(&storage, prefix, db, 5)
            .await
            .expect("stamp epoch 5");

        // Follower reads the stamp and admits it (first batch sets the mark).
        let follower = AppliedEpoch::new();
        let e = read_leader_epoch(&storage, prefix, db)
            .await
            .expect("read marker")
            .expect("marker present");
        follower.admit(db, e).expect("epoch 5 accepted");
        assert_eq!(follower.highest(), Some(5));

        // Same leader re-publishes at the same epoch: accepted (equal-same-writer).
        stamp_leader_epoch(&storage, prefix, db, 5)
            .await
            .expect("re-stamp epoch 5");
        let e = read_leader_epoch(&storage, prefix, db)
            .await
            .unwrap()
            .unwrap();
        follower.admit(db, e).expect("equal epoch accepted");
        assert_eq!(follower.highest(), Some(5));

        // A new leader takes over at epoch 9 (monotonic lease etag) and stamps.
        // The marker only advances, so it now reads 9; follower accepts it.
        stamp_leader_epoch(&storage, prefix, db, 9)
            .await
            .expect("stamp epoch 9");
        let e = read_leader_epoch(&storage, prefix, db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e, 9);
        follower.admit(db, e).expect("newer epoch accepted");
        assert_eq!(follower.highest(), Some(9));

        // The marker is monotonic: a straggler former leader at epoch 5 cannot
        // regress it (stamp is a no-op under the higher stored epoch).
        stamp_leader_epoch(&storage, prefix, db, 5)
            .await
            .expect("stale stamp is a monotonic no-op");
        assert_eq!(
            read_leader_epoch(&storage, prefix, db).await.unwrap(),
            Some(9),
            "marker must not regress under a former leader's lower epoch"
        );

        // Even if a former leader DID stamp a lower epoch (e.g. a marker on a
        // sibling prefix it raced), the follower's high-water gate rejects it.
        assert!(
            !follower.accepts(5),
            "former-leader epoch below high-water must be refused"
        );
        let err = follower
            .admit(db, 5)
            .expect_err("stale epoch rejected at the follower gate");
        assert!(
            err.to_string().contains("stale former-leader"),
            "unexpected: {err}"
        );
        assert_eq!(follower.highest(), Some(9));
    }
}
