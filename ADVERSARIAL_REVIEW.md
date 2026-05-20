# Adversarial Review — haqlite

A bug-hunt of the leadership / write-forwarding / WAL-replication / phase-4
catch-up surface. Each finding: severity, location, the bug, the fix, and a
Status — **Fixed** (implemented + build green) or **Documented** (verified
real; fix specified for a focused follow-up). Line numbers approximate; the
verifier infrastructure in `phase4_chain.rs` (chain verify, equivocation,
empty-anchor) is sound and intentionally left unchanged — the bugs are in the
*consumers* that ignore or weaken its signals.

> Note: the lease clock-skew margin and proactive self-fence fixes live in the
> `hadb` dependency and are addressed there. Findings here that ultimately need
> a storage-layer epoch fence (F11) reference that dependency.

---

## Fixed in this PR

### F1 — [High] Forwarded-write TOCTOU: role checked before the lock, not after — **Fixed**
- `src/forwarding.rs:113-135` (and the same shape exists in `database.rs`
  `execute_local_raw`)
- The handler read `current_role()`, then took `conn.lock()`, then executed,
  with no re-check. The role can flip (demotion / lease loss) between the check
  and acquiring the lock; the read-only authorizer installed on demotion only
  affects statements prepared *after* it is set, so an in-flight forwarded
  write could commit on a demoted leader.
- **Fix:** re-check `current_role() == Leader` **under the connection lock**,
  immediately before `execute`, returning `MISDIRECTED_REQUEST` otherwise.

### F2 — [High] Phase-4 writer catch-up ignores chain break → publishes a truncated base — **Fixed**
- `haqlite-turbolite/src/replicator.rs` `restore_from_phase4_manifest`
- `prepare_phase4_replay` returns `break_reason` (Ok / Gap / Fork /
  BaseMismatch / NoAnchor). The read-only follower path checks it; the **writer
  catch-up path did not** — it applied the verified prefix and advanced the
  publish cursor regardless, so a just-promoted leader published a truncated
  base over real history (data loss on failover).
- **Fix:** if `break_reason != ChainBreak::Ok`, return an error before adopting
  or publishing; do not advance the publish cursor.

---

## Documented (verified real; fix specified)

### F4 — [High] First-delta checksum mismatch is downgraded instead of rejected — **Documented**
- `haqlite-turbolite/src/replay_sink.rs:121-134`
- For the first delta after the cursor (`file.seq == current_seq + 1`), a
  `prev_checksum` mismatch sets `base_file_checksum_required = true` instead of
  erroring — *even though this branch is only reached when a prior changeset
  object actually existed* (`expected_prev_checksum` is `Some`). A real chain
  break can therefore slip through against the materialized-file checksum.
- **Fix:** when `expected_prev_checksum` is `Some` (a prior changeset exists)
  and disagrees, always hard-error (chain break). The base-checksum fallback is
  legitimate only when there was no prior changeset object
  (`base_file_checksum_required` is already initialized from
  `expected_prev_checksum.is_none()`). Requires re-checking the existing
  first-delta tests, which is why it is staged for a verified follow-up rather
  than bundled here.

### F3 — [High] `caught_up` set without comparing applied seq to the chain head — **Documented**
- `haqlite-turbolite/src/follower_behavior.rs:481-504`
- `caught_up = true` whenever `new_version > current_version`, never comparing
  the applied seq to the actual chain head, so a follower that applied a short
  prefix advertises caught-up and serves stale reads.
- **Fix:** thread the discovered chain-head seq out of `prepare_*` and set
  `caught_up = true` only when `final_seq == head_seq`.

### F5 — [Med] `pull_incremental` bridges chain gaps "by page content" — **Documented**
- `src/replicator.rs:181-201`, `follower_behavior.rs:66-89`
- Deliberately merges a forked history rather than failing.
- **Fix:** gate gap-bridging behind an explicit fork-acknowledgement/generation
  check; by default hard-error when `restore()` stopped on a chain break and
  `pull_incremental` would cross it.

### F6 — [Med] Phase-3 `target_page_count` only tracks page-1 sightings → stale trailing pages — **Documented**
- `haqlite-turbolite/src/replay_sink.rs:135-141`
- Set only when a changeset includes page 1; a shrink omitting page 1 leaves
  stale trailing pages (phase-4 carries an explicit `end_page_count`).
- **Fix:** carry the post-commit page count per changeset and use the last
  applied changeset's value, mirroring phase-4.

### F7 — [Med] Phase-4 base anchor uses `change_counter` vs cursor seam — **Documented**
- `haqlite-turbolite/src/replicator.rs:545-584`
- When no replay base is pending, the base is anchored at `change_counter` but
  followers anchor deltas at `cursor.last_applied_seq`; divergence yields a
  base/delta seam gap or overlap.
- **Fix:** when the persisted replay cursor is populated, derive `base_seq` from
  `cursor.last_applied_seq`, not `change_counter`.

### F8 — [Med] Forwarded-write retry is not idempotent — **Documented**
- `src/database.rs:1688-1781` `execute_forwarded`
- Retries on connection/421/5xx with no idempotency key, so a committed-but-
  response-lost write is applied twice.
- **Fix:** client-generated idempotency token on `ForwardedExecute`; leader
  records applied tokens (dedup table in the same transaction) and returns the
  cached result on replay; document at-least-once until durable dedup lands.

### F9 — [Med] Hrana follower reads bypass the VFS replay gate — **Documented**
- `src/hrana.rs:60-99`
- Opens a raw connection on the plain OS path, bypassing the replay gate that
  the builder's follower-read opener routes through → torn reads during
  materialize + per-request role TOCTOU.
- **Fix:** route hrana follower reads through the VFS read opener (same
  `vfs_name`); resolve role once and fence on lease loss.

### F10 — [Low-Med] Bootstrap-race accepts a same-writer manifest without epoch check — **Documented**
- `haqlite-turbolite/src/replicator.rs:278-296`
- On a create-CAS conflict, if `current.writer_id == manifest.writer_id` the
  current payload is treated as authoritative without an epoch/freshness check,
  so a restarted instance reusing its id can adopt a stale lower-epoch manifest.
- **Fix:** also require `current.epoch >= manifest.epoch` (and ideally that the
  decoded cursor is not behind ours).

### F11 — [High] Base SingleWriter replication ships changesets with no leader epoch — **Documented**
- `src/database.rs` SingleWriter path, `src/ops.rs:50`
- Changesets are keyed only by `seq` with no leader epoch, so combined with a
  lease/TOCTOU race a former leader's changesets are accepted on key-name seq
  alone.
- **Fix:** plumb the `hadb` fence/epoch into the SingleWriter publish path,
  stamp it on each changeset, and reject (follower-side) any changeset whose
  epoch is below the current lease epoch. Depends on the `hadb` fencing-token
  contract (`fence_accepts`, strictly increasing) added in that repo.

---

## Test / build notes

- `cargo build --workspace` green; the Fixed cluster compiles.
- Multi-node / live-network replication tests require external infra and are not
  exercised here.
