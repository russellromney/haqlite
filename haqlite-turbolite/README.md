# haqlite-turbolite

> **Experimental.** Under active development; APIs will change without notice.

Tiered HA SQLite. Wraps [`haqlite`](../README.md) with the [`turbolite`](https://github.com/russellromney/turbolite) VFS so SQLite pages live in S3, not on local disk. Used when you want HA SQLite *plus* "the database is the bucket" — page groups uploaded incrementally, sub-50ms cold queries from S3, no local volume required.

## When to reach for this crate

- You want HA SQLite (use `haqlite` alone — local disk + WAL shipping is fine).
- You want HA SQLite **and** the working set has to live in S3 (use `haqlite-turbolite`).

The cost: per-tenant S3 page traffic and a manifest store. The benefit: zero-local-disk databases, hibernating tenants, fast cold start from S3.

## Quick start

```rust
use haqlite_turbolite::{Builder, Mode};
use turbodb::Durability;

let db = Builder::new()
    .mode(Mode::Writer)                        // single-writer with lease
    .durability(Durability::default())          // Continuous (page checkpoints + 1s log shipping)
    .lease_store(my_lease_store)
    .manifest_store(my_manifest_store)
    .walrust_storage(my_walrust_storage)        // for WAL shipping in Continuous/Checkpoint
    .turbolite_storage(my_page_storage)         // page tiering target
    .open("/data/my.db", "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
    .await?;
```

## Durability modes

`turbodb::Durability` has three presets, exposed via `.durability(...)`:

| Mode | Pages reach S3 | Log ships | RPO | Use case |
|---|---|---|---|---|
| `Checkpoint` | on checkpoint trigger (time/commits/WAL bytes) | never | checkpoint interval | dev, single-node, desktop apps |
| `Continuous` (default) | on checkpoint | 1s cadence | ≤ 1s | production tiered HA |
| `Cloud` | every commit, before ack | n/a (pages are the replication) | 0 | multi-writer (`Mode::MultiWriter`) |

## Modes

- `Mode::Writer` — single persistent writer, lease-protected. Production default.
- `Mode::MultiWriter` — multiple writers, per-write lease acquisition. Experimental; requires `Durability::Cloud`.

## Rollback detection

If your storage layer can fork manifests under a tenant (e.g. admin-driven snapshot/rollback), call `.with_rollback_detection(database_id, token)` to compare local vs remote manifest epoch on open and wipe the local cache on mismatch. Requires `.turbolite_http(endpoint, token)` to be set so the builder can fetch the remote manifest.

## See also

- [`haqlite`](../README.md) — base HA SQLite (this crate's parent).
- [`turbolite`](https://github.com/russellromney/turbolite) — the underlying tiered VFS.
- [`hadb`](https://github.com/russellromney/hadb) — replication + lease + manifest abstractions used here.

Apache-2.0.
