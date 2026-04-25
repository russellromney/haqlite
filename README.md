# haqlite

> **Experimental.** haqlite is under active development and not yet stable. APIs will change without notice.

Embed HA SQLite in your app with one line of code. Leader election, WAL replication, write forwarding.

Part of the [hadb](https://github.com/russellromney/hadb) ecosystem for making any embedded database highly available.

`haqlite` is the base HA SQLite library with WAL replication. For tiered HA SQLite with page-level S3 tiering, add the optional [`haqlite-turbolite`](haqlite-turbolite/README.md) sibling crate.

## Quick start

```rust
use haqlite::{HaQLite, SqlValue};

let db = HaQLite::builder()
    .open("/data/my.db", "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
    .await?;

// Writes: forwarded to leader automatically
db.execute(
    "INSERT INTO users (name) VALUES (?1)",
    &[SqlValue::Text("Alice".into())],
)?;

// Reads: always local
let count: i64 = db.query_row("SELECT COUNT(*) FROM users", &[], |r| r.get(0))?;

// Clean shutdown
db.close().await?;
```

## How it works

```
Node 1 (leader)                    Node 2 (follower)
+-----------------------+          +-----------------------+
|  Your App             |          |  Your App             |
|         |             |          |         |             |
|    HaQLite            |          |    HaQLite            |
|    +- execute() ------+--local   |    +- execute() ------+--> HTTP to leader
|    +- query_row() ----+--local   |    +- query_row() ----+--local (read replica)
|    +- Coordinator     |          |    +- Coordinator     |
|    +- SQLite (rw)     |          |    +- SQLite (ro)     |
|    +- walrust sync    |          |    +- walrust pull    |
+-----------+-----------+          +-----------+-----------+
            +--------- lease + storage --------+
```

- **Leader election** via pluggable lease store (S3, NATS, HTTP, etcd). No Raft, no Paxos.
- **WAL replication** via [walrust](https://github.com/russellromney/walrust). Leader syncs WAL frames to S3, followers pull and apply.
- **Write forwarding**: `execute()` on a follower transparently forwards to the leader. Your app doesn't need to know who the leader is.
- **Auto-promotion**: when the leader dies, a follower claims the lease, catches up, and promotes itself.
- **Self-fencing**: if a leader loses its lease, it demotes itself immediately. No split-brain.

## Performance

haqlite adds zero measurable overhead to SQLite. Leader election, WAL replication, write forwarding, and automatic failover run entirely in background tasks with no impact on the read/write hot path.

100K rows, Fly.io performance-2x (dedicated vCPU, NVMe, IAD), single-node leader:

| Operation | SQLite | haqlite (replicated) | Overhead |
|-----------|--------|---------------------|----------|
| Point lookup | 145K/s | 151K/s | **none** |
| Range scan | 8.8K/s | 8.1K/s | **none** |
| Full table scan | 56/s | 55/s | **none** |
| Single INSERT | 19K/s | 28K/s | **faster** |
| UPDATE by PK | 40K/s | 44K/s | **none** |
| Batch INSERT (in txn) | 685K/s | 295K/s | 2.3x |

haqlite sets `synchronous=NORMAL` and `cache_size=64MB` by default (WAL mode best practice). The haqlite API layer (role check, connection lock, SqlValue params) adds <1us per call.

**Durability modes have different write costs:**

| Mode | Write cost | Use case |
|------|-----------|----------|
| Dedicated + Continuous | same as SQLite | Active databases with volume (default) |
| Dedicated + Checkpoint | same as SQLite | Dev / single-node / desktop apps |
| Dedicated + Cloud | ~200ms/write | Every write durable to S3 |

Continuous mode (default) writes locally, ships WAL to S3 in the background via [walrust](https://github.com/russellromney/walrust). Checkpoint mode is the same without WAL shipping — crash loses everything since last checkpoint. Cloud mode uploads every commit to S3 before returning (no WAL).

For page-level S3 tiering (sub-250ms cold queries, transparent page eviction), use the [`haqlite-turbolite`](./crates/haqlite-turbolite) crate.

## Lease and manifest store

haqlite requires a lease store for leader election and (for some durability modes) a manifest store for coordination. Configure via env vars or builder methods.

### Environment variables

One env var per store, scheme picks the backend:

```bash
# S3 (uses builder bucket/endpoint)
HAQLITE_LEASE_URL=s3
HAQLITE_MANIFEST_URL=s3

# S3 explicit
HAQLITE_LEASE_URL=s3://my-bucket?endpoint=https://fly.storage.tigris.dev

# NATS (requires nats-lease / nats-manifest feature)
HAQLITE_LEASE_URL=nats://localhost:4222?bucket=leases
HAQLITE_MANIFEST_URL=nats://localhost:4222?bucket=manifests

# HTTP (for embedded replicas via a proxy)
HAQLITE_LEASE_URL=http://proxy:8080?token=mytoken
HAQLITE_MANIFEST_URL=http://proxy:8080?token=mytoken
```

No fallbacks. If the env var is missing and no store is set on the builder, haqlite errors with a clear message.

### Builder methods

```rust
// Explicit store objects
let db = HaQLite::builder()
    .lease_store(Arc::new(my_nats_store))
    .manifest_store(Arc::new(my_manifest_store))
    .open("/data/my.db", schema)
    .await?;

// HTTP convenience (equivalent to setting the env vars)
let db = HaQLite::builder()
    .lease_endpoint("http://proxy:8080", "my-token")
    .manifest_endpoint("http://proxy:8080", "my-token")
    .open("/data/my.db", schema)
    .await?;
```

### Supported backends

| Backend | Lease | Manifest | Failover | Feature flag |
|---------|-------|----------|----------|-------------|
| S3 | yes | yes | 50-200ms | always on / `s3-manifest` |
| NATS | yes | yes | 2-5ms | `nats-lease` / `nats-manifest` |
| HTTP | yes | yes | 10-15ms | always on |
| etcd | yes | yes | ~50ms | `etcd-lease` / `etcd-manifest` |

**Note:** Tigris S3 does not support atomic conditional PUTs. Use NATS, HTTP, or AWS S3 for the lease store.

## Builder options

```rust
let db = HaQLite::builder()
    .prefix("myapp/")                        // S3 key prefix (default: "haqlite/")
    .endpoint("https://t3.storage.dev")       // S3 endpoint (Tigris, MinIO, R2)
    .instance_id("node-1")                    // default: FLY_MACHINE_ID or UUID
    .address("http://node1.internal:18080")   // default: auto-detected
    .forwarding_port(18080)                   // internal HTTP port (default: 18080)
    .secret("my-auth-token")                  // inter-node forwarding auth
    .mode(HaMode::Dedicated)                  // Dedicated (default) or Shared
    .coordinator_config(config)               // override lease/sync timing
    .open("/data/my.db", schema)
    .await?;
```

## Tiered storage (haqlite-turbolite)

For page-level S3 tiering (sub-250ms cold queries, transparent page eviction), use the `haqlite-turbolite` crate:

```rust
use haqlite_turbolite::{Builder, Mode};

let db = Builder::new()
    .turbolite_http("https://t3.storage.dev", "my-token")
    .manifest_endpoint("https://t3.storage.dev", "my-token")
    .lease_endpoint("https://t3.storage.dev", "my-token")
    .open("/data/my.db", schema)
    .await?;
```

`haqlite-turbolite` wraps base `haqlite` and injects a turbolite VFS for page-level tiering. Two modes:
- `Mode::Writer` (default) — Single writer with lease-per-database. Maps to haqlite's Dedicated mode.
- `Mode::MultiWriter` (experimental) — Multiple writers with per-write lease acquisition. Requires Cloud durability.

## Local mode (no HA)

For development and testing, same API, no coordination:

```rust
let db = HaQLite::local("/tmp/dev.db", schema)?;
db.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Bob".into())]).await?;
```

## Client mode

Connect to an existing HaQLite cluster from outside (no local SQLite, no cluster join):

```rust
use haqlite::{HaQLiteClient, SqlValue};

let client = HaQLiteClient::new("my-bucket")
    .prefix("myapp/")
    .db_name("my")       // must match server's db filename stem
    .connect()
    .await?;

// Discovers leader, forwards reads/writes over HTTP
client.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Eve".into())]).await?;
let row = client.query_row("SELECT COUNT(*) FROM users", &[]).await?;
let count = row[0].as_integer().unwrap();
```

## Configuration

```rust
use haqlite::{CoordinatorConfig, LeaseConfig};

let config = CoordinatorConfig {
    follower_pull_interval: Duration::from_millis(500),  // how often followers pull
    lease: Some(LeaseConfig {
        ttl_secs: 5,                   // lease time-to-live
        renew_interval: Duration::from_secs(2),
        follower_poll_interval: Duration::from_secs(1),
        required_expired_reads: 1,     // prevent premature takeover
        max_consecutive_renewal_errors: 3,
        ..LeaseConfig::new(instance_id, address)
    }),
    ..Default::default()
};
```

## Architecture

```
hadb        - coordination (leader election, role management, metrics)
walrust     - SQLite WAL replication to S3
haqlite     - HA API + write forwarding (base, no tiering)
haqlite-turbolite - HA + turbolite page-level S3 tiering
your app    - uses haqlite or haqlite-turbolite as an embedded library
```

See [hadb](https://github.com/russellromney/hadb) for the full ecosystem.

## License

Apache-2.0
