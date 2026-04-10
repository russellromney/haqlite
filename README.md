# haqlite

> **Experimental.** haqlite is under active development and not yet stable. APIs will change without notice.

Embed HA SQLite in your app with one line of code. Leader election, WAL replication, write forwarding.

Part of the [hadb](https://github.com/russellromney/hadb) ecosystem for making any embedded database highly available.

## Quick start

```rust
use haqlite::{HaQLite, SqlValue};

let db = HaQLite::builder("my-bucket")
    .open("/data/my.db", "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
    .await?;

// Writes: forwarded to leader automatically
db.execute(
    "INSERT INTO users (name) VALUES (?1)",
    &[SqlValue::Text("Alice".into())],
).await?;

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
let db = HaQLite::builder("my-bucket")
    .lease_store(Arc::new(my_nats_store))
    .manifest_store(Arc::new(my_manifest_store))
    .open("/data/my.db", schema)
    .await?;

// HTTP convenience (equivalent to setting the env vars)
let db = HaQLite::builder("my-bucket")
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
let db = HaQLite::builder("my-bucket")
    .prefix("myapp/")                        // S3 key prefix (default: "haqlite/")
    .endpoint("https://t3.storage.dev")       // S3 endpoint (Tigris, MinIO, R2)
    .instance_id("node-1")                    // default: FLY_MACHINE_ID or UUID
    .address("http://node1.internal:18080")   // default: auto-detected
    .forwarding_port(18080)                   // internal HTTP port (default: 18080)
    .secret("my-auth-token")                  // inter-node forwarding auth
    .mode(HaMode::Dedicated)                  // Dedicated (default) or Shared
    .durability(Durability::Replicated)        // Replicated, Synchronous, or Eventual
    .coordinator_config(config)               // override lease/sync timing
    .open("/data/my.db", schema)
    .await?;
```

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
    sync_interval: Duration::from_millis(500),           // WAL sync frequency
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
hadb-io     - shared infrastructure (S3, retry, circuit breaker)
walrust     - SQLite WAL replication to S3
haqlite     - HA API + write forwarding + CLI
your app    - uses haqlite as an embedded library or CLI server
```

See [hadb](https://github.com/russellromney/hadb) for the full ecosystem.

## License

Apache-2.0
