# haqlite

> **Experimental.** haqlite is under active development and not yet stable. APIs will change without notice.

HA SQLite with one line of code. Leader election, WAL replication, write forwarding — just your app + an S3 bucket.

## Quick start

```rust
use haqlite::{HaQLite, SqlValue};

let db = HaQLite::builder("my-bucket")
    .open("/data/my.db", "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
    .await?;

// Writes — forwarded to leader automatically
db.execute(
    "INSERT INTO users (name) VALUES (?1)",
    &[SqlValue::Text("Alice".into())],
).await?;

// Reads — always local
let count: i64 = db.query_row("SELECT COUNT(*) FROM users", &[], |r| r.get(0))?;

// Clean shutdown
db.close().await?;
```

That's it. No S3 client setup, no lease configuration, no write proxy, no role event handlers. HaQLite handles everything internally.

## How it works

```
Node 1 (leader)                    Node 2 (follower)
┌──────────────────────┐           ┌──────────────────────┐
│  Your App            │           │  Your App            │
│         │            │           │         │            │
│    HaQLite           │           │    HaQLite           │
│    ├─ execute() ─────┤──local    │    ├─ execute() ─────┤──► HTTP to leader
│    ├─ query_row() ───┤──local    │    ├─ query_row() ───┤──local (read replica)
│    ├─ Coordinator    │           │    ├─ Coordinator    │
│    ├─ SQLite (rw)    │           │    ├─ SQLite (ro)    │
│    └─ walrust sync   │           │    └─ walrust pull   │
└──────────┬───────────┘           └──────────┬───────────┘
           └────────── S3 bucket ─────────────┘
```

- **Leader election** via S3 conditional PUTs (compare-and-swap on ETags). No Raft, no Paxos — just S3.
- **WAL replication** via [walrust](https://crates.io/crates/walrust). Leader syncs WAL frames to S3, followers pull and apply.
- **Write forwarding**: `execute()` on a follower transparently forwards to the leader via an internal HTTP server. Your app doesn't need to know who the leader is.
- **Auto-promotion**: when the leader dies, a follower claims the lease, catches up from S3, and promotes itself.
- **Self-fencing**: if a leader loses its lease (partition, slow renewal), it demotes itself immediately. No split-brain.

## Builder options

```rust
let db = HaQLite::builder("my-bucket")
    .prefix("myapp/")                        // S3 key prefix (default: "haqlite/")
    .endpoint("https://t3.storage.dev")       // S3 endpoint (Tigris, MinIO, R2)
    .instance_id("node-1")                    // default: FLY_MACHINE_ID or UUID
    .address("http://node1.internal:18080")   // default: auto-detected
    .forwarding_port(18080)                   // internal HTTP port (default: 18080)
    .secret("my-auth-token")                  // inter-node forwarding auth
    .lease_store(my_nats_store)               // custom lease backend (default: S3)
    .coordinator_config(config)               // override lease/sync timing
    .open("/data/my.db", schema)
    .await?;
```

## Local mode (no HA)

For development and testing — same API, no S3:

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

// Discovers leader from S3, forwards reads/writes over HTTP
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

## Custom lease store

By default, haqlite uses S3 conditional PUTs for leader election. For faster failover (2-5ms vs 50-200ms), plug in a NATS lease store:

```rust
use hadb_lease_nats::NatsLeaseStore;

let nats_store = NatsLeaseStore::connect("nats://localhost:4222", "hadb-leases").await?;

let db = HaQLite::builder("my-bucket")
    .lease_store(Arc::new(nats_store))
    .open("/data/my.db", schema)
    .await?;
```

Or in CLI mode, just set the env var (requires `--features nats-lease`):

```bash
WAL_LEASE_NATS_URL=nats://localhost:4222 haqlite serve
```

If NATS is unreachable at startup, haqlite logs an error and falls back to S3 leases.

## What it does

- S3 is the only required dependency (no etcd, no ZooKeeper, no sidecar)
- Pluggable lease store: S3 by default, NATS via `nats-lease` feature, or any `LeaseStore` impl
- `query_row()` is sync (always local), `execute()` is async (forwards to leader if follower)
- Followers catch up from S3 before promoting (warm promotion)
- Forwarding retries with exponential backoff, skips 4xx
- Read concurrency bounded by semaphore (default 32)
- Structured error types (`HaQLiteError`) for matching on failure modes
- Follower readiness tracking (`is_caught_up()`, `replay_position()`)
- Prometheus metrics for lease operations, promotions, catchup timing, readiness

## Architecture

haqlite is one layer in a stack:

```
walrust (WAL sync to S3)
  └─ haqlite (HA coordination + one-liner API)
       └─ your app (HTTP server, gRPC, whatever)
```

haqlite is the first implementation of **[hadb](https://github.com/russellromney/hadb)**, a generic framework for making any embedded database highly available.

## License

Apache-2.0
