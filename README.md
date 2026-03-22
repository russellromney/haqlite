# haqlite

HA building blocks for SQLite. Leases, roles, auto-promotion, self-fencing, WAL replication — all via S3.

Your app servers embed haqlite and become stateful. No separate database cluster needed. Just your app + an S3 bucket.

## How it works

Each node runs a `Coordinator` that manages a single SQLite database:

- **Leader election** via S3 conditional PUTs (compare-and-swap on ETags). No Raft, no Paxos, no consensus protocol — just S3.
- **WAL replication** via [walrust](https://crates.io/crates/walrust). Leader syncs WAL frames to S3 as LTX files. Followers pull and apply incrementals.
- **Auto-promotion**: when the leader's lease expires, a follower claims it, catches up from S3, and promotes itself.
- **Self-fencing**: if a leader loses its lease (network partition, slow renewal), it demotes itself immediately. No split-brain.

```
Node 1 (leader)                    Node 2 (follower)
┌──────────────────────┐           ┌──────────────────────┐
│  Your App            │           │  Your App            │
│    ├─ Coordinator    │           │    ├─ Coordinator    │
│    ├─ SQLite (rw)    │           │    ├─ SQLite (ro)    │
│    └─ walrust sync   │           │    └─ walrust pull   │
└──────────┬───────────┘           └──────────┬───────────┘
           └────────── S3 bucket ─────────────┘
```

## Usage

```rust
use haqlite::{Coordinator, CoordinatorConfig, S3Backend, S3LeaseStore};
use std::sync::Arc;

// Storage + lease store (both backed by S3)
let storage: Arc<dyn StorageBackend> = Arc::new(S3Backend::new(s3_client.clone(), "my-bucket", Some("ha/")));
let lease_store: Arc<dyn LeaseStore> = Arc::new(S3LeaseStore::new(s3_client, "my-bucket".into()));

let config = CoordinatorConfig::default();
let coordinator = Coordinator::new(storage, Some(lease_store), "ha/", config);

// Join the cluster — returns Leader or Follower
let role = coordinator.join("my-db", Path::new("/data/my.db")).await?;

// Listen for role changes
let mut events = coordinator.role_events();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        match event {
            RoleEvent::Promoted { db_name } => { /* now leader — open rw connection */ }
            RoleEvent::Demoted { db_name } => { /* lost leadership — close rw, go read-only */ }
            RoleEvent::Fenced { db_name } => { /* lease lost — stop serving immediately */ }
            _ => {}
        }
    }
});
```

## Features

- **No external dependencies** beyond S3. No etcd, no ZooKeeper, no sidecar.
- **Warm promotion**: followers catch up from S3 before promoting. No stale reads after failover.
- **Structured metrics**: `coordinator.metrics()` exposes lease claims, renewals, promotions, catchup timing — all lock-free atomics.
- **Fault injection**: `ControlledFailStorage` and `FaultyStorage` for chaos testing.
- **88 tests** including split-brain regression tests, chaos tests, and soak tests.

## Configuration

```rust
CoordinatorConfig {
    lease: LeaseConfig {
        ttl_secs: 10,           // Lease TTL
        renewal_interval_secs: 3, // How often the leader renews
        renewal_timeout_secs: 5,  // Max time for a renewal attempt
    },
    replication: ReplicationConfig {
        sync_interval: Duration::from_secs(1), // WAL sync frequency
        ..Default::default()
    },
    replicator_timeout: Duration::from_secs(30),
    max_consecutive_renewal_errors: 3,
}
```

## Architecture

haqlite is one layer in a stack:

```
walrust (WAL sync to S3)
  └─ haqlite (HA coordination: leases, roles, promotion)
       └─ your app (HTTP server, gRPC, whatever)
```

walrust handles the data plane (WAL frames → LTX files → S3). haqlite handles the control plane (who is leader, when to promote, when to fence). Your app handles the application logic.

## License

Apache-2.0
