//! haqlite: HA SQLite with one line of code.
//!
//! Embeddable HA for SQLite — leader election, WAL replication, write forwarding,
//! automatic failover. Just your app servers + an S3 bucket.
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! use haqlite::{HaQLite, SqlValue};
//!
//! let db = HaQLite::builder("my-bucket")
//!     .open("/data/my.db", "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
//!     .await?;
//!
//! // Writes: forwarded to leader automatically
//! db.execute("INSERT INTO users (name) VALUES (?1)", &[SqlValue::Text("Alice".into())]).await?;
//!
//! // Reads: always local
//! let count: i64 = db.query_row("SELECT COUNT(*) FROM users", &[], |r| r.get(0))?;
//! # Ok(())
//! # }
//! ```

pub mod cli_config;
pub mod client;
pub mod database;
pub mod error;
pub mod follower_behavior;
pub mod forwarding;
pub mod hrana;
pub mod ops;
pub mod replicator;
pub mod serve;

// Re-export HaQLite as the primary API.
pub use database::{HaQLite, HaQLiteBuilder};
pub use error::HaQLiteError;
pub use client::{HaQLiteClient, HaQLiteClientBuilder};
pub use forwarding::SqlValue;

// Re-export rusqlite for query params.
pub use rusqlite;

// Re-export hadb types.
pub use hadb::{
    Coordinator, CoordinatorConfig, HaMetrics, HaNode, HaNodeConfig,
    InMemoryLeaseStore, LeaseConfig, LeaseData, LeaseStore, MetricsSnapshot,
    NodeRegistration, NodeRegistry, Role, RoleEvent,
};
pub use hadb::coordinator::JoinResult;

// Re-export hadb-lease-s3 implementations.
pub use hadb_lease_s3::{S3LeaseStore, S3NodeRegistry, S3StorageBackend};

// Re-export SQLite-specific implementations.
pub use follower_behavior::SqliteFollowerBehavior;
pub use replicator::SqliteReplicator;

// Re-export walrust types so downstream crates don't need a direct walrust dependency.
pub use walrust::S3Backend as WalrustS3Backend;
pub use walrust::StorageBackend as WalrustStorageBackend;
pub use walrust::ReplicationConfig;
