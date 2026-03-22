//! haqlite: HA building blocks for SQLite.
//!
//! Provides lease-based role coordination for SQLite databases replicated via walrust.
//! Three clean layers: walrust (WAL sync) → haqlite (HA coordination) → your app.
//!
//! ```ignore
//! let coordinator = Coordinator::new(storage, Some(lease_store), "ha/", config);
//! let role = coordinator.join("my-db", Path::new("/data/my.db")).await?;
//! // role == Leader or Follower
//!
//! // Listen for role changes (promotion, fencing)
//! let mut events = coordinator.role_events();
//! tokio::spawn(async move {
//!     while let Ok(event) = events.recv().await {
//!         match event {
//!             RoleEvent::Promoted { db_name } => { /* now leader */ }
//!             RoleEvent::Fenced { db_name } => { /* stop serving! */ }
//!             _ => {}
//!         }
//!     }
//! });
//! ```

pub mod coordinator;
pub mod follower;
pub mod lease;
pub mod lease_store;
pub mod metrics;
pub mod node_registry;
pub mod types;

// Re-export key types for convenience.
pub use coordinator::Coordinator;
pub use lease::{DbLease, LeaseData};
pub use lease_store::{CasResult, InMemoryLeaseStore, LeaseStore, S3LeaseStore};
pub use metrics::{HaMetrics, MetricsSnapshot};
pub use node_registry::{InMemoryNodeRegistry, NodeRegistration, NodeRegistry, S3NodeRegistry};
pub use types::{CoordinatorConfig, LeaseConfig, Role, RoleEvent};

// Re-export walrust types that consumers need.
pub use walrust::storage::{S3Backend, StorageBackend};
pub use walrust::sync::ReplicationConfig;
pub use walrust::Replicator;
