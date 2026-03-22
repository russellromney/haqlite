use std::time::Duration;

/// Database role in the HA cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Leader => write!(f, "Leader"),
            Role::Follower => write!(f, "Follower"),
        }
    }
}

/// Events emitted when a database's role changes.
///
/// Matches walrust's `ElectionHooks` lifecycle:
/// - `on_promoted` → `Promoted`
/// - `on_demoted` → `Demoted`
/// - `on_sleep` → `Sleeping`
#[derive(Debug, Clone)]
pub enum RoleEvent {
    /// Database joined the cluster with the given role.
    Joined { db_name: String, role: Role },
    /// Follower was promoted to leader (previous leader died/left).
    Promoted { db_name: String },
    /// Leader was demoted to follower (lost lease via CAS conflict).
    /// Consumer should stop writes and switch to read-only.
    Demoted { db_name: String },
    /// Leader lost its lease — must stop serving immediately.
    /// Stricter than Demoted: the database should be fully disconnected.
    Fenced { db_name: String },
    /// Leader signaled sleep (Fly scale-to-zero). Follower should shut down gracefully.
    Sleeping { db_name: String },
}

/// Configuration for S3 CAS lease coordination.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// Unique identifier for this instance (e.g. FLY_MACHINE_ID).
    pub instance_id: String,
    /// Network address for this instance (for client discovery).
    pub address: String,
    /// Lease time-to-live in seconds. Default: 5.
    pub ttl_secs: u64,
    /// How often to renew the lease. Default: 2s.
    pub renew_interval: Duration,
    /// How often followers poll the lease for leader death. Default: 1s.
    pub follower_poll_interval: Duration,
    /// Number of consecutive expired reads required before a follower can claim.
    /// Prevents premature takeover on transient S3 glitches. Default: 1.
    pub required_expired_reads: u32,
    /// Max consecutive renewal errors before self-demoting.
    /// Prevents split-brain during sustained S3 outages: if we can't renew,
    /// our lease is expiring and another node may claim it. Default: 3.
    pub max_consecutive_renewal_errors: u32,
}

impl LeaseConfig {
    pub fn new(instance_id: String, address: String) -> Self {
        Self {
            instance_id,
            address,
            ttl_secs: 5,
            renew_interval: Duration::from_secs(2),
            follower_poll_interval: Duration::from_secs(1),
            required_expired_reads: 1,
            max_consecutive_renewal_errors: 3,
        }
    }
}

/// Top-level configuration for the Coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// WAL sync interval (passed to walrust Replicator). Default: 1s.
    pub sync_interval: Duration,
    /// Snapshot interval (passed to walrust Replicator). Default: 1h.
    pub snapshot_interval: Duration,
    /// Lease config. None = no leases, always Leader (single-node mode).
    pub lease: Option<LeaseConfig>,
    /// How often followers poll for new LTX files. Default: 1s.
    pub follower_pull_interval: Duration,
    /// Timeout for replicator.add() and pull_incremental during promotion.
    /// Prevents hanging forever if S3 is slow or unresponsive.
    /// Default: 30s. Should be less than lease TTL for safety, but this is
    /// a safety net — self-demotion on renewal errors handles stale leases.
    pub replicator_timeout: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(1),
            snapshot_interval: Duration::from_secs(3600),
            lease: None,
            follower_pull_interval: Duration::from_secs(1),
            replicator_timeout: Duration::from_secs(30),
        }
    }
}
