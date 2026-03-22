use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Lightweight HA metrics using atomic counters.
/// Thread-safe, zero-allocation on reads, no external dependencies.
///
/// Access via `coordinator.metrics()` and snapshot via `metrics.snapshot()`.
pub struct HaMetrics {
    // Lease operations
    pub lease_claims_attempted: AtomicU64,
    pub lease_claims_succeeded: AtomicU64,
    pub lease_claims_failed: AtomicU64,
    pub lease_renewals_succeeded: AtomicU64,
    pub lease_renewals_cas_conflict: AtomicU64,
    pub lease_renewals_error: AtomicU64,

    // Promotion lifecycle
    pub promotions_attempted: AtomicU64,
    pub promotions_succeeded: AtomicU64,
    pub promotions_aborted_catchup: AtomicU64,
    pub promotions_aborted_replicator: AtomicU64,
    pub promotions_aborted_timeout: AtomicU64,

    // Demotion
    pub demotions_cas_conflict: AtomicU64,
    pub demotions_sustained_errors: AtomicU64,

    // Follower operations
    pub follower_pulls_succeeded: AtomicU64,
    pub follower_pulls_failed: AtomicU64,
    pub follower_pulls_no_new_data: AtomicU64,

    // Timing (stored as microseconds for atomic access)
    pub last_promotion_duration_us: AtomicU64,
    pub last_catchup_duration_us: AtomicU64,
    pub last_renewal_duration_us: AtomicU64,
}

impl HaMetrics {
    pub fn new() -> Self {
        Self {
            lease_claims_attempted: AtomicU64::new(0),
            lease_claims_succeeded: AtomicU64::new(0),
            lease_claims_failed: AtomicU64::new(0),
            lease_renewals_succeeded: AtomicU64::new(0),
            lease_renewals_cas_conflict: AtomicU64::new(0),
            lease_renewals_error: AtomicU64::new(0),
            promotions_attempted: AtomicU64::new(0),
            promotions_succeeded: AtomicU64::new(0),
            promotions_aborted_catchup: AtomicU64::new(0),
            promotions_aborted_replicator: AtomicU64::new(0),
            promotions_aborted_timeout: AtomicU64::new(0),
            demotions_cas_conflict: AtomicU64::new(0),
            demotions_sustained_errors: AtomicU64::new(0),
            follower_pulls_succeeded: AtomicU64::new(0),
            follower_pulls_failed: AtomicU64::new(0),
            follower_pulls_no_new_data: AtomicU64::new(0),
            last_promotion_duration_us: AtomicU64::new(0),
            last_catchup_duration_us: AtomicU64::new(0),
            last_renewal_duration_us: AtomicU64::new(0),
        }
    }

    /// Take a point-in-time snapshot of all metrics as a plain struct.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            lease_claims_attempted: self.lease_claims_attempted.load(Ordering::Relaxed),
            lease_claims_succeeded: self.lease_claims_succeeded.load(Ordering::Relaxed),
            lease_claims_failed: self.lease_claims_failed.load(Ordering::Relaxed),
            lease_renewals_succeeded: self.lease_renewals_succeeded.load(Ordering::Relaxed),
            lease_renewals_cas_conflict: self.lease_renewals_cas_conflict.load(Ordering::Relaxed),
            lease_renewals_error: self.lease_renewals_error.load(Ordering::Relaxed),
            promotions_attempted: self.promotions_attempted.load(Ordering::Relaxed),
            promotions_succeeded: self.promotions_succeeded.load(Ordering::Relaxed),
            promotions_aborted_catchup: self.promotions_aborted_catchup.load(Ordering::Relaxed),
            promotions_aborted_replicator: self.promotions_aborted_replicator.load(Ordering::Relaxed),
            promotions_aborted_timeout: self.promotions_aborted_timeout.load(Ordering::Relaxed),
            demotions_cas_conflict: self.demotions_cas_conflict.load(Ordering::Relaxed),
            demotions_sustained_errors: self.demotions_sustained_errors.load(Ordering::Relaxed),
            follower_pulls_succeeded: self.follower_pulls_succeeded.load(Ordering::Relaxed),
            follower_pulls_failed: self.follower_pulls_failed.load(Ordering::Relaxed),
            follower_pulls_no_new_data: self.follower_pulls_no_new_data.load(Ordering::Relaxed),
            last_promotion_duration_us: self.last_promotion_duration_us.load(Ordering::Relaxed),
            last_catchup_duration_us: self.last_catchup_duration_us.load(Ordering::Relaxed),
            last_renewal_duration_us: self.last_renewal_duration_us.load(Ordering::Relaxed),
        }
    }

    // Convenience increment helpers.

    pub(crate) fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_duration(&self, target: &AtomicU64, start: Instant) {
        target.store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
    }
}

impl Default for HaMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of all metrics. Serializable.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub lease_claims_attempted: u64,
    pub lease_claims_succeeded: u64,
    pub lease_claims_failed: u64,
    pub lease_renewals_succeeded: u64,
    pub lease_renewals_cas_conflict: u64,
    pub lease_renewals_error: u64,
    pub promotions_attempted: u64,
    pub promotions_succeeded: u64,
    pub promotions_aborted_catchup: u64,
    pub promotions_aborted_replicator: u64,
    pub promotions_aborted_timeout: u64,
    pub demotions_cas_conflict: u64,
    pub demotions_sustained_errors: u64,
    pub follower_pulls_succeeded: u64,
    pub follower_pulls_failed: u64,
    pub follower_pulls_no_new_data: u64,
    pub last_promotion_duration_us: u64,
    pub last_catchup_duration_us: u64,
    pub last_renewal_duration_us: u64,
}
