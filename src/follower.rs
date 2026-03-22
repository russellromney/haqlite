use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{broadcast, watch};

use walrust::storage::StorageBackend;
use walrust::Replicator;

use crate::lease::DbLease;
use crate::metrics::HaMetrics;
use crate::node_registry::{NodeRegistration, NodeRegistry};
use crate::types::RoleEvent;

/// Run the follower pull loop: poll S3 for new LTX files and apply them.
///
/// Returns the final TXID when cancelled via `cancel_rx`.
/// Also updates `txid_out` atomically so the lease monitor can read it for warm promotion.
pub(crate) async fn run_follower_loop(
    storage: Arc<dyn StorageBackend>,
    prefix: String,
    db_name: String,
    db_path: PathBuf,
    initial_txid: u64,
    poll_interval: Duration,
    txid_out: Arc<AtomicU64>,
    mut cancel_rx: watch::Receiver<bool>,
    metrics: Arc<HaMetrics>,
) -> Result<u64> {
    let mut current_txid = initial_txid;
    txid_out.store(current_txid, Ordering::SeqCst);
    let mut interval = tokio::time::interval(poll_interval);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match walrust::sync::pull_incremental(
                    storage.as_ref(),
                    &prefix,
                    &db_name,
                    &db_path,
                    current_txid,
                ).await {
                    Ok(new_txid) => {
                        if new_txid > current_txid {
                            tracing::debug!(
                                "Follower '{}': pulled TXID {} → {}",
                                db_name, current_txid, new_txid
                            );
                            current_txid = new_txid;
                            txid_out.store(current_txid, Ordering::SeqCst);
                            metrics.inc(&metrics.follower_pulls_succeeded);
                        } else {
                            metrics.inc(&metrics.follower_pulls_no_new_data);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Follower '{}': pull failed: {}", db_name, e);
                        metrics.inc(&metrics.follower_pulls_failed);
                    }
                }
            }
            _ = cancel_rx.changed() => {
                tracing::info!("Follower '{}': cancelled at TXID {}", db_name, current_txid);
                return Ok(current_txid);
            }
        }
    }
}

/// Run the lease monitor loop: watch for leader death and auto-promote.
///
/// Requires `required_expired_reads` consecutive expired reads before attempting
/// to claim the lease (prevents premature takeover on transient S3 glitches).
///
/// On promotion: stops follower pull, does warm catch-up via pull_incremental,
/// starts leader sync, emits Promoted.
///
/// `follower_stop_tx` is a SEPARATE channel from `cancel_rx` — it only stops the
/// follower pull loop. The monitor's own `cancel_rx` (from the coordinator) is
/// unaffected when we send on follower_stop_tx, eliminating the shared-channel bug.
pub(crate) async fn run_lease_monitor(
    mut lease: DbLease,
    replicator: Arc<Replicator>,
    storage: Arc<dyn StorageBackend>,
    prefix: String,
    db_name: String,
    db_path: PathBuf,
    follower_txid: Arc<AtomicU64>,
    follower_stop_tx: watch::Sender<bool>,
    role_tx: broadcast::Sender<RoleEvent>,
    poll_interval: Duration,
    renew_interval: Duration,
    required_expired_reads: u32,
    leader_address: Arc<tokio::sync::RwLock<String>>,
    // Shared role ref — updated immediately on promotion/demotion
    role_ref: Arc<crate::coordinator::AtomicRole>,
    self_address: String,
    max_consecutive_renewal_errors: u32,
    replicator_timeout: Duration,
    mut cancel_rx: watch::Receiver<bool>,
    node_registry: Option<Arc<dyn NodeRegistry>>,
    metrics: Arc<HaMetrics>,
) -> Result<bool> {
    let mut interval = tokio::time::interval(poll_interval);
    let mut consecutive_expired: u32 = 0;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let lease_data = lease.read().await;

                // Handle sleeping: leader signaled scale-to-zero.
                if let Ok(Some((ref data, _))) = lease_data {
                    if data.sleeping {
                        tracing::info!(
                            "Lease monitor '{}': leader signaled sleep, emitting Sleeping",
                            db_name
                        );
                        let _ = role_tx.send(RoleEvent::Sleeping {
                            db_name: db_name.clone(),
                        });
                        return Ok(false);
                    }
                }

                let is_expired = match &lease_data {
                    Ok(None) => true,
                    Ok(Some((data, _etag))) => {
                        if !data.is_expired() {
                            *leader_address.write().await = data.address.clone();
                            // Heartbeat node registration with current leader session.
                            if let Some(ref registry) = node_registry {
                                let reg = NodeRegistration {
                                    instance_id: lease.instance_id().to_string(),
                                    address: self_address.clone(),
                                    role: "follower".to_string(),
                                    leader_session_id: data.session_id.clone(),
                                    last_seen: chrono::Utc::now().timestamp() as u64,
                                };
                                if let Err(e) = registry.register(&prefix, &db_name, &reg).await {
                                    tracing::warn!(
                                        "Lease monitor '{}': heartbeat registration failed: {}",
                                        db_name, e
                                    );
                                }
                            }
                        }
                        data.is_expired()
                    }
                    Err(e) => {
                        tracing::error!(
                            "Lease monitor '{}': read failed: {}", db_name, e
                        );
                        consecutive_expired = 0;
                        false
                    }
                };

                if is_expired {
                    consecutive_expired += 1;
                    if consecutive_expired < required_expired_reads {
                        tracing::debug!(
                            "Lease monitor '{}': expired read {}/{}, waiting",
                            db_name, consecutive_expired, required_expired_reads
                        );
                        continue;
                    }

                    metrics.inc(&metrics.lease_claims_attempted);
                    match lease.try_claim().await {
                        Ok(crate::types::Role::Leader) => {
                            tracing::info!(
                                "Lease monitor '{}': claimed lease — promoting to leader",
                                db_name
                            );
                            metrics.inc(&metrics.lease_claims_succeeded);
                            metrics.inc(&metrics.promotions_attempted);
                            let promotion_start = std::time::Instant::now();
                            consecutive_expired = 0;

                            // 1. Stop the follower pull loop via SEPARATE channel.
                            //    This does NOT affect our cancel_rx (different channel).
                            let _ = follower_stop_tx.send(true);

                            // 2. Warm promotion: catch up via pull_incremental.
                            //    If catch-up fails or times out, abort promotion — we
                            //    can't lead with stale data. Release lease and retry.
                            let txid = follower_txid.load(Ordering::SeqCst);
                            if txid > 0 {
                                tracing::info!(
                                    "Lease monitor '{}': warm promotion, catching up from TXID {}",
                                    db_name, txid
                                );
                                let catchup_start = std::time::Instant::now();
                                let catchup_result = tokio::time::timeout(
                                    replicator_timeout,
                                    walrust::sync::pull_incremental(
                                        storage.as_ref(),
                                        &prefix,
                                        &db_name,
                                        &db_path,
                                        txid,
                                    ),
                                ).await;
                                match catchup_result {
                                    Ok(Ok(new_txid)) => {
                                        metrics.record_duration(&metrics.last_catchup_duration_us, catchup_start);
                                        tracing::info!(
                                            "Lease monitor '{}': caught up TXID {} → {}",
                                            db_name, txid, new_txid
                                        );
                                    }
                                    Ok(Err(e)) => {
                                        tracing::error!(
                                            "Lease monitor '{}': warm catch-up failed: {} — aborting promotion, \
                                             pull loop is stopped (S3 likely down), will retry on next lease expiry",
                                            db_name, e
                                        );
                                        metrics.inc(&metrics.promotions_aborted_catchup);
                                        let _ = lease.release().await;
                                        continue;
                                    }
                                    Err(_) => {
                                        tracing::error!(
                                            "Lease monitor '{}': warm catch-up timed out after {:?} — aborting promotion",
                                            db_name, replicator_timeout
                                        );
                                        metrics.inc(&metrics.promotions_aborted_timeout);
                                        let _ = lease.release().await;
                                        continue;
                                    }
                                }
                            }

                            // 3. Start leader sync (snapshot + WAL push).
                            //    If this fails or times out, release lease and continue.
                            //    Promoted event is NOT sent until sync starts successfully.
                            let add_result = tokio::time::timeout(
                                replicator_timeout,
                                replicator.add(&db_name, &db_path),
                            ).await;
                            match add_result {
                                Ok(Ok(())) => {}
                                Ok(Err(e)) => {
                                    tracing::error!(
                                        "Lease monitor '{}': failed to start leader sync: {} — aborting promotion, \
                                         pull loop is stopped, will retry on next lease expiry",
                                        db_name, e
                                    );
                                    metrics.inc(&metrics.promotions_aborted_replicator);
                                    let _ = lease.release().await;
                                    continue;
                                }
                                Err(_) => {
                                    tracing::error!(
                                        "Lease monitor '{}': replicator.add() timed out after {:?} — aborting promotion",
                                        db_name, replicator_timeout
                                    );
                                    metrics.inc(&metrics.promotions_aborted_timeout);
                                    let _ = lease.release().await;
                                    continue;
                                }
                            }

                            // 4. Deregister from node registry (now leader, not a follower).
                            if let Some(ref registry) = node_registry {
                                if let Err(e) = registry
                                    .deregister(&prefix, &db_name, lease.instance_id())
                                    .await
                                {
                                    tracing::warn!(
                                        "Lease monitor '{}': deregister on promotion failed: {}",
                                        db_name, e
                                    );
                                }
                            }

                            // 5. Update shared role + address IMMEDIATELY.
                            role_ref.store(crate::types::Role::Leader);
                            *leader_address.write().await = self_address.clone();

                            // 6. Emit Promoted event — ONLY after replicator.add() succeeded.
                            metrics.inc(&metrics.promotions_succeeded);
                            metrics.record_duration(&metrics.last_promotion_duration_us, promotion_start);
                            let _ = role_tx.send(RoleEvent::Promoted {
                                db_name: db_name.clone(),
                            });

                            // 7. Switch to lease renewal mode.
                            let demoted = run_leader_renewal(
                                &mut lease,
                                &replicator,
                                &db_name,
                                &role_tx,
                                renew_interval,
                                &mut cancel_rx,
                                max_consecutive_renewal_errors,
                                replicator_timeout,
                                metrics.clone(),
                            ).await;

                            if demoted {
                                role_ref.store(crate::types::Role::Follower);
                            }

                            return Ok(true); // promoted
                        }
                        Ok(crate::types::Role::Follower) => {
                            metrics.inc(&metrics.lease_claims_failed);
                            consecutive_expired = 0;
                        }
                        Err(e) => {
                            metrics.inc(&metrics.lease_claims_failed);
                            tracing::error!(
                                "Lease monitor '{}': claim failed: {}", db_name, e
                            );
                        }
                    }
                } else {
                    consecutive_expired = 0;
                }
            }
            _ = cancel_rx.changed() => {
                tracing::info!("Lease monitor '{}': cancelled", db_name);
                return Ok(false);
            }
        }
    }
}

/// Standalone leader lease renewal loop. Called by the coordinator for initial leaders.
///
/// Returns true if demoted (CAS conflict or sustained errors), false if cancelled.
pub async fn run_leader_renewal_standalone(
    mut lease: DbLease,
    replicator: &Arc<Replicator>,
    db_name: &str,
    role_tx: &broadcast::Sender<RoleEvent>,
    renew_interval: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
    max_consecutive_errors: u32,
    replicator_timeout: Duration,
    metrics: Arc<HaMetrics>,
) -> bool {
    run_leader_renewal(
        &mut lease, replicator, db_name, role_tx,
        renew_interval, cancel_rx, max_consecutive_errors, replicator_timeout, metrics,
    ).await
}

/// Leader lease renewal loop. Runs until cancelled, CAS conflict, or sustained errors.
///
/// Returns true if demoted, false if cancelled (graceful leave).
///
/// **Self-demotion on sustained errors:** If `max_consecutive_errors` consecutive
/// renewals fail with transient errors, the leader self-demotes. During an S3 outage,
/// the lease is expiring while we retry — another node may claim it, creating
/// split-brain. Self-demotion prevents this.
async fn run_leader_renewal(
    lease: &mut DbLease,
    replicator: &Arc<Replicator>,
    db_name: &str,
    role_tx: &broadcast::Sender<RoleEvent>,
    renew_interval: Duration,
    cancel_rx: &mut watch::Receiver<bool>,
    max_consecutive_errors: u32,
    replicator_timeout: Duration,
    metrics: Arc<HaMetrics>,
) -> bool {
    let mut interval = tokio::time::interval(renew_interval);
    let mut consecutive_errors: u32 = 0;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let renewal_start = std::time::Instant::now();
                match lease.renew().await {
                    Ok(true) => {
                        metrics.inc(&metrics.lease_renewals_succeeded);
                        metrics.record_duration(&metrics.last_renewal_duration_us, renewal_start);
                        consecutive_errors = 0;
                    }
                    Ok(false) => {
                        // CAS conflict — another node holds the lease.
                        metrics.inc(&metrics.lease_renewals_cas_conflict);
                        metrics.inc(&metrics.demotions_cas_conflict);
                        tracing::error!(
                            "Leader '{}': lease renewal CAS conflict — demoting",
                            db_name
                        );
                        if tokio::time::timeout(replicator_timeout, replicator.remove(db_name)).await.is_err() {
                            tracing::error!(
                                "Leader '{}': replicator.remove() timed out during demotion", db_name
                            );
                        }
                        let _ = role_tx.send(RoleEvent::Demoted {
                            db_name: db_name.to_string(),
                        });
                        return true;
                    }
                    Err(e) => {
                        metrics.inc(&metrics.lease_renewals_error);
                        consecutive_errors += 1;
                        if consecutive_errors >= max_consecutive_errors {
                            metrics.inc(&metrics.demotions_sustained_errors);
                            tracing::error!(
                                "Leader '{}': {} consecutive renewal errors — \
                                 self-demoting to prevent split-brain",
                                db_name, consecutive_errors
                            );
                            if tokio::time::timeout(replicator_timeout, replicator.remove(db_name)).await.is_err() {
                                tracing::error!(
                                    "Leader '{}': replicator.remove() timed out during self-demotion", db_name
                                );
                            }
                            let _ = role_tx.send(RoleEvent::Demoted {
                                db_name: db_name.to_string(),
                            });
                            return true;
                        }
                        tracing::error!(
                            "Leader '{}': lease renewal error ({}/{}), will retry: {}",
                            db_name, consecutive_errors, max_consecutive_errors, e
                        );
                    }
                }
            }
            _ = cancel_rx.changed() => {
                tracing::info!("Leader '{}': renewal cancelled", db_name);
                return false;
            }
        }
    }
}
