//! HTTP writer for haqlite HA experiment.
//!
//! Discovers the leader via S3 (reads the haqlite lease) and sends writes.
//! On failure, polls S3 for a new leader session and reconnects.
//!
//! Usage:
//!   haqlite-ha-writer --bucket my-bucket --prefix ha-test/

use clap::Parser;
use std::sync::Arc;
use tracing::{error, info, warn};

use haqlite::{DbLease, LeaseStore, S3LeaseStore};

const DB_NAME: &str = "ha-db";

#[derive(Parser)]
#[command(name = "haqlite-ha-writer")]
#[command(about = "HTTP writer for haqlite HA experiment — discovers leader via S3 lease")]
struct Args {
    /// S3 bucket containing the lease
    #[arg(long, env = "HA_BUCKET")]
    bucket: String,

    /// S3 prefix (e.g., "ha-test/")
    #[arg(long, env = "HA_PREFIX", default_value = "ha-test/")]
    prefix: String,

    /// S3 endpoint (for Tigris/MinIO)
    #[arg(long, env = "HA_S3_ENDPOINT")]
    endpoint: Option<String>,

    /// Write interval in milliseconds
    #[arg(long, default_value = "100")]
    interval_ms: u64,

    /// Total writes to perform before stopping (0 = unlimited)
    #[arg(long, default_value = "0")]
    total_writes: u64,

    /// Verify data integrity on all nodes after completion.
    /// Provide comma-separated node addresses (e.g., "http://localhost:9001,http://localhost:9002")
    #[arg(long)]
    verify_nodes: Option<String>,

    /// Enable benchmark mode: print a tab-separated summary line suitable for
    /// collecting across multiple runs with different --sync-interval values.
    /// Output: sync_interval_ms, writes, errors, elapsed_s, writes_per_s, promotion_us, catchup_us
    #[arg(long)]
    bench: bool,
}

/// Discover the leader by reading the haqlite lease from S3.
/// Returns (address, session_id) if an active, non-sleeping leader exists.
async fn discover_leader(
    lease_store: Arc<dyn LeaseStore>,
    prefix: &str,
) -> anyhow::Result<Option<(String, String)>> {
    let lease = DbLease::new(
        lease_store,
        prefix,
        DB_NAME,
        "writer",     // dummy instance_id — we never claim
        "writer",     // dummy address
        5,
    );

    match lease.read().await? {
        Some((data, _etag)) => {
            if data.sleeping {
                Ok(None) // sleeping = no active leader
            } else if data.is_expired() {
                Ok(None) // expired = no active leader
            } else {
                Ok(Some((data.address.clone(), data.session_id.clone())))
            }
        }
        None => Ok(None),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!("=== haqlite HA HTTP writer (S3 discovery) ===");
    info!("Bucket: {}", args.bucket);
    info!("Prefix: {}", args.prefix);

    // Build S3 client for lease reading
    let s3_config = if let Some(ref endpoint) = args.endpoint {
        aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .load()
            .await
    } else {
        aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await
    };
    let s3_client = aws_sdk_s3::Client::new(&s3_config);
    let lease_store: Arc<dyn LeaseStore> = Arc::new(S3LeaseStore::new(s3_client, args.bucket.clone()));

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        let _ = shutdown_tx.send(true);
    });

    // Discover initial leader
    let (mut leader_address, mut session_id) = loop {
        match discover_leader(lease_store.clone(), &args.prefix).await {
            Ok(Some((addr, sid))) => {
                info!("Discovered leader: {} (session {})", addr, sid);
                break (addr, sid);
            }
            Ok(None) => {
                info!("No active leader yet, waiting...");
            }
            Err(e) => {
                warn!("S3 discovery failed: {}", e);
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
            _ = shutdown_rx.changed() => {
                info!("Shutdown before leader found");
                return Ok(());
            }
        }
    };

    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(args.interval_ms));
    let mut writes: u64 = 0;
    let mut errors: u64 = 0;
    let mut consecutive_errors: u64 = 0;
    let start = std::time::Instant::now();

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let write_url = format!("{}/write", leader_address);
                match http_client.post(&write_url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        writes += 1;
                        consecutive_errors = 0;
                        if writes % 100 == 0 {
                            let elapsed = start.elapsed().as_secs_f64();
                            let rate = writes as f64 / elapsed;
                            let body: serde_json::Value = resp.json().await.unwrap_or_default();
                            info!("{} writes ({:.0}/s), {} errors, leader={}, count={}",
                                writes, rate, errors, leader_address,
                                body.get("count").and_then(|v| v.as_i64()).unwrap_or(0),
                            );
                        }
                    }
                    Ok(resp) => {
                        errors += 1;
                        consecutive_errors += 1;
                        if consecutive_errors <= 3 || consecutive_errors % 10 == 0 {
                            warn!("Write failed: HTTP {} (consecutive #{})",
                                resp.status(), consecutive_errors);
                        }
                    }
                    Err(e) => {
                        errors += 1;
                        consecutive_errors += 1;
                        if consecutive_errors <= 3 || consecutive_errors % 10 == 0 {
                            warn!("Write failed: {} (consecutive #{})",
                                e, consecutive_errors);
                        }
                    }
                }

                // After 3 consecutive failures, poll S3 for a new leader session
                if consecutive_errors >= 3 {
                    match discover_leader(lease_store.clone(), &args.prefix).await {
                        Ok(Some((addr, sid))) if sid != session_id => {
                            info!("New leader session: {} at {} (was {})",
                                sid, addr, leader_address);
                            leader_address = addr;
                            session_id = sid;
                            consecutive_errors = 0;
                        }
                        Ok(Some(_)) => {
                            // Same session — leader hasn't changed yet
                        }
                        Ok(None) => {
                            // No active leader — keep polling
                        }
                        Err(e) => {
                            if consecutive_errors % 10 == 0 {
                                warn!("S3 discovery error: {}", e);
                            }
                        }
                    }
                }

                if consecutive_errors > 300 {
                    error!("Too many consecutive errors ({}), exiting", consecutive_errors);
                    break;
                }

                // Stop after total_writes if configured.
                if args.total_writes > 0 && writes >= args.total_writes {
                    info!("Reached {} writes, stopping", args.total_writes);
                    break;
                }
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 { writes as f64 / elapsed } else { 0.0 };
    info!(
        "Writer done: {} writes, {} errors, {:.1}s elapsed, {:.0} writes/s",
        writes, errors, elapsed, rate
    );

    // Verify data integrity on all nodes.
    if let Some(ref nodes_str) = args.verify_nodes {
        // Wait for replication to catch up.
        info!("Waiting 3s for replication to catch up before verification...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        let nodes: Vec<&str> = nodes_str.split(',').map(|s| s.trim()).collect();
        let mut all_ok = true;

        for node in &nodes {
            let verify_url = format!("{}/verify", node);
            match http_client.get(&verify_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let body: serde_json::Value = resp.json().await.unwrap_or_default();
                    let ok = body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                    let count = body.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
                    let gaps = body.get("gap_count").and_then(|v| v.as_i64()).unwrap_or(0);
                    let dups = body.get("duplicates").and_then(|v| v.as_i64()).unwrap_or(0);

                    if ok {
                        info!("VERIFY {}: OK ({} rows, 0 gaps, 0 duplicates)", node, count);
                    } else {
                        error!(
                            "VERIFY {}: FAILED ({} rows, {} gaps, {} dups, sample: {})",
                            node, count, gaps, dups,
                            body.get("gaps_sample").unwrap_or(&serde_json::json!([]))
                        );
                        all_ok = false;
                    }
                }
                Ok(resp) => {
                    error!("VERIFY {}: HTTP {}", node, resp.status());
                    all_ok = false;
                }
                Err(e) => {
                    error!("VERIFY {}: connection error: {}", node, e);
                    all_ok = false;
                }
            }

            // Also fetch metrics.
            let metrics_url = format!("{}/metrics", node);
            match http_client.get(&metrics_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let body: serde_json::Value = resp.json().await.unwrap_or_default();
                    info!("METRICS {}: {}", node, body);
                }
                _ => {}
            }
        }

        if all_ok {
            info!("ALL NODES VERIFIED OK");
        } else {
            error!("VERIFICATION FAILED — data integrity issue detected");
            std::process::exit(1);
        }
    }

    // Benchmark summary: collect metrics from all verify_nodes and print TSV line.
    if args.bench {
        let mut promotion_us: u64 = 0;
        let mut catchup_us: u64 = 0;
        let mut promotions: u64 = 0;

        if let Some(ref nodes_str) = args.verify_nodes {
            let nodes: Vec<&str> = nodes_str.split(',').map(|s| s.trim()).collect();
            for node in &nodes {
                let metrics_url = format!("{}/metrics", node);
                if let Ok(resp) = http_client.get(&metrics_url).send().await {
                    if let Ok(body) = resp.json::<serde_json::Value>().await {
                        let p = body.get("last_promotion_duration_us")
                            .and_then(|v| v.as_u64()).unwrap_or(0);
                        let c = body.get("last_catchup_duration_us")
                            .and_then(|v| v.as_u64()).unwrap_or(0);
                        let n = body.get("promotions_succeeded")
                            .and_then(|v| v.as_u64()).unwrap_or(0);
                        if p > promotion_us { promotion_us = p; }
                        if c > catchup_us { catchup_us = c; }
                        promotions += n;
                    }
                }
            }
        }

        // Print header on first run hint, then data line.
        // Format: TSV for easy paste into spreadsheets.
        println!("# sync_interval_ms\twrites\terrors\telapsed_s\twrites_per_s\tpromotion_us\tcatchup_us\tpromotions");
        println!(
            "{}\t{}\t{}\t{:.1}\t{:.0}\t{}\t{}\t{}",
            args.interval_ms, writes, errors, elapsed, rate,
            promotion_us, catchup_us, promotions
        );
    }

    Ok(())
}
