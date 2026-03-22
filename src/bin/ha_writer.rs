//! HTTP writer for haqlite HA experiment.
//!
//! Uses HaQLiteClient to discover the leader and send writes.
//! On failure, HaQLiteClient re-discovers the leader automatically.
//!
//! Usage:
//!   haqlite-ha-writer --bucket my-bucket --prefix ha-test/ --db-name ha

use clap::Parser;
use tracing::{error, info, warn};

use haqlite::{HaQLiteClient, SqlValue};

#[derive(Parser)]
#[command(name = "haqlite-ha-writer")]
#[command(about = "HTTP writer for haqlite HA experiment — uses HaQLiteClient")]
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

    /// Database name — must match the file stem of the server's --db path.
    /// E.g. if the server uses --db /tmp/node1/ha.db, set --db-name ha
    #[arg(long, default_value = "db")]
    db_name: String,

    /// Write interval in milliseconds
    #[arg(long, default_value = "100")]
    interval_ms: u64,

    /// Total writes to perform before stopping (0 = unlimited)
    #[arg(long, default_value = "0")]
    total_writes: u64,

    /// Verify data integrity on all nodes after completion.
    /// Provide comma-separated app server addresses (e.g., "http://localhost:9001,http://localhost:9002")
    #[arg(long)]
    verify_nodes: Option<String>,

    /// Enable benchmark mode: print a tab-separated summary line.
    #[arg(long)]
    bench: bool,

    /// Shared secret for authenticating with the forwarding server
    #[arg(long, env = "HAQLITE_SECRET")]
    secret: Option<String>,
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
    info!("=== haqlite HA HTTP writer (HaQLiteClient) ===");
    info!("Bucket: {}", args.bucket);
    info!("Prefix: {}", args.prefix);
    info!("DB name: {}", args.db_name);

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        let _ = shutdown_tx.send(true);
    });

    // Connect with retry — rebuilds the builder each time since connect() consumes it.
    let client = loop {
        let mut b = HaQLiteClient::new(&args.bucket)
            .prefix(&args.prefix)
            .db_name(&args.db_name);
        if let Some(ref endpoint) = args.endpoint {
            b = b.endpoint(endpoint);
        }
        if let Some(ref secret) = args.secret {
            b = b.secret(secret);
        }

        match b.connect().await {
            Ok(c) => {
                info!("Connected to leader: {}", c.leader_address());
                break c;
            }
            Err(e) => {
                info!("Waiting for leader: {}", e);
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
                match client.execute(
                    "INSERT INTO test_data (id, value) VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM test_data), ?1)",
                    &[SqlValue::Text(format!("row-{}", writes + 1))],
                ).await {
                    Ok(_) => {
                        writes += 1;
                        consecutive_errors = 0;
                        if writes % 100 == 0 {
                            let elapsed = start.elapsed().as_secs_f64();
                            let rate = writes as f64 / elapsed;
                            let count = client.query_row("SELECT COUNT(*) FROM test_data", &[]).await
                                .ok().and_then(|r| r[0].as_integer()).unwrap_or(0);
                            info!("{} writes ({:.0}/s), {} errors, leader={}, count={}",
                                writes, rate, errors, client.leader_address(), count);
                        }
                    }
                    Err(e) => {
                        errors += 1;
                        consecutive_errors += 1;
                        if consecutive_errors <= 3 || consecutive_errors % 10 == 0 {
                            warn!("Write failed: {} (consecutive #{})", e, consecutive_errors);
                        }
                    }
                }

                if consecutive_errors > 300 {
                    error!("Too many consecutive errors ({}), exiting", consecutive_errors);
                    break;
                }

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

    // Verify data integrity on all nodes (uses raw HTTP — these are app server endpoints).
    if let Some(ref nodes_str) = args.verify_nodes {
        info!("Waiting 3s for replication to catch up before verification...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()?;

        let nodes: Vec<&str> = nodes_str.split(',').map(|s| s.trim()).collect();
        let mut all_ok = true;

        for node in &nodes {
            let verify_url = format!("{}/verify", node);
            match http.get(&verify_url).send().await {
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
                            "VERIFY {}: FAILED ({} rows, {} gaps, {} dups)",
                            node, count, gaps, dups
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

            let metrics_url = format!("{}/metrics", node);
            match http.get(&metrics_url).send().await {
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

    // Benchmark summary.
    if args.bench {
        let mut promotion_us: u64 = 0;
        let mut catchup_us: u64 = 0;
        let mut promotions: u64 = 0;

        if let Some(ref nodes_str) = args.verify_nodes {
            let http = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()?;
            let nodes: Vec<&str> = nodes_str.split(',').map(|s| s.trim()).collect();
            for node in &nodes {
                let metrics_url = format!("{}/metrics", node);
                if let Ok(resp) = http.get(&metrics_url).send().await {
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

        println!("# sync_interval_ms\twrites\terrors\telapsed_s\twrites_per_s\tpromotion_us\tcatchup_us\tpromotions");
        println!(
            "{}\t{}\t{}\t{:.1}\t{:.0}\t{}\t{}\t{}",
            args.interval_ms, writes, errors, elapsed, rate,
            promotion_us, catchup_us, promotions
        );
    }

    Ok(())
}
