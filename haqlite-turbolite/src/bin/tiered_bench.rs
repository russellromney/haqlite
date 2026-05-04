//! haqlite-turbolite wrapper for Turbolite's tiered benchmark dataset.
//!
//! This intentionally reuses the Turbolite social benchmark's remote prefix
//! (`social_{size}_btree`) and query shape, but opens the SQLite connection via
//! haqlite-turbolite's Builder so reads travel through the integrated VFS path.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use hadb::InMemoryLeaseStore;
use hadb_storage::CasResult;
use haqlite_turbolite::{Builder, HaMode, Role, SqlValue};
use rusqlite::Connection;
use tokio::sync::Mutex;
use turbodb::{Manifest, ManifestMeta, ManifestStore};
use turbolite::tiered::{
    parse_eqp_output, push_planned_accesses, CacheConfig, CompressionConfig, PrefetchConfig,
    SharedTurboliteVfs, TurboliteConfig, TurboliteSharedState, TurboliteVfs,
};

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Debug)]
struct ManifestPutEvent {
    key: String,
    version: u64,
    at: Instant,
}

#[derive(Clone, Debug)]
struct ManifestGetEvent {
    key: String,
    at: Instant,
}

struct MemoryManifestStore {
    data: Mutex<HashMap<String, Manifest>>,
    events: Mutex<Vec<ManifestPutEvent>>,
    get_events: Mutex<Vec<ManifestGetEvent>>,
    notify: tokio::sync::Notify,
    get_notify: tokio::sync::Notify,
}

impl Default for MemoryManifestStore {
    fn default() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            events: Mutex::new(Vec::new()),
            get_events: Mutex::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
            get_notify: tokio::sync::Notify::new(),
        }
    }
}

impl MemoryManifestStore {
    async fn latest_version(&self, key: &str) -> Option<u64> {
        self.data.lock().await.get(key).map(|m| m.version)
    }

    async fn record_put(&self, key: &str, version: u64) {
        self.events.lock().await.push(ManifestPutEvent {
            key: key.to_string(),
            version,
            at: Instant::now(),
        });
        self.notify.notify_waiters();
    }

    async fn record_get(&self, key: &str) {
        self.get_events.lock().await.push(ManifestGetEvent {
            key: key.to_string(),
            at: Instant::now(),
        });
        self.get_notify.notify_waiters();
    }

    async fn find_event_after(
        &self,
        key: &str,
        after_version: u64,
        not_before: Instant,
    ) -> Option<ManifestPutEvent> {
        self.events
            .lock()
            .await
            .iter()
            .find(|event| {
                event.key == key && event.version > after_version && event.at >= not_before
            })
            .cloned()
    }

    async fn wait_for_version_after(
        &self,
        key: &str,
        after_version: u64,
        not_before: Instant,
        timeout: Duration,
    ) -> Result<ManifestPutEvent> {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(event) = self.find_event_after(key, after_version, not_before).await {
                    return event;
                }
                self.notify.notified().await;
            }
        })
        .await
        .map_err(|_| anyhow!("timed out waiting for manifest {key} after v{after_version}"))
    }

    async fn find_get_after(&self, key: &str, not_before: Instant) -> Option<ManifestGetEvent> {
        self.get_events
            .lock()
            .await
            .iter()
            .find(|event| event.key == key && event.at >= not_before)
            .cloned()
    }

    async fn wait_for_get_after(
        &self,
        key: &str,
        not_before: Instant,
        timeout: Duration,
    ) -> Result<ManifestGetEvent> {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(event) = self.find_get_after(key, not_before).await {
                    return event;
                }
                self.get_notify.notified().await;
            }
        })
        .await
        .map_err(|_| anyhow!("timed out waiting for manifest get {key}"))
    }
}

#[async_trait]
impl ManifestStore for MemoryManifestStore {
    async fn get(&self, key: &str) -> Result<Option<Manifest>> {
        let manifest = self.data.lock().await.get(key).cloned();
        self.record_get(key).await;
        Ok(manifest)
    }

    async fn put(
        &self,
        key: &str,
        manifest: &Manifest,
        expected_version: Option<u64>,
    ) -> Result<CasResult> {
        let mut put_version = None;
        let result = {
            let mut data = self.data.lock().await;
            match expected_version {
                None if data.contains_key(key) => CasResult {
                    success: false,
                    etag: None,
                },
                None => {
                    let mut m = manifest.clone();
                    m.version = 1;
                    data.insert(key.to_string(), m);
                    put_version = Some(1);
                    CasResult {
                        success: true,
                        etag: Some("1".into()),
                    }
                }
                Some(expected) => match data.get(key) {
                    Some(current) if current.version == expected => {
                        let mut m = manifest.clone();
                        m.version = expected + 1;
                        data.insert(key.to_string(), m);
                        put_version = Some(expected + 1);
                        CasResult {
                            success: true,
                            etag: Some((expected + 1).to_string()),
                        }
                    }
                    _ => CasResult {
                        success: false,
                        etag: None,
                    },
                },
            }
        };
        if let Some(version) = put_version {
            self.record_put(key, version).await;
        }
        Ok(result)
    }

    async fn meta(&self, key: &str) -> Result<Option<ManifestMeta>> {
        Ok(self.data.lock().await.get(key).map(ManifestMeta::from))
    }
}

#[derive(Debug)]
struct Cli {
    sizes: String,
    iterations: usize,
    warmup: usize,
    modes: String,
    queries: String,
    page_size: u32,
    ppg: u32,
    prefetch_threads: u32,
    plan_aware: bool,
    post_prefetch: String,
    post_lookup: String,
    profile_prefetch: String,
    profile_lookup: String,
    who_liked_prefetch: String,
    who_liked_lookup: String,
    mutual_prefetch: String,
    mutual_lookup: String,
    idx_filter_prefetch: String,
    idx_filter_lookup: String,
    scan_filter_prefetch: String,
    scan_filter_lookup: String,
    replication_latency: bool,
    replication_interval_ms: u64,
    manifest_poll_ms: u64,
    follower_poll_ms: u64,
    latency_batch_rows: usize,
    latency_payload_bytes: usize,
    latency_timeout_ms: u64,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            sizes: "10000".into(),
            iterations: 1,
            warmup: 0,
            modes: "index,interior".into(),
            queries: "post,idx-filter".into(),
            page_size: 65536,
            ppg: 256,
            prefetch_threads: 8,
            plan_aware: false,
            post_prefetch: "off".into(),
            post_lookup: "off".into(),
            profile_prefetch: "0.1,0.2,0.3".into(),
            profile_lookup: "0,0,0,0".into(),
            who_liked_prefetch: "0.3,0.3,0.4".into(),
            who_liked_lookup: "0,0,0".into(),
            mutual_prefetch: "0.4,0.3,0.3".into(),
            mutual_lookup: "0,0,0".into(),
            idx_filter_prefetch: "0.2,0.3,0.5".into(),
            idx_filter_lookup: "0,0,0".into(),
            scan_filter_prefetch: "off".into(),
            scan_filter_lookup: "off".into(),
            replication_latency: false,
            replication_interval_ms: 1,
            manifest_poll_ms: 1,
            follower_poll_ms: 1,
            latency_batch_rows: 8,
            latency_payload_bytes: 8192,
            latency_timeout_ms: 5000,
        }
    }
}

impl Cli {
    fn parse() -> Result<Self> {
        let mut cli = Self::default();
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--sizes" => cli.sizes = take_arg(&mut args, "--sizes")?,
                "--iterations" => cli.iterations = take_arg(&mut args, "--iterations")?.parse()?,
                "--warmup" => cli.warmup = take_arg(&mut args, "--warmup")?.parse()?,
                "--modes" => cli.modes = take_arg(&mut args, "--modes")?,
                "--queries" => cli.queries = take_arg(&mut args, "--queries")?,
                "--page-size" => cli.page_size = take_arg(&mut args, "--page-size")?.parse()?,
                "--ppg" => cli.ppg = take_arg(&mut args, "--ppg")?.parse()?,
                "--prefetch-threads" => {
                    cli.prefetch_threads = take_arg(&mut args, "--prefetch-threads")?.parse()?
                }
                "--plan-aware" => cli.plan_aware = true,
                "--post-prefetch" => cli.post_prefetch = take_arg(&mut args, "--post-prefetch")?,
                "--post-lookup" => cli.post_lookup = take_arg(&mut args, "--post-lookup")?,
                "--profile-prefetch" => {
                    cli.profile_prefetch = take_arg(&mut args, "--profile-prefetch")?
                }
                "--profile-lookup" => cli.profile_lookup = take_arg(&mut args, "--profile-lookup")?,
                "--who-liked-prefetch" => {
                    cli.who_liked_prefetch = take_arg(&mut args, "--who-liked-prefetch")?
                }
                "--who-liked-lookup" => {
                    cli.who_liked_lookup = take_arg(&mut args, "--who-liked-lookup")?
                }
                "--mutual-prefetch" => {
                    cli.mutual_prefetch = take_arg(&mut args, "--mutual-prefetch")?
                }
                "--mutual-lookup" => cli.mutual_lookup = take_arg(&mut args, "--mutual-lookup")?,
                "--idx-filter-prefetch" => {
                    cli.idx_filter_prefetch = take_arg(&mut args, "--idx-filter-prefetch")?
                }
                "--idx-filter-lookup" => {
                    cli.idx_filter_lookup = take_arg(&mut args, "--idx-filter-lookup")?
                }
                "--scan-filter-prefetch" => {
                    cli.scan_filter_prefetch = take_arg(&mut args, "--scan-filter-prefetch")?
                }
                "--scan-filter-lookup" => {
                    cli.scan_filter_lookup = take_arg(&mut args, "--scan-filter-lookup")?
                }
                "--replication-latency" => cli.replication_latency = true,
                "--replication-interval-ms" => {
                    cli.replication_interval_ms =
                        take_arg(&mut args, "--replication-interval-ms")?.parse()?
                }
                "--manifest-poll-ms" => {
                    cli.manifest_poll_ms = take_arg(&mut args, "--manifest-poll-ms")?.parse()?
                }
                "--follower-poll-ms" => {
                    cli.follower_poll_ms = take_arg(&mut args, "--follower-poll-ms")?.parse()?
                }
                "--latency-batch-rows" => {
                    cli.latency_batch_rows =
                        take_arg(&mut args, "--latency-batch-rows")?.parse()?
                }
                "--latency-payload-bytes" => {
                    cli.latency_payload_bytes =
                        take_arg(&mut args, "--latency-payload-bytes")?.parse()?
                }
                "--latency-timeout-ms" => {
                    cli.latency_timeout_ms =
                        take_arg(&mut args, "--latency-timeout-ms")?.parse()?
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(anyhow!("unknown argument: {other}")),
            }
        }
        Ok(cli)
    }
}

fn take_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow!("{name} requires a value"))
}

fn print_help() {
    println!(
        "haqlite-turbolite-bench\n\
         \n\
         Uses Turbolite's social_{{size}}_btree benchmark prefix, but opens the\n\
         database through haqlite-turbolite with InMemoryLeaseStore.\n\
         \n\
         Options:\n\
           --sizes 10000              comma-separated dataset sizes\n\
           --iterations 1             measured iterations per query\n\
           --warmup 0                 warmup iterations per query\n\
           --modes index,interior     one or more of data,index,interior,none\n\
           --queries post,idx-filter  one or more of post,profile,who-liked,mutual,idx-filter,scan-filter\n\
           --plan-aware               push EXPLAIN QUERY PLAN accesses before queries\n\
           --replication-latency      run leader commit -> follower visibility benchmark\n\
           --replication-interval-ms  continuous WAL sync interval; 0 is treated as 1ms\n\
           --manifest-poll-ms         follower manifest poll interval; 0 is treated as 1ms\n\
           --latency-batch-rows 8     rows inserted per measured transaction\n\
           --latency-payload-bytes 8192 blob bytes per inserted row\n"
    );
}

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET").expect("TIERED_TEST_BUCKET required")
}

fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL_S3"))
        .ok()
}

async fn build_s3_backend(prefix: &str) -> Arc<hadb_storage_s3::S3Storage> {
    let storage = hadb_storage_s3::S3Storage::from_env(test_bucket(), endpoint_url().as_deref())
        .await
        .expect("build S3Storage");
    Arc::new(storage.with_prefix(prefix.to_string()))
}

struct BenchCtx {
    state: TurboliteSharedState,
    s3: Arc<hadb_storage_s3::S3Storage>,
    fetch_count_base: AtomicU64,
    bytes_fetched_base: AtomicU64,
}

impl BenchCtx {
    fn new(state: TurboliteSharedState, s3: Arc<hadb_storage_s3::S3Storage>) -> Self {
        Self {
            fetch_count_base: AtomicU64::new(s3.fetch_count()),
            bytes_fetched_base: AtomicU64::new(s3.bytes_fetched()),
            state,
            s3,
        }
    }

    fn clear_cache_data_only(&self) {
        self.state.clear_cache_data_only();
    }

    fn clear_cache_interior_only(&self) {
        self.state.clear_cache_interior_only();
    }

    fn clear_cache_all(&self) {
        self.state.clear_cache_all();
    }

    fn reset_s3_counters(&self) {
        self.fetch_count_base
            .store(self.s3.fetch_count(), Ordering::Relaxed);
        self.bytes_fetched_base
            .store(self.s3.bytes_fetched(), Ordering::Relaxed);
    }

    fn s3_counters(&self) -> (u64, u64) {
        (
            self.s3
                .fetch_count()
                .saturating_sub(self.fetch_count_base.load(Ordering::Relaxed)),
            self.s3
                .bytes_fetched()
                .saturating_sub(self.bytes_fetched_base.load(Ordering::Relaxed)),
        )
    }
}

fn phash(seed: u64) -> u64 {
    let mut x = seed;
    x = x
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x
}

fn percentile(latencies: &[f64], p: f64) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[idx]
}

struct BenchResult {
    label: String,
    latencies_us: Vec<f64>,
    s3_fetches: Vec<u64>,
    s3_bytes: Vec<u64>,
}

impl BenchResult {
    fn p50(&self) -> f64 {
        percentile(&self.latencies_us, 0.5)
    }

    fn p90(&self) -> f64 {
        percentile(&self.latencies_us, 0.9)
    }

    fn p99(&self) -> f64 {
        percentile(&self.latencies_us, 0.99)
    }

    fn avg_fetches(&self) -> f64 {
        if self.s3_fetches.is_empty() {
            return 0.0;
        }
        self.s3_fetches.iter().sum::<u64>() as f64 / self.s3_fetches.len() as f64
    }

    fn avg_bytes_kb(&self) -> f64 {
        if self.s3_bytes.is_empty() {
            return 0.0;
        }
        (self.s3_bytes.iter().sum::<u64>() as f64 / self.s3_bytes.len() as f64) / 1024.0
    }
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

fn format_ms(us: f64) -> String {
    if us >= 1_000_000.0 {
        format!("{:.1}s", us / 1_000_000.0)
    } else if us >= 1000.0 {
        format!("{:.1}ms", us / 1000.0)
    } else {
        format!("{:.0}us", us)
    }
}

fn format_kb(kb: f64) -> String {
    if kb >= 1024.0 {
        format!("{:.1}MB", kb / 1024.0)
    } else {
        format!("{:.0}KB", kb)
    }
}

fn min_nonzero_ms(ms: u64) -> Duration {
    Duration::from_millis(ms.max(1))
}

fn parse_prefetch_hops(s: &str) -> Vec<f32> {
    s.split(',')
        .filter_map(|v| v.trim().parse::<f32>().ok())
        .collect()
}

fn parse_query_prefetch(s: &str) -> Option<Vec<f32>> {
    match s.trim().to_lowercase().as_str() {
        "off" | "none" | "disabled" | "" => None,
        _ => Some(parse_prefetch_hops(s)),
    }
}

#[derive(Clone, Debug)]
struct SchedulePair {
    search: Option<Vec<f32>>,
    lookup: Option<Vec<f32>>,
}

impl SchedulePair {
    fn pair(search: Option<Vec<f32>>, lookup: Option<Vec<f32>>) -> Self {
        Self { search, lookup }
    }

    fn push(&self) {
        let search_val = match &self.search {
            Some(v) => v
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(","),
            None => "0,0,0".to_string(),
        };
        let lookup_val = match &self.lookup {
            Some(v) => v
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(","),
            None => "0,0,0".to_string(),
        };
        turbolite::tiered::settings::set("prefetch_search", &search_val)
            .expect("settings::set prefetch_search");
        turbolite::tiered::settings::set("prefetch_lookup", &lookup_val)
            .expect("settings::set prefetch_lookup");
    }

    fn label(&self) -> String {
        let fmt = |s: &Option<Vec<f32>>| -> String {
            match s {
                Some(v) => v
                    .iter()
                    .map(|f| format!("{:.2}", f))
                    .collect::<Vec<_>>()
                    .join(","),
                None => "off".to_string(),
            }
        };
        format!("{} / {}", fmt(&self.search), fmt(&self.lookup))
    }
}

fn print_header() {
    println!(
        "  {:<28} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "", "p50", "p90", "p99", "s3 GETs", "s3 bytes"
    );
    println!(
        "  {:-<28} {:->10} {:->10} {:->10} {:->10} {:->12}",
        "", "", "", "", "", ""
    );
}

fn print_result(r: &BenchResult) {
    println!(
        "  {:<28} {:>10} {:>10} {:>10} {:>10} {:>12}",
        r.label,
        format_ms(r.p50()),
        format_ms(r.p90()),
        format_ms(r.p99()),
        format!("{:.1}", r.avg_fetches()),
        format_kb(r.avg_bytes_kb()),
    );
}

fn print_latency_result(label: &str, values_us: &[f64]) {
    println!(
        "  {:<24} {:>10} {:>10} {:>10}",
        label,
        format_ms(percentile(values_us, 0.5)),
        format_ms(percentile(values_us, 0.9)),
        format_ms(percentile(values_us, 0.99)),
    );
}

const Q_POST_DETAIL: &str = "\
SELECT posts.id, posts.content, posts.created_at, posts.like_count,
       users.first_name, users.last_name, users.school, users.city
FROM posts
JOIN users ON users.id = posts.user_id
WHERE posts.id = ?1";

const Q_PROFILE: &str = "\
SELECT users.first_name, users.last_name, users.school, users.city, users.bio,
       posts.id, posts.content, posts.created_at, posts.like_count
FROM users
JOIN posts ON posts.user_id = users.id
WHERE users.id = ?1
ORDER BY posts.created_at DESC
LIMIT 10";

const Q_WHO_LIKED: &str = "\
SELECT users.first_name, users.last_name, users.school, likes.created_at
FROM likes
JOIN users ON users.id = likes.user_id
WHERE likes.post_id = ?1
ORDER BY likes.created_at DESC
LIMIT 50";

const Q_MUTUAL: &str = "\
SELECT users.id, users.first_name, users.last_name, users.school
FROM friendships
JOIN friendships AS friendships_b ON friendships.user_b = friendships_b.user_b
JOIN users ON users.id = friendships.user_b
WHERE friendships.user_a = ?1 AND friendships_b.user_a = ?2
LIMIT 20";

const Q_IDX_FILTER: &str = "SELECT COUNT(*) FROM posts WHERE user_id = ?1";
const Q_SCAN_FILTER: &str = "SELECT COUNT(*) FROM posts WHERE like_count > ?1";

struct QueryDef {
    label: &'static str,
    sql: &'static str,
    param_fn: Box<dyn Fn(usize) -> Vec<rusqlite::types::Value>>,
    schedule: SchedulePair,
}

const LATENCY_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS repl_latency (
    id INTEGER PRIMARY KEY,
    batch INTEGER NOT NULL,
    payload BLOB NOT NULL,
    note TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_repl_latency_batch ON repl_latency(batch);
";

fn push_query_plan(conn: &Connection, sql: &str, params: &[rusqlite::types::Value]) {
    let eqp_sql = format!("EXPLAIN QUERY PLAN {}", sql);
    let mut stmt = match conn.prepare(&eqp_sql) {
        Ok(stmt) => stmt,
        Err(_) => return,
    };
    let mut output = String::new();
    let mut rows = match stmt.query(rusqlite::params_from_iter(params)) {
        Ok(rows) => rows,
        Err(_) => return,
    };
    while let Ok(Some(row)) = rows.next() {
        if let Ok(detail) = row.get::<_, String>(3) {
            output.push_str(&detail);
            output.push('\n');
        }
    }
    push_planned_accesses(parse_eqp_output(&output));
}

fn run_query(
    conn: &Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
    plan_aware: bool,
    schedule: &SchedulePair,
) -> rusqlite::Result<usize> {
    schedule.push();
    if plan_aware {
        push_query_plan(conn, sql, params);
    }
    let mut stmt = conn.prepare_cached(sql)?;
    let rows: Vec<Vec<rusqlite::types::Value>> = stmt
        .query_map(rusqlite::params_from_iter(params), |row| {
            let n = row.as_ref().column_count();
            let mut vals = Vec::with_capacity(n);
            for i in 0..n {
                vals.push(row.get::<_, rusqlite::types::Value>(i)?);
            }
            Ok(vals)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows.len())
}

fn bench_mode(
    conn: &Connection,
    ctx: &BenchCtx,
    mode: &str,
    query: &QueryDef,
    warmup: usize,
    iterations: usize,
    plan_aware: bool,
) -> BenchResult {
    let clear = || match mode {
        "data" => {}
        "index" => ctx.clear_cache_data_only(),
        "interior" => ctx.clear_cache_interior_only(),
        "none" => ctx.clear_cache_all(),
        other => panic!("unknown mode: {other}"),
    };

    for i in 0..warmup {
        clear();
        ctx.reset_s3_counters();
        let params = (query.param_fn)(i);
        if let Err(e) = run_query(conn, query.sql, &params, plan_aware, &query.schedule) {
            eprintln!("    [{mode}] {} warmup {i} error: {e}", query.label);
        }
    }

    let mut latencies = Vec::with_capacity(iterations);
    let mut s3_fetches = Vec::with_capacity(iterations);
    let mut s3_bytes = Vec::with_capacity(iterations);

    for i in 0..iterations {
        clear();
        ctx.reset_s3_counters();
        let params = (query.param_fn)(warmup + i);
        let start = Instant::now();
        match run_query(conn, query.sql, &params, plan_aware, &query.schedule) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fetches, bytes) = ctx.s3_counters();
                s3_fetches.push(fetches);
                s3_bytes.push(bytes);
            }
            Err(e) => eprintln!("    [{mode}] {} iter {i} error: {e}", query.label),
        }
    }

    BenchResult {
        label: format!("[{mode}] {}", query.label),
        latencies_us: latencies,
        s3_fetches,
        s3_bytes,
    }
}

fn make_latency_vfs(
    cache_dir: &Path,
    storage: Arc<dyn hadb_storage::StorageBackend>,
    cli: &Cli,
) -> Result<(SharedTurboliteVfs, String)> {
    let config = TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        cache: CacheConfig {
            pages_per_group: cli.ppg,
            sub_pages_per_frame: 4,
            override_threshold: 100,
            gc_enabled: false,
            ..Default::default()
        },
        compression: CompressionConfig {
            level: 1,
            ..Default::default()
        },
        prefetch: PrefetchConfig {
            threads: cli.prefetch_threads,
            query_plan: cli.plan_aware,
            ..Default::default()
        },
        ..Default::default()
    };
    let vfs = TurboliteVfs::with_backend(config, storage, tokio::runtime::Handle::current())
        .context("create latency Turbolite VFS")?;
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    let vfs_name = format!(
        "haqlite_turbolite_latency_{}",
        VFS_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone())
        .context("register latency VFS")?;
    Ok((shared_vfs, vfs_name))
}

#[allow(clippy::too_many_arguments)]
async fn build_latency_node(
    cache_dir: &Path,
    db_name: &str,
    role: Role,
    instance_id: &str,
    cli: &Cli,
    lease_store: Arc<InMemoryLeaseStore>,
    manifest_store: Arc<dyn ManifestStore>,
    tiered_storage: Arc<dyn hadb_storage::StorageBackend>,
    walrust_storage: Arc<dyn hadb_storage::StorageBackend>,
) -> Result<haqlite_turbolite::HaQLite> {
    let (shared_vfs, vfs_name) = make_latency_vfs(cache_dir, tiered_storage, cli)?;
    let db_path = cache_dir.join(format!("{db_name}.db"));
    Builder::new()
        .prefix("latency/")
        .mode(HaMode::SingleWriter)
        .role(role)
        .durability(turbodb::Durability::Continuous {
            checkpoint: turbodb::CheckpointConfig::default(),
            replication_interval: min_nonzero_ms(cli.replication_interval_ms),
        })
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .walrust_storage(walrust_storage)
        .turbolite_vfs(shared_vfs, &vfs_name)
        .instance_id(instance_id)
        .lease_ttl(2)
        .lease_renew_interval(Duration::from_millis(100))
        .lease_follower_poll_interval(min_nonzero_ms(cli.follower_poll_ms))
        .manifest_poll_interval(min_nonzero_ms(cli.manifest_poll_ms))
        .follower_pull_interval(min_nonzero_ms(cli.follower_poll_ms))
        .disable_forwarding()
        .open(db_path.to_str().expect("utf8 latency db path"), LATENCY_SCHEMA)
        .await
        .map_err(|e| anyhow!("open latency node {instance_id}: {e}"))
}

async fn wait_for_role(
    db: &haqlite_turbolite::HaQLite,
    expected: Role,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if db.role() == Some(expected) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(anyhow!(
        "timed out waiting for role {:?}; current role {:?}",
        expected,
        db.role()
    ))
}

fn follower_batch_count(db: &haqlite_turbolite::HaQLite, batch: i64) -> Result<i64> {
    let rows = db
        .query_values_local(
            "SELECT COUNT(*) FROM repl_latency WHERE batch = ?1",
            &[SqlValue::Integer(batch)],
        )
        .map_err(|e| anyhow!("follower latency query failed: {e}"))?;
    match rows.first().and_then(|row| row.first()) {
        Some(SqlValue::Integer(count)) => Ok(*count),
        other => Err(anyhow!("unexpected follower count row: {other:?}")),
    }
}

async fn wait_for_batch(
    follower: &haqlite_turbolite::HaQLite,
    batch: i64,
    expected_rows: i64,
    timeout: Duration,
) -> Result<Instant> {
    let deadline = Instant::now() + timeout;
    let mut last_err = None;
    while Instant::now() < deadline {
        match follower_batch_count(follower, batch) {
            Ok(count) if count == expected_rows => return Ok(Instant::now()),
            Ok(_) => {}
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Err(anyhow!(
        "timed out waiting for batch {batch}; last error: {}",
        last_err.unwrap_or_else(|| "<none>".to_string())
    ))
}

async fn insert_latency_batch(
    leader: &haqlite_turbolite::HaQLite,
    batch: i64,
    rows: usize,
    payload_bytes: usize,
) -> Result<LatencyWriteTiming> {
    let start = Instant::now();
    leader.execute_async("BEGIN IMMEDIATE", &[]).await?;
    for i in 0..rows {
        let id = batch * rows as i64 + i as i64 + 1;
        let payload = vec![((id as usize + i) % 251) as u8; payload_bytes];
        leader
            .execute_async(
                "INSERT INTO repl_latency (id, batch, payload, note) VALUES (?1, ?2, ?3, ?4)",
                &[
                    SqlValue::Integer(id),
                    SqlValue::Integer(batch),
                    SqlValue::Blob(payload),
                    SqlValue::Text(format!("batch={batch} row={i}")),
                ],
            )
            .await?;
    }
    let commit_start = Instant::now();
    leader.execute_async("COMMIT", &[]).await?;
    let commit_done = Instant::now();
    Ok(LatencyWriteTiming {
        start,
        commit_start,
        commit_done,
    })
}

struct LatencyWriteTiming {
    start: Instant,
    commit_start: Instant,
    commit_done: Instant,
}

async fn run_replication_latency(cli: &Cli) -> Result<()> {
    let run_id = uuid::Uuid::new_v4();
    let db_name = "haqlite_turbolite_latency";
    let root = std::env::temp_dir().join(format!("haqlite-turbolite-latency-{run_id}"));
    let leader_cache = root.join("leader");
    let follower_cache = root.join("follower");
    std::fs::create_dir_all(&leader_cache)?;
    std::fs::create_dir_all(&follower_cache)?;

    let tiered_prefix = format!("latency/{run_id}/pages");
    let walrust_prefix = format!("latency/{run_id}/wal");
    let tiered_storage: Arc<dyn hadb_storage::StorageBackend> =
        build_s3_backend(&tiered_prefix).await;
    let walrust_storage: Arc<dyn hadb_storage::StorageBackend> =
        build_s3_backend(&walrust_prefix).await;
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemoryManifestStore::default());
    let manifest_key = format!("latency/{db_name}/_manifest");

    println!("\n=== CONTINUOUS LEADER -> FOLLOWER LATENCY ===");
    println!("Prefix:              latency/{run_id}");
    println!(
        "Replication interval: {}ms{}",
        cli.replication_interval_ms.max(1),
        if cli.replication_interval_ms == 0 {
            " (0 requested; Tokio interval minimum is 1ms)"
        } else {
            ""
        }
    );
    println!(
        "Manifest poll:        {}ms{}",
        cli.manifest_poll_ms.max(1),
        if cli.manifest_poll_ms == 0 {
            " (0 requested; Tokio interval minimum is 1ms)"
        } else {
            ""
        }
    );
    println!("Follower poll:        {}ms", cli.follower_poll_ms.max(1));
    println!("Batch rows:           {}", cli.latency_batch_rows);
    println!("Payload bytes/row:    {}", cli.latency_payload_bytes);

    let mut leader = build_latency_node(
        &leader_cache,
        db_name,
        Role::Leader,
        "latency-leader",
        cli,
        lease_store.clone(),
        manifest_store.clone() as Arc<dyn ManifestStore>,
        tiered_storage.clone(),
        walrust_storage.clone(),
    )
    .await?;
    wait_for_role(&leader, Role::Leader, Duration::from_secs(3)).await?;

    // Seed the base so a follower can open against a real manifest before
    // measured continuous WAL batches start.
    insert_latency_batch(&leader, 0, cli.latency_batch_rows, cli.latency_payload_bytes).await?;

    let mut follower = build_latency_node(
        &follower_cache,
        db_name,
        Role::Follower,
        "latency-follower",
        cli,
        lease_store,
        manifest_store.clone() as Arc<dyn ManifestStore>,
        tiered_storage,
        walrust_storage,
    )
    .await?;
    wait_for_role(&follower, Role::Follower, Duration::from_secs(3)).await?;
    wait_for_batch(
        &follower,
        0,
        cli.latency_batch_rows as i64,
        Duration::from_millis(cli.latency_timeout_ms),
    )
    .await?;

    let total = cli.warmup + cli.iterations;
    let mut txn_body_us = Vec::with_capacity(cli.iterations);
    let mut commit_us = Vec::with_capacity(cli.iterations);
    let mut begin_to_manifest_us = Vec::with_capacity(cli.iterations);
    let mut commit_to_manifest_us = Vec::with_capacity(cli.iterations);
    let mut manifest_to_commit_ack_us = Vec::with_capacity(cli.iterations);
    let mut manifest_to_first_get_us = Vec::with_capacity(cli.iterations);
    let mut first_get_to_follower_us = Vec::with_capacity(cli.iterations);
    let mut manifest_to_follower_us = Vec::with_capacity(cli.iterations);
    let mut visible_after_commit_us = Vec::with_capacity(cli.iterations);
    let mut end_to_end_us = Vec::with_capacity(cli.iterations);

    for i in 0..total {
        let batch = (i + 1) as i64;
        let manifest_version_before = manifest_store
            .latest_version(&manifest_key)
            .await
            .unwrap_or_default();
        let write = insert_latency_batch(
            &leader,
            batch,
            cli.latency_batch_rows,
            cli.latency_payload_bytes,
        )
        .await?;
        let manifest_event = manifest_store
            .wait_for_version_after(
                &manifest_key,
                manifest_version_before,
                write.commit_start,
                Duration::from_millis(cli.latency_timeout_ms),
            )
            .await?;
        let first_manifest_get = manifest_store
            .wait_for_get_after(
                &manifest_key,
                manifest_event.at,
                Duration::from_millis(cli.latency_timeout_ms),
            )
            .await?;
        let follower_visible = wait_for_batch(
            &follower,
            batch,
            cli.latency_batch_rows as i64,
            Duration::from_millis(cli.latency_timeout_ms),
        )
        .await?;
        let txn_body = write.commit_start.saturating_duration_since(write.start);
        let commit_elapsed = write.commit_done.saturating_duration_since(write.commit_start);
        let begin_to_manifest = manifest_event.at.saturating_duration_since(write.start);
        let commit_to_manifest = manifest_event.at.saturating_duration_since(write.commit_done);
        let manifest_to_commit_ack = write.commit_done.saturating_duration_since(manifest_event.at);
        let manifest_to_first_get = first_manifest_get
            .at
            .saturating_duration_since(manifest_event.at);
        let first_get_to_follower = follower_visible.saturating_duration_since(first_manifest_get.at);
        let visible_after_commit = follower_visible.saturating_duration_since(write.commit_done);
        let end_to_end = follower_visible.saturating_duration_since(write.start);
        let manifest_to_follower = follower_visible.saturating_duration_since(manifest_event.at);

        if i >= cli.warmup {
            txn_body_us.push(txn_body.as_micros() as f64);
            commit_us.push(commit_elapsed.as_micros() as f64);
            begin_to_manifest_us.push(begin_to_manifest.as_micros() as f64);
            commit_to_manifest_us.push(commit_to_manifest.as_micros() as f64);
            manifest_to_commit_ack_us.push(manifest_to_commit_ack.as_micros() as f64);
            manifest_to_first_get_us.push(manifest_to_first_get.as_micros() as f64);
            first_get_to_follower_us.push(first_get_to_follower.as_micros() as f64);
            manifest_to_follower_us.push(manifest_to_follower.as_micros() as f64);
            visible_after_commit_us.push(visible_after_commit.as_micros() as f64);
            end_to_end_us.push(end_to_end.as_micros() as f64);
        }
    }

    println!(
        "\nMeasured {} batches after {} warmup",
        cli.iterations, cli.warmup
    );
    println!("  {:<24} {:>10} {:>10} {:>10}", "", "p50", "p90", "p99");
    println!("  {:-<24} {:->10} {:->10} {:->10}", "", "", "", "");
    print_latency_result("txn body", &txn_body_us);
    print_latency_result("leader commit", &commit_us);
    print_latency_result("begin -> manifest", &begin_to_manifest_us);
    print_latency_result("commit -> manifest", &commit_to_manifest_us);
    print_latency_result("manifest -> commit", &manifest_to_commit_ack_us);
    print_latency_result("manifest -> get", &manifest_to_first_get_us);
    print_latency_result("get -> follower", &first_get_to_follower_us);
    print_latency_result("manifest -> follower", &manifest_to_follower_us);
    print_latency_result("commit -> follower", &visible_after_commit_us);
    print_latency_result("begin -> follower", &end_to_end_us);

    follower.close().await.map_err(|e| anyhow!("{e}"))?;
    leader.close().await.map_err(|e| anyhow!("{e}"))?;
    Ok(())
}

async fn run_size(size: usize, cli: &Cli) -> Result<()> {
    let prefix = format!("social_{}_btree", size);
    let db_name = format!("haqlite_turbolite_bench_{size}.db");
    let cache_dir = std::env::temp_dir().join(format!(
        "haqlite-turbolite-bench-{}-{}",
        size,
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&cache_dir)?;

    let s3 = build_s3_backend(&prefix).await;
    let manifest = turbolite::tiered::get_manifest(s3.as_ref(), &tokio::runtime::Handle::current())
        .context("fetch Turbolite benchmark manifest")?
        .ok_or_else(|| {
            anyhow!(
                "no Turbolite benchmark manifest at prefix {prefix}; run tiered-bench --import auto first"
            )
        })?;

    let n_users = (size / 10).max(100);
    println!(
        "\n--- haqlite-turbolite over {prefix}: {} posts, {} users ({} pages, {} groups) ---",
        format_number(size),
        format_number(n_users),
        manifest.page_count,
        manifest.page_group_keys.len(),
    );
    println!(
        "  Manifest: page_size={} bytes, pages/group={}, sub-pages/frame={}",
        manifest.page_size, manifest.pages_per_group, manifest.sub_pages_per_frame
    );

    let config = TurboliteConfig {
        cache_dir: cache_dir.clone(),
        cache: CacheConfig {
            pages_per_group: cli.ppg,
            gc_enabled: false,
            ..Default::default()
        },
        compression: CompressionConfig {
            level: 1,
            ..Default::default()
        },
        prefetch: PrefetchConfig {
            threads: cli.prefetch_threads,
            query_plan: cli.plan_aware,
            ..Default::default()
        },
        ..Default::default()
    };

    let vfs = TurboliteVfs::with_backend(
        config,
        s3.clone() as Arc<dyn hadb_storage::StorageBackend>,
        tokio::runtime::Handle::current(),
    )
    .context("create Turbolite VFS")?;
    let shared_vfs = SharedTurboliteVfs::new(vfs);
    let ctx = BenchCtx::new(shared_vfs.shared_state(), s3.clone());
    let vfs_name = format!(
        "haqlite_turbolite_bench_{}",
        VFS_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    turbolite::tiered::register_shared(&vfs_name, shared_vfs.clone()).context("register VFS")?;

    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let manifest_store = Arc::new(MemoryManifestStore::default());
    let mut db = Builder::new()
        .prefix("bench/")
        .mode(HaMode::SingleWriter)
        .role(Role::Leader)
        .durability(turbodb::Durability::Cloud)
        .lease_store(lease_store)
        .manifest_store(manifest_store)
        .turbolite_vfs(shared_vfs, &vfs_name)
        .instance_id(&format!("bench-{}", uuid::Uuid::new_v4()))
        .disable_forwarding()
        .open(cache_dir.join(&db_name).to_str().expect("utf8 path"), "")
        .await
        .context("open haqlite-turbolite")?;

    let conn_arc = db.connection().map_err(|e| anyhow!("{e}"))?;
    let conn = conn_arc.lock();
    let integrity: String = conn.query_row("PRAGMA integrity_check(100)", [], |r| r.get(0))?;
    if integrity != "ok" {
        return Err(anyhow!("integrity_check failed: {integrity}"));
    }
    let row_count: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0))?;
    println!("  Verified through haqlite-turbolite: {row_count} posts");

    let query_filter: Vec<String> = cli
        .queries
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .collect();
    let mode_filter: Vec<String> = cli
        .modes
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .collect();
    let mk_pair = |search: &str, lookup: &str| {
        SchedulePair::pair(parse_query_prefetch(search), parse_query_prefetch(lookup))
    };
    let post_pair = mk_pair(&cli.post_prefetch, &cli.post_lookup);
    let profile_pair = mk_pair(&cli.profile_prefetch, &cli.profile_lookup);
    let who_liked_pair = mk_pair(&cli.who_liked_prefetch, &cli.who_liked_lookup);
    let mutual_pair = mk_pair(&cli.mutual_prefetch, &cli.mutual_lookup);
    let idx_filter_pair = mk_pair(&cli.idx_filter_prefetch, &cli.idx_filter_lookup);
    let scan_filter_pair = mk_pair(&cli.scan_filter_prefetch, &cli.scan_filter_lookup);

    let all_queries = vec![
        QueryDef {
            label: "post+user",
            sql: Q_POST_DETAIL,
            param_fn: Box::new(move |i| {
                let pid = phash(i as u64 + 500) % size as u64;
                vec![rusqlite::types::Value::Integer(pid as i64)]
            }),
            schedule: post_pair,
        },
        QueryDef {
            label: "profile",
            sql: Q_PROFILE,
            param_fn: Box::new(move |i| {
                let uid = phash(i as u64 + 100) % n_users as u64;
                vec![rusqlite::types::Value::Integer(uid as i64)]
            }),
            schedule: profile_pair,
        },
        QueryDef {
            label: "who-liked",
            sql: Q_WHO_LIKED,
            param_fn: Box::new(move |i| {
                let pid = phash(i as u64 + 200) % size as u64;
                vec![rusqlite::types::Value::Integer(pid as i64)]
            }),
            schedule: who_liked_pair,
        },
        QueryDef {
            label: "mutual",
            sql: Q_MUTUAL,
            param_fn: Box::new(move |i| {
                let a = phash(i as u64 + 300) % n_users as u64;
                let b = phash(i as u64 + 400) % n_users as u64;
                vec![
                    rusqlite::types::Value::Integer(a as i64),
                    rusqlite::types::Value::Integer(b as i64),
                ]
            }),
            schedule: mutual_pair,
        },
        QueryDef {
            label: "idx-filter",
            sql: Q_IDX_FILTER,
            param_fn: Box::new(move |i| {
                let uid = phash(i as u64 + 600) % n_users as u64;
                vec![rusqlite::types::Value::Integer(uid as i64)]
            }),
            schedule: idx_filter_pair,
        },
        QueryDef {
            label: "scan-filter",
            sql: Q_SCAN_FILTER,
            param_fn: Box::new(move |i| {
                let threshold = (phash(i as u64 + 700) % 50) as i64;
                vec![rusqlite::types::Value::Integer(threshold)]
            }),
            schedule: scan_filter_pair,
        },
    ];

    let queries: Vec<QueryDef> = all_queries
        .into_iter()
        .filter(|q| query_filter.iter().any(|name| q.label.contains(name)))
        .collect();
    for query in &queries {
        println!("  {:12} schedule: {}", query.label, query.schedule.label());
    }
    for mode in ["data", "index", "interior", "none"] {
        if !mode_filter.iter().any(|m| m == mode) {
            continue;
        }
        println!("\n=== HAQLITE-TURBOLITE CACHE LEVEL: {} ===", mode.to_uppercase());
        print_header();
        for query in &queries {
            let result = bench_mode(
                &conn,
                &ctx,
                mode,
                query,
                cli.warmup,
                cli.iterations,
                cli.plan_aware,
            );
            print_result(&result);
        }
    }

    ctx.clear_cache_data_only();
    drop(conn);
    db.close().await.map_err(|e| anyhow!("{e}"))?;
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .context("build runtime")?;

    println!("=== haqlite-turbolite Tiered VFS Benchmark ===");
    println!("Bucket:       {}", test_bucket());
    println!(
        "Endpoint:     {}",
        endpoint_url().as_deref().unwrap_or("(default S3)")
    );
    println!(
        "Reader config: page_size hint={} bytes, pages/group={}",
        cli.page_size, cli.ppg
    );
    println!("Iterations:   {} measured + {} warmup", cli.iterations, cli.warmup);
    println!("Plan-aware:   {}", if cli.plan_aware { "on" } else { "off" });

    runtime.block_on(async {
        if cli.replication_latency {
            run_replication_latency(&cli).await?;
        } else {
            for size in cli.sizes.split(',') {
                let size = size.trim().parse::<usize>()?;
                run_size(size, &cli).await?;
            }
        }
        Result::<()>::Ok(())
    })?;

    println!("\nDone.");
    std::thread::sleep(Duration::from_millis(100));
    Ok(())
}
