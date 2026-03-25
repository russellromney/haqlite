# haqlite Roadmap

## Phase Meridian: Wire Up CLI Commands

> After: hadb-cli + haqlite serve + hrana + ops implementation

`haqlite serve` is production-ready. The 6 non-serve commands (restore, list, verify, compact, replicate, snapshot) plus explain all have issues that make them broken or misleading in practice.

### Problems

1. **No `--prefix` on non-serve commands** -- serve uses prefix `"haqlite/"` but every other command hardcodes `""`. They can't find any data created by serve.
2. **Compact GFS flags are fake** -- `--hourly/--daily/--weekly/--monthly` just get summed into keep-N. No temporal bucketing.
3. **Replicate has no graceful shutdown** -- loops forever, no SIGTERM handling.
4. **Restore uses tracing, not println** -- inconsistent with every other command's output.
5. **Snapshot doesn't report S3 location** -- user can't tell what name was used or where it went.
6. **Explain dumps raw Rust Debug** for product config section.
7. **Arg help text unclear** about "name in S3" vs "local file path".

### Meridian-a: hadb-cli changes

- Add `--prefix` to `S3Args` (`args.rs`): `#[arg(long, env = "HADB_PREFIX", default_value = "")]`. Products apply their own default.
- Replace 4 GFS flags with `--keep N` on `CompactArgs` (default 47, preserves old sum). Honest about what it does.
- Improve help text on positional args (name = "Database name in S3", database = "Path to local database file").
- Update explain default in `runner.rs`, fix test compilation.
- `RetentionSection` in config stays (serve may use it for scheduled snapshots later).

### Meridian-b: haqlite ops prefix threading

- Add `prefix: &str` param to all 7 ops functions (`discover_ltx_files`, `discover_databases`, `list_databases`, `verify_database`, `plan_compact`, `snapshot_database`, `replicate_database`).
- Add `normalize_prefix()` helper (handles missing trailing slash).
- `snapshot_database` returns `SnapshotResult { txid, db_name }` instead of bare `u64`.
- Wrap replicate loop in `tokio::select!` with `hadb_cli::shutdown_signal()`.

### Meridian-c: haqlite CLI dispatch

- Add `DEFAULT_PREFIX = "haqlite/"` and `resolve_prefix()` helper to `main.rs`. If `--prefix` is empty (default from hadb-cli), use `"haqlite/"`.
- Wire `resolve_prefix(&args.s3)` through all 6 commands.
- Fix restore: `println!` instead of `tracing::info!`.
- Fix snapshot: print db_name and TXID from `SnapshotResult`.
- Update compact: `args.keep` instead of `args.hourly + ... + args.monthly`.
- Override `explain()` with clean formatted ServeConfig output.

### Meridian-d: Tests

- Update all 37 existing ops tests: add `""` as prefix param (no test data changes needed).
- Update snapshot tests to destructure `SnapshotResult`.
- Add ~8 new tests: prefix discovery, prefix isolation, prefix normalization, snapshot result fields.
- Fix hadb-cli test compilation for new S3Args/CompactArgs fields.

### Files

| File | Changes |
|------|---------|
| `hadb/hadb-cli/src/args.rs` | prefix on S3Args, --keep on CompactArgs, help text |
| `hadb/hadb-cli/src/runner.rs` | explain default, test fix |
| `hadb/hadb-cli/src/config.rs` | keep field on RetentionSection |
| `haqlite/src/ops.rs` | prefix params, SnapshotResult, shutdown, normalize_prefix |
| `haqlite/src/bin/main.rs` | resolve_prefix, wire prefix, fix outputs, explain override |
| `haqlite/tests/test_ops.rs` | update 37 tests, add ~8 prefix tests |

## SQLite Extensions Support

haqlite should support SQLite extensions (sqlite-vec, FTS5, sqlean, etc.) across the cluster. Extensions must be loaded on ALL connections — leader rw, follower reads, and walrust LTX apply — otherwise WAL replay fails or silently corrupts data.

**Dual-mode design:**

- **Embedded mode**: `.connection_init(|conn| { conn.load_extension(...)?; Ok(()) })` callback on `HaQLiteBuilder`. Stored as `Option<Arc<dyn Fn(&Connection) -> Result<()> + Send + Sync>>`. Called after every `Connection::open`. User controls what gets loaded and how. No S3 coordination needed — same binary = same extensions.

- **Server mode**: `haqlite serve --extension sqlite-vec.so --extension sqlean.so` CLI args. haqlite loads each extension on every connection. Leader writes extension name list to S3 cluster metadata (alongside lease). Followers read the list on startup and verify their local config matches — mismatch = crash with clear error (e.g., "leader requires sqlite-vec but this node doesn't have it configured").

**Implementation:**
- Store `Vec<PathBuf>` (server) or `Arc<dyn Fn>` (embedded) in `HaQLiteInner`
- Call on every `Connection::open` site: leader rw connection, follower fresh read connections, walrust apply connections
- walrust needs to accept an `on_connection_open` callback so it loads extensions on its apply connections too
- S3 verification (server mode only): leader PUTs extension list to `{prefix}/extensions.json`, follower GETs and compares on join

**Testing:**
- Test with a real extension (sqlite-vec or sqlean) loaded on leader and follower
- Test that WAL with extension data replays correctly on follower
- Test that missing extension on follower crashes immediately (not silent corruption)
- Test S3 extension list verification (server mode)

## crates.io Publish

- Verify public API surface is clean (no accidental pub internals)
- `cargo publish --dry-run`
- Publish order: walrust → hadb → hadb-s3 → haqlite

## hakuzu (Kuzu/graph HA)

- `KuzuReplicator: Replicator` (wraps graphstream)
- `GraphFollowerBehavior: FollowerBehavior` (checkpoint tracking, not TXID)
- Extract graphstream crate from graphd
- Integrate into graphd with `--ha` CLI flags

