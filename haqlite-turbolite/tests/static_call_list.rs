//! Source-grep assertion that the hybrid follower-replay path does
//! not call any of the legacy temp-SQLite-restore primitives.
//!
//! Scope: only the bodies of `apply_manifest_payload` (follower
//! poll-time catch-up) and `restore_from_manifest` (replicator
//! pull / promotion catch-up). The fresh-tenant bootstrap path,
//! which legitimately uses `import_sqlite_file`, is out of scope.

use std::fs;
use std::path::PathBuf;

const FORBIDDEN: &[&str] = &[
    "materialize_to_file(",
    "pull_incremental(",
    "import_sqlite_file(",
    "replace_cache_from_sqlite_file",
    "normalize_replayed_sqlite_base",
];

fn read_source(rel: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {}", path.display(), e))
}

/// Strip `//` and `/* */` comments so doc commentary mentioning
/// the legacy names doesn't trip the substring check.
fn strip_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let bytes = src.as_bytes();
    let mut i = 0;
    let mut in_block_comment = false;
    while i < bytes.len() {
        if in_block_comment {
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                in_block_comment = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            // Line comment: skip to end of line.
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            in_block_comment = true;
            i += 2;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Returns the body of `async fn <name>` (between matched braces)
/// or None if the declaration isn't found.
fn extract_async_fn_body(src: &str, fn_name: &str) -> Option<String> {
    let needle = format!("async fn {}", fn_name);
    let start = src.find(&needle)?;
    // Skip past the signature to the first `{`.
    let after_sig = &src[start..];
    let body_start_rel = after_sig.find('{')?;
    let body_start = start + body_start_rel;
    let bytes = src.as_bytes();
    let mut depth = 0i32;
    let mut i = body_start;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(src[body_start..=i].to_string());
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn assert_no_forbidden_calls(rel: &str, fn_name: &str) {
    let raw = read_source(rel);
    let stripped = strip_comments(&raw);
    let body = extract_async_fn_body(&stripped, fn_name).unwrap_or_else(|| {
        panic!(
            "could not locate `async fn {}` in {}; the static call-list \
             test relies on this declaration shape — update the test if \
             the function was renamed",
            fn_name, rel
        )
    });
    for pat in FORBIDDEN {
        if let Some(idx) = body.find(pat) {
            let start = idx.saturating_sub(80);
            let end = (idx + pat.len() + 80).min(body.len());
            let context = &body[start..end];
            panic!(
                "{}::{} body contains forbidden legacy-replay call '{}' — \
                 the hybrid follower-apply path must not regress to the \
                 temp-SQLite-restore design. Context:\n---\n{}\n---",
                rel, fn_name, pat, context
            );
        }
    }
}

#[test]
fn apply_manifest_payload_has_no_legacy_replay_calls() {
    assert_no_forbidden_calls("src/follower_behavior.rs", "apply_manifest_payload");
}

#[test]
fn restore_from_manifest_has_no_legacy_replay_calls() {
    assert_no_forbidden_calls("src/replicator.rs", "restore_from_manifest");
}
