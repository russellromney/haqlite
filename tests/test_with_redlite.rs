//! Integration tests: HaQLite connection() + redlite from_shared_connection().
//!
//! Verifies the library boundary between haqlite and redlite works correctly
//! for Phase Vulcan (Redis databases on the HaQLite/turbolite stack).
//!
//! These tests exercise:
//! - HaQLite::connection() returns a usable shared connection
//! - redlite::Db::from_shared_connection() accepts it
//! - Redis operations work over the shared connection
//! - Data is visible via both redlite and raw SQL on the same connection
//! - All Redis data types work through the shared path
//! - Stop/recreate cycles maintain data (simulating engine stop/wake)
//! - Concurrent operations on the shared connection

use std::sync::Arc;
use redlite::types::ZMember;

fn temp_db() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.db").to_str().expect("path").to_string();
    (dir, path)
}

// ========================================================================
// HAPPY PATH
// ========================================================================

#[tokio::test]
async fn haqlite_connection_works_with_redlite() {
    let (_dir, path) = temp_db();
    let db = haqlite::HaQLite::local(&path, "").expect("local");

    let conn = db.connection().expect("connection");
    let rdb = redlite::Db::from_shared_connection(conn, 1).expect("redlite");

    rdb.set("hello", b"world", None).expect("set");
    let val = rdb.get("hello").expect("get");
    assert_eq!(val, Some(b"world".to_vec()));
}

#[tokio::test]
async fn redlite_tables_visible_via_raw_sql() {
    let (_dir, path) = temp_db();
    let db = haqlite::HaQLite::local(&path, "").expect("local");

    let conn = db.connection().expect("connection");
    let rdb = redlite::Db::from_shared_connection(conn.clone(), 1).expect("redlite");

    // Write via redlite to create schema
    rdb.set("sql-check", b"works", None).expect("set");

    // Verify redlite's tables exist on the same connection
    let guard = conn.lock();
    let count: i64 = guard
        .query_row("SELECT COUNT(*) FROM keys WHERE key = ?1", rusqlite::params!["sql-check"], |r| r.get(0))
        .expect("raw sql query on redlite's keys table");
    assert_eq!(count, 1, "redlite key should be visible via raw SQL on shared connection");

    let val: Vec<u8> = guard
        .query_row(
            "SELECT s.value FROM strings s JOIN keys k ON s.key_id = k.id WHERE k.key = ?1",
            rusqlite::params!["sql-check"],
            |r| r.get(0),
        )
        .expect("raw sql join query");
    assert_eq!(val, b"works");
}

#[tokio::test]
async fn haqlite_schema_and_redlite_coexist() {
    let (_dir, path) = temp_db();
    // Create HaQLite with a custom schema (like a real SQL database would have)
    let db = haqlite::HaQLite::local(&path, "CREATE TABLE IF NOT EXISTS app_data (id INTEGER PRIMARY KEY, val TEXT)").expect("local");

    let conn = db.connection().expect("connection");

    // Write to haqlite's custom schema
    {
        let guard = conn.lock();
        guard.execute("INSERT INTO app_data (val) VALUES (?1)", rusqlite::params!["haqlite-data"]).expect("insert app_data");
    }

    // Now layer redlite on top
    let rdb = redlite::Db::from_shared_connection(conn.clone(), 1).expect("redlite");
    rdb.set("redis-key", b"redis-value", None).expect("set");

    // Both should coexist
    let guard = conn.lock();
    let app_val: String = guard.query_row("SELECT val FROM app_data WHERE id = 1", [], |r| r.get(0)).expect("app_data");
    assert_eq!(app_val, "haqlite-data");
    drop(guard);

    let redis_val = rdb.get("redis-key").expect("get");
    assert_eq!(redis_val, Some(b"redis-value".to_vec()));
}

// ========================================================================
// ALL DATA TYPES
// ========================================================================

#[tokio::test]
async fn all_redis_data_types_work_over_shared_connection() {
    let (_dir, path) = temp_db();
    let db = haqlite::HaQLite::local(&path, "").expect("local");

    let conn = db.connection().expect("connection");
    let rdb = redlite::Db::from_shared_connection(conn, 1).expect("redlite");

    // String
    rdb.set("str-key", b"str-value", None).expect("set");
    assert_eq!(rdb.get("str-key").expect("get"), Some(b"str-value".to_vec()));

    // Hash
    rdb.hset("hash-key", &[("field1", b"val1".as_slice()), ("field2", b"val2".as_slice())]).expect("hset");
    assert_eq!(rdb.hget("hash-key", "field1").expect("hget"), Some(b"val1".to_vec()));
    assert_eq!(rdb.hlen("hash-key").expect("hlen"), 2);

    // List
    rdb.rpush("list-key", &[b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]).expect("rpush");
    assert_eq!(rdb.llen("list-key").expect("llen"), 3);
    assert_eq!(rdb.lindex("list-key", 0).expect("lindex"), Some(b"a".to_vec()));

    // Set
    rdb.sadd("set-key", &[b"x".as_slice(), b"y".as_slice(), b"z".as_slice()]).expect("sadd");
    assert_eq!(rdb.scard("set-key").expect("scard"), 3);
    assert!(rdb.sismember("set-key", b"y").expect("sismember"));

    // Sorted Set
    rdb.zadd("zset-key", &[
        ZMember::new(1.0, b"alpha"),
        ZMember::new(2.0, b"beta"),
        ZMember::new(3.0, b"gamma"),
    ]).expect("zadd");
    assert_eq!(rdb.zcard("zset-key").expect("zcard"), 3);
}

// ========================================================================
// SHARED MODE BEHAVIOR
// ========================================================================

#[tokio::test]
async fn checkpoint_is_noop_in_shared_mode() {
    let (_dir, path) = temp_db();
    let db = haqlite::HaQLite::local(&path, "").expect("local");

    let conn = db.connection().expect("connection");
    let rdb = redlite::Db::from_shared_connection(conn, 1).expect("redlite");

    rdb.set("before-checkpoint", b"data", None).expect("set");

    // This should be a no-op (not crash, not corrupt)
    rdb.checkpoint().expect("checkpoint should be noop");

    // Data should still be intact
    assert_eq!(
        rdb.get("before-checkpoint").expect("get"),
        Some(b"data".to_vec())
    );
}

// ========================================================================
// STOP/RECREATE CYCLE (simulates engine stop/wake)
// ========================================================================

#[tokio::test]
async fn data_survives_haqlite_recreate() {
    let (_dir, path) = temp_db();

    // Create and write data
    {
        let db = haqlite::HaQLite::local(&path, "").expect("local");
        let conn = db.connection().expect("connection");
        let rdb = redlite::Db::from_shared_connection(conn, 1).expect("redlite");

        rdb.set("persist-key", b"persist-value", None).expect("set");
        rdb.hset("persist-hash", &[("f1", b"v1".as_slice())]).expect("hset");
        rdb.rpush("persist-list", &[b"item1".as_slice(), b"item2".as_slice()]).expect("rpush");

        // Drop db (simulates stop)
        drop(rdb);
        drop(db);
    }

    // Recreate and verify data survived
    {
        let db = haqlite::HaQLite::local(&path, "").expect("local reopen");
        let conn = db.connection().expect("connection");
        let rdb = redlite::Db::from_shared_connection(conn, 1).expect("redlite");

        assert_eq!(
            rdb.get("persist-key").expect("get"),
            Some(b"persist-value".to_vec()),
            "String should survive recreate"
        );
        assert_eq!(
            rdb.hget("persist-hash", "f1").expect("hget"),
            Some(b"v1".to_vec()),
            "Hash should survive recreate"
        );
        assert_eq!(rdb.llen("persist-list").expect("llen"), 2, "List should survive recreate");
    }
}

// ========================================================================
// CONCURRENT ACCESS
// ========================================================================

#[tokio::test]
async fn concurrent_operations_on_shared_connection() {
    let (_dir, path) = temp_db();
    let db = haqlite::HaQLite::local(&path, "").expect("local");

    let conn = db.connection().expect("connection");
    let rdb = Arc::new(redlite::Db::from_shared_connection(conn, 1).expect("redlite"));

    let mut handles = vec![];
    for i in 0..10 {
        let rdb = rdb.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("concurrent-{}", i);
            let val = format!("value-{}", i);
            rdb.set(&key, val.as_bytes(), None).expect("set");
            let got = rdb.get(&key).expect("get");
            assert_eq!(got, Some(val.into_bytes()), "concurrent read should match write");
        }));
    }

    for h in handles {
        h.await.expect("task");
    }

    // All 10 keys should exist
    for i in 0..10 {
        let key = format!("concurrent-{}", i);
        assert!(rdb.get(&key).expect("get").is_some(), "key {} should exist", key);
    }
}

// ========================================================================
// EDGE CASES
// ========================================================================

#[tokio::test]
async fn empty_database_operations() {
    let (_dir, path) = temp_db();
    let db = haqlite::HaQLite::local(&path, "").expect("local");

    let conn = db.connection().expect("connection");
    let rdb = redlite::Db::from_shared_connection(conn, 1).expect("redlite");

    // Read from empty db
    assert_eq!(rdb.get("nonexistent").expect("get"), None);
    assert_eq!(rdb.hget("nope", "field").expect("hget"), None);
    assert_eq!(rdb.llen("nope").expect("llen"), 0);
    assert_eq!(rdb.scard("nope").expect("scard"), 0);
    assert_eq!(rdb.zcard("nope").expect("zcard"), 0);
}

#[tokio::test]
async fn large_value_through_shared_connection() {
    let (_dir, path) = temp_db();
    let db = haqlite::HaQLite::local(&path, "").expect("local");

    let conn = db.connection().expect("connection");
    let rdb = redlite::Db::from_shared_connection(conn, 1).expect("redlite");

    // 1MB value
    let big = vec![b'X'; 1_000_000];
    rdb.set("big-key", &big, None).expect("set big");

    let got = rdb.get("big-key").expect("get big").expect("should exist");
    assert_eq!(got.len(), 1_000_000);
    assert!(got.iter().all(|&b| b == b'X'));
}

#[tokio::test]
async fn multiple_redlite_instances_same_connection() {
    // Two redlite Db instances sharing the same HaQLite connection.
    // This verifies the Arc<Mutex> sharing works correctly.
    let (_dir, path) = temp_db();
    let db = haqlite::HaQLite::local(&path, "").expect("local");

    let conn = db.connection().expect("connection");
    let rdb1 = redlite::Db::from_shared_connection(conn.clone(), 1).expect("redlite 1");
    let rdb2 = redlite::Db::from_shared_connection(conn, 1).expect("redlite 2");

    rdb1.set("shared-key", b"from-db1", None).expect("set via db1");

    // db2 should see db1's write
    let val = rdb2.get("shared-key").expect("get via db2");
    assert_eq!(val, Some(b"from-db1".to_vec()));

    // db2 writes, db1 reads
    rdb2.set("shared-key-2", b"from-db2", None).expect("set via db2");
    let val = rdb1.get("shared-key-2").expect("get via db1");
    assert_eq!(val, Some(b"from-db2".to_vec()));
}
