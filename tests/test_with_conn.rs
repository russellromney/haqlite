//! Tests for HaQLite::connection() API and write fencing.

use haqlite::{HaQLite, SqlValue};

fn temp_db() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.db").to_str().expect("path").to_string();
    (dir, path)
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)";

#[tokio::test]
async fn connection_basic_usage() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn = db.connection().expect("connection");
    {
        let guard = conn.lock();
        guard
            .execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Alice"])
            .expect("insert");
    }

    let count: i64 = {
        let guard = conn.lock();
        guard
            .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
            .expect("query")
    };
    assert_eq!(count, 1);
}

#[tokio::test]
async fn connection_transaction() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn = db.connection().expect("connection");
    {
        let mut guard = conn.lock();
        let tx = guard.transaction().expect("begin");
        tx.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Bob"])
            .expect("insert 1");
        tx.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Carol"])
            .expect("insert 2");
        tx.commit().expect("commit");
    }

    let count: i64 = {
        let guard = conn.lock();
        guard
            .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
            .expect("query")
    };
    assert_eq!(count, 2);
}

#[tokio::test]
async fn connection_transaction_rollback() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn = db.connection().expect("connection");

    {
        let guard = conn.lock();
        guard
            .execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Dave"])
            .expect("insert");
    }

    {
        let mut guard = conn.lock();
        let tx = guard.transaction().expect("begin");
        tx.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Eve"])
            .expect("insert in tx");
        tx.rollback().expect("rollback");
    }

    let count: i64 = {
        let guard = conn.lock();
        guard
            .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
            .expect("query")
    };
    assert_eq!(count, 1);
}

#[tokio::test]
async fn connection_same_arc_each_call() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn1 = db.connection().expect("conn1");
    let conn2 = db.connection().expect("conn2");
    assert!(std::sync::Arc::ptr_eq(&conn1, &conn2));
}

#[tokio::test]
async fn connection_prepared_statement_and_column_metadata() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn = db.connection().expect("connection");
    let guard = conn.lock();

    guard
        .execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Frank"])
        .expect("insert");

    let mut stmt = guard.prepare("SELECT id, name FROM users").expect("prepare");
    let columns: Vec<String> = (0..stmt.column_count())
        .map(|i| stmt.column_name(i).expect("col").to_string())
        .collect();
    assert_eq!(columns, vec!["id", "name"]);

    let rows: Vec<(i64, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query_map")
        .map(|r| r.expect("row"))
        .collect();
    assert_eq!(rows, vec![(1, "Frank".to_string())]);
}

#[tokio::test]
async fn connection_empty_table() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn = db.connection().expect("connection");
    let guard = conn.lock();
    let count: i64 = guard
        .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
        .expect("query");
    assert_eq!(count, 0);
}

// ========================================================================
// Fencing tests
// ========================================================================

#[tokio::test]
async fn fence_blocks_writes() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn = db.connection().expect("connection");

    // Insert works before fencing
    {
        let guard = conn.lock();
        guard
            .execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Alice"])
            .expect("insert should succeed before fence");
    }

    // Fence the connection
    db.fence();

    // Writes should fail
    {
        let guard = conn.lock();
        let result = guard.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Bob"]);
        assert!(result.is_err(), "insert should fail after fence");
    }

    // Reads should still work
    {
        let guard = conn.lock();
        let count: i64 = guard
            .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
            .expect("read should succeed after fence");
        assert_eq!(count, 1); // only Alice
    }
}

#[tokio::test]
async fn unfence_allows_writes_again() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn = db.connection().expect("connection");

    db.fence();

    // Writes blocked
    {
        let guard = conn.lock();
        assert!(guard.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Alice"]).is_err());
    }

    db.unfence();

    // Writes allowed again
    {
        let guard = conn.lock();
        guard
            .execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Bob"])
            .expect("insert should succeed after unfence");
    }

    let count: i64 = {
        let guard = conn.lock();
        guard.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).expect("count")
    };
    assert_eq!(count, 1); // only Bob (Alice's insert was denied)
}

#[tokio::test]
async fn fence_blocks_ddl() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn = db.connection().expect("connection");
    db.fence();

    let guard = conn.lock();

    // CREATE TABLE blocked
    assert!(guard.execute_batch("CREATE TABLE other (id INTEGER)").is_err());

    // DROP TABLE blocked
    assert!(guard.execute_batch("DROP TABLE users").is_err());

    // SELECT still works
    let count: i64 = guard
        .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
        .expect("select should work");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn fence_blocks_transactions() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");

    let conn = db.connection().expect("connection");

    // Insert a row first
    {
        let guard = conn.lock();
        guard.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Alice"]).expect("insert");
    }

    db.fence();

    // Transaction with write should fail
    {
        let mut guard = conn.lock();
        // BEGIN itself might succeed (it's a savepoint), but the INSERT inside should fail
        let tx = guard.transaction();
        match tx {
            Ok(tx) => {
                let result = tx.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Bob"]);
                assert!(result.is_err(), "insert in transaction should fail when fenced");
            }
            Err(_) => {
                // BEGIN itself was denied, also acceptable
            }
        }
    }

    // Data unchanged
    {
        let guard = conn.lock();
        let count: i64 = guard.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).expect("count");
        assert_eq!(count, 1); // only Alice
    }
}

#[tokio::test]
async fn fence_blocks_update() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");
    let conn = db.connection().expect("connection");

    {
        let guard = conn.lock();
        guard.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Alice"]).expect("insert");
    }

    db.fence();

    {
        let guard = conn.lock();
        let result = guard.execute("UPDATE users SET name = ?1 WHERE id = 1", rusqlite::params!["Bob"]);
        assert!(result.is_err(), "UPDATE should be blocked when fenced");

        let name: String = guard.query_row("SELECT name FROM users WHERE id = 1", [], |r| r.get(0)).expect("read");
        assert_eq!(name, "Alice");
    }
}

#[tokio::test]
async fn fence_blocks_delete() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");
    let conn = db.connection().expect("connection");

    {
        let guard = conn.lock();
        guard.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Alice"]).expect("insert");
    }

    db.fence();

    {
        let guard = conn.lock();
        let result = guard.execute("DELETE FROM users WHERE id = 1", []);
        assert!(result.is_err(), "DELETE should be blocked when fenced");

        let count: i64 = guard.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).expect("count");
        assert_eq!(count, 1);
    }
}

#[tokio::test]
async fn fence_blocks_pragma_writes() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");
    let conn = db.connection().expect("connection");

    db.fence();

    let guard = conn.lock();
    assert!(guard.execute_batch("PRAGMA user_version = 42").is_err(), "PRAGMA write should be blocked");
}

#[tokio::test]
async fn fence_blocks_attach() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");
    let conn = db.connection().expect("connection");

    db.fence();

    let guard = conn.lock();
    assert!(guard.execute_batch("ATTACH DATABASE ':memory:' AS other").is_err(), "ATTACH should be blocked");
}

#[tokio::test]
async fn fence_blocks_replace() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");
    let conn = db.connection().expect("connection");

    {
        let guard = conn.lock();
        guard.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", []).expect("insert");
    }

    db.fence();

    let guard = conn.lock();
    assert!(guard.execute("REPLACE INTO users (id, name) VALUES (1, 'Bob')", []).is_err(), "REPLACE should be blocked");
}

#[tokio::test]
async fn fence_allows_selects() {
    let (_dir, path) = temp_db();
    let db = HaQLite::local(&path, SCHEMA).expect("local");
    let conn = db.connection().expect("connection");

    {
        let guard = conn.lock();
        guard.execute("INSERT INTO users (name) VALUES (?1)", rusqlite::params!["Alice"]).expect("insert");
    }

    db.fence();

    let guard = conn.lock();
    let count: i64 = guard.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).expect("select");
    assert_eq!(count, 1);

    // Subqueries, joins, CTEs should all work
    let _: i64 = guard.query_row(
        "WITH cte AS (SELECT id FROM users) SELECT COUNT(*) FROM cte",
        [], |r| r.get(0),
    ).expect("CTE select");
}
