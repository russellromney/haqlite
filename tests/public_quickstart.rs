//! Public API smoke test matching the README quick-start shape.

use std::sync::Arc;

use haqlite::{HaQLite, InMemoryLeaseStore, SqlValue};
use tempfile::TempDir;

mod common;

#[tokio::test(flavor = "multi_thread")]
async fn public_quickstart_create_write_read_close() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("quickstart.db");

    let mut db = HaQLite::builder()
        .lease_store(Arc::new(InMemoryLeaseStore::new()))
        .walrust_storage(Arc::new(common::InMemoryStorage::new()))
        .open(
            db_path.to_str().expect("utf8 path"),
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);",
        )
        .await
        .expect("open quickstart db");

    db.execute(
        "INSERT INTO users (name) VALUES (?1)",
        &[SqlValue::Text("Alice".into())],
    )
    .expect("insert");

    let count: i64 = db
        .query_row_local("SELECT COUNT(*) FROM users", &[], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 1);

    db.close().await.expect("close");
}
