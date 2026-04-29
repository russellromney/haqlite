mod common;

use std::sync::Arc;

use common::InMemoryStorage;
use hadb::Replicator;
use haqlite::SqliteReplicator;
use tempfile::TempDir;

#[tokio::test]
async fn plain_sqlite_replicator_add_still_uploads_initial_snapshot() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("plain.db");

    let conn = rusqlite::Connection::open(&db_path).expect("open sqlite db");
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE demo (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO demo (value) VALUES ('hello');",
    )
    .expect("seed database");
    drop(conn);

    let storage = Arc::new(InMemoryStorage::new());
    let replicator = SqliteReplicator::new(
        storage.clone(),
        "plain/",
        walrust::ReplicationConfig::default(),
    );

    replicator
        .add("plain", &db_path)
        .await
        .expect("register database");

    let keys = storage.keys().await;
    assert!(
        keys.contains(&"plain/plain/0001/0000000000000001.hadbp".to_string()),
        "expected initial snapshot upload, got keys: {:?}",
        keys
    );
}
