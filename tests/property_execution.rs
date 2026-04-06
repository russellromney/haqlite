//! Property-based tests for HaQLite execution semantics.
//!
//! Uses proptest to generate random SQL operations and verify invariants.

mod common;

use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use tempfile::TempDir;

use common::InMemoryStorage;
use haqlite::{HaMode, HaQLite, HaQLiteError, InMemoryLeaseStore, InMemoryManifestStore, SqlValue};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS props (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    score REAL DEFAULT 0.0
);";

proptest! {
    #[test]
    fn leader_execute_always_succeeds(
        id in 1..10000i64,
        name in "[-a-zA-Z0-9 ]{1,32}",
        score in any::<f64>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            let db_path = tmp.path().join("leader_prop.db");
            let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

            let result = db.execute(
                "INSERT INTO props (id, name, score) VALUES (?1, ?2, ?3)",
                &[
                    SqlValue::Integer(id),
                    SqlValue::Text(name.clone()),
                    SqlValue::Real(score),
                ],
            ).await;

            if result.is_ok() {
                let count: i64 = db.query_row(
                    "SELECT COUNT(*) FROM props WHERE id = ?1",
                    rusqlite::params![id],
                    |r| r.get(0),
                ).unwrap();
                assert_eq!(count, 1);

                let stored_name: String = db.query_row(
                    "SELECT name FROM props WHERE id = ?1",
                    rusqlite::params![id],
                    |r| r.get(0),
                ).unwrap();
                assert_eq!(stored_name, name);
            }

            db.close().await.unwrap();
            result
        });

        assert!(result.is_ok(), "leader execute should never fail: {:?}", result);
    }
}

proptest! {
    #[test]
    fn query_row_never_mutates_state(
        initial_rows in 0..20usize,
        query_id in 0..100i64,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            let db_path = tmp.path().join("readonly_prop.db");
            let mut db = HaQLite::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

            for i in 0..initial_rows as i64 {
                db.execute(
                    "INSERT INTO props (id, name, score) VALUES (?1, ?2, ?3)",
                    &[
                        SqlValue::Integer(i),
                        SqlValue::Text(format!("row-{}", i)),
                        SqlValue::Real(i as f64 * 1.5),
                    ],
                ).await.unwrap();
            }

            let count_before: i64 = db.query_row(
                "SELECT COUNT(*) FROM props", &[], |r| r.get(0),
            ).unwrap();
            let sum_before: f64 = db.query_row(
                "SELECT COALESCE(SUM(score), 0.0) FROM props", &[], |r| r.get(0),
            ).unwrap();

            let _ = db.query_row(
                "SELECT COUNT(*) FROM props WHERE id > ?1",
                rusqlite::params![query_id],
                |r| r.get::<_, i64>(0),
            );
            let _ = db.query_row(
                "SELECT name FROM props WHERE id = ?1",
                rusqlite::params![query_id % (initial_rows as i64 + 1)],
                |r| r.get::<_, String>(0),
            );
            let _ = db.query_row(
                "SELECT MAX(score) FROM props", &[],
                |r| r.get::<_, f64>(0),
            );
            let _ = db.query_row(
                "SELECT MIN(score) FROM props", &[],
                |r| r.get::<_, f64>(0),
            );

            let count_after: i64 = db.query_row(
                "SELECT COUNT(*) FROM props", &[], |r| r.get(0),
            ).unwrap();
            let sum_after: f64 = db.query_row(
                "SELECT COALESCE(SUM(score), 0.0) FROM props", &[], |r| r.get(0),
            ).unwrap();

            assert_eq!(count_before, count_after,
                "query_row must not change row count: before={}, after={}", count_before, count_after);
            assert!((sum_before - sum_after).abs() < 1e-9,
                "query_row must not change sum of scores: before={}, after={}", sum_before, sum_after);

            db.close().await.unwrap();
        });
    }
}

proptest! {
    #[test]
    fn shared_mode_serialized_writes(
        num_writes in 1..10usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            let storage = Arc::new(InMemoryStorage::new());
            let lease_store = Arc::new(InMemoryLeaseStore::new());
            let manifest_store = Arc::new(InMemoryManifestStore::new());

            let db_path = tmp.path().join("shared_serial.db");
            let mut db = HaQLite::builder("test-bucket")
                .prefix("test/")
                .mode(HaMode::Shared)
                .lease_store(lease_store)
                .manifest_store(manifest_store)
                .walrust_storage(storage)
                .instance_id("prop-writer")
                .manifest_poll_interval(Duration::from_millis(50))
                .write_timeout(Duration::from_secs(5))
                .open(db_path.to_str().unwrap(), SCHEMA)
                .await
                .expect("open shared mode");

            let db = Arc::new(db);
            let mut handles = Vec::new();

            for i in 0..num_writes {
                let db_clone = db.clone();
                let id = i as i64;
                let handle = tokio::spawn(async move {
                    db_clone.execute(
                        "INSERT INTO props (id, name, score) VALUES (?1, ?2, ?3)",
                        &[
                            SqlValue::Integer(id),
                            SqlValue::Text(format!("prop-{}", id)),
                            SqlValue::Real(id as f64),
                        ],
                    ).await
                });
                handles.push(handle);
            }

            let mut successes = 0u64;
            for handle in handles {
                match handle.await.unwrap() {
                    Ok(_) => successes += 1,
                    Err(HaQLiteError::LeaseContention(_)) => {}
                    Err(e) => {
                        panic!("unexpected error: {:?}", e);
                    }
                }
            }

            let count: i64 = db.query_row(
                "SELECT COUNT(*) FROM props", &[], |r| r.get(0),
            ).unwrap();

            if let Ok(mut db) = Arc::try_unwrap(db) {
                let _ = db.close().await;
            }
            (successes, count as u64)
        });

        let (successes, count) = result;
        assert_eq!(successes, count,
            "row count ({}) should equal successful writes ({})", count, successes);
    }
}
