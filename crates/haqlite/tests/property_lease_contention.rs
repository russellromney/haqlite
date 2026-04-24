//! Property-based tests for lease contention and leader demotion.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use proptest::prelude::*;
use tempfile::TempDir;

use common::InMemoryStorage;
use haqlite::{HaQLite, InMemoryLeaseStore, SqlValue};
use hadb::LeaseStore;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, data TEXT);";

proptest! {
    #[test]
    fn no_split_brain_under_lease_contention(
        num_nodes in 2..8usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let lease_store = Arc::new(InMemoryLeaseStore::new());
            let lease_key = "test/prop/_lease";
            let success_count = Arc::new(AtomicU64::new(0));
            let mut handles = Vec::new();

            for node_idx in 0..num_nodes {
                let lease_store = lease_store.clone();
                let success_count = success_count.clone();
                let instance_id = format!("node-{}", node_idx);
                let lease_data = serde_json::to_vec(&serde_json::json!({
                    "instance_id": instance_id,
                    "timestamp": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default().as_millis() as u64,
                    "ttl_secs": 5,
                })).unwrap();

                let handle = tokio::spawn(async move {
                    let result = lease_store.write_if_not_exists(lease_key, lease_data).await;
                    match result {
                        Ok(cas) if cas.success => {
                            success_count.fetch_add(1, Ordering::SeqCst);
                            true
                        }
                        _ => false,
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.await.unwrap();
            }

            let successes = success_count.load(Ordering::SeqCst);
            assert!(successes <= 1,
                "split brain: {} nodes acquired the same lease (num_nodes={})", successes, num_nodes);
        });
    }
}
