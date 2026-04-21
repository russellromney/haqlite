//! Turbolite replicator: hadb::Replicator implementation for turbolite VFS.
//!
//! Translates between turbolite's native Manifest type and turbodb's
//! Backend::Turbolite variant for manifest-based coordination.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use hadb::Replicator;
use turbodb::{
    BTreeManifestEntry as HaBTreeEntry, Backend, FrameEntry as HaFrameEntry, ManifestStore,
    SubframeOverride as HaSubframeOverride,
};
use turbolite::tiered::{
    BTreeManifestEntry as TlBTreeEntry, FrameEntry as TlFrameEntry, Manifest as TurboliteManifest,
    SharedTurboliteVfs, SubframeOverride as TlSubframeOverride, TurboliteVfs,
};

/// Replicator that uses turbolite's VFS manifest for state transfer.
///
/// Instead of shipping WAL frames (like walrust), turbolite replication works
/// by synchronizing the page-group manifest. The VFS handles page-level
/// storage, so "replication" is just manifest propagation.
pub struct TurboliteReplicator {
    vfs: SharedTurboliteVfs,
    manifest_store: Arc<dyn ManifestStore>,
    manifest_key: String,
}

impl TurboliteReplicator {
    pub fn new(
        vfs: SharedTurboliteVfs,
        manifest_store: Arc<dyn ManifestStore>,
        prefix: &str,
        db_name: &str,
    ) -> Self {
        Self {
            vfs,
            manifest_store,
            manifest_key: format!("{}{}/_manifest", prefix, db_name),
        }
    }

    pub fn vfs(&self) -> &TurboliteVfs {
        &self.vfs
    }
}

#[async_trait]
impl Replicator for TurboliteReplicator {
    async fn add(&self, _name: &str, _path: &Path) -> Result<()> {
        // VFS already registered; nothing to do.
        Ok(())
    }

    async fn pull(&self, _name: &str, _path: &Path) -> Result<()> {
        // Fetch manifest from store, extract turbolite state, apply to VFS.
        if let Some(ha_manifest) = self.manifest_store.get(&self.manifest_key).await? {
            if let Backend::Turbolite { .. } = &ha_manifest.storage {
                let tl_manifest = ha_storage_to_turbolite(&ha_manifest.storage);
                self.vfs.set_manifest(tl_manifest);
            }
        }
        Ok(())
    }

    async fn remove(&self, _name: &str) -> Result<()> {
        Ok(())
    }

    async fn sync(&self, _name: &str) -> Result<()> {
        // Flush pending uploads to storage (S3 or HTTP API).
        // In local mode, sync happens via the VFS xSync callback during checkpoint.
        self.vfs
            .flush_to_storage()
            .map_err(|e| anyhow::anyhow!("turbolite flush_to_storage failed: {}", e))?;
        Ok(())
    }
}

// ============================================================================
// Manifest conversion: turbolite <-> hadb
// ============================================================================

/// Convert a turbolite Manifest to hadb Backend::Turbolite.
pub fn turbolite_to_ha_storage(m: &TurboliteManifest) -> Backend {
    Backend::Turbolite {
        turbolite_version: m.version,
        page_count: m.page_count,
        page_size: m.page_size,
        pages_per_group: m.pages_per_group,
        sub_pages_per_frame: m.sub_pages_per_frame,
        strategy: format!("{:?}", m.strategy),
        page_group_keys: m.page_group_keys.clone(),
        frame_tables: m
            .frame_tables
            .iter()
            .map(|ft| {
                ft.iter()
                    .map(|e| HaFrameEntry {
                        offset: e.offset,
                        len: e.len,
                        page_count: 0,
                    })
                    .collect()
            })
            .collect(),
        group_pages: m.group_pages.clone(),
        btrees: m
            .btrees
            .iter()
            .map(|(k, v)| {
                (
                    *k,
                    HaBTreeEntry {
                        name: v.name.clone(),
                        obj_type: v.obj_type.clone(),
                        group_ids: v.group_ids.clone(),
                    },
                )
            })
            .collect(),
        interior_chunk_keys: m
            .interior_chunk_keys
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect(),
        index_chunk_keys: m
            .index_chunk_keys
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect(),
        subframe_overrides: m
            .subframe_overrides
            .iter()
            .map(|ovs| {
                ovs.iter()
                    .map(|(k, v)| {
                        (
                            *k,
                            HaSubframeOverride {
                                key: v.key.clone(),
                                entry: HaFrameEntry {
                                    offset: v.entry.offset,
                                    len: v.entry.len,
                                    page_count: 0,
                                },
                            },
                        )
                    })
                    .collect()
            })
            .collect(),
        db_header: m.db_header.clone(),
    }
}

/// Convert a turbolite Manifest + walrust position to hybrid Backend::TurboliteWalrust.
pub fn turbolite_walrust_to_ha_storage(
    m: &TurboliteManifest,
    walrust_txid: u64,
    walrust_changeset_prefix: &str,
) -> Backend {
    Backend::TurboliteWalrust {
        turbolite_version: m.version,
        page_count: m.page_count,
        page_size: m.page_size,
        pages_per_group: m.pages_per_group,
        sub_pages_per_frame: m.sub_pages_per_frame,
        strategy: format!("{:?}", m.strategy),
        page_group_keys: m.page_group_keys.clone(),
        frame_tables: m.frame_tables.iter().map(|ft| {
            ft.iter().map(|e| HaFrameEntry { offset: e.offset, len: e.len, page_count: 0 }).collect()
        }).collect(),
        group_pages: m.group_pages.clone(),
        btrees: m.btrees.iter().map(|(k, v)| {
            (*k, HaBTreeEntry { name: v.name.clone(), obj_type: v.obj_type.clone(), group_ids: v.group_ids.clone() })
        }).collect(),
        interior_chunk_keys: m.interior_chunk_keys.iter().map(|(k, v)| (*k, v.clone())).collect(),
        index_chunk_keys: m.index_chunk_keys.iter().map(|(k, v)| (*k, v.clone())).collect(),
        subframe_overrides: m.subframe_overrides.iter().map(|ovs| {
            ovs.iter().map(|(k, v)| {
                (*k, HaSubframeOverride { key: v.key.clone(), entry: HaFrameEntry { offset: v.entry.offset, len: v.entry.len, page_count: 0 } })
            }).collect()
        }).collect(),
        db_header: m.db_header.clone(),
        walrust_txid,
        walrust_changeset_prefix: walrust_changeset_prefix.to_string(),
    }
}

/// Convert hadb Backend::Turbolite back to a turbolite Manifest.
///
/// The returned manifest will have `detect_and_normalize_strategy()` called
/// on it by `TurboliteVfs::set_manifest()`, so we do not need to set the
/// strategy field or build the page_index here.
pub fn ha_storage_to_turbolite(storage: &Backend) -> TurboliteManifest {
    match storage {
        Backend::Turbolite {
            turbolite_version, page_count, page_size, pages_per_group, sub_pages_per_frame,
            strategy: _, page_group_keys, frame_tables, group_pages, btrees,
            interior_chunk_keys, index_chunk_keys, subframe_overrides, db_header,
        }
        | Backend::TurboliteWalrust {
            turbolite_version, page_count, page_size, pages_per_group, sub_pages_per_frame,
            strategy: _, page_group_keys, frame_tables, group_pages, btrees,
            interior_chunk_keys, index_chunk_keys, subframe_overrides, db_header,
            walrust_txid: _, walrust_changeset_prefix: _,
        } => TurboliteManifest {
            version: *turbolite_version,
            change_counter: 0,
            // turbodb::Backend does not carry turbolite's epoch field;
            // this conversion path (haqlite manifest_store → turbolite) is
            // used by ha followers catching up from an HA manifest store.
            // Phase Strata's fork epoch is delivered via the direct
            // fetch_and_apply_remote_manifest() path (StorageBackend) and
            // via the engine's pre-wake check — not through here.
            epoch: 0,
            page_count: *page_count,
            page_size: *page_size,
            pages_per_group: *pages_per_group,
            sub_pages_per_frame: *sub_pages_per_frame,
            // strategy is detected from group_pages by detect_and_normalize_strategy()
            strategy: turbolite::tiered::GroupingStrategy::Positional,
            page_group_keys: page_group_keys.clone(),
            frame_tables: frame_tables
                .iter()
                .map(|ft| {
                    ft.iter()
                        .map(|e| TlFrameEntry {
                            offset: e.offset,
                            len: e.len,
                        })
                        .collect()
                })
                .collect(),
            group_pages: group_pages.clone(),
            btrees: btrees
                .iter()
                .map(|(k, v)| {
                    (
                        *k,
                        TlBTreeEntry {
                            name: v.name.clone(),
                            obj_type: v.obj_type.clone(),
                            group_ids: v.group_ids.clone(),
                        },
                    )
                })
                .collect(),
            interior_chunk_keys: interior_chunk_keys
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
            index_chunk_keys: index_chunk_keys
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
            subframe_overrides: subframe_overrides
                .iter()
                .map(|ovs| {
                    ovs.iter()
                        .map(|(k, v)| {
                            (
                                *k,
                                TlSubframeOverride {
                                    key: v.key.clone(),
                                    entry: TlFrameEntry {
                                        offset: v.entry.offset,
                                        len: v.entry.len,
                                    },
                                },
                            )
                        })
                        .collect()
                })
                .collect(),
            // Skip fields are built by detect_and_normalize_strategy()
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            db_header: db_header.clone(),
        },
        _ => {
            // Should never be called with non-Turbolite variant.
            // Return a minimal manifest that won't cause harm.
            TurboliteManifest {
                version: 0,
                change_counter: 0,
                epoch: 0,
                page_count: 0,
                page_size: 0,
                pages_per_group: 0,
                sub_pages_per_frame: 0,
                strategy: turbolite::tiered::GroupingStrategy::Positional,
                page_group_keys: Vec::new(),
                frame_tables: Vec::new(),
                group_pages: Vec::new(),
                btrees: HashMap::new(),
                interior_chunk_keys: HashMap::new(),
                index_chunk_keys: HashMap::new(),
                subframe_overrides: Vec::new(),
                page_index: HashMap::new(),
                btree_groups: HashMap::new(),
                page_to_tree_name: HashMap::new(),
                tree_name_to_groups: HashMap::new(),
                group_to_tree_name: HashMap::new(),
                db_header: None,
            }
        }
    }
}

/// Extract walrust fields from a TurboliteWalrust manifest.
/// Returns (walrust_txid, walrust_changeset_prefix) or None if not a hybrid manifest.
pub fn extract_walrust_fields(storage: &Backend) -> Option<(u64, &str)> {
    match storage {
        Backend::TurboliteWalrust { walrust_txid, walrust_changeset_prefix, .. } => {
            Some((*walrust_txid, walrust_changeset_prefix))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use turbolite::tiered::GroupingStrategy;

    #[test]
    fn roundtrip_empty_manifest() {
        let tl = TurboliteManifest {
            version: 0,
            change_counter: 0,
            page_count: 0,
            page_size: 0,
            pages_per_group: 0,
            sub_pages_per_frame: 0,
            strategy: GroupingStrategy::Positional,
            page_group_keys: Vec::new(),
            frame_tables: Vec::new(),
            group_pages: Vec::new(),
            btrees: HashMap::new(),
            interior_chunk_keys: HashMap::new(),
            index_chunk_keys: HashMap::new(),
            subframe_overrides: Vec::new(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            db_header: None,
            epoch: 0,
        };

        let ha = turbolite_to_ha_storage(&tl);
        let back = ha_storage_to_turbolite(&ha);
        assert_eq!(back.page_count, 0);
        assert_eq!(back.page_size, 0);
        assert!(back.page_group_keys.is_empty());
    }

    #[test]
    fn roundtrip_populated_manifest() {
        let tl = TurboliteManifest {
            version: 5,
            change_counter: 42,
            page_count: 100,
            page_size: 4096,
            pages_per_group: 256,
            sub_pages_per_frame: 16,
            strategy: GroupingStrategy::BTreeAware,
            page_group_keys: vec!["pg/0_v1".into(), "pg/1_v1".into()],
            frame_tables: vec![
                vec![TlFrameEntry {
                    offset: 0,
                    len: 8192,
                }],
                vec![TlFrameEntry {
                    offset: 0,
                    len: 4096,
                }],
            ],
            group_pages: vec![vec![0, 1, 2], vec![3, 4]],
            btrees: HashMap::from([(
                1,
                TlBTreeEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                },
            )]),
            interior_chunk_keys: HashMap::from([(0, "ic/0".into())]),
            index_chunk_keys: HashMap::from([(0, "idx/0".into())]),
            subframe_overrides: vec![
                HashMap::from([(
                    0,
                    TlSubframeOverride {
                        key: "sf/0".into(),
                        entry: TlFrameEntry {
                            offset: 0,
                            len: 1024,
                        },
                    },
                )]),
                HashMap::new(),
            ],
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            db_header: None,
            epoch: 0,
        };

        let ha = turbolite_to_ha_storage(&tl);

        // Verify hadb types
        match &ha {
            Backend::Turbolite {
                page_count,
                page_size,
                pages_per_group,
                page_group_keys,
                btrees,
                subframe_overrides,
                ..
            } => {
                assert_eq!(*page_count, 100);
                assert_eq!(*page_size, 4096);
                assert_eq!(*pages_per_group, 256);
                assert_eq!(page_group_keys.len(), 2);
                assert_eq!(btrees.len(), 1);
                assert!(btrees.contains_key(&1));
                assert_eq!(subframe_overrides.len(), 2);
            }
            _ => panic!("expected Turbolite variant"),
        }

        // Round-trip back
        let back = ha_storage_to_turbolite(&ha);
        assert_eq!(back.page_count, 100);
        assert_eq!(back.page_size, 4096);
        assert_eq!(back.pages_per_group, 256);
        assert_eq!(back.sub_pages_per_frame, 16);
        assert_eq!(back.page_group_keys.len(), 2);
        assert_eq!(back.frame_tables.len(), 2);
        assert_eq!(back.group_pages.len(), 2);
        assert_eq!(back.btrees.len(), 1);
        assert_eq!(back.interior_chunk_keys.len(), 1);
        assert_eq!(back.index_chunk_keys.len(), 1);
        assert_eq!(back.subframe_overrides.len(), 2);
        assert_eq!(back.subframe_overrides[0].len(), 1);
    }

    #[test]
    fn ha_storage_to_turbolite_with_wrong_variant_returns_empty() {
        let walrust = Backend::Walrust {
            txid: 1,
            changeset_prefix: "cs/".into(),
            latest_changeset_key: "cs/1".into(),
            snapshot_key: None,
            snapshot_txid: None,
        };
        let result = ha_storage_to_turbolite(&walrust);
        assert_eq!(result.page_count, 0);
        assert_eq!(result.page_size, 0);
    }
}
