use crate::bonded_download::BondedDownload;
use crate::proxy_orchestrator::ProxyOrchestrator;
use crate::transfer::TransferTracker;
use murmur_coordinator::CoordinatorLifecycle;
use murmur_core::types::{ManifestId, NodeId};
use murmur_net::PeerConnection;
use murmur_overlay::OverlayStateTable;
use murmur_scheduler::ChunkScheduler;
use murmur_storage::ChunkStore;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct NodeState {
    pub node_id: NodeId,
    pub overlay: Arc<RwLock<OverlayStateTable>>,
    pub manifests: Arc<RwLock<HashMap<ManifestId, murmur_core::manifest::Manifest>>>,
    pub manifest_holders: Arc<RwLock<HashMap<ManifestId, HashSet<NodeId>>>>,
    pub coordinator: Arc<Mutex<CoordinatorLifecycle>>,
    pub tracker: Arc<tokio::sync::RwLock<TransferTracker>>,
    pub scheduler: Arc<Mutex<ChunkScheduler>>,
    pub storage: Arc<ChunkStore>,
    pub connections: Arc<RwLock<HashMap<NodeId, Arc<PeerConnection>>>>,
    pub banned_peers: Arc<RwLock<HashSet<NodeId>>>,
    pub download_destinations: Arc<RwLock<HashMap<ManifestId, String>>>,
    pub bonded_downloads: Arc<RwLock<HashMap<ManifestId, BondedDownload>>>,
    pub proxy_orchestrator: Arc<ProxyOrchestrator>,
    pub wan_bandwidth: u64,
    pub malicious: bool,
    pub slow_loris: bool,
}

impl NodeState {
    pub async fn new(
        node_id: NodeId,
        storage_dir: PathBuf,
        malicious: bool,
        slow_loris: bool,
        wan_bandwidth: u64,
    ) -> Result<Self, crate::error::DaemonError> {
        let config = murmur_core::cluster::ClusterConfig::default();
        let overlay = OverlayStateTable::new(config);
        let coordinator = CoordinatorLifecycle::new();
        let scheduler = ChunkScheduler::new(3);
        let storage = ChunkStore::new(storage_dir).await?;

        let overlay = Arc::new(RwLock::new(overlay));
        let connections = Arc::new(RwLock::new(HashMap::new()));

        let proxy_orchestrator = Arc::new(ProxyOrchestrator::new(
            node_id,
            wan_bandwidth,
            overlay.clone(),
            connections.clone(),
        ));

        Ok(Self {
            node_id,
            overlay,
            manifests: Arc::new(RwLock::new(HashMap::new())),
            manifest_holders: Arc::new(RwLock::new(HashMap::new())),
            coordinator: Arc::new(Mutex::new(coordinator)),
            tracker: Arc::new(tokio::sync::RwLock::new(TransferTracker::new())),
            scheduler: Arc::new(Mutex::new(scheduler)),
            storage: Arc::new(storage),
            connections,
            banned_peers: Arc::new(RwLock::new(HashSet::new())),
            download_destinations: Arc::new(RwLock::new(HashMap::new())),
            bonded_downloads: Arc::new(RwLock::new(HashMap::new())),
            proxy_orchestrator,
            wan_bandwidth,
            malicious,
            slow_loris,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_state_initialization() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = NodeState::new(
            NodeId(10),
            temp_dir.path().to_path_buf(),
            false,
            false,
            50_000_000,
        )
        .await
        .unwrap();

        assert_eq!(state.node_id.0, 10);

        // Check initial overlay state
        let overlay = state.overlay.read().await;
        assert_eq!(overlay.active_nodes().len(), 0);
        assert!(overlay.coordinator().is_none());
        assert_eq!(overlay.node_count(), 0);
    }

    #[tokio::test]
    async fn test_manifest_store_add_retrieve() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = NodeState::new(
            NodeId(1),
            temp_dir.path().to_path_buf(),
            false,
            false,
            50_000_000,
        )
        .await
        .unwrap();

        let manifest = murmur_core::manifest::Manifest {
            id: murmur_core::types::ManifestId::new(),
            name: "test.txt".into(),
            total_size: 100,
            chunks: vec![],
            file_hash: [0; 32],
            chunk_size: 1024,
            source: murmur_core::manifest::ManifestSource::LocalFile {
                path: std::path::PathBuf::from("test.txt"),
            },
            created_at: murmur_core::types::SimTime::ZERO,
        };

        state
            .manifests
            .write()
            .await
            .insert(manifest.id, manifest.clone());

        let retrieved = state.manifests.read().await.get(&manifest.id).cloned();
        assert_eq!(retrieved.unwrap().name, "test.txt");
    }
}
