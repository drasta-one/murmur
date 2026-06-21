use serde::{Deserialize, Serialize};
use crate::types::NodeId;

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcRequest {
    Status,
    Seed { url: String },
    ListManifests,
    Reassemble { manifest_id: String, out_path: String },
    StartDownload { manifest_id: String },
    Progress { manifest_id: String },
    Stop,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResponse {
    Status {
        node_id: NodeId,
        active_peers: usize,
        is_coordinator: bool,
    },
    ManifestList {
        manifests: Vec<(String, String)>, // (ManifestId, Name)
    },
    TransferProgressReport {
        manifest_id: String,
        percentage: f64,
        is_complete: bool,
    },
    Ok { message: String },
    Error { message: String },
}
