use serde::{Deserialize, Serialize};
use crate::types::{ChunkId, ManifestId, NodeId};

/// Real-world network message payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetMessage {
    Handshake { node_id: NodeId },
    HeartbeatPing,
    HeartbeatAck,
    RequestManifest { manifest_id: ManifestId },
    ManifestData { manifest: crate::manifest::Manifest },
    RequestChunk { manifest_id: ManifestId, chunk_id: ChunkId },
    ChunkData { manifest_id: ManifestId, chunk_id: ChunkId, data: Vec<u8> },
    ChunkNotFound { manifest_id: ManifestId, chunk_id: ChunkId },
    Bitfield { manifest_id: ManifestId, chunks: Vec<ChunkId> },
    Have { manifest_id: ManifestId, chunk_id: ChunkId },
    AssignFetchRanges {
        url: String,
        manifest_id: ManifestId,
        coordinator_id: NodeId,
        assignments: Vec<(ChunkId, u64, u32)>, // (chunk_id, offset, size)
    },
    RequestMoreWork {
        manifest_id: ManifestId,
        node_id: NodeId, // The node asking for more work
    },
    // Proxy (Phase 7b)
    ProxyConnect { stream_id: u32, host: String, port: u16 },
    ProxyConnectResult { stream_id: u32, success: bool },
    ProxyData { stream_id: u32, data: Vec<u8> },
    ProxyClose { stream_id: u32 },
    // Later we can add coordinator election messages, manifest distribution, etc.
}
