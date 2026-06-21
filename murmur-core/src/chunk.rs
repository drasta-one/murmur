//! Chunk definitions — the atomic unit of data transfer in DOR.

use serde::{Deserialize, Serialize};

use crate::types::{ChunkId, NodeId};

/// Metadata for a single chunk within a manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    /// Chunk identifier (index within the manifest).
    pub id: ChunkId,
    /// Byte offset of this chunk in the original file.
    pub offset: u64,
    /// Size of this chunk in bytes.
    pub size: u32,
    /// BLAKE3 hash of the chunk data.
    pub hash: [u8; 32],
}

/// Monotonic chunk lifecycle. Order is load-bearing:
/// a ChunkStatus can only advance forward, never regress.
/// The merge rule is: take the max() of two statuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum ChunkStatus {
    Unassigned  = 0,
    Assigned    = 1,
    InProgress  = 2,
    Verified    = 3,   // terminal state — never reassigned
}

impl ChunkStatus {
    /// Returns true if this status is terminal.
    /// Terminal chunks must never be reassigned.
    pub fn is_terminal(&self) -> bool {
        matches!(self, ChunkStatus::Verified)
    }
}

/// The ownership/transfer state of a chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkOwnership {
    pub chunk_id:    ChunkId,
    pub status:      ChunkStatus,
    pub assigned_to: Option<NodeId>,   // None when Unassigned
    pub verified_by: Option<NodeId>,   // set permanently on Verified
    pub epoch:       u64,              // which coordinator epoch set this
}

impl ChunkOwnership {
    pub fn new(chunk_id: ChunkId) -> Self {
        Self {
            chunk_id,
            status: ChunkStatus::Unassigned,
            assigned_to: None,
            verified_by: None,
            epoch: 0,
        }
    }

    /// Returns true if this chunk still needs to be downloaded.
    pub fn needs_download(&self) -> bool {
        matches!(self.status, ChunkStatus::Unassigned)
    }

    /// Returns true if this chunk has been successfully verified.
    pub fn is_verified(&self) -> bool {
        matches!(self.status, ChunkStatus::Verified)
    }

    /// Returns the nodes that hold a verified copy of this chunk.
    pub fn holders(&self) -> Vec<NodeId> {
        if let Some(node_id) = self.verified_by {
            vec![node_id]
        } else {
            vec![]
        }
    }
}

/// A node's local view of the chunks it owns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OstFragment {
    pub node_id: NodeId,
    pub epoch: u64,
    pub owned_chunks: Vec<(ChunkId, ChunkStatus)>,
    pub last_updated: crate::types::SimTime,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unassigned_needs_download() {
        let chunk = ChunkOwnership::new(ChunkId(0));
        assert!(chunk.needs_download());
    }

    #[test]
    fn verified_is_complete() {
        let mut chunk = ChunkOwnership::new(ChunkId(1));
        chunk.status = ChunkStatus::Verified;
        chunk.verified_by = Some(NodeId(2));
        assert!(chunk.is_verified());
        assert!(!chunk.needs_download());
        assert_eq!(chunk.holders().len(), 1);
    }
}
