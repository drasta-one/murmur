//! Chunk reassignment on node failure.
//!
//! When a node crashes, all chunks assigned to it need to be reassigned
//! to surviving nodes. This module handles that logic.

use murmur_core::types::{ChunkId, NodeId};

use crate::strategy::ChunkAssignment;

/// Result of a reassignment operation.
#[derive(Debug, Clone)]
pub struct ReassignmentResult {
    /// New assignments for orphaned chunks.
    pub reassigned: Vec<ChunkAssignment>,
    /// Chunks that could not be reassigned (no surviving nodes).
    pub failed: Vec<ChunkId>,
}

/// Reassign orphaned chunks to surviving nodes using round-robin.
///
/// Orphaned chunks are those that were assigned to a failed node.
/// Surviving nodes are the remaining active nodes.
pub fn reassign_chunks(
    orphaned_chunks: &[ChunkId],
    surviving_nodes: &[NodeId],
) -> ReassignmentResult {
    if surviving_nodes.is_empty() {
        return ReassignmentResult {
            reassigned: Vec::new(),
            failed: orphaned_chunks.to_vec(),
        };
    }

    let reassigned: Vec<ChunkAssignment> = orphaned_chunks
        .iter()
        .enumerate()
        .map(|(i, &chunk_id)| ChunkAssignment {
            chunk_id,
            node_id: surviving_nodes[i % surviving_nodes.len()],
        })
        .collect();

    ReassignmentResult {
        reassigned,
        failed: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reassign_to_survivors() {
        let orphaned = vec![ChunkId(0), ChunkId(1), ChunkId(2), ChunkId(3)];
        let survivors = vec![NodeId(2), NodeId(4)];

        let result = reassign_chunks(&orphaned, &survivors);
        assert_eq!(result.reassigned.len(), 4);
        assert!(result.failed.is_empty());

        assert_eq!(result.reassigned[0].node_id, NodeId(2));
        assert_eq!(result.reassigned[1].node_id, NodeId(4));
        assert_eq!(result.reassigned[2].node_id, NodeId(2));
        assert_eq!(result.reassigned[3].node_id, NodeId(4));
    }

    #[test]
    fn no_survivors_all_fail() {
        let orphaned = vec![ChunkId(0), ChunkId(1)];
        let result = reassign_chunks(&orphaned, &[]);
        assert!(result.reassigned.is_empty());
        assert_eq!(result.failed.len(), 2);
    }

    #[test]
    fn empty_orphans() {
        let result = reassign_chunks(&[], &[NodeId(1)]);
        assert!(result.reassigned.is_empty());
        assert!(result.failed.is_empty());
    }
}
