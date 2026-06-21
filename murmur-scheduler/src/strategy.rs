//! Scheduling strategies for chunk-to-node assignment.
//!
//! A scheduling strategy decides which node downloads which chunk.
//! This module provides two strategies:
//! - **Round-robin**: deterministic, fair distribution
//! - **Bandwidth-weighted**: assigns more chunks to faster nodes

use murmur_core::types::{ChunkId, NodeId};

/// A chunk assignment: which node should download which chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkAssignment {
    pub chunk_id: ChunkId,
    pub node_id: NodeId,
}

/// Trait for scheduling strategies.
pub trait SchedulingStrategy {
    /// Assign chunks to nodes.
    ///
    /// Given a list of chunk IDs to download and a list of available nodes,
    /// produce an assignment mapping.
    fn assign(
        &self,
        chunks: &[ChunkId],
        nodes: &[(NodeId, u64)], // (NodeId, effective_bandwidth_bps)
    ) -> Vec<ChunkAssignment>;
}

/// Round-robin scheduling: distribute chunks evenly across nodes.
///
/// Chunk 0 → Node 0, Chunk 1 → Node 1, Chunk 2 → Node 2,
/// Chunk 3 → Node 0, ...
#[derive(Debug, Clone, Default)]
pub struct RoundRobinStrategy;

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl SchedulingStrategy for RoundRobinStrategy {
    fn assign(&self, chunks: &[ChunkId], nodes: &[(NodeId, u64)]) -> Vec<ChunkAssignment> {
        if nodes.is_empty() {
            return Vec::new();
        }

        chunks
            .iter()
            .enumerate()
            .map(|(i, &chunk_id)| ChunkAssignment {
                chunk_id,
                node_id: nodes[i % nodes.len()].0,
            })
            .collect()
    }
}

/// Bandwidth-weighted scheduling: assign more chunks to nodes with higher
/// WAN bandwidth.
///
/// The proportion of chunks assigned to a node is proportional to its
/// WAN bandwidth relative to the total cluster bandwidth.
#[derive(Debug, Clone, Default)]
pub struct BandwidthWeightedStrategy;

impl BandwidthWeightedStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl SchedulingStrategy for BandwidthWeightedStrategy {
    fn assign(&self, chunks: &[ChunkId], nodes: &[(NodeId, u64)]) -> Vec<ChunkAssignment> {
        if nodes.is_empty() || chunks.is_empty() {
            return Vec::new();
        }

        let total_bandwidth: u64 = nodes.iter().map(|(_, bw)| bw).sum();

        if total_bandwidth == 0 {
            // Fallback to round-robin if no bandwidth info
            return RoundRobinStrategy.assign(chunks, nodes);
        }

        // Calculate how many chunks each node should get
        let mut node_quotas: Vec<(NodeId, usize)> = nodes
            .iter()
            .map(|(id, bw)| {
                let share = (*bw as f64 / total_bandwidth as f64) * chunks.len() as f64;
                (*id, share.floor() as usize)
            })
            .collect();

        // Distribute remainder chunks to nodes with highest bandwidth
        let assigned: usize = node_quotas.iter().map(|(_, q)| q).sum();
        let mut remainder = chunks.len() - assigned;

        // Sort by bandwidth descending for remainder distribution
        let mut sorted_nodes: Vec<(usize, u64)> = nodes
            .iter()
            .enumerate()
            .map(|(i, (_, bw))| (i, *bw))
            .collect();
        sorted_nodes.sort_by_key(|b| std::cmp::Reverse(b.1));

        for (idx, _) in sorted_nodes {
            if remainder == 0 {
                break;
            }
            node_quotas[idx].1 += 1;
            remainder -= 1;
        }

        // Build assignments
        let mut assignments = Vec::with_capacity(chunks.len());
        let mut chunk_iter = chunks.iter();

        for (node_id, quota) in &node_quotas {
            for _ in 0..*quota {
                if let Some(&chunk_id) = chunk_iter.next() {
                    assignments.push(ChunkAssignment {
                        chunk_id,
                        node_id: *node_id,
                    });
                }
            }
        }

        assignments
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64, wan_bw: u64) -> (NodeId, u64) {
        (NodeId(id), wan_bw)
    }

    fn chunks(n: u32) -> Vec<ChunkId> {
        (0..n).map(ChunkId).collect()
    }

    #[test]
    fn round_robin_distributes_evenly() {
        let strategy = RoundRobinStrategy;
        let nodes = vec![node(1, 5_000_000), node(2, 5_000_000), node(3, 5_000_000)];
        let chunk_ids = chunks(6);

        let assignments = strategy.assign(&chunk_ids, &nodes);
        assert_eq!(assignments.len(), 6);

        // Chunks 0,3 → Node 1; Chunks 1,4 → Node 2; Chunks 2,5 → Node 3
        assert_eq!(assignments[0].node_id, NodeId(1));
        assert_eq!(assignments[1].node_id, NodeId(2));
        assert_eq!(assignments[2].node_id, NodeId(3));
        assert_eq!(assignments[3].node_id, NodeId(1));
        assert_eq!(assignments[4].node_id, NodeId(2));
        assert_eq!(assignments[5].node_id, NodeId(3));
    }

    #[test]
    fn round_robin_empty_nodes() {
        let strategy = RoundRobinStrategy;
        let assignments = strategy.assign(&chunks(5), &[]);
        assert!(assignments.is_empty());
    }

    #[test]
    fn bandwidth_weighted_assigns_more_to_fast_nodes() {
        let strategy = BandwidthWeightedStrategy;
        let nodes = vec![
            node(1, 10_000_000), // 10 MB/s — 50%
            node(2, 5_000_000),  // 5 MB/s  — 25%
            node(3, 5_000_000),  // 5 MB/s  — 25%
        ];
        let chunk_ids = chunks(20);

        let assignments = strategy.assign(&chunk_ids, &nodes);
        assert_eq!(assignments.len(), 20);

        // Count per node
        let n1 = assignments
            .iter()
            .filter(|a| a.node_id == NodeId(1))
            .count();
        let n2 = assignments
            .iter()
            .filter(|a| a.node_id == NodeId(2))
            .count();
        let n3 = assignments
            .iter()
            .filter(|a| a.node_id == NodeId(3))
            .count();

        // Node 1 should get ~50% (10), nodes 2&3 ~25% each (5)
        assert_eq!(n1, 10);
        assert_eq!(n2, 5);
        assert_eq!(n3, 5);
    }

    #[test]
    fn bandwidth_weighted_handles_remainder() {
        let strategy = BandwidthWeightedStrategy;
        let nodes = vec![node(1, 10_000_000), node(2, 5_000_000)];
        // 7 chunks: 10/(10+5) * 7 = 4.66 → floor(4), 5/(10+5) * 7 = 2.33 → floor(2)
        // assigned = 6, remainder = 1 → goes to node 1 (highest bw)
        let chunk_ids = chunks(7);

        let assignments = strategy.assign(&chunk_ids, &nodes);
        assert_eq!(assignments.len(), 7);

        let n1 = assignments
            .iter()
            .filter(|a| a.node_id == NodeId(1))
            .count();
        let n2 = assignments
            .iter()
            .filter(|a| a.node_id == NodeId(2))
            .count();
        assert_eq!(n1, 5); // 4 + 1 remainder
        assert_eq!(n2, 2);
    }

    #[test]
    fn bandwidth_weighted_empty() {
        let strategy = BandwidthWeightedStrategy;
        assert!(strategy.assign(&chunks(5), &[]).is_empty());
        assert!(strategy.assign(&[], &[node(1, 5_000_000)]).is_empty());
    }
}
