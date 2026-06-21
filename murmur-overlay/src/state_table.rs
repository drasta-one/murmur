//! Cluster-wide overlay state table.
//!
//! The [`OverlayStateTable`] is the authoritative view of all nodes in a DOR
//! cluster, their status, which chunks they own, who the current coordinator
//! is, and the current election term.

use std::collections::HashMap;

use murmur_core::{ClusterConfig, MurmurError, Node, NodeId, NodeStatus};
use tracing::{debug, info, warn};

/// Cluster-wide metadata view.
///
/// Tracks every node in the cluster together with coordinator state and the
/// active election term. All mutations are logged via `tracing`.
#[derive(Debug)]
pub struct OverlayStateTable {
    /// All known nodes keyed by their unique id.
    nodes: HashMap<NodeId, Node>,
    /// The current coordinator, if any.
    coordinator: Option<NodeId>,
    /// Monotonically increasing election term.
    term: u64,
    /// Cluster-level configuration.
    config: ClusterConfig,
    /// Chunk ownership state.
    pub chunk_ownership: HashMap<murmur_core::types::ChunkId, murmur_core::chunk::ChunkOwnership>,
}

impl OverlayStateTable {
    /// Create a new, empty state table with the given cluster configuration.
    pub fn new(config: ClusterConfig) -> Self {
        debug!(
            "OverlayStateTable created with max_nodes={}",
            config.max_nodes
        );
        Self {
            nodes: HashMap::new(),
            coordinator: None,
            term: 0,
            config,
            chunk_ownership: HashMap::new(),
        }
    }

    /// Add a node to the cluster.
    ///
    /// Returns [`MurmurError::ClusterFull`] if `max_nodes` would be exceeded.
    pub fn add_node(&mut self, node: Node) -> Result<(), MurmurError> {
        if self.nodes.len() >= self.config.max_nodes {
            warn!(
                node_id = %node.id,
                max = self.config.max_nodes,
                "Cluster full — cannot add node"
            );
            return Err(MurmurError::ClusterFull(self.config.max_nodes));
        }
        info!(node_id = %node.id, "Adding node to state table");
        self.nodes.insert(node.id, node);
        Ok(())
    }

    /// Remove a node from the cluster, returning it if it existed.
    pub fn remove_node(&mut self, node_id: NodeId) -> Option<Node> {
        let removed = self.nodes.remove(&node_id);
        if removed.is_some() {
            info!(node_id = %node_id, "Removed node from state table");
            // If the removed node was the coordinator, clear it.
            if self.coordinator == Some(node_id) {
                warn!(node_id = %node_id, "Removed node was the coordinator — clearing");
                self.coordinator = None;
            }
        }
        removed
    }

    /// Get an immutable reference to a node by ID.
    pub fn get_node(&self, node_id: NodeId) -> Option<&Node> {
        self.nodes.get(&node_id)
    }

    /// Get a mutable reference to a node by ID.
    pub fn get_node_mut(&mut self, node_id: NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(&node_id)
    }

    /// Return the IDs of all nodes whose status is [`NodeStatus::Active`] or
    /// [`NodeStatus::Coordinator`].
    pub fn active_nodes(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self
            .nodes
            .values()
            .filter(|n| matches!(n.status, NodeStatus::Active | NodeStatus::Coordinator))
            .map(|n| n.id)
            .collect();
        ids.sort();
        ids
    }

    /// Return the IDs of *every* node in the table regardless of status.
    pub fn all_node_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        ids.sort();
        ids
    }

    /// Total number of nodes currently tracked (all statuses).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Set a new coordinator and advance the election term.
    pub fn set_coordinator(&mut self, node_id: NodeId, term: u64) {
        info!(
            node_id = %node_id,
            term,
            "Setting coordinator"
        );
        self.coordinator = Some(node_id);
        self.term = term;
    }

    /// Clear the coordinator (e.g. after a crash or step-down).
    pub fn clear_coordinator(&mut self) {
        if let Some(old) = self.coordinator.take() {
            info!(old_coordinator = %old, "Coordinator cleared");
        }
    }

    /// The current coordinator, if any.
    pub fn coordinator(&self) -> Option<NodeId> {
        self.coordinator
    }

    /// The current election term.
    pub fn current_term(&self) -> u64 {
        self.term
    }

    /// A reference to the cluster configuration.
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Converts the node's authoritative state into an OstFragment for the given node.
    pub fn to_fragment(
        &self,
        node_id: NodeId,
        now: murmur_core::types::SimTime,
    ) -> murmur_core::chunk::OstFragment {
        let mut owned_chunks = Vec::new();
        for (chunk_id, ownership) in &self.chunk_ownership {
            // Include verified chunks
            if ownership.verified_by == Some(node_id) {
                owned_chunks.push((*chunk_id, murmur_core::chunk::ChunkStatus::Verified));
            } else if ownership.assigned_to == Some(node_id) {
                owned_chunks.push((*chunk_id, ownership.status));
            }
        }

        murmur_core::chunk::OstFragment {
            node_id,
            epoch: self.term,
            owned_chunks,
            last_updated: now,
        }
    }

    /// Merges an OstFragment into the global state by taking the maximum status per chunk.
    pub fn merge_fragment(&mut self, fragment: murmur_core::chunk::OstFragment) {
        if fragment.epoch < self.term {
            return; // Ignore stale fragments
        }
        for (chunk_id, status) in fragment.owned_chunks {
            let entry = self
                .chunk_ownership
                .entry(chunk_id)
                .or_insert_with(|| murmur_core::chunk::ChunkOwnership::new(chunk_id));

            if status > entry.status {
                entry.status = status;
                if status.is_terminal() {
                    entry.verified_by = Some(fragment.node_id);
                } else if status == murmur_core::chunk::ChunkStatus::InProgress
                    || status == murmur_core::chunk::ChunkStatus::Assigned
                {
                    entry.assigned_to = Some(fragment.node_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use murmur_core::{NodeConfig, SimTime};

    fn make_node(id: u64) -> Node {
        Node::new(NodeId(id), NodeConfig::default(), SimTime::ZERO)
    }

    fn default_table() -> OverlayStateTable {
        OverlayStateTable::new(ClusterConfig::default())
    }

    #[test]
    fn new_table_is_empty() {
        let table = default_table();
        assert_eq!(table.node_count(), 0);
        assert!(table.coordinator().is_none());
        assert_eq!(table.current_term(), 0);
    }

    #[test]
    fn add_and_get_node() {
        let mut table = default_table();
        let node = make_node(1);
        table.add_node(node).unwrap();
        assert_eq!(table.node_count(), 1);
        assert!(table.get_node(NodeId(1)).is_some());
        assert!(table.get_node(NodeId(99)).is_none());
    }

    #[test]
    fn remove_node_returns_it() {
        let mut table = default_table();
        table.add_node(make_node(1)).unwrap();
        let removed = table.remove_node(NodeId(1));
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, NodeId(1));
        assert_eq!(table.node_count(), 0);
    }

    #[test]
    fn remove_nonexistent_returns_none() {
        let mut table = default_table();
        assert!(table.remove_node(NodeId(42)).is_none());
    }

    #[test]
    fn cluster_full_error() {
        let config = ClusterConfig {
            max_nodes: 2,
            ..ClusterConfig::default()
        };
        let mut table = OverlayStateTable::new(config);
        table.add_node(make_node(1)).unwrap();
        table.add_node(make_node(2)).unwrap();
        let result = table.add_node(make_node(3));
        assert!(result.is_err());
    }

    #[test]
    fn active_nodes_filters_correctly() {
        let mut table = default_table();

        let mut n1 = make_node(1);
        n1.activate();
        table.add_node(n1).unwrap();

        // Node 2 stays in Joining
        table.add_node(make_node(2)).unwrap();

        let mut n3 = make_node(3);
        n3.promote_to_coordinator();
        table.add_node(n3).unwrap();

        let active = table.active_nodes();
        assert_eq!(active, vec![NodeId(1), NodeId(3)]);
    }

    #[test]
    fn all_node_ids_returns_sorted() {
        let mut table = default_table();
        table.add_node(make_node(5)).unwrap();
        table.add_node(make_node(1)).unwrap();
        table.add_node(make_node(3)).unwrap();
        assert_eq!(table.all_node_ids(), vec![NodeId(1), NodeId(3), NodeId(5)]);
    }

    #[test]
    fn coordinator_lifecycle() {
        let mut table = default_table();
        table.add_node(make_node(1)).unwrap();
        table.add_node(make_node(2)).unwrap();

        assert!(table.coordinator().is_none());
        assert_eq!(table.current_term(), 0);

        table.set_coordinator(NodeId(1), 1);
        assert_eq!(table.coordinator(), Some(NodeId(1)));
        assert_eq!(table.current_term(), 1);

        table.set_coordinator(NodeId(2), 2);
        assert_eq!(table.coordinator(), Some(NodeId(2)));
        assert_eq!(table.current_term(), 2);

        table.clear_coordinator();
        assert!(table.coordinator().is_none());
        // Term is preserved after clear
        assert_eq!(table.current_term(), 2);
    }

    #[test]
    fn remove_coordinator_clears_it() {
        let mut table = default_table();
        table.add_node(make_node(1)).unwrap();
        table.set_coordinator(NodeId(1), 1);
        assert_eq!(table.coordinator(), Some(NodeId(1)));

        table.remove_node(NodeId(1));
        assert!(table.coordinator().is_none());
    }

    #[test]
    fn get_node_mut_allows_mutation() {
        let mut table = default_table();
        table.add_node(make_node(1)).unwrap();

        let node = table.get_node_mut(NodeId(1)).unwrap();
        node.activate();

        assert_eq!(
            table.get_node(NodeId(1)).unwrap().status,
            NodeStatus::Active
        );
    }

    #[test]
    fn config_accessor() {
        let config = ClusterConfig {
            max_nodes: 7,
            ..ClusterConfig::default()
        };
        let table = OverlayStateTable::new(config);
        assert_eq!(table.config().max_nodes, 7);
    }

    #[test]
    fn merge_fragment_takes_max_status() {
        let mut table = default_table();
        table.chunk_ownership.insert(
            murmur_core::types::ChunkId(1),
            murmur_core::chunk::ChunkOwnership {
                chunk_id: murmur_core::types::ChunkId(1),
                status: murmur_core::chunk::ChunkStatus::Assigned,
                assigned_to: Some(NodeId(5)),
                verified_by: None,
                epoch: 0,
            },
        );

        // Fragment has InProgress (higher than Assigned)
        let fragment = murmur_core::chunk::OstFragment {
            node_id: NodeId(5),
            epoch: 0,
            owned_chunks: vec![(
                murmur_core::types::ChunkId(1),
                murmur_core::chunk::ChunkStatus::InProgress,
            )],
            last_updated: SimTime::ZERO,
        };
        table.merge_fragment(fragment);

        let owner = table
            .chunk_ownership
            .get(&murmur_core::types::ChunkId(1))
            .unwrap();
        assert_eq!(owner.status, murmur_core::chunk::ChunkStatus::InProgress);
        assert_eq!(owner.assigned_to, Some(NodeId(5)));
    }

    #[test]
    fn merge_does_not_regress_verified_chunks() {
        let mut table = default_table();
        table.chunk_ownership.insert(
            murmur_core::types::ChunkId(1),
            murmur_core::chunk::ChunkOwnership {
                chunk_id: murmur_core::types::ChunkId(1),
                status: murmur_core::chunk::ChunkStatus::Verified,
                assigned_to: None,
                verified_by: Some(NodeId(2)),
                epoch: 0,
            },
        );

        // Fragment reports Assigned (lower than Verified)
        let fragment = murmur_core::chunk::OstFragment {
            node_id: NodeId(5),
            epoch: 0,
            owned_chunks: vec![(
                murmur_core::types::ChunkId(1),
                murmur_core::chunk::ChunkStatus::Assigned,
            )],
            last_updated: SimTime::ZERO,
        };
        table.merge_fragment(fragment);

        let owner = table
            .chunk_ownership
            .get(&murmur_core::types::ChunkId(1))
            .unwrap();
        assert_eq!(owner.status, murmur_core::chunk::ChunkStatus::Verified);
        assert_eq!(owner.verified_by, Some(NodeId(2)));
    }

    #[test]
    fn merge_from_multiple_fragments_is_idempotent() {
        let mut table = default_table();

        let frag1 = murmur_core::chunk::OstFragment {
            node_id: NodeId(5),
            epoch: 0,
            owned_chunks: vec![(
                murmur_core::types::ChunkId(1),
                murmur_core::chunk::ChunkStatus::InProgress,
            )],
            last_updated: SimTime::ZERO,
        };

        let frag2 = murmur_core::chunk::OstFragment {
            node_id: NodeId(6),
            epoch: 0,
            owned_chunks: vec![(
                murmur_core::types::ChunkId(1),
                murmur_core::chunk::ChunkStatus::Assigned,
            )],
            last_updated: SimTime::ZERO,
        };

        table.merge_fragment(frag1.clone());
        table.merge_fragment(frag2);
        table.merge_fragment(frag1);

        let owner = table
            .chunk_ownership
            .get(&murmur_core::types::ChunkId(1))
            .unwrap();
        assert_eq!(owner.status, murmur_core::chunk::ChunkStatus::InProgress);
        assert_eq!(owner.assigned_to, Some(NodeId(5)));
    }

    #[test]
    fn old_epoch_fragment_is_rejected() {
        let mut table = default_table();
        table.term = 2; // current epoch is 2

        let fragment = murmur_core::chunk::OstFragment {
            node_id: NodeId(5),
            epoch: 1, // older epoch
            owned_chunks: vec![(
                murmur_core::types::ChunkId(1),
                murmur_core::chunk::ChunkStatus::Verified,
            )],
            last_updated: SimTime::ZERO,
        };

        table.merge_fragment(fragment);

        assert!(
            !table
                .chunk_ownership
                .contains_key(&murmur_core::types::ChunkId(1))
        );
    }
}
