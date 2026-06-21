//! Topology graph for overlay node reachability.
//!
//! The [`Topology`] tracks directed edges between nodes, representing which
//! nodes can communicate with which others. It supports both unidirectional
//! and bidirectional edges, neighbour queries, and a full-connectivity check
//! via BFS.

use std::collections::{HashMap, HashSet, VecDeque};

use murmur_core::NodeId;
use tracing::debug;

/// Directed graph of node-to-node reachability.
///
/// Internally stored as an adjacency-list (`HashMap<NodeId, HashSet<NodeId>>`).
/// A node only appears in the graph if it has at least one edge (outbound
/// *or* inbound).
#[derive(Debug, Clone)]
pub struct Topology {
    /// Adjacency list: `from → {to₁, to₂, …}`.
    edges: HashMap<NodeId, HashSet<NodeId>>,
}

impl Topology {
    /// Create an empty topology with no nodes or edges.
    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
        }
    }

    /// Add a directed edge from `from` to `to`.
    ///
    /// Both endpoints are implicitly added to the graph if not already present.
    pub fn add_edge(&mut self, from: NodeId, to: NodeId) {
        debug!(from = %from, to = %to, "Adding directed edge");
        self.edges.entry(from).or_default().insert(to);
        // Ensure `to` exists in the map even with no outbound edges so that
        // `node_count` and `all_nodes` account for it.
        self.edges.entry(to).or_default();
    }

    /// Add edges in both directions between `a` and `b`.
    pub fn add_bidirectional(&mut self, a: NodeId, b: NodeId) {
        self.add_edge(a, b);
        self.add_edge(b, a);
    }

    /// Remove a single directed edge.
    pub fn remove_edge(&mut self, from: NodeId, to: NodeId) {
        if let Some(neighbors) = self.edges.get_mut(&from) {
            neighbors.remove(&to);
        }
    }

    /// Remove edges in both directions between `a` and `b`.
    pub fn remove_bidirectional(&mut self, a: NodeId, b: NodeId) {
        self.remove_edge(a, b);
        self.remove_edge(b, a);
    }

    /// Remove *all* edges involving `node_id` (both inbound and outbound)
    /// and remove the node itself from the graph.
    pub fn remove_node(&mut self, node_id: NodeId) {
        // Remove outbound edges.
        self.edges.remove(&node_id);
        // Remove inbound edges from all other nodes.
        for neighbors in self.edges.values_mut() {
            neighbors.remove(&node_id);
        }
        debug!(node_id = %node_id, "Removed node and all its edges");
    }

    /// Returns `true` if there is a direct edge from `from` to `to`.
    pub fn can_reach(&self, from: NodeId, to: NodeId) -> bool {
        self.edges
            .get(&from)
            .is_some_and(|neighbors| neighbors.contains(&to))
    }

    /// Return a sorted list of nodes reachable in one hop from `node_id`.
    pub fn neighbors(&self, node_id: NodeId) -> Vec<NodeId> {
        let mut result: Vec<NodeId> = self
            .edges
            .get(&node_id)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        result.sort();
        result
    }

    /// Check whether the graph is *strongly* connected — every node can reach
    /// every other node via directed edges.
    ///
    /// An empty graph or a graph with a single node is considered fully
    /// connected.
    pub fn is_fully_connected(&self) -> bool {
        let all_nodes = self.all_nodes();
        let n = all_nodes.len();
        if n <= 1 {
            return true;
        }
        // BFS from the first node; must visit every node.
        let start = all_nodes[0];
        let visited = self.bfs(start);
        if visited.len() != n {
            return false;
        }
        // For a *directed* graph we need to check from every node, but the
        // common use-case is bidirectional edges. We do a full check for
        // correctness: reverse-BFS (BFS on the transposed graph).
        let visited_rev = self.bfs_reversed(start);
        visited_rev.len() == n
    }

    /// Number of unique nodes that have at least one edge (inbound or outbound).
    pub fn node_count(&self) -> usize {
        self.all_nodes().len()
    }

    // ── internal helpers ───────────────────────────────────────────────

    /// Collect all unique node IDs in sorted order.
    fn all_nodes(&self) -> Vec<NodeId> {
        let mut set = HashSet::new();
        for (from, tos) in &self.edges {
            set.insert(*from);
            for to in tos {
                set.insert(*to);
            }
        }
        let mut v: Vec<NodeId> = set.into_iter().collect();
        v.sort();
        v
    }

    /// BFS on the forward graph from `start`.
    fn bfs(&self, start: NodeId) -> HashSet<NodeId> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        visited.insert(start);
        queue.push_back(start);
        while let Some(current) = queue.pop_front() {
            if let Some(neighbors) = self.edges.get(&current) {
                for &neighbor in neighbors {
                    if visited.insert(neighbor) {
                        queue.push_back(neighbor);
                    }
                }
            }
        }
        visited
    }

    /// BFS on the *transposed* (reversed) graph from `start`.
    fn bfs_reversed(&self, start: NodeId) -> HashSet<NodeId> {
        // Build a temporary reversed adjacency list.
        let mut reversed: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();
        for (&from, tos) in &self.edges {
            for &to in tos {
                reversed.entry(to).or_default().insert(from);
            }
        }

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        visited.insert(start);
        queue.push_back(start);
        while let Some(current) = queue.pop_front() {
            if let Some(neighbors) = reversed.get(&current) {
                for &neighbor in neighbors {
                    if visited.insert(neighbor) {
                        queue.push_back(neighbor);
                    }
                }
            }
        }
        visited
    }
}

impl Default for Topology {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_topology() {
        let topo = Topology::new();
        assert_eq!(topo.node_count(), 0);
        assert!(topo.is_fully_connected());
    }

    #[test]
    fn add_and_query_edges() {
        let mut topo = Topology::new();
        topo.add_edge(NodeId(1), NodeId(2));

        assert!(topo.can_reach(NodeId(1), NodeId(2)));
        assert!(!topo.can_reach(NodeId(2), NodeId(1)));
        assert_eq!(topo.node_count(), 2);
    }

    #[test]
    fn bidirectional_edges() {
        let mut topo = Topology::new();
        topo.add_bidirectional(NodeId(1), NodeId(2));

        assert!(topo.can_reach(NodeId(1), NodeId(2)));
        assert!(topo.can_reach(NodeId(2), NodeId(1)));
    }

    #[test]
    fn remove_edge() {
        let mut topo = Topology::new();
        topo.add_bidirectional(NodeId(1), NodeId(2));

        topo.remove_edge(NodeId(1), NodeId(2));
        assert!(!topo.can_reach(NodeId(1), NodeId(2)));
        assert!(topo.can_reach(NodeId(2), NodeId(1)));

        topo.remove_bidirectional(NodeId(1), NodeId(2));
        assert!(!topo.can_reach(NodeId(2), NodeId(1)));
    }

    #[test]
    fn remove_node_clears_all_edges() {
        let mut topo = Topology::new();
        topo.add_bidirectional(NodeId(1), NodeId(2));
        topo.add_bidirectional(NodeId(1), NodeId(3));
        topo.add_bidirectional(NodeId(2), NodeId(3));

        topo.remove_node(NodeId(1));

        assert!(!topo.can_reach(NodeId(1), NodeId(2)));
        assert!(!topo.can_reach(NodeId(2), NodeId(1)));
        assert!(!topo.can_reach(NodeId(1), NodeId(3)));
        assert!(!topo.can_reach(NodeId(3), NodeId(1)));
        // 2 ↔ 3 is still intact
        assert!(topo.can_reach(NodeId(2), NodeId(3)));
        assert!(topo.can_reach(NodeId(3), NodeId(2)));
    }

    #[test]
    fn neighbors_sorted() {
        let mut topo = Topology::new();
        topo.add_edge(NodeId(1), NodeId(5));
        topo.add_edge(NodeId(1), NodeId(3));
        topo.add_edge(NodeId(1), NodeId(2));

        assert_eq!(
            topo.neighbors(NodeId(1)),
            vec![NodeId(2), NodeId(3), NodeId(5)]
        );
        assert!(topo.neighbors(NodeId(99)).is_empty());
    }

    #[test]
    fn fully_connected_bidirectional() {
        let mut topo = Topology::new();
        topo.add_bidirectional(NodeId(1), NodeId(2));
        topo.add_bidirectional(NodeId(2), NodeId(3));
        topo.add_bidirectional(NodeId(1), NodeId(3));

        assert!(topo.is_fully_connected());
    }

    #[test]
    fn not_fully_connected_directed() {
        let mut topo = Topology::new();
        // 1 → 2 → 3  (no back-edges — not strongly connected)
        topo.add_edge(NodeId(1), NodeId(2));
        topo.add_edge(NodeId(2), NodeId(3));

        assert!(!topo.is_fully_connected());
    }

    #[test]
    fn single_node_is_connected() {
        let mut topo = Topology::new();
        // A self-loop creates a single-node graph that is connected.
        topo.add_edge(NodeId(1), NodeId(1));
        assert!(topo.is_fully_connected());
    }

    #[test]
    fn fully_connected_directed_cycle() {
        let mut topo = Topology::new();
        topo.add_edge(NodeId(1), NodeId(2));
        topo.add_edge(NodeId(2), NodeId(3));
        topo.add_edge(NodeId(3), NodeId(1));

        assert!(topo.is_fully_connected());
    }
}
