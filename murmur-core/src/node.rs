//! Node primitive — a participating runtime device in the DOR cluster.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::types::{ChunkId, NodeId, SimTime};

/// Configuration for a simulated node's capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Simulated internet bandwidth in bytes/sec (WAN).
    pub wan_bandwidth: u64,
    /// Simulated local network bandwidth in bytes/sec (LAN).
    pub lan_bandwidth: u64,
    /// Simulated latency to internet in ms.
    pub wan_latency_ms: u32,
    /// Simulated latency to local peers in ms.
    pub lan_latency_ms: u32,
    /// Probability of spontaneous failure per simulation tick `[0.0, 1.0)`.
    pub failure_probability: f64,
    /// Storage capacity in bytes.
    pub storage_capacity: u64,
    /// Initial battery level `[0.0, 1.0]`.
    pub initial_battery: f64,
    /// Battery drain rate per simulation tick.
    pub battery_drain_rate: f64,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            wan_bandwidth: 5_000_000,     // 5 MB/s
            lan_bandwidth: 50_000_000,    // 50 MB/s
            wan_latency_ms: 50,
            lan_latency_ms: 2,
            failure_probability: 0.0,
            storage_capacity: 1_073_741_824, // 1 GB
            initial_battery: 1.0,
            battery_drain_rate: 0.0,
        }
    }
}

/// The operational status of a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is available and participating.
    Active,
    /// Node is the elected coordinator.
    Coordinator,
    /// Node has gracefully disconnected.
    Disconnected,
    /// Node has crashed ungracefully.
    Crashed,
    /// Node is in the process of joining the cluster.
    Joining,
    /// Node's battery is critically low.
    LowBattery,
}

impl NodeStatus {
    /// Returns true if the node can accept work.
    pub fn is_available(&self) -> bool {
        matches!(self, NodeStatus::Active | NodeStatus::Coordinator)
    }

    /// Returns true if the node is considered dead/unreachable.
    pub fn is_dead(&self) -> bool {
        matches!(self, NodeStatus::Disconnected | NodeStatus::Crashed)
    }
}

/// A participating runtime device in the DOR cluster.
#[derive(Debug, Clone)]
pub struct Node {
    /// Unique identifier.
    pub id: NodeId,
    /// Node configuration (capabilities).
    pub config: NodeConfig,
    /// Current operational status.
    pub status: NodeStatus,
    /// Simulation time when the node joined the cluster.
    pub joined_at: SimTime,
    /// Set of chunk IDs this node currently holds.
    pub owned_chunks: HashSet<ChunkId>,
    /// Current battery level `[0.0, 1.0]`.
    pub battery: f64,
}

impl Node {
    /// Create a new node with the given ID and configuration.
    pub fn new(id: NodeId, config: NodeConfig, joined_at: SimTime) -> Self {
        let battery = config.initial_battery;
        Self {
            id,
            config,
            status: NodeStatus::Joining,
            joined_at,
            owned_chunks: HashSet::new(),
            battery,
        }
    }

    /// Transition the node to Active status.
    pub fn activate(&mut self) {
        self.status = NodeStatus::Active;
    }

    /// Promote this node to Coordinator.
    pub fn promote_to_coordinator(&mut self) {
        self.status = NodeStatus::Coordinator;
    }

    /// Demote from Coordinator back to Active.
    pub fn demote_to_active(&mut self) {
        if self.status == NodeStatus::Coordinator {
            self.status = NodeStatus::Active;
        }
    }

    /// Mark the node as crashed.
    pub fn crash(&mut self) {
        self.status = NodeStatus::Crashed;
    }

    /// Mark the node as gracefully disconnected.
    pub fn disconnect(&mut self) {
        self.status = NodeStatus::Disconnected;
    }

    /// Add a chunk to this node's ownership set.
    pub fn add_chunk(&mut self, chunk_id: ChunkId) {
        self.owned_chunks.insert(chunk_id);
    }

    /// Check if this node holds a specific chunk.
    pub fn has_chunk(&self, chunk_id: &ChunkId) -> bool {
        self.owned_chunks.contains(chunk_id)
    }

    /// Calculate the time (in ms) to transfer `size_bytes` over WAN.
    pub fn wan_transfer_time_ms(&self, size_bytes: u64) -> u64 {
        if self.config.wan_bandwidth == 0 {
            return u64::MAX;
        }
        let transfer_ms = (size_bytes * 1000) / self.config.wan_bandwidth;
        transfer_ms + self.config.wan_latency_ms as u64
    }

    /// Calculate the time (in ms) to transfer `size_bytes` over LAN.
    pub fn lan_transfer_time_ms(&self, size_bytes: u64) -> u64 {
        if self.config.lan_bandwidth == 0 {
            return u64::MAX;
        }
        let transfer_ms = (size_bytes * 1000) / self.config.lan_bandwidth;
        transfer_ms + self.config.lan_latency_ms as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node(id: u64) -> Node {
        Node::new(NodeId(id), NodeConfig::default(), SimTime::ZERO)
    }

    #[test]
    fn new_node_starts_as_joining() {
        let node = test_node(1);
        assert_eq!(node.status, NodeStatus::Joining);
        assert!(!node.status.is_available());
    }

    #[test]
    fn node_lifecycle() {
        let mut node = test_node(1);
        node.activate();
        assert!(node.status.is_available());

        node.promote_to_coordinator();
        assert_eq!(node.status, NodeStatus::Coordinator);
        assert!(node.status.is_available());

        node.demote_to_active();
        assert_eq!(node.status, NodeStatus::Active);

        node.crash();
        assert!(node.status.is_dead());
    }

    #[test]
    fn chunk_ownership() {
        let mut node = test_node(1);
        let chunk = ChunkId(0);
        assert!(!node.has_chunk(&chunk));
        node.add_chunk(chunk);
        assert!(node.has_chunk(&chunk));
    }

    #[test]
    fn transfer_time_calculation() {
        let node = test_node(1);
        // 5 MB/s WAN, 50ms latency
        // 1MB = 1_048_576 bytes → 1_048_576 * 1000 / 5_000_000 = 209ms + 50ms = 259ms
        let time = node.wan_transfer_time_ms(1_048_576);
        assert_eq!(time, 259);
    }
}
