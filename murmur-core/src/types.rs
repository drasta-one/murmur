//! Shared type identifiers used across all DOR crates.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Unique identifier for a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({})", self.0)
    }
}

/// Unique identifier for a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub Uuid);

impl ClusterId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ClusterId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cluster({})", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a chunk within a manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkId(pub u32);

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Chunk({})", self.0)
    }
}

/// Unique identifier for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TaskId(pub u64);

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task({})", self.0)
    }
}

/// Unique identifier for a transfer manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ManifestId(pub Uuid);

impl ManifestId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ManifestId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Manifest({})", &self.0.to_string()[..8])
    }
}

/// Simulated time in milliseconds from simulation start.
///
/// This is NOT wall-clock time. It is a virtual clock controlled by the simulation engine.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct SimTime(pub u64);

impl SimTime {
    /// Zero time — the start of the simulation.
    pub const ZERO: SimTime = SimTime(0);

    /// Add milliseconds to this time.
    pub fn add_ms(self, ms: u64) -> Self {
        SimTime(self.0.saturating_add(ms))
    }

    /// Duration in ms between two times. Returns 0 if other is in the past.
    pub fn duration_since(self, earlier: SimTime) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl fmt::Display for SimTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 >= 60_000 {
            write!(f, "{}m{}s", self.0 / 60_000, (self.0 % 60_000) / 1000)
        } else if self.0 >= 1_000 {
            write!(f, "{}.{:03}s", self.0 / 1000, self.0 % 1000)
        } else {
            write!(f, "{}ms", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_time_arithmetic() {
        let t1 = SimTime(1000);
        let t2 = t1.add_ms(500);
        assert_eq!(t2, SimTime(1500));
        assert_eq!(t2.duration_since(t1), 500);
        assert_eq!(t1.duration_since(t2), 0); // saturates at 0
    }

    #[test]
    fn sim_time_display() {
        assert_eq!(format!("{}", SimTime(500)), "500ms");
        assert_eq!(format!("{}", SimTime(1500)), "1.500s");
        assert_eq!(format!("{}", SimTime(65000)), "1m5s");
    }

    #[test]
    fn node_id_ordering() {
        let a = NodeId(1);
        let b = NodeId(5);
        let c = NodeId(3);
        let mut ids = vec![b, c, a];
        ids.sort();
        assert_eq!(ids, vec![NodeId(1), NodeId(3), NodeId(5)]);
    }
}
