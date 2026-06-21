//! Cluster configuration and membership.

use serde::{Deserialize, Serialize};

/// Configuration for a DOR cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Default chunk size in bytes.
    pub chunk_size: u32,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Number of missed heartbeats before a node is considered dead.
    pub heartbeat_timeout_multiplier: u32,
    /// Maximum number of retries for a failed task.
    pub max_task_retries: u32,
    /// Maximum number of nodes in the cluster.
    pub max_nodes: usize,
    /// Timeout for collecting OST fragments after election (ms).
    pub ost_recovery_timeout_ms: u64,
    /// Timeout before an InProgress chunk is reassigned (ms).
    pub inprogress_reassignment_timeout_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            chunk_size: 1_048_576, // 1 MB
            heartbeat_interval_ms: 1000,
            heartbeat_timeout_multiplier: 3,
            max_task_retries: 3,
            max_nodes: 50,
            ost_recovery_timeout_ms: 3000,
            inprogress_reassignment_timeout_ms: 10_000,
        }
    }
}
