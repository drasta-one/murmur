//! Error types for the DOR runtime.

use thiserror::Error;

use crate::types::{ChunkId, NodeId, TaskId};

/// Errors that can occur in the DOR runtime.
#[derive(Debug, Error)]
pub enum MurmurError {
    #[error("Node {0} not found in cluster")]
    NodeNotFound(NodeId),

    #[error("Node {0} is not available (status: dead or disconnected)")]
    NodeUnavailable(NodeId),

    #[error("Chunk {0} not found in manifest")]
    ChunkNotFound(ChunkId),

    #[error("Chunk {0} integrity verification failed")]
    ChunkVerificationFailed(ChunkId),

    #[error("Task {0} not found")]
    TaskNotFound(TaskId),

    #[error("No coordinator elected")]
    NoCoordinator,

    #[error("Cluster is empty — no active nodes")]
    EmptyCluster,

    #[error("Maximum cluster size ({0}) exceeded")]
    ClusterFull(usize),

    #[error("Election failed: {0}")]
    ElectionFailed(String),

    #[error("Scheduler error: {0}")]
    SchedulerError(String),

    #[error("Transfer failed: {0}")]
    TransferFailed(String),

    #[error("Simulation error: {0}")]
    SimulationError(String),
}
