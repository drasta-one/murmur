//! Domain events for the DOR runtime.
//!
//! Every significant state transition in the runtime is represented as a `MurmurEvent`.
//! These events provide full observability into cluster, election, task, chunk,
//! transfer, and link behavior.

use serde::Serialize;

use crate::types::{ChunkId, ClusterId, ManifestId, NodeId, SimTime, TaskId};

/// Reason a node left the cluster.
#[derive(Debug, Clone, Serialize)]
pub enum LeaveReason {
    /// Node left gracefully.
    Graceful,
    /// Node timed out (missed heartbeats).
    Timeout,
    /// Node's battery died.
    BatteryDeath,
    /// User-initiated removal.
    Removed,
}

/// All domain events in the DOR runtime.
#[derive(Debug, Clone, Serialize)]
pub enum MurmurEvent {
    // ── Cluster events ──
    NodeJoined {
        node_id: NodeId,
        at: SimTime,
    },
    NodeLeft {
        node_id: NodeId,
        at: SimTime,
        reason: LeaveReason,
    },
    NodeCrashed {
        node_id: NodeId,
        at: SimTime,
    },
    ClusterFormed {
        cluster_id: ClusterId,
        members: Vec<NodeId>,
        at: SimTime,
    },

    // ── Election events ──
    ElectionStarted {
        triggered_by: NodeId,
        at: SimTime,
    },
    CoordinatorElected {
        node_id: NodeId,
        term: u64,
        at: SimTime,
    },
    CoordinatorDied {
        node_id: NodeId,
        at: SimTime,
    },
    RequestOstFragments {
        from_coordinator: NodeId,
        epoch: u64,
        deadline: SimTime,
    },
    OstFragmentReported {
        by: NodeId,
        to: NodeId,
        epoch: u64,
        fragment: crate::chunk::OstFragment,
        sent_at: SimTime,
    },
    RecoveryTimeout {
        coordinator: NodeId,
        epoch: u64,
        at: SimTime,
    },

    // ── Task events ──
    TaskAssigned {
        task_id: TaskId,
        to: NodeId,
        at: SimTime,
    },
    TaskCompleted {
        task_id: TaskId,
        by: NodeId,
        at: SimTime,
    },
    TaskFailed {
        task_id: TaskId,
        by: NodeId,
        reason: String,
        at: SimTime,
    },
    TaskReassigned {
        task_id: TaskId,
        from: NodeId,
        to: NodeId,
        at: SimTime,
    },

    // ── Chunk events ──
    ChunkDownloaded {
        chunk_id: ChunkId,
        by: NodeId,
        at: SimTime,
    },
    ChunkRedistributed {
        chunk_id: ChunkId,
        from: NodeId,
        to: NodeId,
        at: SimTime,
    },
    ChunkVerified {
        chunk_id: ChunkId,
        by: NodeId,
        at: SimTime,
    },
    ChunkCorrupted {
        chunk_id: ChunkId,
        by: NodeId,
        at: SimTime,
    },

    // ── Transfer events ──
    TransferStarted {
        manifest_id: ManifestId,
        at: SimTime,
    },
    TransferCompleted {
        manifest_id: ManifestId,
        at: SimTime,
        total_time_ms: u64,
    },
    TransferFailed {
        manifest_id: ManifestId,
        at: SimTime,
        reason: String,
    },

    // ── Link events ──
    LinkDegraded {
        from: NodeId,
        to: NodeId,
        at: SimTime,
    },
    LinkSevered {
        from: NodeId,
        to: NodeId,
        at: SimTime,
    },
    LinkRestored {
        from: NodeId,
        to: NodeId,
        at: SimTime,
    },

    // ── Bonding events ──
    BondedFetchStarted {
        manifest_id: ManifestId,
        url: String,
        total_size: u64,
        node_count: usize,
        at: SimTime,
    },
    BondedFetchCompleted {
        manifest_id: ManifestId,
        total_size: u64,
        effective_bandwidth_bps: u64,
        at: SimTime,
    },
    WanSpeedMeasured {
        node_id: NodeId,
        bytes_per_sec: u64,
        at: SimTime,
    },
    WanRateLimitDetected {
        node_id: NodeId,
        url: String,
        observed_bps: u64,
        expected_bps: u64,
        at: SimTime,
    },
}

impl MurmurEvent {
    /// Get the simulation time this event occurred at.
    pub fn time(&self) -> SimTime {
        match self {
            MurmurEvent::NodeJoined { at, .. }
            | MurmurEvent::NodeLeft { at, .. }
            | MurmurEvent::NodeCrashed { at, .. }
            | MurmurEvent::ClusterFormed { at, .. }
            | MurmurEvent::ElectionStarted { at, .. }
            | MurmurEvent::CoordinatorElected { at, .. }
            | MurmurEvent::CoordinatorDied { at, .. }
            | MurmurEvent::RequestOstFragments { deadline: at, .. }
            | MurmurEvent::OstFragmentReported { sent_at: at, .. }
            | MurmurEvent::RecoveryTimeout { at, .. }
            | MurmurEvent::TaskAssigned { at, .. }
            | MurmurEvent::TaskCompleted { at, .. }
            | MurmurEvent::TaskFailed { at, .. }
            | MurmurEvent::TaskReassigned { at, .. }
            | MurmurEvent::ChunkDownloaded { at, .. }
            | MurmurEvent::ChunkRedistributed { at, .. }
            | MurmurEvent::ChunkVerified { at, .. }
            | MurmurEvent::ChunkCorrupted { at, .. }
            | MurmurEvent::TransferStarted { at, .. }
            | MurmurEvent::TransferCompleted { at, .. }
            | MurmurEvent::TransferFailed { at, .. }
            | MurmurEvent::LinkDegraded { at, .. }
            | MurmurEvent::LinkSevered { at, .. }
            | MurmurEvent::LinkRestored { at, .. }
            | MurmurEvent::BondedFetchStarted { at, .. }
            | MurmurEvent::BondedFetchCompleted { at, .. }
            | MurmurEvent::WanSpeedMeasured { at, .. }
            | MurmurEvent::WanRateLimitDetected { at, .. } => *at,
        }
    }
}
