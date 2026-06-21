//! Task primitive — a schedulable unit of cooperative work.

use serde::{Deserialize, Serialize};

use crate::types::{ChunkId, NodeId, SimTime, TaskId};

/// The kind of work a task represents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskKind {
    /// Download a chunk from the internet (WAN).
    DownloadChunk { chunk_id: ChunkId },
    /// Download a specific byte range from a URL (WAN bonding).
    FetchUrlRange {
        chunk_ids: Vec<ChunkId>,
        /// The URL to download from.
        url: String,
        /// Byte offset to start downloading from.
        offset: u64,
        /// Number of bytes to download.
        size: u32,
    },
    /// Receive a chunk from a peer node (LAN).
    ReceiveChunkFromPeer { chunk_id: ChunkId, source: NodeId },
    /// Send a chunk to a peer node (LAN).
    SendChunkToPeer { chunk_id: ChunkId, target: NodeId },
    /// Verify the integrity of a chunk against the manifest.
    VerifyChunk { chunk_id: ChunkId },
}

impl TaskKind {
    /// Extract the primary chunk ID this task operates on, if any.
    pub fn chunk_id(&self) -> ChunkId {
        match self {
            TaskKind::DownloadChunk { chunk_id }
            | TaskKind::ReceiveChunkFromPeer { chunk_id, .. }
            | TaskKind::SendChunkToPeer { chunk_id, .. }
            | TaskKind::VerifyChunk { chunk_id } => *chunk_id,
            TaskKind::FetchUrlRange { chunk_ids, .. } => chunk_ids[0],
        }
    }

    /// Extract all chunk IDs this task operates on.
    pub fn chunk_ids(&self) -> Vec<ChunkId> {
        match self {
            TaskKind::FetchUrlRange { chunk_ids, .. } => chunk_ids.clone(),
            _ => vec![self.chunk_id()],
        }
    }
}

/// The current status of a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task is queued and waiting to be executed.
    Queued,
    /// Task is currently being executed.
    InProgress { started_at: SimTime },
    /// Task completed successfully.
    Completed { completed_at: SimTime },
    /// Task failed.
    Failed { failed_at: SimTime, reason: String },
    /// Task was cancelled (e.g., due to node failure).
    Cancelled,
}

impl TaskStatus {
    /// Returns true if the task is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed { .. } | TaskStatus::Failed { .. } | TaskStatus::Cancelled
        )
    }
}

/// Traffic priority/QoS classification for tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TrafficClass {
    Interactive,
    #[default]
    Bulk,
    Background,
}

/// A schedulable unit of cooperative work in the DOR runtime.
#[derive(Debug, Clone)]
pub struct Task {
    /// Unique task identifier.
    pub id: TaskId,
    /// What kind of work this task represents.
    pub kind: TaskKind,
    /// Which node this task is assigned to.
    pub assigned_to: NodeId,
    /// Current task status.
    pub status: TaskStatus,
    /// When the task was created.
    pub created_at: SimTime,
    /// How many times this task has been retried.
    pub retry_count: u32,
    /// Maximum number of retries allowed.
    pub max_retries: u32,
    /// The QoS classification of this task.
    pub traffic_class: TrafficClass,
}

impl Task {
    /// Create a new queued task.
    pub fn new(
        id: TaskId,
        kind: TaskKind,
        assigned_to: NodeId,
        created_at: SimTime,
        max_retries: u32,
    ) -> Self {
        Self {
            id,
            kind,
            assigned_to,
            status: TaskStatus::Queued,
            created_at,
            retry_count: 0,
            max_retries,
            traffic_class: TrafficClass::default(),
        }
    }

    /// Start executing the task.
    pub fn start(&mut self, at: SimTime) {
        self.status = TaskStatus::InProgress { started_at: at };
    }

    /// Mark the task as completed.
    pub fn complete(&mut self, at: SimTime) {
        self.status = TaskStatus::Completed { completed_at: at };
    }

    /// Mark the task as failed.
    pub fn fail(&mut self, at: SimTime, reason: String) {
        self.status = TaskStatus::Failed {
            failed_at: at,
            reason,
        };
    }

    /// Cancel the task.
    pub fn cancel(&mut self) {
        self.status = TaskStatus::Cancelled;
    }

    /// Check if the task can be retried.
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// Increment the retry counter and reset to Queued.
    pub fn retry(&mut self) {
        self.retry_count += 1;
        self.status = TaskStatus::Queued;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_lifecycle() {
        let mut task = Task::new(
            TaskId(1),
            TaskKind::DownloadChunk {
                chunk_id: ChunkId(0),
            },
            NodeId(1),
            SimTime::ZERO,
            3,
        );

        assert_eq!(task.status, TaskStatus::Queued);
        assert!(!task.status.is_terminal());

        task.start(SimTime(100));
        assert!(matches!(task.status, TaskStatus::InProgress { .. }));

        task.complete(SimTime(200));
        assert!(task.status.is_terminal());
    }

    #[test]
    fn task_retry() {
        let mut task = Task::new(
            TaskId(1),
            TaskKind::DownloadChunk {
                chunk_id: ChunkId(0),
            },
            NodeId(1),
            SimTime::ZERO,
            2,
        );

        task.fail(SimTime(100), "timeout".into());
        assert!(task.can_retry());
        task.retry();
        assert_eq!(task.retry_count, 1);
        assert_eq!(task.status, TaskStatus::Queued);

        task.fail(SimTime(200), "timeout".into());
        assert!(task.can_retry());
        task.retry();

        task.fail(SimTime(300), "timeout".into());
        assert!(!task.can_retry()); // max_retries = 2, retry_count = 2
    }
}
