//! Chunk-to-node assignment interface.
//!
//! The [`ChunkScheduler`] orchestrates the full scheduling pipeline:
//! strategy selection, assignment generation, and task creation.

use murmur_core::task::{Task, TaskKind};
use murmur_core::types::{ChunkId, NodeId, SimTime, TaskId};

use crate::strategy::SchedulingStrategy;

/// Orchestrates chunk scheduling using a pluggable strategy.
#[derive(Debug)]
pub struct ChunkScheduler {
    /// Next task ID to assign.
    next_task_id: u64,
    /// Maximum retries per task.
    max_retries: u32,
}

impl ChunkScheduler {
    /// Create a new chunk scheduler.
    pub fn new(max_retries: u32) -> Self {
        Self {
            next_task_id: 0,
            max_retries,
        }
    }

    /// Schedule chunk downloads using the given strategy.
    ///
    /// Returns a list of `Task` objects ready to be enqueued.
    pub fn schedule_downloads(
        &mut self,
        strategy: &dyn SchedulingStrategy,
        chunks: &[ChunkId],
        nodes: &[(NodeId, u64)],
        at: SimTime,
    ) -> Vec<Task> {
        let assignments = strategy.assign(chunks, nodes);

        assignments
            .into_iter()
            .map(|assignment| {
                let task = Task::new(
                    TaskId(self.next_task_id),
                    TaskKind::DownloadChunk {
                        chunk_id: assignment.chunk_id,
                    },
                    assignment.node_id,
                    at,
                    self.max_retries,
                );
                self.next_task_id += 1;
                task
            })
            .collect()
    }

    /// Create a redistribution task: send a chunk from source to target via LAN.
    pub fn create_redistribution_task(
        &mut self,
        chunk_id: ChunkId,
        source: NodeId,
        target: NodeId,
        at: SimTime,
    ) -> (Task, Task) {
        let send_task = Task::new(
            TaskId(self.next_task_id),
            TaskKind::SendChunkToPeer {
                chunk_id,
                target,
            },
            source,
            at,
            self.max_retries,
        );
        self.next_task_id += 1;

        let recv_task = Task::new(
            TaskId(self.next_task_id),
            TaskKind::ReceiveChunkFromPeer {
                chunk_id,
                source,
            },
            target,
            at,
            self.max_retries,
        );
        self.next_task_id += 1;

        (send_task, recv_task)
    }

    /// Create a verification task for a chunk on a specific node.
    pub fn create_verify_task(
        &mut self,
        chunk_id: ChunkId,
        node_id: NodeId,
        at: SimTime,
    ) -> Task {
        let task = Task::new(
            TaskId(self.next_task_id),
            TaskKind::VerifyChunk { chunk_id },
            node_id,
            at,
            0, // verification doesn't retry
        );
        self.next_task_id += 1;
        task
    }

    /// Total tasks created so far.
    pub fn total_tasks_created(&self) -> u64 {
        self.next_task_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::RoundRobinStrategy;

    fn node(id: u64, wan_bw: u64) -> (NodeId, u64) {
        (NodeId(id), wan_bw)
    }

    fn chunks(n: u32) -> Vec<ChunkId> {
        (0..n).map(ChunkId).collect()
    }

    #[test]
    fn schedule_downloads_creates_tasks() {
        let mut scheduler = ChunkScheduler::new(3);
        let strategy = RoundRobinStrategy;
        let nodes = vec![node(1, 5_000_000), node(2, 5_000_000)];
        let chunk_ids = chunks(4);

        let tasks = scheduler.schedule_downloads(&strategy, &chunk_ids, &nodes, SimTime::ZERO);

        assert_eq!(tasks.len(), 4);
        assert_eq!(tasks[0].assigned_to, NodeId(1));
        assert_eq!(tasks[1].assigned_to, NodeId(2));
        assert_eq!(tasks[2].assigned_to, NodeId(1));
        assert_eq!(tasks[3].assigned_to, NodeId(2));
        assert_eq!(scheduler.total_tasks_created(), 4);
    }

    #[test]
    fn redistribution_creates_send_recv_pair() {
        let mut scheduler = ChunkScheduler::new(2);

        let (send, recv) = scheduler.create_redistribution_task(
            ChunkId(5), NodeId(1), NodeId(2), SimTime(1000),
        );

        assert!(matches!(send.kind, TaskKind::SendChunkToPeer { chunk_id: ChunkId(5), target: NodeId(2) }));
        assert_eq!(send.assigned_to, NodeId(1));

        assert!(matches!(recv.kind, TaskKind::ReceiveChunkFromPeer { chunk_id: ChunkId(5), source: NodeId(1) }));
        assert_eq!(recv.assigned_to, NodeId(2));
    }

    #[test]
    fn verify_task() {
        let mut scheduler = ChunkScheduler::new(3);
        let task = scheduler.create_verify_task(ChunkId(0), NodeId(1), SimTime(500));

        assert!(matches!(task.kind, TaskKind::VerifyChunk { chunk_id: ChunkId(0) }));
        assert_eq!(task.max_retries, 0); // no retries for verify
    }

    #[test]
    fn task_ids_are_sequential() {
        let mut scheduler = ChunkScheduler::new(3);
        let strategy = RoundRobinStrategy;
        let nodes = vec![node(1, 5_000_000)];

        let tasks = scheduler.schedule_downloads(&strategy, &chunks(3), &nodes, SimTime::ZERO);
        assert_eq!(tasks[0].id, TaskId(0));
        assert_eq!(tasks[1].id, TaskId(1));
        assert_eq!(tasks[2].id, TaskId(2));

        let verify = scheduler.create_verify_task(ChunkId(0), NodeId(1), SimTime(100));
        assert_eq!(verify.id, TaskId(3));
    }
}
