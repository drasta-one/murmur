//! Priority task queue for the scheduler.
//!
//! Tasks are ordered by priority, then by creation time (FIFO within
//! the same priority level).

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use murmur_core::task::Task;

/// Priority level for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskPriority {
    /// Normal scheduling priority.
    Normal = 0,
    /// Higher priority — reassigned tasks or retries.
    High = 1,
    /// Highest priority — critical recovery tasks.
    Critical = 2,
}

/// A task entry in the priority queue.
#[derive(Debug, Clone)]
pub struct PrioritizedTask {
    /// The task itself.
    pub task: Task,
    /// Priority level.
    pub priority: TaskPriority,
    /// Sequence number for deterministic FIFO ordering.
    pub seq: u64,
}

// Min-time, max-priority ordering for BinaryHeap (which is a max-heap).
impl PartialEq for PrioritizedTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}

impl Eq for PrioritizedTask {}

impl PartialOrd for PrioritizedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritizedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first, then lower seq (FIFO) first
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

/// A priority-ordered task queue.
#[derive(Debug)]
pub struct TaskQueue {
    heap: BinaryHeap<PrioritizedTask>,
    next_seq: u64,
}

impl TaskQueue {
    /// Create an empty task queue.
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_seq: 0,
        }
    }

    /// Enqueue a task with the given priority.
    pub fn push(&mut self, task: Task, priority: TaskPriority) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(PrioritizedTask {
            task,
            priority,
            seq,
        });
    }

    /// Dequeue the highest-priority, oldest task.
    pub fn pop(&mut self) -> Option<PrioritizedTask> {
        self.heap.pop()
    }

    /// Peek at the next task without removing it.
    pub fn peek(&self) -> Option<&PrioritizedTask> {
        self.heap.peek()
    }

    /// Number of tasks in the queue.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Clear all tasks.
    pub fn clear(&mut self) {
        self.heap.clear();
    }
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use murmur_core::task::TaskKind;
    use murmur_core::types::{ChunkId, NodeId, SimTime, TaskId};

    fn make_task(id: u64) -> Task {
        Task::new(
            TaskId(id),
            TaskKind::DownloadChunk {
                chunk_id: ChunkId(id as u32),
            },
            NodeId(1),
            SimTime::ZERO,
            3,
        )
    }

    #[test]
    fn empty_queue() {
        let queue = TaskQueue::new();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn priority_ordering() {
        let mut queue = TaskQueue::new();

        queue.push(make_task(1), TaskPriority::Normal);
        queue.push(make_task(2), TaskPriority::Critical);
        queue.push(make_task(3), TaskPriority::High);

        // Critical first, then High, then Normal
        assert_eq!(queue.pop().unwrap().task.id, TaskId(2));
        assert_eq!(queue.pop().unwrap().task.id, TaskId(3));
        assert_eq!(queue.pop().unwrap().task.id, TaskId(1));
    }

    #[test]
    fn fifo_within_same_priority() {
        let mut queue = TaskQueue::new();

        queue.push(make_task(1), TaskPriority::Normal);
        queue.push(make_task(2), TaskPriority::Normal);
        queue.push(make_task(3), TaskPriority::Normal);

        // FIFO: 1, 2, 3
        assert_eq!(queue.pop().unwrap().task.id, TaskId(1));
        assert_eq!(queue.pop().unwrap().task.id, TaskId(2));
        assert_eq!(queue.pop().unwrap().task.id, TaskId(3));
    }

    #[test]
    fn peek_does_not_consume() {
        let mut queue = TaskQueue::new();
        queue.push(make_task(1), TaskPriority::Normal);

        assert!(queue.peek().is_some());
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn mixed_priorities_and_fifo() {
        let mut queue = TaskQueue::new();

        queue.push(make_task(1), TaskPriority::Normal);
        queue.push(make_task(2), TaskPriority::High);
        queue.push(make_task(3), TaskPriority::Normal);
        queue.push(make_task(4), TaskPriority::High);

        // High first (FIFO: 2, 4), then Normal (FIFO: 1, 3)
        assert_eq!(queue.pop().unwrap().task.id, TaskId(2));
        assert_eq!(queue.pop().unwrap().task.id, TaskId(4));
        assert_eq!(queue.pop().unwrap().task.id, TaskId(1));
        assert_eq!(queue.pop().unwrap().task.id, TaskId(3));
    }
}
