//! State recovery after coordinator re-election.
//!
//! When a new coordinator is elected, it must recover the transfer state
//! from the overlay state table:
//! - Which chunks have been assigned but not completed
//! - Which tasks were in progress on the old coordinator's watch
//! - Which nodes were still active at the time of the crash
//!
//! This module provides the [`RecoveryPlan`] and [`StateRecovery`] types.

use murmur_core::types::{ChunkId, NodeId, SimTime, TaskId};

/// A plan for recovering from a coordinator crash.
///
/// Built by inspecting the overlay state table after re-election.
#[derive(Debug, Clone, Default)]
pub struct RecoveryPlan {
    /// Tasks that were in progress and need re-evaluation.
    pub orphaned_tasks: Vec<TaskId>,
    /// Chunks that were assigned but whose assigned node crashed.
    pub orphaned_chunks: Vec<ChunkId>,
    /// Nodes that are confirmed alive after the crash.
    pub surviving_nodes: Vec<NodeId>,
    /// Nodes that are confirmed dead/unreachable.
    pub dead_nodes: Vec<NodeId>,
    /// When this recovery plan was created.
    pub created_at: SimTime,
}

impl RecoveryPlan {
    /// Create a new empty recovery plan.
    pub fn new(at: SimTime) -> Self {
        Self {
            created_at: at,
            ..Default::default()
        }
    }

    /// Add an orphaned task that needs reassignment.
    pub fn add_orphaned_task(&mut self, task_id: TaskId) {
        self.orphaned_tasks.push(task_id);
    }

    /// Add a chunk that lost its assigned node.
    pub fn add_orphaned_chunk(&mut self, chunk_id: ChunkId) {
        self.orphaned_chunks.push(chunk_id);
    }

    /// Record a surviving node.
    pub fn add_survivor(&mut self, node_id: NodeId) {
        self.surviving_nodes.push(node_id);
    }

    /// Record a dead node.
    pub fn add_dead_node(&mut self, node_id: NodeId) {
        self.dead_nodes.push(node_id);
    }

    /// Does this plan have any recovery work to do?
    pub fn has_work(&self) -> bool {
        !self.orphaned_tasks.is_empty() || !self.orphaned_chunks.is_empty()
    }

    /// Summary for logging.
    pub fn summary(&self) -> String {
        format!(
            "RecoveryPlan: {} orphaned tasks, {} orphaned chunks, {} survivors, {} dead",
            self.orphaned_tasks.len(),
            self.orphaned_chunks.len(),
            self.surviving_nodes.len(),
            self.dead_nodes.len(),
        )
    }
}

/// State recovery engine.
///
/// Given the set of active nodes and the old coordinator, builds a
/// [`RecoveryPlan`] describing what needs to happen.
#[derive(Debug)]
pub struct StateRecovery;

impl StateRecovery {
    /// Build a recovery plan given the dead coordinator and surviving members.
    ///
    /// The caller should inspect the overlay state table to determine which
    /// tasks/chunks were assigned to the dead coordinator or to other dead nodes.
    pub fn build_plan(
        dead_coordinator: NodeId,
        all_members: &[NodeId],
        dead_nodes: &[NodeId],
        at: SimTime,
    ) -> RecoveryPlan {
        let mut plan = RecoveryPlan::new(at);

        for &node_id in all_members {
            if dead_nodes.contains(&node_id) || node_id == dead_coordinator {
                plan.add_dead_node(node_id);
            } else {
                plan.add_survivor(node_id);
            }
        }

        tracing::info!(
            dead_coordinator = %dead_coordinator,
            survivors = plan.surviving_nodes.len(),
            dead = plan.dead_nodes.len(),
            at = %at,
            "Recovery plan built"
        );

        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_plan_has_no_work() {
        let plan = RecoveryPlan::new(SimTime::ZERO);
        assert!(!plan.has_work());
    }

    #[test]
    fn plan_with_orphans_has_work() {
        let mut plan = RecoveryPlan::new(SimTime(5000));
        plan.add_orphaned_task(TaskId(1));
        plan.add_orphaned_chunk(ChunkId(0));
        assert!(plan.has_work());
    }

    #[test]
    fn build_plan_partitions_nodes() {
        let all = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)];
        let dead = vec![NodeId(3)];

        let plan = StateRecovery::build_plan(NodeId(5), &all, &dead, SimTime(10000));

        // Dead: coordinator (5) + node 3
        assert_eq!(plan.dead_nodes.len(), 2);
        assert!(plan.dead_nodes.contains(&NodeId(5)));
        assert!(plan.dead_nodes.contains(&NodeId(3)));

        // Survivors: 1, 2, 4
        assert_eq!(plan.surviving_nodes.len(), 3);
        assert!(plan.surviving_nodes.contains(&NodeId(1)));
        assert!(plan.surviving_nodes.contains(&NodeId(2)));
        assert!(plan.surviving_nodes.contains(&NodeId(4)));
    }

    #[test]
    fn summary_format() {
        let mut plan = RecoveryPlan::new(SimTime::ZERO);
        plan.add_orphaned_task(TaskId(1));
        plan.add_orphaned_task(TaskId(2));
        plan.add_orphaned_chunk(ChunkId(3));
        plan.add_survivor(NodeId(1));
        plan.add_dead_node(NodeId(5));

        let summary = plan.summary();
        assert!(summary.contains("2 orphaned tasks"));
        assert!(summary.contains("1 orphaned chunks"));
        assert!(summary.contains("1 survivors"));
        assert!(summary.contains("1 dead"));
    }
}
