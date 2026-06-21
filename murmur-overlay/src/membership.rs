//! Membership lifecycle management for a DOR cluster.
//!
//! The [`MembershipManager`] handles join, leave, and timeout events, producing
//! [`MembershipChange`] values that downstream components can react to.

use std::collections::HashSet;

use murmur_core::{NodeId, SimTime};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Describes a change resulting from a membership operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipChange {
    /// A new node successfully joined.
    Joined {
        /// The joining node.
        node_id: NodeId,
        /// Simulation time of the join.
        at: SimTime,
    },
    /// A node gracefully left the cluster.
    Left {
        /// The departing node.
        node_id: NodeId,
        /// Simulation time of departure.
        at: SimTime,
    },
    /// A node was evicted because it missed heartbeats.
    TimedOut {
        /// The timed-out node.
        node_id: NodeId,
        /// Simulation time of the timeout.
        at: SimTime,
    },
    /// A join was attempted but the node was already a member.
    AlreadyMember {
        /// The node that was already present.
        node_id: NodeId,
    },
    /// A leave or timeout was attempted but the node was not a member.
    NotMember {
        /// The node that was not found.
        node_id: NodeId,
    },
}

/// Manages the set of nodes currently considered members of the cluster.
///
/// This is a lightweight bookkeeping structure — it does **not** own
/// [`Node`](murmur_core::Node) instances. The actual node data lives in the
/// [`OverlayStateTable`](crate::state_table::OverlayStateTable).
#[derive(Debug)]
pub struct MembershipManager {
    members: HashSet<NodeId>,
}

impl MembershipManager {
    /// Create an empty membership manager.
    pub fn new() -> Self {
        Self {
            members: HashSet::new(),
        }
    }

    /// Handle a node join request.
    ///
    /// Returns [`MembershipChange::Joined`] on success, or
    /// [`MembershipChange::AlreadyMember`] if the node is already tracked.
    pub fn handle_join(&mut self, node_id: NodeId, at: SimTime) -> MembershipChange {
        if !self.members.insert(node_id) {
            warn!(node_id = %node_id, "Join rejected — already a member");
            return MembershipChange::AlreadyMember { node_id };
        }
        info!(node_id = %node_id, at = %at, "Node joined");
        MembershipChange::Joined { node_id, at }
    }

    /// Handle a graceful leave.
    ///
    /// Returns [`MembershipChange::Left`] on success, or
    /// [`MembershipChange::NotMember`] if the node was not tracked.
    pub fn handle_leave(&mut self, node_id: NodeId, at: SimTime) -> MembershipChange {
        if !self.members.remove(&node_id) {
            warn!(node_id = %node_id, "Leave rejected — not a member");
            return MembershipChange::NotMember { node_id };
        }
        info!(node_id = %node_id, at = %at, "Node left");
        MembershipChange::Left { node_id, at }
    }

    /// Handle a heartbeat timeout (forced eviction).
    ///
    /// Returns [`MembershipChange::TimedOut`] on success, or
    /// [`MembershipChange::NotMember`] if the node was not tracked.
    pub fn handle_timeout(&mut self, node_id: NodeId, at: SimTime) -> MembershipChange {
        if !self.members.remove(&node_id) {
            warn!(node_id = %node_id, "Timeout rejected — not a member");
            return MembershipChange::NotMember { node_id };
        }
        warn!(node_id = %node_id, at = %at, "Node timed out");
        MembershipChange::TimedOut { node_id, at }
    }

    /// Returns `true` if the given node is currently a member.
    pub fn is_member(&self, node_id: NodeId) -> bool {
        self.members.contains(&node_id)
    }

    /// Return a sorted list of all current member IDs.
    pub fn members(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.members.iter().copied().collect();
        ids.sort();
        ids
    }

    /// Number of current members.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }
}

impl Default for MembershipManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_and_query() {
        let mut mgr = MembershipManager::new();
        let change = mgr.handle_join(NodeId(1), SimTime(100));

        assert_eq!(
            change,
            MembershipChange::Joined {
                node_id: NodeId(1),
                at: SimTime(100),
            }
        );
        assert!(mgr.is_member(NodeId(1)));
        assert_eq!(mgr.member_count(), 1);
    }

    #[test]
    fn duplicate_join_returns_already_member() {
        let mut mgr = MembershipManager::new();
        mgr.handle_join(NodeId(1), SimTime(0));
        let change = mgr.handle_join(NodeId(1), SimTime(100));

        assert_eq!(
            change,
            MembershipChange::AlreadyMember { node_id: NodeId(1) }
        );
        assert_eq!(mgr.member_count(), 1);
    }

    #[test]
    fn leave_removes_member() {
        let mut mgr = MembershipManager::new();
        mgr.handle_join(NodeId(1), SimTime(0));
        let change = mgr.handle_leave(NodeId(1), SimTime(500));

        assert_eq!(
            change,
            MembershipChange::Left {
                node_id: NodeId(1),
                at: SimTime(500),
            }
        );
        assert!(!mgr.is_member(NodeId(1)));
        assert_eq!(mgr.member_count(), 0);
    }

    #[test]
    fn leave_nonmember_returns_not_member() {
        let mut mgr = MembershipManager::new();
        let change = mgr.handle_leave(NodeId(42), SimTime(0));
        assert_eq!(
            change,
            MembershipChange::NotMember {
                node_id: NodeId(42),
            }
        );
    }

    #[test]
    fn timeout_removes_member() {
        let mut mgr = MembershipManager::new();
        mgr.handle_join(NodeId(1), SimTime(0));
        let change = mgr.handle_timeout(NodeId(1), SimTime(3000));

        assert_eq!(
            change,
            MembershipChange::TimedOut {
                node_id: NodeId(1),
                at: SimTime(3000),
            }
        );
        assert!(!mgr.is_member(NodeId(1)));
    }

    #[test]
    fn timeout_nonmember_returns_not_member() {
        let mut mgr = MembershipManager::new();
        let change = mgr.handle_timeout(NodeId(99), SimTime(0));
        assert_eq!(
            change,
            MembershipChange::NotMember {
                node_id: NodeId(99),
            }
        );
    }

    #[test]
    fn members_sorted() {
        let mut mgr = MembershipManager::new();
        mgr.handle_join(NodeId(5), SimTime(0));
        mgr.handle_join(NodeId(1), SimTime(0));
        mgr.handle_join(NodeId(3), SimTime(0));

        assert_eq!(mgr.members(), vec![NodeId(1), NodeId(3), NodeId(5)]);
    }

    #[test]
    fn full_lifecycle() {
        let mut mgr = MembershipManager::new();

        // Join three nodes
        mgr.handle_join(NodeId(1), SimTime(0));
        mgr.handle_join(NodeId(2), SimTime(0));
        mgr.handle_join(NodeId(3), SimTime(0));
        assert_eq!(mgr.member_count(), 3);

        // Node 2 leaves gracefully
        mgr.handle_leave(NodeId(2), SimTime(1000));
        assert_eq!(mgr.member_count(), 2);
        assert!(!mgr.is_member(NodeId(2)));

        // Node 3 times out
        mgr.handle_timeout(NodeId(3), SimTime(5000));
        assert_eq!(mgr.member_count(), 1);
        assert_eq!(mgr.members(), vec![NodeId(1)]);
    }
}
