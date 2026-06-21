//! Coordinator lifecycle state machine.
//!
//! The coordinator goes through a defined set of states from bootstrap
//! through election to active coordination and potential death/replacement.

use murmur_core::types::{NodeId, SimTime};
use serde::{Deserialize, Serialize};

/// The lifecycle state of the coordination subsystem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoordinatorState {
    /// No coordinator exists yet — system is bootstrapping.
    Bootstrap,
    /// An election is in progress.
    Electing { started_at: SimTime },
    /// Coordinator has been elected and is recovering state from fragments.
    Recovering {
        coordinator: NodeId,
        term: u64,
        since: SimTime,
        received_fragments: Vec<murmur_core::chunk::OstFragment>,
    },
    /// A coordinator is active and managing the cluster.
    Active {
        coordinator: NodeId,
        term: u64,
        since: SimTime,
    },
    /// The coordinator has died; waiting for re-election.
    Dead {
        previous: NodeId,
        died_at: SimTime,
    },
}

/// Manages the coordinator lifecycle state machine.
///
/// Transitions:
/// ```text
///  Bootstrap → Electing → Active → Dead → Electing → Active → ...
/// ```
#[derive(Debug, Clone)]
pub struct CoordinatorLifecycle {
    /// Current state.
    state: CoordinatorState,
    /// History of coordinators for observability (term, node_id, duration).
    history: Vec<CoordinatorRecord>,
}

/// A record of a past coordinator's tenure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorRecord {
    /// The coordinator's node ID.
    pub node_id: NodeId,
    /// The term number.
    pub term: u64,
    /// When this coordinator was elected.
    pub elected_at: SimTime,
    /// How long they served (in ms), or None if still active.
    pub duration_ms: Option<u64>,
}

impl CoordinatorLifecycle {
    /// Create a new lifecycle in the Bootstrap state.
    pub fn new() -> Self {
        Self {
            state: CoordinatorState::Bootstrap,
            history: Vec::new(),
        }
    }

    /// Current state.
    pub fn state(&self) -> &CoordinatorState {
        &self.state
    }

    /// Is there an active coordinator?
    pub fn is_active(&self) -> bool {
        matches!(self.state, CoordinatorState::Active { .. })
    }

    /// Is the coordinator currently recovering?
    pub fn is_recovering(&self) -> bool {
        matches!(self.state, CoordinatorState::Recovering { .. })
    }

    /// Get the current active coordinator, if any.
    pub fn active_coordinator(&self) -> Option<NodeId> {
        match &self.state {
            CoordinatorState::Active { coordinator, .. } => Some(*coordinator),
            CoordinatorState::Recovering { coordinator, .. } => Some(*coordinator),
            _ => None,
        }
    }

    /// Get the current term.
    pub fn current_term(&self) -> u64 {
        match &self.state {
            CoordinatorState::Active { term, .. } => *term,
            CoordinatorState::Recovering { term, .. } => *term,
            _ => self.history.last().map_or(0, |r| r.term),
        }
    }

    /// Transition to the Electing state.
    pub fn start_election(&mut self, at: SimTime) {
        // If a coordinator was active, record their death
        if let CoordinatorState::Active {
            coordinator,
            since,
            ..
        } = &self.state
        {
            if let Some(record) = self.history.last_mut() {
                if record.node_id == *coordinator {
                    record.duration_ms = Some(at.duration_since(record.elected_at));
                }
            }
        }

        self.state = CoordinatorState::Electing { started_at: at };
        tracing::info!(at = %at, "Election started");
    }

    /// Transition to Recovering state after successful election.
    pub fn declare_coordinator(&mut self, node_id: NodeId, term: u64, at: SimTime) {
        self.state = CoordinatorState::Recovering {
            coordinator: node_id,
            term,
            since: at,
            received_fragments: Vec::new(),
        };

        self.history.push(CoordinatorRecord {
            node_id,
            term,
            elected_at: at,
            duration_ms: None,
        });

        tracing::info!(
            coordinator = %node_id,
            term = term,
            at = %at,
            "Coordinator elected, entering recovery"
        );
    }

    /// Transition from Recovering to Active state.
    pub fn complete_recovery(&mut self, at: SimTime) {
        if let CoordinatorState::Recovering { coordinator, term, since: _, .. } = self.state {
            self.state = CoordinatorState::Active {
                coordinator,
                term,
                since: at, // We mark active time from here, though history tracks election time
            };
            tracing::info!(
                coordinator = %coordinator,
                term = term,
                at = %at,
                "Coordinator finished recovery, now Active"
            );
        }
    }


    /// Transition to Dead state (coordinator crashed/disconnected).
    pub fn coordinator_died(&mut self, at: SimTime) {
        if let CoordinatorState::Active { coordinator, since, .. } 
             | CoordinatorState::Recovering { coordinator, since, .. } = &self.state
        {
            let previous = *coordinator;
            let elected_at = *since;

            // Update the history record with duration
            if let Some(record) = self.history.last_mut() {
                if record.node_id == previous {
                    record.duration_ms = Some(at.duration_since(record.elected_at));
                }
            }

            self.state = CoordinatorState::Dead {
                previous,
                died_at: at,
            };

            tracing::warn!(
                coordinator = %previous,
                at = %at,
                "Coordinator died"
            );
        }
    }

    /// Number of coordinator transitions in history.
    pub fn transition_count(&self) -> usize {
        self.history.len()
    }

    /// Get the full coordinator history.
    pub fn history(&self) -> &[CoordinatorRecord] {
        &self.history
    }
}

impl Default for CoordinatorLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_in_bootstrap() {
        let lc = CoordinatorLifecycle::new();
        assert_eq!(*lc.state(), CoordinatorState::Bootstrap);
        assert!(!lc.is_active());
        assert!(lc.active_coordinator().is_none());
    }

    #[test]
    fn full_lifecycle() {
        let mut lc = CoordinatorLifecycle::new();

        // Bootstrap → Electing
        lc.start_election(SimTime(0));
        assert!(matches!(lc.state(), CoordinatorState::Electing { .. }));

        // Electing → Recovering
        lc.declare_coordinator(NodeId(5), 1, SimTime(100));
        assert!(matches!(lc.state(), CoordinatorState::Recovering { .. }));
        assert!(!lc.is_active());
        assert!(lc.is_recovering());
        
        // Recovering → Active
        lc.complete_recovery(SimTime(200));
        assert!(lc.is_active());
        assert!(!lc.is_recovering());
        assert_eq!(lc.active_coordinator(), Some(NodeId(5)));
        assert_eq!(lc.current_term(), 1);

        // Active → Dead
        lc.coordinator_died(SimTime(5000));
        assert!(matches!(lc.state(), CoordinatorState::Dead { .. }));
        assert!(!lc.is_active());

        // Dead → Electing → Recovering → Active (new coordinator)
        lc.start_election(SimTime(5100));
        lc.declare_coordinator(NodeId(4), 2, SimTime(5200));
        lc.complete_recovery(SimTime(5300));
        assert_eq!(lc.active_coordinator(), Some(NodeId(4)));
        assert_eq!(lc.current_term(), 2);
    }

    #[test]
    fn history_records_transitions() {
        let mut lc = CoordinatorLifecycle::new();

        lc.start_election(SimTime(0));
        lc.declare_coordinator(NodeId(5), 1, SimTime(100));
        lc.complete_recovery(SimTime(200));
        lc.coordinator_died(SimTime(5000));
        lc.start_election(SimTime(5100));
        lc.declare_coordinator(NodeId(4), 2, SimTime(5200));

        assert_eq!(lc.transition_count(), 2);

        let history = lc.history();
        assert_eq!(history[0].node_id, NodeId(5));
        assert_eq!(history[0].term, 1);
        assert_eq!(history[0].duration_ms, Some(4900)); // 5000 - 100

        assert_eq!(history[1].node_id, NodeId(4));
        assert_eq!(history[1].term, 2);
        assert!(history[1].duration_ms.is_none()); // still active
    }

    #[test]
    fn coordinator_died_only_from_active() {
        let mut lc = CoordinatorLifecycle::new();
        lc.coordinator_died(SimTime(100)); // no-op from Bootstrap
        assert_eq!(*lc.state(), CoordinatorState::Bootstrap);
    }
}
