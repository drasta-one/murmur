//! Coordinator election protocol — Deterministic Bully algorithm.
//!
//! In the Bully algorithm, the node with the highest [`NodeId`] wins.
//! Election proceeds in rounds:
//!
//! 1. A node detects the coordinator is missing (heartbeat timeout).
//! 2. It sends `ElectionStart` to all nodes with higher IDs.
//! 3. If any higher node responds with `ElectionAlive`, the initiator yields.
//! 4. If no response within the timeout, the initiator declares victory.
//! 5. The winner broadcasts `CoordinatorVictory` to all nodes.

use murmur_core::types::{NodeId, SimTime};
use serde::{Deserialize, Serialize};

/// The current state of an election process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ElectionState {
    /// No election in progress.
    Idle,
    /// This node has started an election and is waiting for responses.
    WaitingForResponses {
        /// When the election was initiated.
        started_at: SimTime,
        /// Who initiated it.
        initiated_by: NodeId,
    },
    /// A higher node has responded; this node yields.
    Yielded {
        /// The higher node that responded.
        yielded_to: NodeId,
    },
    /// This node has won and declared victory.
    Won {
        /// The term number of this election.
        term: u64,
        /// When victory was declared.
        at: SimTime,
    },
}

/// An election message sent between nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionMessage {
    /// "I'm starting an election." Sent to nodes with higher IDs.
    Start { from: NodeId },
    /// "I have a higher ID, stand down." Response from a higher node.
    Alive { from: NodeId },
    /// "I am the new coordinator." Broadcast to all nodes.
    Victory { from: NodeId, term: u64 },
}

/// The Bully election engine for a single node.
///
/// Each node has its own `BullyElection` instance that tracks its view
/// of the election state and decides how to respond to messages.
#[derive(Debug, Clone)]
pub struct BullyElection {
    /// This node's ID.
    node_id: NodeId,
    /// Current election state.
    state: ElectionState,
    /// Current term (incremented on each new coordinator).
    current_term: u64,
    /// Who we believe the coordinator is.
    known_coordinator: Option<NodeId>,
    /// Timeout for waiting for responses during election (ms).
    election_timeout_ms: u64,
}

impl BullyElection {
    /// Create a new election engine for a node.
    pub fn new(node_id: NodeId, election_timeout_ms: u64) -> Self {
        Self {
            node_id,
            state: ElectionState::Idle,
            current_term: 0,
            known_coordinator: None,
            election_timeout_ms,
        }
    }

    /// This node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Current election state.
    pub fn state(&self) -> &ElectionState {
        &self.state
    }

    /// Current term number.
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    /// Who we believe the coordinator is.
    pub fn known_coordinator(&self) -> Option<NodeId> {
        self.known_coordinator
    }

    /// Election timeout in ms.
    pub fn election_timeout_ms(&self) -> u64 {
        self.election_timeout_ms
    }

    /// Initiate an election.
    ///
    /// Returns the list of nodes to send `ElectionStart` messages to
    /// (all nodes with higher IDs than this node).
    pub fn initiate_election(
        &mut self,
        all_active_nodes: &[NodeId],
        at: SimTime,
    ) -> Vec<ElectionMessage> {
        let higher_nodes: Vec<NodeId> = all_active_nodes
            .iter()
            .filter(|&&id| id > self.node_id)
            .copied()
            .collect();

        if higher_nodes.is_empty() {
            // No higher nodes — we win immediately!
            self.current_term += 1;
            self.state = ElectionState::Won {
                term: self.current_term,
                at,
            };
            self.known_coordinator = Some(self.node_id);

            // Broadcast victory to all
            return all_active_nodes
                .iter()
                .filter(|&&id| id != self.node_id)
                .map(|_| ElectionMessage::Victory {
                    from: self.node_id,
                    term: self.current_term,
                })
                .collect();
        }

        self.state = ElectionState::WaitingForResponses {
            started_at: at,
            initiated_by: self.node_id,
        };

        // Send Election Start to all higher nodes
        higher_nodes
            .iter()
            .map(|_| ElectionMessage::Start {
                from: self.node_id,
            })
            .collect()
    }

    /// Handle receiving an `ElectionStart` from a lower node.
    ///
    /// If we have a higher ID, we respond with `Alive` and
    /// start our own election if not already in progress.
    pub fn handle_election_start(
        &mut self,
        from: NodeId,
        all_active_nodes: &[NodeId],
        at: SimTime,
    ) -> Vec<ElectionMessage> {
        let mut messages = Vec::new();

        if self.node_id > from {
            // Respond: "I'm alive, stand down."
            messages.push(ElectionMessage::Alive {
                from: self.node_id,
            });

            // Start our own election if idle
            if self.state == ElectionState::Idle {
                let mut election_msgs = self.initiate_election(all_active_nodes, at);
                messages.append(&mut election_msgs);
            }
        }

        messages
    }

    /// Handle receiving an `Alive` response from a higher node.
    ///
    /// We yield the election to the higher node.
    pub fn handle_alive(&mut self, from: NodeId) {
        if from > self.node_id {
            self.state = ElectionState::Yielded { yielded_to: from };
        }
    }

    /// Handle receiving a `Victory` declaration.
    ///
    /// Accept the new coordinator if the term is valid.
    pub fn handle_victory(&mut self, from: NodeId, term: u64) -> bool {
        if term >= self.current_term {
            self.current_term = term;
            self.known_coordinator = Some(from);
            self.state = ElectionState::Idle;
            true
        } else {
            // Stale victory message — ignore
            false
        }
    }

    /// Check if the election has timed out (no higher nodes responded).
    ///
    /// If so, this node declares victory.
    pub fn check_timeout(
        &mut self,
        now: SimTime,
        all_active_nodes: &[NodeId],
    ) -> Option<Vec<ElectionMessage>> {
        if let ElectionState::WaitingForResponses { started_at, .. } = &self.state {
            if now.duration_since(*started_at) >= self.election_timeout_ms {
                // No response — we win!
                self.current_term += 1;
                self.state = ElectionState::Won {
                    term: self.current_term,
                    at: now,
                };
                self.known_coordinator = Some(self.node_id);

                let victory_msgs: Vec<ElectionMessage> = all_active_nodes
                    .iter()
                    .filter(|&&id| id != self.node_id)
                    .map(|_| ElectionMessage::Victory {
                        from: self.node_id,
                        term: self.current_term,
                    })
                    .collect();

                return Some(victory_msgs);
            }
        }
        None
    }

    /// Reset election state to idle (e.g., after coordinator detected).
    pub fn reset(&mut self) {
        self.state = ElectionState::Idle;
    }

    /// Is this node currently the coordinator?
    pub fn is_coordinator(&self) -> bool {
        self.known_coordinator == Some(self.node_id)
    }
}

/// Given a list of active node IDs, determine the Bully winner
/// (the highest NodeId). This is a utility for quick coordinator lookup.
pub fn bully_winner(active_nodes: &[NodeId]) -> Option<NodeId> {
    active_nodes.iter().max().copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highest_node_wins_immediately() {
        let all = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut election = BullyElection::new(NodeId(3), 5000);

        let msgs = election.initiate_election(&all, SimTime::ZERO);

        // Node 3 is highest — wins immediately, sends victory to 1 and 2
        assert_eq!(election.state, ElectionState::Won { term: 1, at: SimTime::ZERO });
        assert_eq!(msgs.len(), 2);
        assert!(msgs.iter().all(|m| matches!(m, ElectionMessage::Victory { .. })));
        assert!(election.is_coordinator());
    }

    #[test]
    fn lower_node_sends_to_higher() {
        let all = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut election = BullyElection::new(NodeId(1), 5000);

        let msgs = election.initiate_election(&all, SimTime::ZERO);

        // Node 1 sends Start to nodes 2 and 3
        assert!(matches!(election.state, ElectionState::WaitingForResponses { .. }));
        assert_eq!(msgs.len(), 2);
        assert!(msgs.iter().all(|m| matches!(m, ElectionMessage::Start { .. })));
    }

    #[test]
    fn higher_node_responds_alive() {
        let all = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut election3 = BullyElection::new(NodeId(3), 5000);

        let msgs = election3.handle_election_start(NodeId(1), &all, SimTime(100));

        // Node 3 responds with Alive and starts own election (wins immediately)
        assert!(msgs.iter().any(|m| matches!(m, ElectionMessage::Alive { .. })));
        assert!(election3.is_coordinator());
    }

    #[test]
    fn yield_on_alive_response() {
        let all = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut election1 = BullyElection::new(NodeId(1), 5000);

        election1.initiate_election(&all, SimTime::ZERO);
        election1.handle_alive(NodeId(3));

        assert_eq!(
            election1.state,
            ElectionState::Yielded { yielded_to: NodeId(3) }
        );
        assert!(!election1.is_coordinator());
    }

    #[test]
    fn accept_victory() {
        let mut election1 = BullyElection::new(NodeId(1), 5000);

        let accepted = election1.handle_victory(NodeId(3), 1);
        assert!(accepted);
        assert_eq!(election1.known_coordinator(), Some(NodeId(3)));
        assert_eq!(election1.current_term(), 1);
        assert_eq!(election1.state, ElectionState::Idle);
    }

    #[test]
    fn reject_stale_victory() {
        let mut election = BullyElection::new(NodeId(1), 5000);
        election.handle_victory(NodeId(3), 5);

        // Stale term should be rejected
        let accepted = election.handle_victory(NodeId(2), 3);
        assert!(!accepted);
        assert_eq!(election.known_coordinator(), Some(NodeId(3)));
    }

    #[test]
    fn timeout_triggers_victory() {
        let all = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut election2 = BullyElection::new(NodeId(2), 5000);

        election2.initiate_election(&all, SimTime(0));

        // Not timed out yet
        assert!(election2.check_timeout(SimTime(4999), &all).is_none());

        // Timed out — declare victory
        let msgs = election2.check_timeout(SimTime(5000), &all).unwrap();
        assert!(election2.is_coordinator());
        assert_eq!(msgs.len(), 2); // victory to nodes 1 and 3
    }

    #[test]
    fn bully_winner_utility() {
        assert_eq!(
            bully_winner(&[NodeId(3), NodeId(1), NodeId(5), NodeId(2)]),
            Some(NodeId(5))
        );
        assert_eq!(bully_winner(&[]), None);
    }

    #[test]
    fn reset_clears_state() {
        let all = vec![NodeId(1), NodeId(2)];
        let mut election = BullyElection::new(NodeId(1), 5000);
        election.initiate_election(&all, SimTime::ZERO);
        assert!(matches!(election.state, ElectionState::WaitingForResponses { .. }));

        election.reset();
        assert_eq!(election.state, ElectionState::Idle);
    }
}
