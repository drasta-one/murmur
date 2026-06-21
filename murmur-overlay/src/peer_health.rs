//! Per-peer heartbeat health tracking.
//!
//! The [`PeerHealthTracker`] records the last heartbeat time for each peer and
//! can determine which peers have timed out at any given simulation instant.

use std::collections::HashMap;

use murmur_core::{NodeId, SimTime};
use tracing::{debug, warn};

/// Tracks heartbeat liveness for every known peer.
///
/// A peer is considered *alive* if `now − last_heartbeat < timeout_ms`, where
/// `timeout_ms = heartbeat_interval_ms × timeout_multiplier`.
#[derive(Debug)]
pub struct PeerHealthTracker {
    /// Most recent heartbeat time per peer.
    last_heartbeats: HashMap<NodeId, SimTime>,
    /// Timeout threshold in milliseconds.
    timeout_ms: u64,
}

impl PeerHealthTracker {
    /// Create a new tracker.
    ///
    /// The timeout window is `heartbeat_interval_ms * timeout_multiplier`.
    pub fn new(timeout_multiplier: u32, heartbeat_interval_ms: u64) -> Self {
        let timeout_ms = heartbeat_interval_ms * u64::from(timeout_multiplier);
        debug!(
            timeout_ms,
            heartbeat_interval_ms, timeout_multiplier, "PeerHealthTracker created"
        );
        Self {
            last_heartbeats: HashMap::new(),
            timeout_ms,
        }
    }

    /// Record a heartbeat from `node_id` at simulation time `at`.
    pub fn record_heartbeat(&mut self, node_id: NodeId, at: SimTime) {
        debug!(node_id = %node_id, at = %at, "Heartbeat recorded");
        self.last_heartbeats.insert(node_id, at);
    }

    /// Returns `true` if `node_id` has been seen and its last heartbeat is
    /// within the timeout window relative to `now`.
    pub fn is_alive(&self, node_id: NodeId, now: SimTime) -> bool {
        match self.last_heartbeats.get(&node_id) {
            Some(&last) => now.duration_since(last) < self.timeout_ms,
            None => false,
        }
    }

    /// Return a sorted list of tracked peers that have timed out as of `now`.
    pub fn check_timeouts(&self, now: SimTime) -> Vec<NodeId> {
        let mut timed_out: Vec<NodeId> = self
            .last_heartbeats
            .iter()
            .filter(|&(_, &last)| now.duration_since(last) >= self.timeout_ms)
            .map(|(&id, _)| id)
            .collect();
        timed_out.sort();
        if !timed_out.is_empty() {
            warn!(count = timed_out.len(), now = %now, "Peers timed out");
        }
        timed_out
    }

    /// Stop tracking a peer entirely.
    pub fn remove_peer(&mut self, node_id: NodeId) {
        self.last_heartbeats.remove(&node_id);
        debug!(node_id = %node_id, "Peer removed from health tracker");
    }

    /// Return the last-seen heartbeat time for a peer, if tracked.
    pub fn last_seen(&self, node_id: NodeId) -> Option<SimTime> {
        self.last_heartbeats.get(&node_id).copied()
    }

    /// Number of peers currently being tracked.
    pub fn tracked_count(&self) -> usize {
        self.last_heartbeats.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: 1 000 ms interval × 3 multiplier = 3 000 ms timeout.
    fn tracker() -> PeerHealthTracker {
        PeerHealthTracker::new(3, 1000)
    }

    #[test]
    fn new_tracker_is_empty() {
        let t = tracker();
        assert_eq!(t.tracked_count(), 0);
        assert!(!t.is_alive(NodeId(1), SimTime(0)));
    }

    #[test]
    fn record_and_check_alive() {
        let mut t = tracker();
        t.record_heartbeat(NodeId(1), SimTime(1000));

        // 1 000 ms later — well within the 3 000 ms window
        assert!(t.is_alive(NodeId(1), SimTime(2000)));
        assert_eq!(t.last_seen(NodeId(1)), Some(SimTime(1000)));
    }

    #[test]
    fn timeout_detection() {
        let mut t = tracker();
        t.record_heartbeat(NodeId(1), SimTime(0));
        t.record_heartbeat(NodeId(2), SimTime(1000));

        // At t=3000: Node(1) last seen 0, elapsed = 3000 ≥ 3000 → timed out
        //            Node(2) last seen 1000, elapsed = 2000 < 3000 → alive
        let timed_out = t.check_timeouts(SimTime(3000));
        assert_eq!(timed_out, vec![NodeId(1)]);
    }

    #[test]
    fn heartbeat_refresh_prevents_timeout() {
        let mut t = tracker();
        t.record_heartbeat(NodeId(1), SimTime(0));
        // Refresh just before timeout
        t.record_heartbeat(NodeId(1), SimTime(2500));

        // At t=3000 — last seen 2500, elapsed = 500 < 3000 → alive
        assert!(t.is_alive(NodeId(1), SimTime(3000)));
        assert!(t.check_timeouts(SimTime(3000)).is_empty());
    }

    #[test]
    fn remove_peer_stops_tracking() {
        let mut t = tracker();
        t.record_heartbeat(NodeId(1), SimTime(0));
        assert_eq!(t.tracked_count(), 1);

        t.remove_peer(NodeId(1));
        assert_eq!(t.tracked_count(), 0);
        assert!(t.last_seen(NodeId(1)).is_none());
        assert!(!t.is_alive(NodeId(1), SimTime(0)));
    }

    #[test]
    fn tracked_count_accurate() {
        let mut t = tracker();
        t.record_heartbeat(NodeId(1), SimTime(0));
        t.record_heartbeat(NodeId(2), SimTime(0));
        t.record_heartbeat(NodeId(3), SimTime(0));
        assert_eq!(t.tracked_count(), 3);

        t.remove_peer(NodeId(2));
        assert_eq!(t.tracked_count(), 2);
    }

    #[test]
    fn exact_timeout_boundary() {
        let mut t = tracker();
        t.record_heartbeat(NodeId(1), SimTime(0));

        // Exactly at the boundary: elapsed = 2999 < 3000 → alive
        assert!(t.is_alive(NodeId(1), SimTime(2999)));
        // elapsed = 3000 ≥ 3000 → dead
        assert!(!t.is_alive(NodeId(1), SimTime(3000)));
    }

    #[test]
    fn multiple_timeouts_sorted() {
        let mut t = tracker();
        t.record_heartbeat(NodeId(5), SimTime(0));
        t.record_heartbeat(NodeId(1), SimTime(0));
        t.record_heartbeat(NodeId(3), SimTime(0));

        let timed_out = t.check_timeouts(SimTime(5000));
        assert_eq!(timed_out, vec![NodeId(1), NodeId(3), NodeId(5)]);
    }

    #[test]
    fn different_timeout_config() {
        // 500 ms interval × 2 = 1000 ms timeout
        let mut t = PeerHealthTracker::new(2, 500);
        t.record_heartbeat(NodeId(1), SimTime(0));

        assert!(t.is_alive(NodeId(1), SimTime(999)));
        assert!(!t.is_alive(NodeId(1), SimTime(1000)));
    }
}
