//! Heartbeat monitoring — tracks heartbeat send/receive/timeout.
//!
//! The coordinator periodically sends heartbeat pings to all nodes.
//! If a node fails to respond within the timeout window, it is declared dead.

use std::collections::HashMap;

use murmur_core::types::{NodeId, SimTime};

/// Heartbeat monitoring configuration and state.
#[derive(Debug, Clone)]
pub struct HeartbeatMonitor {
    /// Heartbeat interval in ms.
    interval_ms: u64,
    /// Number of missed heartbeats before declaring a node dead.
    timeout_multiplier: u32,
    /// When the last heartbeat was sent by this node.
    last_sent: SimTime,
    /// When each peer's last heartbeat was received.
    last_received: HashMap<NodeId, SimTime>,
}

impl HeartbeatMonitor {
    /// Create a new heartbeat monitor.
    pub fn new(interval_ms: u64, timeout_multiplier: u32) -> Self {
        Self {
            interval_ms,
            timeout_multiplier,
            last_sent: SimTime::ZERO,
            last_received: HashMap::new(),
        }
    }

    /// Heartbeat interval in ms.
    pub fn interval_ms(&self) -> u64 {
        self.interval_ms
    }

    /// Timeout duration in ms (interval × multiplier).
    pub fn timeout_ms(&self) -> u64 {
        self.interval_ms * self.timeout_multiplier as u64
    }

    /// Record that we sent a heartbeat at the given time.
    pub fn record_sent(&mut self, at: SimTime) {
        self.last_sent = at;
    }

    /// When the last heartbeat was sent.
    pub fn last_sent(&self) -> SimTime {
        self.last_sent
    }

    /// Check if it's time to send the next heartbeat.
    pub fn should_send(&self, now: SimTime) -> bool {
        now.duration_since(self.last_sent) >= self.interval_ms
    }

    /// Record receiving a heartbeat from a peer.
    pub fn record_received(&mut self, from: NodeId, at: SimTime) {
        self.last_received.insert(from, at);
    }

    /// When was the last heartbeat from a specific peer?
    pub fn last_received_from(&self, node_id: NodeId) -> Option<SimTime> {
        self.last_received.get(&node_id).copied()
    }

    /// Check if a specific peer has timed out.
    pub fn has_timed_out(&self, node_id: NodeId, now: SimTime) -> bool {
        match self.last_received.get(&node_id) {
            Some(&last) => now.duration_since(last) >= self.timeout_ms(),
            None => true, // never heard from → timed out
        }
    }

    /// Check all peers and return those that have timed out.
    pub fn check_all_timeouts(&self, now: SimTime) -> Vec<NodeId> {
        self.last_received
            .iter()
            .filter(|&(_, last)| now.duration_since(*last) >= self.timeout_ms())
            .map(|(&id, _)| id)
            .collect()
    }

    /// Start tracking a new peer (initialize with current time).
    pub fn track_peer(&mut self, node_id: NodeId, at: SimTime) {
        self.last_received.insert(node_id, at);
    }

    /// Stop tracking a peer.
    pub fn untrack_peer(&mut self, node_id: NodeId) {
        self.last_received.remove(&node_id);
    }

    /// Number of peers being tracked.
    pub fn tracked_count(&self) -> usize {
        self.last_received.len()
    }

    /// Clear all tracking state.
    pub fn clear(&mut self) {
        self.last_received.clear();
        self.last_sent = SimTime::ZERO;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_monitor() {
        let monitor = HeartbeatMonitor::new(1000, 3);
        assert_eq!(monitor.interval_ms(), 1000);
        assert_eq!(monitor.timeout_ms(), 3000);
        assert_eq!(monitor.tracked_count(), 0);
    }

    #[test]
    fn should_send_at_interval() {
        let mut monitor = HeartbeatMonitor::new(1000, 3);
        monitor.record_sent(SimTime(0));

        assert!(!monitor.should_send(SimTime(500)));
        assert!(!monitor.should_send(SimTime(999)));
        assert!(monitor.should_send(SimTime(1000)));
        assert!(monitor.should_send(SimTime(2000)));
    }

    #[test]
    fn track_and_receive() {
        let mut monitor = HeartbeatMonitor::new(1000, 3);
        monitor.track_peer(NodeId(1), SimTime(100));

        assert_eq!(monitor.last_received_from(NodeId(1)), Some(SimTime(100)));
        assert_eq!(monitor.tracked_count(), 1);

        monitor.record_received(NodeId(1), SimTime(1100));
        assert_eq!(monitor.last_received_from(NodeId(1)), Some(SimTime(1100)));
    }

    #[test]
    fn timeout_detection() {
        let mut monitor = HeartbeatMonitor::new(1000, 3);
        monitor.track_peer(NodeId(1), SimTime(0));
        monitor.track_peer(NodeId(2), SimTime(0));

        // At time 2999, nobody has timed out (timeout = 3000ms)
        assert!(!monitor.has_timed_out(NodeId(1), SimTime(2999)));

        // At time 3000, both should time out
        assert!(monitor.has_timed_out(NodeId(1), SimTime(3000)));
        assert!(monitor.has_timed_out(NodeId(2), SimTime(3000)));

        // Node 1 sends a heartbeat at 2500
        monitor.record_received(NodeId(1), SimTime(2500));

        // At 3000, only node 2 times out (node 1 was heard at 2500)
        assert!(!monitor.has_timed_out(NodeId(1), SimTime(3000)));
        assert!(monitor.has_timed_out(NodeId(2), SimTime(3000)));
    }

    #[test]
    fn check_all_timeouts() {
        let mut monitor = HeartbeatMonitor::new(1000, 3);
        monitor.track_peer(NodeId(1), SimTime(0));
        monitor.track_peer(NodeId(2), SimTime(0));
        monitor.track_peer(NodeId(3), SimTime(1000));

        // At 3000: nodes 1 and 2 timed out (last at 0, 0), node 3 ok (last at 1000)
        let mut timed_out = monitor.check_all_timeouts(SimTime(3000));
        timed_out.sort();
        assert_eq!(timed_out, vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn untracked_peer_is_timed_out() {
        let monitor = HeartbeatMonitor::new(1000, 3);
        assert!(monitor.has_timed_out(NodeId(99), SimTime(0)));
    }

    #[test]
    fn untrack_peer() {
        let mut monitor = HeartbeatMonitor::new(1000, 3);
        monitor.track_peer(NodeId(1), SimTime(0));
        assert_eq!(monitor.tracked_count(), 1);

        monitor.untrack_peer(NodeId(1));
        assert_eq!(monitor.tracked_count(), 0);
        assert!(monitor.last_received_from(NodeId(1)).is_none());
    }

    #[test]
    fn clear_resets_all() {
        let mut monitor = HeartbeatMonitor::new(1000, 3);
        monitor.track_peer(NodeId(1), SimTime(0));
        monitor.record_sent(SimTime(500));

        monitor.clear();
        assert_eq!(monitor.tracked_count(), 0);
        assert_eq!(monitor.last_sent(), SimTime::ZERO);
    }
}
