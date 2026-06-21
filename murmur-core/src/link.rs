//! Link primitive — a measurable connection between two nodes.

use serde::{Deserialize, Serialize};

use crate::types::{NodeId, SimTime};

/// Measurable properties of a link between two nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkMetrics {
    /// One-way latency in milliseconds.
    pub latency_ms: u32,
    /// Available throughput in bytes/sec.
    pub throughput: u64,
    /// Packet loss probability `[0.0, 1.0)`.
    pub packet_loss: f64,
    /// Jitter in milliseconds (variance in latency).
    pub jitter_ms: u32,
}

impl Default for LinkMetrics {
    fn default() -> Self {
        Self {
            latency_ms: 2,
            throughput: 50_000_000, // 50 MB/s
            packet_loss: 0.0,
            jitter_ms: 0,
        }
    }
}

/// The operational status of a link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkStatus {
    /// Link is functioning normally.
    Healthy,
    /// Link is experiencing degraded performance.
    Degraded,
    /// Link is completely severed.
    Severed,
}

/// A connection between two nodes in the cluster.
#[derive(Debug, Clone)]
pub struct Link {
    /// Source node.
    pub from: NodeId,
    /// Destination node.
    pub to: NodeId,
    /// Current link metrics.
    pub metrics: LinkMetrics,
    /// Current link status.
    pub status: LinkStatus,
    /// When the link was last measured.
    pub last_measured: SimTime,
}

impl Link {
    /// Create a new healthy link between two nodes.
    pub fn new(from: NodeId, to: NodeId, metrics: LinkMetrics, at: SimTime) -> Self {
        Self {
            from,
            to,
            metrics,
            status: LinkStatus::Healthy,
            last_measured: at,
        }
    }

    /// Calculate the time (in ms) to transfer `size_bytes` over this link.
    /// Returns `None` if the link is severed.
    pub fn transfer_time_ms(&self, size_bytes: u64) -> Option<u64> {
        if self.status == LinkStatus::Severed || self.metrics.throughput == 0 {
            return None;
        }
        let transfer_ms = (size_bytes * 1000) / self.metrics.throughput;
        Some(transfer_ms + self.metrics.latency_ms as u64)
    }

    /// Degrade the link with new metrics.
    pub fn degrade(&mut self, new_metrics: LinkMetrics, at: SimTime) {
        self.metrics = new_metrics;
        self.status = LinkStatus::Degraded;
        self.last_measured = at;
    }

    /// Sever the link completely.
    pub fn sever(&mut self, at: SimTime) {
        self.status = LinkStatus::Severed;
        self.last_measured = at;
    }

    /// Restore the link to healthy with the given metrics.
    pub fn restore(&mut self, metrics: LinkMetrics, at: SimTime) {
        self.metrics = metrics;
        self.status = LinkStatus::Healthy;
        self.last_measured = at;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_link_transfer_time() {
        let link = Link::new(NodeId(1), NodeId(2), LinkMetrics::default(), SimTime::ZERO);
        // 50 MB/s, 2ms latency, 1MB transfer
        // 1_048_576 * 1000 / 50_000_000 = 20ms + 2ms = 22ms
        let time = link.transfer_time_ms(1_048_576);
        assert_eq!(time, Some(22));
    }

    #[test]
    fn severed_link_returns_none() {
        let mut link = Link::new(NodeId(1), NodeId(2), LinkMetrics::default(), SimTime::ZERO);
        link.sever(SimTime(100));
        assert_eq!(link.transfer_time_ms(1_048_576), None);
    }
}
