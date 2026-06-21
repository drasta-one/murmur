//! Dynamic coordinator algorithms for WAN bonding and load balancing.
//!
//! Provides the `NodeThroughput` sliding window tracker and `optimal_chunk_size` calculation.

use murmur_core::types::NodeId;
use tracing::warn;

/// Default granular chunk size in bytes (512 KB).
pub const GRANULAR_CHUNK_SIZE: u64 = 524_288;
/// Maximum batch size in bytes to prevent overwhelming a node.
pub const MAX_BATCH_SIZE: u64 = 16_777_216; // 16 MB

/// Tracks a node's effective WAN throughput using an Exponential Moving Average (EMA).
#[derive(Debug, Clone)]
pub struct NodeThroughput {
    pub node_id: NodeId,
    pub expected_bps: u64,
    pub ema_bps: f64,
    pub alpha: f64,
    pub last_rtt_ms: u64,
}

impl NodeThroughput {
    /// Create a new tracker with a default alpha of 0.3.
    pub fn new(node_id: NodeId, expected_bps: u64) -> Self {
        Self {
            node_id,
            expected_bps,
            ema_bps: expected_bps as f64,
            alpha: 0.3,
            last_rtt_ms: 100, // Reasonable default
        }
    }

    /// Record a new measurement and update the EMA.
    pub fn record_measurement(&mut self, observed_bps: u64, rtt_ms: u64) {
        self.ema_bps = (self.alpha * observed_bps as f64) + ((1.0 - self.alpha) * self.ema_bps);
        self.last_rtt_ms = rtt_ms;
    }

    /// Check if the node is significantly rate limited compared to its expected capability.
    /// Returns true if observed throughput drops to <= 20% of expected capacity.
    pub fn is_rate_limited(&self) -> bool {
        self.ema_bps < (self.expected_bps as f64 * 0.2)
    }

    /// Calculate the optimal batch size (in bytes) to request next.
    /// Targets 4-8 seconds of in-flight data based on current throughput.
    pub fn optimal_batch_size(&self) -> u64 {
        let target_inflight_seconds = 6.0;
        let target_bytes = self.ema_bps * target_inflight_seconds;

        let batch_size = target_bytes as u64;
        let batch_size = batch_size.clamp(GRANULAR_CHUNK_SIZE, MAX_BATCH_SIZE);

        // Round to nearest granular chunk size
        let num_chunks = (batch_size + GRANULAR_CHUNK_SIZE - 1) / GRANULAR_CHUNK_SIZE;
        num_chunks * GRANULAR_CHUNK_SIZE
    }

    /// Calculate how many pipeline batches to maintain.
    /// Prefetch queue size `k = ceil(RTT / chunk_transfer_time)`.
    pub fn pipeline_depth(&self) -> usize {
        let optimal_bytes = self.optimal_batch_size();
        if self.ema_bps <= 0.0 || optimal_bytes == 0 {
            return 1;
        }

        let chunk_transfer_time_ms = (optimal_bytes as f64 / self.ema_bps) * 1000.0;
        if chunk_transfer_time_ms <= 0.0 {
            return 1;
        }

        let k = (self.last_rtt_ms as f64 / chunk_transfer_time_ms).ceil() as usize;
        k.clamp(1, 4) // Keep pipeline bounded
    }
}
