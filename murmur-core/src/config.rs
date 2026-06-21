//! Configuration structures for the DOR simulation.

use serde::{Deserialize, Serialize};

/// Top-level simulation configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationConfig {
    /// Random seed for deterministic simulation.
    pub seed: u64,
    /// Maximum simulation time in ms before timeout.
    pub max_time_ms: u64,
    /// Whether to output a detailed event trace.
    pub trace_events: bool,
    /// Whether to generate a summary report.
    pub generate_report: bool,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            max_time_ms: 300_000, // 5 minutes simulated
            trace_events: true,
            generate_report: true,
        }
    }
}
