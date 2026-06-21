//! # murmur-coordinator
//!
//! Coordinator election and lifecycle management for the DOR runtime.
//!
//! This crate implements:
//! - [`election::BullyElection`] — Deterministic Bully election protocol
//! - [`lifecycle::CoordinatorLifecycle`] — Coordinator state machine
//! - [`heartbeat::HeartbeatMonitor`] — Health monitoring via heartbeats
//! - [`recovery::StateRecovery`] — State recovery after re-election

pub mod election;
pub mod heartbeat;
pub mod lifecycle;
pub mod recovery;
pub mod scheduler;

pub use election::{BullyElection, ElectionMessage, ElectionState};
pub use heartbeat::HeartbeatMonitor;
pub use lifecycle::{CoordinatorLifecycle, CoordinatorRecord, CoordinatorState};
pub use recovery::{RecoveryPlan, StateRecovery};
pub use scheduler::NodeThroughput;
