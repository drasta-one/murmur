//! # murmur-overlay
//!
//! Overlay State Table and cluster topology management for the DOR runtime.
//!
//! This crate implements:
//! - [`OverlayStateTable`] — cluster-wide node metadata and coordinator tracking
//! - [`Topology`] — directed graph of node-to-node reachability
//! - [`PeerHealthTracker`] — per-peer heartbeat liveness monitoring
//! - [`MembershipManager`] — join / leave / timeout lifecycle

pub mod membership;
pub mod peer_health;
pub mod state_table;
pub mod topology;

pub use membership::{MembershipChange, MembershipManager};
pub use peer_health::PeerHealthTracker;
pub use state_table::OverlayStateTable;
pub use topology::Topology;
