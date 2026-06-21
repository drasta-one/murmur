//! # murmur-core
//!
//! Core primitives for the Distributed Overlay Runtime.
//!
//! This crate defines the foundational types that all other DOR crates depend on:
//! - [`NodeId`], [`ChunkId`], [`TaskId`], and other identifiers
//! - [`Node`] — a participating runtime device
//! - [`Link`] — a measurable connection between nodes
//! - [`Task`] — a schedulable unit of cooperative work
//! - [`Manifest`] — the runtime's integrity contract
//! - [`MurmurEvent`] — domain events for observability

pub mod chunk;
pub mod cluster;
pub mod config;
pub mod error;
pub mod event;
pub mod link;
pub mod manifest;
pub mod net;
pub mod node;
pub mod rpc;
pub mod task;
pub mod types;

// Re-export the most commonly used types at the crate root
pub use chunk::{ChunkMeta, ChunkOwnership};
pub use cluster::ClusterConfig;
pub use error::MurmurError;
pub use event::MurmurEvent;
pub use link::{Link, LinkMetrics, LinkStatus};
pub use manifest::Manifest;
pub use node::{Node, NodeConfig, NodeStatus};
pub use task::{Task, TaskKind, TaskStatus};
pub use types::{ChunkId, ClusterId, ManifestId, NodeId, SimTime, TaskId};
