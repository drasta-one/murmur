//! # murmur-scheduler
//!
//! Chunk scheduling and task assignment for the DOR runtime.
//!
//! This crate implements:
//! - [`strategy::SchedulingStrategy`] trait with Round-robin and Bandwidth-weighted
//! - [`chunk_scheduler::ChunkScheduler`] — orchestrates downloads and redistribution
//! - [`task_queue::TaskQueue`] — priority-ordered task queue
//! - [`reassignment`] — chunk reassignment on node failure

pub mod chunk_scheduler;
pub mod reassignment;
pub mod strategy;
pub mod task_queue;

pub use chunk_scheduler::ChunkScheduler;
pub use reassignment::{reassign_chunks, ReassignmentResult};
pub use strategy::{BandwidthWeightedStrategy, ChunkAssignment, RoundRobinStrategy, SchedulingStrategy};
pub use task_queue::{TaskPriority, TaskQueue};
