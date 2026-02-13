//! Agent-based concurrent kernel runtime.
//!
//! `AgentWorld` is the engine entrypoint.

pub mod agent;
pub mod drive;
pub mod economy;
pub mod institution;
pub mod memory;
pub mod operator;
pub mod perception;
pub mod planner;
pub mod scheduler;
pub mod social;
pub mod spatial;
pub mod world;

pub use world::AgentWorld;
pub use world::StepMetrics;
