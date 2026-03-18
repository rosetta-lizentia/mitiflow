//! Partitioned consumer groups via rendezvous hashing + liveliness-driven rebalancing.
//!
//! This module is gated behind the `partition` feature flag.

pub mod hash_ring;
pub mod rebalance;
