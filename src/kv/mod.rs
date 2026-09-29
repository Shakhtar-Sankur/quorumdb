//! The distributed key-value layer: data split into ranges, each range
//! replicated by its own Raft group, many ranges per node (multi-Raft).
//!
//! - [`keys`]: how a node lays out user data and Raft state in its engine.
//! - [`store`]: one node, hosting many replicas on one storage engine.
//! - [`pd`]: the placement driver, which splits and rebalances ranges.
//! - [`sim`]: the cluster simulator, with a linearizability checker.

pub mod client;
pub mod cmd;
pub mod keys;
pub mod pd;
pub mod sim;
pub mod store;
