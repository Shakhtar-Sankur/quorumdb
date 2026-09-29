//! quorumdb: a distributed SQL database, built one proven layer at a time.
//!
//! - [`storage`]: the single-node LSM storage engine and its crash simulator.
//! - [`raft`]: Raft consensus as a pure state machine, and its cluster simulator.
//!
//! See the README for the architecture and the roadmap.

pub mod check;
pub mod codec;
pub mod crc;
pub mod error;
pub mod kv;
pub mod raft;
pub mod rng;
pub mod runtime;
pub mod server;
pub mod sql;
pub mod storage;
pub mod txn;

pub use error::{Error, Result};
pub use storage::engine::{self, Db, Options, Stats, SyncMode};
pub use storage::fs::{self, Fs, RealFs, SimFs};
pub use storage::sim;
pub use storage::wal::Op;
