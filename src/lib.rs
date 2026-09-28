//! quorumdb: a distributed SQL database, built one proven layer at a time.
//!
//! Milestone 1 is the single-node storage engine and the deterministic
//! simulator that tests it. See the README for the roadmap.

pub mod codec;
pub mod crc;
pub mod engine;
pub mod error;
pub mod fs;
pub mod manifest;
pub mod rng;
pub mod sim;
pub mod sstable;
pub mod wal;

pub use engine::{Db, Options, Stats, SyncMode};
pub use error::{Error, Result};
pub use fs::{Fs, RealFs, SimFs};
