//! quorumdb: a distributed SQL database, built one proven layer at a time.
//!
//! Milestones 1 and 2 are the single-node storage engine (a write-ahead
//! log, block-indexed tables with bloom filters, leveled compaction) and the
//! deterministic simulator that tests it. See the README for the roadmap.

pub mod bloom;
pub mod codec;
pub mod crc;
pub mod engine;
pub mod error;
pub mod fs;
pub mod manifest;
pub mod merge;
pub mod rng;
pub mod sim;
pub mod sstable;
pub mod wal;

pub use engine::{Db, Options, Stats, SyncMode};
pub use error::{Error, Result};
pub use fs::{Fs, RealFs, SimFs};
