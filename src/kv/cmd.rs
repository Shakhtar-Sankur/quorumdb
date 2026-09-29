//! Requests clients send to a range, the commands those become in the Raft
//! log, and the answers.

use crate::codec::{Reader, put_bytes, put_u64};
use crate::kv::keys::RangeId;
use crate::raft::NodeId;
use crate::txn::mvcc::{LockInfo, TxnStatus};

/// Identifies a request so its response can be matched to it.
pub type ReqId = u64;

/// The key whose range serves timestamps: it sorts before every other key,
/// so it always lives in the first range.
pub const TSO_KEY: &[u8] = b"\x00\x00tso";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    // ─── The plain key-value interface ───
    /// Linearizable point read.
    Get {
        key: Vec<u8>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Linearizable scan of `[start, end)` (`end` empty: no bound), clamped
    /// to the range; the response says where to resume in the next range.
    Scan {
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    },

    // ─── Transactions (see `txn`) ───
    /// `count` fresh timestamps from the oracle, strictly increasing.
    Timestamp {
        count: u64,
    },
    /// The value of `key` in the snapshot at `ts`.
    MvccGet {
        key: Vec<u8>,
        ts: u64,
    },
    /// Visible pairs in `[start, end)` at `ts`, clamped to the range.
    MvccScan {
        start: Vec<u8>,
        end: Vec<u8>,
        ts: u64,
        limit: usize,
    },
    /// Serializable read validation (see `mvcc::validate_read`).
    ValidateRead {
        key: Vec<u8>,
        start_ts: u64,
        commit_ts: u64,
    },
    /// Serializable validation of a scanned span, clamped to the range.
    ValidateScan {
        start: Vec<u8>,
        end: Vec<u8>,
        start_ts: u64,
        commit_ts: u64,
    },
    Prewrite {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        primary: Vec<u8>,
        start_ts: u64,
        ttl: u64,
    },
    Commit {
        key: Vec<u8>,
        start_ts: u64,
        commit_ts: u64,
    },
    Rollback {
        key: Vec<u8>,
        start_ts: u64,
    },
    CheckTxnStatus {
        primary: Vec<u8>,
        start_ts: u64,
    },
    ResolveLock {
        key: Vec<u8>,
        start_ts: u64,
        commit_ts: Option<u64>,
    },

    // ─── Administration (the placement driver) ───
    /// Split the range at `key`; the right half becomes `new_range`.
    Split {
        key: Vec<u8>,
        new_range: RangeId,
    },
    /// Add or remove one replica.
    ChangeReplicas {
        replicas: Vec<NodeId>,
    },
    TransferLeader {
        to: NodeId,
    },
}

impl Request {
    /// The key that decides which range serves the request.
    pub fn routing_key(&self) -> &[u8] {
        match self {
            Request::Get { key }
            | Request::Put { key, .. }
            | Request::Delete { key }
            | Request::MvccGet { key, .. }
            | Request::ValidateRead { key, .. }
            | Request::Prewrite { key, .. }
            | Request::Commit { key, .. }
            | Request::Rollback { key, .. }
            | Request::ResolveLock { key, .. }
            | Request::Split { key, .. } => key,
            Request::Scan { start, .. }
            | Request::MvccScan { start, .. }
            | Request::ValidateScan { start, .. } => start,
            Request::CheckTxnStatus { primary, .. } => primary,
            Request::Timestamp { .. } => TSO_KEY,
            Request::ChangeReplicas { .. } | Request::TransferLeader { .. } => &[],
        }
    }

    /// Whether retrying after an unknown outcome could apply it twice with
    /// a different effect. Every transactional command is idempotent.
    pub fn is_idempotent(&self) -> bool {
        !matches!(self, Request::Put { .. } | Request::Delete { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Done,
    Value(Option<Vec<u8>>),
    Rows {
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        /// The range ended before the scan did: continue from this key.
        resume: Option<Vec<u8>>,
    },
    /// The first of the requested timestamps.
    Ts(u64),
    Status(TxnStatus),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvError {
    /// Not the leader; try this node if known.
    NotLeader(Option<NodeId>),
    /// This store holds no initialized replica of the range.
    RangeNotFound,
    /// The key belongs to another range (after a split): refresh the route.
    KeyNotInRange,
    /// Try again shortly (e.g. a membership change is in flight).
    Busy,
    /// The proposal was overwritten by a new leader; it did not apply.
    Dropped,
    /// The replica went away with the proposal in flight: it may or may
    /// not apply. Do not blindly retry a non-idempotent write.
    Ambiguous,
    /// Another transaction's lock is in the way: learn its fate first.
    Locked(LockInfo),
    /// A transaction committed this key after ours started.
    WriteConflict,
    /// Our transaction was rolled back (by someone resolving its locks).
    Aborted,
    /// A committed transaction cannot be rolled back.
    AlreadyCommitted(u64),
    Storage(String),
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// A command in a range's Raft log. Commands are evaluated when applied,
/// identically on every replica, so replicas cannot diverge. Anything that
/// depends on a clock carries the proposer's reading of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Applies only if the range is still at `generation`, so two racing
    /// splits (or a split racing a membership change) cannot both apply.
    Split {
        key: Vec<u8>,
        new_range: RangeId,
        generation: u64,
    },
    Prewrite {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        primary: Vec<u8>,
        start_ts: u64,
        ttl: u64,
        now: u64,
    },
    Commit {
        key: Vec<u8>,
        start_ts: u64,
        commit_ts: u64,
    },
    Rollback {
        key: Vec<u8>,
        start_ts: u64,
    },
    CheckTxnStatus {
        primary: Vec<u8>,
        start_ts: u64,
        now: u64,
    },
    ResolveLock {
        key: Vec<u8>,
        start_ts: u64,
        commit_ts: Option<u64>,
    },
    /// Raise the timestamp oracle's durable high-water mark.
    TsoExtend {
        limit: u64,
    },
}

fn put_opt_bytes(out: &mut Vec<u8>, v: &Option<Vec<u8>>) {
    match v {
        Some(v) => {
            out.push(1);
            put_bytes(out, v);
        }
        None => out.push(0),
    }
}

fn opt_bytes(r: &mut Reader) -> Option<Option<Vec<u8>>> {
    match r.u8()? {
        1 => Some(Some(r.bytes()?)),
        0 => Some(None),
        _ => None,
    }
}

fn put_opt_u64(out: &mut Vec<u8>, v: Option<u64>) {
    match v {
        Some(v) => {
            out.push(1);
            put_u64(out, v);
        }
        None => out.push(0),
    }
}

fn opt_u64(r: &mut Reader) -> Option<Option<u64>> {
    match r.u8()? {
        1 => Some(Some(r.u64()?)),
        0 => Some(None),
        _ => None,
    }
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Command::Put { key, value } => {
                out.push(1);
                put_bytes(&mut out, key);
                put_bytes(&mut out, value);
            }
            Command::Delete { key } => {
                out.push(2);
                put_bytes(&mut out, key);
            }
            Command::Split {
                key,
                new_range,
                generation,
            } => {
                out.push(3);
                put_bytes(&mut out, key);
                put_u64(&mut out, *new_range);
                put_u64(&mut out, *generation);
            }
            Command::Prewrite {
                key,
                value,
                primary,
                start_ts,
                ttl,
                now,
            } => {
                out.push(4);
                put_bytes(&mut out, key);
                put_opt_bytes(&mut out, value);
                put_bytes(&mut out, primary);
                put_u64(&mut out, *start_ts);
                put_u64(&mut out, *ttl);
                put_u64(&mut out, *now);
            }
            Command::Commit {
                key,
                start_ts,
                commit_ts,
            } => {
                out.push(5);
                put_bytes(&mut out, key);
                put_u64(&mut out, *start_ts);
                put_u64(&mut out, *commit_ts);
            }
            Command::Rollback { key, start_ts } => {
                out.push(6);
                put_bytes(&mut out, key);
                put_u64(&mut out, *start_ts);
            }
            Command::CheckTxnStatus {
                primary,
                start_ts,
                now,
            } => {
                out.push(7);
                put_bytes(&mut out, primary);
                put_u64(&mut out, *start_ts);
                put_u64(&mut out, *now);
            }
            Command::ResolveLock {
                key,
                start_ts,
                commit_ts,
            } => {
                out.push(8);
                put_bytes(&mut out, key);
                put_u64(&mut out, *start_ts);
                put_opt_u64(&mut out, *commit_ts);
            }
            Command::TsoExtend { limit } => {
                out.push(9);
                put_u64(&mut out, *limit);
            }
        }
        out
    }

    pub fn decode(data: &[u8]) -> Option<Command> {
        let mut r = Reader::new(data);
        let cmd = match r.u8()? {
            1 => Command::Put {
                key: r.bytes()?,
                value: r.bytes()?,
            },
            2 => Command::Delete { key: r.bytes()? },
            3 => Command::Split {
                key: r.bytes()?,
                new_range: r.u64()?,
                generation: r.u64()?,
            },
            4 => Command::Prewrite {
                key: r.bytes()?,
                value: opt_bytes(&mut r)?,
                primary: r.bytes()?,
                start_ts: r.u64()?,
                ttl: r.u64()?,
                now: r.u64()?,
            },
            5 => Command::Commit {
                key: r.bytes()?,
                start_ts: r.u64()?,
                commit_ts: r.u64()?,
            },
            6 => Command::Rollback {
                key: r.bytes()?,
                start_ts: r.u64()?,
            },
            7 => Command::CheckTxnStatus {
                primary: r.bytes()?,
                start_ts: r.u64()?,
                now: r.u64()?,
            },
            8 => Command::ResolveLock {
                key: r.bytes()?,
                start_ts: r.u64()?,
                commit_ts: opt_u64(&mut r)?,
            },
            9 => Command::TsoExtend { limit: r.u64()? },
            _ => return None,
        };
        r.is_empty().then_some(cmd)
    }

    /// The key whose range must own this command, if any.
    pub fn key(&self) -> Option<&[u8]> {
        match self {
            Command::Put { key, .. }
            | Command::Delete { key }
            | Command::Prewrite { key, .. }
            | Command::Commit { key, .. }
            | Command::Rollback { key, .. }
            | Command::ResolveLock { key, .. } => Some(key),
            Command::CheckTxnStatus { primary, .. } => Some(primary),
            Command::TsoExtend { .. } => Some(TSO_KEY),
            Command::Split { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_round_trip() {
        let cmds = vec![
            Command::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            Command::Split {
                key: b"m".to_vec(),
                new_range: 3,
                generation: 2,
            },
            Command::Prewrite {
                key: b"k".to_vec(),
                value: None,
                primary: b"p".to_vec(),
                start_ts: 5,
                ttl: 100,
                now: 7,
            },
            Command::ResolveLock {
                key: b"k".to_vec(),
                start_ts: 5,
                commit_ts: Some(9),
            },
            Command::TsoExtend { limit: 1000 },
        ];
        for c in cmds {
            assert_eq!(Command::decode(&c.encode()), Some(c));
        }
    }
}
