//! Requests clients send to a range, the commands those become in the Raft
//! log, and the answers.

use crate::codec::{Reader, put_bytes, put_u64};
use crate::kv::keys::RangeId;
use crate::raft::NodeId;

/// Identifies a request so its response can be matched to it.
pub type ReqId = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Done,
    Value(Option<Vec<u8>>),
    Rows {
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        /// The range ended before the scan did: continue from this key.
        resume: Option<Vec<u8>>,
    },
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
    Storage(String),
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// A command in a range's Raft log. Commands are evaluated when applied,
/// identically on every replica, so replicas cannot diverge.
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
            _ => return None,
        };
        r.is_empty().then_some(cmd)
    }
}
