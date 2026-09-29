//! How a node lays out everything in its one storage engine.
//!
//! ```text
//! d <user key>                       user data (ranges own disjoint spans of it)
//! r <range id: u64 BE> h             Raft hard state: term, vote, commit
//! r <range id: u64 BE> t             truncated log prefix: index, term, voters
//! r <range id: u64 BE> l <index BE>  Raft log entry
//! r <range id: u64 BE> a             applied state: index, term, descriptor
//! r <range id: u64 BE> i             this node's replica id in the range
//! r <range id: u64 BE> x             tombstone: replica ids below this are dead
//! ```
//!
//! A **replica id** names one membership of one node in one range: the node
//! id in the high 32 bits, an incarnation in the low 32. A node removed from
//! a range and later re-added comes back under a new id, so nothing the old
//! incarnation said (votes, acknowledged entries) can be confused with the
//! new one, and garbage-collecting the old one is always safe.
//!
//! Range-local keys sort by range id, then kind, so a replica's whole Raft
//! state is one contiguous span, and its log is a contiguous sub-span that
//! compaction deletes from the front. The applied state and the data a
//! command writes go into the same atomic batch, so after a crash the state
//! machine is always exactly "the log applied up to `applied`".

use crate::codec::{Reader, put_bytes, put_u32, put_u64};
use crate::raft::{Entry, EntryData, HardState, NodeId, SnapshotMeta};

pub type RangeId = u64;
/// See the module docs: `(node << 32) | incarnation`.
pub type ReplicaId = u64;

pub fn replica_id(node: NodeId, incarnation: u64) -> ReplicaId {
    (node << 32) | (incarnation & 0xFFFF_FFFF)
}

pub fn node_of(replica: ReplicaId) -> NodeId {
    replica >> 32
}

pub fn incarnation_of(replica: ReplicaId) -> u64 {
    replica & 0xFFFF_FFFF
}

pub const DATA: u8 = b'd';
pub const LOCAL: u8 = b'r';

pub fn data_key(user_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(user_key.len() + 1);
    k.push(DATA);
    k.extend_from_slice(user_key);
    k
}

pub fn user_key(data_key: &[u8]) -> &[u8] {
    &data_key[1..]
}

/// The storage-engine span `[start, end)` holding a range's user data.
pub fn data_span(start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let lo = data_key(start);
    let hi = if end.is_empty() {
        vec![DATA + 1]
    } else {
        data_key(end)
    };
    (lo, hi)
}

fn local(range: RangeId, kind: u8) -> Vec<u8> {
    let mut k = vec![LOCAL];
    k.extend_from_slice(&range.to_be_bytes());
    k.push(kind);
    k
}

pub fn hard_state_key(range: RangeId) -> Vec<u8> {
    local(range, b'h')
}

pub fn truncated_key(range: RangeId) -> Vec<u8> {
    local(range, b't')
}

pub fn applied_key(range: RangeId) -> Vec<u8> {
    local(range, b'a')
}

pub fn replica_id_key(range: RangeId) -> Vec<u8> {
    local(range, b'i')
}

pub fn tombstone_key(range: RangeId) -> Vec<u8> {
    local(range, b'x')
}

pub fn log_key(range: RangeId, index: u64) -> Vec<u8> {
    let mut k = local(range, b'l');
    k.extend_from_slice(&index.to_be_bytes());
    k
}

/// The whole span of a range's local keys.
pub fn local_span(range: RangeId) -> (Vec<u8>, Vec<u8>) {
    let mut lo = vec![LOCAL];
    lo.extend_from_slice(&range.to_be_bytes());
    let mut hi = vec![LOCAL];
    hi.extend_from_slice(&(range + 1).to_be_bytes());
    (lo, hi)
}

/// The span of a range's log entries from `from` on.
pub fn log_span(range: RangeId, from: u64) -> (Vec<u8>, Vec<u8>) {
    (log_key(range, from), local(range, b'l' + 1))
}

/// Parse a local key back into `(range, kind)`.
pub fn parse_local(key: &[u8]) -> Option<(RangeId, u8)> {
    if key.len() < 10 || key[0] != LOCAL {
        return None;
    }
    let range = u64::from_be_bytes(key[1..9].try_into().ok()?);
    Some((range, key[9]))
}

/// Which keys a range owns and which replicas hold it (`replicas` are
/// replica ids; see [`node_of`]). `end` empty means +inf.
///
/// `next_incarnation` exceeds the incarnation of every replica ever added
/// to the range, like CockroachDB's NextReplicaID: a replica not listed
/// whose incarnation is below it was removed, for certain; one at or above
/// it may be joining and must be left alone.
/// The generation increases with every split and membership change, so the
/// newest of two descriptors for a range is unambiguous.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RangeDescriptor {
    pub id: RangeId,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub replicas: Vec<ReplicaId>,
    pub generation: u64,
    pub next_incarnation: u64,
}

impl RangeDescriptor {
    /// Whether `replica` was a member of this range and has been removed.
    pub fn has_removed(&self, replica: ReplicaId) -> bool {
        !self.replicas.contains(&replica) && incarnation_of(replica) < self.next_incarnation
    }

    /// Adopt a new set of voters from a membership change.
    pub fn set_replicas(&mut self, replicas: Vec<ReplicaId>) {
        let top = replicas
            .iter()
            .map(|&r| incarnation_of(r) + 1)
            .max()
            .unwrap_or(0);
        self.next_incarnation = self.next_incarnation.max(top);
        self.replicas = replicas;
        self.generation += 1;
    }

    /// This node's replica in the range, if it has one.
    pub fn replica_on(&self, node: NodeId) -> Option<ReplicaId> {
        self.replicas.iter().copied().find(|&r| node_of(r) == node)
    }

    pub fn nodes(&self) -> Vec<NodeId> {
        self.replicas.iter().map(|&r| node_of(r)).collect()
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && (self.end.is_empty() || key < self.end.as_slice())
    }

    pub fn overlaps(&self, other: &RangeDescriptor) -> bool {
        let starts_before_other_ends = other.end.is_empty() || self.start < other.end;
        let ends_after_other_starts = self.end.is_empty() || self.end > other.start;
        starts_before_other_ends && ends_after_other_starts
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        put_u64(out, self.id);
        put_bytes(out, &self.start);
        put_bytes(out, &self.end);
        put_u32(out, self.replicas.len() as u32);
        for r in &self.replicas {
            put_u64(out, *r);
        }
        put_u64(out, self.generation);
        put_u64(out, self.next_incarnation);
    }

    pub fn decode_from(r: &mut Reader) -> Option<RangeDescriptor> {
        let id = r.u64()?;
        let start = r.bytes()?;
        let end = r.bytes()?;
        let n = r.u32()?;
        let replicas = (0..n).map(|_| r.u64()).collect::<Option<Vec<_>>>()?;
        let generation = r.u64()?;
        let next_incarnation = r.u64()?;
        Some(RangeDescriptor {
            id,
            start,
            end,
            replicas,
            generation,
            next_incarnation,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedState {
    pub index: u64,
    pub term: u64,
    pub desc: RangeDescriptor,
}

pub fn encode_applied(a: &AppliedState) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, a.index);
    put_u64(&mut out, a.term);
    a.desc.encode_into(&mut out);
    out
}

pub fn decode_applied(data: &[u8]) -> Option<AppliedState> {
    let mut r = Reader::new(data);
    let a = AppliedState {
        index: r.u64()?,
        term: r.u64()?,
        desc: RangeDescriptor::decode_from(&mut r)?,
    };
    r.is_empty().then_some(a)
}

pub fn encode_u64(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

pub fn decode_u64(data: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(data.try_into().ok()?))
}

pub fn encode_hard_state(h: &HardState) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, h.term);
    put_u64(&mut out, h.vote.unwrap_or(0));
    put_u64(&mut out, h.commit);
    out
}

pub fn decode_hard_state(data: &[u8]) -> Option<HardState> {
    let mut r = Reader::new(data);
    let term = r.u64()?;
    let vote = r.u64()?;
    let commit = r.u64()?;
    Some(HardState {
        term,
        vote: (vote != 0).then_some(vote),
        commit,
    })
}

pub fn encode_snapshot_meta(m: &SnapshotMeta) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, m.index);
    put_u64(&mut out, m.term);
    put_u32(&mut out, m.voters.len() as u32);
    for v in &m.voters {
        put_u64(&mut out, *v);
    }
    out
}

pub fn decode_snapshot_meta(r: &mut Reader) -> Option<SnapshotMeta> {
    let index = r.u64()?;
    let term = r.u64()?;
    let n = r.u32()?;
    let voters = (0..n).map(|_| r.u64()).collect::<Option<Vec<_>>>()?;
    Some(SnapshotMeta {
        index,
        term,
        voters,
    })
}

pub fn encode_entry(e: &Entry) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, e.index);
    put_u64(&mut out, e.term);
    match &e.data {
        EntryData::Empty => out.push(0),
        EntryData::Command(c) => {
            out.push(1);
            put_bytes(&mut out, c);
        }
        EntryData::Config(v) => {
            out.push(2);
            put_u32(&mut out, v.len() as u32);
            for n in v {
                put_u64(&mut out, *n);
            }
        }
    }
    out
}

pub fn decode_entry(data: &[u8]) -> Option<Entry> {
    let mut r = Reader::new(data);
    let index = r.u64()?;
    let term = r.u64()?;
    let data = match r.u8()? {
        0 => EntryData::Empty,
        1 => EntryData::Command(r.bytes()?),
        2 => {
            let n = r.u32()?;
            EntryData::Config((0..n).map(|_| r.u64()).collect::<Option<Vec<_>>>()?)
        }
        _ => return None,
    };
    r.is_empty().then_some(Entry { index, term, data })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_keys_sort_by_range_then_kind_then_index() {
        let mut keys = vec![
            log_key(2, 10),
            applied_key(2),
            log_key(2, 9),
            hard_state_key(1),
            log_key(1, 300),
            truncated_key(2),
        ];
        keys.sort();
        assert_eq!(
            keys,
            vec![
                hard_state_key(1),
                log_key(1, 300),
                applied_key(2),
                log_key(2, 9),
                log_key(2, 10),
                truncated_key(2),
            ]
        );
        let (lo, hi) = log_span(2, 10);
        assert!(log_key(2, 10) >= lo && log_key(2, u64::MAX) < hi && truncated_key(2) >= hi);
    }

    #[test]
    fn descriptors_and_entries_round_trip() {
        let d = RangeDescriptor {
            id: 7,
            start: b"b".to_vec(),
            end: Vec::new(),
            replicas: vec![1, 2, 3],
            generation: 4,
            next_incarnation: 1,
        };
        let a = AppliedState {
            index: 9,
            term: 3,
            desc: d.clone(),
        };
        assert_eq!(decode_applied(&encode_applied(&a)), Some(a));
        assert!(d.contains(b"zzz") && !d.contains(b"a"));
        for data in [
            EntryData::Empty,
            EntryData::Command(b"x".to_vec()),
            EntryData::Config(vec![1, 5]),
        ] {
            let e = Entry {
                index: 3,
                term: 2,
                data,
            };
            assert_eq!(decode_entry(&encode_entry(&e)), Some(e));
        }
    }
}
