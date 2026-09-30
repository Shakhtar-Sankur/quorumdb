//! How a node lays out everything in its one storage engine.
//!
//! ```text
//! d <escaped user key> 00 01 r                  raw value (the plain KV interface)
//! d <escaped user key> 00 01 l                  MVCC lock of an in-flight transaction
//! d <escaped user key> 00 01 w <!commit_ts BE>  MVCC write record, newest first
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

// ─── User data ─────────────────────────────────────────────────────────
//
// A user key is escaped (0x00 becomes 0x00 0xFF) and terminated by
// 0x00 0x01, which keeps every version of every key in user-key order and
// lets a range's span be expressed on escaped prefixes alone.

pub const CF_RAW: u8 = b'r';
pub const CF_LOCK: u8 = b'l';
pub const CF_WRITE: u8 = b'w';

fn escape_into(out: &mut Vec<u8>, key: &[u8]) {
    for &b in key {
        if b == 0 {
            out.extend_from_slice(&[0, 0xFF]);
        } else {
            out.push(b);
        }
    }
}

fn key_prefix(user_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(user_key.len() + 4);
    k.push(DATA);
    escape_into(&mut k, user_key);
    k.extend_from_slice(&[0, 1]);
    k
}

/// The engine key of a user key's raw (non-transactional) value.
pub fn raw_key(user_key: &[u8]) -> Vec<u8> {
    let mut k = key_prefix(user_key);
    k.push(CF_RAW);
    k
}

pub fn lock_key(user_key: &[u8]) -> Vec<u8> {
    let mut k = key_prefix(user_key);
    k.push(CF_LOCK);
    k
}

/// Write records sort newest first: the timestamp is stored inverted.
pub fn write_key(user_key: &[u8], commit_ts: u64) -> Vec<u8> {
    let mut k = key_prefix(user_key);
    k.push(CF_WRITE);
    k.extend_from_slice(&(!commit_ts).to_be_bytes());
    k
}

/// The bloom prefix of every write record of `user_key`: the engine key
/// without its timestamp.
pub fn write_prefix(user_key: &[u8]) -> Vec<u8> {
    let mut k = key_prefix(user_key);
    k.push(CF_WRITE);
    k
}

/// The engine's bloom prefix extractor: a write record's key minus its
/// timestamp, so reading a key's versions skips tables that have none.
pub fn bloom_prefix_len(key: &[u8]) -> Option<usize> {
    if key.first() != Some(&DATA) {
        return None;
    }
    let mut i = 1;
    loop {
        match (key.get(i)?, key.get(i + 1)?) {
            (0, 1) => break,
            (0, _) => i += 2,
            _ => i += 1,
        }
    }
    let cf = i + 2;
    (key.get(cf) == Some(&CF_WRITE) && key.len() == cf + 1 + 8).then_some(cf + 1)
}

/// The span of every write record of `user_key` at or below `ts`.
pub fn writes_at_or_below(user_key: &[u8], ts: u64) -> (Vec<u8>, Vec<u8>) {
    let lo = write_key(user_key, ts);
    let mut hi = key_prefix(user_key);
    hi.push(CF_WRITE + 1);
    (lo, hi)
}

/// Split an engine data key into `(user key, column family, timestamp)`.
pub fn decode_data_key(key: &[u8]) -> Option<(Vec<u8>, u8, Option<u64>)> {
    if key.first() != Some(&DATA) {
        return None;
    }
    let mut user = Vec::new();
    let mut i = 1;
    loop {
        match (key.get(i)?, key.get(i + 1)) {
            (0, Some(0xFF)) => {
                user.push(0);
                i += 2;
            }
            (0, Some(1)) => {
                i += 2;
                break;
            }
            (0, _) => return None,
            (&b, _) => {
                user.push(b);
                i += 1;
            }
        }
    }
    let cf = *key.get(i)?;
    let ts = match key.get(i + 1..) {
        Some(rest) if rest.len() == 8 => Some(!u64::from_be_bytes(rest.try_into().ok()?)),
        _ => None,
    };
    Some((user, cf, ts))
}

/// The user key of an engine data key.
pub fn user_key(data_key: &[u8]) -> Vec<u8> {
    decode_data_key(data_key)
        .map(|(k, _, _)| k)
        .unwrap_or_default()
}

/// The storage-engine span holding every version of every user key in
/// `[start, end)` (`end` empty: unbounded).
pub fn data_span(start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut lo = vec![DATA];
    escape_into(&mut lo, start);
    let hi = if end.is_empty() {
        vec![DATA + 1]
    } else {
        let mut hi = vec![DATA];
        escape_into(&mut hi, end);
        hi
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
    fn bloom_prefix_is_a_write_key_without_its_timestamp() {
        for user in [&b"k"[..], b"", b"a\x00b", b"\x00\x01"] {
            let prefix = write_prefix(user);
            let key = write_key(user, 42);
            assert_eq!(bloom_prefix_len(&key), Some(prefix.len()));
            assert!(key.starts_with(&prefix));
            let (lo, hi) = writes_at_or_below(user, 99);
            assert!(lo.starts_with(&prefix) && hi > key);
            assert_eq!(bloom_prefix_len(&lock_key(user)), None);
            assert_eq!(bloom_prefix_len(&raw_key(user)), None);
        }
        assert_eq!(bloom_prefix_len(b"x"), None);
    }

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
    fn data_keys_keep_user_key_order_and_round_trip() {
        let users: Vec<&[u8]> = vec![
            b"",
            b"\x00",
            b"\x00\x00",
            b"\x00\x01",
            b"a",
            b"a\x00",
            b"ab",
            b"b",
        ];
        // Every engine key of a smaller user key sorts before every engine
        // key of a larger one.
        for w in users.windows(2) {
            let top = [raw_key(w[0]), lock_key(w[0]), write_key(w[0], 0)]
                .into_iter()
                .max()
                .unwrap();
            let bottom = [raw_key(w[1]), lock_key(w[1]), write_key(w[1], u64::MAX)]
                .into_iter()
                .min()
                .unwrap();
            assert!(top < bottom, "{:?} vs {:?}", w[0], w[1]);
        }
        for u in &users {
            assert_eq!(
                decode_data_key(&write_key(u, 42)),
                Some((u.to_vec(), CF_WRITE, Some(42)))
            );
            assert_eq!(
                decode_data_key(&lock_key(u)),
                Some((u.to_vec(), CF_LOCK, None))
            );
            let (lo, hi) = data_span(u, b"b");
            if *u < &b"b"[..] {
                assert!(raw_key(u) >= lo && raw_key(u) < hi);
            }
        }
        // Newer write records sort first.
        assert!(write_key(b"k", 9) < write_key(b"k", 8));
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
