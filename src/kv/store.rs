//! A store is one node's share of the cluster: one storage engine holding
//! many replicas, each a member of one range's Raft group.
//!
//! Each call to [`Store::process`] drains every replica's `Ready` and:
//! 1. persists all of their hard states, log entries and snapshots in one
//!    atomic batch and **one sync** (group commit across ranges);
//! 2. only then releases their outgoing messages;
//! 3. applies committed commands, each range's data writes and its new
//!    applied index in one batch, so the state machine is always exactly
//!    the log applied up to that index;
//! 4. answers linearizable reads, whose index is now applied;
//! 5. builds snapshots for lagging followers and compacts logs.
//!
//! Replicas are identified by replica id, not node id (see [`keys`]): a
//! message for a newer replica id than the one held means the old one was
//! removed from the range, and it is destroyed; a message for an older one
//! is dropped. Destroyed replicas leave a tombstone, so they never return.

use std::collections::BTreeMap;

use crate::codec::{Reader, put_bytes, put_u32};
use crate::error::{Error, Result};
use crate::kv::cmd::{Command, KvError, ReqId, Request, Response};
use crate::kv::keys::{self, AppliedState, RangeDescriptor, RangeId, ReplicaId};
use crate::raft::{
    self, Entry, EntryData, HardState, Msg, NodeId, ProposeError, Raft, Ready, Snapshot,
    SnapshotMeta,
};
use crate::storage::engine::{Db, Options, SyncMode};
use crate::storage::fs::Fs;
use crate::storage::wal::Op;

/// A range's log starts here, so every replica, including ones created by
/// a split, begins from the same synthetic snapshot.
pub const INIT_INDEX: u64 = 5;
pub const INIT_TERM: u64 = 5;

/// Deliberately broken behaviour, to prove the cluster simulator catches it.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreFault {
    None,
    /// Do not sync Raft state before sending messages that promise it
    /// (votes, appended entries): a crash then breaks those promises.
    SkipRaftSync,
    /// Serve reads from the local state without ReadIndex.
    StaleLocalReads,
    /// Treat a snapshot that does not list this replica as its removal: a
    /// bug the simulator found. The snapshot can predate our own addition.
    SnapshotSelfRemoval,
}

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub raft: raft::Config,
    /// Compact a replica's log once this many entries are applied beyond it.
    pub compact_after: u64,
    pub engine: Options,
    pub fault: StoreFault,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            raft: raft::Config::default(),
            compact_after: 128,
            engine: Options {
                sync: SyncMode::Manual,
                ..Options::default()
            },
            fault: StoreFault::None,
        }
    }
}

/// A Raft message for one range's group. `msg.from` and `msg.to` are
/// replica ids; the destination node is `keys::node_of(msg.to)`.
#[derive(Clone, Debug)]
pub struct RangeMessage {
    pub range: RangeId,
    pub msg: raft::Message,
}

/// What a store tells the placement driver about one of its replicas.
#[derive(Clone, Debug)]
pub struct ReplicaReport {
    pub desc: RangeDescriptor,
    pub replica: ReplicaId,
    pub leader: bool,
    pub applied: u64,
    /// Live keys in the range; leaders only (zero elsewhere).
    pub keys: u64,
    /// The middle key, a good place to split; leaders only.
    pub mid_key: Option<Vec<u8>>,
}

struct Replica {
    /// `None` until the replica receives a snapshot: it knows nothing yet.
    desc: Option<RangeDescriptor>,
    raft: Raft,
    applied: u64,
    /// The last log index on disk, to delete a longer stale tail.
    persisted_last: u64,
    /// Proposals made here: log index -> (term, request).
    proposals: BTreeMap<u64, (u64, ReqId)>,
    /// Reads waiting for Raft to confirm leadership: read id -> request.
    reads: BTreeMap<u64, (ReqId, Request)>,
    /// Reads confirmed at a read index, waiting for it to be applied.
    ready_reads: Vec<(u64, ReqId, Request)>,
    /// Removed from the range: destroy after this round.
    removed: bool,
}

pub struct Store<F: Fs> {
    pub id: NodeId,
    db: Db<F>,
    cfg: StoreConfig,
    replicas: BTreeMap<RangeId, Replica>,
    /// Per range, the lowest replica id that may still exist here.
    tombstones: BTreeMap<RangeId, ReplicaId>,
    outbox: Vec<(NodeId, RangeMessage)>,
    responses: Vec<(ReqId, std::result::Result<Response, KvError>)>,
    next_read: u64,
    seed: u64,
}

fn snapshot_data(desc: &RangeDescriptor, pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    desc.encode_into(&mut out);
    put_u32(&mut out, pairs.len() as u32);
    for (k, v) in pairs {
        put_bytes(&mut out, k);
        put_bytes(&mut out, v);
    }
    out
}

type SnapshotContents = (RangeDescriptor, Vec<(Vec<u8>, Vec<u8>)>);

fn decode_snapshot_data(data: &[u8]) -> Option<SnapshotContents> {
    let mut r = Reader::new(data);
    let desc = RangeDescriptor::decode_from(&mut r)?;
    let n = r.u32()?;
    let mut pairs = Vec::with_capacity(n.min(1 << 20) as usize);
    for _ in 0..n {
        pairs.push((r.bytes()?, r.bytes()?));
    }
    r.is_empty().then_some((desc, pairs))
}

fn corrupt(what: &str) -> Error {
    Error::Corrupt(format!("store: {what}"))
}

fn propose_error(e: ProposeError) -> KvError {
    match e {
        ProposeError::NotLeader(hint) => KvError::NotLeader(hint.map(keys::node_of)),
        ProposeError::Busy => KvError::Busy,
    }
}

impl<F: Fs> Store<F> {
    /// Open (or recover) the store in `fs`, rebuilding every replica from
    /// its persisted Raft state and applied state.
    pub fn open(fs: F, id: NodeId, cfg: StoreConfig, seed: u64) -> Result<Store<F>> {
        let db = Db::open(fs, cfg.engine.clone())?;
        let mut store = Store {
            id,
            db,
            cfg,
            replicas: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            outbox: Vec::new(),
            responses: Vec::new(),
            next_read: 0,
            seed,
        };
        store.load()?;
        Ok(store)
    }

    fn load(&mut self) -> Result<()> {
        #[derive(Default)]
        struct Found {
            replica: Option<ReplicaId>,
            hard: Option<HardState>,
            truncated: Option<SnapshotMeta>,
            applied: Option<AppliedState>,
            entries: Vec<Entry>,
        }
        let mut found: BTreeMap<RangeId, Found> = BTreeMap::new();
        for row in self.db.range(&[keys::LOCAL], Some(&[keys::LOCAL + 1])) {
            let (k, v) = row?;
            let (range, kind) = keys::parse_local(&k).ok_or_else(|| corrupt("bad local key"))?;
            let f = found.entry(range).or_default();
            match kind {
                b'i' => {
                    f.replica = Some(keys::decode_u64(&v).ok_or_else(|| corrupt("replica id"))?)
                }
                b'x' => {
                    let t = keys::decode_u64(&v).ok_or_else(|| corrupt("tombstone"))?;
                    self.tombstones.insert(range, t);
                }
                b'h' => {
                    f.hard = Some(keys::decode_hard_state(&v).ok_or_else(|| corrupt("hard state"))?)
                }
                b't' => {
                    f.truncated = Some(
                        keys::decode_snapshot_meta(&mut Reader::new(&v))
                            .ok_or_else(|| corrupt("truncated state"))?,
                    )
                }
                b'a' => {
                    f.applied =
                        Some(keys::decode_applied(&v).ok_or_else(|| corrupt("applied state"))?)
                }
                b'l' => f
                    .entries
                    .push(keys::decode_entry(&v).ok_or_else(|| corrupt("log entry"))?),
                _ => return Err(corrupt("unknown local key kind")),
            }
        }
        for (range, f) in found {
            let Some(rid) = f.replica else {
                continue; // only a tombstone
            };
            let hard = f.hard.unwrap_or_default();
            let replica = match (f.applied, f.truncated) {
                (Some(applied), Some(snap)) => {
                    let entries: Vec<Entry> = f
                        .entries
                        .into_iter()
                        .filter(|e| e.index > snap.index)
                        .collect();
                    let raft = Raft::restore(
                        rid,
                        self.cfg.raft.clone(),
                        self.seed ^ rid ^ range,
                        hard,
                        snap,
                        entries,
                        applied.index,
                    );
                    let last = raft.last_index();
                    let mut r = Replica::new(raft, Some(applied.desc.clone()), applied.index, last);
                    // It applied its own removal but crashed before cleanup.
                    r.removed = applied.desc.has_removed(rid);
                    r
                }
                _ => {
                    let raft = Raft::restore(
                        rid,
                        self.cfg.raft.clone(),
                        self.seed ^ rid ^ range,
                        hard,
                        SnapshotMeta::default(),
                        Vec::new(),
                        0,
                    );
                    Replica::new(raft, None, 0, 0)
                }
            };
            self.replicas.insert(range, replica);
        }
        self.destroy_removed()
    }

    /// Create the first replica of a range on this store, at the synthetic
    /// initial snapshot. Every initial member must be bootstrapped alike.
    pub fn bootstrap(&mut self, desc: RangeDescriptor) -> Result<()> {
        let rid = desc
            .replica_on(self.id)
            .ok_or_else(|| corrupt("bootstrap: not a member"))?;
        let ops = init_range_ops(&desc, rid, HardState::default());
        self.db.write_batch(ops)?;
        self.db.sync()?;
        let raft = self.init_raft(&desc, rid, HardState::default());
        self.replicas.insert(
            desc.id,
            Replica::new(raft, Some(desc), INIT_INDEX, INIT_INDEX),
        );
        Ok(())
    }

    fn init_raft(&self, desc: &RangeDescriptor, rid: ReplicaId, prior: HardState) -> Raft {
        Raft::restore(
            rid,
            self.cfg.raft.clone(),
            self.seed ^ rid ^ desc.id,
            init_hard_state(prior),
            SnapshotMeta {
                index: INIT_INDEX,
                term: INIT_TERM,
                voters: desc.replicas.clone(),
            },
            Vec::new(),
            INIT_INDEX,
        )
    }

    // ─── Inputs ──────────────────────────────────────────────────────────

    pub fn tick(&mut self) {
        for r in self.replicas.values_mut() {
            r.raft.tick();
        }
    }

    /// Deliver a Raft message from a peer.
    pub fn step(&mut self, m: RangeMessage) -> Result<()> {
        let to = m.msg.to;
        if keys::node_of(to) != self.id || to < self.tombstones.get(&m.range).copied().unwrap_or(0)
        {
            return Ok(());
        }
        if let Msg::Snapshot(snap) = &m.msg.msg {
            // Refuse a snapshot whose span overlaps another replica here:
            // that replica must split or be removed first.
            let Some((desc, _)) = decode_snapshot_data(&snap.data) else {
                return Ok(());
            };
            let overlaps = self.replicas.iter().any(|(&id, r)| {
                id != m.range && r.desc.as_ref().is_some_and(|d| d.overlaps(&desc))
            });
            if overlaps {
                return Ok(());
            }
        }
        match self.replicas.get(&m.range).map(|r| r.raft.id) {
            Some(held) if held > to => return Ok(()), // for a dead, older incarnation
            Some(held) if held < to => {
                // We were removed and re-added: the old incarnation is dead.
                self.destroy(m.range, to)?;
                self.create_uninitialized(m.range, to);
            }
            Some(_) => {}
            None => self.create_uninitialized(m.range, to),
        }
        self.replicas
            .get_mut(&m.range)
            .expect("present")
            .raft
            .step(m.msg);
        Ok(())
    }

    fn create_uninitialized(&mut self, range: RangeId, rid: ReplicaId) {
        let raft = Raft::new_empty(rid, self.cfg.raft.clone(), self.seed ^ rid ^ range);
        self.replicas.insert(range, Replica::new(raft, None, 0, 0));
    }

    /// Submit a client request to a range's replica here. The answer comes
    /// back from [`Store::take_responses`], possibly in a later round.
    pub fn submit(&mut self, range: RangeId, req_id: ReqId, req: Request) {
        if let Err(e) = self.try_submit(range, req_id, req) {
            self.responses.push((req_id, Err(e)));
        }
    }

    fn try_submit(
        &mut self,
        range: RangeId,
        req_id: ReqId,
        req: Request,
    ) -> std::result::Result<(), KvError> {
        let fault = self.cfg.fault;
        let replica = self
            .replicas
            .get_mut(&range)
            .ok_or(KvError::RangeNotFound)?;
        let desc = replica.desc.clone().ok_or(KvError::RangeNotFound)?;
        if !replica.raft.is_leader() {
            return Err(KvError::NotLeader(replica.raft.leader().map(keys::node_of)));
        }
        let key = match &req {
            Request::Get { key } | Request::Put { key, .. } | Request::Delete { key } => Some(key),
            Request::Scan { start, .. } => Some(start),
            _ => None,
        };
        if key.is_some_and(|k| !desc.contains(k)) {
            return Err(KvError::KeyNotInRange);
        }
        match req {
            Request::Get { .. } | Request::Scan { .. } => {
                if fault == StoreFault::StaleLocalReads {
                    replica.ready_reads.push((0, req_id, req));
                    return Ok(());
                }
                self.next_read += 1;
                let read_id = self.next_read;
                let replica = self.replicas.get_mut(&range).expect("present");
                replica.raft.read_index(read_id).map_err(propose_error)?;
                replica.reads.insert(read_id, (req_id, req));
            }
            Request::Put { key, value } => {
                self.propose(range, req_id, Command::Put { key, value })?
            }
            Request::Delete { key } => self.propose(range, req_id, Command::Delete { key })?,
            Request::Split { key, new_range } => {
                if key <= desc.start || !desc.contains(&key) {
                    return Err(KvError::KeyNotInRange);
                }
                let cmd = Command::Split {
                    key,
                    new_range,
                    generation: desc.generation,
                };
                self.propose(range, req_id, cmd)?;
            }
            Request::ChangeReplicas { replicas } => {
                let (index, term) = replica
                    .raft
                    .propose_config(replicas)
                    .map_err(propose_error)?;
                replica.proposals.insert(index, (term, req_id));
            }
            Request::TransferLeader { to } => {
                if let Some(rid) = desc.replica_on(to) {
                    replica.raft.transfer_leader(rid);
                }
                self.responses.push((req_id, Ok(Response::Done)));
            }
        }
        Ok(())
    }

    fn propose(
        &mut self,
        range: RangeId,
        req_id: ReqId,
        cmd: Command,
    ) -> std::result::Result<(), KvError> {
        let replica = self.replicas.get_mut(&range).expect("present");
        let (index, term) = replica.raft.propose(cmd.encode()).map_err(propose_error)?;
        replica.proposals.insert(index, (term, req_id));
        Ok(())
    }

    // ─── Outputs ─────────────────────────────────────────────────────────

    /// Messages to send, addressed by destination node.
    pub fn take_messages(&mut self) -> Vec<(NodeId, RangeMessage)> {
        std::mem::take(&mut self.outbox)
    }

    pub fn take_responses(&mut self) -> Vec<(ReqId, std::result::Result<Response, KvError>)> {
        std::mem::take(&mut self.responses)
    }

    pub fn report(&self) -> Vec<ReplicaReport> {
        self.replicas
            .values()
            .filter_map(|r| {
                let desc = r.desc.clone()?;
                let leader = r.raft.is_leader();
                let (keys, mid_key) = if leader {
                    let (lo, hi) = keys::data_span(&desc.start, &desc.end);
                    let all: Vec<Vec<u8>> = self
                        .db
                        .range(&lo, Some(&hi))
                        .filter_map(|r| r.ok().map(|(k, _)| k))
                        .collect();
                    let mid = all.get(all.len() / 2).map(|k| keys::user_key(k).to_vec());
                    (all.len() as u64, mid)
                } else {
                    (0, None)
                };
                Some(ReplicaReport {
                    desc,
                    replica: r.raft.id,
                    leader,
                    applied: r.applied,
                    keys,
                    mid_key,
                })
            })
            .collect()
    }

    pub fn has_replica(&self, range: RangeId) -> bool {
        self.replicas.get(&range).is_some_and(|r| r.desc.is_some())
    }

    /// Destroy replica `rid` of `range`, which the placement driver says
    /// its range removed. A replica id is never reused, so this is safe
    /// even if the node has since been re-added under a newer id: then the
    /// ids differ and nothing happens.
    pub fn gc_replica(&mut self, range: RangeId, rid: ReplicaId) -> Result<()> {
        if self.replicas.get(&range).map(|r| r.raft.id) == Some(rid) {
            self.destroy(range, rid + 1)?;
        }
        Ok(())
    }

    // ─── The processing loop ─────────────────────────────────────────────

    pub fn process(&mut self) -> Result<()> {
        loop {
            let ids: Vec<RangeId> = self
                .replicas
                .iter()
                .filter(|(_, r)| r.raft.has_ready())
                .map(|(&id, _)| id)
                .collect();
            if ids.is_empty() {
                break;
            }
            let readies: Vec<(RangeId, Ready)> = ids
                .into_iter()
                .map(|id| {
                    (
                        id,
                        self.replicas.get_mut(&id).expect("present").raft.ready(),
                    )
                })
                .collect();
            self.handle_readies(readies)?;
        }
        self.serve_reads()?;
        self.compact_logs()?;
        self.destroy_removed()?;
        Ok(())
    }

    fn handle_readies(&mut self, readies: Vec<(RangeId, Ready)>) -> Result<()> {
        // 1. Persist every replica's Raft state in one batch, one sync.
        let mut ops = Vec::new();
        let mut installed: Vec<(RangeId, RangeDescriptor, SnapshotMeta)> = Vec::new();
        for (range, ready) in &readies {
            let replica = self.replicas.get_mut(range).expect("present");
            let durable =
                ready.hard_state.is_some() || ready.snapshot.is_some() || !ready.entries.is_empty();
            if durable {
                ops.push(Op::Put(
                    keys::replica_id_key(*range),
                    keys::encode_u64(replica.raft.id),
                ));
            }
            if let Some(hs) = &ready.hard_state {
                ops.push(Op::Put(
                    keys::hard_state_key(*range),
                    keys::encode_hard_state(hs),
                ));
            }
            if let Some(snap) = &ready.snapshot {
                let (desc, pairs) =
                    decode_snapshot_data(&snap.data).ok_or_else(|| corrupt("snapshot"))?;
                // Clear what this replica held before, and its whole log.
                for span in [replica.desc.as_ref(), Some(&desc)].into_iter().flatten() {
                    let (lo, hi) = keys::data_span(&span.start, &span.end);
                    for row in self.db.range(&lo, Some(&hi)) {
                        ops.push(Op::Delete(row?.0));
                    }
                }
                let (lo, hi) = keys::log_span(*range, 0);
                for row in self.db.range(&lo, Some(&hi)) {
                    ops.push(Op::Delete(row?.0));
                }
                for (k, v) in pairs {
                    ops.push(Op::Put(keys::data_key(&k), v));
                }
                let applied = AppliedState {
                    index: snap.meta.index,
                    term: snap.meta.term,
                    desc: desc.clone(),
                };
                ops.push(Op::Put(
                    keys::applied_key(*range),
                    keys::encode_applied(&applied),
                ));
                ops.push(Op::Put(
                    keys::truncated_key(*range),
                    keys::encode_snapshot_meta(&snap.meta),
                ));
                replica.persisted_last = snap.meta.index;
                installed.push((*range, desc, snap.meta.clone()));
            }
            if let Some(last) = ready.entries.last() {
                for e in &ready.entries {
                    ops.push(Op::Put(
                        keys::log_key(*range, e.index),
                        keys::encode_entry(e),
                    ));
                }
                for stale in last.index + 1..=replica.persisted_last {
                    ops.push(Op::Delete(keys::log_key(*range, stale)));
                }
                replica.persisted_last = last.index;
            }
        }
        self.db.write_batch(ops)?;
        if self.cfg.fault != StoreFault::SkipRaftSync {
            self.db.sync()?;
        }

        // 2. Now that it is durable, release the messages.
        for (range, ready) in &readies {
            for m in &ready.messages {
                self.outbox.push((
                    keys::node_of(m.to),
                    RangeMessage {
                        range: *range,
                        msg: m.clone(),
                    },
                ));
            }
        }
        for (range, desc, meta) in installed {
            let replica = self.replicas.get_mut(&range).expect("present");
            // A snapshot can predate our own addition: only a descriptor
            // that has already added and then removed us means we are gone.
            replica.removed = if self.cfg.fault == StoreFault::SnapshotSelfRemoval {
                !desc.replicas.contains(&replica.raft.id)
            } else {
                desc.has_removed(replica.raft.id)
            };
            replica.desc = Some(desc);
            replica.applied = meta.index;
        }

        // 3. Apply committed entries.
        let mut new_ranges = Vec::new();
        for (range, ready) in &readies {
            if !ready.committed.is_empty() {
                new_ranges.extend(self.apply(*range, &ready.committed)?);
            }
        }
        for (desc, rid, prior) in new_ranges {
            let raft = self.init_raft(&desc, rid, prior);
            self.replicas.insert(
                desc.id,
                Replica::new(raft, Some(desc), INIT_INDEX, INIT_INDEX),
            );
        }

        // 4. Reads confirmed by a quorum wait for their index to apply.
        let mut snapshot_requests = Vec::new();
        for (range, ready) in readies {
            let Some(replica) = self.replicas.get_mut(&range) else {
                continue;
            };
            for (read_id, index) in ready.reads {
                if let Some((req_id, req)) = replica.reads.remove(&read_id) {
                    replica.ready_reads.push((index, req_id, req));
                }
            }
            for read_id in ready.failed_reads {
                if let Some((req_id, _)) = replica.reads.remove(&read_id) {
                    self.responses.push((req_id, Err(KvError::NotLeader(None))));
                }
            }
            for peer in ready.snapshot_requests {
                snapshot_requests.push((range, peer));
            }
        }

        // 5. Snapshots for followers that fell behind the compacted log.
        for (range, peer) in snapshot_requests {
            let Some(replica) = self.replicas.get(&range) else {
                continue;
            };
            let Some(desc) = replica.desc.clone() else {
                continue;
            };
            let Some(meta) = replica.raft.snapshot_meta(replica.applied) else {
                continue;
            };
            let (lo, hi) = keys::data_span(&desc.start, &desc.end);
            let pairs = self
                .db
                .range(&lo, Some(&hi))
                .map(|row| row.map(|(k, v)| (keys::user_key(&k).to_vec(), v)))
                .collect::<Result<Vec<_>>>()?;
            let data = snapshot_data(&desc, &pairs);
            let replica = self.replicas.get_mut(&range).expect("present");
            replica.raft.send_snapshot(peer, Snapshot { meta, data });
        }
        Ok(())
    }

    /// Apply committed entries to one range. Returns ranges created by
    /// splits: descriptor, this node's replica id, and any hard state that
    /// replica already had here.
    fn apply(
        &mut self,
        range: RangeId,
        entries: &[Entry],
    ) -> Result<Vec<(RangeDescriptor, ReplicaId, HardState)>> {
        let mut ops = Vec::new();
        let mut created: Vec<RangeDescriptor> = Vec::new();
        let replica = self.replicas.get_mut(&range).expect("present");
        let rid = replica.raft.id;
        let Some(mut desc) = replica.desc.clone() else {
            return Ok(Vec::new());
        };
        let mut last_term = 0;
        for e in entries {
            last_term = e.term;
            let result: std::result::Result<Response, KvError> = match &e.data {
                EntryData::Empty => Ok(Response::Done),
                EntryData::Config(voters) => {
                    desc.set_replicas(voters.clone());
                    if !voters.contains(&rid) {
                        replica.removed = true;
                    }
                    Ok(Response::Done)
                }
                EntryData::Command(bytes) => match Command::decode(bytes) {
                    None => Err(KvError::Storage("undecodable command".into())),
                    Some(Command::Put { key, value }) => {
                        if desc.contains(&key) {
                            ops.push(Op::Put(keys::data_key(&key), value));
                            Ok(Response::Done)
                        } else {
                            Err(KvError::KeyNotInRange)
                        }
                    }
                    Some(Command::Delete { key }) => {
                        if desc.contains(&key) {
                            ops.push(Op::Delete(keys::data_key(&key)));
                            Ok(Response::Done)
                        } else {
                            Err(KvError::KeyNotInRange)
                        }
                    }
                    Some(Command::Split {
                        key,
                        new_range,
                        generation,
                    }) => {
                        if generation != desc.generation
                            || key <= desc.start
                            || !desc.contains(&key)
                        {
                            Err(KvError::Busy)
                        } else {
                            let rhs = RangeDescriptor {
                                id: new_range,
                                start: key.clone(),
                                end: std::mem::replace(&mut desc.end, key),
                                replicas: desc.replicas.clone(),
                                generation: 0,
                                next_incarnation: desc.next_incarnation,
                            };
                            desc.generation += 1;
                            created.push(rhs);
                            Ok(Response::Done)
                        }
                    }
                },
            };
            if let Some((term, req_id)) = replica.proposals.remove(&e.index) {
                let result = if term == e.term {
                    result
                } else {
                    Err(KvError::Dropped)
                };
                self.responses.push((req_id, result));
            }
        }
        let last = entries.last().expect("non-empty").index;
        replica.applied = last;
        replica.desc = Some(desc.clone());
        let removed = replica.removed;
        let applied = AppliedState {
            index: last,
            term: last_term,
            desc,
        };
        ops.push(Op::Put(
            keys::applied_key(range),
            keys::encode_applied(&applied),
        ));

        // A split creates the right-hand range here at its initial snapshot,
        // unless this node's replica of it is already initialized (added by
        // snapshot after the split) or was removed from it (a newer id, or a
        // tombstone, covers ours).
        let mut new_ranges = Vec::new();
        for rhs in created {
            if removed {
                continue;
            }
            let tomb = self.tombstones.get(&rhs.id).copied().unwrap_or(0);
            match self.replicas.get(&rhs.id) {
                _ if rid < tomb => continue,
                Some(r) if r.desc.is_some() || r.raft.id != rid => continue,
                existing => {
                    let prior = existing
                        .map(|r| r.raft.hard_state_now())
                        .unwrap_or_default();
                    ops.extend(init_range_ops(&rhs, rid, prior));
                    new_ranges.push((rhs, rid, prior));
                }
            }
        }
        self.db.write_batch(ops)?;
        Ok(new_ranges)
    }

    fn serve_reads(&mut self) -> Result<()> {
        let fault = self.cfg.fault;
        for replica in self.replicas.values_mut() {
            if replica.ready_reads.is_empty() {
                continue;
            }
            let applied = replica.applied;
            let (now, later): (Vec<_>, Vec<_>) =
                replica.ready_reads.drain(..).partition(|(index, _, _)| {
                    *index <= applied || fault == StoreFault::StaleLocalReads
                });
            replica.ready_reads = later;
            let desc = replica.desc.clone();
            for (_, req_id, req) in now {
                let result = match (&desc, req) {
                    (None, _) => Err(KvError::RangeNotFound),
                    (Some(d), Request::Get { key }) => {
                        if d.contains(&key) {
                            Ok(Response::Value(self.db.get(&keys::data_key(&key))?))
                        } else {
                            Err(KvError::KeyNotInRange)
                        }
                    }
                    (Some(d), Request::Scan { start, end, limit }) => {
                        if !d.contains(&start) {
                            Err(KvError::KeyNotInRange)
                        } else {
                            // Clamp to this range; the client continues in the next.
                            let clamped = !d.end.is_empty() && (end.is_empty() || d.end < end);
                            let stop = if clamped { d.end.clone() } else { end };
                            let (lo, hi) = keys::data_span(&start, &stop);
                            let rows = self
                                .db
                                .range(&lo, Some(&hi))
                                .take(limit)
                                .map(|row| row.map(|(k, v)| (keys::user_key(&k).to_vec(), v)))
                                .collect::<Result<Vec<_>>>()?;
                            Ok(Response::Rows {
                                rows,
                                resume: clamped.then(|| d.end.clone()),
                            })
                        }
                    }
                    _ => Err(KvError::Busy),
                };
                self.responses.push((req_id, result));
            }
        }
        Ok(())
    }

    fn compact_logs(&mut self) -> Result<()> {
        let mut ops = Vec::new();
        for (&range, replica) in self.replicas.iter_mut() {
            let applied = replica.applied;
            if replica.desc.is_none()
                || applied < replica.raft.first_index() + self.cfg.compact_after
            {
                continue;
            }
            let Some(meta) = replica.raft.snapshot_meta(applied) else {
                continue;
            };
            let first = replica.raft.first_index();
            replica.raft.compact(applied);
            ops.push(Op::Put(
                keys::truncated_key(range),
                keys::encode_snapshot_meta(&meta),
            ));
            for i in first..=applied {
                ops.push(Op::Delete(keys::log_key(range, i)));
            }
        }
        if !ops.is_empty() {
            self.db.write_batch(ops)?;
        }
        Ok(())
    }

    fn destroy_removed(&mut self) -> Result<()> {
        let removed: Vec<(RangeId, ReplicaId)> = self
            .replicas
            .iter()
            .filter(|(_, r)| r.removed)
            .map(|(&id, r)| (id, r.raft.id))
            .collect();
        for (range, rid) in removed {
            self.destroy(range, rid + 1)?;
        }
        Ok(())
    }

    /// Delete a replica's data and Raft state, leaving a tombstone that
    /// rejects every replica id below `tombstone` from now on.
    fn destroy(&mut self, range: RangeId, tombstone: ReplicaId) -> Result<()> {
        let Some(replica) = self.replicas.remove(&range) else {
            return Ok(());
        };
        let mut ops = Vec::new();
        if let Some(desc) = &replica.desc {
            let (lo, hi) = keys::data_span(&desc.start, &desc.end);
            for row in self.db.range(&lo, Some(&hi)) {
                ops.push(Op::Delete(row?.0));
            }
        }
        let (lo, hi) = keys::local_span(range);
        for row in self.db.range(&lo, Some(&hi)) {
            ops.push(Op::Delete(row?.0));
        }
        let tombstone = tombstone.max(self.tombstones.get(&range).copied().unwrap_or(0));
        ops.push(Op::Put(
            keys::tombstone_key(range),
            keys::encode_u64(tombstone),
        ));
        self.tombstones.insert(range, tombstone);
        self.db.write_batch(ops)?;
        self.db.sync()?;
        // In-flight proposals may still commit under another leader.
        for (_, (_, req_id)) in replica.proposals {
            self.responses.push((req_id, Err(KvError::Ambiguous)));
        }
        for (_, (req_id, _)) in replica.reads {
            self.responses.push((req_id, Err(KvError::RangeNotFound)));
        }
        for (_, req_id, _) in replica.ready_reads {
            self.responses.push((req_id, Err(KvError::RangeNotFound)));
        }
        Ok(())
    }

    /// Diagnostics: `(range, replica id, descriptor, is leader, term, applied)`.
    pub fn describe(&self) -> Vec<(RangeId, ReplicaId, Option<RangeDescriptor>, bool, u64, u64)> {
        self.replicas
            .iter()
            .map(|(&id, r)| {
                (
                    id,
                    r.raft.id,
                    r.desc.clone(),
                    r.raft.is_leader(),
                    r.raft.term(),
                    r.applied,
                )
            })
            .collect()
    }

    /// `(applied index, checksum of the range's data)` for a replica, so the
    /// simulator can check that replicas at the same index agree exactly.
    pub fn range_digest(&self, range: RangeId) -> Result<Option<(u64, u32, u64)>> {
        let Some(r) = self.replicas.get(&range) else {
            return Ok(None);
        };
        let Some(desc) = &r.desc else {
            return Ok(None);
        };
        let (lo, hi) = keys::data_span(&desc.start, &desc.end);
        let mut buf = Vec::new();
        let mut rows = 0;
        for row in self.db.range(&lo, Some(&hi)) {
            let (k, v) = row?;
            put_bytes(&mut buf, &k);
            put_bytes(&mut buf, &v);
            rows += 1;
        }
        Ok(Some((r.applied, crate::crc::crc32(&buf), rows)))
    }

    /// One replica's Raft view, for failure reports.
    pub fn debug_replica(&self, range: RangeId) -> String {
        match self.replicas.get(&range) {
            None => format!(
                "no replica (tombstone {:?})",
                self.tombstones.get(&range).map(|t| format!("{t:x}"))
            ),
            Some(r) => format!(
                "{:?} term {} voters {:x?} leader {:?} last {} commit {} applied {} tombstone {:?}",
                r.raft.role(),
                r.raft.term(),
                r.raft.voters(),
                r.raft.leader().map(|l| format!("{l:x}")),
                r.raft.last_index(),
                r.raft.commit_index(),
                r.applied,
                self.tombstones.get(&range).map(|t| format!("{t:x}"))
            ),
        }
    }

    /// Read a user key straight from this store's engine (for checks).
    pub fn local_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(&keys::data_key(key))
    }
}

impl Replica {
    fn new(
        raft: Raft,
        desc: Option<RangeDescriptor>,
        applied: u64,
        persisted_last: u64,
    ) -> Replica {
        Replica {
            desc,
            raft,
            applied,
            persisted_last,
            proposals: BTreeMap::new(),
            reads: BTreeMap::new(),
            ready_reads: Vec::new(),
            removed: false,
        }
    }
}

/// The hard state a new range's replica starts with: at least the initial
/// term, never going back on a vote the replica already cast.
fn init_hard_state(prior: HardState) -> HardState {
    if prior.term >= INIT_TERM {
        HardState {
            term: prior.term,
            vote: prior.vote,
            commit: INIT_INDEX,
        }
    } else {
        HardState {
            term: INIT_TERM,
            vote: None,
            commit: INIT_INDEX,
        }
    }
}

fn init_range_ops(desc: &RangeDescriptor, rid: ReplicaId, prior: HardState) -> Vec<Op> {
    let meta = SnapshotMeta {
        index: INIT_INDEX,
        term: INIT_TERM,
        voters: desc.replicas.clone(),
    };
    let applied = AppliedState {
        index: INIT_INDEX,
        term: INIT_TERM,
        desc: desc.clone(),
    };
    vec![
        Op::Put(keys::replica_id_key(desc.id), keys::encode_u64(rid)),
        Op::Put(
            keys::hard_state_key(desc.id),
            keys::encode_hard_state(&init_hard_state(prior)),
        ),
        Op::Put(
            keys::truncated_key(desc.id),
            keys::encode_snapshot_meta(&meta),
        ),
        Op::Put(keys::applied_key(desc.id), keys::encode_applied(&applied)),
    ]
}
