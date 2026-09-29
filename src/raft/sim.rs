//! Deterministic simulation of a Raft cluster. One seed drives everything:
//! message delays, loss, duplication and reordering; network partitions;
//! node crashes and restarts; clock skew between nodes; membership changes;
//! log compaction and snapshots; and a client workload of writes and
//! linearizable reads.
//!
//! Checked continuously:
//! - **Election safety**: at most one leader per term.
//! - **State machine safety**: no two nodes apply different commands at the
//!   same log index.
//! - **Durability**: every write acknowledged to a client survives, and ends
//!   up applied everywhere once the network heals.
//! - **Linearizable reads**: a read observes every write acknowledged
//!   before the read began.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use super::{
    Config, Entry, EntryData, HardState, Message, NodeId, Raft, RaftFault, Snapshot, SnapshotMeta,
};
use crate::rng::Rng;

/// What a node keeps on disk. The simulator persists every `Ready` before
/// sending its messages, exactly as a real node must.
#[derive(Clone, Debug, Default)]
struct Disk {
    hard: HardState,
    snap: SnapshotMeta,
    entries: Vec<Entry>,
    /// The state machine: applied commands in order, from index 1. It is
    /// durable together with `applied`, as if written in one batch.
    state: Vec<(u64, Vec<u8>)>,
    applied: u64,
}

impl Disk {
    fn persist_entries(&mut self, entries: &[Entry]) {
        let Some(first) = entries.first() else {
            return;
        };
        self.entries.retain(|e| e.index < first.index);
        self.entries.extend_from_slice(entries);
    }
}

fn encode_state(state: &[(u64, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, cmd) in state {
        crate::codec::put_u64(&mut out, *i);
        crate::codec::put_bytes(&mut out, cmd);
    }
    out
}

fn decode_state(data: &[u8]) -> Vec<(u64, Vec<u8>)> {
    let mut r = crate::codec::Reader::new(data);
    let mut out = Vec::new();
    while !r.is_empty() {
        let i = r.u64().expect("snapshot");
        out.push((i, r.bytes().expect("snapshot")));
    }
    out
}

struct Node {
    raft: Raft,
    disk: Disk,
    up: bool,
    /// Milliseconds between ticks: clock skew between nodes.
    tick_ms: u64,
    /// Reads acknowledged by Raft, waiting for the state machine to catch up.
    reads: Vec<(u64, u64)>,
}

#[derive(Clone, Debug)]
enum Event {
    Deliver(Message),
    Tick(NodeId),
    Crash,
    Restart(NodeId),
    Partition,
    Heal,
    ClientWrite,
    ClientRead,
    Membership,
    Transfer,
}

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub seed: u64,
    pub messages: u64,
    pub dropped: u64,
    pub crashes: u64,
    pub partitions: u64,
    pub elections: u64,
    pub acked_writes: u64,
    pub reads: u64,
    pub snapshots: u64,
    pub config_changes: u64,
    pub transfers: u64,
}

struct PendingWrite {
    cmd: Vec<u8>,
    node: NodeId,
    index: u64,
    term: u64,
}

struct PendingRead {
    started: u64,
    /// Writes acknowledged before the read began: it must observe all of them.
    must_see: usize,
}

pub struct Sim {
    seed: u64,
    rng: Rng,
    now: u64,
    queue: BinaryHeap<Reverse<(u64, u64, usize)>>,
    events: Vec<Option<Event>>,
    seq: u64,
    nodes: BTreeMap<NodeId, Node>,
    cfg: Config,
    /// Connectivity: nodes in different groups cannot talk.
    group: BTreeMap<NodeId, u8>,
    drop_percent: u64,
    /// Mean milliseconds between crashes, and the longest a node stays down.
    crash_gap: u64,
    down_for: u64,
    // Checking.
    leaders: BTreeMap<u64, NodeId>,
    applied_at: BTreeMap<u64, Vec<u8>>,
    pending_writes: Vec<PendingWrite>,
    acked: Vec<Vec<u8>>,
    pending_reads: BTreeMap<u64, PendingRead>,
    next_cmd: u64,
    next_read: u64,
    report: Report,
    compact_every: u64,
}

impl Sim {
    pub fn new(seed: u64, fault: RaftFault) -> Sim {
        let mut rng = Rng::new(seed);
        let cfg = Config {
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_append_entries: 1 + rng.below(16) as usize,
            // Always on. Without pre-vote, plain Raft can livelock under
            // clock skew: a candidate that cannot win but ticks faster keeps
            // raising the term, so the node that could win always asks one
            // term too late. The simulator found exactly this (seed 4946).
            pre_vote: true,
            check_quorum: rng.chance(75),
            fault,
        };
        let n = 3 + 2 * rng.below(2); // 3 or 5
        let pool = n + 2; // spare nodes to add later
        let voters: Vec<NodeId> = (1..=n).collect();
        let mut sim = Sim {
            seed,
            now: 0,
            queue: BinaryHeap::new(),
            events: Vec::new(),
            seq: 0,
            nodes: BTreeMap::new(),
            group: BTreeMap::new(),
            drop_percent: 0,
            crash_gap: 0,
            down_for: 0,
            leaders: BTreeMap::new(),
            applied_at: BTreeMap::new(),
            pending_writes: Vec::new(),
            acked: Vec::new(),
            pending_reads: BTreeMap::new(),
            next_cmd: 0,
            next_read: 0,
            report: Report {
                seed,
                ..Report::default()
            },
            compact_every: 5 + rng.below(40),
            cfg,
            rng,
        };
        // Chaos varies by seed: calm, stormy, or a hurricane of crashes and
        // loss, which is what exposes bugs needing several leader failures
        // in quick succession (like the Figure 8 commit bug).
        let (gap, down, drop) =
            [(800, 800, 10), (250, 400, 15), (60, 150, 30)][sim.rng.below(3) as usize];
        sim.crash_gap = gap;
        sim.down_for = down;
        sim.drop_percent = sim.rng.below(drop);
        for id in 1..=pool {
            let disk = Disk {
                snap: SnapshotMeta {
                    index: 0,
                    term: 0,
                    voters: if id <= n { voters.clone() } else { Vec::new() },
                },
                ..Disk::default()
            };
            let tick_ms = 7 + sim.rng.below(7); // up to 2x skew between nodes
            let raft = sim.boot(id, &disk);
            sim.nodes.insert(
                id,
                Node {
                    raft,
                    disk,
                    up: true,
                    tick_ms,
                    reads: Vec::new(),
                },
            );
            sim.group.insert(id, 0);
            sim.schedule(tick_ms, Event::Tick(id));
        }
        sim
    }

    fn boot(&mut self, id: NodeId, disk: &Disk) -> Raft {
        let entries: Vec<Entry> = disk
            .entries
            .iter()
            .filter(|e| e.index > disk.snap.index)
            .cloned()
            .collect();
        Raft::restore(
            id,
            self.cfg.clone(),
            self.rng.next_u64(),
            disk.hard,
            disk.snap.clone(),
            entries,
            disk.applied,
        )
    }

    fn schedule(&mut self, delay: u64, event: Event) {
        self.seq += 1;
        self.events.push(Some(event));
        self.queue
            .push(Reverse((self.now + delay, self.seq, self.events.len() - 1)));
    }

    fn fail(&self, what: String) -> String {
        format!("seed {}, t={}ms: {what}", self.seed, self.now)
    }

    /// Run for `duration` ms of simulated time with faults, then heal
    /// everything and check that the cluster converges.
    pub fn run(mut self, duration: u64) -> Result<Report, String> {
        let faults = [
            (Event::ClientWrite, 3),
            (Event::ClientRead, 7),
            (Event::Crash, 400),
            (Event::Partition, 700),
            (Event::Membership, 900),
            (Event::Transfer, 1100),
        ];
        for (e, every) in faults {
            let d = self.rng.below(every) + 1;
            self.schedule(d, e);
        }
        // Peek, never pop-and-discard: a dropped Tick would stop a node's
        // clock forever.
        while let Some(&Reverse((t, _, i))) = self.queue.peek() {
            if t > duration {
                break;
            }
            self.queue.pop();
            self.now = t;
            let event = self.events[i].take().expect("event");
            self.handle(event, true)?;
        }
        // Heal: everything up, fully connected, no loss; let it settle.
        self.drop_percent = 0;
        for g in self.group.values_mut() {
            *g = 0;
        }
        let down: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| !n.up)
            .map(|(&id, _)| id)
            .collect();
        for id in down {
            self.restart(id)?;
        }
        // Liveness: within 20 simulated seconds of healing, a leader must
        // emerge and every voter must converge on the same state.
        let end = self.now + 20_000;
        let mut next_check = self.now + 500;
        while let Some(&Reverse((t, _, i))) = self.queue.peek() {
            if t > next_check {
                if self.check_converged().is_ok() {
                    return Ok(self.report);
                }
                next_check += 500;
                if next_check > end {
                    break;
                }
                continue;
            }
            self.queue.pop();
            self.now = t;
            let event = self.events[i].take().expect("event");
            self.handle(event, false)?;
        }
        self.check_converged()?;
        Ok(self.report)
    }

    fn handle(&mut self, event: Event, faults: bool) -> Result<(), String> {
        match event {
            Event::Deliver(m) => {
                let to = m.to;
                if self.nodes.get(&to).is_some_and(|n| n.up) {
                    self.nodes.get_mut(&to).expect("node").raft.step(m);
                    self.process(to)?;
                }
            }
            Event::Tick(id) => {
                let node = self.nodes.get_mut(&id).expect("node");
                let tick_ms = node.tick_ms;
                if node.up {
                    node.raft.tick();
                    self.process(id)?;
                }
                self.schedule(tick_ms, Event::Tick(id));
            }
            Event::ClientWrite if faults => {
                self.client_write()?;
                let d = 1 + self.rng.below(6);
                self.schedule(d, Event::ClientWrite);
            }
            Event::ClientRead if faults => {
                self.client_read()?;
                let d = 1 + self.rng.below(14);
                self.schedule(d, Event::ClientRead);
            }
            Event::Crash if faults => {
                let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
                let id = ids[self.rng.below(ids.len() as u64) as usize];
                if self.nodes[&id].up {
                    self.nodes.get_mut(&id).expect("node").up = false;
                    self.report.crashes += 1;
                    let d = 20 + self.rng.below(self.down_for);
                    self.schedule(d, Event::Restart(id));
                }
                let d = 1 + self.rng.below(2 * self.crash_gap);
                self.schedule(d, Event::Crash);
            }
            Event::Restart(id) => self.restart(id)?,
            Event::Partition if faults => {
                for g in self.group.values_mut() {
                    *g = self.rng.below(2) as u8;
                }
                self.report.partitions += 1;
                let d = 100 + self.rng.below(1500);
                self.schedule(d, Event::Heal);
                let d = 1 + self.rng.below(1400);
                self.schedule(d, Event::Partition);
            }
            Event::Heal => {
                for g in self.group.values_mut() {
                    *g = 0;
                }
            }
            Event::Membership if faults => {
                self.change_membership();
                let d = 1 + self.rng.below(1800);
                self.schedule(d, Event::Membership);
            }
            Event::Transfer if faults => {
                self.transfer();
                let d = 1 + self.rng.below(2200);
                self.schedule(d, Event::Transfer);
            }
            _ => {}
        }
        Ok(())
    }

    fn restart(&mut self, id: NodeId) -> Result<(), String> {
        let disk = self.nodes[&id].disk.clone();
        if self.nodes[&id].up {
            return Ok(());
        }
        let raft = self.boot(id, &disk);
        let node = self.nodes.get_mut(&id).expect("node");
        node.raft = raft;
        node.up = true;
        node.reads.clear();
        Ok(())
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.up && n.raft.is_leader())
            .max_by_key(|(_, n)| n.raft.term())
            .map(|(&id, _)| id)
    }

    fn client_write(&mut self) -> Result<(), String> {
        let Some(id) = self.leader() else {
            return Ok(());
        };
        self.next_cmd += 1;
        let cmd = format!("cmd-{}", self.next_cmd).into_bytes();
        let node = self.nodes.get_mut(&id).expect("node");
        if let Ok((index, term)) = node.raft.propose(cmd.clone()) {
            self.pending_writes.push(PendingWrite {
                cmd,
                node: id,
                index,
                term,
            });
            self.process(id)?;
        }
        Ok(())
    }

    fn client_read(&mut self) -> Result<(), String> {
        // Ask whichever node believes it leads, stale or not: a correct
        // ReadIndex must refuse or confirm leadership first.
        let candidates: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.up && n.raft.is_leader())
            .map(|(&id, _)| id)
            .collect();
        if candidates.is_empty() {
            return Ok(());
        }
        let id = candidates[self.rng.below(candidates.len() as u64) as usize];
        self.next_read += 1;
        let read_id = self.next_read;
        if self
            .nodes
            .get_mut(&id)
            .expect("node")
            .raft
            .read_index(read_id)
            .is_ok()
        {
            self.pending_reads.insert(
                read_id,
                PendingRead {
                    started: self.now,
                    must_see: self.acked.len(),
                },
            );
            self.process(id)?;
        }
        Ok(())
    }

    fn change_membership(&mut self) {
        let Some(id) = self.leader() else {
            return;
        };
        let voters = self.nodes[&id].raft.voters().to_vec();
        let all: Vec<NodeId> = self.nodes.keys().copied().collect();
        let mut new = voters.clone();
        let add = voters.len() < 3 || (voters.len() < 5 && self.rng.chance(50));
        if add {
            let spare: Vec<NodeId> = all
                .iter()
                .copied()
                .filter(|n| !voters.contains(n))
                .collect();
            if spare.is_empty() {
                return;
            }
            new.push(spare[self.rng.below(spare.len() as u64) as usize]);
        } else {
            let victim = voters[self.rng.below(voters.len() as u64) as usize];
            new.retain(|&v| v != victim);
        }
        let node = self.nodes.get_mut(&id).expect("node");
        if node.raft.propose_config(new).is_ok() {
            self.report.config_changes += 1;
            let _ = self.process(id);
        }
    }

    fn transfer(&mut self) {
        let Some(id) = self.leader() else {
            return;
        };
        let voters = self.nodes[&id].raft.voters().to_vec();
        let to = voters[self.rng.below(voters.len() as u64) as usize];
        if to != id {
            self.report.transfers += 1;
            self.nodes
                .get_mut(&id)
                .expect("node")
                .raft
                .transfer_leader(to);
            let _ = self.process(id);
        }
    }

    /// Drain a node's `Ready`: persist, send, apply, answer reads, compact.
    fn process(&mut self, id: NodeId) -> Result<(), String> {
        loop {
            let node = self.nodes.get_mut(&id).expect("node");
            if !node.raft.has_ready() {
                break;
            }
            let ready = node.raft.ready();

            // Election safety.
            if node.raft.is_leader() {
                let term = node.raft.term();
                match self.leaders.get(&term) {
                    Some(&other) if other != id => {
                        return Err(
                            self.fail(format!("two leaders in term {term}: {other} and {id}"))
                        );
                    }
                    None => {
                        self.leaders.insert(term, id);
                        self.report.elections += 1;
                    }
                    _ => {}
                }
            }

            // 1. Persist.
            let node = self.nodes.get_mut(&id).expect("node");
            if let Some(hs) = ready.hard_state {
                node.disk.hard = hs;
            }
            if let Some(snap) = &ready.snapshot {
                node.disk.snap = snap.meta.clone();
                node.disk.entries.clear();
                node.disk.state = decode_state(&snap.data);
                node.disk.applied = snap.meta.index;
                self.report.snapshots += 1;
            }
            node.disk.persist_entries(&ready.entries);

            // 2. Send.
            for m in ready.messages {
                self.send(m);
            }

            // 3. Apply.
            for e in &ready.committed {
                self.apply(id, e)?;
            }

            // 4. Answer reads whose index is applied; fail the rest cleanly.
            let node = self.nodes.get_mut(&id).expect("node");
            node.reads.extend(ready.reads);
            for r in ready.failed_reads {
                self.pending_reads.remove(&r);
            }
            self.serve_reads(id)?;

            // 5. Snapshots for lagging followers.
            for peer in ready.snapshot_requests {
                let node = self.nodes.get_mut(&id).expect("node");
                let applied = node.disk.applied;
                if let Some(meta) = node.raft.snapshot_meta(applied) {
                    let data = encode_state(&node.disk.state);
                    node.raft.send_snapshot(peer, Snapshot { meta, data });
                }
            }

            // 6. Compact the log behind the applied index.
            let node = self.nodes.get_mut(&id).expect("node");
            let applied = node.disk.applied;
            if applied >= node.raft.first_index() + self.compact_every
                && let Some(meta) = node.raft.snapshot_meta(applied)
            {
                node.raft.compact(applied);
                node.disk.snap = meta;
                node.disk.entries.retain(|e| e.index > applied);
            }
        }
        Ok(())
    }

    fn send(&mut self, m: Message) {
        self.report.messages += 1;
        let connected = self.group.get(&m.from) == self.group.get(&m.to);
        if !connected || self.rng.chance(self.drop_percent) {
            self.report.dropped += 1;
            return;
        }
        let delay = 1 + self.rng.below(12);
        if self.rng.chance(2) {
            let extra = delay + self.rng.below(30);
            self.schedule(extra, Event::Deliver(m.clone()));
        }
        self.schedule(delay, Event::Deliver(m));
    }

    fn apply(&mut self, id: NodeId, e: &Entry) -> Result<(), String> {
        let applied = self.nodes[&id].disk.applied;
        if e.index != applied + 1 {
            return Err(self.fail(format!(
                "node {id} applied index {} after {applied}",
                e.index
            )));
        }
        let node = self.nodes.get_mut(&id).expect("node");
        node.disk.applied = e.index;
        let cmd = match &e.data {
            EntryData::Command(c) => c.clone(),
            EntryData::Empty => b"(empty)".to_vec(),
            EntryData::Config(v) => format!("(config {v:?})").into_bytes(),
        };
        // State machine safety: one command per index, cluster-wide.
        match self.applied_at.get(&e.index) {
            Some(prev) if *prev != cmd => {
                return Err(self.fail(format!(
                    "state machine safety: node {id} applied {:?} at index {}, another node applied {:?}",
                    String::from_utf8_lossy(&cmd),
                    e.index,
                    String::from_utf8_lossy(prev)
                )));
            }
            Some(_) => {}
            None => {
                self.applied_at.insert(e.index, cmd.clone());
            }
        }
        if let EntryData::Command(c) = &e.data {
            node.disk.state.push((e.index, c.clone()));
        }
        // Acknowledge the client if this node proposed it, in this term.
        let mut i = 0;
        while i < self.pending_writes.len() {
            let w = &self.pending_writes[i];
            if w.node == id && w.index == e.index {
                let w = self.pending_writes.swap_remove(i);
                if w.term == e.term {
                    self.acked.push(w.cmd);
                    self.report.acked_writes += 1;
                }
            } else {
                i += 1;
            }
        }
        Ok(())
    }

    fn serve_reads(&mut self, id: NodeId) -> Result<(), String> {
        let node = self.nodes.get_mut(&id).expect("node");
        let applied = node.disk.applied;
        let (ready, waiting): (Vec<_>, Vec<_>) =
            node.reads.drain(..).partition(|&(_, i)| i <= applied);
        node.reads = waiting;
        if ready.is_empty() {
            return Ok(());
        }
        let seen: BTreeSet<&[u8]> = node.disk.state.iter().map(|(_, c)| c.as_slice()).collect();
        for (read_id, _) in ready {
            let Some(r) = self.pending_reads.remove(&read_id) else {
                continue;
            };
            self.report.reads += 1;
            if let Some(missing) = self.acked[..r.must_see]
                .iter()
                .find(|c| !seen.contains(c.as_slice()))
            {
                return Err(self.fail(format!(
                    "stale read: a read started at t={}ms on node {id} missed {:?}, acknowledged before it began",
                    r.started,
                    String::from_utf8_lossy(missing)
                )));
            }
        }
        Ok(())
    }

    /// Every node's Raft state, for failure reports.
    fn dump(&self) -> String {
        self.nodes
            .iter()
            .map(|(id, n)| {
                let r = &n.raft;
                format!(
                    "  n{id}{}: {:?} term {} vote {:?} voters {:?} last {}/{} commit {} leader {:?}",
                    if n.up { "" } else { " (down)" },
                    r.role(),
                    r.term(),
                    n.disk.hard.vote,
                    r.voters(),
                    r.last_index(),
                    r.entry_term(r.last_index()).unwrap_or(0),
                    r.commit_index(),
                    r.leader()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn check_converged(&self) -> Result<(), String> {
        let leader = self
            .leader()
            .ok_or_else(|| self.fail(format!("no leader 20s after healing\n{}", self.dump())))?;
        let voters = self.nodes[&leader].raft.voters().to_vec();
        let reference = &self.nodes[&leader].disk;
        let have: BTreeSet<&Vec<u8>> = reference.state.iter().map(|(_, c)| c).collect();
        if let Some(lost) = self.acked.iter().find(|c| !have.contains(c)) {
            return Err(self.fail(format!(
                "acknowledged write {:?} was lost",
                String::from_utf8_lossy(lost)
            )));
        }
        for v in voters {
            let d = &self.nodes[&v].disk;
            if d.state != reference.state {
                let (r, l) = (&self.nodes[&v].raft, &self.nodes[&leader].raft);
                return Err(self.fail(format!(
                    "node {v} did not converge: {} commands applied, leader {leader} has {}\n  \
                     node {v}: {:?} term {} last {} commit {} first {} applied {} leader {:?}\n  \
                     leader: term {} last {} commit {} first {} progress {}\n{}",
                    d.state.len(),
                    reference.state.len(),
                    r.role(),
                    r.term(),
                    r.last_index(),
                    r.commit_index(),
                    r.first_index(),
                    d.applied,
                    r.leader(),
                    l.term(),
                    l.last_index(),
                    l.commit_index(),
                    l.first_index(),
                    l.debug_progress(),
                    self.dump()
                )));
            }
        }
        Ok(())
    }
}

/// Run one seed for `duration_ms` of simulated time.
pub fn run(seed: u64, duration_ms: u64, fault: RaftFault) -> Result<Report, String> {
    let sim = Sim::new(seed, fault);
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sim.run(duration_ms))) {
        Ok(result) => result,
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            Err(format!("seed {seed}: a node panicked: {msg}"))
        }
    }
}
