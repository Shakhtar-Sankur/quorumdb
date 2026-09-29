//! Deterministic simulation of a whole multi-Raft cluster: nodes running
//! real [`Store`]s on simulated disks that lose unsynced data when they
//! crash, a lossy partitioned network with clock skew, the placement driver
//! splitting and rebalancing ranges, and clients reading and writing keys
//! through it all.
//!
//! Every client operation is recorded, and at the end each key's history
//! must be **linearizable**: explainable as if every operation took effect
//! at one instant between its call and its return. That single property
//! covers lost writes, stale reads, dirty reads and values flipping back,
//! across leader changes, splits, replica moves and crashes.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

use crate::check::linearizability::{self, OpKind, Operation};
use crate::kv::cmd::{KvError, ReqId, Request, Response};
use crate::kv::keys::RangeDescriptor;
use crate::kv::pd::{Action, Pd, PdConfig};
use crate::kv::store::{RangeMessage, Store, StoreConfig, StoreFault};
use crate::raft::NodeId;
use crate::rng::Rng;
use crate::storage::engine::{Options, SyncMode};
use crate::storage::fs::SimFs;

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub seed: u64,
    pub ops: u64,
    pub writes: u64,
    pub reads: u64,
    pub unknown: u64,
    pub crashes: u64,
    pub partitions: u64,
    pub messages: u64,
    pub dropped: u64,
    pub ranges: u64,
    pub replica_moves: u64,
    pub keys_checked: u64,
}

#[derive(Clone, Debug)]
enum Event {
    Deliver(NodeId, NodeId, RangeMessage),
    Tick(NodeId),
    Crash,
    Restart(NodeId),
    Partition,
    Heal,
    PdRound,
    /// A client starts its next operation.
    Start(usize),
    /// A client (re)sends its current operation.
    Send(usize),
    /// A request reaches a node.
    Arrive {
        node: NodeId,
        range: u64,
        req_id: ReqId,
        req: Request,
    },
    /// A response reaches its client.
    Respond(ReqId, Result<Response, KvError>),
    /// A client gives up on an attempt: (client, op_seq, request).
    Timeout(usize, u64, ReqId),
}

struct Node {
    store: Option<Store<SimFs>>,
    fs: SimFs,
    tick_ms: u64,
}

#[derive(Clone, Debug)]
enum ClientOp {
    Put(Vec<u8>, u64),
    Delete(Vec<u8>),
    Get(Vec<u8>),
}

struct Client {
    op: Option<ClientOp>,
    /// Bumped per operation, to ignore stale timeouts and responses.
    op_seq: u64,
    call: u64,
    leader_hint: Option<NodeId>,
    attempt_req: Option<ReqId>,
    deadline: u64,
}

pub struct Sim {
    seed: u64,
    rng: Rng,
    now: u64,
    queue: BinaryHeap<Reverse<(u64, u64, usize)>>,
    events: Vec<Option<Event>>,
    seq: u64,
    nodes: BTreeMap<NodeId, Node>,
    store_cfg: StoreConfig,
    pd: Pd,
    group: BTreeMap<NodeId, u8>,
    drop_percent: u64,
    clients: Vec<Client>,
    keys: Vec<Vec<u8>>,
    /// Request id -> (client, op_seq); PD's requests are not listed.
    requests: BTreeMap<ReqId, (usize, u64)>,
    next_req: ReqId,
    next_value: u64,
    history: BTreeMap<Vec<u8>, Vec<Operation>>,
    report: Report,
    faults: bool,
    /// Clients stop starting new operations for the final checks.
    stopped: bool,
    /// Set QDB_TRACE=1 to print every cluster-level event.
    trace: bool,
}

fn value_bytes(v: u64) -> Vec<u8> {
    format!("v{v}").into_bytes()
}

fn parse_value(b: &[u8]) -> Option<u64> {
    std::str::from_utf8(b).ok()?.strip_prefix('v')?.parse().ok()
}

impl Sim {
    pub fn new(seed: u64, fault: StoreFault) -> Result<Sim, String> {
        let mut rng = Rng::new(seed);
        let initial = 3;
        let total = 3 + rng.below(3); // up to two spare nodes to move replicas to
        let store_cfg = StoreConfig {
            raft: crate::raft::Config {
                election_ticks: 10,
                heartbeat_ticks: 2,
                max_append_entries: 1 + rng.below(32) as usize,
                pre_vote: true,
                check_quorum: rng.chance(50),
                fault: crate::raft::RaftFault::None,
            },
            compact_after: 8 + rng.below(64),
            engine: Options {
                sync: SyncMode::Manual,
                memtable_bytes: 512 + rng.below(8192) as usize,
                block_bytes: 64 + rng.below(512) as usize,
                table_bytes: 1024 + rng.below(8192) as usize,
                l0_compact_at: 2 + rng.below(4) as usize,
                level1_bytes: 4096 + rng.below(16384),
                level_multiplier: 2 + rng.below(4),
                bloom_bits_per_key: 10,
                fault: crate::storage::engine::Fault::None,
            },
            fault,
        };
        let key_count = 20 + rng.below(100);
        let keys = (0..key_count)
            .map(|i| format!("key{:05}", (i * 7919) % 100_000).into_bytes())
            .collect();
        let pd = Pd::new(
            PdConfig {
                replication: 3,
                split_keys: 4 + rng.below(20),
                dead_after: 2_000,
                rebalance: true,
            },
            2,
        );
        let clients = (0..3 + rng.below(4))
            .map(|_| Client {
                op: None,
                op_seq: 0,
                call: 0,
                leader_hint: None,
                attempt_req: None,
                deadline: 0,
            })
            .collect();
        let mut sim = Sim {
            seed,
            now: 0,
            queue: BinaryHeap::new(),
            events: Vec::new(),
            seq: 0,
            nodes: BTreeMap::new(),
            store_cfg,
            pd,
            group: BTreeMap::new(),
            drop_percent: rng.below(8),
            clients,
            keys,
            requests: BTreeMap::new(),
            next_req: 0,
            next_value: 0,
            history: BTreeMap::new(),
            report: Report {
                seed,
                ..Report::default()
            },
            faults: true,
            stopped: false,
            trace: std::env::var("QDB_TRACE").is_ok(),
            rng,
        };
        let first = RangeDescriptor {
            id: 1,
            start: Vec::new(),
            end: Vec::new(),
            replicas: (1..=initial)
                .map(|n| crate::kv::keys::replica_id(n, 0))
                .collect(),
            generation: 0,
            next_incarnation: 1,
        };
        for id in 1..=total {
            let fs = SimFs::new(sim.rng.next_u64());
            let mut store = Store::open(fs.clone(), id, sim.store_cfg.clone(), seed ^ (id << 32))
                .map_err(|e| format!("seed {seed}: open failed: {e}"))?;
            if id <= initial {
                store
                    .bootstrap(first.clone())
                    .map_err(|e| format!("seed {seed}: bootstrap failed: {e}"))?;
            }
            let tick_ms = 7 + sim.rng.below(7);
            sim.nodes.insert(
                id,
                Node {
                    store: Some(store),
                    fs,
                    tick_ms,
                },
            );
            sim.group.insert(id, 0);
            sim.schedule(tick_ms, Event::Tick(id));
        }
        Ok(sim)
    }

    fn schedule(&mut self, delay: u64, event: Event) {
        self.seq += 1;
        self.events.push(Some(event));
        self.queue
            .push(Reverse((self.now + delay, self.seq, self.events.len() - 1)));
    }

    fn log(&self, what: impl FnOnce() -> String) {
        if self.trace {
            eprintln!("t={:>6} {}", self.now, what());
        }
    }

    fn fail(&self, what: String) -> String {
        format!("seed {}, t={}ms: {what}", self.seed, self.now)
    }

    pub fn run(mut self, duration: u64) -> Result<Report, String> {
        for c in 0..self.clients.len() {
            let d = 1 + self.rng.below(50);
            self.schedule(d, Event::Start(c));
        }
        self.schedule(100, Event::PdRound);
        let d = 300 + self.rng.below(1500);
        self.schedule(d, Event::Crash);
        let d = 300 + self.rng.below(2000);
        self.schedule(d, Event::Partition);
        self.run_until(duration)?;

        // Heal everything, stop injecting faults, and let the cluster settle
        // while the clients keep working.
        self.faults = false;
        self.drop_percent = 0;
        for g in self.group.values_mut() {
            *g = 0;
        }
        let down: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.store.is_none())
            .map(|(&id, _)| id)
            .collect();
        for id in down {
            self.restart(id)?;
        }
        self.run_until(self.now + 10_000)?;

        // Stop the clients, let in-flight work drain, then read every key.
        self.stopped = true;
        self.run_until(self.now + 3_000)?;
        self.final_reads()?;
        self.check_history()?;
        self.check_replicas()?;
        self.report.ranges = self.pd.descriptors().len() as u64;
        Ok(self.report)
    }

    fn run_until(&mut self, end: u64) -> Result<(), String> {
        while let Some(&Reverse((t, _, i))) = self.queue.peek() {
            if t > end {
                break;
            }
            self.queue.pop();
            self.now = t;
            let event = self.events[i].take().expect("event");
            self.handle(event)?;
        }
        self.now = self.now.max(end);
        Ok(())
    }

    fn up(&self, id: NodeId) -> bool {
        self.nodes.get(&id).is_some_and(|n| n.store.is_some())
    }

    fn handle(&mut self, event: Event) -> Result<(), String> {
        match event {
            Event::Deliver(from, to, m) => {
                if let Some(store) = self.nodes.get_mut(&to).and_then(|n| n.store.as_mut()) {
                    let _ = from;
                    store
                        .step(m)
                        .map_err(|e| format!("seed {}: step failed: {e}", self.seed))?;
                    self.process(to)?;
                }
            }
            Event::Tick(id) => {
                let tick_ms = self.nodes[&id].tick_ms;
                if let Some(store) = self.nodes.get_mut(&id).and_then(|n| n.store.as_mut()) {
                    store.tick();
                    self.process(id)?;
                }
                self.schedule(tick_ms, Event::Tick(id));
            }
            Event::Crash => {
                if self.faults {
                    let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
                    let id = ids[self.rng.below(ids.len() as u64) as usize];
                    // Keep a majority of nodes up, so progress stays possible.
                    let up = self.nodes.values().filter(|n| n.store.is_some()).count();
                    if self.up(id) && up > self.nodes.len() / 2 + 1 {
                        let node = self.nodes.get_mut(&id).expect("node");
                        node.store = None;
                        node.fs.crash();
                        self.log(|| format!("CRASH node {id}"));
                        self.report.crashes += 1;
                        let d = 50 + self.rng.below(1500);
                        self.schedule(d, Event::Restart(id));
                    }
                    let d = 100 + self.rng.below(1500);
                    self.schedule(d, Event::Crash);
                }
            }
            Event::Restart(id) => self.restart(id)?,
            Event::Partition => {
                if self.faults {
                    for g in self.group.values_mut() {
                        *g = 0;
                    }
                    let ids: Vec<NodeId> = self.group.keys().copied().collect();
                    let id = ids[self.rng.below(ids.len() as u64) as usize];
                    self.group.insert(id, 1);
                    self.report.partitions += 1;
                    let d = 100 + self.rng.below(1500);
                    self.schedule(d, Event::Heal);
                    let d = 200 + self.rng.below(2500);
                    self.schedule(d, Event::Partition);
                }
            }
            Event::Heal => {
                for g in self.group.values_mut() {
                    *g = 0;
                }
            }
            Event::PdRound => {
                self.pd_round()?;
                self.schedule(100, Event::PdRound);
            }
            Event::Start(c) => self.start_op(c),
            Event::Send(c) => self.send_op(c),
            Event::Arrive {
                node,
                range,
                req_id,
                req,
            } => {
                self.log(|| format!("REQ {req_id} -> node {node} range {range}: {req:?}"));
                if let Some(store) = self.nodes.get_mut(&node).and_then(|n| n.store.as_mut()) {
                    store.submit(range, req_id, req);
                    self.process(node)?;
                }
            }
            Event::Respond(req_id, result) => self.on_response(req_id, result)?,
            Event::Timeout(c, op_seq, req_id) => self.on_timeout(c, op_seq, req_id),
        }
        Ok(())
    }

    fn restart(&mut self, id: NodeId) -> Result<(), String> {
        if self.up(id) {
            return Ok(());
        }
        let fs = self.nodes[&id].fs.clone();
        let store = Store::open(
            fs,
            id,
            self.store_cfg.clone(),
            self.seed ^ (id << 32) ^ self.now,
        )
        .map_err(|e| self.fail(format!("node {id} failed to recover: {e}")))?;
        self.nodes.get_mut(&id).expect("node").store = Some(store);
        self.log(|| format!("RESTART node {id}"));
        Ok(())
    }

    /// Run a node's store and route everything it produced.
    fn process(&mut self, id: NodeId) -> Result<(), String> {
        let store = self
            .nodes
            .get_mut(&id)
            .and_then(|n| n.store.as_mut())
            .expect("up");
        if let Err(e) = store.process() {
            return Err(self.fail(format!("node {id}: {e}")));
        }
        let messages = store.take_messages();
        let responses = store.take_responses();
        for (to, m) in messages {
            self.report.messages += 1;
            let connected = self.group.get(&id) == self.group.get(&to);
            if !connected || self.rng.chance(self.drop_percent) {
                self.report.dropped += 1;
                continue;
            }
            let delay = 1 + self.rng.below(10);
            self.schedule(delay, Event::Deliver(id, to, m));
        }
        for (req_id, result) in responses {
            if self.requests.contains_key(&req_id) {
                let delay = 1 + self.rng.below(5);
                self.schedule(delay, Event::Respond(req_id, result));
            }
        }
        Ok(())
    }

    fn pd_round(&mut self) -> Result<(), String> {
        let now = self.now;
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for &id in &ids {
            if let Some(store) = self.nodes[&id].store.as_ref() {
                let reports = store.report();
                self.pd.report(id, now, reports);
            }
        }
        for action in self.pd.schedule(now) {
            match action {
                Action::Submit { node, range, req } => {
                    self.log(|| format!("PD -> node {node} range {range}: {req:?}"));
                    if matches!(req, Request::ChangeReplicas { .. }) {
                        self.report.replica_moves += 1;
                    }
                    self.next_req += 1;
                    let req_id = self.next_req;
                    if let Some(store) = self.nodes.get_mut(&node).and_then(|n| n.store.as_mut()) {
                        store.submit(range, req_id, req);
                        self.process(node)?;
                    }
                }
                Action::Gc {
                    node,
                    range,
                    replica,
                } => {
                    self.log(|| format!("PD GC node {node} range {range} replica {replica:x}"));
                    if let Some(store) = self.nodes.get_mut(&node).and_then(|n| n.store.as_mut()) {
                        store
                            .gc_replica(range, replica)
                            .map_err(|e| format!("seed {}: gc failed: {e}", self.seed))?;
                        self.process(node)?;
                    }
                }
            }
        }
        Ok(())
    }

    // ─── Clients ──────────────────────────────────────────────────────────

    fn start_op(&mut self, c: usize) {
        if self.stopped {
            return;
        }
        let key = self.keys[self.rng.below(self.keys.len() as u64) as usize].clone();
        let roll = self.rng.below(100);
        let op = if roll < 45 {
            self.next_value += 1;
            ClientOp::Put(key, self.next_value)
        } else if roll < 52 {
            ClientOp::Delete(key)
        } else {
            ClientOp::Get(key)
        };
        let client = &mut self.clients[c];
        client.op_seq += 1;
        client.op = Some(op);
        client.call = self.now;
        client.deadline = self.now + 2_000;
        self.report.ops += 1;
        self.send_op(c);
    }

    fn key_of(op: &ClientOp) -> &[u8] {
        match op {
            ClientOp::Put(k, _) | ClientOp::Delete(k) | ClientOp::Get(k) => k,
        }
    }

    fn send_op(&mut self, c: usize) {
        let Some(op) = self.clients[c].op.clone() else {
            return;
        };
        let key = Self::key_of(&op).to_vec();
        let Some((desc, pd_leader)) = self.pd.route(&key) else {
            self.schedule(20, Event::Send(c));
            return;
        };
        let nodes = desc.nodes();
        let hint = self.clients[c].leader_hint.filter(|n| nodes.contains(n));
        let node = hint
            .or(pd_leader)
            .unwrap_or_else(|| nodes[self.rng.below(nodes.len() as u64) as usize]);
        let req = match op {
            ClientOp::Put(k, v) => Request::Put {
                key: k,
                value: value_bytes(v),
            },
            ClientOp::Delete(k) => Request::Delete { key: k },
            ClientOp::Get(k) => Request::Get { key: k },
        };
        self.next_req += 1;
        let req_id = self.next_req;
        let op_seq = self.clients[c].op_seq;
        self.requests.insert(req_id, (c, op_seq));
        self.clients[c].attempt_req = Some(req_id);
        let delay = 1 + self.rng.below(5);
        self.schedule(
            delay,
            Event::Arrive {
                node,
                range: desc.id,
                req_id,
                req,
            },
        );
        self.schedule(500, Event::Timeout(c, op_seq, req_id));
    }

    fn record(&mut self, c: usize, kind: OpKind, ret: Option<u64>) {
        let client = &self.clients[c];
        let key = Self::key_of(client.op.as_ref().expect("op")).to_vec();
        self.history.entry(key).or_default().push(Operation {
            kind,
            call: client.call,
            ret,
            client: c as u64,
        });
    }

    fn finish(&mut self, c: usize) {
        self.clients[c].op = None;
        self.clients[c].attempt_req = None;
        if !self.stopped {
            let d = 1 + self.rng.below(20);
            self.schedule(d, Event::Start(c));
        }
    }

    fn on_response(
        &mut self,
        req_id: ReqId,
        result: Result<Response, KvError>,
    ) -> Result<(), String> {
        self.log(|| format!("RESP {req_id}: {result:?}"));
        let Some((c, op_seq)) = self.requests.remove(&req_id) else {
            return Ok(());
        };
        let client = &self.clients[c];
        if client.op_seq != op_seq || client.attempt_req != Some(req_id) || client.op.is_none() {
            return Ok(());
        }
        let op = client.op.clone().expect("op");
        match result {
            Ok(resp) => {
                match (&op, resp) {
                    (ClientOp::Put(_, v), Response::Done) => {
                        self.report.writes += 1;
                        self.record(c, OpKind::Write(Some(*v)), Some(self.now));
                    }
                    (ClientOp::Delete(_), Response::Done) => {
                        self.report.writes += 1;
                        self.record(c, OpKind::Write(None), Some(self.now));
                    }
                    (ClientOp::Get(_), Response::Value(v)) => {
                        self.report.reads += 1;
                        let parsed = match v {
                            None => None,
                            Some(b) => Some(
                                parse_value(&b)
                                    .ok_or_else(|| self.fail("read an unknown value".into()))?,
                            ),
                        };
                        self.record(c, OpKind::Read(parsed), Some(self.now));
                    }
                    (op, resp) => {
                        return Err(self.fail(format!("mismatched response {resp:?} to {op:?}")));
                    }
                }
                self.finish(c);
            }
            Err(KvError::Ambiguous) => self.give_up(c),
            Err(e) => {
                // Definitely not applied: safe to retry, after a refresh.
                self.clients[c].leader_hint = match e {
                    KvError::NotLeader(hint) => hint,
                    _ => None,
                };
                if self.now > self.clients[c].deadline {
                    self.give_up(c);
                } else {
                    let d = 5 + self.rng.below(30);
                    self.schedule(d, Event::Send(c));
                }
            }
        }
        Ok(())
    }

    fn on_timeout(&mut self, c: usize, op_seq: u64, req_id: ReqId) {
        let client = &self.clients[c];
        if client.op_seq != op_seq || client.attempt_req != Some(req_id) || client.op.is_none() {
            return;
        }
        let retry_read = matches!(client.op, Some(ClientOp::Get(_))) && self.now <= client.deadline;
        self.requests.remove(&req_id);
        self.clients[c].leader_hint = None;
        if retry_read {
            // A read that timed out observed nothing: try again.
            self.send_op(c);
        } else {
            self.give_up(c);
        }
    }

    /// The outcome is unknown: a write may yet apply, a read saw nothing.
    fn give_up(&mut self, c: usize) {
        match self.clients[c].op.clone() {
            Some(ClientOp::Put(_, v)) => self.record(c, OpKind::Write(Some(v)), None),
            Some(ClientOp::Delete(_)) => self.record(c, OpKind::Write(None), None),
            _ => {}
        }
        self.report.unknown += 1;
        self.finish(c);
    }

    // ─── Final checks ─────────────────────────────────────────────────────

    /// Read every key once more through the cluster; each must answer
    /// (liveness), and the answers join the history.
    fn final_reads(&mut self) -> Result<(), String> {
        let keys = self.keys.clone();
        let c = 0;
        // Client 0 may still be mid-operation: settle it as unknown.
        if self.clients[c].op.is_some() {
            self.give_up(c);
        }
        for key in keys {
            self.clients[c].op_seq += 1;
            self.clients[c].op = Some(ClientOp::Get(key.clone()));
            self.clients[c].call = self.now;
            self.clients[c].deadline = self.now + 5_000;
            self.send_op(c);
            let give_up_at = self.now + 6_000;
            while self.clients[c].op.is_some() && self.now < give_up_at {
                let Some(&Reverse((t, _, i))) = self.queue.peek() else {
                    break;
                };
                self.queue.pop();
                self.now = t;
                let event = self.events[i].take().expect("event");
                self.handle(event)?;
            }
            if self.clients[c].op.is_some()
                || self
                    .history
                    .get(&key)
                    .and_then(|h| h.last())
                    .is_none_or(|o| o.ret.is_none())
            {
                let range = self.pd.route(&key).map(|(d, _)| d.id).unwrap_or(0);
                let views: Vec<String> = self
                    .nodes
                    .iter()
                    .map(|(id, n)| {
                        let view = n
                            .store
                            .as_ref()
                            .map_or("down".to_string(), |s| s.debug_replica(range));
                        format!("  node {id} range {range}: {view}")
                    })
                    .collect();
                return Err(self.fail(format!(
                    "key {:?} could not be read after the cluster healed\n{}\n{}",
                    String::from_utf8_lossy(&key),
                    views.join("\n"),
                    self.dump()
                )));
            }
        }
        Ok(())
    }

    fn check_history(&mut self) -> Result<(), String> {
        for (key, ops) in &self.history {
            if let Err(e) = linearizability::check(ops) {
                return Err(self.fail(format!("key {:?}: {e}", String::from_utf8_lossy(key))));
            }
            self.report.keys_checked += 1;
        }
        Ok(())
    }

    /// Every replica of a range that has applied the same log index must
    /// hold exactly the same data: replicas are deterministic state machines.
    fn check_replicas(&mut self) -> Result<(), String> {
        self.run_until(self.now + 3_000)?;
        for d in self.pd.descriptors() {
            let mut seen: BTreeMap<u64, (NodeId, u32, u64)> = BTreeMap::new();
            for (&id, n) in &self.nodes {
                let Some(store) = &n.store else { continue };
                let digest = store
                    .range_digest(d.id)
                    .map_err(|e| self.fail(format!("node {id}: {e}")))?;
                let Some((applied, crc, rows)) = digest else {
                    continue;
                };
                match seen.get(&applied) {
                    Some(&(other, c, r)) if c != crc => {
                        return Err(self.fail(format!(
                            "replicas diverged: range {} at applied index {applied} holds {r} rows on node {other} \
                             but {rows} rows (different data) on node {id}",
                            d.id
                        )));
                    }
                    Some(_) => {}
                    None => {
                        seen.insert(applied, (id, crc, rows));
                    }
                }
            }
        }
        Ok(())
    }

    fn dump(&self) -> String {
        let mut out = String::new();
        for d in self.pd.descriptors() {
            out.push_str(&format!(
                "  range {} [{}, {}) nodes {:?} gen {}\n",
                d.id,
                String::from_utf8_lossy(&d.start),
                String::from_utf8_lossy(&d.end),
                d.nodes(),
                d.generation
            ));
        }
        for (id, n) in &self.nodes {
            match &n.store {
                None => out.push_str(&format!("  node {id}: down\n")),
                Some(s) => {
                    for (range, rid, desc, leader, term, applied) in s.describe() {
                        out.push_str(&format!(
                            "  node {id} range {range} replica {rid:x}: {} term {term} applied {applied} gen {:?}\n",
                            if leader { "LEADER" } else { "follower" },
                            desc.map(|d| d.generation)
                        ));
                    }
                }
            }
        }
        out
    }
}

pub fn run(seed: u64, duration_ms: u64, fault: StoreFault) -> Result<Report, String> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Sim::new(seed, fault)?.run(duration_ms)
    }));
    match result {
        Ok(r) => r,
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
