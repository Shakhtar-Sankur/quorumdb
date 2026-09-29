//! Deterministic simulation of distributed transactions: concurrent clients
//! run transactions (point reads, writes, scans across ranges) against the
//! full sharded cluster while nodes crash on disks that lose unsynced
//! writes, the network partitions and drops messages, clocks drift, and the
//! placement driver splits and moves ranges. Afterwards every transaction
//! whose outcome was lost is resolved, and the whole history goes through
//! the isolation checker: snapshot isolation always, serializability too
//! when the transactions asked for it.

use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::rc::Rc;

use crate::check::isolation::{self, CommittedTxn};
use crate::kv::client::KvClient;
use crate::kv::cmd::{KvError, ReqId, Request, Response};
use crate::kv::keys::{self, RangeDescriptor};
use crate::kv::pd::{Action, Pd, PdConfig};
use crate::kv::store::{RangeMessage, Store, StoreConfig, StoreFault};
use crate::raft::NodeId;
use crate::rng::Rng;
use crate::runtime::{Executor, Io};
use crate::storage::engine::{Options, SyncMode};
use crate::storage::fs::SimFs;
use crate::txn::client::{Isolation, Txn, TxnError, TxnFault, TxnOptions};
use crate::txn::mvcc::{MvccFault, TxnStatus};

/// A planted bug, for proving the simulator catches it.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnSimFault {
    None,
    SkipWriteConflict,
    ReadIgnoresLocks,
    SkipReadValidation,
    TsoServeBeforeDurable,
}

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub seed: u64,
    pub serializable: bool,
    pub committed: u64,
    pub aborted: u64,
    pub unknown_resolved: u64,
    pub crashes: u64,
    pub partitions: u64,
    pub ranges: u64,
    pub dependency_edges: u64,
    /// Snapshot-isolation runs that exhibited a write-skew cycle, which
    /// snapshot isolation allows.
    pub write_skew_seen: bool,
}

#[derive(Debug)]
enum Event {
    Deliver(NodeId, RangeMessage),
    Tick(NodeId),
    Crash,
    Restart(NodeId),
    Partition,
    Heal,
    PdRound,
    Arrive(NodeId, u64, ReqId, Request),
    Respond(ReqId, Result<Response, KvError>),
    Wake,
}

struct Node {
    store: Option<Store<SimFs>>,
    fs: SimFs,
    tick_ms: u64,
    /// Clock skew: this node's clock reads `now + offset`.
    offset: u64,
}

#[derive(Clone, Debug)]
struct Record {
    id: u64,
    began: u64,
    finished: u64,
    start_ts: u64,
    primary: Option<Vec<u8>>,
    reads: Vec<(Vec<u8>, Option<u64>)>,
    writes: Vec<Vec<u8>>,
    outcome: Outcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Committed(u64),
    Aborted,
    Unknown,
}

const PD_REQ_BASE: ReqId = 1 << 62;

/// A transaction whose outcome was lost, and what its primary says now.
type Resolution = (usize, Result<TxnStatus, String>);

pub struct Sim {
    seed: u64,
    rng: Rng,
    now: u64,
    queue: BinaryHeap<Reverse<(u64, u64, usize)>>,
    events: Vec<Option<Event>>,
    seq: u64,
    nodes: BTreeMap<NodeId, Node>,
    store_cfg: StoreConfig,
    pd: Rc<RefCell<Pd>>,
    group: BTreeMap<NodeId, u8>,
    drop_percent: u64,
    faults: bool,
    io: Rc<Io>,
    exec: Executor,
    records: Rc<RefCell<Vec<Record>>>,
    stop: Rc<Cell<bool>>,
    next_pd_req: ReqId,
    wake_at: Option<u64>,
    report: Report,
    isolation: Isolation,
}

fn value_of(txn: u64) -> Vec<u8> {
    format!("t{txn}").into_bytes()
}

fn writer_of(v: &[u8]) -> Option<u64> {
    std::str::from_utf8(v).ok()?.strip_prefix('t')?.parse().ok()
}

impl Sim {
    pub fn new(seed: u64, fault: TxnSimFault) -> Result<Sim, String> {
        let mut rng = Rng::new(seed);
        let serializable = match fault {
            TxnSimFault::SkipReadValidation => true,
            _ => rng.chance(50),
        };
        let isolation = if serializable {
            Isolation::Serializable
        } else {
            Isolation::Snapshot
        };
        let total = 3 + rng.below(3);
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
                memtable_bytes: 1024 + rng.below(8192) as usize,
                block_bytes: 64 + rng.below(512) as usize,
                table_bytes: 2048 + rng.below(8192) as usize,
                l0_compact_at: 2 + rng.below(4) as usize,
                level1_bytes: 8192 + rng.below(16384),
                level_multiplier: 2 + rng.below(4),
                bloom_bits_per_key: 10,
                fault: crate::storage::engine::Fault::None,
            },
            fault: if fault == TxnSimFault::TsoServeBeforeDurable {
                StoreFault::TsoServeBeforeDurable
            } else {
                StoreFault::None
            },
            mvcc_fault: match fault {
                TxnSimFault::SkipWriteConflict => MvccFault::SkipWriteConflict,
                TxnSimFault::ReadIgnoresLocks => MvccFault::ReadIgnoresLocks,
                _ => MvccFault::None,
            },
        };
        let pd = Rc::new(RefCell::new(Pd::new(
            PdConfig {
                replication: 3,
                split_keys: 6 + rng.below(20),
                dead_after: 2_000,
                rebalance: true,
            },
            2,
        )));
        let router = {
            let pd = pd.clone();
            move |key: &[u8]| pd.borrow().route(key)
        };
        let io = Io::new(rng.next_u64(), router);
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
            drop_percent: rng.below(6),
            faults: true,
            io,
            exec: Executor::default(),
            records: Rc::new(RefCell::new(Vec::new())),
            stop: Rc::new(Cell::new(false)),
            next_pd_req: PD_REQ_BASE,
            wake_at: None,
            report: Report {
                seed,
                serializable,
                ..Report::default()
            },
            isolation,
            rng,
        };
        let first = RangeDescriptor {
            id: 1,
            start: Vec::new(),
            end: Vec::new(),
            replicas: (1..=3).map(|n| keys::replica_id(n, 0)).collect(),
            generation: 0,
            next_incarnation: 1,
        };
        for id in 1..=total {
            let fs = SimFs::new(sim.rng.next_u64());
            let mut store = Store::open(fs.clone(), id, sim.store_cfg.clone(), seed ^ (id << 32))
                .map_err(|e| format!("seed {seed}: open failed: {e}"))?;
            if id <= 3 {
                store
                    .bootstrap(first.clone())
                    .map_err(|e| format!("seed {seed}: bootstrap failed: {e}"))?;
            }
            let tick_ms = 7 + sim.rng.below(7);
            let offset = sim.rng.below(100);
            sim.nodes.insert(
                id,
                Node {
                    store: Some(store),
                    fs,
                    tick_ms,
                    offset,
                },
            );
            sim.group.insert(id, 0);
            sim.schedule(tick_ms, Event::Tick(id));
        }
        let key_count = 4 + sim.rng.below(24);
        let clients = 2 + sim.rng.below(5);
        let fault_txn = if fault == TxnSimFault::SkipReadValidation {
            TxnFault::SkipReadValidation
        } else {
            TxnFault::None
        };
        for c in 0..clients {
            let kv = KvClient::new(sim.io.clone());
            let opts = TxnOptions {
                isolation,
                lock_ttl: 300 + sim.rng.below(2_000),
                fault: fault_txn,
            };
            let records = sim.records.clone();
            let stop = sim.stop.clone();
            let seed = sim.rng.next_u64() ^ c;
            sim.exec
                .spawn(client_loop(kv, opts, records, stop, key_count, seed, c));
        }
        Ok(sim)
    }

    fn schedule(&mut self, delay: u64, event: Event) {
        self.seq += 1;
        self.events.push(Some(event));
        self.queue
            .push(Reverse((self.now + delay, self.seq, self.events.len() - 1)));
    }

    fn fail(&self, what: String) -> String {
        format!(
            "seed {}, t={}ms ({:?}): {what}",
            self.seed, self.now, self.isolation
        )
    }

    pub fn run(mut self, duration: u64) -> Result<Report, String> {
        self.schedule(100, Event::PdRound);
        let d = 500 + self.rng.below(1500);
        self.schedule(d, Event::Crash);
        let d = 500 + self.rng.below(2000);
        self.schedule(d, Event::Partition);
        self.pump();
        self.run_until(duration)?;

        // Heal, stop the clients, and let everything settle.
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
        self.stop.set(true);
        let deadline = self.now + 15_000;
        while self.exec.live_tasks() > 0 && self.now < deadline {
            self.run_until(self.now + 100)?;
        }
        if self.exec.live_tasks() > 0 {
            return Err(self.fail(format!(
                "{} clients still stuck 15s after healing",
                self.exec.live_tasks()
            )));
        }

        // Settle every transaction whose outcome the client never learned:
        // its primary key decides, and expired locks are rolled back.
        let unknown: Vec<(usize, u64, Vec<u8>)> = self
            .records
            .borrow()
            .iter()
            .enumerate()
            .filter(|(_, r)| r.outcome == Outcome::Unknown)
            .filter_map(|(i, r)| r.primary.clone().map(|p| (i, r.start_ts, p)))
            .collect();
        let resolved: Rc<RefCell<Vec<Resolution>>> = Rc::new(RefCell::new(Vec::new()));
        {
            let kv = KvClient::new(self.io.clone());
            let resolved = resolved.clone();
            self.exec.spawn(async move {
                for (i, start_ts, primary) in unknown {
                    let mut status = Err("unresolved".to_string());
                    for _ in 0..20 {
                        match kv
                            .call(Request::CheckTxnStatus {
                                primary: primary.clone(),
                                start_ts,
                            })
                            .await
                        {
                            Ok(Response::Status(TxnStatus::Locked)) => kv.io.sleep(500).await,
                            Ok(Response::Status(s)) => {
                                status = Ok(s);
                                break;
                            }
                            other => status = Err(format!("{other:?}")),
                        }
                    }
                    resolved.borrow_mut().push((i, status));
                }
            });
        }
        self.pump();
        let deadline = self.now + 60_000;
        while self.exec.live_tasks() > 0 && self.now < deadline {
            self.run_until(self.now + 100)?;
        }
        for (i, status) in resolved.borrow().iter() {
            let mut records = self.records.borrow_mut();
            let r = &mut records[*i];
            r.outcome = match status {
                Ok(TxnStatus::Committed(c)) => Outcome::Committed(*c),
                Ok(_) => Outcome::Aborted,
                Err(e) => {
                    return Err(self.fail(format!("could not learn the fate of T{}: {e}", r.id)));
                }
            };
            self.report.unknown_resolved += 1;
        }

        self.check()?;
        self.report.ranges = self.pd.borrow().descriptors().len() as u64;
        Ok(self.report)
    }

    fn check(&mut self) -> Result<(), String> {
        let records = self.records.borrow();
        let mut committed = Vec::new();
        for r in records.iter() {
            match r.outcome {
                Outcome::Committed(commit_ts) => {
                    committed.push(CommittedTxn {
                        id: r.id,
                        start_ts: r.start_ts,
                        commit_ts,
                        began: r.began,
                        finished: r.finished,
                        reads: r.reads.clone(),
                        writes: r.writes.clone(),
                    });
                }
                Outcome::Aborted => self.report.aborted += 1,
                Outcome::Unknown => {}
            }
        }
        self.report.committed = committed.len() as u64;
        let serializable = self.isolation == Isolation::Serializable;
        let result = isolation::check(&committed, serializable).map_err(|e| self.fail(e))?;
        self.report.dependency_edges = result.edges as u64;
        self.report.write_skew_seen = result.cycle.is_some();
        Ok(())
    }

    /// Run client tasks, then turn what they sent into network events.
    fn pump(&mut self) {
        loop {
            self.io.set_now(self.now);
            self.exec.run_ready();
            let out = self.io.take_outbox();
            if out.is_empty() {
                break;
            }
            for (id, node, range, req) in out {
                let d = 1 + self.rng.below(5);
                self.schedule(d, Event::Arrive(node, range, id, req));
            }
        }
        if let Some(t) = self.io.next_timer()
            && self.wake_at.is_none_or(|w| w > t || w <= self.now)
        {
            self.wake_at = Some(t);
            let d = t.saturating_sub(self.now).max(1);
            self.schedule(d, Event::Wake);
        }
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
            self.pump();
        }
        self.now = self.now.max(end);
        self.pump();
        Ok(())
    }

    fn up(&self, id: NodeId) -> bool {
        self.nodes.get(&id).is_some_and(|n| n.store.is_some())
    }

    fn store(&mut self, id: NodeId) -> Option<&mut Store<SimFs>> {
        let now = self.now;
        let node = self.nodes.get_mut(&id)?;
        let offset = node.offset;
        let store = node.store.as_mut()?;
        store.set_clock(now + offset);
        Some(store)
    }

    fn handle(&mut self, event: Event) -> Result<(), String> {
        match event {
            Event::Deliver(to, m) => {
                if let Some(store) = self.store(to) {
                    store
                        .step(m)
                        .map_err(|e| format!("seed {}: step failed: {e}", self.seed))?;
                    self.process(to)?;
                }
            }
            Event::Tick(id) => {
                let tick_ms = self.nodes[&id].tick_ms;
                if let Some(store) = self.store(id) {
                    store.tick();
                    self.process(id)?;
                }
                self.schedule(tick_ms, Event::Tick(id));
            }
            Event::Crash => {
                if self.faults {
                    let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
                    let id = ids[self.rng.below(ids.len() as u64) as usize];
                    let up = self.nodes.values().filter(|n| n.store.is_some()).count();
                    if self.up(id) && up > self.nodes.len() / 2 + 1 {
                        let node = self.nodes.get_mut(&id).expect("node");
                        node.store = None;
                        node.fs.crash();
                        self.report.crashes += 1;
                        let d = 50 + self.rng.below(1500);
                        self.schedule(d, Event::Restart(id));
                    }
                    let d = 200 + self.rng.below(1500);
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
                    let d = 300 + self.rng.below(2500);
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
            Event::Arrive(node, range, id, req) => {
                if let Some(store) = self.store(node) {
                    store.submit(range, id, req);
                    self.process(node)?;
                }
            }
            Event::Respond(id, result) => self.io.deliver(id, result),
            Event::Wake => self.wake_at = None,
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
        Ok(())
    }

    fn process(&mut self, id: NodeId) -> Result<(), String> {
        let store = self.store(id).expect("up");
        if let Err(e) = store.process() {
            return Err(self.fail(format!("node {id}: {e}")));
        }
        let messages = store.take_messages();
        let responses = store.take_responses();
        for (to, m) in messages {
            let connected = self.group.get(&id) == self.group.get(&to);
            if !connected || self.rng.chance(self.drop_percent) {
                continue;
            }
            let d = 1 + self.rng.below(10);
            self.schedule(d, Event::Deliver(to, m));
        }
        for (req_id, result) in responses {
            if req_id < PD_REQ_BASE {
                let d = 1 + self.rng.below(5);
                self.schedule(d, Event::Respond(req_id, result));
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
                self.pd.borrow_mut().report(id, now, reports);
            }
        }
        let actions = self.pd.borrow_mut().schedule(now);
        for action in actions {
            match action {
                Action::Submit { node, range, req } => {
                    self.next_pd_req += 1;
                    let id = self.next_pd_req;
                    if let Some(store) = self.store(node) {
                        store.submit(range, id, req);
                        self.process(node)?;
                    }
                }
                Action::Gc {
                    node,
                    range,
                    replica,
                } => {
                    if let Some(store) = self.store(node) {
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
}

/// One client: run random transactions until told to stop.
async fn client_loop(
    kv: KvClient,
    opts: TxnOptions,
    records: Rc<RefCell<Vec<Record>>>,
    stop: Rc<Cell<bool>>,
    key_count: u64,
    seed: u64,
    client: u64,
) {
    let mut rng = Rng::new(seed);
    let mut n = 0;
    while !stop.get() {
        n += 1;
        let id = client * 1_000_000 + n;
        let began = kv.io.now();
        let mut txn = match Txn::begin(kv.clone(), opts.clone()).await {
            Ok(t) => t,
            Err(_) => {
                kv.io.sleep(20).await;
                continue;
            }
        };
        let mut record = Record {
            id,
            began,
            finished: 0,
            start_ts: txn.start_ts,
            primary: None,
            reads: Vec::new(),
            writes: Vec::new(),
            outcome: Outcome::Aborted,
        };
        let key = |i: u64| format!("key{i:03}").into_bytes();
        let mut failed = false;
        for _ in 0..1 + rng.below(4) {
            let roll = rng.below(100);
            if roll < 45 {
                let k = key(rng.below(key_count));
                if record.writes.contains(&k) {
                    continue;
                }
                match txn.get(&k).await {
                    Ok(v) => record.reads.push((k, v.and_then(|v| writer_of(&v)))),
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            } else if roll < 55 {
                // A scan across whatever ranges the keys now span.
                let a = rng.below(key_count);
                let b = a + 1 + rng.below(key_count - a);
                match txn.scan(&key(a), &key(b), usize::MAX / 4).await {
                    Ok(rows) => {
                        let seen: BTreeMap<Vec<u8>, Vec<u8>> = rows.into_iter().collect();
                        for i in a..b {
                            let k = key(i);
                            if !record.writes.contains(&k)
                                && !record.reads.iter().any(|(r, _)| *r == k)
                            {
                                let w = seen.get(&k).and_then(|v| writer_of(v));
                                record.reads.push((k, w));
                            }
                        }
                    }
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            } else {
                let k = key(rng.below(key_count));
                if !record.writes.contains(&k) {
                    txn.put(&k, &value_of(id));
                    record.writes.push(k);
                }
            }
        }
        record.writes.sort();
        record.primary = record.writes.first().cloned();
        if failed {
            txn.rollback().await;
        } else {
            record.outcome = match txn.commit().await {
                Ok(commit_ts) => Outcome::Committed(commit_ts),
                Err(TxnError::Unknown) => Outcome::Unknown,
                Err(_) => Outcome::Aborted,
            };
        }
        record.finished = if record.outcome == Outcome::Unknown {
            u64::MAX
        } else {
            kv.io.now()
        };
        if record.outcome == Outcome::Committed(record.start_ts) {
            // Read-only: serializable at its snapshot.
            record.writes.clear();
        }
        records.borrow_mut().push(record);
        kv.io.sleep(rng.below(10)).await;
    }
}

pub fn run(seed: u64, duration_ms: u64, fault: TxnSimFault) -> Result<Report, String> {
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
            Err(format!("seed {seed}: panicked: {msg}"))
        }
    }
}
