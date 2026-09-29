//! Runs a whole cluster inside one process: several stores, the placement
//! driver, the async runtime for client sessions, and an in-memory network
//! between the nodes. The server drives it with the wall clock and real
//! disks; tests drive it with virtual time and simulated disks.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use crate::error::Result;
use crate::kv::cmd::ReqId;
use crate::kv::keys::{self, RangeDescriptor};
use crate::kv::pd::{Action, Pd, PdConfig};
use crate::kv::store::{RangeMessage, Store, StoreConfig};
use crate::raft::NodeId;
use crate::runtime::{Executor, Io};
use crate::storage::fs::Fs;

const PD_REQ_BASE: ReqId = 1 << 62;
const TICK_MS: u64 = 10;
const PD_EVERY_MS: u64 = 250;

pub struct LocalCluster<F: Fs> {
    stores: BTreeMap<NodeId, Store<F>>,
    pd: Rc<RefCell<Pd>>,
    pub io: Rc<Io>,
    pub exec: Executor,
    network: VecDeque<(NodeId, RangeMessage)>,
    now: u64,
    last_tick: u64,
    last_pd: u64,
    next_pd_req: ReqId,
}

impl<F: Fs> LocalCluster<F> {
    /// Open (or create) a cluster with one store per filesystem. A fresh
    /// cluster gets its first range, spanning every key, on all nodes.
    pub fn open(filesystems: Vec<F>, cfg: StoreConfig, split_keys: u64) -> Result<LocalCluster<F>> {
        let mut stores = BTreeMap::new();
        for (i, fs) in filesystems.into_iter().enumerate() {
            let id = i as NodeId + 1;
            stores.insert(id, Store::open(fs, id, cfg.clone(), 0x5EED ^ id)?);
        }
        let fresh = stores
            .values()
            .all(|s| s.report().is_empty() && !s.has_replica(1));
        if fresh {
            let n = stores.len().min(3) as NodeId;
            let first = RangeDescriptor {
                id: 1,
                start: Vec::new(),
                end: Vec::new(),
                replicas: (1..=n).map(|node| keys::replica_id(node, 0)).collect(),
                generation: 0,
                next_incarnation: 1,
            };
            for node in 1..=n {
                stores
                    .get_mut(&node)
                    .expect("store")
                    .bootstrap(first.clone())?;
            }
        }
        let pd = Rc::new(RefCell::new(Pd::new(
            PdConfig {
                replication: stores.len().min(3),
                split_keys,
                dead_after: 5_000,
                rebalance: true,
            },
            // Raised past every existing range id as stores report in.
            2,
        )));
        let router = {
            let pd = pd.clone();
            move |key: &[u8]| pd.borrow().route(key)
        };
        Ok(LocalCluster {
            stores,
            pd,
            io: Io::new(0xC0FFEE, router),
            exec: Executor::default(),
            network: VecDeque::new(),
            now: 0,
            last_tick: 0,
            last_pd: 0,
            next_pd_req: PD_REQ_BASE,
        })
    }

    pub fn now(&self) -> u64 {
        self.now
    }

    /// Advance to `now` (milliseconds) and do everything that is due.
    /// Returns whether any work was done.
    pub fn step(&mut self, now: u64) -> Result<bool> {
        self.now = self.now.max(now);
        let mut worked = false;
        for s in self.stores.values_mut() {
            s.set_clock(self.now);
        }
        if self.now >= self.last_tick + TICK_MS {
            self.last_tick = self.now;
            for s in self.stores.values_mut() {
                s.tick();
            }
            worked = true;
        }
        if self.now >= self.last_pd + PD_EVERY_MS || self.last_pd == 0 {
            self.last_pd = self.now.max(1);
            self.pd_round()?;
        }
        for _ in 0..1_000 {
            let mut busy = false;
            self.io.set_now(self.now);
            busy |= self.exec.run_ready() > 0;
            for (id, node, range, req) in self.io.take_outbox() {
                busy = true;
                if let Some(s) = self.stores.get_mut(&node) {
                    s.submit(range, id, req);
                }
            }
            let ids: Vec<NodeId> = self.stores.keys().copied().collect();
            for id in ids {
                let store = self.stores.get_mut(&id).expect("store");
                store.process()?;
                for (to, m) in store.take_messages() {
                    self.network.push_back((to, m));
                }
                for (req_id, result) in store.take_responses() {
                    busy = true;
                    if req_id < PD_REQ_BASE {
                        self.io.deliver(req_id, result);
                    }
                }
            }
            while let Some((to, m)) = self.network.pop_front() {
                busy = true;
                if let Some(s) = self.stores.get_mut(&to) {
                    s.step(m)?;
                }
            }
            if !busy {
                break;
            }
            worked = true;
        }
        Ok(worked)
    }

    fn pd_round(&mut self) -> Result<()> {
        for (&id, s) in &self.stores {
            self.pd.borrow_mut().report(id, self.now, s.report());
        }
        let actions = self.pd.borrow_mut().schedule(self.now);
        for a in actions {
            match a {
                Action::Submit { node, range, req } => {
                    self.next_pd_req += 1;
                    if let Some(s) = self.stores.get_mut(&node) {
                        s.submit(range, self.next_pd_req, req);
                    }
                }
                Action::Gc {
                    node,
                    range,
                    replica,
                } => {
                    if let Some(s) = self.stores.get_mut(&node) {
                        s.gc_replica(range, replica)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Run a future to completion on virtual time (tests and tools).
    pub fn block_on<T: 'static>(
        &mut self,
        fut: impl std::future::Future<Output = T> + 'static,
    ) -> Result<T> {
        let out: Rc<RefCell<Option<T>>> = Rc::new(RefCell::new(None));
        let slot = out.clone();
        self.exec.spawn(async move {
            let v = fut.await;
            *slot.borrow_mut() = Some(v);
        });
        let mut t = self.now;
        while out.borrow().is_none() {
            t += 1;
            self.step(t)?;
            if t > self.now + 600_000 {
                break;
            }
        }
        let v = out.borrow_mut().take();
        Ok(v.expect("the future did not finish within 10 simulated minutes"))
    }

    pub fn ranges(&self) -> usize {
        self.pd.borrow().descriptors().len()
    }
}
