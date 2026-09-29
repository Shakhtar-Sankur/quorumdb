//! The placement driver decides where ranges live, like TiKV's PD. It
//! holds only soft state, rebuilt from what stores report, and acts only by
//! submitting ordinary requests to range leaders, so a wrong or stale
//! decision can delay the cluster but never corrupt it: every split and
//! membership change is still decided by Raft, checked against the range's
//! generation when applied.
//!
//! Each round it:
//! - splits ranges that grew past a size threshold, at their middle key;
//! - replaces replicas on nodes that stopped reporting;
//! - brings every range to the replication factor, preferring the least
//!   loaded live nodes;
//! - moves replicas from the most to the least loaded node;
//! - spreads leadership evenly;
//! - tells stores to garbage-collect replicas their range has removed.

use std::collections::{BTreeMap, BTreeSet};

use crate::kv::cmd::Request;
use crate::kv::keys::{self, RangeDescriptor, RangeId, ReplicaId};
use crate::kv::store::ReplicaReport;
use crate::raft::NodeId;

#[derive(Clone, Debug)]
pub struct PdConfig {
    pub replication: usize,
    /// Split a range once it holds more keys than this.
    pub split_keys: u64,
    /// A node that has not reported for this long (ms) is considered dead.
    pub dead_after: u64,
    pub rebalance: bool,
}

impl Default for PdConfig {
    fn default() -> Self {
        PdConfig {
            replication: 3,
            split_keys: 10_000,
            dead_after: 5_000,
            rebalance: true,
        }
    }
}

/// Something the placement driver wants done.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Submit this request to the range's leader on `node`.
    Submit {
        node: NodeId,
        range: RangeId,
        req: Request,
    },
    /// Tell `node` to destroy replica `replica` of `range`.
    Gc {
        node: NodeId,
        range: RangeId,
        replica: ReplicaId,
    },
}

pub struct Pd {
    cfg: PdConfig,
    next_range: RangeId,
    /// The newest descriptor seen for each range.
    ranges: BTreeMap<RangeId, RangeDescriptor>,
    leaders: BTreeMap<RangeId, NodeId>,
    sizes: BTreeMap<RangeId, (u64, Option<Vec<u8>>)>,
    last_seen: BTreeMap<NodeId, u64>,
    /// What each node last said it holds: range -> (generation, replica id).
    held: BTreeMap<NodeId, BTreeMap<RangeId, (u64, ReplicaId)>>,
    /// Rebalancing moves in progress: once the new replica is added, drop
    /// the one on this node.
    moving_from: BTreeMap<RangeId, NodeId>,
    /// Ranges with an action in flight, and when it was issued.
    busy: BTreeMap<RangeId, u64>,
}

impl Pd {
    pub fn new(cfg: PdConfig, first_free_range: RangeId) -> Pd {
        Pd {
            cfg,
            next_range: first_free_range,
            ranges: BTreeMap::new(),
            leaders: BTreeMap::new(),
            sizes: BTreeMap::new(),
            last_seen: BTreeMap::new(),
            held: BTreeMap::new(),
            moving_from: BTreeMap::new(),
            busy: BTreeMap::new(),
        }
    }

    pub fn report(&mut self, node: NodeId, now: u64, reports: Vec<ReplicaReport>) {
        self.last_seen.insert(node, now);
        let mut held = BTreeMap::new();
        for r in reports {
            let id = r.desc.id;
            held.insert(id, (r.desc.generation, r.replica));
            let newer = self
                .ranges
                .get(&id)
                .is_none_or(|d| r.desc.generation >= d.generation);
            if newer {
                if self
                    .ranges
                    .get(&id)
                    .is_some_and(|d| r.desc.generation > d.generation)
                {
                    self.busy.remove(&id);
                }
                self.ranges.insert(id, r.desc.clone());
            }
            if r.leader && newer {
                self.leaders.insert(id, node);
                self.sizes.insert(id, (r.keys, r.mid_key));
            }
        }
        self.held.insert(node, held);
    }

    /// The range that owns `key`, and its leader if known. When descriptors
    /// overlap during a split, the one starting latest is the newest.
    pub fn route(&self, key: &[u8]) -> Option<(RangeDescriptor, Option<NodeId>)> {
        self.ranges
            .values()
            .filter(|d| d.contains(key))
            .max_by(|a, b| a.start.cmp(&b.start))
            .map(|d| (d.clone(), self.leaders.get(&d.id).copied()))
    }

    pub fn descriptors(&self) -> Vec<RangeDescriptor> {
        self.ranges.values().cloned().collect()
    }

    fn live(&self, now: u64) -> BTreeSet<NodeId> {
        self.last_seen
            .iter()
            .filter(|(_, t)| now.saturating_sub(**t) < self.cfg.dead_after)
            .map(|(&n, _)| n)
            .collect()
    }

    pub fn schedule(&mut self, now: u64) -> Vec<Action> {
        let mut actions = Vec::new();
        let live = self.live(now);
        // Anything in flight for too long probably failed: forget it.
        self.busy.retain(|_, &mut t| now.saturating_sub(t) < 3_000);

        // Garbage: replicas whose range has moved on without them.
        // A replica the newest descriptor has removed is gone for good,
        // even if the node was re-added since (under a newer replica id).
        // One newer than that descriptor may be joining: it is left alone.
        for (&node, held) in &self.held {
            for (&range, &(_, replica)) in held {
                if let Some(d) = self.ranges.get(&range)
                    && d.has_removed(replica)
                {
                    actions.push(Action::Gc {
                        node,
                        range,
                        replica,
                    });
                }
            }
        }

        let mut load: BTreeMap<NodeId, usize> = live.iter().map(|&n| (n, 0)).collect();
        let mut leads: BTreeMap<NodeId, usize> = live.iter().map(|&n| (n, 0)).collect();
        for d in self.ranges.values() {
            for n in d.nodes() {
                *load.entry(n).or_default() += 1;
            }
            if let Some(l) = self.leaders.get(&d.id) {
                *leads.entry(*l).or_default() += 1;
            }
        }

        let ranges: Vec<RangeDescriptor> = self.ranges.values().cloned().collect();
        for d in ranges {
            if self.busy.contains_key(&d.id) {
                continue;
            }
            let Some(&leader) = self.leaders.get(&d.id) else {
                continue;
            };
            if !live.contains(&leader) {
                continue;
            }
            let submit = |req| Action::Submit {
                node: leader,
                range: d.id,
                req,
            };
            let nodes = d.nodes();
            let dead: Vec<NodeId> = nodes
                .iter()
                .copied()
                .filter(|n| !live.contains(n))
                .collect();
            let least_loaded = |exclude: &[NodeId], load: &BTreeMap<NodeId, usize>| {
                load.iter()
                    .filter(|(n, _)| live.contains(n) && !exclude.contains(n))
                    .min_by_key(|(n, l)| (**l, **n))
                    .map(|(&n, _)| n)
            };
            let without = |node: NodeId| -> Vec<ReplicaId> {
                d.replicas
                    .iter()
                    .copied()
                    .filter(|&r| keys::node_of(r) != node)
                    .collect()
            };
            // A new replica id: the node, and an incarnation no earlier
            // member of this range can have used.
            let with = |node: NodeId| -> Vec<ReplicaId> {
                let mut r = d.replicas.clone();
                r.push(keys::replica_id(node, d.next_incarnation));
                r.sort_unstable();
                r
            };
            if !nodes
                .iter()
                .any(|&n| Some(n) == self.moving_from.get(&d.id).copied())
            {
                self.moving_from.remove(&d.id);
            }

            let action = if let Some((keys, Some(mid))) = self.sizes.get(&d.id).cloned()
                && keys > self.cfg.split_keys
            {
                let new_range = self.next_range;
                self.next_range += 1;
                self.sizes.remove(&d.id);
                Some(submit(Request::Split {
                    key: mid,
                    new_range,
                }))
            } else if nodes.len() > self.cfg.replication {
                // Over-replicated: finish a planned move, else drop a dead
                // replica, else the most loaded one; never the leader.
                let planned = self
                    .moving_from
                    .get(&d.id)
                    .copied()
                    .filter(|&n| n != leader);
                let victim = planned.or_else(|| dead.first().copied()).or_else(|| {
                    nodes
                        .iter()
                        .copied()
                        .filter(|&n| n != leader)
                        .max_by_key(|n| (load.get(n).copied().unwrap_or(0), *n))
                });
                match (planned, victim) {
                    // The planned source is the leader: hand leadership away first.
                    (None, _) if self.moving_from.get(&d.id) == Some(&leader) => {
                        let to = nodes
                            .iter()
                            .copied()
                            .find(|&n| n != leader && live.contains(&n));
                        to.map(|t| submit(Request::TransferLeader { to: t }))
                    }
                    (_, Some(v)) => {
                        self.moving_from.remove(&d.id);
                        Some(submit(Request::ChangeReplicas {
                            replicas: without(v),
                        }))
                    }
                    _ => None,
                }
            } else if nodes.len() < self.cfg.replication || !dead.is_empty() {
                // Under-replicated, or a replica is dead: add a live node
                // first, then the dead one is dropped as surplus.
                least_loaded(&nodes, &load)
                    .map(|n| submit(Request::ChangeReplicas { replicas: with(n) }))
            } else if self.cfg.rebalance {
                let most = nodes
                    .iter()
                    .copied()
                    .max_by_key(|n| (load.get(n).copied().unwrap_or(0), *n));
                let fewest = least_loaded(&nodes, &load);
                match (most, fewest) {
                    (Some(m), Some(f)) if load[&m] > load[&f] + 1 => {
                        *load.get_mut(&m).expect("live") -= 1;
                        *load.get_mut(&f).expect("live") += 1;
                        self.moving_from.insert(d.id, m);
                        Some(submit(Request::ChangeReplicas { replicas: with(f) }))
                    }
                    _ => {
                        // Leadership balance.
                        let target = nodes
                            .iter()
                            .copied()
                            .filter(|n| live.contains(n))
                            .min_by_key(|n| (leads.get(n).copied().unwrap_or(0), *n));
                        match target {
                            Some(t)
                                if leads.get(&leader).copied().unwrap_or(0)
                                    > leads.get(&t).copied().unwrap_or(0) + 1 =>
                            {
                                *leads.get_mut(&leader).expect("live") -= 1;
                                *leads.entry(t).or_default() += 1;
                                Some(submit(Request::TransferLeader { to: t }))
                            }
                            _ => None,
                        }
                    }
                }
            } else {
                None
            };
            if let Some(a) = action {
                self.busy.insert(d.id, now);
                actions.push(a);
            }
        }
        actions
    }
}
