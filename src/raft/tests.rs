use super::*;

/// A tiny synchronous network for unit tests: deliver every message
/// immediately, except to nodes that are cut off.
struct Net {
    nodes: BTreeMap<NodeId, Raft>,
    applied: BTreeMap<NodeId, Vec<Entry>>,
    cut: BTreeSet<NodeId>,
}

impl Net {
    fn new(n: u64) -> Net {
        let voters: Vec<NodeId> = (1..=n).collect();
        let nodes = voters
            .iter()
            .map(|&id| {
                let snap = SnapshotMeta {
                    index: 0,
                    term: 0,
                    voters: voters.clone(),
                };
                let raft = Raft::restore(
                    id,
                    Config::default(),
                    7,
                    HardState::default(),
                    snap,
                    vec![],
                    0,
                );
                (id, raft)
            })
            .collect();
        Net {
            nodes,
            applied: BTreeMap::new(),
            cut: BTreeSet::new(),
        }
    }

    fn settle(&mut self) {
        loop {
            let mut msgs = Vec::new();
            for (id, raft) in self.nodes.iter_mut() {
                while raft.has_ready() {
                    let r = raft.ready();
                    msgs.extend(r.messages);
                    self.applied.entry(*id).or_default().extend(r.committed);
                }
            }
            if msgs.is_empty() {
                return;
            }
            for m in msgs {
                if !self.cut.contains(&m.from)
                    && !self.cut.contains(&m.to)
                    && let Some(n) = self.nodes.get_mut(&m.to)
                {
                    n.step(m);
                }
            }
        }
    }

    fn elect(&mut self, id: NodeId) {
        self.nodes.get_mut(&id).unwrap().campaign(true);
        self.settle();
        assert!(self.nodes[&id].is_leader(), "node {id} was not elected");
    }

    fn commands(&self, id: NodeId) -> Vec<Vec<u8>> {
        self.applied
            .get(&id)
            .map(|es| {
                es.iter()
                    .filter_map(|e| match &e.data {
                        EntryData::Command(c) => Some(c.clone()),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[test]
fn a_single_node_elects_itself_and_commits() {
    let mut net = Net::new(1);
    net.elect(1);
    net.nodes
        .get_mut(&1)
        .unwrap()
        .propose(b"x".to_vec())
        .unwrap();
    net.settle();
    assert_eq!(net.commands(1), vec![b"x".to_vec()]);
}

#[test]
fn three_nodes_elect_and_replicate() {
    let mut net = Net::new(3);
    net.elect(1);
    assert!(net.nodes[&2].leader() == Some(1) && net.nodes[&3].leader() == Some(1));
    for i in 0..10u8 {
        net.nodes.get_mut(&1).unwrap().propose(vec![i]).unwrap();
    }
    net.settle();
    for id in 1..=3 {
        assert_eq!(net.commands(id).len(), 10, "node {id}");
    }
}

#[test]
fn a_minority_cannot_commit() {
    let mut net = Net::new(3);
    net.elect(1);
    net.cut.insert(2);
    net.cut.insert(3);
    net.nodes
        .get_mut(&1)
        .unwrap()
        .propose(b"lost".to_vec())
        .unwrap();
    net.settle();
    assert!(net.commands(1).is_empty());
}

#[test]
fn a_divergent_follower_is_repaired() {
    let mut net = Net::new(3);
    net.elect(1);
    // Node 1 appends entries only it will ever hold.
    net.cut.insert(1);
    for i in 0..5u8 {
        net.nodes.get_mut(&1).unwrap().propose(vec![i]).unwrap();
    }
    net.settle();
    // Node 2 leads the others and commits different entries.
    net.nodes.get_mut(&1).unwrap().become_follower(1, None);
    net.elect(2);
    for i in 10..13u8 {
        net.nodes.get_mut(&2).unwrap().propose(vec![i]).unwrap();
    }
    net.settle();
    net.cut.clear();
    for _ in 0..4 {
        net.nodes.get_mut(&2).unwrap().tick();
        net.settle();
    }
    assert_eq!(net.commands(1), vec![vec![10], vec![11], vec![12]]);
    assert_eq!(net.nodes[&1].last_index(), net.nodes[&2].last_index());
}

#[test]
fn read_index_needs_a_quorum() {
    let mut net = Net::new(3);
    net.elect(1);
    net.nodes
        .get_mut(&1)
        .unwrap()
        .propose(b"a".to_vec())
        .unwrap();
    net.settle();
    net.cut.insert(2);
    net.cut.insert(3);
    net.nodes.get_mut(&1).unwrap().read_index(1).unwrap();
    let r = net.nodes.get_mut(&1).unwrap().ready();
    assert!(r.reads.is_empty(), "served a read without a quorum");
    net.cut.clear();
    net.nodes.get_mut(&1).unwrap().read_index(2).unwrap();
    let mut reads = Vec::new();
    loop {
        let mut msgs = Vec::new();
        for raft in net.nodes.values_mut() {
            while raft.has_ready() {
                let r = raft.ready();
                reads.extend(r.reads);
                msgs.extend(r.messages);
            }
        }
        if msgs.is_empty() {
            break;
        }
        for m in msgs {
            net.nodes.get_mut(&m.to).unwrap().step(m);
        }
    }
    assert!(
        reads
            .iter()
            .any(|&(id, index)| id == 2 && index == net.nodes[&1].commit_index())
    );
}

#[test]
fn membership_change_adds_a_voter() {
    let mut net = Net::new(3);
    net.nodes
        .insert(4, Raft::new_empty(4, Config::default(), 9));
    net.elect(1);
    net.nodes
        .get_mut(&1)
        .unwrap()
        .propose(b"before".to_vec())
        .unwrap();
    net.settle();
    net.nodes
        .get_mut(&1)
        .unwrap()
        .propose_config(vec![1, 2, 3, 4])
        .unwrap();
    net.settle();
    assert_eq!(net.nodes[&1].voters(), &[1, 2, 3, 4]);
    net.nodes
        .get_mut(&1)
        .unwrap()
        .propose(b"after".to_vec())
        .unwrap();
    net.settle();
    assert_eq!(net.nodes[&4].voters(), &[1, 2, 3, 4]);
    assert_eq!(net.commands(4), vec![b"before".to_vec(), b"after".to_vec()]);
    // Removing two voters at once is refused.
    assert_eq!(
        net.nodes.get_mut(&1).unwrap().propose_config(vec![1, 2]),
        Err(ProposeError::Busy)
    );
}

#[test]
fn leadership_transfer() {
    let mut net = Net::new(3);
    net.elect(1);
    net.nodes.get_mut(&1).unwrap().transfer_leader(3);
    net.settle();
    assert!(net.nodes[&3].is_leader());
    assert!(!net.nodes[&1].is_leader());
}
