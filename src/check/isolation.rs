//! An isolation checker for transaction histories, in the spirit of Jepsen's
//! Elle: it reconstructs what every transaction observed and proves the
//! history is **snapshot isolated** or **serializable**, or prints the
//! anomaly.
//!
//! Every write installs a value unique to its transaction, so each read
//! names exactly which transaction's version it saw.
//!
//! Snapshot isolation is checked against the timestamps the database used:
//! - every read returns the newest version committed at or before the
//!   reader's snapshot (no dirty, stale, or fractured reads);
//! - two transactions that wrote the same key never overlap (no lost
//!   updates: first committer wins);
//! - a transaction that began after another finished has a later snapshot
//!   than that one's commit (timestamps respect real time).
//!
//! Serializability additionally requires the dependency graph (Adya's
//! ww, wr and rw edges) to be acyclic: then some serial order explains
//! every read. Snapshot isolation famously allows cycles through two rw
//! edges (write skew); serializable isolation must not.

use std::collections::BTreeMap;

pub type TxnId = u64;

#[derive(Clone, Debug)]
pub struct CommittedTxn {
    pub id: TxnId,
    pub start_ts: u64,
    pub commit_ts: u64,
    /// When the client began and learned the outcome (simulated ms).
    pub began: u64,
    pub finished: u64,
    /// `(key, writer)`: which transaction's version each read saw
    /// (`None`: the key had never been written).
    pub reads: Vec<(Vec<u8>, Option<TxnId>)>,
    pub writes: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Edge {
    /// ww: wrote the next version of a key.
    Ww,
    /// wr: read a version the other wrote.
    Wr,
    /// rw: overwrote a version the other read (an anti-dependency).
    Rw,
}

pub struct Report {
    pub txns: usize,
    pub edges: usize,
    /// A dependency cycle, if any (allowed under snapshot isolation only if
    /// it includes two adjacent rw edges).
    pub cycle: Option<String>,
}

fn name(k: &[u8]) -> String {
    String::from_utf8_lossy(k).into_owned()
}

/// Check a history. `serializable` demands an acyclic dependency graph.
pub fn check(txns: &[CommittedTxn], serializable: bool) -> Result<Report, String> {
    let by_id: BTreeMap<TxnId, &CommittedTxn> = txns.iter().map(|t| (t.id, t)).collect();

    // Version order per key: writers sorted by commit timestamp.
    let mut versions: BTreeMap<&[u8], Vec<&CommittedTxn>> = BTreeMap::new();
    for t in txns {
        for k in &t.writes {
            versions.entry(k.as_slice()).or_default().push(t);
        }
    }
    for vs in versions.values_mut() {
        vs.sort_by_key(|t| t.commit_ts);
        // First committer wins: no two writers of a key may overlap.
        for w in vs.windows(2) {
            let (a, b) = (w[0], w[1]);
            if b.start_ts < a.commit_ts {
                return Err(format!(
                    "lost update: T{} [{}, {}] and T{} [{}, {}] both wrote the same key and committed",
                    a.id, a.start_ts, a.commit_ts, b.id, b.start_ts, b.commit_ts
                ));
            }
        }
    }

    // Snapshot reads, and real-time order of timestamps.
    for t in txns {
        for (k, seen) in &t.reads {
            let expected = versions
                .get(k.as_slice())
                .and_then(|vs| {
                    vs.iter()
                        .rev()
                        .find(|w| w.commit_ts <= t.start_ts && w.id != t.id)
                })
                .map(|w| w.id);
            if *seen != expected {
                let what = match seen {
                    Some(w) if !by_id.contains_key(w) => {
                        format!("T{w}'s version, and T{w} never committed")
                    }
                    Some(w) => format!("T{w}'s version (committed at {})", by_id[w].commit_ts),
                    None => "no version".to_string(),
                };
                return Err(format!(
                    "T{} (snapshot at {}) read key {:?} and saw {what}; its snapshot holds {}",
                    t.id,
                    t.start_ts,
                    name(k),
                    expected.map_or("no version".to_string(), |e| format!("T{e}'s version")),
                ));
            }
        }
    }
    let mut by_finish: Vec<&CommittedTxn> = txns.iter().collect();
    by_finish.sort_by_key(|t| t.finished);
    let mut max_commit_finished: Vec<(u64, u64, TxnId)> = Vec::new(); // (finished, max commit_ts so far, txn)
    let mut best = (0u64, 0u64);
    for t in &by_finish {
        if t.commit_ts > best.0 {
            best = (t.commit_ts, t.id);
        }
        max_commit_finished.push((t.finished, best.0, best.1));
    }
    for t in txns {
        // Every transaction that finished before `t` began.
        let i = max_commit_finished.partition_point(|(f, _, _)| *f < t.began);
        if i > 0 {
            let (_, c, w) = max_commit_finished[i - 1];
            if c >= t.start_ts {
                return Err(format!(
                    "time went backwards: T{} began after T{w} committed at {c}, yet got snapshot {}",
                    t.id, t.start_ts
                ));
            }
        }
    }

    // The dependency graph.
    let mut edges: BTreeMap<TxnId, Vec<(TxnId, Edge)>> = BTreeMap::new();
    let mut n_edges = 0;
    let mut add = |from: TxnId, to: TxnId, e: Edge| {
        if from != to {
            edges.entry(from).or_default().push((to, e));
            n_edges += 1;
        }
    };
    for vs in versions.values() {
        for w in vs.windows(2) {
            add(w[0].id, w[1].id, Edge::Ww);
        }
    }
    for t in txns {
        for (k, seen) in &t.reads {
            let vs = versions.get(k.as_slice()).map(Vec::as_slice).unwrap_or(&[]);
            if let Some(w) = seen {
                add(*w, t.id, Edge::Wr);
            }
            // The version after the one we saw overwrote our read.
            let pos = match seen {
                Some(w) => vs.iter().position(|v| v.id == *w).map(|p| p + 1),
                None => Some(0),
            };
            if let Some(next) = pos.and_then(|p| vs.get(p)) {
                add(t.id, next.id, Edge::Rw);
            }
        }
    }
    let cycle = find_cycle(&edges);
    if let Some(c) = &cycle
        && serializable
    {
        return Err(format!("not serializable: dependency cycle {c}"));
    }
    Ok(Report {
        txns: txns.len(),
        edges: n_edges,
        cycle,
    })
}

/// Any cycle in the graph, as `T1 -ww-> T2 -rw-> T1`.
fn find_cycle(edges: &BTreeMap<TxnId, Vec<(TxnId, Edge)>>) -> Option<String> {
    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        New,
        Active,
        Done,
    }
    let nodes: Vec<TxnId> = edges.keys().copied().collect();
    let mut mark: BTreeMap<TxnId, Mark> = BTreeMap::new();
    for &root in &nodes {
        if mark.get(&root).copied().unwrap_or(Mark::New) != Mark::New {
            continue;
        }
        // Iterative DFS with the path kept for reporting.
        let mut stack: Vec<(TxnId, usize)> = vec![(root, 0)];
        let mut path: Vec<(TxnId, Edge)> = Vec::new();
        mark.insert(root, Mark::Active);
        while let Some(&mut (node, ref mut i)) = stack.last_mut() {
            let out = edges.get(&node).map(Vec::as_slice).unwrap_or(&[]);
            if *i < out.len() {
                let (next, e) = out[*i];
                *i += 1;
                match mark.get(&next).copied().unwrap_or(Mark::New) {
                    Mark::Active => {
                        // Found a cycle: from `next` along the path to `node`.
                        let start = stack
                            .iter()
                            .position(|(n, _)| *n == next)
                            .expect("on stack");
                        let mut s = format!("T{next}");
                        for (j, (_, edge)) in path.iter().enumerate().skip(start) {
                            let to = stack[j + 1].0;
                            s.push_str(&format!(
                                " -{}-> T{to}",
                                format!("{edge:?}").to_lowercase()
                            ));
                        }
                        s.push_str(&format!(" -{}-> T{next}", format!("{e:?}").to_lowercase()));
                        return Some(s);
                    }
                    Mark::New => {
                        mark.insert(next, Mark::Active);
                        path.push((node, e));
                        stack.push((next, 0));
                    }
                    Mark::Done => {}
                }
            } else {
                mark.insert(node, Mark::Done);
                stack.pop();
                path.pop();
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(
        id: TxnId,
        start: u64,
        commit: u64,
        reads: &[(&str, Option<TxnId>)],
        writes: &[&str],
    ) -> CommittedTxn {
        CommittedTxn {
            id,
            start_ts: start,
            commit_ts: commit,
            began: start,
            finished: commit,
            reads: reads
                .iter()
                .map(|(k, w)| (k.as_bytes().to_vec(), *w))
                .collect(),
            writes: writes.iter().map(|k| k.as_bytes().to_vec()).collect(),
        }
    }

    #[test]
    fn a_serial_history_passes() {
        let h = vec![
            t(1, 1, 2, &[], &["x"]),
            t(2, 3, 4, &[("x", Some(1))], &["y"]),
            t(3, 5, 6, &[("y", Some(2))], &[]),
        ];
        assert!(check(&h, true).unwrap().cycle.is_none());
    }

    #[test]
    fn write_skew_is_snapshot_isolated_but_not_serializable() {
        // Both read x and y at the initial state, each writes one of them.
        let h = vec![
            t(1, 1, 3, &[("x", None), ("y", None)], &["x"]),
            t(2, 2, 4, &[("x", None), ("y", None)], &["y"]),
        ];
        let si = check(&h, false).unwrap();
        assert!(si.cycle.is_some(), "write skew is a cycle of two rw edges");
        let err = check(&h, true).err().unwrap();
        assert!(err.contains("not serializable"), "{err}");
    }

    #[test]
    fn a_lost_update_is_caught() {
        let h = vec![
            t(1, 1, 3, &[("x", None)], &["x"]),
            t(2, 2, 4, &[("x", None)], &["x"]),
        ];
        assert!(check(&h, false).err().unwrap().contains("lost update"));
    }

    #[test]
    fn a_stale_read_is_caught() {
        let h = vec![t(1, 1, 2, &[], &["x"]), t(2, 5, 6, &[("x", None)], &[])];
        assert!(check(&h, false).err().unwrap().contains("read key"));
    }
}
