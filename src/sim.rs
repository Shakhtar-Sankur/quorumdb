//! Deterministic simulation. One seed drives a random workload, random
//! engine settings, and random power losses on a simulated disk. After every
//! crash the recovered database is checked against a model:
//!
//! - Nothing durable is lost: every write that was synced, flushed, or made
//!   in `SyncMode::Always` is still there.
//! - Nothing is reordered or invented: the recovered state equals the model
//!   after some prefix of the writes. Writes after the durable point may be
//!   lost, but only from the end.
//!
//! A failing seed replays exactly, so any bug it finds can be debugged.

use std::collections::BTreeMap;

use crate::engine::{Db, Fault, Options, SyncMode};
use crate::fs::SimFs;
use crate::rng::Rng;
use crate::wal::Op;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub seed: u64,
    pub steps: usize,
    pub writes: u64,
    pub crashes: u64,
    pub torn_writes: u64,
    pub flushes: u64,
    pub compactions: u64,
    pub lost_unsynced: u64,
}

fn apply(model: &mut Model, op: &Op) {
    match op {
        Op::Put(k, v) => {
            model.insert(k.clone(), v.clone());
        }
        Op::Delete(k) => {
            model.remove(k);
        }
    }
}

/// Run one seed. `Err` carries a description of the first violation found.
pub fn run(seed: u64, steps: usize, fault: Fault) -> Result<Report, String> {
    let mut rng = Rng::new(seed);
    let fs = SimFs::new(rng.next_u64());
    let opts = Options {
        sync: if rng.chance(50) {
            SyncMode::Always
        } else {
            SyncMode::Manual
        },
        memtable_bytes: 64 + rng.below(1024) as usize,
        compact_at: 2 + rng.below(5) as usize,
        fault,
    };
    let fail =
        |step: usize, what: String| format!("seed {seed}, step {step} ({:?}): {what}", opts.sync);
    let mut db =
        Db::open(fs.clone(), opts.clone()).map_err(|e| fail(0, format!("open failed: {e}")))?;

    // `durable` is the model at the last point the engine promised
    // durability; `pending` holds the acknowledged writes since then.
    let mut durable = Model::new();
    let mut pending: Vec<Op> = Vec::new();
    let mut current = Model::new();
    let mut report = Report {
        seed,
        steps,
        ..Report::default()
    };
    let (mut flushes, mut compactions) = (0, 0);

    let key_space = 8 + rng.below(56);
    for step in 1..=steps {
        let roll = rng.below(100);
        if roll < 70 {
            let key = format!("k{:03}", rng.below(key_space)).into_bytes();
            let op = if roll < 55 {
                let len = rng.below(24) as usize;
                Op::Put(key, rng.bytes(len))
            } else {
                Op::Delete(key)
            };
            let result = match &op {
                Op::Put(k, v) => db.put(k, v),
                Op::Delete(k) => db.delete(k),
            };
            result.map_err(|e| fail(step, format!("write failed: {e}")))?;
            apply(&mut current, &op);
            pending.push(op);
            report.writes += 1;
            if opts.sync == SyncMode::Always {
                durable = current.clone();
                pending.clear();
            }
        } else if roll < 76 {
            db.sync()
                .map_err(|e| fail(step, format!("sync failed: {e}")))?;
            durable = current.clone();
            pending.clear();
        } else if roll < 79 {
            db.flush()
                .map_err(|e| fail(step, format!("flush failed: {e}")))?;
            durable = current.clone();
            pending.clear();
        } else if roll < 83 {
            let stats = db.stats();
            flushes += stats.flushes;
            compactions += stats.compactions;
            drop(db);
            fs.crash();
            db = Db::open(fs.clone(), opts.clone())
                .map_err(|e| fail(step, format!("recovery failed: {e}")))?;
            let recovered: Model = db.scan().into_iter().collect();

            // Find the prefix of pending writes the recovered state matches.
            let mut state = durable.clone();
            let mut kept = 0;
            let mut matched = state == recovered;
            while !matched && kept < pending.len() {
                apply(&mut state, &pending[kept]);
                kept += 1;
                matched = state == recovered;
            }
            if !matched {
                let lost: Vec<_> = durable
                    .keys()
                    .filter(|k| !recovered.contains_key(*k))
                    .take(5)
                    .collect();
                return Err(fail(
                    step,
                    format!(
                        "recovered state matches no prefix of the {} acknowledged writes since the durable point \
                         ({} keys durable, {} recovered; durable keys missing: {:?})",
                        pending.len(),
                        durable.len(),
                        recovered.len(),
                        lost.iter()
                            .map(|k| String::from_utf8_lossy(k))
                            .collect::<Vec<_>>()
                    ),
                ));
            }
            report.lost_unsynced += (pending.len() - kept) as u64;
            durable = state;
            current = durable.clone();
            pending.clear();
        } else {
            let key = format!("k{:03}", rng.below(key_space)).into_bytes();
            let got = db.get(&key);
            if got.as_ref() != current.get(&key) {
                return Err(fail(
                    step,
                    format!(
                        "get {:?} returned {got:?}, expected {:?}",
                        String::from_utf8_lossy(&key),
                        current.get(&key)
                    ),
                ));
            }
        }
    }

    let final_state: Model = db.scan().into_iter().collect();
    if final_state != current {
        return Err(fail(steps, "final scan differs from the model".to_string()));
    }
    let stats = db.stats();
    report.flushes = flushes + stats.flushes;
    report.compactions = compactions + stats.compactions;
    report.crashes = fs.crashes();
    report.torn_writes = fs.torn_writes();
    Ok(report)
}
