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

use crate::rng::Rng;
use crate::storage::engine::{Db, Fault, Options, Stats, SyncMode};
use crate::storage::fs::SimFs;
use crate::storage::wal::Op;

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
    pub trivial_moves: u64,
    pub tombstones_dropped: u64,
    pub lost_unsynced: u64,
    /// The deepest level any table reached during the run.
    pub max_level: usize,
}

impl Report {
    fn add_stats(&mut self, stats: Stats) {
        self.flushes += stats.flushes;
        self.compactions += stats.compactions;
        self.trivial_moves += stats.trivial_moves;
        self.tombstones_dropped += stats.tombstones_dropped;
    }
}

fn apply(model: &mut Model, batch: &[Op]) {
    for op in batch {
        match op {
            Op::Put(k, v) => {
                model.insert(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                model.remove(k);
            }
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
        // Tiny sizes, so a short run exercises many blocks per table, many
        // tables per level, and several levels.
        memtable_bytes: 64 + rng.below(1024) as usize,
        block_bytes: 16 + rng.below(256) as usize,
        table_bytes: 64 + rng.below(1024) as usize,
        l0_compact_at: 2 + rng.below(4) as usize,
        level1_bytes: 128 + rng.below(1024),
        level_multiplier: 2 + rng.below(3),
        bloom_bits_per_key: rng.below(13) as usize,
        fault,
    };
    let fail =
        |step: usize, what: String| format!("seed {seed}, step {step} ({:?}): {what}", opts.sync);
    let mut db =
        Db::open(fs.clone(), opts.clone()).map_err(|e| fail(0, format!("open failed: {e}")))?;

    // `durable` is the model at the last point the engine promised
    // durability; `pending` holds the acknowledged writes since then.
    let mut durable = Model::new();
    let mut pending: Vec<Vec<Op>> = Vec::new();
    let mut current = Model::new();
    let mut report = Report {
        seed,
        steps,
        ..Report::default()
    };
    let key_space = 8 + rng.below(248);
    for step in 1..=steps {
        let roll = rng.below(100);
        if roll < 70 {
            // Mostly single writes; sometimes an atomic batch of several.
            let size = if rng.chance(20) { 2 + rng.below(4) } else { 1 };
            let batch: Vec<Op> = (0..size)
                .map(|_| {
                    let key = format!("k{:03}", rng.below(key_space)).into_bytes();
                    if rng.below(70) < 55 {
                        let len = rng.below(24) as usize;
                        Op::Put(key, rng.bytes(len))
                    } else {
                        Op::Delete(key)
                    }
                })
                .collect();
            let result = match batch.as_slice() {
                [Op::Put(k, v)] => db.put(k, v),
                [Op::Delete(k)] => db.delete(k),
                _ => db.write_batch(batch.clone()),
            };
            result.map_err(|e| fail(step, format!("write failed: {e}")))?;
            apply(&mut current, &batch);
            pending.push(batch);
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
        } else if roll < 78 {
            db.flush()
                .map_err(|e| fail(step, format!("flush failed: {e}")))?;
            durable = current.clone();
            pending.clear();
        } else if roll < 79 {
            // A full compaction writes nothing to the log, so it changes
            // neither what is durable nor what is pending.
            db.compact()
                .map_err(|e| fail(step, format!("compaction failed: {e}")))?;
        } else if roll < 83 {
            report.add_stats(db.stats());
            drop(db);
            fs.crash();
            db = Db::open(fs.clone(), opts.clone())
                .map_err(|e| fail(step, format!("recovery failed: {e}")))?;
            let recovered: Model = db
                .scan()
                .map_err(|e| fail(step, format!("scan after recovery failed: {e}")))?
                .into_iter()
                .collect();

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
                let names = |keys: Vec<&Vec<u8>>| {
                    keys.into_iter()
                        .take(5)
                        .map(|k| String::from_utf8_lossy(k).into_owned())
                        .collect::<Vec<_>>()
                };
                let missing = names(
                    durable
                        .keys()
                        .filter(|k| !recovered.contains_key(*k))
                        .collect(),
                );
                // Present after recovery, yet deleted (or never written) both
                // at the durable point and after every acknowledged write.
                let resurrected = names(
                    recovered
                        .keys()
                        .filter(|k| !durable.contains_key(*k) && !current.contains_key(*k))
                        .collect(),
                );
                return Err(fail(
                    step,
                    format!(
                        "recovered state matches no prefix of the {} acknowledged writes since the durable point \
                         ({} keys durable, {} recovered; durable keys missing: {missing:?}; \
                         deleted keys back from the dead: {resurrected:?})",
                        pending.len(),
                        durable.len(),
                        recovered.len(),
                    ),
                ));
            }
            report.lost_unsynced += (pending.len() - kept) as u64;
            durable = state;
            current = durable.clone();
            pending.clear();
        } else if roll < 90 {
            // A range scan over a random span must match the model exactly.
            let a = format!("k{:03}", rng.below(key_space + 1)).into_bytes();
            let b = format!("k{:03}", rng.below(key_space + 1)).into_bytes();
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            let got: Vec<(Vec<u8>, Vec<u8>)> = db
                .range(&lo, Some(&hi))
                .collect::<crate::Result<_>>()
                .map_err(|e| fail(step, format!("range scan failed: {e}")))?;
            let want: Vec<(Vec<u8>, Vec<u8>)> = current
                .range(lo.clone()..hi.clone())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if got != want {
                return Err(fail(
                    step,
                    format!(
                        "range [{}, {}) returned {} rows, expected {}",
                        String::from_utf8_lossy(&lo),
                        String::from_utf8_lossy(&hi),
                        got.len(),
                        want.len()
                    ),
                ));
            }
        } else {
            let key = format!("k{:03}", rng.below(key_space)).into_bytes();
            let got = db
                .get(&key)
                .map_err(|e| fail(step, format!("get failed: {e}")))?;
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
        report.max_level = report.max_level.max(db.max_level());
    }

    let final_state: Model = db
        .scan()
        .map_err(|e| fail(steps, format!("final scan failed: {e}")))?
        .into_iter()
        .collect();
    if final_state != current {
        return Err(fail(steps, "final scan differs from the model".to_string()));
    }
    report.add_stats(db.stats());
    report.crashes = fs.crashes();
    report.torn_writes = fs.torn_writes();
    Ok(report)
}
