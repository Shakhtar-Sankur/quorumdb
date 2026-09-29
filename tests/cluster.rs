use quorumdb::kv::sim;
use quorumdb::kv::store::StoreFault;

fn seeds() -> u64 {
    std::env::var("QDB_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}

#[test]
fn every_key_history_is_linearizable_through_splits_moves_and_crashes() {
    let mut total = sim::Report::default();
    for seed in 0..seeds() {
        let r = sim::run(seed, 10_000, StoreFault::None).unwrap_or_else(|msg| panic!("{msg}"));
        total.writes += r.writes;
        total.reads += r.reads;
        total.crashes += r.crashes;
        total.ranges += r.ranges;
        total.replica_moves += r.replica_moves;
        total.keys_checked += r.keys_checked;
    }
    assert!(total.writes > 0 && total.reads > 0 && total.crashes > 0);
    assert!(total.ranges > seeds(), "ranges never split");
    assert!(total.replica_moves > 0, "replicas never moved");
}

#[test]
fn cluster_same_seed_replays_identically() {
    let a = sim::run(5, 5_000, StoreFault::None).unwrap();
    let b = sim::run(5, 5_000, StoreFault::None).unwrap();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}

/// Each planted bug is a real mistake in a replicated store; the
/// simulator must catch every one. Release builds only (slow in debug).
#[test]
#[cfg_attr(debug_assertions, ignore)]
fn every_planted_cluster_bug_is_caught() {
    for fault in [
        StoreFault::SkipRaftSync,
        StoreFault::StaleLocalReads,
        StoreFault::SnapshotSelfRemoval,
    ] {
        let caught =
            (0..500).find_map(|seed| sim::run(seed, 10_000, fault).err().map(|m| (seed, m)));
        match caught {
            Some((seed, msg)) => eprintln!(
                "{fault:?} caught at seed {seed}: {}",
                msg.lines().next().unwrap_or("")
            ),
            None => panic!("{fault:?} survived 500 seeds"),
        }
    }
}
