use quorumdb::engine::Fault;
use quorumdb::sim;

fn seeds() -> u64 {
    std::env::var("QDB_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300)
}

#[test]
fn no_durable_write_is_ever_lost() {
    let mut crashes = 0;
    for seed in 0..seeds() {
        let report = sim::run(seed, 1500, Fault::None).unwrap_or_else(|msg| panic!("{msg}"));
        crashes += report.crashes;
    }
    assert!(crashes > 0, "the simulator never crashed anything");
}

#[test]
fn the_same_seed_replays_identically() {
    let a = sim::run(42, 1500, Fault::None).unwrap();
    let b = sim::run(42, 1500, Fault::None).unwrap();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}

/// A simulator that never fails proves nothing. Each planted bug below is a
/// real durability mistake; the simulator must catch every one of them.
#[test]
fn every_planted_bug_is_caught() {
    for fault in [
        Fault::SkipWalTruncate,
        Fault::SkipDirSync,
        Fault::NoSyncBeforeManifest,
    ] {
        let caught =
            (0..500).find_map(|seed| sim::run(seed, 1500, fault).err().map(|msg| (seed, msg)));
        match caught {
            Some((seed, msg)) => eprintln!("{fault:?} caught at seed {seed}: {msg}"),
            None => panic!("{fault:?} survived 500 seeds: the simulator missed a real bug"),
        }
    }
}
