use quorumdb::txn::sim::{self, TxnSimFault};

fn seeds() -> u64 {
    std::env::var("QDB_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40)
}

#[test]
fn every_history_is_snapshot_isolated_and_serializable_when_asked() {
    let (mut committed, mut serializable_runs, mut si_runs) = (0, 0, 0);
    for seed in 0..seeds() {
        let r = sim::run(seed, 10_000, TxnSimFault::None).unwrap_or_else(|msg| panic!("{msg}"));
        committed += r.committed;
        if r.serializable {
            serializable_runs += 1;
        } else {
            si_runs += 1;
        }
    }
    assert!(committed > 0);
    assert!(
        serializable_runs > 0 && si_runs > 0,
        "both isolation levels must run"
    );
}

#[test]
fn txn_same_seed_replays_identically() {
    let a = sim::run(3, 5_000, TxnSimFault::None).unwrap();
    let b = sim::run(3, 5_000, TxnSimFault::None).unwrap();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}

/// Each planted bug is a real transaction-processing mistake; the checker
/// must catch every one. Release builds only (slow in debug).
#[test]
#[cfg_attr(debug_assertions, ignore)]
fn every_planted_transaction_bug_is_caught() {
    for fault in [
        TxnSimFault::SkipWriteConflict,
        TxnSimFault::ReadIgnoresLocks,
        TxnSimFault::SkipReadValidation,
        TxnSimFault::TsoServeBeforeDurable,
    ] {
        let caught =
            (0..200).find_map(|seed| sim::run(seed, 10_000, fault).err().map(|m| (seed, m)));
        match caught {
            Some((seed, msg)) => eprintln!("{fault:?} caught at seed {seed}: {msg}"),
            None => panic!("{fault:?} survived 200 seeds"),
        }
    }
}
