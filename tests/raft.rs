use quorumdb::raft::{RaftFault, sim};

fn seeds() -> u64 {
    std::env::var("QDB_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

#[test]
fn raft_is_safe_and_live_under_partitions_crashes_and_membership_changes() {
    let mut total = sim::Report::default();
    for seed in 0..seeds() {
        let r = sim::run(seed, 10_000, RaftFault::None).unwrap_or_else(|msg| panic!("{msg}"));
        total.acked_writes += r.acked_writes;
        total.reads += r.reads;
        total.crashes += r.crashes;
        total.snapshots += r.snapshots;
        total.config_changes += r.config_changes;
        total.transfers += r.transfers;
    }
    assert!(total.acked_writes > 0 && total.reads > 0 && total.crashes > 0);
    assert!(total.snapshots > 0 && total.config_changes > 0 && total.transfers > 0);
}

#[test]
fn raft_same_seed_replays_identically() {
    let a = sim::run(9, 5_000, RaftFault::None).unwrap();
    let b = sim::run(9, 5_000, RaftFault::None).unwrap();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}

/// Each planted bug is a real consensus mistake; the simulator must catch
/// every one. Slow in debug builds, so it runs in release only.
#[test]
#[cfg_attr(debug_assertions, ignore)]
fn every_planted_raft_bug_is_caught() {
    for fault in [
        RaftFault::VoteIgnoresLog,
        RaftFault::CommitOldTerm,
        RaftFault::SkipPrevCheck,
        RaftFault::ReadWithoutQuorum,
    ] {
        let caught =
            (0..3000).find_map(|seed| sim::run(seed, 10_000, fault).err().map(|m| (seed, m)));
        match caught {
            Some((seed, msg)) => eprintln!("{fault:?} caught at seed {seed}: {msg}"),
            None => panic!("{fault:?} survived 3000 seeds: the simulator missed a real bug"),
        }
    }
}

/// Stepping back is replaying: travelling to an event from either
/// direction must reach exactly the same cluster state.
#[test]
fn debugger_time_travel_is_exact() {
    use quorumdb::raft::debug::Debugger;
    let mut d = Debugger::new(3, RaftFault::None, 3_000);
    d.goto(1_500);
    let (at, nodes) = (d.now(), d.nodes().to_vec());
    d.goto(4_000);
    assert_ne!(d.nodes(), &nodes[..], "nothing happened in 2,500 events");
    d.goto(1_500);
    assert_eq!((d.now(), d.nodes()), (at, &nodes[..]));
    let out = d.exec("back 500");
    assert!(out.starts_with("back at #1000"), "{out}");
}

/// Run to a planted bug's violation in the debugger, then rewind to the
/// election that caused it: the violation is the one `raft-sim` reports,
/// and the debugger stops on it.
#[test]
fn debugger_stops_at_a_planted_bug_and_breakpoints() {
    use quorumdb::raft::debug::Debugger;
    let fault = RaftFault::CommitOldTerm;
    let (seed, msg) = (1100..3000)
        .find_map(|seed| sim::run(seed, 10_000, fault).err().map(|m| (seed, m)))
        .expect("the planted bug is caught");
    let mut d = Debugger::new(seed, fault, 10_000);
    let out = d.exec("continue");
    assert!(
        out.contains(&format!("VIOLATION after #{}: {msg}", d.position())),
        "{out}"
    );
    let end = d.position();
    // Breakpoints stop before the end: rewind, then stop at each election.
    d.goto(0);
    d.exec("break leader");
    let out = d.exec("continue");
    assert!(out.contains("-> Leader"), "{out}");
    assert!(d.position() < end && d.outcome().is_none());
    let html = quorumdb::raft::debug::html(seed, fault, 10_000);
    assert!(html.contains("state machine safety") && !html.contains("/*DATA*/"));
}
