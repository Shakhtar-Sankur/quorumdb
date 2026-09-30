use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use quorumdb::engine::Fault;
use quorumdb::rng::Rng;
use quorumdb::{Db, Options, RealFs, SyncMode, sim};

const USAGE: &str = "\
usage:
  quorumdb tpcc <empty dir> [--warehouses N] [--terminals N] [--seconds N]
                                             TPC-C-derived benchmark on a 3-node cluster
  quorumdb debug --seed N [--fault NAME] [--ms N] [--html FILE]
                                             time-travel debugger for one Raft simulation
  quorumdb server <dir> [--nodes N] [--listen ADDR]
                                             run a SQL server (Postgres protocol) on an N-node cluster
  quorumdb shell <dir>                       interactive shell on a real directory
  quorumdb sim [--seeds N] [--steps N] [--from S] [--fault NAME]
                                             run the crash simulator
  quorumdb raft-sim [--seeds N] [--ms N] [--from S] [--fault NAME]
                                             run the Raft cluster simulator
  quorumdb kv-sim [--seeds N] [--ms N] [--from S] [--fault NAME]
                                             run the multi-Raft cluster simulator
  quorumdb txn-sim [--seeds N] [--ms N] [--from S] [--fault NAME]
                                             run the distributed transaction simulator
  quorumdb bench <empty dir> [--keys N] [--value-bytes N] [--reads N]
                                             benchmark on a real directory
storage faults (to prove the simulator catches them):
  skip-wal-truncate | skip-dir-sync | no-sync-before-manifest | drop-tombstones-early
raft faults:
  vote-ignores-log | commit-old-term | skip-prev-check | read-without-quorum
cluster faults:
  skip-raft-sync | stale-local-reads | snapshot-self-removal
transaction faults:
  skip-write-conflict | read-ignores-locks | skip-read-validation | tso-serve-before-durable";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("shell") if args.len() == 2 => shell(&args[1]),
        Some("sim") => simulate(&args[1..]),
        Some("raft-sim") => raft_simulate(&args[1..]),
        Some("kv-sim") => kv_simulate(&args[1..]),
        Some("txn-sim") => txn_simulate(&args[1..]),
        Some("server") if args.len() >= 2 => server(&args[1], &args[2..]),
        Some("debug") => debug(&args[1..]),
        Some("tpcc") if args.len() >= 2 => tpcc(&args[1], &args[2..]),
        Some("bench") if args.len() >= 2 => bench(&args[1], &args[2..]),
        _ => Err(USAGE.to_string()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("{msg}");
            ExitCode::FAILURE
        }
    }
}

fn parse_fault(name: &str) -> Result<Fault, String> {
    Ok(match name {
        "none" => Fault::None,
        "skip-wal-truncate" => Fault::SkipWalTruncate,
        "skip-dir-sync" => Fault::SkipDirSync,
        "no-sync-before-manifest" => Fault::NoSyncBeforeManifest,
        "drop-tombstones-early" => Fault::DropTombstonesEarly,
        other => return Err(format!("unknown fault: {other}\n{USAGE}")),
    })
}

/// Parse `--flag value` pairs, handing each to `set`.
fn parse_flags(
    args: &[String],
    mut set: impl FnMut(&str, &str) -> Result<(), String>,
) -> Result<(), String> {
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        set(flag, value)?;
    }
    Ok(())
}

fn num(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag}: not a number: {value}"))
}

fn simulate(args: &[String]) -> Result<(), String> {
    let (mut seeds, mut steps, mut from, mut fault_name) =
        (1000u64, 2000usize, 0u64, "none".to_string());
    parse_flags(args, |flag, value| {
        match flag {
            "--seeds" => seeds = num(flag, value)?,
            "--steps" => steps = num(flag, value)? as usize,
            "--from" => from = num(flag, value)?,
            "--fault" => fault_name = value.to_string(),
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
        Ok(())
    })?;
    let fault = parse_fault(&fault_name)?;

    let start = Instant::now();
    let mut total = sim::Report::default();
    for seed in from..from + seeds {
        match sim::run(seed, steps, fault) {
            Ok(r) => {
                total.writes += r.writes;
                total.crashes += r.crashes;
                total.torn_writes += r.torn_writes;
                total.flushes += r.flushes;
                total.compactions += r.compactions;
                total.trivial_moves += r.trivial_moves;
                total.tombstones_dropped += r.tombstones_dropped;
                total.lost_unsynced += r.lost_unsynced;
                total.max_level = total.max_level.max(r.max_level);
            }
            Err(msg) => {
                let fault_flag = if fault == Fault::None {
                    String::new()
                } else {
                    format!(" --fault {fault_name}")
                };
                return Err(format!(
                    "FAILED {msg}\nreplay: quorumdb sim --from {seed} --seeds 1 --steps {steps}{fault_flag}"
                ));
            }
        }
    }
    println!(
        "ok: {seeds} seeds x {steps} steps in {:.1}s\n  {} writes, {} crashes ({} torn writes), {} flushes\n  \
         {} compactions ({} trivial moves), {} tombstones dropped, tables reached level {}\n  \
         {} unsynced writes lost as allowed, 0 durable writes lost",
        start.elapsed().as_secs_f64(),
        total.writes,
        total.crashes,
        total.torn_writes,
        total.flushes,
        total.compactions,
        total.trivial_moves,
        total.tombstones_dropped,
        total.max_level,
        total.lost_unsynced
    );
    Ok(())
}

fn shell(dir: &str) -> Result<(), String> {
    let fs = RealFs::open(dir).map_err(|e| e.to_string())?;
    let mut db = Db::open(
        fs,
        Options {
            sync: SyncMode::Always,
            ..Options::default()
        },
    )
    .map_err(|e| e.to_string())?;
    let help = "commands: put K V | get K | del K | scan | flush | compact | levels | quit";
    println!("quorumdb shell on {dir}. {help}");
    let stdin = io::stdin();
    loop {
        print!("> ");
        io::stdout().flush().ok();
        let mut line = String::new();
        if stdin
            .lock()
            .read_line(&mut line)
            .map_err(|e| e.to_string())?
            == 0
        {
            return Ok(());
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        let outcome = match parts.as_slice() {
            ["put", k, v @ ..] if !v.is_empty() => db
                .put(k.as_bytes(), v.join(" ").as_bytes())
                .map(|_| "ok".to_string()),
            ["get", k] => db.get(k.as_bytes()).map(|v| {
                v.map_or("(nil)".to_string(), |v| {
                    String::from_utf8_lossy(&v).into_owned()
                })
            }),
            ["del", k] => db.delete(k.as_bytes()).map(|_| "ok".to_string()),
            ["scan"] => db.scan().map(|rows| {
                rows.iter()
                    .map(|(k, v)| {
                        format!(
                            "{} = {}",
                            String::from_utf8_lossy(k),
                            String::from_utf8_lossy(v)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }),
            ["levels"] => Ok(levels(&db)),
            ["flush"] => db
                .flush()
                .map(|_| format!("ok ({} tables)", db.table_count())),
            ["compact"] => db
                .compact()
                .map(|_| format!("ok ({} tables)", db.table_count())),
            ["quit"] | ["exit"] => return Ok(()),
            [] => continue,
            _ => Ok(help.to_string()),
        };
        match outcome {
            Ok(text) if !text.is_empty() => println!("{text}"),
            Ok(_) => {}
            Err(e) => println!("error: {e}"),
        }
    }
}

fn levels<F: quorumdb::Fs>(db: &Db<F>) -> String {
    db.level_summary()
        .iter()
        .enumerate()
        .filter(|(_, (tables, _))| *tables > 0)
        .map(|(level, (tables, bytes))| format!("L{level}: {tables} tables, {bytes} bytes"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn rate(ops: usize, elapsed: Duration) -> String {
    format!("{:>10.0} ops/s", ops as f64 / elapsed.as_secs_f64())
}

fn bench(dir: &str, args: &[String]) -> Result<(), String> {
    let (mut keys, mut value_bytes, mut reads, mut synced) =
        (1_000_000usize, 100usize, 200_000usize, 1000usize);
    parse_flags(args, |flag, value| {
        match flag {
            "--keys" => keys = num(flag, value)? as usize,
            "--value-bytes" => value_bytes = num(flag, value)? as usize,
            "--reads" => reads = num(flag, value)? as usize,
            "--synced-writes" => synced = num(flag, value)? as usize,
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
        Ok(())
    })?;
    if std::fs::read_dir(dir).is_ok_and(|mut d| d.next().is_some()) {
        return Err(format!("{dir} is not empty; bench needs a fresh directory"));
    }
    let err = |e: quorumdb::Error| e.to_string();
    let fs = RealFs::open(dir).map_err(|e| e.to_string())?;
    let mut db = Db::open(
        fs,
        Options {
            sync: SyncMode::Manual,
            ..Options::default()
        },
    )
    .map_err(err)?;
    let key = |i: u64| {
        format!(
            "user{:012}",
            i.wrapping_mul(0x9E37_79B9_7F4A_7C15) % 1_000_000_000_000
        )
    };
    let mut rng = Rng::new(7);
    let value = rng.bytes(value_bytes);
    println!("quorumdb bench: {keys} keys, {value_bytes}-byte values, default options, {dir}");

    let start = Instant::now();
    for i in 0..keys as u64 {
        db.put(key(i).as_bytes(), &value).map_err(err)?;
    }
    db.sync().map_err(err)?;
    let fill = start.elapsed();
    println!(
        "fill, random order     {}  ({:.1}s)",
        rate(keys, fill),
        fill.as_secs_f64()
    );

    let mut latencies = Vec::with_capacity(reads);
    let start = Instant::now();
    for _ in 0..reads {
        let k = key(rng.below(keys as u64));
        let t = Instant::now();
        let found = db.get(k.as_bytes()).map_err(err)?;
        latencies.push(t.elapsed());
        if found.is_none() {
            return Err(format!("bench: {k} missing"));
        }
    }
    let elapsed = start.elapsed();
    latencies.sort_unstable();
    println!(
        "read, present keys     {}  (p50 {:.1?}, p99 {:.1?})",
        rate(reads, elapsed),
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.99)
    );

    let before = db.stats();
    // Absent keys inside every table's key range: only the bloom filters
    // can rule a table out without reading a block.
    let start = Instant::now();
    for i in 0..reads as u64 {
        if db
            .get(format!("{}x", key(i)).as_bytes())
            .map_err(err)?
            .is_some()
        {
            return Err("bench: an absent key was found".to_string());
        }
    }
    let elapsed = start.elapsed();
    let after = db.stats();
    let (skips, blocks) = (
        after.bloom_skips - before.bloom_skips,
        after.block_reads - before.block_reads,
    );
    println!(
        "read, absent keys      {}  (bloom filters skipped {:.1}% of table probes)",
        rate(reads, elapsed),
        100.0 * skips as f64 / (skips + blocks).max(1) as f64
    );

    let start = Instant::now();
    let mut rows = 0;
    for row in db.iter() {
        row.map_err(err)?;
        rows += 1;
    }
    let elapsed = start.elapsed();
    if rows != keys {
        return Err(format!("bench: scan found {rows} rows, expected {keys}"));
    }
    println!("full scan              {}", rate(rows, elapsed));

    if synced > 0 {
        let start = Instant::now();
        for i in 0..synced as u64 {
            db.put(format!("synced{i:08}").as_bytes(), &value)
                .map_err(err)?;
            db.sync().map_err(err)?;
        }
        let elapsed = start.elapsed();
        println!(
            "write + fsync          {}  ({:.1?} each)",
            rate(synced, elapsed),
            elapsed / synced as u32
        );
    }

    let stats = db.stats();
    let user_bytes = (keys * (value_bytes + 16)) as f64;
    println!(
        "write amplification    {:>10.1}x  ({} flushes, {} compactions, {} trivial moves)",
        (stats.bytes_flushed + stats.bytes_compacted) as f64 / user_bytes,
        stats.flushes,
        stats.compactions,
        stats.trivial_moves
    );
    println!("{}", levels(&db));
    Ok(())
}

fn raft_fault(name: &str) -> Result<quorumdb::raft::RaftFault, String> {
    use quorumdb::raft::RaftFault;
    Ok(match name {
        "none" => RaftFault::None,
        "vote-ignores-log" => RaftFault::VoteIgnoresLog,
        "commit-old-term" => RaftFault::CommitOldTerm,
        "skip-prev-check" => RaftFault::SkipPrevCheck,
        "read-without-quorum" => RaftFault::ReadWithoutQuorum,
        other => return Err(format!("unknown fault: {other}\n{USAGE}")),
    })
}

/// The time-travel debugger: an interactive session on one seed of the
/// Raft simulator, or (`--html`) the whole run as a timeline page.
fn debug(args: &[String]) -> Result<(), String> {
    use quorumdb::raft::debug::{self, Debugger};
    let (mut seed, mut ms, mut fault_name, mut html) = (0u64, 10_000u64, "none".to_string(), None);
    parse_flags(args, |flag, value| {
        match flag {
            "--seed" => seed = num(flag, value)?,
            "--ms" => ms = num(flag, value)?,
            "--fault" => fault_name = value.to_string(),
            "--html" => html = Some(value.to_string()),
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
        Ok(())
    })?;
    let fault = raft_fault(&fault_name)?;
    if let Some(path) = html {
        std::fs::write(&path, debug::html(seed, fault, ms)).map_err(|e| format!("{path}: {e}"))?;
        println!("wrote {path}");
        return Ok(());
    }
    let mut d = Debugger::new(seed, fault, ms);
    println!(
        "quorumdb time-travel debugger: Raft simulator, seed {seed}, fault {fault_name}. Type help."
    );
    print!("{}", d.exec("state"));
    let stdin = io::stdin();
    loop {
        print!("(qdb) ");
        io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
            println!();
            return Ok(());
        }
        let line = line.trim();
        if line == "quit" || line == "q" || line == "exit" {
            return Ok(());
        }
        print!("{}", d.exec(line));
    }
}

fn raft_simulate(args: &[String]) -> Result<(), String> {
    use quorumdb::raft::sim as rsim;
    let (mut seeds, mut ms, mut from, mut fault_name) =
        (100u64, 10_000u64, 0u64, "none".to_string());
    parse_flags(args, |flag, value| {
        match flag {
            "--seeds" => seeds = num(flag, value)?,
            "--ms" => ms = num(flag, value)?,
            "--from" => from = num(flag, value)?,
            "--fault" => fault_name = value.to_string(),
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
        Ok(())
    })?;
    let fault = raft_fault(&fault_name)?;
    let start = Instant::now();
    let mut t = rsim::Report::default();
    for seed in from..from + seeds {
        match rsim::run(seed, ms, fault) {
            Ok(r) => {
                t.messages += r.messages;
                t.dropped += r.dropped;
                t.crashes += r.crashes;
                t.partitions += r.partitions;
                t.elections += r.elections;
                t.acked_writes += r.acked_writes;
                t.reads += r.reads;
                t.snapshots += r.snapshots;
                t.config_changes += r.config_changes;
                t.transfers += r.transfers;
            }
            Err(msg) => {
                return Err(format!(
                    "FAILED {msg}\nreplay: quorumdb raft-sim --from {seed} --seeds 1 --ms {ms} --fault {fault_name}\n\
                     debug:  quorumdb debug --seed {seed} --ms {ms} --fault {fault_name}"
                ));
            }
        }
    }
    println!(
        "ok: {seeds} clusters x {:.0}s simulated in {:.1}s\n  {} messages ({} lost), {} crashes, {} partitions, {} leaders elected\n  \
         {} membership changes, {} leadership transfers, {} snapshots installed\n  \
         {} writes acknowledged, {} linearizable reads, 0 safety violations",
        ms as f64 / 1000.0,
        start.elapsed().as_secs_f64(),
        t.messages,
        t.dropped,
        t.crashes,
        t.partitions,
        t.elections,
        t.config_changes,
        t.transfers,
        t.snapshots,
        t.acked_writes,
        t.reads
    );
    Ok(())
}

fn kv_simulate(args: &[String]) -> Result<(), String> {
    use quorumdb::kv::sim as ksim;
    use quorumdb::kv::store::StoreFault;
    let (mut seeds, mut ms, mut from, mut fault_name) =
        (20u64, 10_000u64, 0u64, "none".to_string());
    parse_flags(args, |flag, value| {
        match flag {
            "--seeds" => seeds = num(flag, value)?,
            "--ms" => ms = num(flag, value)?,
            "--from" => from = num(flag, value)?,
            "--fault" => fault_name = value.to_string(),
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
        Ok(())
    })?;
    let fault = match fault_name.as_str() {
        "none" => StoreFault::None,
        "skip-raft-sync" => StoreFault::SkipRaftSync,
        "stale-local-reads" => StoreFault::StaleLocalReads,
        "snapshot-self-removal" => StoreFault::SnapshotSelfRemoval,
        other => return Err(format!("unknown fault: {other}\n{USAGE}")),
    };
    let start = Instant::now();
    let mut t = ksim::Report::default();
    for seed in from..from + seeds {
        match ksim::run(seed, ms, fault) {
            Ok(r) => {
                t.ops += r.ops;
                t.writes += r.writes;
                t.reads += r.reads;
                t.unknown += r.unknown;
                t.crashes += r.crashes;
                t.partitions += r.partitions;
                t.messages += r.messages;
                t.dropped += r.dropped;
                t.ranges += r.ranges;
                t.replica_moves += r.replica_moves;
                t.keys_checked += r.keys_checked;
            }
            Err(msg) => {
                return Err(format!(
                    "FAILED {msg}\nreplay: quorumdb kv-sim --from {seed} --seeds 1 --ms {ms} --fault {fault_name}"
                ));
            }
        }
    }
    println!(
        "ok: {seeds} clusters x {:.0}s simulated in {:.1}s\n  {} messages ({} lost), {} crashes, {} partitions\n  \
         {} ranges after splits, {} replica moves\n  \
         {} writes, {} reads, {} with unknown outcome; {} key histories linearizable",
        ms as f64 / 1000.0,
        start.elapsed().as_secs_f64(),
        t.messages,
        t.dropped,
        t.crashes,
        t.partitions,
        t.ranges,
        t.replica_moves,
        t.writes,
        t.reads,
        t.unknown,
        t.keys_checked
    );
    Ok(())
}

fn txn_simulate(args: &[String]) -> Result<(), String> {
    use quorumdb::txn::sim::{self as tsim, TxnSimFault};
    let (mut seeds, mut ms, mut from, mut fault_name) =
        (20u64, 10_000u64, 0u64, "none".to_string());
    parse_flags(args, |flag, value| {
        match flag {
            "--seeds" => seeds = num(flag, value)?,
            "--ms" => ms = num(flag, value)?,
            "--from" => from = num(flag, value)?,
            "--fault" => fault_name = value.to_string(),
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
        Ok(())
    })?;
    let fault = match fault_name.as_str() {
        "none" => TxnSimFault::None,
        "skip-write-conflict" => TxnSimFault::SkipWriteConflict,
        "read-ignores-locks" => TxnSimFault::ReadIgnoresLocks,
        "skip-read-validation" => TxnSimFault::SkipReadValidation,
        "tso-serve-before-durable" => TxnSimFault::TsoServeBeforeDurable,
        other => return Err(format!("unknown fault: {other}\n{USAGE}")),
    };
    let start = Instant::now();
    let (mut committed, mut aborted, mut resolved, mut crashes, mut partitions, mut edges) =
        (0, 0, 0, 0, 0, 0);
    let (mut si_runs, mut ser_runs, mut skew) = (0, 0, 0);
    for seed in from..from + seeds {
        match tsim::run(seed, ms, fault) {
            Ok(r) => {
                committed += r.committed;
                aborted += r.aborted;
                resolved += r.unknown_resolved;
                crashes += r.crashes;
                partitions += r.partitions;
                edges += r.dependency_edges;
                if r.serializable {
                    ser_runs += 1;
                } else {
                    si_runs += 1;
                    skew += r.write_skew_seen as u64;
                }
            }
            Err(msg) => {
                return Err(format!(
                    "FAILED {msg}\nreplay: quorumdb txn-sim --from {seed} --seeds 1 --ms {ms} --fault {fault_name}"
                ));
            }
        }
    }
    println!(
        "ok: {seeds} clusters x {:.0}s simulated in {:.1}s ({ser_runs} serializable, {si_runs} snapshot isolation)\n  \
         {committed} transactions committed, {aborted} aborted, {resolved} with lost outcomes resolved afterwards\n  \
         {crashes} crashes, {partitions} partitions; {edges} dependency edges checked\n  \
         serializable runs: every history serializable; snapshot runs: all snapshot isolated, {skew} showed write skew (allowed)",
        ms as f64 / 1000.0,
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

fn server(dir: &str, args: &[String]) -> Result<(), String> {
    let (mut nodes, mut addr) = (3u64, "127.0.0.1:5432".to_string());
    parse_flags(args, |flag, value| {
        match flag {
            "--nodes" => nodes = num(flag, value)?.max(1),
            "--listen" => addr = value.to_string(),
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
        Ok(())
    })?;
    quorumdb::server::serve(quorumdb::server::ServerConfig {
        dir: dir.to_string(),
        nodes,
        addr,
    })
}

fn tpcc(dir: &str, args: &[String]) -> Result<(), String> {
    use quorumdb::bench::tpcc::{self, KINDS, Stats};
    use quorumdb::kv::client::KvClient;
    use quorumdb::kv::store::StoreConfig;
    use quorumdb::server::cluster::LocalCluster;
    use quorumdb::sql::exec::Session;
    use quorumdb::txn::client::TxnOptions;
    use std::cell::RefCell;
    use std::rc::Rc;

    let (mut warehouses, mut terminals, mut seconds) = (0u64, 10u64, 60u64);
    parse_flags(args, |flag, value| {
        match flag {
            "--warehouses" => warehouses = num(flag, value)?,
            "--terminals" => terminals = num(flag, value)?.max(1),
            "--seconds" => seconds = num(flag, value)?.max(1),
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
        Ok(())
    })?;
    if std::fs::read_dir(dir).is_ok_and(|mut d| d.next().is_some()) {
        return Err(format!("{dir} is not empty; tpcc needs a fresh directory"));
    }
    // TPC-C: ten terminals per warehouse, each bound to one district.
    if warehouses == 0 {
        warehouses = terminals.div_ceil(tpcc::DISTRICTS);
    }
    let err = |e: quorumdb::Error| e.to_string();
    let fs = (1..=3)
        .map(|n| RealFs::open(format!("{dir}/node{n}")))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;
    let mut cluster = LocalCluster::open(fs, StoreConfig::default(), 2_000).map_err(err)?;
    let clock = Instant::now();
    let ms = move || clock.elapsed().as_millis() as u64;
    cluster.set_clock(ms);
    let run = |cluster: &mut LocalCluster<RealFs>, done: &dyn Fn() -> bool| -> Result<(), String> {
        while !done() {
            if !cluster.step(ms()).map_err(err)? {
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
        }
        Ok(())
    };
    let io0 = cluster.io.clone();
    let session = move || Session::new(KvClient::new(io0.clone()), TxnOptions::default());

    println!(
        "TPC-C (derived): {warehouses} warehouse(s), {terminals} terminals, {seconds}s, 3 nodes, serializable"
    );
    let loaded: Rc<RefCell<Option<Result<(), String>>>> = Rc::new(RefCell::new(None));
    {
        let (loaded, sessions) = (loaded.clone(), (0..16).map(|_| session()).collect());
        cluster.exec.spawn(async move {
            let r = tpcc::load(sessions, warehouses)
                .await
                .map_err(|e| e.message());
            *loaded.borrow_mut() = Some(r);
        });
    }
    let t0 = ms();
    run(&mut cluster, &|| loaded.borrow().is_some())?;
    loaded.borrow_mut().take().expect("done")?;
    println!("loaded in {:.1}s", (ms() - t0) as f64 / 1000.0);

    let stats: Vec<Rc<RefCell<Stats>>> = (0..terminals)
        .map(|_| Rc::new(RefCell::new(Stats::default())))
        .collect();
    let start = ms();
    let until = start + seconds * 1000;
    let finished = Rc::new(RefCell::new(0u64));
    for (i, st) in stats.iter().enumerate() {
        let io = cluster.io.clone();
        let (st, finished, s) = (st.clone(), finished.clone(), session());
        cluster.exec.spawn(async move {
            // Round-robin over warehouses, then over their districts.
            let home = tpcc::Home {
                w: 1 + i as u64 % warehouses,
                d: 1 + (i as u64 / warehouses) % tpcc::DISTRICTS,
                warehouses,
            };
            tpcc::terminal(s, io, until, home, 1000 + i as u64, st).await;
            *finished.borrow_mut() += 1;
        });
    }
    run(&mut cluster, &|| *finished.borrow() == terminals)?;
    let elapsed = (ms() - start) as f64 / 1000.0;

    let mut total = Stats::default();
    for st in &stats {
        total.merge(&st.borrow());
    }
    let new_orders = total.latencies[0].len() as f64;
    println!(
        "\n  tpmC {:.0}   ({} New-Orders committed in {:.1}s)",
        new_orders * 60.0 / elapsed,
        new_orders,
        elapsed
    );
    println!(
        "  {:<13} {:>8} {:>9} {:>9} {:>9}",
        "transaction", "commits", "p50 ms", "p99 ms", "retries"
    );
    for kind in KINDS {
        let mut l = total.latencies[kind as usize].clone();
        l.sort_unstable();
        let pct = |p: f64| {
            l.get(((l.len() as f64 * p) as usize).min(l.len().saturating_sub(1)))
                .copied()
                .unwrap_or(0)
        };
        println!(
            "  {:<13} {:>8} {:>9} {:>9} {:>9}",
            format!("{kind:?}"),
            l.len(),
            pct(0.5),
            pct(0.99),
            total.retries[kind as usize]
        );
    }
    println!(
        "  {} serialization conflicts retried, {} gave up after 30 tries, {} intentional rollbacks (1% of New-Orders), {} errors",
        total.retries.iter().sum::<u64>(),
        total.gave_up,
        total.rollbacks,
        total.errors
    );

    type Checked = Option<Result<Vec<String>, String>>;
    let checked: Rc<RefCell<Checked>> = Rc::new(RefCell::new(None));
    {
        let (checked, mut s) = (checked.clone(), session());
        cluster.exec.spawn(async move {
            let r = tpcc::check_consistency(&mut s, warehouses)
                .await
                .map_err(|e| e.message());
            *checked.borrow_mut() = Some(r);
        });
    }
    run(&mut cluster, &|| checked.borrow().is_some())?;
    match checked.borrow_mut().take().expect("done")? {
        p if p.is_empty() => println!("  consistency conditions 1 and 2: hold"),
        p => return Err(format!("consistency violated:\n  {}", p.join("\n  "))),
    }
    Ok(())
}
