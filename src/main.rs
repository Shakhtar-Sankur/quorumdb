use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::time::Instant;

use quorumdb::engine::Fault;
use quorumdb::{Db, Options, RealFs, SyncMode, sim};

const USAGE: &str = "\
usage:
  quorumdb shell <dir>                       interactive shell on a real directory
  quorumdb sim [--seeds N] [--steps N] [--from S] [--fault NAME]
                                             run the crash simulator
faults (to prove the simulator catches them):
  skip-wal-truncate | skip-dir-sync | no-sync-before-manifest";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("shell") if args.len() == 2 => shell(&args[1]),
        Some("sim") => simulate(&args[1..]),
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
        other => return Err(format!("unknown fault: {other}\n{USAGE}")),
    })
}

fn simulate(args: &[String]) -> Result<(), String> {
    let (mut seeds, mut steps, mut from, mut fault_name) =
        (1000u64, 2000usize, 0u64, "none".to_string());
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        let num = || {
            value
                .parse::<u64>()
                .map_err(|_| format!("{flag}: not a number: {value}"))
        };
        match flag.as_str() {
            "--seeds" => seeds = num()?,
            "--steps" => steps = num()? as usize,
            "--from" => from = num()?,
            "--fault" => fault_name = value.clone(),
            other => return Err(format!("unknown flag: {other}\n{USAGE}")),
        }
    }
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
                total.lost_unsynced += r.lost_unsynced;
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
        "ok: {seeds} seeds x {steps} steps in {:.1}s\n  {} writes, {} crashes ({} torn writes), {} flushes, {} compactions\n  \
         {} unsynced writes lost as allowed, 0 durable writes lost",
        start.elapsed().as_secs_f64(),
        total.writes,
        total.crashes,
        total.torn_writes,
        total.flushes,
        total.compactions,
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
    let help = "commands: put K V | get K | del K | scan | flush | compact | quit";
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
            ["get", k] => Ok(db.get(k.as_bytes()).map_or("(nil)".to_string(), |v| {
                String::from_utf8_lossy(&v).into_owned()
            })),
            ["del", k] => db.delete(k.as_bytes()).map(|_| "ok".to_string()),
            ["scan"] => Ok(db
                .scan()
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{} = {}",
                        String::from_utf8_lossy(k),
                        String::from_utf8_lossy(v)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")),
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
