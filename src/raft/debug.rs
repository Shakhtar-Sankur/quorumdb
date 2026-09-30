//! A time-travel debugger for the Raft simulator.
//!
//! The simulator is deterministic, so a seed is a recording of a whole run:
//! stepping forward handles one event at a time, and stepping *back* just
//! replays the seed from the start to an earlier event, which takes
//! milliseconds. On top of that sit breakpoints (a new leader, a crash, a
//! partition, a violation of a safety property...), views of every node's
//! state and log, and an HTML export of the whole run as a timeline.
//!
//! ```text
//! $ quorumdb debug --seed 7 --fault commit-old-term
//! (qdb) continue
//! (qdb) back 30
//! (qdb) state
//! ```

use std::collections::VecDeque;
use std::fmt::Write as _;

use super::RaftFault;
use super::sim::{NodeView, Report, Sim, Step};

/// What makes `continue` stop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Break {
    /// A node becomes leader.
    Leader,
    /// A node crashes.
    Crash,
    /// The network splits.
    Partition,
    /// Some node's commit index advances.
    Commit,
    /// A term reaches this number.
    Term(u64),
    /// Anything happens to this node.
    Node(u64),
    /// Simulated time reaches this many ms.
    Time(u64),
}

impl Break {
    fn parse(words: &[&str]) -> Result<Break, String> {
        let num = |w: Option<&&str>| -> Result<u64, String> {
            w.and_then(|w| w.trim_start_matches(['n', 't']).parse().ok())
                .ok_or_else(|| "expected a number".to_string())
        };
        Ok(match words.first().copied() {
            Some("leader") => Break::Leader,
            Some("crash") => Break::Crash,
            Some("partition") => Break::Partition,
            Some("commit") => Break::Commit,
            Some("term") => Break::Term(num(words.get(1))?),
            Some("node") => Break::Node(num(words.get(1))?),
            Some("time") => Break::Time(num(words.get(1))?),
            _ => {
                return Err(
                    "break on: leader | crash | partition | commit | term N | node N | time MS"
                        .into(),
                );
            }
        })
    }
}

/// A notable change between two consecutive states.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Role {
        node: u64,
        from: String,
        to: String,
        term: u64,
    },
    Term {
        node: u64,
        from: u64,
        to: u64,
    },
    Down {
        node: u64,
    },
    Up {
        node: u64,
    },
    Commit {
        node: u64,
        from: u64,
        to: u64,
    },
    Voters {
        node: u64,
        voters: Vec<u64>,
    },
    Partitioned {
        sides: Vec<Vec<u64>>,
    },
    Healed,
}

impl Change {
    fn node(&self) -> Option<u64> {
        match self {
            Change::Role { node, .. }
            | Change::Term { node, .. }
            | Change::Down { node }
            | Change::Up { node }
            | Change::Commit { node, .. }
            | Change::Voters { node, .. } => Some(*node),
            Change::Partitioned { .. } | Change::Healed => None,
        }
    }

    fn describe(&self) -> String {
        match self {
            Change::Role {
                node,
                from,
                to,
                term,
            } => format!("n{node}: {from} -> {to} (term {term})"),
            Change::Term { node, from, to } => format!("n{node}: term {from} -> {to}"),
            Change::Down { node } => format!("n{node} crashed (its unsynced state is gone)"),
            Change::Up { node } => format!("n{node} restarted from its disk"),
            Change::Commit { node, from, to } => format!("n{node}: commit {from} -> {to}"),
            Change::Voters { node, voters } => format!("n{node}: voters now {voters:?}"),
            Change::Partitioned { sides } => format!(
                "network partitioned: {}",
                sides
                    .iter()
                    .map(|s| format!("{s:?}"))
                    .collect::<Vec<_>>()
                    .join(" | ")
            ),
            Change::Healed => "network healed".into(),
        }
    }

    fn hits(&self, b: &Break) -> bool {
        match (b, self) {
            (Break::Leader, Change::Role { to, .. }) => to == "Leader",
            (Break::Crash, Change::Down { .. }) => true,
            (Break::Partition, Change::Partitioned { .. }) => true,
            (Break::Commit, Change::Commit { .. }) => true,
            (Break::Term(t), Change::Term { to, .. }) => to >= t,
            (Break::Term(t), Change::Role { term, .. }) => term >= t,
            (Break::Node(n), c) => c.node() == Some(*n),
            _ => false,
        }
    }
}

/// The changes from `a` to `b`, the node lists of two consecutive states.
pub fn diff(a: &[NodeView], b: &[NodeView]) -> Vec<Change> {
    let mut out = Vec::new();
    for (x, y) in a.iter().zip(b) {
        let node = y.id;
        if x.up && !y.up {
            out.push(Change::Down { node });
        }
        if !x.up && y.up {
            out.push(Change::Up { node });
        }
        if x.role != y.role {
            out.push(Change::Role {
                node,
                from: x.role.clone(),
                to: y.role.clone(),
                term: y.term,
            });
        } else if x.term != y.term {
            out.push(Change::Term {
                node,
                from: x.term,
                to: y.term,
            });
        }
        if y.commit > x.commit {
            out.push(Change::Commit {
                node,
                from: x.commit,
                to: y.commit,
            });
        }
        if x.voters != y.voters && y.up && x.up {
            out.push(Change::Voters {
                node,
                voters: y.voters.clone(),
            });
        }
    }
    let groups = |v: &[NodeView]| v.iter().map(|n| n.group).collect::<Vec<_>>();
    if groups(a) != groups(b) {
        if b.iter().all(|n| n.group == 0) {
            out.push(Change::Healed);
        } else {
            let sides = [0, 1]
                .iter()
                .map(|g| b.iter().filter(|n| n.group == *g).map(|n| n.id).collect())
                .filter(|s: &Vec<u64>| !s.is_empty())
                .collect();
            out.push(Change::Partitioned { sides });
        }
    }
    out
}

/// One handled event, as the trace keeps it.
#[derive(Clone, Debug)]
pub struct Record {
    /// Events handled before and including this one.
    pub n: u64,
    pub t: u64,
    pub event: String,
    pub changes: Vec<Change>,
}

impl Record {
    fn show(&self, out: &mut String) {
        let _ = writeln!(out, "#{:<6} t={:>5}ms  {}", self.n, self.t, self.event);
        for c in &self.changes {
            let _ = writeln!(out, "{:17}* {}", "", c.describe());
        }
    }
}

const TRACE_KEEP: usize = 500;

fn hits_break(breaks: &[Break], r: &Record) -> bool {
    breaks.iter().any(|b| match b {
        Break::Time(t) => r.t >= *t,
        b => r.changes.iter().any(|c| c.hits(b)),
    })
}

pub struct Debugger {
    seed: u64,
    fault: RaftFault,
    ms: u64,
    sim: Sim,
    view: Vec<NodeView>,
    n: u64,
    trace: VecDeque<Record>,
    outcome: Option<Result<Report, String>>,
    pub breaks: Vec<Break>,
}

impl Debugger {
    /// A debugger at the start of `seed`'s run of `ms` simulated ms.
    pub fn new(seed: u64, fault: RaftFault, ms: u64) -> Debugger {
        let mut sim = Sim::new(seed, fault);
        sim.begin(ms);
        let view = sim.nodes();
        Debugger {
            seed,
            fault,
            ms,
            sim,
            view,
            n: 0,
            trace: VecDeque::new(),
            outcome: None,
            breaks: Vec::new(),
        }
    }

    /// Events handled so far.
    pub fn position(&self) -> u64 {
        self.n
    }

    pub fn now(&self) -> u64 {
        self.sim.now()
    }

    pub fn nodes(&self) -> &[NodeView] {
        &self.view
    }

    /// The run's result, once it is over.
    pub fn outcome(&self) -> Option<&Result<Report, String>> {
        self.outcome.as_ref()
    }

    /// Handle one event. `None` once the run is over.
    pub fn step(&mut self) -> Option<Record> {
        if self.outcome.is_some() {
            return None;
        }
        match self.sim.step() {
            Step::Event(event) => {
                self.n += 1;
                let view = self.sim.nodes();
                let changes = diff(&self.view, &view);
                self.view = view;
                let r = Record {
                    n: self.n,
                    t: self.sim.now(),
                    event,
                    changes,
                };
                if self.trace.len() == TRACE_KEEP {
                    self.trace.pop_front();
                }
                self.trace.push_back(r.clone());
                Some(r)
            }
            Step::Finished(result) => {
                self.view = self.sim.nodes();
                self.outcome = Some(result);
                None
            }
        }
    }

    /// Travel to just after event `n`: forward by stepping, backward by
    /// replaying the seed from the start.
    pub fn goto(&mut self, n: u64) {
        if n < self.n {
            let breaks = std::mem::take(&mut self.breaks);
            *self = Debugger::new(self.seed, self.fault, self.ms);
            self.breaks = breaks;
        }
        while self.n < n && self.step().is_some() {}
    }

    /// Step until a breakpoint hits or the run ends. Returns the records
    /// of the events that changed something, and the one that stopped it.
    pub fn resume(&mut self, stop: &dyn Fn(&Record) -> bool) -> (Vec<Record>, Option<Record>) {
        let mut notable = Vec::new();
        while let Some(r) = self.step() {
            if stop(&r) {
                return (notable, Some(r));
            }
            if !r.changes.is_empty() {
                if notable.len() == 20 {
                    notable.remove(0);
                }
                notable.push(r);
            }
        }
        (notable, None)
    }

    /// Run one debugger command and return what it prints.
    pub fn exec(&mut self, line: &str) -> String {
        let words: Vec<&str> = line.split_whitespace().collect();
        let count = |i: usize| words.get(i).and_then(|w| w.parse::<u64>().ok());
        let mut out = String::new();
        match words.first().copied().unwrap_or("") {
            "" => {}
            "s" | "step" => {
                for _ in 0..count(1).unwrap_or(1) {
                    match self.step() {
                        Some(r) => r.show(&mut out),
                        None => break,
                    }
                }
            }
            "n" | "next" => {
                for _ in 0..count(1).unwrap_or(1) {
                    let (_, hit) = self.resume(&|r| !r.changes.is_empty());
                    match hit {
                        Some(r) => r.show(&mut out),
                        None => break,
                    }
                }
            }
            "c" | "continue" => {
                let breaks = self.breaks.clone();
                let (notable, hit) = self.resume(&|r| hits_break(&breaks, r));
                if hit.is_none() && !notable.is_empty() {
                    out.push_str("...the last events that changed something:\n");
                }
                for r in notable.iter().skip(notable.len().saturating_sub(8)) {
                    r.show(&mut out);
                }
                if let Some(r) = hit {
                    out.push_str("breakpoint hit:\n");
                    r.show(&mut out);
                }
            }
            "b" | "back" => {
                let to = self.n.saturating_sub(count(1).unwrap_or(1));
                self.goto(to);
                let _ = writeln!(out, "back at #{} t={}ms", self.n, self.now());
                if let Some(r) = self.trace.back() {
                    r.show(&mut out);
                }
            }
            "g" | "goto" => match count(1) {
                Some(n) => {
                    self.goto(n);
                    let _ = writeln!(out, "at #{} t={}ms", self.n, self.now());
                }
                None => out.push_str("goto N: travel to just after event N\n"),
            },
            "u" | "until" => match count(1) {
                Some(ms) => {
                    let (_, hit) = self.resume(&|r| r.t >= ms);
                    if let Some(r) = hit {
                        r.show(&mut out);
                    }
                }
                None => out.push_str("until MS: run to a simulated time\n"),
            },
            "state" | "st" => self.show_state(&mut out),
            "log" | "l" => match count(1).and_then(|id| self.sim.log(id)) {
                Some((snap, entries)) => {
                    let _ = writeln!(
                        out,
                        "n{}: snapshot through index {snap}",
                        count(1).unwrap_or(0)
                    );
                    let keep = count(2).unwrap_or(15) as usize;
                    for (i, t, what) in entries.iter().skip(entries.len().saturating_sub(keep)) {
                        let _ = writeln!(out, "  {i:>5}  term {t:<3} {what}");
                    }
                }
                None => out.push_str("log N [K]: the last K entries of node N's log\n"),
            },
            "trace" | "t" => {
                let k = count(1).unwrap_or(20) as usize;
                for r in self.trace.iter().skip(self.trace.len().saturating_sub(k)) {
                    r.show(&mut out);
                }
            }
            "break" => match Break::parse(&words[1..]) {
                Ok(b) => {
                    let _ = writeln!(out, "breakpoint {}: {b:?}", self.breaks.len() + 1);
                    self.breaks.push(b);
                }
                Err(e) => out.push_str(&(e + "\n")),
            },
            "breaks" => {
                for (i, b) in self.breaks.iter().enumerate() {
                    let _ = writeln!(out, "{}: {b:?}", i + 1);
                }
            }
            "delete" => {
                self.breaks.clear();
                out.push_str("breakpoints cleared\n");
            }
            "info" => {
                let _ = writeln!(
                    out,
                    "seed {} fault {:?}, {}ms of chaos; at #{} t={}ms, {} events queued",
                    self.seed,
                    self.fault,
                    self.ms,
                    self.n,
                    self.now(),
                    self.sim.pending_events()
                );
            }
            "h" | "help" => out.push_str(HELP),
            other => {
                let _ = writeln!(out, "unknown command {other:?}; try help");
            }
        }
        if let Some(result) = &self.outcome {
            match result {
                Ok(r) => {
                    let _ = writeln!(
                        out,
                        "run over at #{}: every check passed ({} leaders elected, {} writes acknowledged)",
                        self.n, r.elections, r.acked_writes
                    );
                }
                Err(e) => {
                    let _ = writeln!(
                        out,
                        "VIOLATION after #{}: {e}\n(`trace 30` shows how it happened; `back N` rewinds)",
                        self.n
                    );
                }
            }
        }
        out
    }

    fn show_state(&self, out: &mut String) {
        let _ = writeln!(out, "#{} t={}ms", self.n, self.now());
        let _ = writeln!(
            out,
            "  node  status      role          term  vote  leader  commit  applied  log (index/term)  voters"
        );
        for v in &self.view {
            let _ = writeln!(
                out,
                "  n{:<3}  {:<10}  {:<12}  {:>4}  {:>4}  {:>6}  {:>6}  {:>7}  {:>16}  {:?}",
                v.id,
                if !v.up {
                    "down".to_string()
                } else if self.view.iter().any(|n| n.group != 0) {
                    format!("side {}", v.group)
                } else {
                    "up".to_string()
                },
                v.role,
                v.term,
                v.vote.map_or("-".into(), |n| format!("n{n}")),
                v.leader.map_or("-".into(), |n| format!("n{n}")),
                v.commit,
                v.applied,
                format!("{}..{}/{}", v.first_index, v.last_index, v.last_term),
                v.voters
            );
        }
    }
}

const HELP: &str = "\
  step [N]         handle the next N events (s)
  next [N]         run to the next event that changes something (n)
  continue         run until a breakpoint, a violation, or the end (c)
  back [N]         rewind N events, by replaying the seed (b)
  goto N           travel to just after event N (g)
  until MS         run to simulated time MS (u)
  state            every node: role, term, vote, commit, log, voters (st)
  log N [K]        the last K entries of node N's durable log (l)
  trace [K]        the last K events and what they changed (t)
  break KIND       stop on: leader | crash | partition | commit | term N | node N | time MS
  breaks, delete   list or clear breakpoints
  info, help, quit
";

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '<' => out.push_str("\\u003c"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn role_code(role: &str) -> u8 {
    match role {
        "PreCandidate" => 1,
        "Candidate" => 2,
        "Leader" => 3,
        _ => 0,
    }
}

fn frame_json(n: u64, t: u64, event: &str, changes: &[Change], nodes: &[NodeView]) -> String {
    let nodes: Vec<String> = nodes
        .iter()
        .map(|v| {
            format!(
                "[{},{},{},{},{},{},{},{},{},{},{}]",
                v.id,
                u8::from(v.up),
                v.group,
                role_code(&v.role),
                v.term,
                v.commit,
                v.last_index,
                v.applied,
                v.leader.unwrap_or(0),
                u8::from(v.up && !v.voters.contains(&v.id)),
                v.last_term
            )
        })
        .collect();
    let changes: Vec<String> = changes.iter().map(|c| json_str(&c.describe())).collect();
    format!(
        "{{\"n\":{n},\"t\":{t},\"e\":{},\"c\":[{}],\"s\":[{}]}}",
        json_str(event),
        changes.join(","),
        nodes.join(",")
    )
}

/// Run `seed` to the end and render it as one self-contained HTML page: a
/// lane per node showing its role over time, the crashes, partitions and
/// any violation, and a scrubber that shows the whole cluster's state at
/// every event that changed something.
pub fn html(seed: u64, fault: RaftFault, ms: u64) -> String {
    let mut d = Debugger::new(seed, fault, ms);
    let mut frames = vec![frame_json(0, 0, "start", &[], d.nodes())];
    let mut last = None;
    while let Some(r) = d.step() {
        if !r.changes.is_empty() {
            frames.push(frame_json(r.n, r.t, &r.event, &r.changes, d.nodes()));
            last = None;
        } else {
            last = Some(r);
        }
    }
    // The final event, where a violation is detected, may change no state.
    if let Some(r) = last {
        frames.push(frame_json(r.n, r.t, &r.event, &[], d.nodes()));
    }
    let (ok, verdict) = match d.outcome() {
        Some(Ok(r)) => (
            true,
            format!(
                "every check passed: {} leaders elected, {} writes acknowledged, {} linearizable reads, {} crashes, {} partitions",
                r.elections, r.acked_writes, r.reads, r.crashes, r.partitions
            ),
        ),
        Some(Err(e)) => (false, e.clone()),
        None => (true, String::new()),
    };
    let data = format!(
        "{{\"seed\":{seed},\"fault\":{},\"ms\":{ms},\"end\":{},\"events\":{},\"ok\":{ok},\"verdict\":{},\"frames\":[{}]}}",
        json_str(&format!("{fault:?}")),
        d.now(),
        d.position(),
        json_str(&verdict),
        frames.join(",\n")
    );
    HTML.replace("/*DATA*/null", &data)
}

const HTML: &str = include_str!("debug.html");
