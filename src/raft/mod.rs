//! Raft consensus, as a deterministic state machine with no I/O.
//!
//! The caller feeds it clock ticks, proposals and incoming messages, then
//! drains a [`Ready`]: state to persist, messages to send, and committed
//! entries to apply, in that order. Because the core never touches a disk,
//! a network or a clock, the simulator can drive thousands of nodes through
//! partitions and crashes and replay any run exactly from its seed.
//!
//! Implemented from the Raft paper and Ongaro's dissertation:
//! - Leader election with randomized timeouts, **pre-vote** (a partitioned
//!   node cannot disrupt the cluster when it returns) and **check-quorum**
//!   (a leader that cannot reach a majority steps down).
//! - Log replication with fast backtracking by conflicting term, and
//!   optimistic pipelining once a follower's log matches.
//! - Commitment only of entries from the leader's own term (Figure 8).
//! - **ReadIndex** linearizable reads without writing to the log.
//! - **Snapshots and log compaction**, for followers that fall behind the
//!   leader's compacted log and for new replicas.
//! - **Single-server membership changes**, effective when appended, one at a
//!   time, and only after the leader has committed an entry in its term.
//! - **Leadership transfer** via `TimeoutNow`.

pub mod sim;

use std::collections::{BTreeMap, BTreeSet};

use crate::rng::Rng;

pub type NodeId = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryData {
    /// Appended by every new leader so it can commit, and serve reads, in its term.
    Empty,
    Command(Vec<u8>),
    /// The complete new set of voters, differing from the old by one node.
    Config(Vec<NodeId>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub data: EntryData,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub index: u64,
    pub term: u64,
    pub voters: Vec<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    /// The state machine at `meta.index`, opaque to Raft.
    pub data: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: u64,
    pub vote: Option<NodeId>,
    pub commit: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Msg {
    /// `pre` asks "would you vote for me?" without anyone changing term.
    /// `force` (from leadership transfer) overrides leader stickiness.
    Vote {
        pre: bool,
        force: bool,
        last_index: u64,
        last_term: u64,
    },
    VoteResp {
        pre: bool,
        granted: bool,
    },
    Append {
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
        /// Echoed back, so the leader knows which heartbeat round a
        /// follower has acknowledged (for ReadIndex).
        probe: u64,
    },
    /// On success `index` is the follower's last matching index; on
    /// rejection it is a hint of where the leader should back up to.
    AppendResp {
        success: bool,
        index: u64,
        probe: u64,
    },
    Snapshot(Snapshot),
    TimeoutNow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub term: u64,
    pub msg: Msg,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

/// Deliberately broken behaviour, to prove the simulator catches each class
/// of consensus bug. Never set outside tests.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftFault {
    None,
    /// Grant votes without checking the candidate's log is up to date.
    VoteIgnoresLog,
    /// Commit entries from earlier terms by counting replicas (Figure 8).
    CommitOldTerm,
    /// Accept appended entries without checking the previous entry matches.
    SkipPrevCheck,
    /// Serve ReadIndex without confirming leadership with a quorum.
    ReadWithoutQuorum,
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Election timeout, randomized in `[election_ticks, 2 * election_ticks)`.
    pub election_ticks: u64,
    pub heartbeat_ticks: u64,
    pub max_append_entries: usize,
    pub pre_vote: bool,
    pub check_quorum: bool,
    pub fault: RaftFault,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_append_entries: 64,
            pre_vote: true,
            check_quorum: true,
            fault: RaftFault::None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader(Option<NodeId>),
    /// A leadership transfer is in progress, or a membership change is
    /// already pending, or the leader has not yet committed in its term.
    Busy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProgressState {
    /// Unsure where the follower's log matches: one append per heartbeat.
    Probe,
    /// Logs match: stream entries optimistically.
    Replicate,
    /// Waiting for the follower to install the snapshot at this index.
    Snapshot(u64),
}

#[derive(Clone, Debug)]
struct Progress {
    matched: u64,
    next: u64,
    state: ProgressState,
    /// Heard from since the last check-quorum round.
    active: bool,
    acked_probe: u64,
    /// Ticks since a snapshot was requested, to retry lost snapshots.
    snapshot_wait: u64,
}

#[derive(Clone, Debug)]
struct PendingRead {
    id: u64,
    index: u64,
    probe: u64,
}

/// The in-memory log: entries after a compacted prefix `[1, snap_index]`.
#[derive(Clone, Debug, Default)]
struct Log {
    snap_index: u64,
    snap_term: u64,
    entries: Vec<Entry>,
}

impl Log {
    fn last_index(&self) -> u64 {
        self.snap_index + self.entries.len() as u64
    }

    fn last_term(&self) -> u64 {
        self.entries.last().map_or(self.snap_term, |e| e.term)
    }

    fn term(&self, index: u64) -> Option<u64> {
        if index == self.snap_index {
            Some(self.snap_term)
        } else if index < self.snap_index || index > self.last_index() {
            None
        } else {
            Some(self.entries[(index - self.snap_index - 1) as usize].term)
        }
    }

    /// Entries in `[from, to]`, at most `max` of them.
    fn slice(&self, from: u64, to: u64, max: usize) -> Vec<Entry> {
        if from > to || from <= self.snap_index {
            return Vec::new();
        }
        let lo = (from - self.snap_index - 1) as usize;
        let hi = ((to - self.snap_index) as usize).min(self.entries.len());
        self.entries[lo..hi.max(lo)]
            .iter()
            .take(max)
            .cloned()
            .collect()
    }

    fn truncate_from(&mut self, index: u64) {
        self.entries
            .truncate((index - self.snap_index - 1) as usize);
    }
}

/// What the caller must do after driving the node: persist `hard_state`,
/// `snapshot` and `entries` durably, then send `messages`, then apply
/// `snapshot` and `committed` to the state machine, then answer `reads`.
#[derive(Debug, Default)]
pub struct Ready {
    pub hard_state: Option<HardState>,
    /// A snapshot received from the leader, replacing the state machine and log.
    pub snapshot: Option<Snapshot>,
    /// Log entries to write. They replace every persisted entry at or after
    /// the first one's index, so any longer persisted tail must be deleted.
    pub entries: Vec<Entry>,
    pub messages: Vec<Message>,
    pub committed: Vec<Entry>,
    /// Linearizable reads, `(id, index)`: answer once `index` is applied.
    pub reads: Vec<(u64, u64)>,
    /// Reads that can no longer be served here (leadership was lost).
    pub failed_reads: Vec<u64>,
    /// Peers that need a snapshot, to be built from the state machine and
    /// handed to [`Raft::send_snapshot`].
    pub snapshot_requests: Vec<NodeId>,
}

pub struct Raft {
    pub id: NodeId,
    cfg: Config,
    term: u64,
    vote: Option<NodeId>,
    log: Log,
    commit: u64,
    /// Entries up to here have been handed to the caller to apply.
    delivered: u64,
    role: Role,
    leader: Option<NodeId>,
    /// `(index, voters)`: every configuration in the log, oldest first. The
    /// last one is in force, whether committed or not.
    configs: Vec<(u64, Vec<NodeId>)>,
    progress: BTreeMap<NodeId, Progress>,
    votes: BTreeMap<NodeId, bool>,
    election_elapsed: u64,
    heartbeat_elapsed: u64,
    timeout: u64,
    rng: Rng,
    probe: u64,
    reads: Vec<PendingRead>,
    transfer_to: Option<NodeId>,
    // Output, drained by `ready()`.
    persisted: HardState,
    unstable: u64,
    out: Ready,
}

impl Raft {
    /// A node restarting from persisted state: a hard state, a snapshot (the
    /// compacted prefix and the configuration at its end), the log after it,
    /// and the index already applied to the state machine.
    pub fn restore(
        id: NodeId,
        cfg: Config,
        seed: u64,
        hard: HardState,
        snap: SnapshotMeta,
        entries: Vec<Entry>,
        applied: u64,
    ) -> Raft {
        let mut configs = vec![(snap.index, snap.voters.clone())];
        for e in &entries {
            if let EntryData::Config(v) = &e.data {
                configs.push((e.index, v.clone()));
            }
        }
        let log = Log {
            snap_index: snap.index,
            snap_term: snap.term,
            entries,
        };
        let commit = hard.commit.max(snap.index).min(log.last_index());
        let mut raft = Raft {
            id,
            term: hard.term,
            vote: hard.vote,
            commit,
            delivered: applied.max(snap.index).min(commit),
            role: Role::Follower,
            leader: None,
            configs,
            progress: BTreeMap::new(),
            votes: BTreeMap::new(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            timeout: 0,
            rng: Rng::new(seed ^ id.wrapping_mul(0x9E37_79B9)),
            probe: 0,
            reads: Vec::new(),
            transfer_to: None,
            persisted: HardState {
                term: hard.term,
                vote: hard.vote,
                commit,
            },
            unstable: log.last_index() + 1,
            log,
            cfg,
            out: Ready::default(),
        };
        raft.reset_timeout();
        raft
    }

    /// A brand-new, empty replica: it learns everything from a leader's snapshot.
    pub fn new_empty(id: NodeId, cfg: Config, seed: u64) -> Raft {
        Raft::restore(
            id,
            cfg,
            seed,
            HardState::default(),
            SnapshotMeta::default(),
            Vec::new(),
            0,
        )
    }

    // ─── Accessors ────────────────────────────────────────────────────────

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// The hard state as it stands now (persisted or about to be).
    pub fn hard_state_now(&self) -> HardState {
        self.hard_state()
    }

    pub fn commit_index(&self) -> u64 {
        self.commit
    }

    pub fn last_index(&self) -> u64 {
        self.log.last_index()
    }

    pub fn first_index(&self) -> u64 {
        self.log.snap_index + 1
    }

    pub fn voters(&self) -> &[NodeId] {
        &self.configs.last().expect("always one config").1
    }

    /// The configuration in force at `index`.
    pub fn voters_at(&self, index: u64) -> Vec<NodeId> {
        self.configs
            .iter()
            .rev()
            .find(|(i, _)| *i <= index)
            .map_or_else(|| self.configs[0].1.clone(), |(_, v)| v.clone())
    }

    pub fn entry_term(&self, index: u64) -> Option<u64> {
        self.log.term(index)
    }

    /// Whether a membership change is appended but not yet committed.
    pub fn config_pending(&self) -> bool {
        self.configs.last().expect("config").0 > self.commit
    }

    /// Match index per follower, for a leader (tests and the debugger).
    pub fn progress(&self) -> Vec<(NodeId, u64)> {
        self.progress
            .iter()
            .map(|(&id, p)| (id, p.matched))
            .collect()
    }

    /// A leader's view of each follower, for diagnostics.
    pub fn debug_progress(&self) -> String {
        self.progress
            .iter()
            .map(|(id, p)| format!("{id}:{:?} match {} next {}", p.state, p.matched, p.next))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn is_voter(&self) -> bool {
        self.voters().contains(&self.id)
    }

    fn quorum(&self) -> usize {
        self.voters().len() / 2 + 1
    }

    // ─── Driving the node ─────────────────────────────────────────────────

    pub fn tick(&mut self) {
        match self.role {
            Role::Leader => self.tick_leader(),
            _ => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.timeout && self.is_voter() {
                    self.campaign(false);
                }
            }
        }
    }

    fn tick_leader(&mut self) {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;
        for p in self.progress.values_mut() {
            if let ProgressState::Snapshot(_) = p.state {
                p.snapshot_wait += 1;
                if p.snapshot_wait > 4 * self.cfg.election_ticks {
                    // The snapshot was probably lost: ask for a new one.
                    p.state = ProgressState::Probe;
                }
            }
        }
        if self.election_elapsed >= self.cfg.election_ticks {
            self.election_elapsed = 0;
            if self.cfg.check_quorum {
                let id = self.id;
                let active = self
                    .voters()
                    .iter()
                    .filter(|&&v| v == id || self.progress.get(&v).is_some_and(|p| p.active))
                    .count();
                for p in self.progress.values_mut() {
                    p.active = false;
                }
                if active < self.quorum() {
                    self.become_follower(self.term, None);
                    return;
                }
            }
            // A transfer that has not finished within an election timeout failed.
            self.transfer_to = None;
        }
        if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
            self.heartbeat_elapsed = 0;
            self.broadcast_heartbeat();
        }
    }

    /// Append a command. Returns its index; it is committed once it comes
    /// back in `Ready::committed` with the same term.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(u64, u64), ProposeError> {
        self.check_can_propose()?;
        Ok(self.append_entry(EntryData::Command(data)))
    }

    /// Propose adding or removing one voter. Only one change may be in
    /// flight, and only once the leader has committed an entry in its term,
    /// which closes the single-server-change bug found after the paper.
    pub fn propose_config(&mut self, voters: Vec<NodeId>) -> Result<(u64, u64), ProposeError> {
        self.check_can_propose()?;
        let old: BTreeSet<_> = self.voters().iter().copied().collect();
        let new: BTreeSet<_> = voters.iter().copied().collect();
        if self.config_pending()
            || self.log.term(self.commit) != Some(self.term)
            || old.symmetric_difference(&new).count() != 1
        {
            return Err(ProposeError::Busy);
        }
        Ok(self.append_entry(EntryData::Config(new.into_iter().collect())))
    }

    fn check_can_propose(&self) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader));
        }
        if self.transfer_to.is_some() {
            return Err(ProposeError::Busy);
        }
        Ok(())
    }

    /// Request a linearizable read. When it appears in `Ready::reads` with an
    /// index, the read may be served once the state machine applies that index.
    pub fn read_index(&mut self, id: u64) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader));
        }
        // Until the leader commits in its term it does not know the true
        // commit index.
        if self.log.term(self.commit) != Some(self.term) {
            return Err(ProposeError::Busy);
        }
        if self.voters() == [self.id] || self.cfg.fault == RaftFault::ReadWithoutQuorum {
            self.out.reads.push((id, self.commit));
            return Ok(());
        }
        self.probe += 1;
        self.reads.push(PendingRead {
            id,
            index: self.commit,
            probe: self.probe,
        });
        self.broadcast_heartbeat_with(self.probe);
        Ok(())
    }

    /// Hand leadership to `to`, once its log is up to date.
    pub fn transfer_leader(&mut self, to: NodeId) {
        if self.role != Role::Leader || to == self.id || !self.voters().contains(&to) {
            return;
        }
        self.transfer_to = Some(to);
        self.election_elapsed = 0;
        if self
            .progress
            .get(&to)
            .is_some_and(|p| p.matched == self.log.last_index())
        {
            self.send(to, Msg::TimeoutNow);
        } else {
            self.send_append(to, false);
        }
    }

    /// Discard log entries up to `index`, which must already be applied.
    /// The caller has persisted the snapshot's metadata and deletes the entries.
    pub fn compact(&mut self, index: u64) {
        if index <= self.log.snap_index || index > self.delivered {
            return;
        }
        let term = self
            .log
            .term(index)
            .expect("applied entries are in the log");
        let drop = (index - self.log.snap_index) as usize;
        self.log.entries.drain(..drop);
        self.log.snap_index = index;
        self.log.snap_term = term;
        let base = self.voters_at(index);
        self.configs.retain(|(i, _)| *i > index);
        self.configs.insert(0, (index, base));
    }

    /// Metadata for a snapshot of the state machine at `index` (the applied index).
    pub fn snapshot_meta(&self, index: u64) -> Option<SnapshotMeta> {
        Some(SnapshotMeta {
            index,
            term: self.log.term(index)?,
            voters: self.voters_at(index),
        })
    }

    /// Send a snapshot the caller built in answer to `Ready::snapshot_requests`.
    pub fn send_snapshot(&mut self, to: NodeId, snapshot: Snapshot) {
        if self.role != Role::Leader {
            return;
        }
        if let Some(p) = self.progress.get_mut(&to) {
            p.state = ProgressState::Snapshot(snapshot.meta.index);
            p.snapshot_wait = 0;
            self.send(to, Msg::Snapshot(snapshot));
        }
    }

    pub fn has_ready(&self) -> bool {
        let o = &self.out;
        self.hard_state() != self.persisted
            || o.snapshot.is_some()
            || self.unstable <= self.log.last_index()
            || !o.messages.is_empty()
            || self.commit > self.delivered
            || !o.reads.is_empty()
            || !o.failed_reads.is_empty()
            || !o.snapshot_requests.is_empty()
    }

    pub fn ready(&mut self) -> Ready {
        let mut ready = std::mem::take(&mut self.out);
        let hs = self.hard_state();
        if hs != self.persisted {
            ready.hard_state = Some(hs);
            self.persisted = hs;
        }
        if self.unstable <= self.log.last_index() {
            ready.entries = self
                .log
                .slice(self.unstable, self.log.last_index(), usize::MAX);
        }
        self.unstable = self.log.last_index() + 1;
        if self.commit > self.delivered {
            ready.committed = self.log.slice(self.delivered + 1, self.commit, usize::MAX);
            self.delivered = self.commit;
        }
        ready
    }

    fn hard_state(&self) -> HardState {
        HardState {
            term: self.term,
            vote: self.vote,
            commit: self.commit,
        }
    }

    // ─── Messages ─────────────────────────────────────────────────────────

    pub fn step(&mut self, m: Message) {
        if m.term > self.term {
            match m.msg {
                Msg::Vote { pre, force, .. } => {
                    // Leader stickiness (dissertation 4.2.3): while we hear
                    // from a live leader, ignore candidates, so a flapping or
                    // removed node cannot disrupt the cluster.
                    let lease =
                        self.leader.is_some() && self.election_elapsed < self.cfg.election_ticks;
                    if lease && !force {
                        return;
                    }
                    if !pre {
                        // Adopt the term, but keep the election timer: it is
                        // reset only by a live leader or by granting a vote.
                        // Otherwise a candidate that cannot win (stale log,
                        // fast clock) resets everyone's timer forever and
                        // starves the node that could.
                        let elapsed = self.election_elapsed;
                        self.become_follower(m.term, None);
                        self.election_elapsed = elapsed;
                    }
                }
                Msg::VoteResp {
                    pre: true,
                    granted: true,
                } => {}
                Msg::Append { .. } | Msg::Snapshot(_) => self.become_follower(m.term, Some(m.from)),
                _ => self.become_follower(m.term, None),
            }
        } else if m.term < self.term {
            match m.msg {
                // Tell a stale leader about the newer term so it steps down.
                Msg::Append { .. } | Msg::Snapshot(_) => self.send(
                    m.from,
                    Msg::AppendResp {
                        success: false,
                        index: self.log.last_index(),
                        probe: 0,
                    },
                ),
                // Reply with our term so a candidate that fell behind catches
                // up. Silence would let a node with a faster clock stay ahead
                // forever and starve the one candidate that could win.
                Msg::Vote { pre, .. } => self.send(
                    m.from,
                    Msg::VoteResp {
                        pre,
                        granted: false,
                    },
                ),
                _ => {}
            }
            return;
        }

        match m.msg {
            Msg::Vote {
                pre,
                last_index,
                last_term,
                ..
            } => self.handle_vote(m.from, m.term, pre, last_index, last_term),
            Msg::VoteResp { pre, granted } => self.handle_vote_resp(m.from, pre, granted),
            Msg::Append {
                prev_index,
                prev_term,
                entries,
                commit,
                probe,
            } => {
                if self.role != Role::Follower {
                    self.become_follower(m.term, Some(m.from));
                }
                self.leader = Some(m.from);
                self.election_elapsed = 0;
                self.handle_append(m.from, prev_index, prev_term, entries, commit, probe);
            }
            Msg::AppendResp {
                success,
                index,
                probe,
            } => self.handle_append_resp(m.from, success, index, probe),
            Msg::Snapshot(snap) => {
                if self.role != Role::Follower {
                    self.become_follower(m.term, Some(m.from));
                }
                self.leader = Some(m.from);
                self.election_elapsed = 0;
                self.handle_snapshot(m.from, snap);
            }
            Msg::TimeoutNow => {
                if self.is_voter() {
                    self.campaign(true);
                }
            }
        }
    }

    fn handle_vote(&mut self, from: NodeId, term: u64, pre: bool, last_index: u64, last_term: u64) {
        let up_to_date = (last_term, last_index) >= (self.log.last_term(), self.log.last_index())
            || self.cfg.fault == RaftFault::VoteIgnoresLog;
        let can_vote = self.vote == Some(from)
            || (self.vote.is_none() && self.leader.is_none())
            || (pre && term > self.term);
        let granted = up_to_date && can_vote;
        if granted && !pre {
            self.vote = Some(from);
            self.election_elapsed = 0;
        }
        // A granted pre-vote answers in the candidate's future term.
        let reply_term = if pre && granted { term } else { self.term };
        self.out.messages.push(Message {
            from: self.id,
            to: from,
            term: reply_term,
            msg: Msg::VoteResp { pre, granted },
        });
    }

    fn handle_vote_resp(&mut self, from: NodeId, pre: bool, granted: bool) {
        let expected = if pre {
            Role::PreCandidate
        } else {
            Role::Candidate
        };
        if self.role != expected {
            return;
        }
        self.votes.insert(from, granted);
        let yes = self
            .voters()
            .iter()
            .filter(|v| self.votes.get(v) == Some(&true))
            .count();
        let no = self
            .voters()
            .iter()
            .filter(|v| self.votes.get(v) == Some(&false))
            .count();
        if yes >= self.quorum() {
            if pre {
                self.campaign_real(false);
            } else {
                self.become_leader();
            }
        } else if no >= self.quorum() {
            self.become_follower(self.term, None);
        }
    }

    fn handle_append(
        &mut self,
        from: NodeId,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
        probe: u64,
    ) {
        if prev_index < self.commit {
            // Everything up to our commit index already matches the leader.
            let skip = (self.commit - prev_index) as usize;
            if skip >= entries.len() {
                let index = self.commit;
                self.send(
                    from,
                    Msg::AppendResp {
                        success: true,
                        index,
                        probe,
                    },
                );
                return;
            }
            let rest = entries[skip..].to_vec();
            let prev_term = self
                .log
                .term(self.commit)
                .expect("committed entries are present");
            return self.handle_append(from, self.commit, prev_term, rest, commit, probe);
        }
        let matches = self.log.term(prev_index) == Some(prev_term)
            || (self.cfg.fault == RaftFault::SkipPrevCheck && prev_index <= self.log.last_index());
        if !matches {
            let hint = self.reject_hint(prev_index);
            self.send(
                from,
                Msg::AppendResp {
                    success: false,
                    index: hint,
                    probe,
                },
            );
            return;
        }
        let last_new = prev_index + entries.len() as u64;
        for entry in entries {
            match self.log.term(entry.index) {
                Some(t) if t == entry.term => continue,
                Some(_) => {
                    assert!(
                        entry.index > self.commit,
                        "raft: a committed entry conflicts with the leader"
                    );
                    self.truncate_log(entry.index);
                    self.push_entry(entry);
                }
                None => self.push_entry(entry),
            }
        }
        let new_commit = commit.min(last_new);
        if new_commit > self.commit {
            self.commit = new_commit;
        }
        self.send(
            from,
            Msg::AppendResp {
                success: true,
                index: last_new,
                probe,
            },
        );
    }

    /// Where a leader should retry from after we reject `prev_index`: before
    /// the first entry of the conflicting term, skipping a whole term per
    /// round trip instead of one entry.
    fn reject_hint(&self, prev_index: u64) -> u64 {
        let last = self.log.last_index();
        if prev_index > last {
            return last;
        }
        let Some(term) = self.log.term(prev_index) else {
            return self.commit;
        };
        let mut i = prev_index;
        while i > self.commit.max(self.log.snap_index) && self.log.term(i - 1) == Some(term) {
            i -= 1;
        }
        (i - 1).max(self.commit)
    }

    fn handle_append_resp(&mut self, from: NodeId, success: bool, index: u64, probe: u64) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.log.last_index();
        let Some(p) = self.progress.get_mut(&from) else {
            return;
        };
        p.active = true;
        p.acked_probe = p.acked_probe.max(probe);
        if success {
            if index > p.matched {
                p.matched = index;
            }
            p.next = p.next.max(p.matched + 1);
            match p.state {
                ProgressState::Probe => p.state = ProgressState::Replicate,
                ProgressState::Snapshot(at) if p.matched >= at => {
                    p.state = ProgressState::Replicate
                }
                _ => {}
            }
            let caught_up = p.matched == last;
            self.maybe_commit();
            self.release_reads();
            if self.transfer_to == Some(from) && caught_up {
                self.send(from, Msg::TimeoutNow);
            }
            if self.progress.get(&from).is_some_and(|p| p.next <= last) {
                self.send_append(from, false);
            }
        } else {
            if index < p.matched {
                return; // stale
            }
            // Back up, but never below what is known to match.
            p.next = (index + 1).min(p.next.saturating_sub(1)).max(p.matched + 1);
            if let ProgressState::Replicate = p.state {
                p.state = ProgressState::Probe;
            }
            if let ProgressState::Snapshot(_) = p.state {
                p.state = ProgressState::Probe;
            }
            self.send_append(from, false);
        }
    }

    fn handle_snapshot(&mut self, from: NodeId, snap: Snapshot) {
        let meta = snap.meta.clone();
        if meta.index <= self.commit {
            let index = self.commit;
            self.send(
                from,
                Msg::AppendResp {
                    success: true,
                    index,
                    probe: 0,
                },
            );
            return;
        }
        if self.log.term(meta.index) == Some(meta.term) {
            // We already hold this prefix: just learn that it is committed.
            self.commit = meta.index;
        } else {
            self.log = Log {
                snap_index: meta.index,
                snap_term: meta.term,
                entries: Vec::new(),
            };
            self.configs = vec![(meta.index, meta.voters.clone())];
            self.commit = meta.index;
            self.delivered = meta.index;
            self.unstable = meta.index + 1;
            self.out.snapshot = Some(snap);
        }
        self.send(
            from,
            Msg::AppendResp {
                success: true,
                index: meta.index,
                probe: 0,
            },
        );
    }

    // ─── Elections ────────────────────────────────────────────────────────

    fn campaign(&mut self, force: bool) {
        if self.cfg.pre_vote && !force {
            self.role = Role::PreCandidate;
            self.leader = None;
            self.votes.clear();
            self.election_elapsed = 0;
            self.reset_timeout();
            self.request_votes(true, false);
        } else {
            self.campaign_real(force);
        }
    }

    fn campaign_real(&mut self, force: bool) {
        self.fail_reads();
        self.term += 1;
        self.vote = Some(self.id);
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.clear();
        self.election_elapsed = 0;
        self.reset_timeout();
        self.request_votes(false, force);
    }

    fn request_votes(&mut self, pre: bool, force: bool) {
        self.votes.insert(self.id, true);
        let term = if pre { self.term + 1 } else { self.term };
        let (last_index, last_term) = (self.log.last_index(), self.log.last_term());
        for peer in self.voters().to_vec() {
            if peer != self.id {
                self.out.messages.push(Message {
                    from: self.id,
                    to: peer,
                    term,
                    msg: Msg::Vote {
                        pre,
                        force,
                        last_index,
                        last_term,
                    },
                });
            }
        }
        if self.votes.values().filter(|&&g| g).count() >= self.quorum() {
            if pre {
                self.campaign_real(force);
            } else {
                self.become_leader();
            }
        }
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        if self.role == Role::Leader {
            self.fail_reads();
        }
        if term > self.term {
            self.term = term;
            self.vote = None;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.transfer_to = None;
        self.election_elapsed = 0;
        self.reset_timeout();
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.transfer_to = None;
        let next = self.log.last_index() + 1;
        self.progress = self
            .voters()
            .iter()
            .filter(|&&v| v != self.id)
            .map(|&v| {
                (
                    v,
                    Progress {
                        matched: 0,
                        next,
                        state: ProgressState::Probe,
                        active: true,
                        acked_probe: 0,
                        snapshot_wait: 0,
                    },
                )
            })
            .collect();
        self.append_entry(EntryData::Empty);
    }

    fn reset_timeout(&mut self) {
        self.timeout = self.cfg.election_ticks + self.rng.below(self.cfg.election_ticks.max(1));
    }

    // ─── Replication ──────────────────────────────────────────────────────

    fn append_entry(&mut self, data: EntryData) -> (u64, u64) {
        let entry = Entry {
            index: self.log.last_index() + 1,
            term: self.term,
            data,
        };
        let at = (entry.index, entry.term);
        self.push_entry(entry);
        self.maybe_commit();
        for peer in self.peers() {
            self.send_append(peer, false);
        }
        at
    }

    fn push_entry(&mut self, entry: Entry) {
        if let EntryData::Config(voters) = &entry.data {
            self.configs.push((entry.index, voters.clone()));
            if self.role == Role::Leader {
                self.sync_progress();
            }
        }
        self.unstable = self.unstable.min(entry.index);
        self.log.entries.push(entry);
    }

    fn truncate_log(&mut self, from: u64) {
        self.log.truncate_from(from);
        self.configs.retain(|(i, _)| *i < from);
        self.unstable = self.unstable.min(from);
    }

    /// Match the leader's progress map to the configuration in force.
    fn sync_progress(&mut self) {
        let voters = self.voters().to_vec();
        self.progress.retain(|id, _| voters.contains(id));
        let next = self.log.last_index() + 1;
        for v in voters {
            if v != self.id {
                self.progress.entry(v).or_insert(Progress {
                    matched: 0,
                    next,
                    state: ProgressState::Probe,
                    active: true,
                    acked_probe: 0,
                    snapshot_wait: 0,
                });
            }
        }
    }

    fn peers(&self) -> Vec<NodeId> {
        self.progress.keys().copied().collect()
    }

    fn send(&mut self, to: NodeId, msg: Msg) {
        self.out.messages.push(Message {
            from: self.id,
            to,
            term: self.term,
            msg,
        });
    }

    fn broadcast_heartbeat(&mut self) {
        self.broadcast_heartbeat_with(self.probe);
    }

    fn broadcast_heartbeat_with(&mut self, probe: u64) {
        for peer in self.peers() {
            self.send_append_probe(peer, true, probe);
        }
    }

    fn send_append(&mut self, to: NodeId, heartbeat: bool) {
        self.send_append_probe(to, heartbeat, self.probe);
    }

    fn send_append_probe(&mut self, to: NodeId, heartbeat: bool, probe: u64) {
        let last = self.log.last_index();
        let Some(p) = self.progress.get(&to).cloned() else {
            return;
        };
        if let ProgressState::Snapshot(_) = p.state {
            if heartbeat {
                // Keep the follower's election timer quiet, and keep reads moving.
                let prev = p.matched.min(self.log.snap_index);
                if let Some(prev_term) = self.log.term(prev) {
                    let msg = Msg::Append {
                        prev_index: prev,
                        prev_term,
                        entries: Vec::new(),
                        commit: self.commit.min(p.matched),
                        probe,
                    };
                    self.send(to, msg);
                }
            }
            return;
        }
        let prev_index = p.next - 1;
        let Some(prev_term) = self.log.term(prev_index) else {
            // The entries this follower needs are compacted away.
            if !self.out.snapshot_requests.contains(&to) {
                self.out.snapshot_requests.push(to);
            }
            if let Some(p) = self.progress.get_mut(&to) {
                p.state = ProgressState::Snapshot(self.log.snap_index);
                p.snapshot_wait = 0;
            }
            return;
        };
        let entries = match p.state {
            ProgressState::Probe if !heartbeat => self.log.slice(p.next, last, 1),
            ProgressState::Probe => Vec::new(),
            _ => self.log.slice(p.next, last, self.cfg.max_append_entries),
        };
        if entries.is_empty() && !heartbeat && p.next <= last {
            return;
        }
        if let (ProgressState::Replicate, Some(e)) = (p.state, entries.last()) {
            self.progress.get_mut(&to).expect("present").next = e.index + 1;
        }
        let msg = Msg::Append {
            prev_index,
            prev_term,
            entries,
            commit: self.commit,
            probe,
        };
        self.send(to, msg);
    }

    fn maybe_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.log.last_index();
        let mut matched: Vec<u64> = self
            .voters()
            .iter()
            .map(|v| {
                if *v == self.id {
                    last
                } else {
                    self.progress.get(v).map_or(0, |p| p.matched)
                }
            })
            .collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&candidate) = matched.get(self.quorum() - 1) else {
            return;
        };
        if candidate <= self.commit {
            return;
        }
        // Figure 8: only an entry from the current term is committed by
        // counting replicas; earlier entries commit indirectly with it.
        let in_term = self.log.term(candidate) == Some(self.term);
        if in_term || self.cfg.fault == RaftFault::CommitOldTerm {
            self.commit = candidate;
            // Propagate the new commit index promptly.
            for peer in self.peers() {
                if self.progress.get(&peer).is_some_and(|p| p.next > last) {
                    self.send_append(peer, true);
                }
            }
            // A leader removed from the configuration steps down once the
            // change commits.
            if !self.voters_at(self.commit).contains(&self.id) && !self.config_pending() {
                self.become_follower(self.term, None);
            }
        }
    }

    fn release_reads(&mut self) {
        if self.reads.is_empty() {
            return;
        }
        let id = self.id;
        let quorum = self.quorum();
        let voters = self.voters().to_vec();
        let progress = &self.progress;
        let mut kept = Vec::new();
        for r in self.reads.drain(..) {
            let acks = voters
                .iter()
                .filter(|&&v| v == id || progress.get(&v).is_some_and(|p| p.acked_probe >= r.probe))
                .count();
            if acks >= quorum {
                self.out.reads.push((r.id, r.index));
            } else {
                kept.push(r);
            }
        }
        self.reads = kept;
    }

    fn fail_reads(&mut self) {
        self.out
            .failed_reads
            .extend(self.reads.drain(..).map(|r| r.id));
    }
}

#[cfg(test)]
mod tests;
