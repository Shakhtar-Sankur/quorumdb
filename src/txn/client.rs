//! The transaction coordinator, run by the client, after Percolator.
//!
//! 1. `begin` takes a start timestamp from the oracle: the snapshot.
//! 2. Reads see the newest commit at or below it. A lock in the way belongs
//!    to a transaction that may commit below our snapshot, so we first learn
//!    its fate from its primary key, finishing or rolling it back.
//! 3. Writes are buffered. At commit, every written key is *prewritten*: a
//!    lock is placed, failing on any commit after our start (first committer
//!    wins, so no lost updates) or on another's lock.
//! 4. A commit timestamp is taken. Under **serializable** isolation, every
//!    key and range read is validated: nothing may have committed in it
//!    between our start and commit timestamps (write-snapshot isolation,
//!    Yabandeh & Gómez Ferro), which rules out write skew and phantoms.
//! 5. The primary key is committed: that single Raft command is the commit
//!    point. The other keys are then committed; any left behind are
//!    finished by the next reader that meets them.

use std::collections::{BTreeMap, BTreeSet};

use crate::kv::client::{CallError, KvClient};
use crate::kv::cmd::{KvError, Request, Response};
use crate::runtime::join_all;
use crate::txn::mvcc::{LockInfo, TxnStatus};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    Snapshot,
    Serializable,
}

/// Deliberately broken client behaviour, for the simulator.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnFault {
    None,
    /// Serializable transactions skip read validation: write skew slips by.
    SkipReadValidation,
}

#[derive(Clone, Debug)]
pub struct TxnOptions {
    pub isolation: Isolation,
    /// Milliseconds before a crashed transaction's locks may be rolled back.
    pub lock_ttl: u64,
    pub fault: TxnFault,
}

impl Default for TxnOptions {
    fn default() -> Self {
        TxnOptions {
            isolation: Isolation::Serializable,
            lock_ttl: 3_000,
            fault: TxnFault::None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnError {
    /// A conflict with another transaction; retrying may succeed.
    Conflict(String),
    /// The transaction was rolled back.
    Aborted,
    /// The commit may or may not have happened.
    Unknown,
    Other(String),
}

impl std::fmt::Display for TxnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxnError::Conflict(why) => write!(f, "could not serialize access: {why}"),
            TxnError::Aborted => write!(f, "transaction aborted"),
            TxnError::Unknown => write!(f, "commit outcome unknown"),
            TxnError::Other(why) => write!(f, "{why}"),
        }
    }
}

fn other(e: CallError) -> TxnError {
    TxnError::Other(format!("{e:?}"))
}

/// One timestamp from the oracle.
pub async fn timestamp(kv: &KvClient) -> Result<u64, TxnError> {
    match kv.call(Request::Timestamp { count: 1 }).await {
        Ok(Response::Ts(t)) => Ok(t),
        Ok(r) => Err(TxnError::Other(format!("unexpected {r:?}"))),
        Err(e) => Err(other(e)),
    }
}

/// Learn the fate of the transaction that holds `lock`, and finish its
/// lock on this key accordingly. If it is still running, wait: 1 ms at
/// first, doubling with each `attempt`, since most locks are released
/// within a millisecond or two.
pub async fn resolve_lock(kv: &KvClient, lock: &LockInfo, attempt: u32) -> Result<(), TxnError> {
    let status = kv
        .call(Request::CheckTxnStatus {
            primary: lock.primary.clone(),
            start_ts: lock.start_ts,
        })
        .await;
    let commit_ts = match status {
        Ok(Response::Status(TxnStatus::Committed(c))) => Some(c),
        Ok(Response::Status(TxnStatus::RolledBack)) => None,
        Ok(Response::Status(TxnStatus::Locked)) => {
            let cap = 1u64 << attempt.min(6);
            kv.io.sleep(cap / 2 + 1 + kv.io.rand(cap)).await;
            return Ok(());
        }
        Ok(r) => return Err(TxnError::Other(format!("unexpected {r:?}"))),
        Err(e) => return Err(other(e)),
    };
    kv.call(Request::ResolveLock {
        key: lock.key.clone(),
        start_ts: lock.start_ts,
        commit_ts,
    })
    .await
    .map(|_| ())
    .map_err(other)
}

/// The most telling error among concurrent results: an abort, then a
/// conflict, then anything else.
fn pick_error(results: Vec<Result<(), TxnError>>) -> Option<TxnError> {
    let mut errors: Vec<TxnError> = results.into_iter().filter_map(Result::err).collect();
    errors.sort_by_key(|e| match e {
        TxnError::Aborted => 0,
        TxnError::Conflict(_) => 1,
        _ => 2,
    });
    errors.into_iter().next()
}

pub struct Txn {
    kv: KvClient,
    opts: TxnOptions,
    pub start_ts: u64,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    reads: BTreeSet<Vec<u8>>,
    scans: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Txn {
    pub async fn begin(kv: KvClient, opts: TxnOptions) -> Result<Txn, TxnError> {
        let start_ts = timestamp(&kv).await?;
        Ok(Txn {
            kv,
            opts,
            start_ts,
            writes: BTreeMap::new(),
            reads: BTreeSet::new(),
            scans: Vec::new(),
        })
    }

    pub fn isolation(&self) -> Isolation {
        self.opts.isolation
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }
        for attempt in 0..50 {
            let req = Request::MvccGet {
                key: key.to_vec(),
                ts: self.start_ts,
            };
            match self.kv.call(req).await {
                Ok(Response::Value(v)) => {
                    self.reads.insert(key.to_vec());
                    return Ok(v);
                }
                Err(CallError::Kv(KvError::Locked(l))) => {
                    resolve_lock(&self.kv, &l, attempt).await?
                }
                Ok(r) => return Err(TxnError::Other(format!("unexpected {r:?}"))),
                Err(e) => return Err(other(e)),
            }
        }
        Err(TxnError::Conflict("key stayed locked".into()))
    }

    /// Pairs in `[start, end)` (`end` empty: unbounded), at most `limit`,
    /// across as many ranges as it takes, including our own writes.
    pub async fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnError> {
        let mut found: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut cursor = start.to_vec();
        let mut scanned_to = end.to_vec();
        'ranges: loop {
            let mut attempts = 0;
            let (rows, resume) = loop {
                attempts += 1;
                if attempts > 50 {
                    return Err(TxnError::Conflict("range stayed locked".into()));
                }
                let req = Request::MvccScan {
                    start: cursor.clone(),
                    end: end.to_vec(),
                    ts: self.start_ts,
                    limit: limit + self.writes.len(),
                };
                match self.kv.call(req).await {
                    Ok(Response::Rows { rows, resume }) => break (rows, resume),
                    Err(CallError::Kv(KvError::Locked(l))) => {
                        resolve_lock(&self.kv, &l, attempts).await?
                    }
                    Ok(r) => return Err(TxnError::Other(format!("unexpected {r:?}"))),
                    Err(e) => return Err(other(e)),
                }
            };
            found.extend(rows);
            match resume {
                Some(next)
                    if (end.is_empty() || next.as_slice() < end)
                        && found.len() < limit + self.writes.len() =>
                {
                    cursor = next;
                }
                Some(next) if found.len() >= limit + self.writes.len() => {
                    scanned_to = next;
                    break 'ranges;
                }
                _ => break 'ranges,
            }
        }
        // Our own buffered writes win.
        for (k, v) in self.writes.range(start.to_vec()..) {
            if !end.is_empty() && k.as_slice() >= end {
                break;
            }
            match v {
                Some(v) => found.insert(k.clone(), v.clone()),
                None => found.remove(k),
            };
        }
        let rows: Vec<(Vec<u8>, Vec<u8>)> = found.into_iter().take(limit).collect();
        // What was actually observed: through the last row returned if the
        // limit cut the scan short.
        let observed_end = if rows.len() == limit {
            rows.last().map(|(k, _)| {
                let mut e = k.clone();
                e.push(0);
                e
            })
        } else {
            None
        }
        .unwrap_or(scanned_to);
        self.scans.push((start.to_vec(), observed_end));
        Ok(rows)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), None);
    }

    pub fn is_read_only(&self) -> bool {
        self.writes.is_empty()
    }

    /// Commit, returning the commit timestamp.
    pub async fn commit(&mut self) -> Result<u64, TxnError> {
        if self.writes.is_empty() {
            // A read-only transaction is serializable at its snapshot.
            return Ok(self.start_ts);
        }
        let primary = self.writes.keys().next().expect("non-empty").clone();
        let writes: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .writes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Phase 1: lock every key, all at once. Parallel requests share
        // Raft rounds and fsyncs (group commit), so this costs about one
        // round trip however many keys are written.
        let results = join_all(
            writes
                .iter()
                .map(|(k, v)| self.prewrite(k, v, &primary))
                .collect(),
        )
        .await;
        if let Some(e) = pick_error(results) {
            self.rollback().await;
            return Err(e);
        }

        let commit_ts = match timestamp(&self.kv).await {
            Ok(t) => t,
            Err(e) => {
                self.rollback().await;
                return Err(e);
            }
        };

        // Serializable: everything we read must still be current at commit_ts.
        if self.opts.isolation == Isolation::Serializable
            && self.opts.fault != TxnFault::SkipReadValidation
        {
            let mut checks: Vec<Request> = self
                .reads
                .iter()
                .filter(|k| !self.writes.contains_key(*k))
                .map(|k| Request::ValidateRead {
                    key: k.clone(),
                    start_ts: self.start_ts,
                    commit_ts,
                })
                .collect();
            for (s, e) in &self.scans {
                checks.push(Request::ValidateScan {
                    start: s.clone(),
                    end: e.clone(),
                    start_ts: self.start_ts,
                    commit_ts,
                });
            }
            let results = join_all(checks.into_iter().map(|c| self.validate(c)).collect()).await;
            if let Some(e) = pick_error(results) {
                self.rollback().await;
                return Err(e);
            }
        }

        // Phase 2: the commit point.
        let req = Request::Commit {
            key: primary.clone(),
            start_ts: self.start_ts,
            commit_ts,
        };
        match self.kv.call(req).await {
            Ok(_) => {}
            Err(CallError::Kv(KvError::Aborted)) => {
                self.rollback().await;
                return Err(TxnError::Aborted);
            }
            Err(_) => return Err(TxnError::Unknown),
        }
        // Best effort, in parallel: a reader finishes any secondary left locked.
        let secondaries = writes
            .iter()
            .filter(|(k, _)| *k != primary)
            .map(|(key, _)| {
                self.kv.call(Request::Commit {
                    key: key.clone(),
                    start_ts: self.start_ts,
                    commit_ts,
                })
            });
        join_all(secondaries.collect()).await;
        Ok(commit_ts)
    }

    /// Lock one key for this transaction, resolving other transactions'
    /// locks in the way.
    async fn prewrite(
        &self,
        key: &[u8],
        value: &Option<Vec<u8>>,
        primary: &[u8],
    ) -> Result<(), TxnError> {
        for attempt in 0..20 {
            let req = Request::Prewrite {
                key: key.to_vec(),
                value: value.clone(),
                primary: primary.to_vec(),
                start_ts: self.start_ts,
                ttl: self.opts.lock_ttl,
            };
            match self.kv.call(req).await {
                Ok(_) => return Ok(()),
                Err(CallError::Kv(KvError::Locked(l))) => {
                    resolve_lock(&self.kv, &l, attempt).await?
                }
                Err(CallError::Kv(KvError::WriteConflict)) => {
                    return Err(TxnError::Conflict(format!(
                        "write conflict on {:?}",
                        String::from_utf8_lossy(key)
                    )));
                }
                Err(CallError::Kv(KvError::Aborted)) => return Err(TxnError::Aborted),
                Err(e) => return Err(other(e)),
            }
        }
        Err(TxnError::Conflict("key stayed locked".into()))
    }

    /// Validate one key or span, following a span across ranges.
    async fn validate(&self, check: Request) -> Result<(), TxnError> {
        let mut check = check;
        loop {
            match self.kv.call(check.clone()).await {
                Ok(Response::Rows {
                    resume: Some(next), ..
                }) => {
                    let Request::ValidateScan {
                        end,
                        start_ts,
                        commit_ts,
                        ..
                    } = check
                    else {
                        return Ok(());
                    };
                    if !end.is_empty() && next >= end {
                        return Ok(());
                    }
                    check = Request::ValidateScan {
                        start: next,
                        end,
                        start_ts,
                        commit_ts,
                    };
                }
                Ok(_) => return Ok(()),
                Err(CallError::Kv(KvError::WriteConflict | KvError::Locked(_))) => {
                    return Err(TxnError::Conflict(
                        "a read was overwritten before commit".into(),
                    ));
                }
                Err(e) => return Err(other(e)),
            }
        }
    }

    /// Undo any locks we placed. The primary first: its rollback record
    /// decides the transaction, even if a prewrite of it is still in flight.
    pub async fn rollback(&mut self) {
        let mut keys = self.writes.keys().cloned();
        let Some(primary) = keys.next() else {
            return;
        };
        let rollback = |key| {
            self.kv.call(Request::Rollback {
                key,
                start_ts: self.start_ts,
            })
        };
        let _ = rollback(primary).await;
        join_all(keys.map(rollback).collect()).await;
    }
}
