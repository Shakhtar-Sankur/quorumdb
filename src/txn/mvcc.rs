//! Multi-version concurrency control on one range, after Google's
//! Percolator (as used by TiKV). Every function here runs inside a range's
//! state machine when a command is applied, identically on every replica.
//!
//! Per user key, two column families (see `kv::keys`):
//! - **lock**: at most one, left by a transaction's prewrite until it
//!   commits or rolls back. It names the transaction's *primary* key, whose
//!   fate decides the whole transaction.
//! - **write**: one record per commit, keyed by commit timestamp, newest
//!   first, holding the value; or a *rollback* record, keyed by a start
//!   timestamp, that fences off a late prewrite of a rolled-back transaction.
//!
//! A read at timestamp `ts` returns the newest committed value at or below
//! `ts`, unless a lock from a transaction that started at or below `ts` is
//! in the way: that transaction may commit below `ts`, so the reader must
//! first learn its fate.

use std::collections::BTreeMap;

use crate::codec::{Reader, put_bytes, put_u64};
use crate::error::Result;
use crate::kv::keys;
use crate::storage::engine::Db;
use crate::storage::fs::Fs;
use crate::storage::wal::Op;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lock {
    pub primary: Vec<u8>,
    pub start_ts: u64,
    /// Milliseconds after `time` that the lock may be presumed abandoned.
    pub ttl: u64,
    /// The proposer's clock when the lock was written.
    pub time: u64,
    /// The value to write on commit; `None` deletes.
    pub value: Option<Vec<u8>>,
}

impl Lock {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_bytes(&mut out, &self.primary);
        put_u64(&mut out, self.start_ts);
        put_u64(&mut out, self.ttl);
        put_u64(&mut out, self.time);
        match &self.value {
            Some(v) => {
                out.push(1);
                put_bytes(&mut out, v);
            }
            None => out.push(0),
        }
        out
    }

    fn decode(data: &[u8]) -> Option<Lock> {
        let mut r = Reader::new(data);
        let lock = Lock {
            primary: r.bytes()?,
            start_ts: r.u64()?,
            ttl: r.u64()?,
            time: r.u64()?,
            value: match r.u8()? {
                1 => Some(r.bytes()?),
                0 => None,
                _ => return None,
            },
        };
        r.is_empty().then_some(lock)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteKind {
    Put(Vec<u8>),
    Delete,
    Rollback,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteRecord {
    pub start_ts: u64,
    pub kind: WriteKind,
}

impl WriteRecord {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u64(&mut out, self.start_ts);
        match &self.kind {
            WriteKind::Put(v) => {
                out.push(1);
                put_bytes(&mut out, v);
            }
            WriteKind::Delete => out.push(2),
            WriteKind::Rollback => out.push(3),
        }
        out
    }

    fn decode(data: &[u8]) -> Option<WriteRecord> {
        let mut r = Reader::new(data);
        let start_ts = r.u64()?;
        let kind = match r.u8()? {
            1 => WriteKind::Put(r.bytes()?),
            2 => WriteKind::Delete,
            3 => WriteKind::Rollback,
            _ => return None,
        };
        r.is_empty().then_some(WriteRecord { start_ts, kind })
    }
}

/// A key and its value.
pub type Pair = (Vec<u8>, Vec<u8>);

/// A lock a reader or writer ran into.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockInfo {
    pub key: Vec<u8>,
    pub primary: Vec<u8>,
    pub start_ts: u64,
    pub ttl: u64,
    pub time: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnStatus {
    Committed(u64),
    RolledBack,
    /// Still in flight, and not yet expired.
    Locked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvccError {
    /// Another transaction holds a lock on the key.
    Locked(LockInfo),
    /// Another transaction committed the key after we started.
    WriteConflict,
    /// Our transaction was rolled back (by a reader resolving its lock).
    Aborted,
    /// A committed transaction cannot be rolled back.
    AlreadyCommitted(u64),
}

/// Deliberately broken behaviour, to prove the transaction simulator
/// catches each class of isolation bug.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MvccFault {
    None,
    /// Prewrite ignores writes committed after the transaction started:
    /// lost updates.
    SkipWriteConflict,
    /// Reads ignore locks, so they can miss a commit below their timestamp.
    ReadIgnoresLocks,
}

/// The engine plus the writes of the batch being applied, so a command
/// sees the effects of earlier commands in the same batch.
pub struct Overlay<'a, F: Fs> {
    db: &'a Db<F>,
    pending: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl<'a, F: Fs> Overlay<'a, F> {
    pub fn new(db: &'a Db<F>) -> Self {
        Overlay {
            db,
            pending: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.pending.get(key) {
            Some(v) => Ok(v.clone()),
            None => self.db.get(key),
        }
    }

    /// Keys in `[lo, hi)`, merging pending writes over the engine.
    pub fn scan(&self, lo: &[u8], hi: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for row in self.db.range(lo, Some(hi)) {
            let (k, v) = row?;
            merged.insert(k, v);
        }
        for (k, v) in self.pending.range(lo.to_vec()..hi.to_vec()) {
            match v {
                Some(v) => merged.insert(k.clone(), v.clone()),
                None => merged.remove(k),
            };
        }
        Ok(merged.into_iter().collect())
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.pending.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.pending.insert(key, None);
    }

    pub fn into_ops(self) -> Vec<Op> {
        self.pending
            .into_iter()
            .map(|(k, v)| match v {
                Some(v) => Op::Put(k, v),
                None => Op::Delete(k),
            })
            .collect()
    }
}

fn load_lock<F: Fs>(ov: &Overlay<F>, key: &[u8]) -> Result<Option<Lock>> {
    Ok(ov.get(&keys::lock_key(key))?.and_then(|v| Lock::decode(&v)))
}

/// Write records of `key` with timestamp at or below `ts`, newest first.
fn writes_below<F: Fs>(ov: &Overlay<F>, key: &[u8], ts: u64) -> Result<Vec<(u64, WriteRecord)>> {
    let (lo, hi) = keys::writes_at_or_below(key, ts);
    let mut out = Vec::new();
    for (k, v) in ov.scan(&lo, &hi)? {
        if let (Some((_, keys::CF_WRITE, Some(t))), Some(rec)) =
            (keys::decode_data_key(&k), WriteRecord::decode(&v))
        {
            out.push((t, rec));
        }
    }
    Ok(out)
}

/// Every write record of `key`, newest first.
fn all_writes<F: Fs>(ov: &Overlay<F>, key: &[u8]) -> Result<Vec<(u64, WriteRecord)>> {
    writes_below(ov, key, u64::MAX)
}

/// The record our transaction left on `key`: its commit, or a rollback.
fn own_record<F: Fs>(
    ov: &Overlay<F>,
    key: &[u8],
    start_ts: u64,
) -> Result<Option<(u64, WriteRecord)>> {
    Ok(all_writes(ov, key)?
        .into_iter()
        .take_while(|(t, _)| *t >= start_ts)
        .find(|(_, r)| r.start_ts == start_ts))
}

fn lock_info(key: &[u8], l: &Lock) -> LockInfo {
    LockInfo {
        key: key.to_vec(),
        primary: l.primary.clone(),
        start_ts: l.start_ts,
        ttl: l.ttl,
        time: l.time,
    }
}

/// The value of `key` as of `ts`, or the lock in the way.
pub fn get<F: Fs>(
    ov: &Overlay<F>,
    key: &[u8],
    ts: u64,
    fault: MvccFault,
) -> Result<std::result::Result<Option<Vec<u8>>, LockInfo>> {
    if fault != MvccFault::ReadIgnoresLocks
        && let Some(l) = load_lock(ov, key)?
        && l.start_ts <= ts
    {
        return Ok(Err(lock_info(key, &l)));
    }
    for (_, rec) in writes_below(ov, key, ts)? {
        match rec.kind {
            WriteKind::Put(v) => return Ok(Ok(Some(v))),
            WriteKind::Delete => return Ok(Ok(None)),
            WriteKind::Rollback => continue,
        }
    }
    Ok(Ok(None))
}

/// Visible `(key, value)` pairs for user keys in `[start, end)` as of `ts`.
pub fn scan<F: Fs>(
    ov: &Overlay<F>,
    start: &[u8],
    end: &[u8],
    ts: u64,
    limit: usize,
    fault: MvccFault,
) -> Result<std::result::Result<Vec<Pair>, LockInfo>> {
    let (lo, hi) = keys::data_span(start, end);
    let mut out = Vec::new();
    let mut current: Option<Vec<u8>> = None;
    let mut decided = false;
    for (k, v) in ov.scan(&lo, &hi)? {
        let Some((user, cf, t)) = keys::decode_data_key(&k) else {
            continue;
        };
        if current.as_ref() != Some(&user) {
            if out.len() >= limit {
                break;
            }
            current = Some(user.clone());
            decided = false;
        }
        if decided {
            continue;
        }
        match (cf, t) {
            (keys::CF_LOCK, _) => {
                if let Some(l) = Lock::decode(&v)
                    && l.start_ts <= ts
                    && fault != MvccFault::ReadIgnoresLocks
                {
                    return Ok(Err(lock_info(&user, &l)));
                }
            }
            (keys::CF_WRITE, Some(t)) if t <= ts => {
                if let Some(rec) = WriteRecord::decode(&v) {
                    match rec.kind {
                        WriteKind::Put(val) => {
                            out.push((user, val));
                            decided = true;
                        }
                        WriteKind::Delete => decided = true,
                        WriteKind::Rollback => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(Ok(out))
}

pub struct Prewrite {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub primary: Vec<u8>,
    pub start_ts: u64,
    pub ttl: u64,
    pub now: u64,
}

pub fn prewrite<F: Fs>(
    ov: &mut Overlay<F>,
    p: Prewrite,
    fault: MvccFault,
) -> Result<std::result::Result<(), MvccError>> {
    if let Some(l) = load_lock(ov, &p.key)? {
        return Ok(if l.start_ts == p.start_ts {
            Ok(()) // a retry of our own prewrite
        } else {
            Err(MvccError::Locked(lock_info(&p.key, &l)))
        });
    }
    for (t, rec) in all_writes(ov, &p.key)? {
        if t < p.start_ts {
            break;
        }
        if rec.start_ts == p.start_ts {
            return Ok(match rec.kind {
                WriteKind::Rollback => Err(MvccError::Aborted),
                _ => Ok(()), // already committed: a late retry
            });
        }
        if rec.kind != WriteKind::Rollback && fault != MvccFault::SkipWriteConflict {
            return Ok(Err(MvccError::WriteConflict));
        }
    }
    let lock = Lock {
        primary: p.primary,
        start_ts: p.start_ts,
        ttl: p.ttl,
        time: p.now,
        value: p.value,
    };
    ov.put(keys::lock_key(&p.key), lock.encode());
    Ok(Ok(()))
}

pub fn commit<F: Fs>(
    ov: &mut Overlay<F>,
    key: &[u8],
    start_ts: u64,
    commit_ts: u64,
) -> Result<std::result::Result<(), MvccError>> {
    match load_lock(ov, key)? {
        Some(l) if l.start_ts == start_ts => {
            let kind = match l.value {
                Some(v) => WriteKind::Put(v),
                None => WriteKind::Delete,
            };
            ov.put(
                keys::write_key(key, commit_ts),
                WriteRecord { start_ts, kind }.encode(),
            );
            ov.delete(keys::lock_key(key));
            Ok(Ok(()))
        }
        _ => Ok(match own_record(ov, key, start_ts)? {
            Some((_, r)) if r.kind != WriteKind::Rollback => Ok(()),
            _ => Err(MvccError::Aborted),
        }),
    }
}

pub fn rollback<F: Fs>(
    ov: &mut Overlay<F>,
    key: &[u8],
    start_ts: u64,
) -> Result<std::result::Result<(), MvccError>> {
    if let Some((commit_ts, r)) = own_record(ov, key, start_ts)? {
        return Ok(match r.kind {
            WriteKind::Rollback => Ok(()),
            _ => Err(MvccError::AlreadyCommitted(commit_ts)),
        });
    }
    if load_lock(ov, key)?.is_some_and(|l| l.start_ts == start_ts) {
        ov.delete(keys::lock_key(key));
    }
    // Fence: a prewrite of this transaction arriving late must now fail.
    let fence = WriteRecord {
        start_ts,
        kind: WriteKind::Rollback,
    };
    ov.put(keys::write_key(key, start_ts), fence.encode());
    Ok(Ok(()))
}

/// Decide a transaction's fate from its primary key: committed, rolled
/// back, or still running. An expired lock is rolled back here, and a
/// missing one is fenced, so the answer can never change afterwards.
pub fn check_txn_status<F: Fs>(
    ov: &mut Overlay<F>,
    primary: &[u8],
    start_ts: u64,
    now: u64,
) -> Result<TxnStatus> {
    if let Some(l) = load_lock(ov, primary)?
        && l.start_ts == start_ts
    {
        if l.time.saturating_add(l.ttl) > now {
            return Ok(TxnStatus::Locked);
        }
        rollback(ov, primary, start_ts)?.ok();
        return Ok(TxnStatus::RolledBack);
    }
    match own_record(ov, primary, start_ts)? {
        Some((commit_ts, r)) if r.kind != WriteKind::Rollback => {
            Ok(TxnStatus::Committed(commit_ts))
        }
        Some(_) => Ok(TxnStatus::RolledBack),
        None => {
            rollback(ov, primary, start_ts)?.ok();
            Ok(TxnStatus::RolledBack)
        }
    }
}

/// Finish a secondary lock of a transaction whose fate is known.
pub fn resolve<F: Fs>(
    ov: &mut Overlay<F>,
    key: &[u8],
    start_ts: u64,
    commit_ts: Option<u64>,
) -> Result<()> {
    if !load_lock(ov, key)?.is_some_and(|l| l.start_ts == start_ts) {
        return Ok(());
    }
    match commit_ts {
        Some(c) => {
            commit(ov, key, start_ts, c)?.ok();
        }
        None => {
            rollback(ov, key, start_ts)?.ok();
        }
    }
    Ok(())
}

/// Serializable read validation (write-snapshot isolation): a key the
/// transaction read at `start_ts` must have no commit in
/// `(start_ts, commit_ts]` and no other transaction's lock that could
/// commit there. Otherwise the read is stale at `commit_ts`.
pub fn validate_read<F: Fs>(
    ov: &Overlay<F>,
    key: &[u8],
    start_ts: u64,
    commit_ts: u64,
) -> Result<std::result::Result<(), MvccError>> {
    if let Some(l) = load_lock(ov, key)?
        && l.start_ts != start_ts
        && l.start_ts < commit_ts
    {
        return Ok(Err(MvccError::Locked(lock_info(key, &l))));
    }
    for (t, rec) in writes_below(ov, key, commit_ts)? {
        if t <= start_ts {
            break;
        }
        if rec.kind != WriteKind::Rollback && rec.start_ts != start_ts {
            return Ok(Err(MvccError::WriteConflict));
        }
    }
    Ok(Ok(()))
}

/// Serializable validation of a scanned span: no key in `[start, end)` may
/// have been committed in `(start_ts, commit_ts]` or be locked by another
/// transaction that could commit there. Catches phantoms as well as
/// changed rows.
pub fn validate_range<F: Fs>(
    ov: &Overlay<F>,
    start: &[u8],
    end: &[u8],
    start_ts: u64,
    commit_ts: u64,
) -> Result<std::result::Result<(), MvccError>> {
    let (lo, hi) = keys::data_span(start, end);
    for (k, v) in ov.scan(&lo, &hi)? {
        let Some((user, cf, t)) = keys::decode_data_key(&k) else {
            continue;
        };
        match (cf, t) {
            (keys::CF_LOCK, _) => {
                if let Some(l) = Lock::decode(&v)
                    && l.start_ts != start_ts
                    && l.start_ts < commit_ts
                {
                    return Ok(Err(MvccError::Locked(lock_info(&user, &l))));
                }
            }
            (keys::CF_WRITE, Some(t)) if t > start_ts && t <= commit_ts => {
                if let Some(rec) = WriteRecord::decode(&v)
                    && rec.kind != WriteKind::Rollback
                    && rec.start_ts != start_ts
                {
                    return Ok(Err(MvccError::WriteConflict));
                }
            }
            _ => {}
        }
    }
    Ok(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::Options;
    use crate::storage::fs::SimFs;

    fn db() -> Db<SimFs> {
        Db::open(SimFs::new(1), Options::default()).unwrap()
    }

    fn apply(db: &mut Db<SimFs>, f: impl FnOnce(&mut Overlay<SimFs>)) {
        let ops = {
            let mut ov = Overlay::new(db);
            f(&mut ov);
            ov.into_ops()
        };
        db.write_batch(ops).unwrap();
    }

    fn pw(key: &[u8], value: &[u8], primary: &[u8], start_ts: u64) -> Prewrite {
        Prewrite {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            primary: primary.to_vec(),
            start_ts,
            ttl: 100,
            now: 0,
        }
    }

    fn read(db: &Db<SimFs>, key: &[u8], ts: u64) -> std::result::Result<Option<Vec<u8>>, LockInfo> {
        get(&Overlay::new(db), key, ts, MvccFault::None).unwrap()
    }

    #[test]
    fn snapshot_reads_see_only_commits_at_or_below_their_timestamp() {
        let mut db = db();
        apply(&mut db, |ov| {
            prewrite(ov, pw(b"k", b"v1", b"k", 10), MvccFault::None)
                .unwrap()
                .unwrap();
            commit(ov, b"k", 10, 11).unwrap().unwrap();
            prewrite(ov, pw(b"k", b"v2", b"k", 20), MvccFault::None)
                .unwrap()
                .unwrap();
        });
        assert_eq!(read(&db, b"k", 10), Ok(None));
        assert_eq!(read(&db, b"k", 15), Ok(Some(b"v1".to_vec())));
        assert!(
            read(&db, b"k", 25).is_err(),
            "a lock at or below the read timestamp blocks it"
        );
        apply(&mut db, |ov| commit(ov, b"k", 20, 21).unwrap().unwrap());
        assert_eq!(read(&db, b"k", 25), Ok(Some(b"v2".to_vec())));
        assert_eq!(read(&db, b"k", 20), Ok(Some(b"v1".to_vec())));
    }

    #[test]
    fn write_write_conflicts_and_rollback_fences() {
        let mut db = db();
        apply(&mut db, |ov| {
            prewrite(ov, pw(b"k", b"a", b"k", 10), MvccFault::None)
                .unwrap()
                .unwrap();
            commit(ov, b"k", 10, 12).unwrap().unwrap();
            // Started before that commit: a lost update, refused.
            assert_eq!(
                prewrite(ov, pw(b"k", b"b", b"k", 11), MvccFault::None).unwrap(),
                Err(MvccError::WriteConflict)
            );
            // Rolled back before its prewrite arrives: fenced.
            assert_eq!(
                check_txn_status(ov, b"p", 30, 0).unwrap(),
                TxnStatus::RolledBack
            );
            assert_eq!(
                prewrite(ov, pw(b"p", b"late", b"p", 30), MvccFault::None).unwrap(),
                Err(MvccError::Aborted)
            );
        });
    }

    #[test]
    fn status_of_committed_and_expired_transactions() {
        let mut db = db();
        apply(&mut db, |ov| {
            prewrite(ov, pw(b"p", b"1", b"p", 10), MvccFault::None)
                .unwrap()
                .unwrap();
            prewrite(ov, pw(b"s", b"2", b"p", 10), MvccFault::None)
                .unwrap()
                .unwrap();
            assert_eq!(
                check_txn_status(ov, b"p", 10, 50).unwrap(),
                TxnStatus::Locked
            );
            commit(ov, b"p", 10, 11).unwrap().unwrap();
            assert_eq!(
                check_txn_status(ov, b"p", 10, 500).unwrap(),
                TxnStatus::Committed(11)
            );
            resolve(ov, b"s", 10, Some(11)).unwrap();
            prewrite(ov, pw(b"q", b"x", b"q", 20), MvccFault::None)
                .unwrap()
                .unwrap();
            assert_eq!(
                check_txn_status(ov, b"q", 20, 500).unwrap(),
                TxnStatus::RolledBack
            );
            assert_eq!(commit(ov, b"q", 20, 21).unwrap(), Err(MvccError::Aborted));
        });
        assert_eq!(read(&db, b"s", 30), Ok(Some(b"2".to_vec())));
        assert_eq!(read(&db, b"q", 30), Ok(None));
    }

    #[test]
    fn read_validation_catches_a_commit_after_the_read() {
        let mut db = db();
        apply(&mut db, |ov| {
            prewrite(ov, pw(b"k", b"a", b"k", 15), MvccFault::None)
                .unwrap()
                .unwrap();
            commit(ov, b"k", 15, 16).unwrap().unwrap();
            // Read at 10, committing at 20: the commit at 16 invalidates it.
            assert_eq!(
                validate_read(ov, b"k", 10, 20).unwrap(),
                Err(MvccError::WriteConflict)
            );
            // Read at 17: nothing since.
            assert_eq!(validate_read(ov, b"k", 17, 20).unwrap(), Ok(()));
        });
    }
}
