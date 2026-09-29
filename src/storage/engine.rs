//! A log-structured storage engine: a write-ahead log, an in-memory sorted
//! memtable, immutable block-indexed tables on disk arranged in levels, and
//! a manifest naming the live files.
//!
//! Levels work as in LevelDB and RocksDB. Level 0 holds freshly flushed
//! tables whose key ranges may overlap. Every deeper level is one sorted run
//! split into tables with disjoint ranges, and may hold `level_multiplier`
//! times more bytes than the level above. Compaction merges a table (or all
//! of level 0) with the tables it overlaps one level down.
//!
//! Durability rules the code follows, and the simulator checks:
//! 1. A write is appended to the log before it is applied or acknowledged.
//! 2. In `SyncMode::Always` it is synced before `put`/`delete` returns;
//!    in `SyncMode::Manual` everything is durable once `sync()` returns.
//! 3. A table is synced before any manifest names it.
//! 4. A new manifest becomes current only through write-temp, sync, rename,
//!    sync-dir; the old log and tables are removed only after that.
//! 5. Recovery truncates a torn log tail before appending anything new,
//!    or the next crash would hide every later record behind the garbage.
//! 6. A deletion is dropped by compaction only when no deeper level could
//!    hold an older value for its key; otherwise that value would reappear.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;

use crate::error::{Error, Result};
use crate::storage::fs::Fs;
use crate::storage::manifest::{self, Manifest};
use crate::storage::merge::{MergeIter, Source};
use crate::storage::sstable::{Table, TableBuilder};
use crate::storage::wal::{self, Op, Record};

const MANIFEST: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";

/// Level 0 plus six sorted levels: with the default sizes the last level
/// alone may hold a terabyte.
pub const NUM_LEVELS: usize = 7;

fn wal_name(id: u64) -> String {
    format!("wal-{id:06}.log")
}

fn table_name(id: u64) -> String {
    format!("sst-{id:06}.sst")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// Sync the log on every write. Slow, and nothing acknowledged is ever lost.
    Always,
    /// Sync only when `sync()` is called. Writes since the last sync may be
    /// lost in a crash, but never reordered or corrupted.
    Manual,
}

/// Deliberately broken behaviour, used to prove the simulator catches each
/// class of bug. Never set outside tests.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    None,
    SkipWalTruncate,
    SkipDirSync,
    NoSyncBeforeManifest,
    DropTombstonesEarly,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub sync: SyncMode,
    /// Flush the memtable to a level 0 table once it holds this many bytes.
    pub memtable_bytes: usize,
    /// Target size of a data block, the unit read from disk by a lookup.
    pub block_bytes: usize,
    /// Compaction starts a new output table once one reaches this size.
    pub table_bytes: usize,
    /// Compact level 0 into level 1 once it has this many tables.
    pub l0_compact_at: usize,
    /// Size limit of level 1. Each deeper level may be `level_multiplier`
    /// times larger than the one above it.
    pub level1_bytes: u64,
    pub level_multiplier: u64,
    /// Bloom filter bits per key. Ten gives about 1% false positives.
    pub bloom_bits_per_key: usize,
    #[doc(hidden)]
    pub fault: Fault,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            sync: SyncMode::Always,
            memtable_bytes: 4 << 20,
            block_bytes: 4 << 10,
            table_bytes: 2 << 20,
            l0_compact_at: 4,
            level1_bytes: 10 << 20,
            level_multiplier: 10,
            bloom_bits_per_key: 10,
            fault: Fault::None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub flushes: u64,
    pub compactions: u64,
    /// Compactions that only moved a table down a level, rewriting nothing.
    pub trivial_moves: u64,
    pub recovered_records: u64,
    pub truncated_tail_bytes: u64,
    pub bytes_flushed: u64,
    pub bytes_compacted: u64,
    pub tombstones_dropped: u64,
    /// Table lookups the bloom filter answered without reading a block.
    pub bloom_skips: u64,
    /// Data blocks read by point lookups.
    pub block_reads: u64,
}

pub struct Db<F: Fs> {
    fs: F,
    opts: Options,
    mem: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    mem_bytes: usize,
    /// `levels[0]` is oldest first and may overlap; every other level is
    /// sorted by key with disjoint table ranges.
    levels: Vec<Vec<Table>>,
    /// Per level, the largest key of the table last compacted from it, so
    /// successive compactions sweep the key space round-robin.
    compact_ptr: Vec<Vec<u8>>,
    wal_id: u64,
    next_id: u64,
    seq: u64,
    stats: Stats,
    bloom_skips: Cell<u64>,
    block_reads: Cell<u64>,
}

impl<F: Fs> Db<F> {
    /// Open the database in `fs`, creating it if empty and recovering it if not.
    pub fn open(fs: F, opts: Options) -> Result<Self> {
        let mut db = Db {
            fs,
            opts,
            mem: BTreeMap::new(),
            mem_bytes: 0,
            levels: (0..NUM_LEVELS).map(|_| Vec::new()).collect(),
            compact_ptr: vec![Vec::new(); NUM_LEVELS],
            wal_id: 0,
            next_id: 1,
            seq: 0,
            stats: Stats::default(),
            bloom_skips: Cell::new(0),
            block_reads: Cell::new(0),
        };
        match db.fs.read(MANIFEST)? {
            None => {
                db.wal_id = db.alloc_id();
                let wal = wal_name(db.wal_id);
                db.fs.write_new(&wal, &[])?;
                db.fs.sync(&wal)?;
                db.write_manifest()?;
            }
            Some(bytes) => {
                let m = manifest::decode(&bytes)?;
                db.next_id = m.next_id;
                db.wal_id = m.wal_id;
                db.seq = m.last_seq;
                for (level, id) in m.tables {
                    let level = level as usize;
                    if level >= NUM_LEVELS {
                        return Err(Error::Corrupt(format!(
                            "manifest: table {id} at level {level}"
                        )));
                    }
                    let table = Table::open(&db.fs, id, table_name(id)).map_err(|e| match e {
                        Error::Io(e) if e.kind() == io::ErrorKind::NotFound => {
                            Error::Corrupt(format!("manifest names missing table {id}"))
                        }
                        e => e,
                    })?;
                    db.levels[level].push(table);
                }
                for level in &mut db.levels[1..] {
                    level.sort_by(|a, b| a.smallest().cmp(b.smallest()));
                    if level.windows(2).any(|w| w[0].largest() >= w[1].smallest()) {
                        return Err(Error::Corrupt(
                            "manifest: overlapping tables within a level".into(),
                        ));
                    }
                }
                db.replay_wal()?;
            }
        }
        db.remove_orphans()?;
        Ok(db)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(vec![Op::Put(key.to_vec(), value.to_vec())])
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(vec![Op::Delete(key.to_vec())])
    }

    /// Apply several writes atomically: after a crash either all of them are
    /// recovered or none is. Later operations on the same key win.
    pub fn write_batch(&mut self, ops: Vec<Op>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        self.write(ops)
    }

    /// Newest first: the memtable, level 0 newest to oldest, then at most one
    /// table per deeper level. The first answer found, value or deletion, wins.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.mem.get(key) {
            return Ok(value.clone());
        }
        for table in self.levels[0].iter().rev() {
            if table.overlaps(key, key)
                && let Some(value) = self.probe(table, key)?
            {
                return Ok(value);
            }
        }
        for level in &self.levels[1..] {
            let i = level.partition_point(|t| t.largest() < key);
            if let Some(table) = level.get(i)
                && table.smallest() <= key
                && let Some(value) = self.probe(table, key)?
            {
                return Ok(value);
            }
        }
        Ok(None)
    }

    fn probe(&self, table: &Table, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        if !table.may_contain(key) {
            self.bloom_skips.set(self.bloom_skips.get() + 1);
            return Ok(None);
        }
        self.block_reads.set(self.block_reads.get() + 1);
        table.get(&self.fs, key)
    }

    /// Every live key and value in key order, streamed a block at a time.
    pub fn iter(&self) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_ {
        self.range(&[], None)
    }

    /// Live keys in `[start, end)` in key order (no upper bound if `end` is
    /// `None`), streamed a block at a time. Tables that end before `start`
    /// are never read.
    pub fn range<'a>(
        &'a self,
        start: &'a [u8],
        end: Option<&'a [u8]>,
    ) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a {
        let mut sources: Vec<Source<'_>> = vec![Box::new(
            self.mem
                .range::<[u8], _>((std::ops::Bound::Included(start), std::ops::Bound::Unbounded))
                .map(|(k, v)| Ok((k.clone(), v.clone()))),
        )];
        let wanted = move |t: &&Table| t.largest() >= start && end.is_none_or(|e| t.smallest() < e);
        for table in self.levels[0].iter().rev().filter(wanted) {
            sources.push(Box::new(table.iter_from(&self.fs, start)));
        }
        for level in self.levels[1..].iter().filter(|l| !l.is_empty()) {
            let first = level.partition_point(|t| t.largest() < start);
            let tables = level[first..]
                .iter()
                .take_while(move |t| end.is_none_or(|e| t.smallest() < e));
            sources.push(Box::new(
                tables.flat_map(move |t| t.iter_from(&self.fs, start)),
            ));
        }
        MergeIter::new(sources)
            .take_while(move |entry| match (entry, end) {
                (Ok((k, _)), Some(e)) => k.as_slice() < e,
                _ => true,
            })
            .filter_map(|entry| match entry {
                Ok((k, Some(v))) => Some(Ok((k, v))),
                Ok((_, None)) => None,
                Err(e) => Some(Err(e)),
            })
    }

    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.iter().collect()
    }

    /// Make every write so far durable.
    pub fn sync(&mut self) -> Result<()> {
        Ok(self.fs.sync(&wal_name(self.wal_id))?)
    }

    /// Write the memtable to a new level 0 table and start a fresh log.
    /// Everything written so far is durable once this returns.
    pub fn flush(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return self.sync();
        }
        let mut builder = TableBuilder::new(self.opts.block_bytes, self.opts.bloom_bits_per_key);
        for (k, v) in &self.mem {
            builder.add(k, v.as_deref());
        }
        let table_id = self.alloc_id();
        let table = self.write_table(table_id, builder.finish())?;
        self.stats.bytes_flushed += table.size;

        let old_wal = self.wal_id;
        let new_wal = self.alloc_id();
        self.fs.write_new(&wal_name(new_wal), &[])?;
        self.fs.sync(&wal_name(new_wal))?;

        self.levels[0].push(table);
        self.wal_id = new_wal;
        self.write_manifest()?;
        self.fs.remove(&wal_name(old_wal))?;

        self.mem.clear();
        self.mem_bytes = 0;
        self.stats.flushes += 1;
        while let Some(level) = self.pick_compaction() {
            self.compact_level(level)?;
        }
        Ok(())
    }

    /// Merge every table into one sorted run in the deepest occupied level
    /// (at least level 1). Nothing lies below it, so every deletion is dropped.
    pub fn compact(&mut self) -> Result<()> {
        let occupied: Vec<usize> = (0..NUM_LEVELS)
            .filter(|&l| !self.levels[l].is_empty())
            .collect();
        let Some(&deepest) = occupied.last() else {
            return Ok(());
        };
        let out_level = deepest.max(1);
        if occupied == [out_level] && self.levels[out_level].len() == 1 {
            return Ok(());
        }
        let inputs = occupied
            .iter()
            .map(|&l| (l, (0..self.levels[l].len()).collect()))
            .collect();
        self.run_compaction(inputs, out_level)
    }

    pub fn stats(&self) -> Stats {
        Stats {
            bloom_skips: self.bloom_skips.get(),
            block_reads: self.block_reads.get(),
            ..self.stats
        }
    }

    pub fn table_count(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    /// `(tables, bytes)` for each level.
    pub fn level_summary(&self) -> Vec<(usize, u64)> {
        self.levels
            .iter()
            .map(|l| (l.len(), l.iter().map(|t| t.size).sum()))
            .collect()
    }

    /// The deepest level holding any table.
    pub fn max_level(&self) -> usize {
        (0..NUM_LEVELS)
            .rev()
            .find(|&l| !self.levels[l].is_empty())
            .unwrap_or(0)
    }

    fn level_limit(&self, level: usize) -> u64 {
        (1..level).fold(self.opts.level1_bytes, |bytes, _| {
            bytes.saturating_mul(self.opts.level_multiplier)
        })
    }

    /// The level most over its limit, if any is.
    fn pick_compaction(&self) -> Option<usize> {
        let mut best = (1.0, None);
        let l0 = self.levels[0].len() as f64 / self.opts.l0_compact_at.max(1) as f64;
        if l0 >= best.0 {
            best = (l0, Some(0));
        }
        for level in 1..NUM_LEVELS - 1 {
            let bytes: u64 = self.levels[level].iter().map(|t| t.size).sum();
            let score = bytes as f64 / self.level_limit(level).max(1) as f64;
            if score >= best.0 && score > 0.0 {
                best = (score, Some(level));
            }
        }
        best.1
    }

    fn compact_level(&mut self, level: usize) -> Result<()> {
        let upper: Vec<usize> = if level == 0 {
            (0..self.levels[0].len()).collect()
        } else {
            let tables = &self.levels[level];
            let ptr = &self.compact_ptr[level];
            vec![
                tables
                    .iter()
                    .position(|t| t.smallest() > ptr.as_slice())
                    .unwrap_or(0),
            ]
        };
        let lo = upper
            .iter()
            .map(|&i| self.levels[level][i].smallest())
            .min()
            .expect("non-empty")
            .to_vec();
        let hi = upper
            .iter()
            .map(|&i| self.levels[level][i].largest())
            .max()
            .expect("non-empty")
            .to_vec();
        let lower: Vec<usize> = (0..self.levels[level + 1].len())
            .filter(|&i| self.levels[level + 1][i].overlaps(&lo, &hi))
            .collect();
        self.compact_ptr[level] = hi;

        if upper.len() == 1 && lower.is_empty() {
            // Nothing to merge with: move the table down without rewriting it.
            let table = self.levels[level].remove(upper[0]);
            let below = &mut self.levels[level + 1];
            let at = below.partition_point(|t| t.smallest() < table.smallest());
            below.insert(at, table);
            self.write_manifest()?;
            self.stats.trivial_moves += 1;
            return Ok(());
        }
        self.run_compaction(vec![(level, upper), (level + 1, lower)], level + 1)
    }

    /// Merge the given tables (as `(level, indices)`) into new tables at
    /// `out_level`, then swap them in with one manifest write.
    fn run_compaction(&mut self, inputs: Vec<(usize, Vec<usize>)>, out_level: usize) -> Result<()> {
        let mut next_id = self.next_id;
        let mut outputs: Vec<Table> = Vec::new();
        let mut dropped = 0;
        {
            let mut sources: Vec<Source<'_>> = Vec::new();
            for (level, indices) in &inputs {
                let tables: Vec<&Table> =
                    indices.iter().map(|&i| &self.levels[*level][i]).collect();
                if *level == 0 {
                    // Overlapping tables: one source each, newest first.
                    for t in tables.into_iter().rev() {
                        sources.push(Box::new(t.iter(&self.fs)));
                    }
                } else if !tables.is_empty() {
                    sources.push(Box::new(tables.into_iter().flat_map(|t| t.iter(&self.fs))));
                }
            }
            let mut builder: Option<TableBuilder> = None;
            for entry in MergeIter::new(sources) {
                let (key, value) = entry?;
                if value.is_none()
                    && (self.opts.fault == Fault::DropTombstonesEarly
                        || !self.deeper_may_hold(out_level, &key))
                {
                    dropped += 1;
                    continue;
                }
                let b = builder.get_or_insert_with(|| {
                    TableBuilder::new(self.opts.block_bytes, self.opts.bloom_bits_per_key)
                });
                b.add(&key, value.as_deref());
                if b.estimated_size() >= self.opts.table_bytes {
                    let finished = builder.take().expect("just used").finish();
                    outputs.push(self.write_table(next_id, finished)?);
                    next_id += 1;
                }
            }
            if let Some(b) = builder {
                outputs.push(self.write_table(next_id, b.finish())?);
                next_id += 1;
            }
        }
        self.next_id = next_id;

        let mut removed = Vec::new();
        for (level, mut indices) in inputs {
            indices.sort_unstable();
            for i in indices.into_iter().rev() {
                removed.push(self.levels[level].remove(i));
            }
        }
        self.stats.bytes_compacted += outputs.iter().map(|t| t.size).sum::<u64>();
        let out = &mut self.levels[out_level];
        out.extend(outputs);
        out.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        debug_assert!(
            out.windows(2).all(|w| w[0].largest() < w[1].smallest()),
            "compaction produced overlapping tables in level {out_level}"
        );
        self.write_manifest()?;
        for table in removed {
            self.fs.remove(&table.name)?;
        }
        self.stats.compactions += 1;
        self.stats.tombstones_dropped += dropped;
        Ok(())
    }

    /// Whether a level below `level` has a table whose range covers `key`,
    /// in which case a deletion of `key` must be kept to hide it.
    fn deeper_may_hold(&self, level: usize, key: &[u8]) -> bool {
        self.levels[level + 1..].iter().any(|tables| {
            let i = tables.partition_point(|t| t.largest() < key);
            tables.get(i).is_some_and(|t| t.smallest() <= key)
        })
    }

    fn write_table(&self, id: u64, bytes: Vec<u8>) -> Result<Table> {
        let name = table_name(id);
        self.fs.write_new(&name, &bytes)?;
        if self.opts.fault != Fault::NoSyncBeforeManifest {
            self.fs.sync(&name)?;
        }
        Table::open(&self.fs, id, name)
    }

    fn write(&mut self, ops: Vec<Op>) -> Result<()> {
        self.seq += 1;
        let wal = wal_name(self.wal_id);
        let rec = Record { seq: self.seq, ops };
        self.fs.append(&wal, &wal::encode(&rec))?;
        if self.opts.sync == SyncMode::Always {
            self.fs.sync(&wal)?;
        }
        for op in rec.ops {
            self.apply(op);
        }
        if self.mem_bytes >= self.opts.memtable_bytes {
            self.flush()?;
        }
        Ok(())
    }

    fn apply(&mut self, op: Op) {
        match op {
            Op::Put(k, v) => {
                self.mem_bytes += k.len() + v.len() + 16;
                self.mem.insert(k, Some(v));
            }
            Op::Delete(k) => {
                self.mem_bytes += k.len() + 16;
                self.mem.insert(k, None);
            }
        }
    }

    fn replay_wal(&mut self) -> Result<()> {
        let name = wal_name(self.wal_id);
        let data = self
            .fs
            .read(&name)?
            .ok_or_else(|| Error::Corrupt(format!("manifest names missing log {}", self.wal_id)))?;
        let (records, valid) = wal::decode_all(&data);
        self.stats.recovered_records = records.len() as u64;
        for rec in records {
            self.seq = self.seq.max(rec.seq);
            for op in rec.ops {
                self.apply(op);
            }
        }
        if valid < data.len() {
            self.stats.truncated_tail_bytes = (data.len() - valid) as u64;
            if self.opts.fault != Fault::SkipWalTruncate {
                self.fs.truncate(&name, valid as u64)?;
                self.fs.sync(&name)?;
            }
        }
        Ok(())
    }

    fn write_manifest(&mut self) -> Result<()> {
        let m = Manifest {
            next_id: self.next_id,
            wal_id: self.wal_id,
            last_seq: self.seq,
            tables: self
                .levels
                .iter()
                .enumerate()
                .flat_map(|(level, tables)| tables.iter().map(move |t| (level as u32, t.id)))
                .collect(),
        };
        self.fs.write_new(MANIFEST_TMP, &manifest::encode(&m))?;
        self.fs.sync(MANIFEST_TMP)?;
        self.fs.rename(MANIFEST_TMP, MANIFEST)?;
        if self.opts.fault != Fault::SkipDirSync {
            self.fs.sync_dir()?;
        }
        Ok(())
    }

    /// Remove files left behind by a crash mid-flush or mid-compaction.
    fn remove_orphans(&mut self) -> Result<()> {
        let mut live = vec![MANIFEST.to_string(), wal_name(self.wal_id)];
        live.extend(self.levels.iter().flatten().map(|t| t.name.clone()));
        let mut removed = false;
        for name in self.fs.list()? {
            if !live.contains(&name) {
                self.fs.remove(&name)?;
                removed = true;
            }
        }
        if removed && self.opts.fault != Fault::SkipDirSync {
            self.fs.sync_dir()?;
        }
        Ok(())
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}
