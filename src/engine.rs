//! A log-structured storage engine: a write-ahead log, an in-memory sorted
//! memtable, immutable sorted tables on disk, and a manifest naming the live
//! files.
//!
//! Durability rules the code follows, and the simulator checks:
//! 1. A write is appended to the log before it is applied or acknowledged.
//! 2. In `SyncMode::Always` it is synced before `put`/`delete` returns;
//!    in `SyncMode::Manual` everything is durable once `sync()` returns.
//! 3. A table is synced before any manifest names it.
//! 4. A new manifest becomes current only through write-temp, sync, rename,
//!    sync-dir; the old log is removed only after that.
//! 5. Recovery truncates a torn log tail before appending anything new,
//!    or the next crash would hide every later record behind the garbage.

use std::collections::BTreeMap;

use crate::error::{Error, Result};
use crate::fs::Fs;
use crate::manifest::{self, Manifest};
use crate::sstable::{self, Entry, Table};
use crate::wal::{self, Op, Record};

const MANIFEST: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";

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
/// class of durability bug. Never set outside tests.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    None,
    SkipWalTruncate,
    SkipDirSync,
    NoSyncBeforeManifest,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub sync: SyncMode,
    /// Flush the memtable to a table once it holds this many bytes.
    pub memtable_bytes: usize,
    /// Compact all tables into one once there are this many.
    pub compact_at: usize,
    #[doc(hidden)]
    pub fault: Fault,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            sync: SyncMode::Always,
            memtable_bytes: 4 << 20,
            compact_at: 8,
            fault: Fault::None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub flushes: u64,
    pub compactions: u64,
    pub recovered_records: u64,
    pub truncated_tail_bytes: u64,
}

pub struct Db<F: Fs> {
    fs: F,
    opts: Options,
    mem: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    mem_bytes: usize,
    /// Oldest first; newer tables shadow older ones.
    tables: Vec<Table>,
    wal_id: u64,
    next_id: u64,
    seq: u64,
    stats: Stats,
}

impl<F: Fs> Db<F> {
    /// Open the database in `fs`, creating it if empty and recovering it if not.
    pub fn open(fs: F, opts: Options) -> Result<Self> {
        let mut db = Db {
            fs,
            opts,
            mem: BTreeMap::new(),
            mem_bytes: 0,
            tables: Vec::new(),
            wal_id: 0,
            next_id: 1,
            seq: 0,
            stats: Stats::default(),
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
                for id in m.tables {
                    let data = db.fs.read(&table_name(id))?.ok_or_else(|| {
                        Error::Corrupt(format!("manifest names missing table {id}"))
                    })?;
                    db.tables.push(Table::new(id, sstable::decode(&data)?));
                }
                db.replay_wal()?;
            }
        }
        db.remove_orphans()?;
        Ok(db)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(Op::Put(key.to_vec(), value.to_vec()))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(Op::Delete(key.to_vec()))
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.mem.get(key) {
            return value.clone();
        }
        self.tables
            .iter()
            .rev()
            .find_map(|t| t.get(key))
            .cloned()
            .flatten()
    }

    /// Every live key and value, in key order.
    pub fn scan(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut merged: BTreeMap<&[u8], &Option<Vec<u8>>> = BTreeMap::new();
        for table in &self.tables {
            for (k, v) in table.entries() {
                merged.insert(k, v);
            }
        }
        for (k, v) in &self.mem {
            merged.insert(k, v);
        }
        merged
            .into_iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k.to_vec(), v.clone())))
            .collect()
    }

    /// Make every write so far durable.
    pub fn sync(&mut self) -> Result<()> {
        Ok(self.fs.sync(&wal_name(self.wal_id))?)
    }

    /// Write the memtable to a new table and start a fresh log. Everything
    /// written so far is durable once this returns.
    pub fn flush(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return self.sync();
        }
        let entries: Vec<Entry> = self
            .mem
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let table_id = self.alloc_id();
        let name = table_name(table_id);
        self.fs.write_new(&name, &sstable::encode(&entries))?;
        if self.opts.fault != Fault::NoSyncBeforeManifest {
            self.fs.sync(&name)?;
        }

        let old_wal = self.wal_id;
        let new_wal = self.alloc_id();
        self.fs.write_new(&wal_name(new_wal), &[])?;
        self.fs.sync(&wal_name(new_wal))?;

        self.tables.push(Table::new(table_id, entries));
        self.wal_id = new_wal;
        self.write_manifest()?;
        self.fs.remove(&wal_name(old_wal))?;

        self.mem.clear();
        self.mem_bytes = 0;
        self.stats.flushes += 1;
        if self.tables.len() >= self.opts.compact_at {
            self.compact()?;
        }
        Ok(())
    }

    /// Merge every table into one. Tombstones can be dropped because no
    /// older table remains for them to hide anything in.
    pub fn compact(&mut self) -> Result<()> {
        if self.tables.len() < 2 {
            return Ok(());
        }
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for table in &self.tables {
            for (k, v) in table.entries() {
                merged.insert(k.clone(), v.clone());
            }
        }
        let entries: Vec<Entry> = merged.into_iter().filter(|(_, v)| v.is_some()).collect();
        let id = self.alloc_id();
        let name = table_name(id);
        self.fs.write_new(&name, &sstable::encode(&entries))?;
        if self.opts.fault != Fault::NoSyncBeforeManifest {
            self.fs.sync(&name)?;
        }
        let old: Vec<u64> = self.tables.iter().map(|t| t.id).collect();
        self.tables = vec![Table::new(id, entries)];
        self.write_manifest()?;
        for id in old {
            self.fs.remove(&table_name(id))?;
        }
        self.stats.compactions += 1;
        Ok(())
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    fn write(&mut self, op: Op) -> Result<()> {
        self.seq += 1;
        let wal = wal_name(self.wal_id);
        self.fs.append(
            &wal,
            &wal::encode(&Record {
                seq: self.seq,
                op: op.clone(),
            }),
        )?;
        if self.opts.sync == SyncMode::Always {
            self.fs.sync(&wal)?;
        }
        self.apply(op);
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
            self.apply(rec.op);
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
            tables: self.tables.iter().map(|t| t.id).collect(),
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
        live.extend(self.tables.iter().map(|t| table_name(t.id)));
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
