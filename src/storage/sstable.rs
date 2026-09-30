//! Sorted string tables: immutable, sorted runs of keys on disk. A tombstone
//! (`None`) records a deletion so it can hide older values in deeper levels
//! until compaction proves it safe to drop.
//!
//! File layout:
//! ```text
//! [data block]... [bloom filter] [block index] [footer: 44 bytes]
//! data block:  [entries...][crc32 of entries: u32]
//! entry:       [kind: u8][key][value if put]          (byte strings length-prefixed)
//! block index: [n: u32] n x [first key][last key][offset: u64][len: u32]
//! footer:      [bloom off: u64][bloom len: u32][index off: u64][index len: u32]
//!              [entry count: u64][crc32 of everything from the bloom filter to here: u32]
//!              [magic "QDBSST02"]
//! ```
//! Opening a table reads only the footer, the bloom filter and the index; a
//! lookup then reads exactly one data block. Every block and the metadata are
//! checksummed, and a table is synced before any manifest names it, so a
//! checksum failure here is real corruption, never an expected crash artefact.

use std::sync::Arc;

use crate::codec::{Reader, put_bytes, put_u32, put_u64};
use crate::crc::crc32;
use crate::error::{Error, Result};
use crate::storage::bloom::{self, Bloom};
use crate::storage::cache::BlockCache;
use crate::storage::fs::Fs;

const MAGIC: &[u8; 8] = b"QDBSST02";
/// A table whose bloom filter also holds each key's prefix. Many bits away
/// from `MAGIC`, so no single bit flip turns one into the other.
const MAGIC_PREFIXED: &[u8; 8] = b"QDBPSST1";

/// Where a key's bloom prefix ends, for keys that have one. Tables built
/// with it can rule themselves out of a scan confined to one prefix.
pub type PrefixFn = fn(&[u8]) -> Option<usize>;
const FOOTER: u64 = 44;

/// A key and its value, or `None` for a tombstone.
pub type Entry = (Vec<u8>, Option<Vec<u8>>);

fn put_entry(out: &mut Vec<u8>, key: &[u8], value: Option<&[u8]>) {
    match value {
        Some(v) => {
            out.push(1);
            put_bytes(out, key);
            put_bytes(out, v);
        }
        None => {
            out.push(0);
            put_bytes(out, key);
        }
    }
}

/// A block entry borrowed from the block's bytes.
type RawEntry<'a> = (&'a [u8], Option<&'a [u8]>);

fn slice<'a>(r: &mut Reader<'a>) -> Option<&'a [u8]> {
    let len = r.u32()? as usize;
    r.take(len)
}

/// Verify a block's checksum and split it into borrowed entries, so a
/// lookup copies only the one value it returns.
fn parse_block(data: &[u8]) -> std::result::Result<Vec<RawEntry<'_>>, &'static str> {
    if data.len() < 4 {
        return Err("short block");
    }
    let (body, tail) = data.split_at(data.len() - 4);
    if crc32(body) != u32::from_le_bytes(tail.try_into().expect("4 bytes")) {
        return Err("block checksum mismatch");
    }
    let mut r = Reader::new(body);
    let mut entries = Vec::new();
    while !r.is_empty() {
        let entry = match r.u8() {
            Some(1) => slice(&mut r).zip(slice(&mut r)).map(|(k, v)| (k, Some(v))),
            Some(0) => slice(&mut r).map(|k| (k, None)),
            _ => None,
        };
        entries.push(entry.ok_or("truncated entry")?);
    }
    Ok(entries)
}

struct BlockHandle {
    first: Vec<u8>,
    last: Vec<u8>,
    offset: u64,
    len: u32,
}

/// Builds a table in memory from keys added in strictly increasing order.
pub struct TableBuilder {
    block_bytes: usize,
    bits_per_key: usize,
    out: Vec<u8>,
    block: Vec<u8>,
    block_first: Option<Vec<u8>>,
    block_last: Vec<u8>,
    index: Vec<BlockHandle>,
    hashes: Vec<u64>,
    count: u64,
    prefix: Option<PrefixFn>,
    last_prefix: Option<Vec<u8>>,
}

impl TableBuilder {
    pub fn new(block_bytes: usize, bits_per_key: usize) -> Self {
        TableBuilder {
            block_bytes: block_bytes.max(1),
            bits_per_key,
            out: Vec::new(),
            block: Vec::new(),
            block_first: None,
            block_last: Vec::new(),
            index: Vec::new(),
            hashes: Vec::new(),
            count: 0,
            prefix: None,
            last_prefix: None,
        }
    }

    /// Also add each key's prefix, as `prefix` finds it, to the filter.
    pub fn with_prefix(mut self, prefix: Option<PrefixFn>) -> Self {
        self.prefix = prefix;
        self
    }

    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) {
        debug_assert!(
            self.count == 0 || key > self.last_key(),
            "keys must be added in increasing order"
        );
        if self.block_first.is_none() {
            self.block_first = Some(key.to_vec());
        }
        put_entry(&mut self.block, key, value);
        self.block_last = key.to_vec();
        self.hashes.push(bloom::hash(key));
        if let Some(n) = self.prefix.and_then(|f| f(key)) {
            let p = &key[..n];
            if self.last_prefix.as_deref() != Some(p) {
                self.hashes.push(bloom::hash(p));
                self.last_prefix = Some(p.to_vec());
            }
        }
        self.count += 1;
        if self.block.len() >= self.block_bytes {
            self.finish_block();
        }
    }

    fn last_key(&self) -> &[u8] {
        if self.block_first.is_some() {
            &self.block_last
        } else {
            &self.index.last().expect("count > 0").last
        }
    }

    fn finish_block(&mut self) {
        let Some(first) = self.block_first.take() else {
            return;
        };
        let crc = crc32(&self.block);
        put_u32(&mut self.block, crc);
        self.index.push(BlockHandle {
            first,
            last: std::mem::take(&mut self.block_last),
            offset: self.out.len() as u64,
            len: self.block.len() as u32,
        });
        self.out.append(&mut self.block);
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn estimated_size(&self) -> usize {
        self.out.len() + self.block.len()
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.finish_block();
        let bloom = Bloom::build(&self.hashes, self.bits_per_key).encode();
        let mut index = Vec::new();
        put_u32(&mut index, self.index.len() as u32);
        for h in &self.index {
            put_bytes(&mut index, &h.first);
            put_bytes(&mut index, &h.last);
            put_u64(&mut index, h.offset);
            put_u32(&mut index, h.len);
        }
        let bloom_off = self.out.len() as u64;
        self.out.extend_from_slice(&bloom);
        let index_off = self.out.len() as u64;
        self.out.extend_from_slice(&index);
        put_u64(&mut self.out, bloom_off);
        put_u32(&mut self.out, bloom.len() as u32);
        put_u64(&mut self.out, index_off);
        put_u32(&mut self.out, index.len() as u32);
        put_u64(&mut self.out, self.count);
        let meta_crc = crc32(&self.out[bloom_off as usize..]);
        put_u32(&mut self.out, meta_crc);
        self.out.extend_from_slice(if self.prefix.is_some() {
            MAGIC_PREFIXED
        } else {
            MAGIC
        });
        self.out
    }
}

/// An open table: its index and bloom filter in memory, its data on disk.
pub struct Table {
    pub id: u64,
    pub name: String,
    pub size: u64,
    pub entries: u64,
    index: Vec<BlockHandle>,
    bloom: Bloom,
    /// Whether the bloom filter holds key prefixes too.
    prefixed: bool,
    cache: Option<Arc<BlockCache>>,
}

impl Table {
    pub fn open<F: Fs>(fs: &F, id: u64, name: String) -> Result<Table> {
        let corrupt = |why: &str| Error::Corrupt(format!("{name}: {why}"));
        let size = fs.size(&name)?;
        if size < FOOTER {
            return Err(corrupt("shorter than a footer"));
        }
        let footer = fs.read_at(&name, size - FOOTER, FOOTER as usize)?;
        let prefixed = match &footer[36..] {
            m if m == MAGIC => false,
            m if m == MAGIC_PREFIXED => true,
            _ => return Err(corrupt("bad magic")),
        };
        let mut r = Reader::new(&footer[..36]);
        let fields = (|| Some((r.u64()?, r.u32()?, r.u64()?, r.u32()?, r.u64()?, r.u32()?)))();
        let (bloom_off, bloom_len, index_off, index_len, entries, meta_crc) =
            fields.ok_or_else(|| corrupt("short footer"))?;
        if bloom_off.checked_add(bloom_len as u64) != Some(index_off)
            || index_off.checked_add(index_len as u64) != Some(size - FOOTER)
        {
            return Err(corrupt("footer offsets disagree with the file size"));
        }
        // The checksum covers the bloom filter, the index and the footer
        // fields before it.
        let meta_len = bloom_len as usize + index_len as usize;
        let meta = fs.read_at(&name, bloom_off, meta_len + 32)?;
        if crc32(&meta) != meta_crc {
            return Err(corrupt("metadata checksum mismatch"));
        }
        let (bloom_bytes, index_bytes) = meta[..meta_len].split_at(bloom_len as usize);
        let bloom = Bloom::decode(bloom_bytes).ok_or_else(|| corrupt("bad bloom filter"))?;

        let mut r = Reader::new(index_bytes);
        let n = r.u32().ok_or_else(|| corrupt("short index"))?;
        let mut index = Vec::with_capacity(n.min(1 << 20) as usize);
        for _ in 0..n {
            let handle = (|| {
                Some(BlockHandle {
                    first: r.bytes()?,
                    last: r.bytes()?,
                    offset: r.u64()?,
                    len: r.u32()?,
                })
            })();
            index.push(handle.ok_or_else(|| corrupt("truncated index"))?);
        }
        if index.is_empty() || !r.is_empty() {
            return Err(corrupt("empty or malformed index"));
        }
        if index.windows(2).any(|w| w[0].last >= w[1].first)
            || index.iter().any(|h| h.first > h.last)
        {
            return Err(corrupt("index keys out of order"));
        }
        Ok(Table {
            id,
            name,
            size,
            entries,
            index,
            bloom,
            prefixed,
            cache: None,
        })
    }

    /// Serve blocks through `cache`, shared with the database's other tables.
    pub fn with_cache(mut self, cache: Arc<BlockCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn smallest(&self) -> &[u8] {
        &self.index[0].first
    }

    pub fn largest(&self) -> &[u8] {
        &self.index[self.index.len() - 1].last
    }

    pub fn overlaps(&self, lo: &[u8], hi: &[u8]) -> bool {
        self.smallest() <= hi && self.largest() >= lo
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }

    /// Whether any key with this bloom prefix may be here.
    pub fn may_contain_prefix(&self, prefix: &[u8]) -> bool {
        !self.prefixed || self.bloom.may_contain(prefix)
    }

    /// `Some(None)` is a tombstone; `None` means this table knows nothing
    /// about `key`. Does not consult the bloom filter; callers do.
    pub fn get<F: Fs>(&self, fs: &F, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        let i = self.index.partition_point(|h| h.last.as_slice() < key);
        match self.index.get(i) {
            Some(h) if h.first.as_slice() <= key => {
                let entries = self.entries(fs, i)?;
                Ok(entries
                    .binary_search_by(|(k, _)| k.as_slice().cmp(key))
                    .ok()
                    .map(|j| entries[j].1.clone()))
            }
            _ => Ok(None),
        }
    }

    /// Block `i`'s entries, from the cache if it holds them.
    fn entries<F: Fs>(&self, fs: &F, i: usize) -> Result<Arc<Vec<Entry>>> {
        if let Some(entries) = self.cache.as_ref().and_then(|c| c.get((self.id, i))) {
            return Ok(entries);
        }
        let h = &self.index[i];
        let data = fs.read_at(&self.name, h.offset, h.len as usize)?;
        let entries: Vec<Entry> = parse_block(&data)
            .map_err(|why| self.corrupt_block(i, why))?
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.map(<[u8]>::to_vec)))
            .collect();
        let entries = Arc::new(entries);
        if let Some(c) = &self.cache {
            c.insert((self.id, i), entries.clone(), h.len as usize);
        }
        Ok(entries)
    }

    fn corrupt_block(&self, i: usize, why: &str) -> Error {
        Error::Corrupt(format!("{} block {i}: {why}", self.name))
    }

    /// Every entry in key order, reading one block at a time.
    pub fn iter<'a, F: Fs>(&'a self, fs: &'a F) -> TableIter<'a, F> {
        TableIter {
            table: self,
            fs,
            next_block: 0,
            buf: Arc::new(Vec::new()),
            pos: 0,
            seek: None,
        }
    }

    /// Entries with keys at or after `start`, skipping every block that
    /// ends before it.
    pub fn iter_from<'a, F: Fs>(&'a self, fs: &'a F, start: &'a [u8]) -> TableIter<'a, F> {
        let first = self.index.partition_point(|h| h.last.as_slice() < start);
        TableIter {
            table: self,
            fs,
            next_block: first,
            buf: Arc::new(Vec::new()),
            pos: 0,
            seek: Some(start),
        }
    }
}

pub struct TableIter<'a, F: Fs> {
    table: &'a Table,
    fs: &'a F,
    next_block: usize,
    buf: Arc<Vec<Entry>>,
    pos: usize,
    /// Skip entries before this key in the first block read.
    seek: Option<&'a [u8]>,
}

impl<F: Fs> Iterator for TableIter<'_, F> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.buf.get(self.pos) {
                self.pos += 1;
                return Some(Ok(entry.clone()));
            }
            if self.next_block >= self.table.index.len() {
                return None;
            }
            let block = self.table.entries(self.fs, self.next_block);
            self.next_block += 1;
            match block {
                Ok(entries) => {
                    self.pos = match self.seek.take() {
                        Some(start) => entries.partition_point(|(k, _)| k.as_slice() < start),
                        None => 0,
                    };
                    self.buf = entries;
                }
                Err(e) => {
                    self.next_block = self.table.index.len();
                    return Some(Err(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::fs::SimFs;

    fn build(n: u32, block_bytes: usize) -> (SimFs, Table) {
        let fs = SimFs::new(1);
        let mut b = TableBuilder::new(block_bytes, 10);
        for i in 0..n {
            let key = format!("key{i:05}");
            if i % 7 == 0 {
                b.add(key.as_bytes(), None);
            } else {
                b.add(key.as_bytes(), Some(format!("v{i}").as_bytes()));
            }
        }
        fs.write_new("t.sst", &b.finish()).unwrap();
        let t = Table::open(&fs, 1, "t.sst".to_string()).unwrap();
        (fs, t)
    }

    #[test]
    fn lookups_across_many_blocks() {
        let (fs, t) = build(1000, 64);
        assert!(
            t.index.len() > 50,
            "expected many blocks, got {}",
            t.index.len()
        );
        assert_eq!(t.entries, 1000);
        assert_eq!(t.get(&fs, b"key00003").unwrap(), Some(Some(b"v3".to_vec())));
        assert_eq!(t.get(&fs, b"key00007").unwrap(), Some(None));
        assert_eq!(
            t.get(&fs, b"key00999").unwrap(),
            Some(Some(b"v999".to_vec()))
        );
        assert_eq!(t.get(&fs, b"key01000").unwrap(), None);
        assert_eq!(t.get(&fs, b"a").unwrap(), None);
        assert_eq!(
            (t.smallest(), t.largest()),
            (&b"key00000"[..], &b"key00999"[..])
        );
    }

    #[test]
    fn iterates_every_entry_in_order() {
        let (fs, t) = build(500, 100);
        let keys: Vec<Vec<u8>> = t.iter(&fs).map(|e| e.unwrap().0).collect();
        assert_eq!(keys.len(), 500);
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn any_single_bit_flip_is_detected() {
        let mut b = TableBuilder::new(32, 10);
        for i in 0..20u32 {
            b.add(format!("k{i:02}").as_bytes(), Some(b"value"));
        }
        let bytes = b.finish();
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut bad = bytes.clone();
                bad[i] ^= 1 << bit;
                let fs = SimFs::new(1);
                fs.write_new("t", &bad).unwrap();
                let detected = match Table::open(&fs, 1, "t".to_string()) {
                    Err(_) => true,
                    Ok(t) => t.iter(&fs).any(|e| e.is_err()),
                };
                assert!(detected, "flip at byte {i} bit {bit} went unnoticed");
            }
        }
    }
}
