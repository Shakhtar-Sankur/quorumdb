//! Sorted string tables: immutable, sorted runs of keys written when the
//! memtable flushes. A tombstone (`None`) records a deletion so it can hide
//! older values in older tables until compaction drops it.
//!
//! File: `[magic "QDBSST01"][count: u64][entries][crc32(everything before): u32]`
//! Entry: `[kind: u8][key][value if put]`, byte strings length-prefixed.
//!
//! A table is synced before any manifest names it, so a table that fails
//! its checksum is real corruption, never an expected crash artefact.

use crate::codec::{Reader, put_bytes, put_u32, put_u64};
use crate::crc::crc32;
use crate::error::{Error, Result};

const MAGIC: &[u8; 8] = b"QDBSST01";

/// A key and its value, or `None` for a tombstone.
pub type Entry = (Vec<u8>, Option<Vec<u8>>);

pub fn encode(entries: &[Entry]) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    put_u64(&mut out, entries.len() as u64);
    for (key, value) in entries {
        match value {
            Some(v) => {
                out.push(1);
                put_bytes(&mut out, key);
                put_bytes(&mut out, v);
            }
            None => {
                out.push(0);
                put_bytes(&mut out, key);
            }
        }
    }
    let crc = crc32(&out);
    put_u32(&mut out, crc);
    out
}

pub fn decode(data: &[u8]) -> Result<Vec<Entry>> {
    let corrupt = |why: &str| Error::Corrupt(format!("sstable: {why}"));
    if data.len() < MAGIC.len() + 12 || &data[..8] != MAGIC {
        return Err(corrupt("bad header"));
    }
    let (body, tail) = data.split_at(data.len() - 4);
    if crc32(body) != u32::from_le_bytes(tail.try_into().expect("4 bytes")) {
        return Err(corrupt("checksum mismatch"));
    }
    let mut r = Reader::new(&body[8..]);
    let count = r.u64().ok_or_else(|| corrupt("short"))?;
    let mut entries = Vec::with_capacity(count.min(1 << 20) as usize);
    for _ in 0..count {
        let entry = match r.u8() {
            Some(1) => r.bytes().zip(r.bytes()).map(|(k, v)| (k, Some(v))),
            Some(0) => r.bytes().map(|k| (k, None)),
            _ => None,
        };
        entries.push(entry.ok_or_else(|| corrupt("truncated entry"))?);
    }
    if !r.is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    if entries.windows(2).any(|w| w[0].0 >= w[1].0) {
        return Err(corrupt("keys out of order"));
    }
    Ok(entries)
}

/// A loaded table. Milestone 1 keeps tables in memory; block indexes and
/// reads from disk come with the next storage milestone.
pub struct Table {
    pub id: u64,
    entries: Vec<Entry>,
}

impl Table {
    pub fn new(id: u64, entries: Vec<Entry>) -> Self {
        Table { id, entries }
    }

    /// `Some(None)` is a tombstone; `None` means this table knows nothing about `key`.
    pub fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .ok()
            .map(|i| &self.entries[i].1)
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<Entry> {
        vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), None),
            (b"c".to_vec(), Some(Vec::new())),
        ]
    }

    #[test]
    fn round_trip_and_lookup() {
        let entries = decode(&encode(&sample())).unwrap();
        assert_eq!(entries, sample());
        let t = Table::new(1, entries);
        assert_eq!(t.get(b"a"), Some(&Some(b"1".to_vec())));
        assert_eq!(t.get(b"b"), Some(&None));
        assert_eq!(t.get(b"z"), None);
    }

    #[test]
    fn any_single_bit_flip_is_detected() {
        let bytes = encode(&sample());
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut bad = bytes.clone();
                bad[i] ^= 1 << bit;
                assert!(
                    decode(&bad).is_err(),
                    "flip at byte {i} bit {bit} went unnoticed"
                );
            }
        }
    }
}
