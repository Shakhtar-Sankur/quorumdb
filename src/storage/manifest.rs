//! The manifest names the live write-ahead log and tables, and the level of
//! each table. It is replaced atomically: written to a temporary file,
//! synced, renamed over the old one, and the directory synced. After a crash
//! either the old or the new manifest is in place, never a mixture.
//!
//! File: `[magic "QDBMAN02"][next_id][wal_id][last_seq][n]`
//! `n x [level: u32][table id: u64]` `[crc32: u32]`

use crate::codec::{Reader, put_u32, put_u64};
use crate::crc::crc32;
use crate::error::{Error, Result};

const MAGIC: &[u8; 8] = b"QDBMAN02";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub next_id: u64,
    pub wal_id: u64,
    pub last_seq: u64,
    /// `(level, table id)`. Level 0 tables are listed oldest first.
    pub tables: Vec<(u32, u64)>,
}

pub fn encode(m: &Manifest) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    put_u64(&mut out, m.next_id);
    put_u64(&mut out, m.wal_id);
    put_u64(&mut out, m.last_seq);
    put_u64(&mut out, m.tables.len() as u64);
    for &(level, id) in &m.tables {
        put_u32(&mut out, level);
        put_u64(&mut out, id);
    }
    let crc = crc32(&out);
    put_u32(&mut out, crc);
    out
}

pub fn decode(data: &[u8]) -> Result<Manifest> {
    let corrupt = |why: &str| Error::Corrupt(format!("manifest: {why}"));
    if data.len() < MAGIC.len() + 4 || &data[..8] != MAGIC {
        return Err(corrupt("bad header"));
    }
    let (body, tail) = data.split_at(data.len() - 4);
    if crc32(body) != u32::from_le_bytes(tail.try_into().expect("4 bytes")) {
        return Err(corrupt("checksum mismatch"));
    }
    let mut r = Reader::new(&body[8..]);
    let short = || corrupt("short");
    let (next_id, wal_id, last_seq) = (
        r.u64().ok_or_else(short)?,
        r.u64().ok_or_else(short)?,
        r.u64().ok_or_else(short)?,
    );
    let n = r.u64().ok_or_else(short)?;
    let tables = (0..n)
        .map(|_| r.u32().zip(r.u64()).ok_or_else(short))
        .collect::<Result<Vec<_>>>()?;
    if !r.is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(Manifest {
        next_id,
        wal_id,
        last_seq,
        tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let m = Manifest {
            next_id: 9,
            wal_id: 8,
            last_seq: 42,
            tables: vec![(0, 3), (0, 5), (2, 7)],
        };
        assert_eq!(decode(&encode(&m)).unwrap(), m);
    }

    #[test]
    fn detects_corruption() {
        let mut bytes = encode(&Manifest {
            next_id: 2,
            wal_id: 1,
            last_seq: 0,
            tables: vec![],
        });
        bytes[10] ^= 1;
        assert!(decode(&bytes).is_err());
    }
}
