//! Write-ahead log. Every mutation is appended here before it is
//! acknowledged, so the memtable can always be rebuilt after a crash.
//!
//! Record: `[len: u32][crc32(payload): u32][payload]`
//! Payload: `[seq: u64][kind: u8][key][value if put]`, byte strings length-prefixed.
//!
//! Decoding stops at the first record that is short, oversized, or fails its
//! checksum: that is the torn tail of the last write before a crash.

use crate::codec::{Reader, put_bytes, put_u32, put_u64};
use crate::crc::crc32;

const MAX_RECORD: usize = 64 << 20;
const KIND_DELETE: u8 = 0;
const KIND_PUT: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub op: Op,
}

pub fn encode(rec: &Record) -> Vec<u8> {
    let mut payload = Vec::new();
    put_u64(&mut payload, rec.seq);
    match &rec.op {
        Op::Put(k, v) => {
            payload.push(KIND_PUT);
            put_bytes(&mut payload, k);
            put_bytes(&mut payload, v);
        }
        Op::Delete(k) => {
            payload.push(KIND_DELETE);
            put_bytes(&mut payload, k);
        }
    }
    let mut out = Vec::with_capacity(payload.len() + 8);
    put_u32(&mut out, payload.len() as u32);
    put_u32(&mut out, crc32(&payload));
    out.extend_from_slice(&payload);
    out
}

fn decode_payload(payload: &[u8]) -> Option<Record> {
    let mut r = Reader::new(payload);
    let seq = r.u64()?;
    let op = match r.u8()? {
        KIND_PUT => Op::Put(r.bytes()?, r.bytes()?),
        KIND_DELETE => Op::Delete(r.bytes()?),
        _ => return None,
    };
    r.is_empty().then_some(Record { seq, op })
}

/// Every intact record, and the length of the valid prefix they occupy.
pub fn decode_all(data: &[u8]) -> (Vec<Record>, usize) {
    let mut records = Vec::new();
    let mut valid = 0;
    let mut r = Reader::new(data);
    while let (Some(len), Some(crc)) = (r.u32(), r.u32()) {
        let len = len as usize;
        if len > MAX_RECORD {
            break;
        }
        let Some(payload) = r.take(len) else { break };
        if crc32(payload) != crc {
            break;
        }
        let Some(rec) = decode_payload(payload) else {
            break;
        };
        records.push(rec);
        valid += 8 + len;
    }
    (records, valid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<Record> {
        vec![
            Record {
                seq: 1,
                op: Op::Put(b"a".to_vec(), b"1".to_vec()),
            },
            Record {
                seq: 2,
                op: Op::Delete(b"a".to_vec()),
            },
            Record {
                seq: 3,
                op: Op::Put(b"b".to_vec(), Vec::new()),
            },
        ]
    }

    #[test]
    fn round_trip() {
        let bytes: Vec<u8> = sample().iter().flat_map(encode).collect();
        assert_eq!(decode_all(&bytes), (sample(), bytes.len()));
    }

    #[test]
    fn stops_at_a_torn_tail() {
        let mut bytes: Vec<u8> = sample().iter().flat_map(encode).collect();
        let whole = bytes.len();
        bytes.truncate(whole - 3);
        let (recs, valid) = decode_all(&bytes);
        assert_eq!(recs, sample()[..2]);
        assert_eq!(valid, whole - encode(&sample()[2]).len());
    }

    #[test]
    fn stops_at_a_flipped_bit() {
        let mut bytes: Vec<u8> = sample().iter().flat_map(encode).collect();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x10;
        assert_eq!(decode_all(&bytes).0, sample()[..2]);
    }
}
