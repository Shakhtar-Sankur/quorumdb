//! SQL values and types, and how rows and keys are encoded in the store.
//!
//! A table's rows live under `t <table id: u32 BE> <encoded primary key>`.
//! Primary keys use an order-preserving encoding, so a range of keys in SQL
//! is a range of keys in the store, and a primary-key range scan touches
//! only the ranges that hold it.

use std::cmp::Ordering;
use std::fmt;

use crate::codec::{Reader, put_bytes, put_u64};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    Int,
    Float,
    Text,
    Bool,
}

impl DataType {
    pub fn name(self) -> &'static str {
        match self {
            DataType::Int => "BIGINT",
            DataType::Float => "DOUBLE PRECISION",
            DataType::Text => "TEXT",
            DataType::Bool => "BOOLEAN",
        }
    }

    /// The Postgres type OID, for the wire protocol.
    pub fn oid(self) -> u32 {
        match self {
            DataType::Int => 20,
            DataType::Float => 701,
            DataType::Text => 25,
            DataType::Bool => 16,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Int(_) => Some(DataType::Int),
            Value::Float(_) => Some(DataType::Float),
            Value::Text(_) => Some(DataType::Text),
            Value::Bool(_) => Some(DataType::Bool),
        }
    }

    /// Coerce to a column's type, or explain why not.
    pub fn coerce(self, to: DataType) -> Result<Value, String> {
        Ok(match (self, to) {
            (Value::Null, _) => Value::Null,
            (Value::Int(i), DataType::Float) => Value::Float(i as f64),
            (Value::Float(f), DataType::Int) if f.fract() == 0.0 => Value::Int(f as i64),
            (Value::Text(s), DataType::Int) => Value::Int(
                s.trim()
                    .parse()
                    .map_err(|_| format!("invalid integer: {s:?}"))?,
            ),
            (Value::Text(s), DataType::Float) => Value::Float(
                s.trim()
                    .parse()
                    .map_err(|_| format!("invalid number: {s:?}"))?,
            ),
            (Value::Text(s), DataType::Bool) => match s.to_ascii_lowercase().as_str() {
                "t" | "true" | "yes" | "on" | "1" => Value::Bool(true),
                "f" | "false" | "no" | "off" | "0" => Value::Bool(false),
                _ => return Err(format!("invalid boolean: {s:?}")),
            },
            (v, t) if v.data_type() == Some(t) => v,
            (v, t) => return Err(format!("cannot store {v} in a {} column", t.name())),
        })
    }

    /// SQL comparison; `None` when either side is NULL.
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// A total order for sorting and grouping: by type, NULLs last (as in
    /// PostgreSQL's ascending order).
    pub fn sort_cmp(&self, other: &Value) -> Ordering {
        fn rank(v: &Value) -> u8 {
            match v {
                Value::Bool(_) => 1,
                Value::Int(_) | Value::Float(_) => 2,
                Value::Text(_) => 3,
                Value::Null => 4,
            }
        }
        self.compare(other)
            .unwrap_or_else(|| rank(self).cmp(&rank(other)))
    }

    /// The text form sent to clients.
    pub fn to_text(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Int(i) => Some(i.to_string()),
            Value::Float(f) => Some(if f.fract() == 0.0 && f.abs() < 1e15 {
                format!("{f:.0}")
            } else {
                f.to_string()
            }),
            Value::Text(s) => Some(s.clone()),
            Value::Bool(b) => Some(if *b { "t" } else { "f" }.to_string()),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        self.sort_cmp(other) == Ordering::Equal
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Text(s) => write!(f, "'{s}'"),
            v => write!(f, "{}", v.to_text().unwrap_or_default()),
        }
    }
}

// ─── Encodings ────────────────────────────────────────────────────────────

/// Encode a primary-key value so byte order matches value order.
pub fn encode_key(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.push(0),
        Value::Bool(b) => {
            out.push(1);
            out.push(*b as u8);
        }
        Value::Int(i) => {
            out.push(2);
            out.extend_from_slice(&((*i as u64) ^ (1 << 63)).to_be_bytes());
        }
        Value::Float(f) => {
            out.push(4);
            let bits = f.to_bits();
            let ordered = if bits >> 63 == 1 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            out.extend_from_slice(&ordered.to_be_bytes());
        }
        Value::Text(s) => {
            out.push(3);
            for &b in s.as_bytes() {
                if b == 0 {
                    out.extend_from_slice(&[0, 0xFF]);
                } else {
                    out.push(b);
                }
            }
            out.extend_from_slice(&[0, 1]);
        }
    }
}

pub fn encode_row(row: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in row {
        match v {
            Value::Null => out.push(0),
            Value::Int(i) => {
                out.push(1);
                put_u64(&mut out, *i as u64);
            }
            Value::Float(f) => {
                out.push(2);
                put_u64(&mut out, f.to_bits());
            }
            Value::Text(s) => {
                out.push(3);
                put_bytes(&mut out, s.as_bytes());
            }
            Value::Bool(b) => {
                out.push(4);
                out.push(*b as u8);
            }
        }
    }
    out
}

pub fn decode_row(data: &[u8]) -> Option<Vec<Value>> {
    let mut r = Reader::new(data);
    let mut row = Vec::new();
    while !r.is_empty() {
        row.push(match r.u8()? {
            0 => Value::Null,
            1 => Value::Int(r.u64()? as i64),
            2 => Value::Float(f64::from_bits(r.u64()?)),
            3 => Value::Text(String::from_utf8(r.bytes()?).ok()?),
            4 => Value::Bool(r.u8()? != 0),
            _ => return None,
        });
    }
    Some(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_encoding_preserves_order() {
        let vals = [
            Value::Int(i64::MIN),
            Value::Int(-5),
            Value::Int(0),
            Value::Int(7),
            Value::Int(i64::MAX),
        ];
        let enc: Vec<Vec<u8>> = vals
            .iter()
            .map(|v| {
                let mut o = Vec::new();
                encode_key(v, &mut o);
                o
            })
            .collect();
        assert!(enc.windows(2).all(|w| w[0] < w[1]));
        let texts = ["", "a", "a\0", "ab", "b"];
        let enc: Vec<Vec<u8>> = texts
            .iter()
            .map(|t| {
                let mut o = Vec::new();
                encode_key(&Value::Text(t.to_string()), &mut o);
                o
            })
            .collect();
        assert!(enc.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn rows_round_trip() {
        let row = vec![
            Value::Int(-3),
            Value::Null,
            Value::Text("héllo".into()),
            Value::Float(2.5),
            Value::Bool(true),
        ];
        assert_eq!(decode_row(&encode_row(&row)), Some(row));
    }
}
