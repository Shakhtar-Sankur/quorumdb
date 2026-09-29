//! A streaming k-way merge of sorted sources. Scans and compactions both use
//! it, so neither ever holds more than one block per table in memory.
//!
//! Sources are given newest first. When several hold the same key, the
//! newest wins and the rest are skipped. Tombstones are passed through: only
//! the caller knows whether a deletion is still needed to hide older data.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::error::Result;
use crate::storage::sstable::Entry;

pub type Source<'a> = Box<dyn Iterator<Item = Result<Entry>> + 'a>;

/// A source's next entry as `(key, source index, value)`, reversed so the
/// max-heap yields the smallest key, and for equal keys the newest source.
type Head = Reverse<(Vec<u8>, usize, Option<Vec<u8>>)>;

pub struct MergeIter<'a> {
    sources: Vec<Source<'a>>,
    heads: BinaryHeap<Head>,
    pending_error: Option<crate::error::Error>,
    done: bool,
}

impl<'a> MergeIter<'a> {
    pub fn new(sources: Vec<Source<'a>>) -> Self {
        let mut merge = MergeIter {
            sources,
            heads: BinaryHeap::new(),
            pending_error: None,
            done: false,
        };
        for i in 0..merge.sources.len() {
            merge.advance(i);
        }
        merge
    }

    fn advance(&mut self, i: usize) {
        match self.sources[i].next() {
            Some(Ok((k, v))) => self.heads.push(Reverse((k, i, v))),
            Some(Err(e)) => {
                self.pending_error.get_or_insert(e);
            }
            None => {}
        }
    }
}

impl Iterator for MergeIter<'_> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(e) = self.pending_error.take() {
            self.done = true;
            return Some(Err(e));
        }
        let Reverse((key, i, value)) = self.heads.pop()?;
        self.advance(i);
        while let Some(Reverse((k, _, _))) = self.heads.peek() {
            if *k != key {
                break;
            }
            let Reverse((_, j, _)) = self.heads.pop().expect("peeked");
            self.advance(j);
        }
        if let Some(e) = self.pending_error.take() {
            self.done = true;
            return Some(Err(e));
        }
        Some(Ok((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(entries: &[(&str, Option<&str>)]) -> Source<'static> {
        let owned: Vec<Result<Entry>> = entries
            .iter()
            .map(|(k, v)| Ok((k.as_bytes().to_vec(), v.map(|v| v.as_bytes().to_vec()))))
            .collect();
        Box::new(owned.into_iter())
    }

    #[test]
    fn newest_source_wins_and_order_is_kept() {
        let newest = source(&[("b", None), ("d", Some("d2"))]);
        let older = source(&[("a", Some("a1")), ("b", Some("b1")), ("d", Some("d1"))]);
        let oldest = source(&[("b", Some("b0")), ("c", Some("c0")), ("e", Some("e0"))]);
        let merged: Vec<Entry> = MergeIter::new(vec![newest, older, oldest])
            .map(|e| e.unwrap())
            .collect();
        let expect: Vec<Entry> = [
            ("a", Some("a1")),
            ("b", None),
            ("c", Some("c0")),
            ("d", Some("d2")),
            ("e", Some("e0")),
        ]
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.map(|v| v.as_bytes().to_vec())))
        .collect();
        assert_eq!(merged, expect);
    }
}
