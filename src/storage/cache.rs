//! A least-recently-used cache of decoded table blocks, shared by all the
//! tables of one database. Tables never change once written, so a block
//! cached under its table id and index stays valid until the table is
//! deleted, after which its entries simply age out.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::storage::sstable::Entry;

/// A block: its table's id and its position in that table's index.
type BlockId = (u64, usize);

pub struct BlockCache {
    capacity: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Block, its entries, the tick it was last used and its size in bytes.
    blocks: HashMap<BlockId, (Arc<Vec<Entry>>, u64, usize)>,
    /// Blocks by last use, oldest first.
    by_use: BTreeMap<u64, BlockId>,
    tick: u64,
    bytes: usize,
    hits: u64,
}

impl BlockCache {
    /// A cache holding up to `capacity` bytes of blocks (0 disables it).
    pub fn new(capacity: usize) -> Self {
        BlockCache {
            capacity,
            inner: Mutex::new(Inner::default()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn get(&self, id: BlockId) -> Option<Arc<Vec<Entry>>> {
        let mut c = self.lock();
        let c = &mut *c;
        c.tick += 1;
        let (entries, used, _) = c.blocks.get_mut(&id)?;
        c.by_use.remove(used);
        *used = c.tick;
        c.by_use.insert(c.tick, id);
        c.hits += 1;
        Some(entries.clone())
    }

    pub fn insert(&self, id: BlockId, entries: Arc<Vec<Entry>>, bytes: usize) {
        if bytes > self.capacity {
            return;
        }
        let mut c = self.lock();
        let c = &mut *c;
        c.tick += 1;
        if let Some((_, used, size)) = c.blocks.insert(id, (entries, c.tick, bytes)) {
            c.by_use.remove(&used);
            c.bytes -= size;
        }
        c.by_use.insert(c.tick, id);
        c.bytes += bytes;
        while c.bytes > self.capacity {
            let Some((_, old)) = c.by_use.pop_first() else {
                break;
            };
            if let Some((_, _, size)) = c.blocks.remove(&old) {
                c.bytes -= size;
            }
        }
    }

    /// Lookups answered from the cache.
    pub fn hits(&self) -> u64 {
        self.lock().hits
    }

    pub fn bytes(&self) -> usize {
        self.lock().bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(n: u8) -> Arc<Vec<Entry>> {
        Arc::new(vec![(vec![n], Some(vec![n]))])
    }

    #[test]
    fn evicts_least_recently_used_within_capacity() {
        let c = BlockCache::new(30);
        c.insert((1, 0), block(0), 10);
        c.insert((1, 1), block(1), 10);
        c.insert((1, 2), block(2), 10);
        assert!(c.get((1, 0)).is_some()); // now (1, 1) is the oldest
        c.insert((2, 0), block(3), 10);
        assert!(c.get((1, 1)).is_none());
        assert!(c.get((1, 0)).is_some() && c.get((1, 2)).is_some() && c.get((2, 0)).is_some());
        assert_eq!(c.bytes(), 30);
        c.insert((9, 9), block(9), 31); // larger than the whole cache
        assert!(c.get((9, 9)).is_none());
        assert_eq!(c.hits(), 4);
    }
}
