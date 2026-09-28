//! The engine's only view of storage. `RealFs` is the operating system;
//! `SimFs` is a simulated disk that crashes the way real hardware does, so
//! the simulator can test every recovery path deterministically.
//!
//! The durability contract the engine is written against, and that `SimFs`
//! enforces on a crash:
//! - Bytes written to a file survive a crash only after `sync(file)`.
//!   Unsynced appended bytes may survive in part, and the last surviving
//!   byte may be torn (corrupted).
//! - Creating, renaming and removing names survive only after `sync_dir()`.
//!   Until then a crash can undo them.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::rc::Rc;

use crate::rng::Rng;

/// A flat directory of files, addressed by name.
pub trait Fs {
    /// The whole file, or `None` if it does not exist.
    fn read(&self, name: &str) -> io::Result<Option<Vec<u8>>>;
    /// Create or truncate `name` and write `data`. Not durable until `sync`.
    fn write_new(&self, name: &str, data: &[u8]) -> io::Result<()>;
    /// Append to `name`, creating it if needed. Not durable until `sync`.
    fn append(&self, name: &str, data: &[u8]) -> io::Result<()>;
    /// Make the current contents of `name` durable.
    fn sync(&self, name: &str) -> io::Result<()>;
    fn truncate(&self, name: &str, len: u64) -> io::Result<()>;
    /// Atomically replace `to` with `from`. Not durable until `sync_dir`.
    fn rename(&self, from: &str, to: &str) -> io::Result<()>;
    /// Remove `name` if it exists. Not durable until `sync_dir`.
    fn remove(&self, name: &str) -> io::Result<()>;
    fn list(&self) -> io::Result<Vec<String>>;
    /// Make every create, rename and remove so far durable.
    fn sync_dir(&self) -> io::Result<()>;
}

fn not_found(name: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, name.to_string())
}

// ─── Real disk ───────────────────────────────────────────────────────────

pub struct RealFs {
    dir: PathBuf,
    handles: RefCell<HashMap<String, File>>,
}

impl RealFs {
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(RealFs {
            dir,
            handles: RefCell::new(HashMap::new()),
        })
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

impl Fs for RealFs {
    fn read(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
        match fs::read(self.path(name)) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn write_new(&self, name: &str, data: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.path(name))?;
        file.write_all(data)?;
        self.handles.borrow_mut().insert(name.to_string(), file);
        Ok(())
    }

    fn append(&self, name: &str, data: &[u8]) -> io::Result<()> {
        let mut handles = self.handles.borrow_mut();
        if !handles.contains_key(name) {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.path(name))?;
            handles.insert(name.to_string(), file);
        }
        handles
            .get_mut(name)
            .expect("inserted above")
            .write_all(data)
    }

    fn sync(&self, name: &str) -> io::Result<()> {
        match self.handles.borrow().get(name) {
            Some(file) => file.sync_data(),
            None => File::open(self.path(name))?.sync_data(),
        }
    }

    fn truncate(&self, name: &str, len: u64) -> io::Result<()> {
        self.handles.borrow_mut().remove(name);
        OpenOptions::new()
            .write(true)
            .open(self.path(name))?
            .set_len(len)
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut handles = self.handles.borrow_mut();
        handles.remove(from);
        handles.remove(to);
        fs::rename(self.path(from), self.path(to))
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        self.handles.borrow_mut().remove(name);
        match fs::remove_file(self.path(name)) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }

    fn list(&self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            if let Some(name) = entry?.file_name().to_str() {
                names.push(name.to_string());
            }
        }
        names.sort();
        Ok(names)
    }

    fn sync_dir(&self) -> io::Result<()> {
        File::open(&self.dir)?.sync_all()
    }
}

// ─── Simulated disk ──────────────────────────────────────────────────────

#[derive(Default)]
struct Inode {
    current: Vec<u8>,
    durable: Vec<u8>,
}

struct SimState {
    next_ino: u64,
    inodes: HashMap<u64, Inode>,
    names: BTreeMap<String, u64>,
    durable_names: BTreeMap<String, u64>,
    rng: Rng,
    crashes: u64,
    torn_writes: u64,
}

/// A deterministic simulated disk. Cloning shares the same disk, so a test
/// can hold one handle, give another to the engine, and crash it at will.
#[derive(Clone)]
pub struct SimFs(Rc<RefCell<SimState>>);

impl SimFs {
    pub fn new(seed: u64) -> Self {
        SimFs(Rc::new(RefCell::new(SimState {
            next_ino: 1,
            inodes: HashMap::new(),
            names: BTreeMap::new(),
            durable_names: BTreeMap::new(),
            rng: Rng::new(seed),
            crashes: 0,
            torn_writes: 0,
        })))
    }

    /// Power loss. Every name reverts to its last `sync_dir`, every file to
    /// its last `sync`, except that unsynced appended bytes may survive in
    /// part, and the last surviving byte may be torn.
    pub fn crash(&self) {
        let mut st = self.0.borrow_mut();
        let st = &mut *st;
        st.crashes += 1;
        st.names = st.durable_names.clone();
        let live: HashSet<u64> = st.names.values().copied().collect();
        st.inodes.retain(|ino, _| live.contains(ino));

        let mut inos: Vec<u64> = live.into_iter().collect();
        inos.sort_unstable();
        for ino in inos {
            let inode = st.inodes.get_mut(&ino).expect("live inode");
            let survived = if inode.current == inode.durable {
                inode.current.clone()
            } else if inode.current.len() > inode.durable.len()
                && inode.current.starts_with(&inode.durable)
            {
                let extra = inode.current.len() - inode.durable.len();
                let keep = st.rng.below(extra as u64 + 1) as usize;
                let mut bytes = inode.current[..inode.durable.len() + keep].to_vec();
                if keep > 0 && st.rng.chance(25) {
                    let last = bytes.len() - 1;
                    bytes[last] ^= 1 << st.rng.below(8);
                    st.torn_writes += 1;
                }
                bytes
            } else if st.rng.chance(50) {
                // An unsynced overwrite or truncate: it either reached the disk or it did not.
                inode.current.clone()
            } else {
                inode.durable.clone()
            };
            inode.durable = survived.clone();
            inode.current = survived;
        }
    }

    pub fn crashes(&self) -> u64 {
        self.0.borrow().crashes
    }

    pub fn torn_writes(&self) -> u64 {
        self.0.borrow().torn_writes
    }

    fn with_inode<T>(&self, name: &str, f: impl FnOnce(&mut Inode) -> T) -> io::Result<T> {
        let mut st = self.0.borrow_mut();
        let ino = *st.names.get(name).ok_or_else(|| not_found(name))?;
        Ok(f(st.inodes.get_mut(&ino).expect("named inode")))
    }

    fn create(&self, name: &str) -> u64 {
        let mut st = self.0.borrow_mut();
        if let Some(&ino) = st.names.get(name) {
            return ino;
        }
        let ino = st.next_ino;
        st.next_ino += 1;
        st.inodes.insert(ino, Inode::default());
        st.names.insert(name.to_string(), ino);
        ino
    }
}

impl Fs for SimFs {
    fn read(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
        let st = self.0.borrow();
        Ok(st.names.get(name).map(|ino| st.inodes[ino].current.clone()))
    }

    fn write_new(&self, name: &str, data: &[u8]) -> io::Result<()> {
        self.create(name);
        self.with_inode(name, |inode| inode.current = data.to_vec())
    }

    fn append(&self, name: &str, data: &[u8]) -> io::Result<()> {
        self.create(name);
        self.with_inode(name, |inode| inode.current.extend_from_slice(data))
    }

    fn sync(&self, name: &str) -> io::Result<()> {
        self.with_inode(name, |inode| inode.durable = inode.current.clone())
    }

    fn truncate(&self, name: &str, len: u64) -> io::Result<()> {
        self.with_inode(name, |inode| inode.current.truncate(len as usize))
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut st = self.0.borrow_mut();
        let ino = st.names.remove(from).ok_or_else(|| not_found(from))?;
        st.names.insert(to.to_string(), ino);
        Ok(())
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        self.0.borrow_mut().names.remove(name);
        Ok(())
    }

    fn list(&self) -> io::Result<Vec<String>> {
        Ok(self.0.borrow().names.keys().cloned().collect())
    }

    fn sync_dir(&self) -> io::Result<()> {
        let mut st = self.0.borrow_mut();
        st.durable_names = st.names.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synced_data_and_names_survive_a_crash() {
        let fs = SimFs::new(1);
        fs.append("a", b"hello").unwrap();
        fs.sync("a").unwrap();
        fs.sync_dir().unwrap();
        fs.crash();
        assert_eq!(fs.read("a").unwrap().as_deref(), Some(&b"hello"[..]));
    }

    #[test]
    fn a_name_that_was_never_dir_synced_disappears() {
        let fs = SimFs::new(1);
        fs.append("a", b"hello").unwrap();
        fs.sync("a").unwrap();
        fs.crash();
        assert_eq!(fs.read("a").unwrap(), None);
    }

    #[test]
    fn unsynced_appends_survive_only_as_a_prefix() {
        for seed in 0..200 {
            let fs = SimFs::new(seed);
            fs.append("a", b"durable").unwrap();
            fs.sync("a").unwrap();
            fs.sync_dir().unwrap();
            fs.append("a", b"-maybe").unwrap();
            fs.crash();
            let got = fs.read("a").unwrap().unwrap();
            assert!(
                got.starts_with(b"durable"),
                "seed {seed}: synced bytes lost"
            );
            assert!(got.len() <= b"durable-maybe".len());
        }
    }
}
