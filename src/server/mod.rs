//! The quorumdb server: a cluster of nodes in one process, each with its
//! own directory on disk, answering SQL over the PostgreSQL wire protocol,
//! so `psql` and other Postgres clients can connect.
//!
//! One thread runs everything from a single event loop: sockets are
//! non-blocking, each connection is an async session task on the same
//! deterministic runtime the simulator uses, and the cluster advances with
//! the wall clock.

pub mod cluster;
pub mod pgwire;

use std::cell::RefCell;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::kv::client::KvClient;
use crate::kv::store::StoreConfig;
use crate::storage::fs::RealFs;
use crate::txn::client::TxnOptions;
use cluster::LocalCluster;
use pgwire::Conn;

pub struct ServerConfig {
    pub dir: String,
    pub nodes: u64,
    pub addr: String,
}

pub fn serve(cfg: ServerConfig) -> Result<(), String> {
    let filesystems = (1..=cfg.nodes)
        .map(|n| RealFs::open(format!("{}/node{n}", cfg.dir)))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;
    let mut cluster = LocalCluster::open(filesystems, StoreConfig::default(), 50_000)
        .map_err(|e| e.to_string())?;
    let listener =
        TcpListener::bind(&cfg.addr).map_err(|e| format!("cannot listen on {}: {e}", cfg.addr))?;
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    eprintln!(
        "quorumdb: {} nodes in {}, listening on {} (connect with: psql -h {} -p {} -U quorum)",
        cfg.nodes,
        cfg.dir,
        cfg.addr,
        cfg.addr.split(':').next().unwrap_or("127.0.0.1"),
        cfg.addr.rsplit(':').next().unwrap_or("5432"),
    );

    let start = Instant::now();
    let mut conns: Vec<(TcpStream, Rc<RefCell<Conn>>)> = Vec::new();
    let mut buf = vec![0u8; 64 << 10];
    let mut next_pid = 1;
    loop {
        let mut active = false;
        while let Ok((stream, _)) = listener.accept() {
            active = true;
            let _ = stream.set_nonblocking(true);
            let _ = stream.set_nodelay(true);
            let conn = Rc::new(RefCell::new(Conn::default()));
            let kv = KvClient::new(cluster.io.clone());
            next_pid += 1;
            cluster.exec.spawn(pgwire::session(
                conn.clone(),
                kv,
                TxnOptions::default(),
                next_pid,
            ));
            conns.push((stream, conn));
        }
        for (stream, conn) in conns.iter_mut() {
            loop {
                match stream.read(&mut buf) {
                    Ok(0) => {
                        conn.borrow_mut().close();
                        break;
                    }
                    Ok(n) => {
                        active = true;
                        conn.borrow_mut().feed(&buf[..n]);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        conn.borrow_mut().close();
                        break;
                    }
                }
            }
        }
        let now = start.elapsed().as_millis() as u64;
        active |= cluster.step(now).map_err(|e| format!("cluster: {e}"))?;
        for (stream, conn) in conns.iter_mut() {
            let mut c = conn.borrow_mut();
            if !c.out.is_empty() {
                match stream.write(&c.out) {
                    Ok(n) => {
                        c.out.drain(..n);
                        active = true;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => c.close(),
                }
            }
        }
        conns.retain(|(_, c)| {
            let c = c.borrow();
            !(c.finished && c.out.is_empty())
        });
        if !active {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
