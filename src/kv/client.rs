//! The client side of the key-value layer: find a key's range, send the
//! request to its leader, follow redirects, refresh stale routes, and retry
//! what is safe to retry.

use std::rc::Rc;

use crate::kv::cmd::{KvError, Request, Response};
use crate::runtime::Io;

/// How long one attempt may take before the client tries again.
const ATTEMPT_MS: u64 = 400;

#[derive(Clone)]
pub struct KvClient {
    pub io: Rc<Io>,
    /// Give up on a request after this long.
    pub deadline_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallError {
    /// A definite answer from the store: a conflict, a lock, an abort.
    Kv(KvError),
    /// The request may or may not have taken effect.
    Unknown,
}

impl KvClient {
    pub fn new(io: Rc<Io>) -> KvClient {
        KvClient {
            io,
            deadline_ms: 5_000,
        }
    }

    /// Send `req` to the leader of the range owning its key, retrying
    /// through leader changes, splits and lost messages until the deadline.
    /// Requests that are not idempotent are never re-sent after an attempt
    /// whose outcome is unknown.
    pub async fn call(&self, req: Request) -> Result<Response, CallError> {
        let deadline = self.io.now() + self.deadline_ms;
        let mut hint = None;
        let mut sent_once = false;
        loop {
            if self.io.now() >= deadline {
                return Err(if sent_once && !req.is_idempotent() {
                    CallError::Unknown
                } else {
                    CallError::Kv(KvError::Busy)
                });
            }
            let Some((desc, leader)) = self.io.route(req.routing_key()) else {
                self.io.sleep(20).await;
                continue;
            };
            let nodes = desc.nodes();
            let node = hint
                .filter(|n| nodes.contains(n))
                .or(leader)
                .unwrap_or_else(|| nodes[self.io.rand(nodes.len() as u64) as usize]);
            let id = self.io.send(node, desc.id, req.clone());
            match self.io.wait(id, ATTEMPT_MS).await {
                None => {
                    if !req.is_idempotent() {
                        return Err(CallError::Unknown);
                    }
                    self.io.learn_leader(desc.id, None);
                    sent_once = true;
                    hint = None;
                }
                Some(Ok(resp)) => {
                    self.io.learn_leader(desc.id, Some(node));
                    return Ok(resp);
                }
                Some(Err(KvError::NotLeader(h))) => {
                    self.io.learn_leader(desc.id, h);
                    hint = h;
                    // A known new leader: go straight there. No leader yet
                    // (an election): give it a moment.
                    if h.is_none() {
                        self.io.sleep(2 + self.io.rand(10)).await;
                    }
                }
                Some(Err(
                    KvError::KeyNotInRange
                    | KvError::RangeNotFound
                    | KvError::Busy
                    | KvError::Dropped,
                )) => {
                    hint = None;
                    self.io.sleep(5 + self.io.rand(25)).await;
                }
                Some(Err(KvError::Ambiguous)) => {
                    if !req.is_idempotent() {
                        return Err(CallError::Unknown);
                    }
                    hint = None;
                }
                Some(Err(e)) => return Err(CallError::Kv(e)),
            }
        }
    }
}
