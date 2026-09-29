//! A tiny deterministic async runtime, and the I/O boundary between client
//! code (the KV client, transactions, SQL) and whatever drives the cluster:
//! the simulator, or a real server.
//!
//! Client logic is written as ordinary sequential `async` code. The driver
//! owns an [`Executor`] and an [`Io`]: it collects requests clients sent,
//! delivers responses and advances time, and polls tasks in a fixed FIFO
//! order, so a simulation stays exactly reproducible from its seed.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use crate::kv::cmd::{KvError, ReqId, Request, Response};
use crate::kv::keys::{RangeDescriptor, RangeId};
use crate::raft::NodeId;

type Task = Pin<Box<dyn Future<Output = ()>>>;

struct TaskWaker {
    id: usize,
    queue: Arc<Mutex<VecDeque<usize>>>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.queue.lock().expect("queue").push_back(self.id);
    }
}

/// Polls spawned tasks in the order they were woken.
#[derive(Default)]
pub struct Executor {
    tasks: Vec<Option<Task>>,
    queue: Arc<Mutex<VecDeque<usize>>>,
}

impl Executor {
    pub fn spawn(&mut self, fut: impl Future<Output = ()> + 'static) {
        self.tasks.push(Some(Box::pin(fut)));
        self.queue
            .lock()
            .expect("queue")
            .push_back(self.tasks.len() - 1);
    }

    /// Poll every woken task until none is ready. Returns tasks polled.
    pub fn run_ready(&mut self) -> usize {
        let mut polled = 0;
        loop {
            let next = self.queue.lock().expect("queue").pop_front();
            let Some(id) = next else {
                return polled;
            };
            let Some(task) = self.tasks.get_mut(id).and_then(Option::as_mut) else {
                continue;
            };
            let waker = Waker::from(Arc::new(TaskWaker {
                id,
                queue: self.queue.clone(),
            }));
            polled += 1;
            if task
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
            {
                self.tasks[id] = None;
            }
        }
    }

    pub fn live_tasks(&self) -> usize {
        self.tasks.iter().filter(|t| t.is_some()).count()
    }
}

/// Where a request should go.
pub type Route = (RangeDescriptor, Option<NodeId>);

/// Looks up the route for a key.
pub type Router = Box<dyn Fn(&[u8]) -> Option<Route>>;

#[derive(Default)]
struct IoState {
    now: u64,
    next_req: ReqId,
    outbox: Vec<(ReqId, NodeId, RangeId, Request)>,
    responses: BTreeMap<ReqId, Result<Response, KvError>>,
    wakers: BTreeMap<ReqId, Waker>,
    timers: Vec<(u64, Waker)>,
    rng: u64,
}

/// The shared I/O state: requests out, responses in, the clock, and the
/// routing table. Clients hold an `Rc<Io>`; the driver holds another.
pub struct Io {
    state: RefCell<IoState>,
    router: Router,
}

impl Io {
    pub fn new(seed: u64, router: impl Fn(&[u8]) -> Option<Route> + 'static) -> Rc<Io> {
        Rc::new(Io {
            state: RefCell::new(IoState {
                rng: seed | 1,
                ..IoState::default()
            }),
            router: Box::new(router),
        })
    }

    pub fn now(&self) -> u64 {
        self.state.borrow().now
    }

    /// A deterministic pseudo-random number, for jitter and choices.
    pub fn rand(&self, n: u64) -> u64 {
        let mut s = self.state.borrow_mut();
        s.rng ^= s.rng << 13;
        s.rng ^= s.rng >> 7;
        s.rng ^= s.rng << 17;
        s.rng % n.max(1)
    }

    pub fn route(&self, key: &[u8]) -> Option<Route> {
        (self.router)(key)
    }

    /// Queue a request for the driver to deliver.
    pub fn send(&self, node: NodeId, range: RangeId, req: Request) -> ReqId {
        let mut s = self.state.borrow_mut();
        s.next_req += 1;
        let id = s.next_req;
        s.outbox.push((id, node, range, req));
        id
    }

    /// Wait for a response, or give up after `timeout_ms` (`None`).
    pub async fn wait(&self, id: ReqId, timeout_ms: u64) -> Option<Result<Response, KvError>> {
        let deadline = self.now() + timeout_ms;
        WaitResponse {
            io: self,
            id,
            deadline,
            timer_set: false,
        }
        .await
    }

    pub async fn sleep(&self, ms: u64) {
        let until = self.now() + ms;
        Sleep {
            io: self,
            until,
            timer_set: false,
        }
        .await
    }

    // ─── Driver side ──────────────────────────────────────────────────────

    pub fn take_outbox(&self) -> Vec<(ReqId, NodeId, RangeId, Request)> {
        std::mem::take(&mut self.state.borrow_mut().outbox)
    }

    pub fn deliver(&self, id: ReqId, result: Result<Response, KvError>) {
        let mut s = self.state.borrow_mut();
        if let Some(w) = s.wakers.remove(&id) {
            s.responses.insert(id, result);
            w.wake();
        }
    }

    /// Advance the clock, waking every timer that is due.
    pub fn set_now(&self, now: u64) {
        let mut s = self.state.borrow_mut();
        s.now = s.now.max(now);
        let now = s.now;
        let (due, later): (Vec<_>, Vec<_>) = s.timers.drain(..).partition(|(t, _)| *t <= now);
        s.timers = later;
        drop(s);
        for (_, w) in due {
            w.wake();
        }
    }

    /// The earliest pending timer, so the driver knows when to wake it.
    pub fn next_timer(&self) -> Option<u64> {
        self.state.borrow().timers.iter().map(|(t, _)| *t).min()
    }
}

// A future registers its timer once. Re-registering on every poll would let
// stale timers wake the task, register more timers, and multiply without
// bound: the simulator caught exactly that, as millions of spurious wakeups.
struct WaitResponse<'a> {
    io: &'a Io,
    id: ReqId,
    deadline: u64,
    timer_set: bool,
}

impl Future for WaitResponse<'_> {
    type Output = Option<Result<Response, KvError>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let io = self.io;
        let mut s = io.state.borrow_mut();
        if let Some(r) = s.responses.remove(&self.id) {
            s.wakers.remove(&self.id);
            return Poll::Ready(Some(r));
        }
        if s.now >= self.deadline {
            s.wakers.remove(&self.id);
            return Poll::Ready(None);
        }
        s.wakers.insert(self.id, cx.waker().clone());
        if !self.timer_set {
            s.timers.push((self.deadline, cx.waker().clone()));
            drop(s);
            self.timer_set = true;
        }
        Poll::Pending
    }
}

struct Sleep<'a> {
    io: &'a Io,
    until: u64,
    timer_set: bool,
}

impl Future for Sleep<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let io = self.io;
        let mut s = io.state.borrow_mut();
        if s.now >= self.until {
            return Poll::Ready(());
        }
        if !self.timer_set {
            s.timers.push((self.until, cx.waker().clone()));
            drop(s);
            self.timer_set = true;
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tasks_wait_for_responses_and_timers() {
        let io = Io::new(1, |_| None);
        let mut ex = Executor::default();
        let log = Rc::new(RefCell::new(Vec::new()));
        {
            let (io, log) = (io.clone(), log.clone());
            ex.spawn(async move {
                let id = io.send(1, 1, Request::Get { key: b"k".to_vec() });
                let r = io.wait(id, 100).await;
                log.borrow_mut().push(format!("got {r:?}"));
                io.sleep(50).await;
                log.borrow_mut().push(format!("woke at {}", io.now()));
                let id = io.send(1, 1, Request::Get { key: b"k".to_vec() });
                let r = io.wait(id, 10).await;
                log.borrow_mut().push(format!("timeout {}", r.is_none()));
            });
        }
        ex.run_ready();
        let out = io.take_outbox();
        assert_eq!(out.len(), 1);
        io.deliver(out[0].0, Ok(Response::Done));
        ex.run_ready();
        io.set_now(60);
        ex.run_ready();
        io.set_now(100);
        ex.run_ready();
        assert_eq!(
            *log.borrow(),
            vec!["got Some(Ok(Done))", "woke at 60", "timeout true"]
        );
        assert_eq!(ex.live_tasks(), 0);
    }
}
