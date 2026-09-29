//! A linearizability checker for a history of reads and writes on one
//! register (one key), after Wing & Gong with Lowe's memoization, the
//! algorithm behind Knossos and Porcupine.
//!
//! A history is linearizable if every operation can be placed at a single
//! instant between its invocation and its response, such that the result
//! is a valid sequential execution: each read returns the latest write.
//! Operations whose outcome is unknown (the client timed out) have their
//! response at infinity: they may take effect at any point after they were
//! invoked, or never.
//!
//! Keys are independent (linearizability is compositional), so a cluster
//! history is checked one key at a time.

use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpKind {
    /// Write a value (`None` is a delete).
    Write(Option<u64>),
    /// A read that returned this value.
    Read(Option<u64>),
}

#[derive(Clone, Debug)]
pub struct Operation {
    pub kind: OpKind,
    pub call: u64,
    /// `None`: the outcome is unknown.
    pub ret: Option<u64>,
    /// For error messages: which client issued it.
    pub client: u64,
}

#[derive(Clone, Copy)]
struct Event {
    op: usize,
    is_call: bool,
    prev: usize,
    next: usize,
    /// For a call event, the index of its matching return event.
    matching: usize,
}

const HEAD: usize = usize::MAX - 1;
const NIL: usize = usize::MAX;

/// Check one register's history. `Err` describes why it is not linearizable.
pub fn check(ops: &[Operation]) -> Result<(), String> {
    // Reads with an unknown outcome observed nothing: drop them.
    let ops: Vec<&Operation> = ops
        .iter()
        .filter(|o| o.ret.is_some() || matches!(o.kind, OpKind::Write(_)))
        .collect();
    if ops.is_empty() {
        return Ok(());
    }
    // Events sorted by time; at equal times calls come before returns, so
    // touching operations count as concurrent.
    let mut order: Vec<(u64, bool, usize)> = Vec::with_capacity(ops.len() * 2);
    for (i, o) in ops.iter().enumerate() {
        order.push((o.call, true, i));
        order.push((o.ret.unwrap_or(u64::MAX), false, i));
    }
    order.sort_by_key(|&(t, is_call, i)| (t, !is_call, i));
    let mut events: Vec<Event> = order
        .iter()
        .map(|&(_, is_call, op)| Event {
            op,
            is_call,
            prev: NIL,
            next: NIL,
            matching: NIL,
        })
        .collect();
    let n = events.len();
    let mut ret_of = vec![NIL; ops.len()];
    for (i, e) in events.iter().enumerate() {
        if !e.is_call {
            ret_of[e.op] = i;
        }
    }
    for i in 0..n {
        events[i].prev = if i == 0 { HEAD } else { i - 1 };
        events[i].next = if i + 1 == n { NIL } else { i + 1 };
        if events[i].is_call {
            events[i].matching = ret_of[events[i].op];
        }
    }
    let mut head = 0usize;

    let words = ops.len().div_ceil(64);
    let mut linearized = vec![0u64; words];
    let mut cache: HashSet<(Vec<u64>, Option<u64>)> = HashSet::new();
    let mut state: Option<u64> = None;
    // Stack of (call event, state before it).
    let mut stack: Vec<(usize, Option<u64>)> = Vec::new();
    let mut entry = head;
    let mut steps: u64 = 0;

    // Unlink the call event `c` and its return from the list.
    fn lift(events: &mut [Event], head: &mut usize, c: usize) {
        for idx in [c, events[c].matching] {
            let (p, nx) = (events[idx].prev, events[idx].next);
            if p == HEAD {
                *head = nx;
            } else {
                events[p].next = nx;
            }
            if nx != NIL {
                events[nx].prev = p;
            }
        }
    }
    fn unlift(events: &mut [Event], head: &mut usize, c: usize) {
        for idx in [events[c].matching, c] {
            let (p, nx) = (events[idx].prev, events[idx].next);
            if p == HEAD {
                *head = idx;
            } else {
                events[p].next = idx;
            }
            if nx != NIL {
                events[nx].prev = idx;
            }
        }
    }

    loop {
        steps += 1;
        if steps > 50_000_000 {
            return Err("linearizability search exceeded its budget".to_string());
        }
        if head == NIL {
            return Ok(());
        }
        if entry == NIL {
            return Err(explain(&ops));
        }
        let e = events[entry];
        if e.is_call {
            let op = ops[e.op];
            let (ok, next_state) = match op.kind {
                OpKind::Write(v) => (true, v),
                OpKind::Read(v) => (v == state, state),
            };
            if ok {
                let mut bits = linearized.clone();
                bits[e.op / 64] |= 1 << (e.op % 64);
                if cache.insert((bits.clone(), next_state)) {
                    stack.push((entry, state));
                    linearized = bits;
                    state = next_state;
                    lift(&mut events, &mut head, entry);
                    entry = head;
                    continue;
                }
            }
            entry = e.next;
        } else {
            // A return event before its call was linearized: backtrack.
            let Some((c, prev_state)) = stack.pop() else {
                return Err(explain(&ops));
            };
            let op = events[c].op;
            linearized[op / 64] &= !(1 << (op % 64));
            state = prev_state;
            unlift(&mut events, &mut head, c);
            entry = events[c].next;
        }
    }
}

fn explain(ops: &[&Operation]) -> String {
    let mut sorted: Vec<&&Operation> = ops.iter().collect();
    sorted.sort_by_key(|o| o.call);
    let lines: Vec<String> = sorted
        .iter()
        .take(40)
        .map(|o| {
            let ret = o.ret.map_or("?".to_string(), |r| r.to_string());
            format!("    client {} [{}, {}] {:?}", o.client, o.call, ret, o.kind)
        })
        .collect();
    format!(
        "history is not linearizable ({} operations; first 40 by invocation):\n{}",
        ops.len(),
        lines.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(v: u64, call: u64, ret: u64) -> Operation {
        Operation {
            kind: OpKind::Write(Some(v)),
            call,
            ret: Some(ret),
            client: 0,
        }
    }

    fn r(v: Option<u64>, call: u64, ret: u64) -> Operation {
        Operation {
            kind: OpKind::Read(v),
            call,
            ret: Some(ret),
            client: 1,
        }
    }

    #[test]
    fn sequential_history_is_linearizable() {
        assert!(check(&[w(1, 0, 1), r(Some(1), 2, 3), w(2, 4, 5), r(Some(2), 6, 7)]).is_ok());
    }

    #[test]
    fn concurrent_read_may_see_either_value() {
        assert!(check(&[w(1, 0, 1), w(2, 2, 10), r(Some(1), 3, 4), r(Some(2), 5, 6)]).is_ok());
    }

    #[test]
    fn a_stale_read_is_caught() {
        // The write of 2 finished before the read began, yet it saw 1.
        assert!(check(&[w(1, 0, 1), w(2, 2, 3), r(Some(1), 4, 5)]).is_err());
    }

    #[test]
    fn values_cannot_flip_back() {
        // Two sequential reads see 2 then 1 while both writes overlap them:
        // impossible, since 1 would have to be written after 2.
        let h = [
            w(1, 0, 100),
            w(2, 0, 100),
            r(Some(2), 10, 20),
            r(Some(1), 30, 40),
            r(Some(2), 50, 60),
        ];
        assert!(check(&h).is_err());
    }

    #[test]
    fn a_write_with_unknown_outcome_may_apply_late_or_never() {
        let lost = Operation {
            kind: OpKind::Write(Some(9)),
            call: 5,
            ret: None,
            client: 2,
        };
        assert!(check(&[w(1, 0, 1), lost.clone(), r(Some(1), 10, 11)]).is_ok());
        assert!(check(&[w(1, 0, 1), lost, r(Some(9), 10, 11), r(Some(9), 12, 13)]).is_ok());
        // But it cannot take effect before it was invoked.
        let late = Operation {
            kind: OpKind::Write(Some(7)),
            call: 50,
            ret: None,
            client: 2,
        };
        assert!(check(&[w(1, 0, 1), r(Some(7), 10, 11), late]).is_err());
    }
}
