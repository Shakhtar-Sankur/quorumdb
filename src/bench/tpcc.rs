//! A TPC-C-derived benchmark: the order-processing workload every
//! distributed SQL database publishes numbers for, run through quorumdb's
//! own SQL layer, as serializable transactions, by concurrent terminals.
//!
//! It follows the TPC-C schema and its five transactions (New-Order,
//! Payment, Order-Status, Delivery, Stock-Level) in the standard mix and
//! terminal-to-district binding, with honest differences:
//! - the item table and the initial order history are scaled down
//!   (10,000 items, not 100,000; 10 initial orders per district, not
//!   3,000); customers are at full scale (3,000 per district);
//! - customers are always chosen by id with a uniform distribution (no
//!   last-name lookups, no NURand skew), and every order line and payment
//!   stays in the terminal's home warehouse;
//! - composite keys are flattened into one integer, since quorumdb has
//!   single-column primary keys;
//! - each hot, frequently-updated column set (warehouse and district
//!   year-to-date totals, the district's next order id, a customer's
//!   balance) lives in its own table, as CockroachDB's TPC-C does with
//!   column families, so updating a total does not invalidate a
//!   concurrent transaction that only read a tax rate from the same row;
//! - terminals run without keying and think times.
//!
//! It is not an audited TPC-C result. The metric is tpmC: New-Order
//! transactions committed per minute.
//!
//! After the run it checks TPC-C's consistency conditions, which only hold
//! if every transaction was atomic and isolated.

use std::cell::RefCell;
use std::rc::Rc;

use crate::rng::Rng;
use crate::runtime::join_all;
use crate::sql::exec::{ExecError, Session};
use crate::sql::types::Value;

pub const DISTRICTS: u64 = 10;
pub const CUSTOMERS: u64 = 3_000;
pub const ITEMS: u64 = 10_000;
pub const INITIAL_ORDERS: u64 = 10;

pub fn district(w: u64, d: u64) -> u64 {
    w * 100 + d
}
pub fn customer(w: u64, d: u64, c: u64) -> u64 {
    district(w, d) * 10_000 + c
}
pub fn stock(w: u64, i: u64) -> u64 {
    w * 1_000_000 + i
}
pub fn order(dk: u64, o: u64) -> u64 {
    dk * 10_000_000 + o
}
pub fn order_line(ok: u64, n: u64) -> u64 {
    ok * 20 + n
}

pub const SCHEMA: &str = "
CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_tax FLOAT);
CREATE TABLE warehouse_ytd (w_id INT PRIMARY KEY, w_ytd FLOAT);
CREATE TABLE district (d_key INT PRIMARY KEY, d_tax FLOAT);
CREATE TABLE district_ytd (d_key INT PRIMARY KEY, d_ytd FLOAT);
CREATE TABLE district_next (d_key INT PRIMARY KEY, d_next_o_id INT);
CREATE TABLE customer (c_key INT PRIMARY KEY, c_last TEXT, c_discount FLOAT);
CREATE TABLE customer_balance (c_key INT PRIMARY KEY, c_balance FLOAT, c_ytd_payment FLOAT,
                               c_payment_cnt INT, c_delivery_cnt INT);
CREATE TABLE item (i_id INT PRIMARY KEY, i_name TEXT, i_price FLOAT);
CREATE TABLE stock (s_key INT PRIMARY KEY, s_quantity INT, s_ytd INT, s_order_cnt INT);
CREATE TABLE orders (o_key INT PRIMARY KEY, o_c_key INT, o_ol_cnt INT, o_carrier_id INT);
CREATE TABLE new_order (no_key INT PRIMARY KEY);
CREATE TABLE order_line (ol_key INT PRIMARY KEY, ol_i_id INT, ol_s_key INT, ol_quantity INT,
                         ol_amount FLOAT, ol_delivered BOOLEAN);
CREATE TABLE history (h_c_key INT, h_amount FLOAT);
";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
}

pub const KINDS: [Kind; 5] = [
    Kind::NewOrder,
    Kind::Payment,
    Kind::OrderStatus,
    Kind::Delivery,
    Kind::StockLevel,
];

/// What one terminal measured.
#[derive(Default, Clone, Debug)]
pub struct Stats {
    /// Per transaction kind: latencies (ms) of committed transactions.
    pub latencies: [Vec<u64>; 5],
    /// Per transaction kind: serialization conflicts retried.
    pub retries: [u64; 5],
    /// New-Orders rolled back on purpose (TPC-C's 1% invalid item).
    pub rollbacks: u64,
    pub errors: u64,
    /// Transactions abandoned after losing every retry to conflicts.
    pub gave_up: u64,
}

impl Stats {
    pub fn merge(&mut self, o: &Stats) {
        for (a, b) in self.latencies.iter_mut().zip(&o.latencies) {
            a.extend_from_slice(b);
        }
        for (a, b) in self.retries.iter_mut().zip(&o.retries) {
            *a += b;
        }
        self.rollbacks += o.rollbacks;
        self.errors += o.errors;
        self.gave_up += o.gave_up;
    }
}

enum Outcome {
    Commit,
    /// The transaction chose to roll back (not an error).
    Rollback,
}

async fn q(s: &mut Session, sql: &str) -> Result<Vec<Vec<Value>>, ExecError> {
    let mut results = s.execute(sql).await;
    match results.pop() {
        Some(Ok(r)) => Ok(r.rows),
        Some(Err(e)) => Err(e),
        None => Ok(Vec::new()),
    }
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i,
        Value::Float(f) => *f as i64,
        _ => 0,
    }
}

fn float(v: &Value) -> f64 {
    match v {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        _ => 0.0,
    }
}

/// Load the initial database for `warehouses` warehouses.
/// Create the schema and load the initial database, with one worker per
/// session running insert batches in parallel.
pub async fn load(mut sessions: Vec<Session>, warehouses: u64) -> Result<(), ExecError> {
    let s = sessions.first_mut().expect("at least one session");
    for r in s.execute(SCHEMA).await {
        r?;
    }
    let mut rng = Rng::new(42);
    let chunked = |rows: Vec<String>, table: &str| -> Vec<String> {
        rows.chunks(100)
            .map(|c| format!("INSERT INTO {table} VALUES {}", c.join(", ")))
            .collect()
    };
    let mut stmts = Vec::new();
    stmts.extend(chunked(
        (1..=ITEMS)
            .map(|i| format!("({i}, 'item-{i}', {})", 1 + rng.below(100)))
            .collect(),
        "item",
    ));
    for w in 1..=warehouses {
        stmts.push(format!("INSERT INTO warehouse VALUES ({w}, 0.1)"));
        stmts.push(format!(
            "INSERT INTO warehouse_ytd VALUES ({w}, {})",
            30_000 * DISTRICTS
        ));
        stmts.extend(chunked(
            (1..=ITEMS)
                .map(|i| format!("({}, {}, 0, 0)", stock(w, i), 10 + rng.below(91)))
                .collect(),
            "stock",
        ));
        for d in 1..=DISTRICTS {
            let dk = district(w, d);
            stmts.push(format!("INSERT INTO district VALUES ({dk}, 0.05)"));
            stmts.push(format!("INSERT INTO district_ytd VALUES ({dk}, 30000)"));
            stmts.push(format!(
                "INSERT INTO district_next VALUES ({dk}, {})",
                INITIAL_ORDERS + 1
            ));
            stmts.extend(chunked(
                (1..=CUSTOMERS)
                    .map(|c| format!("({}, 'cust-{c}', 0.1)", customer(w, d, c)))
                    .collect(),
                "customer",
            ));
            stmts.extend(chunked(
                (1..=CUSTOMERS)
                    .map(|c| format!("({}, -10, 10, 1, 0)", customer(w, d, c)))
                    .collect(),
                "customer_balance",
            ));
            let (mut orders, mut lines, mut new_orders) = (Vec::new(), Vec::new(), Vec::new());
            for o in 1..=INITIAL_ORDERS {
                let ok = order(dk, o);
                let delivered = o <= INITIAL_ORDERS - 3;
                orders.push(format!(
                    "({ok}, {}, 5, {})",
                    customer(w, d, 1 + rng.below(CUSTOMERS)),
                    if delivered { "1" } else { "NULL" }
                ));
                if !delivered {
                    new_orders.push(format!("({ok})"));
                }
                for n in 1..=5 {
                    let i = 1 + rng.below(ITEMS);
                    lines.push(format!(
                        "({}, {i}, {}, 5, {}, {delivered})",
                        order_line(ok, n),
                        stock(w, i),
                        if delivered {
                            0.0
                        } else {
                            (1 + rng.below(9_999)) as f64 / 100.0
                        }
                    ));
                }
            }
            stmts.extend(chunked(orders, "orders"));
            stmts.extend(chunked(lines, "order_line"));
            stmts.extend(chunked(new_orders, "new_order"));
        }
    }
    let queue = Rc::new(RefCell::new(stmts.into_iter()));
    let workers = sessions.into_iter().map(|mut s| {
        let queue = queue.clone();
        async move {
            loop {
                let Some(stmt) = queue.borrow_mut().next() else {
                    return Ok(());
                };
                q(&mut s, &stmt).await?;
            }
        }
    });
    join_all(workers.collect()).await.into_iter().collect()
}

/// A terminal's home: TPC-C binds each terminal to one warehouse and
/// district, so New-Orders spread across districts.
#[derive(Clone, Copy)]
pub struct Home {
    pub w: u64,
    pub d: u64,
    pub warehouses: u64,
}

async fn new_order(s: &mut Session, rng: &mut Rng, home: Home) -> Result<Outcome, ExecError> {
    let w = home.w;
    let dk = district(w, home.d);
    let ck = dk * 10_000 + 1 + rng.below(CUSTOMERS);
    let lines = 5 + rng.below(11);
    let invalid = rng.chance(1); // TPC-C: 1% of New-Orders name an unknown item
    q(s, "BEGIN").await?;
    let tax = float(&q(s, &format!("SELECT w_tax FROM warehouse WHERE w_id = {w}")).await?[0][0]);
    let d_tax =
        float(&q(s, &format!("SELECT d_tax FROM district WHERE d_key = {dk}")).await?[0][0]);
    let next = int(&q(
        s,
        &format!("SELECT d_next_o_id FROM district_next WHERE d_key = {dk}"),
    )
    .await?[0][0]) as u64;
    q(
        s,
        &format!(
            "UPDATE district_next SET d_next_o_id = {} WHERE d_key = {dk}",
            next + 1
        ),
    )
    .await?;
    let discount = float(
        &q(
            s,
            &format!("SELECT c_discount FROM customer WHERE c_key = {ck}"),
        )
        .await?[0][0],
    );
    let ok = order(dk, next);
    q(
        s,
        &format!("INSERT INTO orders VALUES ({ok}, {ck}, {lines}, NULL)"),
    )
    .await?;
    q(s, &format!("INSERT INTO new_order VALUES ({ok})")).await?;
    let mut total = 0.0;
    for n in 1..=lines {
        let i = if invalid && n == lines {
            ITEMS + 1
        } else {
            1 + rng.below(ITEMS)
        };
        let item = q(s, &format!("SELECT i_price FROM item WHERE i_id = {i}")).await?;
        let Some(row) = item.first() else {
            q(s, "ROLLBACK").await?;
            return Ok(Outcome::Rollback);
        };
        let price = float(&row[0]);
        let sk = stock(w, i);
        let qty = int(&q(
            s,
            &format!("SELECT s_quantity FROM stock WHERE s_key = {sk}"),
        )
        .await?[0][0]);
        let amount = 1 + rng.below(10) as i64;
        let new_qty = if qty - amount >= 10 {
            qty - amount
        } else {
            qty - amount + 91
        };
        q(
            s,
            &format!(
                "UPDATE stock SET s_quantity = {new_qty}, s_ytd = s_ytd + {amount}, s_order_cnt = s_order_cnt + 1 WHERE s_key = {sk}"
            ),
        )
        .await?;
        let line_amount = amount as f64 * price;
        total += line_amount;
        q(
            s,
            &format!(
                "INSERT INTO order_line VALUES ({}, {i}, {sk}, {amount}, {line_amount}, false)",
                order_line(ok, n)
            ),
        )
        .await?;
    }
    let _ = (
        total * (1.0 - discount) * (1.0 + tax + d_tax),
        home.warehouses,
    );
    q(s, "COMMIT").await?;
    Ok(Outcome::Commit)
}

async fn payment(s: &mut Session, rng: &mut Rng, home: Home) -> Result<Outcome, ExecError> {
    let w = home.w;
    let dk = district(w, 1 + rng.below(DISTRICTS));
    let ck = dk * 10_000 + 1 + rng.below(CUSTOMERS);
    let amount = (100 + rng.below(499_901)) as f64 / 100.0;
    q(s, "BEGIN").await?;
    q(
        s,
        &format!("UPDATE warehouse_ytd SET w_ytd = w_ytd + {amount} WHERE w_id = {w}"),
    )
    .await?;
    q(
        s,
        &format!("UPDATE district_ytd SET d_ytd = d_ytd + {amount} WHERE d_key = {dk}"),
    )
    .await?;
    q(
        s,
        &format!(
            "UPDATE customer_balance SET c_balance = c_balance - {amount}, c_ytd_payment = c_ytd_payment + {amount}, \
             c_payment_cnt = c_payment_cnt + 1 WHERE c_key = {ck}"
        ),
    )
    .await?;
    q(s, &format!("INSERT INTO history VALUES ({ck}, {amount})")).await?;
    q(s, "COMMIT").await?;
    Ok(Outcome::Commit)
}

async fn order_status(s: &mut Session, rng: &mut Rng, home: Home) -> Result<Outcome, ExecError> {
    let w = home.w;
    let dk = district(w, 1 + rng.below(DISTRICTS));
    let ck = dk * 10_000 + 1 + rng.below(CUSTOMERS);
    q(s, "BEGIN").await?;
    q(
        s,
        &format!("SELECT c_last FROM customer WHERE c_key = {ck}"),
    )
    .await?;
    q(
        s,
        &format!("SELECT c_balance FROM customer_balance WHERE c_key = {ck}"),
    )
    .await?;
    let last = q(
        s,
        &format!(
            "SELECT o_key, o_carrier_id FROM orders WHERE o_key >= {} AND o_key < {} AND o_c_key = {ck} \
             ORDER BY o_key DESC LIMIT 1",
            order(dk, 0),
            order(dk + 1, 0)
        ),
    )
    .await?;
    if let Some(row) = last.first() {
        let ok = int(&row[0]) as u64;
        q(
            s,
            &format!(
                "SELECT ol_i_id, ol_quantity, ol_amount FROM order_line WHERE ol_key >= {} AND ol_key < {}",
                order_line(ok, 0),
                order_line(ok + 1, 0)
            ),
        )
        .await?;
    }
    q(s, "COMMIT").await?;
    Ok(Outcome::Commit)
}

async fn delivery(s: &mut Session, rng: &mut Rng, home: Home) -> Result<Outcome, ExecError> {
    let w = home.w;
    let carrier = 1 + rng.below(10);
    q(s, "BEGIN").await?;
    for d in 1..=DISTRICTS {
        let dk = district(w, d);
        let oldest = q(
            s,
            &format!(
                "SELECT no_key FROM new_order WHERE no_key >= {} AND no_key < {} ORDER BY no_key LIMIT 1",
                order(dk, 0),
                order(dk + 1, 0)
            ),
        )
        .await?;
        let Some(row) = oldest.first() else { continue };
        let ok = int(&row[0]) as u64;
        q(s, &format!("DELETE FROM new_order WHERE no_key = {ok}")).await?;
        q(
            s,
            &format!("UPDATE orders SET o_carrier_id = {carrier} WHERE o_key = {ok}"),
        )
        .await?;
        let ck = int(&q(s, &format!("SELECT o_c_key FROM orders WHERE o_key = {ok}")).await?[0][0]);
        let (lo, hi) = (order_line(ok, 0), order_line(ok + 1, 0));
        let sum = q(
            s,
            &format!(
                "SELECT SUM(ol_amount) FROM order_line WHERE ol_key >= {lo} AND ol_key < {hi}"
            ),
        )
        .await?;
        let sum = sum.first().map_or(0.0, |r| float(&r[0]));
        q(
            s,
            &format!(
                "UPDATE order_line SET ol_delivered = true WHERE ol_key >= {lo} AND ol_key < {hi}"
            ),
        )
        .await?;
        q(
            s,
            &format!(
                "UPDATE customer_balance SET c_balance = c_balance + {sum}, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_key = {ck}"
            ),
        )
        .await?;
    }
    q(s, "COMMIT").await?;
    Ok(Outcome::Commit)
}

async fn stock_level(s: &mut Session, rng: &mut Rng, home: Home) -> Result<Outcome, ExecError> {
    let w = home.w;
    let dk = district(w, home.d);
    let threshold = 10 + rng.below(11);
    q(s, "BEGIN").await?;
    let next = int(&q(
        s,
        &format!("SELECT d_next_o_id FROM district_next WHERE d_key = {dk}"),
    )
    .await?[0][0]) as u64;
    let first = next.saturating_sub(20).max(1);
    q(
        s,
        &format!(
            "SELECT COUNT(*) FROM order_line ol JOIN stock st ON st.s_key = ol.ol_s_key \
             WHERE ol.ol_key >= {} AND ol.ol_key < {} AND st.s_quantity < {threshold}",
            order_line(order(dk, first), 0),
            order_line(order(dk, next), 0)
        ),
    )
    .await?;
    q(s, "COMMIT").await?;
    Ok(Outcome::Commit)
}

/// Pick a transaction by the standard TPC-C mix.
fn pick(rng: &mut Rng) -> Kind {
    match rng.below(100) {
        0..45 => Kind::NewOrder,
        45..88 => Kind::Payment,
        88..92 => Kind::OrderStatus,
        92..96 => Kind::Delivery,
        _ => Kind::StockLevel,
    }
}

/// One terminal: run transactions until `until` (ms on the session's
/// clock), retrying serialization conflicts, and record what happened.
pub async fn terminal(
    mut s: Session,
    io: std::rc::Rc<crate::runtime::Io>,
    until: u64,
    home: Home,
    seed: u64,
    stats: Rc<RefCell<Stats>>,
) {
    let now = || io.now();
    let mut rng = Rng::new(seed);
    while now() < until {
        let kind = pick(&mut rng);
        let started = now();
        let tx_rng = Rng::new(rng.next_u64());
        for attempt in 0..=30u64 {
            if attempt == 30 {
                stats.borrow_mut().gave_up += 1;
                break;
            }
            let mut attempt_rng = tx_rng.clone();
            let r = match kind {
                Kind::NewOrder => new_order(&mut s, &mut attempt_rng, home).await,
                Kind::Payment => payment(&mut s, &mut attempt_rng, home).await,
                Kind::OrderStatus => order_status(&mut s, &mut attempt_rng, home).await,
                Kind::Delivery => delivery(&mut s, &mut attempt_rng, home).await,
                Kind::StockLevel => stock_level(&mut s, &mut attempt_rng, home).await,
            };
            match r {
                Ok(Outcome::Commit) => {
                    stats.borrow_mut().latencies[kind as usize].push(now() - started);
                    break;
                }
                Ok(Outcome::Rollback) => {
                    stats.borrow_mut().rollbacks += 1;
                    break;
                }
                Err(ExecError::Retry(why)) => {
                    if std::env::var("TPCC_DEBUG").is_ok() {
                        eprintln!("{kind:?} retry: {why}");
                    }
                    let _ = s.execute("ROLLBACK").await;
                    stats.borrow_mut().retries[kind as usize] += 1;
                    // Exponential backoff with jitter, so conflicting
                    // terminals stop colliding in lockstep.
                    let cap = 2u64 << attempt.min(6);
                    io.sleep(1 + rng.below(cap)).await;
                }
                Err(e) => {
                    if std::env::var("TPCC_DEBUG").is_ok() {
                        eprintln!("{kind:?} error: {} {}", e.code(), e.message());
                    }
                    let _ = s.execute("ROLLBACK").await;
                    stats.borrow_mut().errors += 1;
                    break;
                }
            }
        }
    }
}

/// TPC-C's consistency conditions 1 and 2, checked after the run.
pub async fn check_consistency(s: &mut Session, warehouses: u64) -> Result<Vec<String>, ExecError> {
    let mut problems = Vec::new();
    for w in 1..=warehouses {
        let w_ytd = float(
            &q(
                s,
                &format!("SELECT w_ytd FROM warehouse_ytd WHERE w_id = {w}"),
            )
            .await?[0][0],
        );
        let d_ytd = float(
            &q(
                s,
                &format!(
                    "SELECT SUM(d_ytd) FROM district_ytd WHERE d_key > {} AND d_key <= {}",
                    district(w, 0),
                    district(w, DISTRICTS)
                ),
            )
            .await?[0][0],
        );
        if (w_ytd - d_ytd).abs() > 0.01 {
            problems.push(format!(
                "warehouse {w}: W_YTD {w_ytd:.2} != sum(D_YTD) {d_ytd:.2}"
            ));
        }
        for d in 1..=DISTRICTS {
            let dk = district(w, d);
            let next = int(&q(
                s,
                &format!("SELECT d_next_o_id FROM district_next WHERE d_key = {dk}"),
            )
            .await?[0][0]);
            let max_o = q(
                s,
                &format!(
                    "SELECT MAX(o_key) FROM orders WHERE o_key >= {} AND o_key < {}",
                    order(dk, 0),
                    order(dk + 1, 0)
                ),
            )
            .await?;
            let max_o = int(&max_o[0][0]) - order(dk, 0) as i64;
            if next - 1 != max_o {
                problems.push(format!(
                    "district {dk}: D_NEXT_O_ID - 1 = {} but max(O_ID) = {max_o}",
                    next - 1
                ));
            }
        }
    }
    Ok(problems)
}
