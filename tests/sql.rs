//! SQL end to end: statements run through sessions on an in-process
//! three-node cluster (simulated disks, virtual time), through the same
//! transactions, Raft groups and storage engines as the server.

use std::cell::RefCell;
use std::rc::Rc;

use quorumdb::kv::client::KvClient;
use quorumdb::kv::store::StoreConfig;
use quorumdb::server::cluster::LocalCluster;
use quorumdb::sql::exec::{QueryResult, Session};
use quorumdb::storage::fs::SimFs;
use quorumdb::txn::client::TxnOptions;

fn cluster(split_keys: u64) -> LocalCluster<SimFs> {
    let fs = (0..3).map(SimFs::new).collect();
    LocalCluster::open(fs, StoreConfig::default(), split_keys).unwrap()
}

type Shared = Rc<RefCell<Option<Session>>>;

fn session(c: &LocalCluster<SimFs>) -> Shared {
    Rc::new(RefCell::new(Some(Session::new(
        KvClient::new(c.io.clone()),
        TxnOptions::default(),
    ))))
}

/// Run SQL and return each statement's result (or error text).
fn sql(c: &mut LocalCluster<SimFs>, s: &Shared, text: &str) -> Vec<Result<QueryResult, String>> {
    let s = s.clone();
    let text = text.to_string();
    c.block_on(async move {
        let mut session = s.borrow_mut().take().expect("session in use");
        let out = session
            .execute(&text)
            .await
            .into_iter()
            .map(|r| r.map_err(|e| format!("{}: {}", e.code(), e.message())))
            .collect();
        *s.borrow_mut() = Some(session);
        out
    })
    .unwrap()
}

fn rows(c: &mut LocalCluster<SimFs>, s: &Shared, text: &str) -> Vec<Vec<String>> {
    let r = sql(c, s, text)
        .pop()
        .unwrap()
        .unwrap_or_else(|e| panic!("{text}: {e}"));
    r.rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| v.to_text().unwrap_or_else(|| "NULL".into()))
                .collect()
        })
        .collect()
}

#[test]
fn queries_joins_aggregates_and_plans() {
    let mut c = cluster(10_000);
    let s = session(&c);
    for r in sql(
        &mut c,
        &s,
        "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, city TEXT);
         CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total FLOAT);
         INSERT INTO customers VALUES (1, 'Asha', 'Kolkata'), (2, 'Ben', 'London'), (3, 'Chen', 'Kolkata'), (4, 'Dara', NULL);
         INSERT INTO orders VALUES (10, 1, 250.5), (11, 1, 99.5), (12, 2, 40), (13, 3, 510.25), (14, 3, 12);",
    ) {
        r.unwrap();
    }
    assert_eq!(
        rows(
            &mut c,
            &s,
            "SELECT c.name, COUNT(*), SUM(o.total) AS spent FROM customers c JOIN orders o ON o.customer_id = c.id \
             GROUP BY c.name ORDER BY spent DESC"
        ),
        vec![
            vec!["Chen", "2", "522.25"],
            vec!["Asha", "2", "350"],
            vec!["Ben", "1", "40"]
        ]
    );
    assert_eq!(
        rows(
            &mut c,
            &s,
            "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id >= 2 AND c.id <= 4 ORDER BY c.name, o.id"
        ),
        vec![
            vec!["Ben", "12"],
            vec!["Chen", "13"],
            vec!["Chen", "14"],
            vec!["Dara", "NULL"]
        ]
    );
    let plan = rows(
        &mut c,
        &s,
        "EXPLAIN SELECT name FROM customers WHERE id = 3",
    );
    assert!(
        plan[0][0].starts_with("Get customers by primary key"),
        "{plan:?}"
    );
    let plan = rows(
        &mut c,
        &s,
        "EXPLAIN SELECT * FROM orders WHERE id > 10 AND id <= 13",
    );
    assert!(plan[0][0].starts_with("Range scan orders"), "{plan:?}");
    assert_eq!(
        rows(
            &mut c,
            &s,
            "SELECT COUNT(*) FROM orders WHERE total > 50 AND customer_id <> 2"
        ),
        vec![vec!["3"]]
    );
    assert_eq!(
        rows(
            &mut c,
            &s,
            "SELECT name FROM customers WHERE name LIKE '%a' ORDER BY name"
        ),
        vec![vec!["Asha"], vec!["Dara"]]
    );
    let dup = sql(&mut c, &s, "INSERT INTO customers VALUES (1, 'X', 'Y')")
        .pop()
        .unwrap();
    assert!(dup.unwrap_err().starts_with("23505"));
    let missing = sql(&mut c, &s, "SELECT * FROM nope").pop().unwrap();
    assert!(missing.unwrap_err().starts_with("42P01"));
}

#[test]
fn transactions_commit_roll_back_and_fail_cleanly() {
    let mut c = cluster(10_000);
    let s = session(&c);
    sql(&mut c, &s, "CREATE TABLE t (k INT PRIMARY KEY, v TEXT)");
    sql(&mut c, &s, "BEGIN; INSERT INTO t VALUES (1, 'a'); ROLLBACK");
    assert!(rows(&mut c, &s, "SELECT * FROM t").is_empty());
    sql(
        &mut c,
        &s,
        "BEGIN; INSERT INTO t VALUES (1, 'a'), (2, 'b'); UPDATE t SET v = v || '!' WHERE k = 2; COMMIT",
    );
    assert_eq!(
        rows(&mut c, &s, "SELECT v FROM t ORDER BY k"),
        vec![vec!["a"], vec!["b!"]]
    );
    // After an error, the transaction refuses work until it ends.
    let r = sql(&mut c, &s, "BEGIN; INSERT INTO t VALUES (1, 'dup')");
    assert!(r.last().unwrap().is_err());
    let r = sql(&mut c, &s, "SELECT 1").pop().unwrap();
    assert!(r.unwrap_err().starts_with("25P02"));
    sql(&mut c, &s, "COMMIT");
    assert_eq!(rows(&mut c, &s, "SELECT COUNT(*) FROM t"), vec![vec!["2"]]);
}

/// The classic write skew: two doctors on call, each goes off call after
/// checking that the other is still on. Snapshot isolation lets both
/// commit, leaving nobody on call; serializable must refuse one.
#[test]
fn serializable_prevents_write_skew() {
    let mut c = cluster(10_000);
    let (a, b) = (session(&c), session(&c));
    sql(
        &mut c,
        &a,
        "CREATE TABLE doctors (name TEXT PRIMARY KEY, on_call BOOLEAN); INSERT INTO doctors VALUES ('alice', true), ('bob', true)",
    );
    sql(&mut c, &a, "BEGIN");
    sql(&mut c, &b, "BEGIN");
    assert_eq!(
        rows(&mut c, &a, "SELECT COUNT(*) FROM doctors WHERE on_call"),
        vec![vec!["2"]]
    );
    assert_eq!(
        rows(&mut c, &b, "SELECT COUNT(*) FROM doctors WHERE on_call"),
        vec![vec!["2"]]
    );
    sql(
        &mut c,
        &a,
        "UPDATE doctors SET on_call = false WHERE name = 'alice'",
    );
    sql(
        &mut c,
        &b,
        "UPDATE doctors SET on_call = false WHERE name = 'bob'",
    );
    let first = sql(&mut c, &a, "COMMIT").pop().unwrap();
    let second = sql(&mut c, &b, "COMMIT").pop().unwrap();
    assert!(first.is_ok(), "{first:?}");
    assert!(
        second.as_ref().unwrap_err().starts_with("40001"),
        "{second:?}"
    );
    assert_eq!(
        rows(&mut c, &a, "SELECT COUNT(*) FROM doctors WHERE on_call"),
        vec![vec!["1"]]
    );
}

/// Many sessions move money between accounts at once, with retries on
/// conflict. However they interleave, and however the table splits across
/// ranges, no money is created or destroyed.
#[test]
fn concurrent_bank_transfers_conserve_money() {
    let mut c = cluster(8); // tiny ranges: the table splits while in use
    let setup = session(&c);
    let mut stmt = String::from(
        "CREATE TABLE accounts (id INT PRIMARY KEY, balance BIGINT NOT NULL); INSERT INTO accounts VALUES ",
    );
    stmt.push_str(
        &(0..40)
            .map(|i| format!("({i}, 100)"))
            .collect::<Vec<_>>()
            .join(", "),
    );
    for r in sql(&mut c, &setup, &stmt) {
        r.unwrap();
    }
    // Let the placement driver split the table across ranges and nodes.
    let t0 = c.now();
    for t in t0..t0 + 3_000 {
        c.step(t).unwrap();
    }
    assert!(c.ranges() > 1, "the table never split across ranges");
    let done = Rc::new(RefCell::new(0));
    for worker in 0..8u64 {
        let mut s = Session::new(KvClient::new(c.io.clone()), TxnOptions::default());
        let done = done.clone();
        c.exec.spawn(async move {
            let mut seed = worker * 7919 + 1;
            let mut next = || {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed
            };
            for _ in 0..15 {
                let (from, to, amount) = (next() % 40, next() % 40, next() % 30);
                if from == to {
                    continue;
                }
                let txn = format!(
                    "BEGIN; UPDATE accounts SET balance = balance - {amount} WHERE id = {from}; \
                     UPDATE accounts SET balance = balance + {amount} WHERE id = {to}; COMMIT"
                );
                for _attempt in 0..20 {
                    let r = s.execute(&txn).await;
                    if r.iter().all(|x| x.is_ok()) {
                        break;
                    }
                    s.execute("ROLLBACK").await;
                }
            }
            *done.borrow_mut() += 1;
        });
    }
    let mut t = c.now();
    while *done.borrow() < 8 {
        t += 1;
        c.step(t).unwrap();
        assert!(t < 3_600_000, "workers did not finish");
    }
    let total = rows(
        &mut c,
        &setup,
        "SELECT SUM(balance), COUNT(*) FROM accounts",
    );
    assert_eq!(
        total,
        vec![vec!["4000", "40"]],
        "money was created or destroyed"
    );
}
