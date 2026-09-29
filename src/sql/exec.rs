//! Planning and executing SQL statements over distributed transactions.
//!
//! Every statement runs inside a [`Txn`]: either the session's explicit
//! transaction (`BEGIN` ... `COMMIT`), or its own, committed at the end and
//! transparently retried if it lost a serialization conflict. Tables, the
//! catalog, and rows are all ordinary keys in the sharded store, so a table
//! splits across ranges and nodes like anything else.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::codec::{Reader, put_bytes, put_u32};
use crate::kv::client::KvClient;
use crate::sql::parser::{
    self, AggFn, BinOp, ColumnDef, Expr, JoinKind, Select, SelectItem, Statement, UnaryOp,
};
use crate::sql::types::{self, DataType, Value};
use crate::txn::client::{Txn, TxnError, TxnOptions};

// ─── Catalog ──────────────────────────────────────────────────────────────

const CATALOG: &[u8] = b"c/";
const CATALOG_END: &[u8] = b"c0";
const NEXT_TABLE_ID: &[u8] = b"n/tables";
/// The hidden column that keys a table declared without a primary key.
const ROWID: &str = "rowid";

#[derive(Clone, Debug)]
pub struct TableDef {
    pub id: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Index of the primary-key column; the last column if it is the
    /// hidden `rowid`.
    pub pk: usize,
    pub hidden_rowid: bool,
}

impl TableDef {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u32(&mut out, self.id);
        put_bytes(&mut out, self.name.as_bytes());
        put_u32(&mut out, self.columns.len() as u32);
        for c in &self.columns {
            put_bytes(&mut out, c.name.as_bytes());
            out.push(match c.ty {
                DataType::Int => 0,
                DataType::Float => 1,
                DataType::Text => 2,
                DataType::Bool => 3,
            });
            out.push(c.primary_key as u8);
            out.push(c.not_null as u8);
        }
        out.push(self.hidden_rowid as u8);
        out
    }

    fn decode(data: &[u8]) -> Option<TableDef> {
        let mut r = Reader::new(data);
        let id = r.u32()?;
        let name = String::from_utf8(r.bytes()?).ok()?;
        let n = r.u32()?;
        let mut columns = Vec::new();
        for _ in 0..n {
            let name = String::from_utf8(r.bytes()?).ok()?;
            let ty = match r.u8()? {
                0 => DataType::Int,
                1 => DataType::Float,
                2 => DataType::Text,
                _ => DataType::Bool,
            };
            columns.push(ColumnDef {
                name,
                ty,
                primary_key: r.u8()? != 0,
                not_null: r.u8()? != 0,
            });
        }
        let hidden_rowid = r.u8()? != 0;
        let pk = columns.iter().position(|c| c.primary_key)?;
        Some(TableDef {
            id,
            name,
            columns,
            pk,
            hidden_rowid,
        })
    }

    fn prefix(&self) -> Vec<u8> {
        let mut k = b"t".to_vec();
        k.extend_from_slice(&self.id.to_be_bytes());
        k
    }

    fn prefix_end(&self) -> Vec<u8> {
        let mut k = b"t".to_vec();
        k.extend_from_slice(&(self.id + 1).to_be_bytes());
        k
    }

    fn row_key(&self, pk: &Value) -> Vec<u8> {
        let mut k = self.prefix();
        types::encode_key(pk, &mut k);
        k
    }

    /// Visible columns (the hidden rowid excluded).
    fn visible(&self) -> usize {
        self.columns.len() - self.hidden_rowid as usize
    }
}

// ─── Results and errors ───────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<Vec<Value>>,
    /// The command tag: `SELECT 3`, `INSERT 0 2`, `CREATE TABLE`, ...
    pub tag: String,
}

impl QueryResult {
    fn command(tag: impl Into<String>) -> QueryResult {
        QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            tag: tag.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExecError {
    /// Lost a conflict with another transaction; retrying may succeed.
    Retry(String),
    /// Anything else, reported to the client with a SQLSTATE code.
    Error(&'static str, String),
}

impl ExecError {
    pub fn code(&self) -> &'static str {
        match self {
            ExecError::Retry(_) => "40001",
            ExecError::Error(code, _) => code,
        }
    }

    pub fn message(&self) -> String {
        match self {
            ExecError::Retry(m) => {
                format!("could not serialize access due to concurrent update ({m})")
            }
            ExecError::Error(_, m) => m.clone(),
        }
    }
}

fn err(msg: impl Into<String>) -> ExecError {
    ExecError::Error("42000", msg.into())
}

fn from_txn(e: TxnError) -> ExecError {
    match e {
        TxnError::Conflict(m) => ExecError::Retry(m),
        TxnError::Aborted => ExecError::Retry("transaction aborted".into()),
        TxnError::Unknown => ExecError::Error(
            "08006",
            "commit outcome unknown: the connection to the cluster was lost".into(),
        ),
        TxnError::Other(m) => ExecError::Error("58000", m),
    }
}

type R<T> = Result<T, ExecError>;

// ─── Scope: the columns visible to expressions ────────────────────────────

#[derive(Clone, Debug)]
struct Scope {
    /// (table alias, column name, type, hidden)
    cols: Vec<(String, String, DataType, bool)>,
}

impl Scope {
    fn of(t: &TableDef, alias: &str) -> Scope {
        Scope {
            cols: t
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    (
                        alias.to_string(),
                        c.name.clone(),
                        c.ty,
                        t.hidden_rowid && i == t.pk,
                    )
                })
                .collect(),
        }
    }

    fn join(&self, other: &Scope) -> Scope {
        let mut cols = self.cols.clone();
        cols.extend(other.cols.iter().cloned());
        Scope { cols }
    }

    fn find(&self, table: Option<&str>, name: &str) -> R<usize> {
        let hits: Vec<usize> = self
            .cols
            .iter()
            .enumerate()
            .filter(|(_, (t, c, _, _))| c == name && table.is_none_or(|q| q == t))
            .map(|(i, _)| i)
            .collect();
        match hits.as_slice() {
            [i] => Ok(*i),
            [] => Err(ExecError::Error(
                "42703",
                match table {
                    Some(t) => format!("column \"{t}.{name}\" does not exist"),
                    None => format!("column \"{name}\" does not exist"),
                },
            )),
            _ => Err(ExecError::Error(
                "42702",
                format!("column reference \"{name}\" is ambiguous"),
            )),
        }
    }
}

// ─── Expressions ──────────────────────────────────────────────────────────

fn truthy(v: &Value) -> bool {
    matches!(v, Value::Bool(true))
}

fn like(text: &str, pattern: &str) -> bool {
    fn go(t: &[char], p: &[char]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some(('%', rest)) => (0..=t.len()).any(|i| go(&t[i..], rest)),
            Some(('_', rest)) => !t.is_empty() && go(&t[1..], rest),
            Some((c, rest)) => t.first() == Some(c) && go(&t[1..], rest),
        }
    }
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    go(&t, &p)
}

fn arith(a: Value, op: BinOp, b: Value) -> R<Value> {
    use Value::*;
    if a.is_null() || b.is_null() {
        return Ok(Null);
    }
    let overflow = || ExecError::Error("22003", "integer out of range".into());
    Ok(match (a, b) {
        (Int(x), Int(y)) => match op {
            BinOp::Add => Int(x.checked_add(y).ok_or_else(overflow)?),
            BinOp::Sub => Int(x.checked_sub(y).ok_or_else(overflow)?),
            BinOp::Mul => Int(x.checked_mul(y).ok_or_else(overflow)?),
            BinOp::Div | BinOp::Mod if y == 0 => {
                return Err(ExecError::Error("22012", "division by zero".into()));
            }
            BinOp::Div => Int(x / y),
            _ => Int(x % y),
        },
        (x, y) => {
            let f = |v: Value| match v {
                Int(i) => Ok(i as f64),
                Float(f) => Ok(f),
                other => Err(err(format!("operator does not apply to {other}"))),
            };
            let (x, y) = (f(x)?, f(y)?);
            Float(match op {
                BinOp::Add => x + y,
                BinOp::Sub => x - y,
                BinOp::Mul => x * y,
                BinOp::Div | BinOp::Mod if y == 0.0 => {
                    return Err(ExecError::Error("22012", "division by zero".into()));
                }
                BinOp::Div => x / y,
                _ => x % y,
            })
        }
    })
}

/// Evaluate an expression against one row. Aggregates are not allowed here.
fn eval(e: &Expr, scope: &Scope, row: &[Value]) -> R<Value> {
    Ok(match e {
        Expr::Literal(v) => v.clone(),
        Expr::Column(t, c) => row[scope.find(t.as_deref(), c)?].clone(),
        Expr::Unary(UnaryOp::Neg, x) => match eval(x, scope, row)? {
            Value::Int(i) => Value::Int(-i),
            Value::Float(f) => Value::Float(-f),
            Value::Null => Value::Null,
            v => return Err(err(format!("cannot negate {v}"))),
        },
        Expr::Unary(UnaryOp::Not, x) => match eval(x, scope, row)? {
            Value::Bool(b) => Value::Bool(!b),
            Value::Null => Value::Null,
            v => return Err(err(format!("NOT needs a boolean, got {v}"))),
        },
        Expr::IsNull(x, negated) => Value::Bool(eval(x, scope, row)?.is_null() != *negated),
        Expr::Like(x, p, negated) => match (eval(x, scope, row)?, eval(p, scope, row)?) {
            (Value::Text(t), Value::Text(p)) => Value::Bool(like(&t, &p) != *negated),
            _ => Value::Null,
        },
        Expr::Binary(a, op, b) => {
            let (x, y) = (eval(a, scope, row)?, eval(b, scope, row)?);
            binary(x, *op, y)?
        }
        Expr::Aggregate(..) => return Err(err("aggregate functions are not allowed here")),
    })
}

fn binary(x: Value, op: BinOp, y: Value) -> R<Value> {
    Ok(match op {
        BinOp::And => match (x, y) {
            (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
            (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
            _ => Value::Null,
        },
        BinOp::Or => match (x, y) {
            (Value::Bool(true), _) | (_, Value::Bool(true)) => Value::Bool(true),
            (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
            _ => Value::Null,
        },
        BinOp::Concat => match (x, y) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (a, b) => Value::Text(format!(
                "{}{}",
                a.to_text().unwrap_or_default(),
                b.to_text().unwrap_or_default()
            )),
        },
        BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
            match x.compare(&y) {
                None => Value::Null,
                Some(o) => Value::Bool(match op {
                    BinOp::Eq => o == Ordering::Equal,
                    BinOp::NotEq => o != Ordering::Equal,
                    BinOp::Lt => o == Ordering::Less,
                    BinOp::LtEq => o != Ordering::Greater,
                    BinOp::Gt => o == Ordering::Greater,
                    _ => o != Ordering::Less,
                }),
            }
        }
        _ => arith(x, op, y)?,
    })
}

fn has_aggregate(e: &Expr) -> bool {
    match e {
        Expr::Aggregate(..) => true,
        Expr::Unary(_, x) | Expr::IsNull(x, _) => has_aggregate(x),
        Expr::Binary(a, _, b) | Expr::Like(a, b, _) => has_aggregate(a) || has_aggregate(b),
        _ => false,
    }
}

/// Evaluate over a group of rows: columns come from the group's first row,
/// aggregates fold over all of them.
fn eval_group(e: &Expr, scope: &Scope, rows: &[Vec<Value>]) -> R<Value> {
    Ok(match e {
        Expr::Aggregate(f, arg) => {
            let vals: Vec<Value> = match arg {
                None => rows.iter().map(|_| Value::Int(1)).collect(),
                Some(a) => rows
                    .iter()
                    .map(|r| eval(a, scope, r))
                    .collect::<R<Vec<_>>>()?
                    .into_iter()
                    .filter(|v| !v.is_null())
                    .collect(),
            };
            match f {
                AggFn::Count => Value::Int(vals.len() as i64),
                AggFn::Sum | AggFn::Avg => {
                    if vals.is_empty() {
                        Value::Null
                    } else {
                        let mut sum = Value::Int(0);
                        for v in &vals {
                            sum = arith(sum, BinOp::Add, v.clone())?;
                        }
                        if *f == AggFn::Avg {
                            arith(
                                arith(sum, BinOp::Add, Value::Float(0.0))?,
                                BinOp::Div,
                                Value::Float(vals.len() as f64),
                            )?
                        } else {
                            sum
                        }
                    }
                }
                AggFn::Min => vals
                    .into_iter()
                    .min_by(|a, b| a.sort_cmp(b))
                    .unwrap_or(Value::Null),
                AggFn::Max => vals
                    .into_iter()
                    .max_by(|a, b| a.sort_cmp(b))
                    .unwrap_or(Value::Null),
            }
        }
        Expr::Unary(op, x) => eval(
            &Expr::Unary(*op, Box::new(Expr::Literal(eval_group(x, scope, rows)?))),
            scope,
            &[],
        )?,
        Expr::IsNull(x, n) => Value::Bool(eval_group(x, scope, rows)?.is_null() != *n),
        Expr::Binary(a, op, b) => binary(
            eval_group(a, scope, rows)?,
            *op,
            eval_group(b, scope, rows)?,
        )?,
        Expr::Like(a, b, n) => eval(
            &Expr::Like(
                Box::new(Expr::Literal(eval_group(a, scope, rows)?)),
                Box::new(Expr::Literal(eval_group(b, scope, rows)?)),
                *n,
            ),
            scope,
            &[],
        )?,
        other => match rows.first() {
            Some(r) => eval(other, scope, r)?,
            None => match other {
                Expr::Literal(v) => v.clone(),
                _ => Value::Null,
            },
        },
    })
}

fn show(e: &Expr) -> String {
    match e {
        Expr::Literal(v) => v.to_string(),
        Expr::Column(Some(t), c) => format!("{t}.{c}"),
        Expr::Column(None, c) => c.clone(),
        Expr::Unary(UnaryOp::Neg, x) => format!("-{}", show(x)),
        Expr::Unary(UnaryOp::Not, x) => format!("NOT {}", show(x)),
        Expr::IsNull(x, false) => format!("{} IS NULL", show(x)),
        Expr::IsNull(x, true) => format!("{} IS NOT NULL", show(x)),
        Expr::Like(a, b, n) => format!(
            "{} {}LIKE {}",
            show(a),
            if *n { "NOT " } else { "" },
            show(b)
        ),
        Expr::Aggregate(f, None) => format!("{}(*)", format!("{f:?}").to_lowercase()),
        Expr::Aggregate(f, Some(x)) => format!("{}({})", format!("{f:?}").to_lowercase(), show(x)),
        Expr::Binary(a, op, b) => {
            let o = match op {
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Eq => "=",
                BinOp::NotEq => "<>",
                BinOp::Lt => "<",
                BinOp::LtEq => "<=",
                BinOp::Gt => ">",
                BinOp::GtEq => ">=",
                BinOp::And => "AND",
                BinOp::Or => "OR",
                BinOp::Concat => "||",
            };
            format!("({} {o} {})", show(a), show(b))
        }
    }
}

// ─── Access paths ─────────────────────────────────────────────────────────

/// A range bound on the primary key: the value, and whether it is included.
type Bound = (Value, bool);

/// How to read a table: the planner picks the narrowest the WHERE allows.
#[derive(Clone, Debug)]
enum Access {
    Get(Value),
    Range(Option<Bound>, Option<Bound>),
    Full,
}

fn conjuncts(e: &Expr) -> Vec<&Expr> {
    match e {
        Expr::Binary(a, BinOp::And, b) => {
            let mut v = conjuncts(a);
            v.extend(conjuncts(b));
            v
        }
        other => vec![other],
    }
}

/// Look for `pk = c`, or bounds `pk > c` and `pk < c`, in the WHERE clause.
fn plan_access(t: &TableDef, alias: &str, filter: Option<&Expr>) -> Access {
    let Some(f) = filter else {
        return Access::Full;
    };
    let pk = &t.columns[t.pk].name;
    let is_pk = |e: &Expr| matches!(e, Expr::Column(q, c) if c == pk && q.as_deref().is_none_or(|q| q == alias));
    let (mut lo, mut hi): (Option<Bound>, Option<Bound>) = (None, None);
    for c in conjuncts(f) {
        let Expr::Binary(a, op, b) = c else { continue };
        let (op, lit) = match (a.as_ref(), b.as_ref()) {
            (x, Expr::Literal(v)) if is_pk(x) => (*op, v.clone()),
            (Expr::Literal(v), x) if is_pk(x) => {
                let flipped = match op {
                    BinOp::Lt => BinOp::Gt,
                    BinOp::LtEq => BinOp::GtEq,
                    BinOp::Gt => BinOp::Lt,
                    BinOp::GtEq => BinOp::LtEq,
                    o => *o,
                };
                (flipped, v.clone())
            }
            _ => continue,
        };
        let Ok(lit) = lit.coerce(t.columns[t.pk].ty) else {
            continue;
        };
        match op {
            BinOp::Eq => return Access::Get(lit),
            BinOp::Gt => lo = Some((lit, false)),
            BinOp::GtEq => lo = Some((lit, true)),
            BinOp::Lt => hi = Some((lit, false)),
            BinOp::LtEq => hi = Some((lit, true)),
            _ => {}
        }
    }
    if lo.is_some() || hi.is_some() {
        Access::Range(lo, hi)
    } else {
        Access::Full
    }
}

fn describe_access(t: &TableDef, a: &Access) -> String {
    let pk = &t.columns[t.pk].name;
    match a {
        Access::Get(v) => format!("Get {} by primary key {pk} = {v}", t.name),
        Access::Full => format!("Full scan {}", t.name),
        Access::Range(lo, hi) => {
            let lo = lo.as_ref().map_or("-inf".to_string(), |(v, inc)| {
                format!("{}{v}", if *inc { "[" } else { "(" })
            });
            let hi = hi.as_ref().map_or("+inf".to_string(), |(v, inc)| {
                format!("{v}{}", if *inc { "]" } else { ")" })
            });
            format!("Range scan {} on {pk} {lo}, {hi}", t.name)
        }
    }
}

// ─── The session ──────────────────────────────────────────────────────────

pub struct Session {
    kv: KvClient,
    opts: TxnOptions,
    txn: Option<Txn>,
    /// An error inside an explicit transaction: everything fails until ROLLBACK.
    failed: bool,
    rowid_counter: u64,
}

impl Session {
    pub fn new(kv: KvClient, opts: TxnOptions) -> Session {
        Session {
            kv,
            opts,
            txn: None,
            failed: false,
            rowid_counter: 0,
        }
    }

    /// `I` idle, `T` in a transaction, `E` in a failed transaction.
    pub fn status(&self) -> u8 {
        match (&self.txn, self.failed) {
            (_, true) => b'E',
            (Some(_), _) => b'T',
            (None, _) => b'I',
        }
    }

    /// Execute every statement in `sql`, stopping at the first error.
    pub async fn execute(&mut self, sql: &str) -> Vec<R<QueryResult>> {
        let stmts = match parser::parse(sql) {
            Ok(s) => s,
            Err(e) => {
                if self.txn.is_some() {
                    self.failed = true;
                }
                return vec![Err(ExecError::Error("42601", e))];
            }
        };
        let mut out = Vec::new();
        for stmt in stmts {
            let r = self.statement(stmt).await;
            let stop = r.is_err();
            out.push(r);
            if stop {
                break;
            }
        }
        out
    }

    async fn statement(&mut self, stmt: Statement) -> R<QueryResult> {
        match stmt {
            Statement::Begin => {
                if self.txn.is_some() {
                    return Ok(QueryResult::command("BEGIN"));
                }
                self.txn = Some(
                    Txn::begin(self.kv.clone(), self.opts.clone())
                        .await
                        .map_err(from_txn)?,
                );
                self.failed = false;
                Ok(QueryResult::command("BEGIN"))
            }
            Statement::Commit => {
                let failed = std::mem::replace(&mut self.failed, false);
                match self.txn.take() {
                    None => Ok(QueryResult::command("COMMIT")),
                    Some(mut t) if failed => {
                        t.rollback().await;
                        Ok(QueryResult::command("ROLLBACK"))
                    }
                    Some(mut t) => t
                        .commit()
                        .await
                        .map(|_| QueryResult::command("COMMIT"))
                        .map_err(from_txn),
                }
            }
            Statement::Rollback => {
                self.failed = false;
                if let Some(mut t) = self.txn.take() {
                    t.rollback().await;
                }
                Ok(QueryResult::command("ROLLBACK"))
            }
            Statement::Set => Ok(QueryResult::command("SET")),
            stmt => {
                if self.failed {
                    return Err(ExecError::Error(
                        "25P02",
                        "current transaction is aborted, commands ignored until end of transaction block".into(),
                    ));
                }
                if let Some(mut txn) = self.txn.take() {
                    let r = run(&mut txn, &stmt, &mut self.rowid_counter).await;
                    if r.is_err() {
                        self.failed = true;
                    }
                    self.txn = Some(txn);
                    return r;
                }
                // Autocommit: run in a transaction of its own, retrying if it
                // lost a serialization conflict.
                let mut attempt = 0;
                loop {
                    attempt += 1;
                    let mut txn = Txn::begin(self.kv.clone(), self.opts.clone())
                        .await
                        .map_err(from_txn)?;
                    let r = match run(&mut txn, &stmt, &mut self.rowid_counter).await {
                        Ok(res) => txn.commit().await.map(|_| res).map_err(from_txn),
                        Err(e) => {
                            txn.rollback().await;
                            Err(e)
                        }
                    };
                    match r {
                        Err(ExecError::Retry(_)) if attempt < 10 => {
                            self.kv.io.sleep(attempt * 5 + self.kv.io.rand(20)).await;
                        }
                        other => return other,
                    }
                }
            }
        }
    }
}

// ─── Statement execution ──────────────────────────────────────────────────

async fn table(txn: &mut Txn, name: &str) -> R<TableDef> {
    let mut key = CATALOG.to_vec();
    key.extend_from_slice(name.as_bytes());
    match txn.get(&key).await.map_err(from_txn)? {
        Some(v) => TableDef::decode(&v).ok_or_else(|| err("corrupt catalog entry")),
        None => Err(ExecError::Error(
            "42P01",
            format!("relation \"{name}\" does not exist"),
        )),
    }
}

async fn read_rows(txn: &mut Txn, t: &TableDef, access: &Access) -> R<Vec<Vec<Value>>> {
    let decode = |v: &[u8]| types::decode_row(v).ok_or_else(|| err("corrupt row"));
    match access {
        Access::Get(pk) => match txn.get(&t.row_key(pk)).await.map_err(from_txn)? {
            Some(v) => Ok(vec![decode(&v)?]),
            None => Ok(Vec::new()),
        },
        Access::Full | Access::Range(..) => {
            let (start, end) = match access {
                Access::Range(lo, hi) => (
                    lo.as_ref()
                        .map_or_else(|| t.prefix(), |(v, _)| t.row_key(v)),
                    hi.as_ref().map_or_else(
                        || t.prefix_end(),
                        |(v, inc)| {
                            let mut k = t.row_key(v);
                            if *inc {
                                k.push(0xFF);
                            }
                            k
                        },
                    ),
                ),
                _ => (t.prefix(), t.prefix_end()),
            };
            let rows = txn
                .scan(&start, &end, usize::MAX / 4)
                .await
                .map_err(from_txn)?;
            rows.iter().map(|(_, v)| decode(v)).collect()
        }
    }
}

async fn run(txn: &mut Txn, stmt: &Statement, rowid: &mut u64) -> R<QueryResult> {
    match stmt {
        Statement::CreateTable {
            name,
            columns,
            if_not_exists,
        } => create_table(txn, name, columns, *if_not_exists).await,
        Statement::DropTable { name, if_exists } => {
            let t = match table(txn, name).await {
                Ok(t) => t,
                Err(_) if *if_exists => return Ok(QueryResult::command("DROP TABLE")),
                Err(e) => return Err(e),
            };
            for (k, _) in txn
                .scan(&t.prefix(), &t.prefix_end(), usize::MAX / 4)
                .await
                .map_err(from_txn)?
            {
                txn.delete(&k);
            }
            let mut key = CATALOG.to_vec();
            key.extend_from_slice(name.as_bytes());
            txn.delete(&key);
            Ok(QueryResult::command("DROP TABLE"))
        }
        Statement::ShowTables => {
            let rows = txn
                .scan(CATALOG, CATALOG_END, usize::MAX / 4)
                .await
                .map_err(from_txn)?;
            Ok(QueryResult {
                columns: vec![("table_name".into(), DataType::Text)],
                rows: rows
                    .iter()
                    .filter_map(|(_, v)| TableDef::decode(v))
                    .map(|t| vec![Value::Text(t.name)])
                    .collect(),
                tag: format!("SELECT {}", rows.len()),
            })
        }
        Statement::Insert {
            table: name,
            columns,
            rows,
        } => insert(txn, name, columns.as_deref(), rows, rowid).await,
        Statement::Select(sel) => select(txn, sel).await,
        Statement::Update {
            table: name,
            set,
            filter,
        } => update(txn, name, set, filter.as_ref()).await,
        Statement::Delete {
            table: name,
            filter,
        } => {
            let t = table(txn, name).await?;
            let scope = Scope::of(&t, name);
            let access = plan_access(&t, name, filter.as_ref());
            let mut n = 0;
            for row in read_rows(txn, &t, &access).await? {
                if filter
                    .as_ref()
                    .map_or(Ok(true), |f| eval(f, &scope, &row).map(|v| truthy(&v)))?
                {
                    txn.delete(&t.row_key(&row[t.pk]));
                    n += 1;
                }
            }
            Ok(QueryResult::command(format!("DELETE {n}")))
        }
        Statement::Explain(inner) => explain(txn, inner).await,
        other => Err(err(format!("{other:?} cannot run here"))),
    }
}

async fn create_table(
    txn: &mut Txn,
    name: &str,
    columns: &[ColumnDef],
    if_not_exists: bool,
) -> R<QueryResult> {
    let mut key = CATALOG.to_vec();
    key.extend_from_slice(name.as_bytes());
    if txn.get(&key).await.map_err(from_txn)?.is_some() {
        return if if_not_exists {
            Ok(QueryResult::command("CREATE TABLE"))
        } else {
            Err(ExecError::Error(
                "42P07",
                format!("relation \"{name}\" already exists"),
            ))
        };
    }
    let mut cols = columns.to_vec();
    let mut names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    names.sort_unstable();
    if names.windows(2).any(|w| w[0] == w[1]) {
        return Err(ExecError::Error(
            "42701",
            "a column name appears twice".into(),
        ));
    }
    let hidden_rowid = !cols.iter().any(|c| c.primary_key);
    if hidden_rowid {
        cols.push(ColumnDef {
            name: ROWID.into(),
            ty: DataType::Int,
            primary_key: true,
            not_null: true,
        });
    }
    let id = match txn.get(NEXT_TABLE_ID).await.map_err(from_txn)? {
        Some(v) => u32::from_be_bytes(v.try_into().map_err(|_| err("corrupt catalog counter"))?),
        None => 1,
    };
    txn.put(NEXT_TABLE_ID, &(id + 1).to_be_bytes());
    let pk = cols
        .iter()
        .position(|c| c.primary_key)
        .expect("has a primary key");
    let def = TableDef {
        id,
        name: name.to_string(),
        columns: cols,
        pk,
        hidden_rowid,
    };
    txn.put(&key, &def.encode());
    Ok(QueryResult::command("CREATE TABLE"))
}

fn check_row(t: &TableDef, row: Vec<Value>) -> R<Vec<Value>> {
    row.into_iter()
        .zip(&t.columns)
        .map(|(v, c)| {
            let v = v
                .coerce(c.ty)
                .map_err(|e| ExecError::Error("22P02", format!("column \"{}\": {e}", c.name)))?;
            if v.is_null() && c.not_null {
                return Err(ExecError::Error(
                    "23502",
                    format!(
                        "null value in column \"{}\" violates not-null constraint",
                        c.name
                    ),
                ));
            }
            Ok(v)
        })
        .collect()
}

async fn insert(
    txn: &mut Txn,
    name: &str,
    columns: Option<&[String]>,
    rows: &[Vec<Expr>],
    rowid: &mut u64,
) -> R<QueryResult> {
    let t = table(txn, name).await?;
    let targets: Vec<usize> = match columns {
        None => (0..t.visible()).collect(),
        Some(cols) => cols
            .iter()
            .map(|c| {
                t.columns.iter().position(|d| &d.name == c).ok_or_else(|| {
                    ExecError::Error(
                        "42703",
                        format!("column \"{c}\" of relation \"{name}\" does not exist"),
                    )
                })
            })
            .collect::<R<_>>()?,
    };
    let empty = Scope { cols: Vec::new() };
    for exprs in rows {
        if exprs.len() != targets.len() {
            return Err(err(format!(
                "INSERT has {} expressions but {} target columns",
                exprs.len(),
                targets.len()
            )));
        }
        let mut row = vec![Value::Null; t.columns.len()];
        for (e, &i) in exprs.iter().zip(&targets) {
            row[i] = eval(e, &empty, &[])?;
        }
        // An integer key left out is generated: unique, and increasing
        // within a session.
        if row[t.pk].is_null() && t.columns[t.pk].ty == DataType::Int {
            *rowid += 1;
            row[t.pk] = Value::Int(((txn.start_ts as i64) << 20) + *rowid as i64);
        }
        let row = check_row(&t, row)?;
        let key = t.row_key(&row[t.pk]);
        if txn.get(&key).await.map_err(from_txn)?.is_some() {
            return Err(ExecError::Error(
                "23505",
                format!(
                    "duplicate key value violates unique constraint \"{name}_pkey\": ({})=({})",
                    t.columns[t.pk].name,
                    row[t.pk].to_text().unwrap_or_default()
                ),
            ));
        }
        txn.put(&key, &types::encode_row(&row));
    }
    Ok(QueryResult::command(format!("INSERT 0 {}", rows.len())))
}

async fn update(
    txn: &mut Txn,
    name: &str,
    set: &[(String, Expr)],
    filter: Option<&Expr>,
) -> R<QueryResult> {
    let t = table(txn, name).await?;
    let scope = Scope::of(&t, name);
    let targets: Vec<(usize, &Expr)> = set
        .iter()
        .map(|(c, e)| {
            t.columns
                .iter()
                .position(|d| &d.name == c)
                .map(|i| (i, e))
                .ok_or_else(|| {
                    ExecError::Error(
                        "42703",
                        format!("column \"{c}\" of relation \"{name}\" does not exist"),
                    )
                })
        })
        .collect::<R<_>>()?;
    let access = plan_access(&t, name, filter);
    let mut n = 0;
    for row in read_rows(txn, &t, &access).await? {
        if !filter.map_or(Ok(true), |f| eval(f, &scope, &row).map(|v| truthy(&v)))? {
            continue;
        }
        let mut new = row.clone();
        for (i, e) in &targets {
            new[*i] = eval(e, &scope, &row)?;
        }
        let new = check_row(&t, new)?;
        if new[t.pk] != row[t.pk] {
            let key = t.row_key(&new[t.pk]);
            if txn.get(&key).await.map_err(from_txn)?.is_some() {
                return Err(ExecError::Error(
                    "23505",
                    format!("duplicate key value violates unique constraint \"{name}_pkey\""),
                ));
            }
            txn.delete(&t.row_key(&row[t.pk]));
        }
        txn.put(&t.row_key(&new[t.pk]), &types::encode_row(&new));
        n += 1;
    }
    Ok(QueryResult::command(format!("UPDATE {n}")))
}

// ─── SELECT ───────────────────────────────────────────────────────────────

/// The steps a SELECT will take, for EXPLAIN.
struct Plan {
    steps: Vec<String>,
}

async fn select(txn: &mut Txn, sel: &Select) -> R<QueryResult> {
    let mut plan = Plan { steps: Vec::new() };
    let (scope, rows) = from_where(txn, sel, &mut plan, false).await?;
    finish_select(sel, scope, rows, &mut plan)
}

/// FROM, JOINs and WHERE: the rows the rest of the query works on.
async fn from_where(
    txn: &mut Txn,
    sel: &Select,
    plan: &mut Plan,
    dry: bool,
) -> R<(Scope, Vec<Vec<Value>>)> {
    let Some(from) = &sel.from else {
        return Ok((Scope { cols: Vec::new() }, vec![Vec::new()]));
    };
    let alias = from.alias.clone().unwrap_or_else(|| from.name.clone());
    let t = table(txn, &from.name).await?;
    // Push the WHERE clause down to the first table's access path only if
    // it cannot reject rows a LEFT JOIN would have padded.
    let pushable = sel.joins.is_empty() || sel.joins.iter().all(|j| j.kind == JoinKind::Inner);
    let access = if pushable {
        plan_access(&t, &alias, sel.filter.as_ref())
    } else {
        Access::Full
    };
    plan.steps.push(describe_access(&t, &access));
    let mut scope = Scope::of(&t, &alias);
    let mut rows = if dry {
        Vec::new()
    } else {
        read_rows(txn, &t, &access).await?
    };

    for j in &sel.joins {
        let jalias = j
            .table
            .alias
            .clone()
            .unwrap_or_else(|| j.table.name.clone());
        let jt = table(txn, &j.table.name).await?;
        let jscope = Scope::of(&jt, &jalias);
        let right = if dry {
            Vec::new()
        } else {
            read_rows(txn, &jt, &Access::Full).await?
        };
        let joined = scope.join(&jscope);
        // An equi-join on one column from each side becomes a hash join.
        let equi = match &j.on {
            Expr::Binary(a, BinOp::Eq, b) => {
                let side = |e: &Expr, s: &Scope| match e {
                    Expr::Column(q, c) => s.find(q.as_deref(), c).ok(),
                    _ => None,
                };
                match (
                    side(a, &scope),
                    side(b, &jscope),
                    side(b, &scope),
                    side(a, &jscope),
                ) {
                    (Some(l), Some(r), _, _) | (_, _, Some(l), Some(r)) => Some((l, r)),
                    _ => None,
                }
            }
            _ => None,
        };
        let kind = if j.kind == JoinKind::Left {
            "Left"
        } else {
            "Inner"
        };
        let mut out = Vec::new();
        match equi {
            Some((l, r)) => {
                plan.steps
                    .push(format!("{kind} hash join {} on {}", jt.name, show(&j.on)));
                let mut table: BTreeMap<Vec<u8>, Vec<&Vec<Value>>> = BTreeMap::new();
                for row in &right {
                    if !row[r].is_null() {
                        let mut k = Vec::new();
                        types::encode_key(&row[r], &mut k);
                        table.entry(k).or_default().push(row);
                    }
                }
                for lrow in &rows {
                    let mut k = Vec::new();
                    types::encode_key(&lrow[l], &mut k);
                    let matches = if lrow[l].is_null() {
                        None
                    } else {
                        table.get(&k)
                    };
                    match matches {
                        Some(ms) => {
                            for m in ms {
                                let mut row = lrow.clone();
                                row.extend(m.iter().cloned());
                                out.push(row);
                            }
                        }
                        None if j.kind == JoinKind::Left => {
                            let mut row = lrow.clone();
                            row.extend(std::iter::repeat_n(Value::Null, jt.columns.len()));
                            out.push(row);
                        }
                        None => {}
                    }
                }
            }
            None => {
                plan.steps.push(format!(
                    "{kind} nested loop join {} on {}",
                    jt.name,
                    show(&j.on)
                ));
                for lrow in &rows {
                    let mut matched = false;
                    for rrow in &right {
                        let mut row = lrow.clone();
                        row.extend(rrow.iter().cloned());
                        if truthy(&eval(&j.on, &joined, &row)?) {
                            matched = true;
                            out.push(row);
                        }
                    }
                    if !matched && j.kind == JoinKind::Left {
                        let mut row = lrow.clone();
                        row.extend(std::iter::repeat_n(Value::Null, jt.columns.len()));
                        out.push(row);
                    }
                }
            }
        }
        scope = joined;
        rows = out;
    }

    if let Some(f) = &sel.filter {
        plan.steps.push(format!("Filter {}", show(f)));
        let mut kept = Vec::new();
        for row in rows {
            if truthy(&eval(f, &scope, &row)?) {
                kept.push(row);
            }
        }
        rows = kept;
    }
    Ok((scope, rows))
}

fn finish_select(
    sel: &Select,
    scope: Scope,
    rows: Vec<Vec<Value>>,
    plan: &mut Plan,
) -> R<QueryResult> {
    // The output columns.
    let mut items: Vec<(Expr, String)> = Vec::new();
    for item in &sel.items {
        match item {
            SelectItem::Wildcard => {
                for (t, c, _, hidden) in &scope.cols {
                    if !hidden {
                        items.push((Expr::Column(Some(t.clone()), c.clone()), c.clone()));
                    }
                }
            }
            SelectItem::Expr(e, alias) => {
                let name = alias.clone().unwrap_or_else(|| match e {
                    Expr::Column(_, c) => c.clone(),
                    Expr::Aggregate(f, _) => format!("{f:?}").to_lowercase(),
                    _ => "?column?".to_string(),
                });
                items.push((e.clone(), name));
            }
        }
    }
    let grouped = !sel.group_by.is_empty()
        || items.iter().any(|(e, _)| has_aggregate(e))
        || sel.having.as_ref().is_some_and(has_aggregate);

    // Each output row, with its sort key.
    let mut out: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
    let order_value =
        |e: &Expr, projected: &[Value], eval_input: &dyn Fn(&Expr) -> R<Value>| -> R<Value> {
            if let Expr::Column(None, name) = e
                && let Some(i) = items.iter().position(|(_, n)| n == name)
            {
                return Ok(projected[i].clone());
            }
            if let Expr::Literal(Value::Int(n)) = e
                && *n >= 1
                && (*n as usize) <= projected.len()
            {
                return Ok(projected[*n as usize - 1].clone());
            }
            eval_input(e)
        };
    if grouped {
        let mut groups: BTreeMap<Vec<u8>, Vec<Vec<Value>>> = BTreeMap::new();
        for row in rows {
            let mut key = Vec::new();
            for g in &sel.group_by {
                types::encode_key(&eval(g, &scope, &row)?, &mut key);
            }
            groups.entry(key).or_default().push(row);
        }
        if groups.is_empty() && sel.group_by.is_empty() {
            groups.insert(Vec::new(), Vec::new()); // SELECT COUNT(*) over nothing is one row
        }
        plan.steps.push(if sel.group_by.is_empty() {
            "Aggregate".to_string()
        } else {
            format!(
                "Hash aggregate by {}",
                sel.group_by.iter().map(show).collect::<Vec<_>>().join(", ")
            )
        });
        for (_, g) in groups {
            if let Some(h) = &sel.having
                && !truthy(&eval_group(h, &scope, &g)?)
            {
                continue;
            }
            let projected: Vec<Value> = items
                .iter()
                .map(|(e, _)| eval_group(e, &scope, &g))
                .collect::<R<_>>()?;
            let keys = sel
                .order_by
                .iter()
                .map(|(e, _)| order_value(e, &projected, &|e| eval_group(e, &scope, &g)))
                .collect::<R<_>>()?;
            out.push((projected, keys));
        }
    } else {
        for row in rows {
            let projected: Vec<Value> = items
                .iter()
                .map(|(e, _)| eval(e, &scope, &row))
                .collect::<R<_>>()?;
            let keys = sel
                .order_by
                .iter()
                .map(|(e, _)| order_value(e, &projected, &|e| eval(e, &scope, &row)))
                .collect::<R<_>>()?;
            out.push((projected, keys));
        }
    }
    if !sel.order_by.is_empty() {
        plan.steps.push(format!(
            "Sort by {}",
            sel.order_by
                .iter()
                .map(|(e, d)| format!("{}{}", show(e), if *d { " DESC" } else { "" }))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        out.sort_by(|a, b| {
            for (i, (_, desc)) in sel.order_by.iter().enumerate() {
                let o = a.1[i].sort_cmp(&b.1[i]);
                let o = if *desc { o.reverse() } else { o };
                if o != Ordering::Equal {
                    return o;
                }
            }
            Ordering::Equal
        });
    }
    let offset = sel.offset.unwrap_or(0) as usize;
    let limit = sel.limit.map_or(usize::MAX, |l| l as usize);
    if sel.limit.is_some() || sel.offset.is_some() {
        plan.steps.push(format!(
            "Limit {}{}",
            sel.limit.map_or("all".into(), |l| l.to_string()),
            if offset > 0 {
                format!(" offset {offset}")
            } else {
                String::new()
            }
        ));
    }
    let rows: Vec<Vec<Value>> = out
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(r, _)| r)
        .collect();

    // Column types: from the schema for plain columns, else from the data.
    let columns = items
        .iter()
        .enumerate()
        .map(|(i, (e, name))| {
            let ty = match e {
                Expr::Column(q, c) => scope.find(q.as_deref(), c).ok().map(|j| scope.cols[j].2),
                Expr::Aggregate(AggFn::Count, _) => Some(DataType::Int),
                Expr::Aggregate(AggFn::Avg, _) => Some(DataType::Float),
                _ => None,
            }
            .or_else(|| rows.iter().find_map(|r| r[i].data_type()))
            .unwrap_or(DataType::Text);
            (name.clone(), ty)
        })
        .collect();
    let n = rows.len();
    Ok(QueryResult {
        columns,
        rows,
        tag: format!("SELECT {n}"),
    })
}

async fn explain(txn: &mut Txn, stmt: &Statement) -> R<QueryResult> {
    let mut plan = Plan { steps: Vec::new() };
    match stmt {
        Statement::Select(sel) => {
            let (scope, _) = from_where(txn, sel, &mut plan, true).await?;
            finish_select(sel, scope, Vec::new(), &mut plan)?;
        }
        Statement::Update {
            table: name,
            filter,
            ..
        }
        | Statement::Delete {
            table: name,
            filter,
        } => {
            let t = table(txn, name).await?;
            plan.steps
                .push(describe_access(&t, &plan_access(&t, name, filter.as_ref())));
            if let Some(f) = filter {
                plan.steps.push(format!("Filter {}", show(f)));
            }
            plan.steps.push(
                if matches!(stmt, Statement::Update { .. }) {
                    "Update rows"
                } else {
                    "Delete rows"
                }
                .to_string(),
            );
        }
        Statement::Insert {
            table: name, rows, ..
        } => plan
            .steps
            .push(format!("Insert {} rows into {name}", rows.len())),
        other => plan.steps.push(format!("{other:?}")),
    }
    let rows: Vec<Vec<Value>> = plan
        .steps
        .iter()
        .enumerate()
        .map(|(i, s)| vec![Value::Text(format!("{}{s}", "  ".repeat(i)))])
        .collect();
    let n = rows.len();
    Ok(QueryResult {
        columns: vec![("QUERY PLAN".into(), DataType::Text)],
        rows,
        tag: format!("EXPLAIN {n}"),
    })
}
