//! The PostgreSQL frontend/backend protocol, version 3: startup, the simple
//! query protocol, and error and status reporting. Every value is sent in
//! text format with its Postgres type OID, so clients render it natively.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::kv::client::KvClient;
use crate::sql::exec::Session;
use crate::sql::types::DataType;
use crate::txn::client::TxnOptions;

const SSL_REQUEST: u32 = 80877103;
const CANCEL_REQUEST: u32 = 80877102;
const PROTOCOL_3: u32 = 196608;

/// Bytes in from the socket, bytes out to it, shared with the event loop.
#[derive(Default)]
pub struct Conn {
    input: Vec<u8>,
    pub out: Vec<u8>,
    closed: bool,
    /// The session has ended; drop the connection once `out` is flushed.
    pub finished: bool,
    waker: Option<Waker>,
}

impl Conn {
    pub fn feed(&mut self, bytes: &[u8]) {
        self.input.extend_from_slice(bytes);
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn close(&mut self) {
        self.closed = true;
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }
}

struct ReadExact {
    conn: Rc<RefCell<Conn>>,
    n: usize,
}

impl Future for ReadExact {
    type Output = Option<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut c = self.conn.borrow_mut();
        if c.input.len() >= self.n {
            let rest = c.input.split_off(self.n);
            return Poll::Ready(Some(std::mem::replace(&mut c.input, rest)));
        }
        if c.closed {
            return Poll::Ready(None);
        }
        c.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

async fn read(conn: &Rc<RefCell<Conn>>, n: usize) -> Option<Vec<u8>> {
    ReadExact {
        conn: conn.clone(),
        n,
    }
    .await
}

fn u32_at(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// Builds one backend message.
struct Msg(Vec<u8>);

impl Msg {
    fn new(kind: u8) -> Msg {
        Msg(vec![kind, 0, 0, 0, 0])
    }

    fn i16(mut self, v: i16) -> Msg {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    fn i32(mut self, v: i32) -> Msg {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    fn byte(mut self, v: u8) -> Msg {
        self.0.push(v);
        self
    }

    fn cstr(mut self, s: &str) -> Msg {
        self.0.extend_from_slice(s.as_bytes());
        self.0.push(0);
        self
    }

    fn bytes(mut self, b: &[u8]) -> Msg {
        self.0.extend_from_slice(b);
        self
    }

    fn send(mut self, conn: &Rc<RefCell<Conn>>) {
        let len = (self.0.len() - 1) as u32;
        self.0[1..5].copy_from_slice(&len.to_be_bytes());
        conn.borrow_mut().out.extend_from_slice(&self.0);
    }
}

fn error(conn: &Rc<RefCell<Conn>>, code: &str, message: &str) {
    Msg::new(b'E')
        .byte(b'S')
        .cstr("ERROR")
        .byte(b'V')
        .cstr("ERROR")
        .byte(b'C')
        .cstr(code)
        .byte(b'M')
        .cstr(message)
        .byte(0)
        .send(conn);
}

fn type_len(ty: DataType) -> i16 {
    match ty {
        DataType::Int | DataType::Float => 8,
        DataType::Bool => 1,
        DataType::Text => -1,
    }
}

/// Serve one client connection until it goes away.
pub async fn session(conn: Rc<RefCell<Conn>>, kv: KvClient, opts: TxnOptions, pid: i32) {
    if startup(&conn, pid).await.is_none() {
        conn.borrow_mut().finished = true;
        return;
    }
    let mut session = Session::new(kv, opts);
    loop {
        let Some(head) = read(&conn, 5).await else {
            break;
        };
        let len = u32_at(&head[1..]) as usize;
        let Some(body) = read(&conn, len.saturating_sub(4)).await else {
            break;
        };
        match head[0] {
            b'Q' => {
                let sql =
                    String::from_utf8_lossy(body.strip_suffix(&[0]).unwrap_or(&body)).into_owned();
                if sql.trim().trim_matches(';').trim().is_empty() {
                    Msg::new(b'I').send(&conn);
                } else {
                    for result in session.execute(&sql).await {
                        match result {
                            Ok(r) => {
                                if !r.columns.is_empty() {
                                    let mut m = Msg::new(b'T').i16(r.columns.len() as i16);
                                    for (name, ty) in &r.columns {
                                        m = m
                                            .cstr(name)
                                            .i32(0)
                                            .i16(0)
                                            .i32(ty.oid() as i32)
                                            .i16(type_len(*ty))
                                            .i32(-1)
                                            .i16(0);
                                    }
                                    m.send(&conn);
                                    for row in &r.rows {
                                        let mut m = Msg::new(b'D').i16(row.len() as i16);
                                        for v in row {
                                            m = match v.to_text() {
                                                None => m.i32(-1),
                                                Some(t) => {
                                                    m.i32(t.len() as i32).bytes(t.as_bytes())
                                                }
                                            };
                                        }
                                        m.send(&conn);
                                    }
                                }
                                Msg::new(b'C').cstr(&r.tag).send(&conn);
                            }
                            Err(e) => error(&conn, e.code(), &e.message()),
                        }
                    }
                }
                Msg::new(b'Z').byte(session.status()).send(&conn);
            }
            b'X' => break,
            // The extended query protocol (prepared statements): politely
            // decline, and stay in sync with the client.
            b'P' | b'B' | b'D' | b'E' | b'C' | b'H' | b'F' => {}
            b'S' => {
                error(
                    &conn,
                    "0A000",
                    "the extended query protocol is not supported yet; use simple queries",
                );
                Msg::new(b'Z').byte(session.status()).send(&conn);
            }
            other => {
                error(
                    &conn,
                    "08P01",
                    &format!("unexpected message type {:?}", other as char),
                );
                break;
            }
        }
    }
    conn.borrow_mut().finished = true;
}

async fn startup(conn: &Rc<RefCell<Conn>>, pid: i32) -> Option<()> {
    loop {
        let len = u32_at(&read(conn, 4).await?) as usize;
        let body = read(conn, len.checked_sub(4)?).await?;
        let code = u32_at(&body);
        match code {
            SSL_REQUEST => conn.borrow_mut().out.push(b'N'),
            CANCEL_REQUEST => return None,
            PROTOCOL_3 => break,
            _ => {
                error(conn, "08P01", "unsupported protocol version");
                return None;
            }
        }
    }
    Msg::new(b'R').i32(0).send(conn);
    for (k, v) in [
        ("server_version", "15.0 (quorumdb)"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("TimeZone", "UTC"),
    ] {
        Msg::new(b'S').cstr(k).cstr(v).send(conn);
    }
    Msg::new(b'K').i32(pid).i32(0x5EC2E7).send(conn);
    Msg::new(b'Z').byte(b'I').send(conn);
    Some(())
}
