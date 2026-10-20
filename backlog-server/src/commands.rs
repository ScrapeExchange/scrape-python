//! Command dispatch: authentication, the redis-py handshake commands
//! and the `BACKLOG.*` commands.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::codec::Codec;
use crate::lane::LaneId;
use crate::meta::{ALL_STATES, Reason, State};
use crate::queue::{ForceOutcome, PopItem, QueueInfo};
use crate::resp::Value;

/// One operation for a queue's owner task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Add(Vec<u64>),
    Force(u64),
    Pop {
        n: usize,
        lease: Option<Duration>,
    },
    Touch {
        id: u64,
        lease: Option<Duration>,
    },
    Complete(Vec<u64>),
    Mark {
        id: u64,
        state: State,
        reason: Reason,
    },
    Requeue {
        id: u64,
        lane: LaneId,
    },
    State(Vec<u64>),
    Info,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Added(Vec<bool>),
    Force(ForceOutcome),
    Popped(Vec<PopItem>),
    Bool(bool),
    Count(u64),
    Ok,
    States(Vec<Option<State>>),
    Info(QueueInfo),
    Path(String),
    Err(String),
}

pub type BoxReply<'a> = Pin<Box<dyn Future<Output = Reply> + Send + 'a>>;

pub trait QueueOps: Send + Sync {
    fn call(&self, req: Request) -> BoxReply<'_>;
}

pub struct QueueEntry {
    pub codec: Codec,
    pub ops: Arc<dyn QueueOps>,
    pub invalid_ids: Arc<AtomicU64>,
}

impl QueueEntry {
    pub fn new(codec: Codec, ops: Arc<dyn QueueOps>) -> QueueEntry {
        QueueEntry {
            codec,
            ops,
            invalid_ids: Arc::new(AtomicU64::new(0)),
        }
    }
}

pub struct Ctx {
    pub queues: HashMap<String, QueueEntry>,
    pub password: Option<String>,
}

#[derive(Debug)]
pub struct ConnState {
    pub authed: bool,
    /// Negotiated protocol: 2 until a successful `HELLO 3`.
    pub proto: u8,
}

impl Default for ConnState {
    fn default() -> ConnState {
        ConnState {
            authed: false,
            proto: 2,
        }
    }
}

fn ok() -> Value {
    Value::Simple("OK".into())
}

fn err(msg: &str) -> Value {
    Value::Error(msg.to_string())
}

fn bulk(s: &str) -> Value {
    Value::Bulk(Some(s.as_bytes().to_vec()))
}

fn wrong_args() -> Value {
    err("ERR wrong number of arguments")
}

fn text(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

fn upper(b: &[u8]) -> String {
    text(b).to_ascii_uppercase()
}

fn parse_u64(b: &[u8]) -> Option<u64> {
    text(b).parse::<u64>().ok()
}

fn check_password(c: &Ctx, user: Option<&[u8]>, pass: &[u8]) -> bool {
    if user.is_some_and(|u| u != b"default") {
        return false;
    }
    match &c.password {
        Some(p) => p.as_bytes() == pass,
        None => true,
    }
}

fn auth(c: &Ctx, conn: &mut ConnState, args: &[Vec<u8>]) -> Value {
    let (user, pass): (Option<&[u8]>, &[u8]) = match args.len() {
        2 => (None, &args[1]),
        3 => (Some(&args[1]), &args[2]),
        _ => return wrong_args(),
    };
    if check_password(c, user, pass) {
        conn.authed = true;
        ok()
    } else {
        err("WRONGPASS invalid username-password pair")
    }
}

fn hello(c: &Ctx, conn: &mut ConnState, args: &[Vec<u8>]) -> Value {
    let proto: u8 = match args.get(1).map(|v| text(v)) {
        None => conn.proto,
        Some("2") => 2,
        Some("3") => 3,
        Some(_) => return err("NOPROTO unsupported protocol version"),
    };
    let mut i: usize = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "AUTH" if i + 2 < args.len() => {
                if !check_password(c, Some(&args[i + 1]), &args[i + 2]) {
                    return err("WRONGPASS invalid username-password pair");
                }
                conn.authed = true;
                i += 3;
            }
            "SETNAME" if i + 1 < args.len() => i += 2,
            _ => return err("ERR syntax error"),
        }
    }
    conn.proto = proto;
    Value::Map(vec![
        (bulk("server"), bulk("backlog-server")),
        (bulk("version"), bulk(env!("CARGO_PKG_VERSION"))),
        (bulk("proto"), Value::Int(i64::from(proto))),
        (bulk("id"), Value::Int(0)),
        (bulk("mode"), bulk("standalone")),
        (bulk("role"), bulk("master")),
        (bulk("modules"), Value::Array(Some(Vec::new()))),
    ])
}

/// Execute one command.
pub async fn dispatch(
    args: &[Vec<u8>],
    conn: &mut ConnState,
    c: &Ctx,
) -> Value {
    let Some(first) = args.first() else {
        return err("ERR empty command");
    };
    let name: String = upper(first);
    if c.password.is_none() {
        conn.authed = true;
    }
    match name.as_str() {
        "AUTH" => return auth(c, conn, args),
        "HELLO" => return hello(c, conn, args),
        "PING" => {
            return match args.get(1) {
                Some(m) => Value::Bulk(Some(m.clone())),
                None => Value::Simple("PONG".into()),
            };
        }
        _ => {}
    }
    if !conn.authed {
        return err("NOAUTH Authentication required.");
    }
    match name.as_str() {
        "CLIENT" => {
            if args.get(1).is_some_and(|s| upper(s) == "GETNAME") {
                Value::Bulk(None)
            } else {
                ok()
            }
        }
        "SELECT" => match args.get(1).map(|v| text(v)) {
            Some("0") => ok(),
            Some(_) => err("ERR DB index is out of range"),
            None => wrong_args(),
        },
        "COMMAND" => Value::Array(Some(Vec::new())),
        "INFO" => bulk("# Server\r\nserver:backlog-server\r\n"),
        "QUIT" => ok(),
        n if n.starts_with("BACKLOG.") => backlog(&n[8..], args, c).await,
        _ => err("ERR unknown command"),
    }
}

async fn backlog(op: &str, args: &[Vec<u8>], c: &Ctx) -> Value {
    let Some(qname) = args.get(1) else {
        return wrong_args();
    };
    let Some(q) = c.queues.get(text(qname)) else {
        return err("ERR unknown queue");
    };
    let rest: &[Vec<u8>] = &args[2..];
    let one_id = |b: &[u8]| -> Option<u64> {
        let r: Option<u64> = q.codec.encode(text(b)).ok();
        if r.is_none() {
            q.invalid_ids.fetch_add(1, Ordering::Relaxed);
        }
        r
    };
    let req: Request = match op {
        "ADD" => {
            if rest.is_empty() {
                return wrong_args();
            }
            let enc: Vec<Option<u64>> =
                rest.iter().map(|b| one_id(b)).collect();
            let ids: Vec<u64> = enc.iter().flatten().copied().collect();
            let added: Vec<bool> = if ids.is_empty() {
                Vec::new()
            } else {
                match q.ops.call(Request::Add(ids)).await {
                    Reply::Added(v) => v,
                    other => return reply_value(other, q.codec),
                }
            };
            let mut it = added.into_iter();
            let out: Vec<Value> = enc
                .iter()
                .map(|e| match e {
                    None => Value::Int(-1),
                    Some(_) => Value::Int(i64::from(it.next() == Some(true))),
                })
                .collect();
            return Value::Array(Some(out));
        }
        "FORCE" => match rest {
            [id] => match one_id(id) {
                Some(id) => Request::Force(id),
                None => return err("ERR invalid id"),
            },
            _ => return wrong_args(),
        },
        "POP" => {
            let (n, lease) = match rest {
                [n] => (parse_u64(n), None),
                [n, kw, s] if upper(kw) == "LEASE" => {
                    (parse_u64(n), parse_u64(s).map(Duration::from_secs))
                }
                _ => return wrong_args(),
            };
            let Some(n) = n else {
                return err("ERR value is not an integer");
            };
            if rest.len() == 3 && lease.is_none() {
                return err("ERR value is not an integer");
            }
            Request::Pop {
                n: n as usize,
                lease,
            }
        }
        "TOUCH" => {
            let (id, lease) = match rest {
                [id] => (id, None),
                [id, s] => match parse_u64(s) {
                    Some(s) => (id, Some(Duration::from_secs(s))),
                    None => return err("ERR value is not an integer"),
                },
                _ => return wrong_args(),
            };
            match one_id(id) {
                Some(id) => Request::Touch { id, lease },
                None => return err("ERR invalid id"),
            }
        }
        "COMPLETE" => {
            if rest.is_empty() {
                return wrong_args();
            }
            let ids: Vec<u64> = rest.iter().filter_map(|b| one_id(b)).collect();
            if ids.is_empty() {
                return Value::Int(0);
            }
            Request::Complete(ids)
        }
        "MARK" => {
            let [id, state, reason] = rest else {
                return wrong_args();
            };
            let state: State = match State::parse(text(state)) {
                Some(s) if s.is_terminal() => s,
                _ => return err("ERR bad state"),
            };
            match one_id(id) {
                Some(id) => Request::Mark {
                    id,
                    state,
                    reason: Reason::from_name(text(reason)),
                },
                None => return err("ERR invalid id"),
            }
        }
        "REQUEUE" => {
            let (id, lane) = match rest {
                [id] => (id, LaneId::Main),
                [id, kw] if upper(kw) == "PRIORITY" => (id, LaneId::Priority),
                _ => return wrong_args(),
            };
            match one_id(id) {
                Some(id) => Request::Requeue { id, lane },
                None => return err("ERR invalid id"),
            }
        }
        "STATE" => {
            if rest.is_empty() {
                return wrong_args();
            }
            let enc: Vec<Option<u64>> =
                rest.iter().map(|b| q.codec.encode(text(b)).ok()).collect();
            let ids: Vec<u64> = enc.iter().flatten().copied().collect();
            let states: Vec<Option<State>> = if ids.is_empty() {
                Vec::new()
            } else {
                match q.ops.call(Request::State(ids)).await {
                    Reply::States(v) => v,
                    other => return reply_value(other, q.codec),
                }
            };
            let mut it = states.into_iter();
            let out: Vec<Value> = enc
                .iter()
                .map(|e| match e {
                    None => Value::Bulk(None),
                    Some(_) => match it.next().flatten() {
                        Some(s) => bulk(s.name()),
                        None => Value::Bulk(None),
                    },
                })
                .collect();
            return Value::Array(Some(out));
        }
        "INFO" if rest.is_empty() => Request::Info,
        "SNAPSHOT" if rest.is_empty() => Request::Snapshot,
        "INFO" | "SNAPSHOT" => return wrong_args(),
        _ => return err("ERR unknown command"),
    };
    let r: Reply = q.ops.call(req).await;
    reply_value(r, q.codec)
}

fn reply_value(r: Reply, codec: Codec) -> Value {
    match r {
        Reply::Added(v) => Value::Array(Some(
            v.into_iter().map(|b| Value::Int(i64::from(b))).collect(),
        )),
        Reply::Force(o) => Value::Simple(o.name().into()),
        Reply::Popped(items) => Value::Array(Some(
            items
                .into_iter()
                .map(|p| {
                    Value::Array(Some(vec![
                        bulk(&codec.decode(p.id)),
                        Value::Int(i64::from(p.force)),
                    ]))
                })
                .collect(),
        )),
        Reply::Bool(b) => Value::Int(i64::from(b)),
        Reply::Count(n) => Value::Int(n as i64),
        Reply::Ok => ok(),
        Reply::States(v) => Value::Array(Some(
            v.into_iter()
                .map(|s| match s {
                    Some(s) => bulk(s.name()),
                    None => Value::Bulk(None),
                })
                .collect(),
        )),
        Reply::Info(i) => info_value(&i),
        Reply::Path(p) => bulk(&p),
        Reply::Err(e) => Value::Error(format!("ERR io: {e}")),
    }
}

fn info_value(i: &QueueInfo) -> Value {
    let mut out: Vec<Value> = Vec::new();
    for s in ALL_STATES {
        out.push(bulk(s.name()));
        out.push(Value::Int(i.counts[s as usize] as i64));
    }
    for (k, v) in [
        ("leased", i.leased),
        ("lane_main", i.lane_main),
        ("lane_priority", i.lane_priority),
        ("base_entries", i.base_entries),
        ("delta_entries", i.delta_entries),
        ("generation", i.generation),
    ] {
        out.push(bulk(k));
        out.push(Value::Int(v as i64));
    }
    Value::Array(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Fake {
        seen: Mutex<Vec<Request>>,
    }

    impl QueueOps for Fake {
        fn call(&self, req: Request) -> BoxReply<'_> {
            self.seen.lock().unwrap().push(req.clone());
            let r: Reply = match req {
                Request::Add(ids) => {
                    Reply::Added(ids.iter().map(|i| i % 2 == 0).collect())
                }
                Request::Pop { .. } => Reply::Popped(vec![
                    PopItem {
                        id: 0,
                        force: false,
                    },
                    PopItem {
                        id: u64::MAX,
                        force: true,
                    },
                ]),
                Request::Complete(ids) => Reply::Count(ids.len() as u64),
                Request::Info => Reply::Info(QueueInfo {
                    counts: [1, 2, 3, 4, 5],
                    ..QueueInfo::default()
                }),
                _ => Reply::Ok,
            };
            Box::pin(async move { r })
        }
    }

    fn ctx(password: Option<&str>) -> (Ctx, Arc<Fake>) {
        let fake: Arc<Fake> = Arc::new(Fake::default());
        let mut queues: HashMap<String, QueueEntry> = HashMap::new();
        queues.insert(
            "youtube:video".into(),
            QueueEntry::new(Codec::YoutubeVideo, fake.clone()),
        );
        (
            Ctx {
                queues,
                password: password.map(String::from),
            },
            fake,
        )
    }

    fn a(v: &[&str]) -> Vec<Vec<u8>> {
        v.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    async fn run(c: &Ctx, conn: &mut ConnState, v: &[&str]) -> Value {
        dispatch(&a(v), conn, c).await
    }

    fn bulk(s: &str) -> Value {
        Value::Bulk(Some(s.as_bytes().to_vec()))
    }

    #[tokio::test]
    async fn noauth_before_auth() {
        let (c, _) = ctx(Some("pw"));
        let mut conn: ConnState = ConnState::default();
        let v: Value =
            run(&c, &mut conn, &["BACKLOG.INFO", "youtube:video"]).await;
        assert_eq!(v, Value::Error("NOAUTH Authentication required.".into()));
        assert_eq!(
            run(&c, &mut conn, &["PING"]).await,
            Value::Simple("PONG".into())
        );
        assert_eq!(
            run(&c, &mut conn, &["AUTH", "pw"]).await,
            Value::Simple("OK".into())
        );
        assert!(matches!(
            run(&c, &mut conn, &["BACKLOG.INFO", "youtube:video"]).await,
            Value::Array(_)
        ));
    }

    #[tokio::test]
    async fn auth_wrong_password() {
        let (c, _) = ctx(Some("pw"));
        let mut conn: ConnState = ConnState::default();
        let v: Value = run(&c, &mut conn, &["AUTH", "default", "nope"]).await;
        assert_eq!(
            v,
            Value::Error("WRONGPASS invalid username-password pair".into())
        );
        assert!(!conn.authed);
        let v: Value = run(&c, &mut conn, &["AUTH", "default", "pw"]).await;
        assert_eq!(v, Value::Simple("OK".into()));
        let mut conn3: ConnState = ConnState::default();
        let v: Value =
            run(&c, &mut conn3, &["HELLO", "3", "AUTH", "default", "pw"]).await;
        assert!(matches!(v, Value::Map(_)));
        assert!(conn3.authed);
    }

    #[tokio::test]
    async fn handshake_commands_ok() {
        let (c, _) = ctx(None);
        let mut conn: ConnState = ConnState::default();
        for cmd in [
            &["CLIENT", "SETNAME", "x"][..],
            &["CLIENT", "SETINFO", "LIB-NAME", "redis-py"],
            &["client", "setinfo", "LIB-VER", "8.1.0"],
            &["SELECT", "0"],
        ] {
            assert_eq!(
                run(&c, &mut conn, cmd).await,
                Value::Simple("OK".into()),
                "{cmd:?}"
            );
        }
        assert!(matches!(
            run(&c, &mut conn, &["SELECT", "3"]).await,
            Value::Error(_)
        ));
        assert!(matches!(run(&c, &mut conn, &["HELLO", "4"]).await,
                         Value::Error(e) if e.starts_with("NOPROTO")));
        assert!(matches!(
            run(&c, &mut conn, &["HELLO", "2"]).await,
            Value::Map(_)
        ));
        assert_eq!(conn.proto, 2);
        let Value::Map(m) = run(&c, &mut conn, &["HELLO", "3"]).await else {
            panic!("not a map");
        };
        assert!(m.contains(&(bulk("proto"), Value::Int(3))));
        assert_eq!(conn.proto, 3);
    }

    #[tokio::test]
    async fn add_reports_invalid_per_id() {
        let (c, fake) = ctx(None);
        let mut conn: ConnState = ConnState::default();
        // AAAAAAAAAAA encodes to 0 (even -> added), AAAAAAAAAAE to 1.
        let v: Value = run(
            &c,
            &mut conn,
            &[
                "BACKLOG.ADD",
                "youtube:video",
                "AAAAAAAAAAA",
                "bad",
                "AAAAAAAAAAE",
            ],
        )
        .await;
        assert_eq!(
            v,
            Value::Array(Some(vec![
                Value::Int(1),
                Value::Int(-1),
                Value::Int(0),
            ]))
        );
        assert_eq!(fake.seen.lock().unwrap()[0], Request::Add(vec![0, 1]));
        let e: &QueueEntry = &c.queues["youtube:video"];
        assert_eq!(e.invalid_ids.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn pop_reply_shape() {
        let (c, fake) = ctx(None);
        let mut conn: ConnState = ConnState::default();
        let v: Value = run(
            &c,
            &mut conn,
            &["backlog.pop", "youtube:video", "2", "LEASE", "90"],
        )
        .await;
        assert_eq!(
            v,
            Value::Array(Some(vec![
                Value::Array(Some(vec![bulk("AAAAAAAAAAA"), Value::Int(0)])),
                Value::Array(Some(vec![bulk("__________8"), Value::Int(1)])),
            ]))
        );
        assert_eq!(
            fake.seen.lock().unwrap()[0],
            Request::Pop {
                n: 2,
                lease: Some(Duration::from_secs(90)),
            }
        );
    }

    #[tokio::test]
    async fn mark_and_requeue_parse() {
        let (c, fake) = ctx(None);
        let mut conn: ConnState = ConnState::default();
        let v: Value = run(
            &c,
            &mut conn,
            &[
                "BACKLOG.MARK",
                "youtube:video",
                "AAAAAAAAAAE",
                "failed",
                "weird_reason",
            ],
        )
        .await;
        assert_eq!(v, Value::Simple("OK".into()));
        let v: Value = run(
            &c,
            &mut conn,
            &[
                "BACKLOG.MARK",
                "youtube:video",
                "AAAAAAAAAAE",
                "scraped",
                "x",
            ],
        )
        .await;
        assert_eq!(v, Value::Error("ERR bad state".into()));
        run(
            &c,
            &mut conn,
            &[
                "BACKLOG.REQUEUE",
                "youtube:video",
                "AAAAAAAAAAE",
                "PRIORITY",
            ],
        )
        .await;
        let seen = fake.seen.lock().unwrap();
        assert_eq!(
            seen[0],
            Request::Mark {
                id: 1,
                state: State::Failed,
                reason: Reason::Other,
            }
        );
        assert_eq!(
            seen[1],
            Request::Requeue {
                id: 1,
                lane: LaneId::Priority
            }
        );
    }

    #[tokio::test]
    async fn info_reply_pairs() {
        let (c, _) = ctx(None);
        let mut conn: ConnState = ConnState::default();
        let Value::Array(Some(v)) =
            run(&c, &mut conn, &["BACKLOG.INFO", "youtube:video"]).await
        else {
            panic!("not an array");
        };
        assert_eq!(
            &v[..4],
            &[
                bulk("queued"),
                Value::Int(1),
                bulk("scraped"),
                Value::Int(2)
            ]
        );
        assert_eq!(v.len(), 22);
    }

    #[tokio::test]
    async fn unknown_queue_error() {
        let (c, _) = ctx(None);
        let mut conn: ConnState = ConnState::default();
        assert_eq!(
            run(&c, &mut conn, &["BACKLOG.INFO", "tiktok:video"]).await,
            Value::Error("ERR unknown queue".into())
        );
        assert_eq!(
            run(&c, &mut conn, &["BACKLOG.ADD", "youtube:video"]).await,
            Value::Error("ERR wrong number of arguments".into())
        );
        assert_eq!(
            run(&c, &mut conn, &["FLUSHALL"]).await,
            Value::Error("ERR unknown command".into())
        );
    }
}
