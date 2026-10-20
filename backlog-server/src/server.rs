//! TCP listener, one owner task per queue, periodic ticks, metrics
//! endpoint and graceful shutdown.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{error, info, warn};

use crate::base::BaseInfo;
use crate::commands::{
    BoxReply, ConnState, Ctx, QueueEntry, QueueOps, Reply, Request, dispatch,
};
use crate::compaction;
use crate::config::ServeConfig;
use crate::fsutil::check_data_dir;
use crate::metrics::{self, QueueMetrics};
use crate::queue::{Queue, QueueError, QueueParams};
use crate::resp::{self, Value};

const CHANNEL_BOUND: usize = 1024;
const LEASE_TICK: Duration = Duration::from_secs(10);
const COMPACTION_TICK: Duration = Duration::from_secs(1);
/// Upper bound on one POP batch.
const MAX_POP: usize = 10_000;

type Envelope = (Request, oneshot::Sender<Reply>);

/// Client side of a queue's owner task.
pub struct QueueHandle {
    tx: mpsc::Sender<Envelope>,
}

impl QueueOps for QueueHandle {
    fn call(&self, req: Request) -> BoxReply<'_> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            if self.tx.send((req, tx)).await.is_err() {
                return Reply::Err("queue stopped".into());
            }
            rx.await
                .unwrap_or_else(|_| Reply::Err("queue stopped".into()))
        })
    }
}

/// A started server.
pub struct Running {
    pub addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    stop: watch::Sender<bool>,
    join: JoinHandle<Result<()>>,
}

impl Running {
    /// Stop accepting, let every owner checkpoint, and wait.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.stop.send(true);
        self.join.await.context("server task panicked")?
    }
}

fn unix_now() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0)
}

fn unix_now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Run until SIGTERM or SIGINT, then shut down cleanly.
pub async fn run(cfg: ServeConfig) -> Result<()> {
    let r: Running = start(cfg).await?;
    shutdown_signal().await;
    info!("shutdown requested");
    r.shutdown().await
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    )
    .expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

/// Open every queue, bind both listeners and start serving.
pub async fn start(cfg: ServeConfig) -> Result<Running> {
    let declared = cfg.validate()?;
    check_data_dir(&cfg.data_dir)?;
    let (stop, stop_rx) = watch::channel(false);
    let mut queues: HashMap<String, QueueEntry> = HashMap::new();
    let mut all_metrics: Vec<Arc<QueueMetrics>> = Vec::new();
    let mut owners: Vec<JoinHandle<()>> = Vec::new();
    for (name, codec) in declared {
        let dir: PathBuf = cfg.queue_dir(&name);
        let q: Queue = Queue::open(&dir, cfg.queue_params())
            .with_context(|| format!("opening queue {name}"))?;
        let i = q.info();
        info!(
            queue = %name,
            replayed = q.replayed(),
            generation = i.generation,
            base_entries = i.base_entries,
            queued = i.counts[0],
            "queue opened"
        );
        let m: Arc<QueueMetrics> = Arc::new(QueueMetrics::new(&name));
        m.set_info(i);
        m.refresh_disk(&dir);
        let (tx, rx) = mpsc::channel::<Envelope>(CHANNEL_BOUND);
        let snapshot_root: PathBuf = cfg.data_dir.join("snapshots");
        owners.push(tokio::spawn(owner(
            name.clone(),
            q,
            rx,
            m.clone(),
            OwnerTimers::from(&cfg),
            snapshot_root,
            stop_rx.clone(),
        )));
        let mut entry: QueueEntry =
            QueueEntry::new(codec, Arc::new(QueueHandle { tx }));
        entry.invalid_ids = m.invalid_ids.clone();
        queues.insert(name, entry);
        all_metrics.push(m);
    }
    let ctx: Arc<Ctx> = Arc::new(Ctx {
        queues,
        password: cfg.password.clone(),
    });
    let listener: TcpListener = TcpListener::bind(cfg.listen)
        .await
        .with_context(|| format!("binding {}", cfg.listen))?;
    let mlistener: TcpListener = TcpListener::bind(cfg.metrics_listen)
        .await
        .with_context(|| format!("binding {}", cfg.metrics_listen))?;
    let addr: SocketAddr = listener.local_addr()?;
    let metrics_addr: SocketAddr = mlistener.local_addr()?;
    info!(%addr, %metrics_addr, "listening");
    let accept: JoinHandle<()> =
        tokio::spawn(accept_loop(listener, ctx, stop_rx.clone()));
    let mserve: JoinHandle<()> =
        tokio::spawn(metrics_loop(mlistener, all_metrics, stop_rx.clone()));
    let mut wait = stop_rx;
    let join: JoinHandle<Result<()>> = tokio::spawn(async move {
        let _ = wait.wait_for(|s| *s).await;
        let _ = accept.await;
        let _ = mserve.await;
        for o in owners {
            o.await.context("queue owner panicked")?;
        }
        info!("stopped");
        Ok(())
    });
    Ok(Running {
        addr,
        metrics_addr,
        stop,
        join,
    })
}

#[derive(Clone, Copy)]
struct OwnerTimers {
    fsync: Duration,
    checkpoint: Duration,
}

impl From<&ServeConfig> for OwnerTimers {
    fn from(c: &ServeConfig) -> OwnerTimers {
        OwnerTimers {
            fsync: Duration::from_millis(c.fsync_interval_ms),
            checkpoint: Duration::from_secs(c.checkpoint_interval_secs),
        }
    }
}

fn op_name(r: &Request) -> &'static str {
    match r {
        Request::Add(_) => "ADD",
        Request::Force(_) => "FORCE",
        Request::Pop { .. } => "POP",
        Request::Touch { .. } => "TOUCH",
        Request::Complete(_) => "COMPLETE",
        Request::Mark { .. } => "MARK",
        Request::Requeue { .. } => "REQUEUE",
        Request::State(_) => "STATE",
        Request::Info => "INFO",
        Request::Snapshot => "SNAPSHOT",
    }
}

fn handle(
    q: &mut Queue,
    req: Request,
    params: &QueueParams,
    snapshot_root: &std::path::Path,
) -> Result<Reply, QueueError> {
    let ts: u32 = unix_now();
    Ok(match req {
        Request::Add(ids) => {
            let mut out: Vec<bool> = Vec::with_capacity(ids.len());
            for id in ids {
                out.push(q.add(id, ts)?);
            }
            Reply::Added(out)
        }
        Request::Force(id) => Reply::Force(q.force(id, ts)?),
        Request::Pop { n, lease } => Reply::Popped(q.pop(
            n.min(MAX_POP),
            lease.unwrap_or(params.lease_default),
            Instant::now(),
        )?),
        Request::Touch { id, lease } => Reply::Bool(q.touch(
            id,
            lease.unwrap_or(params.lease_default),
            Instant::now(),
        )),
        Request::Complete(ids) => {
            let n: u64 = ids.len() as u64;
            for id in ids {
                q.complete(id, ts)?;
            }
            Reply::Count(n)
        }
        Request::Mark { id, state, reason } => {
            q.mark(id, state, reason, ts)?;
            Reply::Ok
        }
        Request::Requeue { id, lane } => Reply::Bool(q.requeue(id, lane, ts)?),
        Request::State(ids) => {
            Reply::States(ids.into_iter().map(|id| q.state(id)).collect())
        }
        Request::Info => Reply::Info(q.info()),
        Request::Snapshot => {
            Reply::Path(q.snapshot(snapshot_root)?.display().to_string())
        }
    })
}

type CompactionTask = JoinHandle<(std::io::Result<BaseInfo>, Instant)>;

async fn owner(
    name: String,
    mut q: Queue,
    mut rx: mpsc::Receiver<Envelope>,
    m: Arc<QueueMetrics>,
    t: OwnerTimers,
    snapshot_root: PathBuf,
    mut stop: watch::Receiver<bool>,
) {
    let params: QueueParams = *q.params();
    let mut fsync_tick = interval(t.fsync);
    let mut ckpt_tick = interval(t.checkpoint);
    let mut lease_tick = interval(LEASE_TICK);
    let mut comp_tick = interval(COMPACTION_TICK);
    for tk in [
        &mut fsync_tick,
        &mut ckpt_tick,
        &mut lease_tick,
        &mut comp_tick,
    ] {
        tk.set_missed_tick_behavior(MissedTickBehavior::Delay);
    }
    let mut compaction: Option<CompactionTask> = None;
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some((req, reply_tx)) = msg else { break };
                let op: &'static str = op_name(&req);
                let started: Instant = Instant::now();
                let reply: Reply =
                    match handle(&mut q, req, &params, &snapshot_root) {
                        Ok(r) => r,
                        Err(e) => {
                            error!(queue = %name, op, error = %e,
                                   "command failed");
                            Reply::Err(e.to_string())
                        }
                    };
                let ok: bool = !matches!(reply, Reply::Err(_));
                m.op(op, ok, started.elapsed().as_secs_f64());
                m.set_info(q.info());
                let _ = reply_tx.send(reply);
            }
            _ = fsync_tick.tick() => {
                let started: Instant = Instant::now();
                match q.sync() {
                    Ok(()) => m.fsync(started.elapsed().as_secs_f64()),
                    Err(e) => error!(queue = %name, error = %e, "fsync failed"),
                }
            }
            _ = ckpt_tick.tick() => {
                if let Err(e) = q.write_checkpoint() {
                    error!(queue = %name, error = %e, "checkpoint failed");
                }
                m.refresh_disk(q.dir());
                m.set_info(q.info());
            }
            _ = lease_tick.tick() => {
                match q.expire_leases(Instant::now()) {
                    Ok(0) => {}
                    Ok(n) => {
                        warn!(queue = %name, expired = n, "leases expired");
                        m.leases_expired(n as u64);
                    }
                    Err(e) => error!(queue = %name, error = %e,
                                     "lease expiry failed"),
                }
            }
            _ = comp_tick.tick(), if compaction.is_none() => {
                if q.should_compact(Instant::now())
                    && let Some(job) = q.start_compaction()
                {
                    info!(queue = %name, generation = job.generation,
                          "compaction started");
                    let started: Instant = Instant::now();
                    compaction = Some(tokio::task::spawn_blocking(move || {
                        (compaction::run(&job), started)
                    }));
                }
            }
            done = async { compaction.as_mut().expect("guarded").await },
                if compaction.is_some() => {
                compaction = None;
                finish(&name, &mut q, &m, done);
            }
            _ = stop.wait_for(|s| *s) => break,
        }
    }
    if let Some(task) = compaction.take() {
        let done = task.await;
        finish(&name, &mut q, &m, done);
    }
    match q.write_checkpoint() {
        Ok(()) => info!(queue = %name, "checkpoint written at shutdown"),
        Err(e) => error!(queue = %name, error = %e, "final checkpoint failed"),
    }
}

fn finish(
    name: &str,
    q: &mut Queue,
    m: &QueueMetrics,
    done: Result<(std::io::Result<BaseInfo>, Instant), tokio::task::JoinError>,
) {
    match done {
        Ok((Ok(info), started)) => match q.finish_compaction(info) {
            Ok(()) => {
                let secs: f64 = started.elapsed().as_secs_f64();
                m.compaction(secs, unix_now_f64());
                m.set_info(q.info());
                info!(queue = %name, generation = info.generation,
                      entries = info.count, seconds = secs,
                      "compaction finished");
            }
            Err(e) => {
                error!(queue = %name, error = %e, "installing compaction");
                q.abort_compaction(Instant::now());
            }
        },
        Ok((Err(e), _)) => {
            error!(queue = %name, error = %e, "compaction failed");
            q.abort_compaction(Instant::now());
        }
        Err(e) => {
            error!(queue = %name, error = %e, "compaction panicked");
            q.abort_compaction(Instant::now());
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    ctx: Arc<Ctx>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            res = listener.accept() => match res {
                Ok((stream, _)) => {
                    let ctx: Arc<Ctx> = ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = serve_conn(stream, ctx).await {
                            warn!(error = %e, "connection error");
                        }
                    });
                }
                Err(e) => warn!(error = %e, "accept failed"),
            },
            _ = stop.wait_for(|s| *s) => return,
        }
    }
}

async fn serve_conn(mut stream: TcpStream, ctx: Arc<Ctx>) -> Result<()> {
    let _ = stream.set_nodelay(true);
    let mut buf: BytesMut = BytesMut::with_capacity(16 * 1024);
    let mut out: Vec<u8> = Vec::new();
    let mut conn: ConnState = ConnState::default();
    loop {
        loop {
            match resp::parse(&mut buf) {
                Ok(Some(args)) => {
                    if args.is_empty() {
                        continue;
                    }
                    let quit: bool = args[0].eq_ignore_ascii_case(b"QUIT");
                    let v: Value = dispatch(&args, &mut conn, &ctx).await;
                    resp::encode_proto(&v, &mut out, conn.proto);
                    if quit {
                        stream.write_all(&out).await?;
                        return Ok(());
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let v: Value = Value::Error(format!("ERR {e}"));
                    resp::encode(&v, &mut out);
                    stream.write_all(&out).await?;
                    return Ok(());
                }
            }
        }
        if !out.is_empty() {
            stream.write_all(&out).await?;
            out.clear();
        }
        if stream.read_buf(&mut buf).await? == 0 {
            return Ok(());
        }
    }
}

async fn metrics_loop(
    listener: TcpListener,
    queues: Vec<Arc<QueueMetrics>>,
    mut stop: watch::Receiver<bool>,
) {
    let queues: Arc<Vec<Arc<QueueMetrics>>> = Arc::new(queues);
    loop {
        tokio::select! {
            res = listener.accept() => {
                let Ok((stream, _)) = res else { continue };
                let queues = queues.clone();
                tokio::spawn(async move {
                    let _ = serve_metrics(stream, &queues).await;
                });
            }
            _ = stop.wait_for(|s| *s) => return,
        }
    }
}

async fn serve_metrics(
    mut s: TcpStream,
    queues: &[Arc<QueueMetrics>],
) -> std::io::Result<()> {
    let mut req: Vec<u8> = Vec::new();
    let mut chunk: [u8; 1024] = [0; 1024];
    while !req.windows(4).any(|w| w == b"\r\n\r\n") && req.len() < 8192 {
        let n: usize = s.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        req.extend_from_slice(&chunk[..n]);
    }
    let first: String = String::from_utf8_lossy(&req)
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    let path: &str = first.split_whitespace().nth(1).unwrap_or("");
    let (status, body): (&str, String) =
        if first.starts_with("GET ") && path == "/metrics" {
            ("200 OK", metrics::render(queues))
        } else {
            ("404 Not Found", "not found\n".to_string())
        };
    let head: String = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).await?;
    s.write_all(body.as_bytes()).await?;
    s.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn test_cfg(dir: &std::path::Path) -> ServeConfig {
        let mut c: ServeConfig = crate::config::tests::cfg(&[
            "--listen",
            "127.0.0.1:0",
            "--metrics-listen",
            "127.0.0.1:0",
            "--fsync-interval-ms",
            "50",
        ]);
        c.data_dir = dir.to_path_buf();
        c
    }

    async fn roundtrip(s: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
        s.write_all(cmd).await.unwrap();
        let mut buf: Vec<u8> = vec![0; 4096];
        let n: usize = s.read(&mut buf).await.unwrap();
        buf.truncate(n);
        buf
    }

    async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut s: TcpStream = TcpStream::connect(addr).await.unwrap();
        let req: String = format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n");
        s.write_all(req.as_bytes()).await.unwrap();
        let mut out: String = String::new();
        s.read_to_string(&mut out).await.unwrap();
        out
    }

    #[tokio::test]
    async fn serve_roundtrip_over_tcp() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let r: Running = start(test_cfg(d.path())).await.unwrap();
        let mut s: TcpStream = TcpStream::connect(r.addr).await.unwrap();
        let add: &[u8] =
            b"*3\r\n$11\r\nBACKLOG.ADD\r\n$13\r\nyoutube:video\r\n\
              $11\r\ndQw4w9WgXcQ\r\n";
        assert_eq!(roundtrip(&mut s, add).await, b"*1\r\n:1\r\n");
        let pop: &[u8] =
            b"*3\r\n$11\r\nBACKLOG.POP\r\n$13\r\nyoutube:video\r\n$1\r\n5\r\n";
        assert_eq!(
            roundtrip(&mut s, pop).await,
            b"*1\r\n*2\r\n$11\r\ndQw4w9WgXcQ\r\n:0\r\n".to_vec()
        );
        let m: String = http_get(r.metrics_addr, "/metrics").await;
        assert!(m.starts_with("HTTP/1.1 200"), "{m}");
        assert!(
            m.contains(
                "backlog_items{queue=\"youtube:video\",state=\"queued\"} 1"
            ),
            "{m}"
        );
        assert!(
            m.contains(
                "backlog_items{queue=\"youtube:video\",state=\"leased\"} 1"
            ),
            "{m}"
        );
        assert!(http_get(r.metrics_addr, "/nope").await.contains("404"));
        drop(s);
        r.shutdown().await.unwrap();

        let r: Running = start(test_cfg(d.path())).await.unwrap();
        let mut s: TcpStream = TcpStream::connect(r.addr).await.unwrap();
        let st: &[u8] = b"BACKLOG.STATE youtube:video dQw4w9WgXcQ\r\n";
        assert_eq!(
            roundtrip(&mut s, st).await,
            b"*1\r\n$6\r\nqueued\r\n".to_vec()
        );
        drop(s);
        r.shutdown().await.unwrap();
    }
}
