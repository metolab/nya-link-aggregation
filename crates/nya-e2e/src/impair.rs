//! Userspace impairment proxy.
//!
//! Each connection is sliced into MSS packets on a virtual WAN:
//! delay/jitter per packet, independent drops, RTO retransmit, cwnd backoff.
//! That is IP-loss + TCP-retransmit behaviour (userspace). This host has no
//! CAP_NET_ADMIN so we cannot attach `tc netem` to Linux TCP.
//! Blackhole holds packets without ACK (RTO storms until the hole lifts).
//! Disconnect RSTs current TCP sessions.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::Rng;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

#[derive(Clone, Debug)]
pub struct ImpairConfig {
    /// Target path RTT. Each direction delays RTT/2.
    pub rtt: Duration,
    pub jitter: Duration,
    /// 0.0–1.0. A chunk is extra-stalled (RTO-like) with this probability.
    pub loss: f64,
    pub blackhole: bool,
}

impl Default for ImpairConfig {
    fn default() -> Self {
        Self {
            rtt: Duration::from_millis(10),
            jitter: Duration::ZERO,
            loss: 0.0,
            blackhole: false,
        }
    }
}

pub(crate) struct ImpairInner {
    pub rtt_us: AtomicU64,
    pub jitter_us: AtomicU64,
    extra_us: AtomicU64,
    pub loss_ppm: AtomicU64,
    pub blackhole: AtomicBool,
    pub drop_all: AtomicBool,
    stop: tokio::sync::watch::Sender<bool>,
    pub wake: Notify,
    pub bytes_fwd: AtomicU64,
    pub bytes_rev: AtomicU64,
    pub retrans: AtomicU64,
    pub conns: AtomicU64,
    pub drops: AtomicU64,
    kills: Mutex<Vec<tokio::sync::watch::Sender<bool>>>,
    conns_ctrl: Mutex<Vec<Arc<ConnCtrl>>>,
}

/// Per-TCP-connection knobs. One link can host several overlay connections.
pub(crate) struct ConnCtrl {
    pub blackhole: AtomicBool,
    /// Pause reading client→server so the real TCP send buffer / overlay writer fills.
    pub stall: AtomicBool,
    pub alive: AtomicBool,
    kill: tokio::sync::watch::Sender<bool>,
}

impl ImpairInner {
    pub(crate) fn one_way(&self) -> Duration {
        let us = self.rtt_us.load(Ordering::Relaxed) / 2
            + self.extra_us.load(Ordering::Relaxed) / 2
            + jitter_us(self.jitter_us.load(Ordering::Relaxed));
        Duration::from_micros(us)
    }

    fn loss_p(&self) -> f64 {
        self.loss_ppm.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
}

fn jitter_us(max: u64) -> u64 {
    if max == 0 {
        0
    } else {
        rand::thread_rng().gen_range(0..=max)
    }
}

#[derive(Clone)]
pub struct LinkHandle {
    pub name: String,
    pub listen: SocketAddr,
    inner: Arc<ImpairInner>,
}

#[derive(Clone, Debug)]
pub struct LinkStats {
    pub name: String,
    pub listen: SocketAddr,
    pub rtt: Duration,
    pub jitter: Duration,
    pub loss: f64,
    pub blackhole: bool,
    pub bytes_fwd: u64,
    pub bytes_rev: u64,
    pub conns: u64,
    pub drops: u64,
    /// WAN-level retransmits (packet loss recovery).
    pub retrans: u64,
}

impl LinkHandle {
    pub fn stats(&self) -> LinkStats {
        LinkStats {
            name: self.name.clone(),
            listen: self.listen,
            rtt: Duration::from_micros(self.inner.rtt_us.load(Ordering::Relaxed)),
            jitter: Duration::from_micros(self.inner.jitter_us.load(Ordering::Relaxed)),
            loss: self.inner.loss_p(),
            blackhole: self.inner.blackhole.load(Ordering::Relaxed),
            bytes_fwd: self.inner.bytes_fwd.load(Ordering::Relaxed),
            bytes_rev: self.inner.bytes_rev.load(Ordering::Relaxed),
            conns: self.inner.conns.load(Ordering::Relaxed),
            drops: self.inner.drops.load(Ordering::Relaxed),
            retrans: self.inner.retrans.load(Ordering::Relaxed),
        }
    }

    pub fn set_rtt(&self, rtt: Duration) {
        self.inner
            .rtt_us
            .store(rtt.as_micros() as u64, Ordering::Relaxed);
        self.inner.wake.notify_waiters();
    }

    pub fn set_jitter(&self, j: Duration) {
        self.inner
            .jitter_us
            .store(j.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn set_loss(&self, p: f64) {
        let ppm = (p.clamp(0.0, 1.0) * 1_000_000.0) as u64;
        self.inner.loss_ppm.store(ppm, Ordering::Relaxed);
    }

    pub fn set_blackhole(&self, on: bool) {
        self.inner.blackhole.store(on, Ordering::Relaxed);
        self.inner.wake.notify_waiters();
        info!(link = %self.name, on, "blackhole");
    }

    pub fn set_extra(&self, extra: Duration) {
        self.inner
            .extra_us
            .store(extra.as_micros() as u64, Ordering::Relaxed);
        self.inner.wake.notify_waiters();
    }

    /// Temporarily add `extra` RTT for `hold`, then restore.
    pub fn spike(&self, extra: Duration, hold: Duration) {
        let inner = self.inner.clone();
        let name = self.name.clone();
        inner
            .extra_us
            .store(extra.as_micros() as u64, Ordering::Relaxed);
        inner.wake.notify_waiters();
        info!(link = %name, ?extra, ?hold, "spike start");
        tokio::spawn(async move {
            tokio::time::sleep(hold).await;
            inner.extra_us.store(0, Ordering::Relaxed);
            inner.wake.notify_waiters();
            info!(link = %name, "spike end");
        });
    }

    /// RST/close all current TCP sessions on this link.
    pub fn disconnect_all(&self) {
        let mut g = self.inner.kills.lock().unwrap();
        let n = g.len();
        for tx in g.drain(..) {
            let _ = tx.send(true);
        }
        self.inner.drops.fetch_add(n as u64, Ordering::Relaxed);
        info!(link = %self.name, n, "disconnect_all");
    }

    fn live_conns(&self) -> Vec<Arc<ConnCtrl>> {
        self.inner
            .conns_ctrl
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.alive.load(Ordering::Relaxed))
            .cloned()
            .collect()
    }

    pub fn live_conn_count(&self) -> usize {
        self.live_conns().len()
    }

    /// Blackhole a single overlay TCP connection (0-based among live conns).
    pub fn set_conn_blackhole(&self, idx: usize, on: bool) {
        let live = self.live_conns();
        if let Some(c) = live.get(idx) {
            c.blackhole.store(on, Ordering::Relaxed);
            self.inner.wake.notify_waiters();
            info!(link = %self.name, idx, on, nlive = live.len(), "conn blackhole");
        }
    }

    /// Stall client→server reads on one connection (send-buffer / HOL).
    pub fn set_conn_stall(&self, idx: usize, on: bool) {
        let live = self.live_conns();
        if let Some(c) = live.get(idx) {
            c.stall.store(on, Ordering::Relaxed);
            self.inner.wake.notify_waiters();
            info!(link = %self.name, idx, on, nlive = live.len(), "conn stall");
        }
    }

    pub fn clear_conn_faults(&self) {
        let all = self.inner.conns_ctrl.lock().unwrap();
        for c in all.iter() {
            c.blackhole.store(false, Ordering::Relaxed);
            c.stall.store(false, Ordering::Relaxed);
        }
        self.inner.wake.notify_waiters();
    }

    /// Stop accept + WAN pipes. Clone-safe (stop lives on ImpairInner).
    pub fn shutdown(&self) {
        self.inner.drop_all.store(true, Ordering::SeqCst);
        let _ = self.inner.stop.send(true);
        self.inner.wake.notify_waiters();
        self.disconnect_all();
    }

    /// RST a single overlay TCP connection.
    pub fn disconnect_conn(&self, idx: usize) {
        let live = self.live_conns();
        if let Some(c) = live.get(idx) {
            c.alive.store(false, Ordering::Relaxed);
            let _ = c.kill.send(true);
            self.inner.drops.fetch_add(1, Ordering::Relaxed);
            info!(link = %self.name, idx, "disconnect_conn");
        }
    }
}

pub async fn spawn_link(
    name: String,
    backend: SocketAddr,
    cfg: ImpairConfig,
) -> anyhow::Result<LinkHandle> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen = listener.local_addr()?;
    let (stop_tx, _) = tokio::sync::watch::channel(false);
    let inner = Arc::new(ImpairInner {
        rtt_us: AtomicU64::new(cfg.rtt.as_micros() as u64),
        jitter_us: AtomicU64::new(cfg.jitter.as_micros() as u64),
        extra_us: AtomicU64::new(0),
        loss_ppm: AtomicU64::new((cfg.loss.clamp(0.0, 1.0) * 1_000_000.0) as u64),
        blackhole: AtomicBool::new(cfg.blackhole),
        drop_all: AtomicBool::new(false),
        stop: stop_tx,
        wake: Notify::new(),
        bytes_fwd: AtomicU64::new(0),
        bytes_rev: AtomicU64::new(0),
        retrans: AtomicU64::new(0),
        conns: AtomicU64::new(0),
        drops: AtomicU64::new(0),
        kills: Mutex::new(Vec::new()),
        conns_ctrl: Mutex::new(Vec::new()),
    });
    let handle = LinkHandle {
        name: name.clone(),
        listen,
        inner: inner.clone(),
    };
    tokio::spawn(async move {
        let mut stop = inner.stop.subscribe();
        loop {
            tokio::select! {
                _ = stop.wait_for(|v| *v) => break,
                acc = listener.accept() => {
                    match acc {
                        Ok((down, peer)) => {
                            let inner = inner.clone();
                            let backend = backend;
                            let name = name.clone();
                            tokio::spawn(async move {
                                if let Err(e) = serve_conn(down, peer, backend, inner, &name).await {
                                    debug!(link = %name, %peer, error = %e, "impair conn end");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(link = %name, error = %e, "impair accept");
                            break;
                        }
                    }
                }
            }
        }
    });
    info!(link = %handle.name, %listen, %backend, "impair listening");
    Ok(handle)
}

async fn serve_conn(
    down: TcpStream,
    peer: SocketAddr,
    backend: SocketAddr,
    inner: Arc<ImpairInner>,
    name: &str,
) -> anyhow::Result<()> {
    let _ = down.set_nodelay(true);
    let up = TcpStream::connect(backend).await?;
    let _ = up.set_nodelay(true);
    inner.conns.fetch_add(1, Ordering::Relaxed);
    let (kill_tx, mut kill_rx) = tokio::sync::watch::channel(false);
    inner.kills.lock().unwrap().push(kill_tx.clone());
    let conn = Arc::new(ConnCtrl {
        blackhole: AtomicBool::new(false),
        stall: AtomicBool::new(false),
        alive: AtomicBool::new(true),
        kill: kill_tx,
    });
    inner.conns_ctrl.lock().unwrap().push(conn.clone());

    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = up.into_split();
    let i1 = inner.clone();
    let i2 = inner.clone();
    let c1 = conn.clone();
    let c2 = conn.clone();
    let fwd = crate::packet_wan::wan_pipe(&mut dr, &mut uw, i1, c1, true);
    let rev = crate::packet_wan::wan_pipe(&mut ur, &mut dw, i2, c2, false);

    tokio::select! {
        r = fwd => { let _ = r; }
        r = rev => { let _ = r; }
        _ = kill_rx.wait_for(|v| *v) => {}
    }
    conn.alive.store(false, Ordering::Relaxed);
    debug!(link = %name, %peer, "impair conn closed");
    Ok(())
}
