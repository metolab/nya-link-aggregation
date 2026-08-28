//! 15-minute mixed-fault suite for typical production mixes.
//!
//! Topology: 3 or 5 same-class peer links, optionally plus 1–2 slightly
//! slower but mostly-stable links. Workload: matching 3/5 ping streams,
//! plus one bulk stream per slow link.
//!
//! Cases in [`suite`] run concurrently so one wall-clock window covers
//! several operating points.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use tracing::info;

use crate::harness::{start, Harness, HarnessSpec};
use crate::impair::{ImpairConfig, LinkHandle};
use crate::report::{fmt_dur, ScenarioReport, Sla};
use crate::workload::{bulk_echo, ping_for, WorkloadStats};
use nya_core::SessionSnapshot;

const PING: Duration = Duration::from_millis(40);
const PING_TO_FLOOR: Duration = Duration::from_millis(1500);
const WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Kind {
    FlashDisconnect,
    Offline,
    Blackhole,
    DelaySpike,
    DelayShift,
    LossBurst,
    JitterBurst,
    ConnBlackhole,
    ConnStall,
    ConnDisconnect,
}

impl Kind {
    fn all() -> &'static [Kind] {
        &[
            Kind::FlashDisconnect,
            Kind::Offline,
            Kind::Blackhole,
            Kind::DelaySpike,
            Kind::DelayShift,
            Kind::LossBurst,
            Kind::JitterBurst,
            Kind::ConnBlackhole,
            Kind::ConnStall,
            Kind::ConnDisconnect,
        ]
    }

    fn mild() -> &'static [Kind] {
        &[Kind::DelaySpike, Kind::JitterBurst, Kind::LossBurst]
    }

    fn name(self) -> &'static str {
        match self {
            Kind::FlashDisconnect => "flash_disconnect",
            Kind::Offline => "offline",
            Kind::Blackhole => "blackhole",
            Kind::DelaySpike => "delay_spike",
            Kind::DelayShift => "delay_shift",
            Kind::LossBurst => "loss_burst",
            Kind::JitterBurst => "jitter_burst",
            Kind::ConnBlackhole => "conn_blackhole",
            Kind::ConnStall => "conn_stall",
            Kind::ConnDisconnect => "conn_disconnect",
        }
    }

    fn hard_down(self) -> bool {
        matches!(self, Kind::Offline | Kind::Blackhole)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Same-class peers; take the full independent fault mix.
    Peer,
    /// Slightly slower, mostly stable; mild noise only.
    Slow,
}

#[derive(Clone, Copy, Debug)]
pub struct LinkSpec {
    pub name: &'static str,
    pub rtt_ms: u64,
    pub jitter_ms: u64,
    pub loss: f64,
    pub role: Role,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MixBand {
    /// 11–16ms peers (lab / metro).
    Near,
    /// 60–100ms peers (regional / cellular).
    Mid,
    /// 120–150ms peers (cross-region).
    High,
    /// 160–200ms peers (long-haul / sat-like terrestrial).
    Far,
}

impl MixBand {
    pub fn all() -> &'static [MixBand] {
        &[MixBand::Near, MixBand::Mid, MixBand::High, MixBand::Far]
    }

    pub fn parse(s: &str) -> Option<MixBand> {
        match s.trim().to_ascii_lowercase().as_str() {
            "near" | "lab" => Some(MixBand::Near),
            "mid" | "60" | "60-100" => Some(MixBand::Mid),
            "high" | "120" | "120-150" => Some(MixBand::High),
            "far" | "160" | "160-200" => Some(MixBand::Far),
            _ => None,
        }
    }

    pub fn tag(self) -> &'static str {
        match self {
            MixBand::Near => "near",
            MixBand::Mid => "mid",
            MixBand::High => "high",
            MixBand::Far => "far",
        }
    }

    pub fn suite(self) -> Vec<MixCase> {
        match self {
            MixBand::Near => vec![peer3(), peer5(), peer3_slow1(), peer5_slow2()],
            MixBand::Mid => mid_suite(),
            MixBand::High => high_suite(),
            MixBand::Far => far_suite(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MixCase {
    pub name: &'static str,
    pub links: Vec<LinkSpec>,
    pub ping_streams: usize,
    pub bulk_streams: usize,
    pub connections: u32,
    pub p99_ms: u64,
    pub p50_ms: u64,
    pub min_success: f64,
    pub max_hard: u32,
}

impl MixCase {
    fn spec(&self) -> HarnessSpec {
        HarnessSpec {
            link_cfgs: self
                .links
                .iter()
                .map(|l| {
                    (
                        l.name.to_string(),
                        ImpairConfig {
                            rtt: Duration::from_millis(l.rtt_ms),
                            jitter: Duration::from_millis(l.jitter_ms),
                            loss: l.loss,
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            connections: self.connections,
            psk: "e2e-psk".into(),
        }
    }

    fn peers(&self) -> impl Iterator<Item = &LinkSpec> {
        self.links.iter().filter(|l| l.role == Role::Peer)
    }

    fn has_slow(&self) -> bool {
        self.links.iter().any(|l| l.role == Role::Slow)
    }

    fn ping_timeout(&self) -> Duration {
        let max_rtt = self.links.iter().map(|l| l.rtt_ms).max().unwrap_or(20);
        Duration::from_millis((max_rtt * 12).max(PING_TO_FLOOR.as_millis() as u64))
    }
}

fn peer(name: &'static str, rtt_ms: u64, jitter_ms: u64, loss: f64) -> LinkSpec {
    LinkSpec {
        name,
        rtt_ms,
        jitter_ms,
        loss,
        role: Role::Peer,
    }
}

fn slow(name: &'static str, rtt_ms: u64, jitter_ms: u64, loss: f64) -> LinkSpec {
    LinkSpec {
        name,
        rtt_ms,
        jitter_ms,
        loss,
        role: Role::Slow,
    }
}

fn peer3_links() -> Vec<LinkSpec> {
    vec![
        peer("a", 12, 2, 0.001),
        peer("b", 14, 3, 0.002),
        peer("c", 16, 2, 0.001),
    ]
}

fn peer5_links() -> Vec<LinkSpec> {
    vec![
        peer("a", 11, 2, 0.001),
        peer("b", 12, 2, 0.002),
        peer("c", 13, 3, 0.001),
        peer("d", 15, 2, 0.002),
        peer("e", 16, 3, 0.001),
    ]
}

/// 3 same-class peers, 3 ping streams.
pub fn peer3() -> MixCase {
    MixCase {
        name: "peer3",
        links: peer3_links(),
        ping_streams: 3,
        bulk_streams: 0,
        connections: 2,
        p99_ms: 500,
        p50_ms: 40,
        min_success: 0.93,
        max_hard: 2,
    }
}

/// 5 same-class peers, 5 ping streams.
pub fn peer5() -> MixCase {
    MixCase {
        name: "peer5",
        links: peer5_links(),
        ping_streams: 5,
        bulk_streams: 0,
        connections: 2,
        p99_ms: 500,
        p50_ms: 40,
        min_success: 0.93,
        max_hard: 3,
    }
}

/// 3 peers + one slightly slower, mostly-stable link; 3 pings + 1 bulk.
pub fn peer3_slow1() -> MixCase {
    let mut links = peer3_links();
    links.push(slow("s", 30, 2, 0.0005));
    MixCase {
        name: "peer3_slow1",
        links,
        ping_streams: 3,
        bulk_streams: 1,
        connections: 2,
        p99_ms: 500,
        p50_ms: 40,
        min_success: 0.93,
        max_hard: 2,
    }
}

/// 5 peers + two slightly slower, mostly-stable links; 5 pings + 2 bulk.
pub fn peer5_slow2() -> MixCase {
    let mut links = peer5_links();
    links.push(slow("s1", 28, 2, 0.0005));
    links.push(slow("s2", 35, 3, 0.0005));
    MixCase {
        name: "peer5_slow2",
        links,
        ping_streams: 5,
        bulk_streams: 2,
        connections: 2,
        p99_ms: 500,
        p50_ms: 40,
        min_success: 0.93,
        max_hard: 4,
    }
}

fn case(
    name: &'static str,
    links: Vec<LinkSpec>,
    ping_streams: usize,
    bulk_streams: usize,
    max_hard: u32,
    p50_ms: u64,
    p99_ms: u64,
    min_success: f64,
) -> MixCase {
    MixCase {
        name,
        links,
        ping_streams,
        bulk_streams,
        connections: 2,
        p99_ms,
        p50_ms,
        min_success,
        max_hard,
    }
}

fn with_slow(mut peers: Vec<LinkSpec>, extras: Vec<LinkSpec>) -> Vec<LinkSpec> {
    peers.extend(extras);
    peers
}

fn mid_suite() -> Vec<MixCase> {
    // 60–100ms peers. Jitter ~10–16ms, loss ~0.5–1%. Slow ~125–138ms:
    // class-jump from 62ms (101ms) but not backup (144ms).
    let p3 = vec![
        peer("a", 68, 10, 0.005),
        peer("b", 82, 12, 0.008),
        peer("c", 96, 14, 0.006),
    ];
    let p5 = vec![
        peer("a", 62, 10, 0.005),
        peer("b", 70, 12, 0.008),
        peer("c", 78, 12, 0.006),
        peer("d", 86, 14, 0.009),
        peer("e", 96, 16, 0.007),
    ];
    vec![
        case("mid_peer3", p3.clone(), 3, 0, 2, 125, 1200, 0.92),
        case("mid_peer5", p5.clone(), 5, 0, 3, 125, 1200, 0.92),
        case(
            "mid_peer3_slow1",
            with_slow(p3, vec![slow("s", 128, 8, 0.002)]),
            3,
            1,
            2,
            125,
            1200,
            0.92,
        ),
        case(
            "mid_peer5_slow2",
            with_slow(
                p5,
                vec![slow("s1", 125, 8, 0.002), slow("s2", 138, 10, 0.002)],
            ),
            5,
            2,
            4,
            125,
            1200,
            0.92,
        ),
    ]
}

fn high_suite() -> Vec<MixCase> {
    // 120–150ms. Jitter ~18–28ms, loss ~1–1.5%. Slow ~195–215ms:
    // class-jump from 122ms (191ms), backup starts at 264ms.
    let p3 = vec![
        peer("a", 124, 18, 0.010),
        peer("b", 136, 22, 0.012),
        peer("c", 148, 24, 0.010),
    ];
    let p5 = vec![
        peer("a", 122, 18, 0.010),
        peer("b", 128, 20, 0.012),
        peer("c", 134, 22, 0.011),
        peer("d", 142, 24, 0.014),
        peer("e", 148, 28, 0.012),
    ];
    vec![
        case("high_peer3", p3.clone(), 3, 0, 2, 190, 1800, 0.90),
        case("high_peer5", p5.clone(), 5, 0, 3, 190, 1800, 0.90),
        case(
            "high_peer3_slow1",
            with_slow(p3, vec![slow("s", 198, 16, 0.004)]),
            3,
            1,
            2,
            190,
            1800,
            0.90,
        ),
        case(
            "high_peer5_slow2",
            with_slow(
                p5,
                vec![slow("s1", 195, 16, 0.004), slow("s2", 215, 18, 0.004)],
            ),
            5,
            2,
            4,
            190,
            1800,
            0.90,
        ),
    ]
}

fn far_suite() -> Vec<MixCase> {
    // 160–200ms. Jitter ~25–40ms, loss ~1.5–2%. Slow ~255–280ms:
    // class-jump from 162ms (251ms), backup starts at 344ms.
    let p3 = vec![
        peer("a", 168, 28, 0.015),
        peer("b", 182, 32, 0.018),
        peer("c", 196, 36, 0.016),
    ];
    let p5 = vec![
        peer("a", 162, 26, 0.014),
        peer("b", 172, 30, 0.016),
        peer("c", 182, 32, 0.015),
        peer("d", 190, 36, 0.018),
        peer("e", 198, 40, 0.017),
    ];
    vec![
        case("far_peer3", p3.clone(), 3, 0, 2, 250, 2500, 0.88),
        case("far_peer5", p5.clone(), 5, 0, 3, 250, 2500, 0.88),
        case(
            "far_peer3_slow1",
            with_slow(p3, vec![slow("s", 258, 20, 0.005)]),
            3,
            1,
            2,
            250,
            2500,
            0.88,
        ),
        case(
            "far_peer5_slow2",
            with_slow(
                p5,
                vec![slow("s1", 255, 20, 0.005), slow("s2", 280, 24, 0.005)],
            ),
            5,
            2,
            4,
            250,
            2500,
            0.88,
        ),
    ]
}

pub fn suite() -> Vec<MixCase> {
    MixBand::Near.suite()
}

pub fn suite_for(bands: &[MixBand]) -> Vec<MixCase> {
    bands.iter().flat_map(|b| b.suite()).collect()
}

/// `near`, `mid`, `high`, `far`, comma-lists, or `all`.
pub fn parse_bands(s: &str) -> Option<Vec<MixBand>> {
    let s = s.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }
    if s == "all" {
        return Some(MixBand::all().to_vec());
    }
    let mut out = Vec::new();
    for part in s.split(',') {
        let b = MixBand::parse(part)?;
        if !out.contains(&b) {
            out.push(b);
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

#[derive(Clone, Copy)]
struct Baseline {
    rtt: Duration,
    jitter: Duration,
    loss: f64,
}

impl From<&LinkSpec> for Baseline {
    fn from(l: &LinkSpec) -> Self {
        Self {
            rtt: Duration::from_millis(l.rtt_ms),
            jitter: Duration::from_millis(l.jitter_ms),
            loss: l.loss,
        }
    }
}

/// DelayShift target RTT. Always strictly slower than `base_ms`.
/// Near-lab (≤24ms) keeps the original 70–110 / 100–180 windows so the
/// 11–16ms suite does not change underfoot.
fn delay_shift_range(base_ms: u64, harsh: bool) -> (u64, u64) {
    if base_ms <= 24 {
        return if harsh { (100, 180) } else { (70, 110) };
    }
    let (lo, hi) = if harsh {
        (
            (base_ms + 80).max(base_ms.saturating_mul(18) / 10),
            (base_ms + 180).max(base_ms.saturating_mul(28) / 10),
        )
    } else {
        (
            (base_ms + 50).max(base_ms.saturating_mul(17) / 10),
            (base_ms + 150).max(base_ms.saturating_mul(25) / 10),
        )
    };
    let lo = lo.max(base_ms + 20);
    (lo, hi.max(lo + 1))
}

fn extra_range(base_ms: u64, slow: bool, harsh: bool) -> (u64, u64) {
    if slow {
        return ((base_ms / 8).max(20), (base_ms / 3).max(60));
    }
    if base_ms <= 24 {
        return if harsh { (120, 400) } else { (80, 250) };
    }
    if harsh {
        (base_ms.max(120), base_ms.saturating_mul(2).max(400))
    } else {
        (base_ms.max(80), base_ms.saturating_mul(2).max(250))
    }
}

fn jitter_range(base_ms: u64, slow: bool, harsh: bool) -> (u64, u64) {
    if slow {
        return ((base_ms / 20).max(6), (base_ms / 10).max(14));
    }
    if base_ms <= 24 {
        return if harsh { (20, 40) } else { (12, 28) };
    }
    if harsh {
        ((base_ms / 5).max(20), (base_ms / 2).max(40))
    } else {
        ((base_ms / 6).max(12), (base_ms / 2).max(28))
    }
}

fn loss_range(base_ms: u64, slow: bool, harsh: bool) -> (f64, f64) {
    let rtt = base_ms as f64;
    if slow {
        let lo = (0.01 + rtt / 20_000.0).min(0.04);
        let hi = (0.03 + rtt / 8_000.0).min(0.08);
        return (lo, hi.max(lo + 0.005));
    }
    if base_ms <= 24 {
        return if harsh { (0.08, 0.18) } else { (0.04, 0.12) };
    }
    if harsh {
        let lo = (0.08 + rtt / 4_000.0).min(0.22);
        let hi = (0.18 + rtt / 2_500.0).min(0.30);
        (lo, hi.max(lo + 0.02))
    } else {
        let lo = (0.04 + rtt / 5_000.0).min(0.15);
        let hi = (0.12 + rtt / 2_500.0).min(0.22);
        (lo, hi.max(lo + 0.02))
    }
}

fn pick_ms(rng: &mut impl Rng, lo: u64, hi: u64) -> u64 {
    if hi <= lo {
        lo
    } else {
        rng.gen_range(lo..=hi)
    }
}

fn pick_loss(rng: &mut impl Rng, lo: f64, hi: f64) -> f64 {
    if hi <= lo {
        lo
    } else {
        rng.gen_range(lo..=hi)
    }
}

#[derive(Clone)]
struct FaultEvent {
    t_ms: u64,
    link: String,
    kind: &'static str,
    hold_ms: u64,
    note: String,
}

#[derive(Clone)]
struct SwitchEvent {
    t_ms: u64,
    migrates: u64,
    failbacks: u64,
    failbacks_upgrade: u64,
    failbacks_class_empty: u64,
    hol_rebalances: u64,
    path_down: u64,
    path_added: u64,
    alive: usize,
    path_rtts: String,
}

#[derive(Clone)]
pub struct MixedOpts {
    pub duration: Duration,
    pub harsh: bool,
    pub jobs: usize,
    pub filter: Option<String>,
    /// Empty means near (lab 11–16ms).
    pub bands: Vec<MixBand>,
}

impl MixedOpts {
    pub fn suite(duration: Duration) -> Self {
        Self {
            duration,
            harsh: false,
            jobs: crate::default_jobs(),
            filter: None,
            bands: vec![MixBand::Near],
        }
    }
}

struct Shared {
    t0: Instant,
    hard: AtomicU32,
    max_hard: u32,
    storm: AtomicBool,
    harsh: bool,
    events: Mutex<Vec<FaultEvent>>,
    switches: Mutex<Vec<SwitchEvent>>,
    covered: Mutex<BTreeMap<(String, &'static str), u32>>,
}

fn restore(link: &LinkHandle, b: Baseline) {
    link.set_rtt(b.rtt);
    link.set_jitter(b.jitter);
    link.set_loss(b.loss);
    link.set_extra(Duration::ZERO);
    link.set_blackhole(false);
    link.clear_conn_faults();
}

fn actor_idle_ms(harsh: bool, slow: bool, rng: &mut impl Rng) -> u64 {
    if slow {
        rng.gen_range(5000..=15000)
    } else if harsh {
        rng.gen_range(400..=2000)
    } else {
        rng.gen_range(800..=3500)
    }
}

fn actor_gap_ms(harsh: bool, slow: bool, rng: &mut impl Rng) -> u64 {
    if slow {
        rng.gen_range(4000..=12000)
    } else if harsh {
        rng.gen_range(800..=2500)
    } else {
        rng.gen_range(1500..=4000)
    }
}

fn actor_loop_ms(harsh: bool, slow: bool, rng: &mut impl Rng) -> u64 {
    if slow {
        rng.gen_range(8000..=20000)
    } else if harsh {
        rng.gen_range(800..=4000)
    } else {
        rng.gen_range(2000..=8000)
    }
}

async fn fire(
    link: &LinkHandle,
    b: Baseline,
    kind: Kind,
    role: Role,
    shared: &Shared,
    rng: &mut impl Rng,
) -> Duration {
    let harsh = shared.harsh;
    let slow = role == Role::Slow;
    let hold = match kind {
        Kind::FlashDisconnect | Kind::ConnDisconnect => Duration::from_millis(200),
        Kind::Offline => Duration::from_millis(if harsh {
            rng.gen_range(4000..=8000)
        } else {
            rng.gen_range(2000..=7000)
        }),
        Kind::Blackhole => Duration::from_millis(if harsh {
            rng.gen_range(3000..=8000)
        } else {
            rng.gen_range(1500..=5000)
        }),
        Kind::DelaySpike => Duration::from_millis(if slow {
            rng.gen_range(400..=1200)
        } else {
            rng.gen_range(800..=2500)
        }),
        Kind::DelayShift => Duration::from_millis(if harsh {
            rng.gen_range(4000..=9000)
        } else {
            rng.gen_range(2500..=7000)
        }),
        Kind::LossBurst | Kind::JitterBurst => Duration::from_millis(if slow {
            rng.gen_range(800..=2500)
        } else if harsh {
            rng.gen_range(3000..=8000)
        } else {
            rng.gen_range(2000..=6000)
        }),
        Kind::ConnBlackhole | Kind::ConnStall => Duration::from_millis(rng.gen_range(1500..=4000)),
    };
    let base_ms = b.rtt.as_millis() as u64;
    let mut note = String::new();
    match kind {
        Kind::FlashDisconnect => {
            link.disconnect_all();
            note = "rst_all".into();
        }
        Kind::Offline => {
            link.set_blackhole(true);
            link.disconnect_all();
            note = "blackhole+rst".into();
        }
        Kind::Blackhole => {
            link.set_blackhole(true);
        }
        Kind::DelaySpike => {
            let (lo, hi) = extra_range(base_ms, slow, harsh);
            let extra = Duration::from_millis(pick_ms(rng, lo, hi));
            link.set_extra(extra);
            note = format!("extra={}ms", extra.as_millis());
        }
        Kind::DelayShift => {
            let (lo, hi) = delay_shift_range(base_ms, harsh);
            let shifted = Duration::from_millis(pick_ms(rng, lo, hi));
            link.set_rtt(shifted);
            note = format!("rtt→{}ms", shifted.as_millis());
        }
        Kind::LossBurst => {
            let (lo, hi) = loss_range(base_ms, slow, harsh);
            let p = pick_loss(rng, lo, hi);
            link.set_loss(p);
            note = format!("loss={p:.3}");
        }
        Kind::JitterBurst => {
            let (lo, hi) = jitter_range(base_ms, slow, harsh);
            let j = Duration::from_millis(pick_ms(rng, lo, hi));
            link.set_jitter(j);
            note = format!("jitter={}ms", j.as_millis());
        }
        Kind::ConnBlackhole => {
            link.set_conn_blackhole(0, true);
            note = "conn0".into();
        }
        Kind::ConnStall => {
            link.set_conn_stall(0, true);
            note = "conn0".into();
        }
        Kind::ConnDisconnect => {
            link.disconnect_conn(0);
            note = "rst_conn0".into();
        }
    }
    let t_ms = shared.t0.elapsed().as_millis() as u64;
    info!(link = %link.name, kind = kind.name(), role = ?role, ?hold, %note, "fault start");
    {
        let mut g = shared.events.lock().unwrap();
        g.push(FaultEvent {
            t_ms,
            link: link.name.clone(),
            kind: kind.name(),
            hold_ms: hold.as_millis() as u64,
            note: note.clone(),
        });
        *shared
            .covered
            .lock()
            .unwrap()
            .entry((link.name.clone(), kind.name()))
            .or_insert(0) += 1;
    }
    tokio::time::sleep(hold).await;
    restore(link, b);
    info!(link = %link.name, kind = kind.name(), "fault end");
    hold
}

async fn acquire_hard(shared: &Shared, deadline: Instant) -> bool {
    loop {
        if Instant::now() + Duration::from_secs(4) >= deadline {
            return false;
        }
        if shared.storm.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(400)).await;
            continue;
        }
        let n = shared.hard.load(Ordering::SeqCst);
        if n < shared.max_hard
            && shared
                .hard
                .compare_exchange(n, n + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

async fn link_actor(
    link: LinkHandle,
    b: Baseline,
    role: Role,
    coverage: Vec<Kind>,
    deadline: Instant,
    shared: Arc<Shared>,
) {
    restore(&link, b);
    let mut rng = StdRng::from_entropy();
    let slow = role == Role::Slow;
    for kind in coverage {
        if Instant::now() + Duration::from_secs(4) >= deadline {
            break;
        }
        let idle = Duration::from_millis(actor_idle_ms(shared.harsh, slow, &mut rng));
        tokio::time::sleep(idle).await;
        if kind.hard_down() {
            if acquire_hard(&shared, deadline).await {
                fire(&link, b, kind, role, &shared, &mut rng).await;
                shared.hard.fetch_sub(1, Ordering::SeqCst);
            }
        } else {
            fire(&link, b, kind, role, &shared, &mut rng).await;
        }
        tokio::time::sleep(Duration::from_millis(actor_gap_ms(
            shared.harsh,
            slow,
            &mut rng,
        )))
        .await;
    }
    while Instant::now() + Duration::from_secs(6) < deadline {
        tokio::time::sleep(Duration::from_millis(actor_loop_ms(
            shared.harsh,
            slow,
            &mut rng,
        )))
        .await;
        if Instant::now() + Duration::from_secs(6) >= deadline {
            break;
        }
        let pool = if slow { Kind::mild() } else { Kind::all() };
        let mut kind = *pool.choose(&mut rng).unwrap();
        if kind.hard_down() {
            let n = shared.hard.load(Ordering::SeqCst);
            if !shared.storm.load(Ordering::SeqCst)
                && n < shared.max_hard
                && shared
                    .hard
                    .compare_exchange(n, n + 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                fire(&link, b, kind, role, &shared, &mut rng).await;
                shared.hard.fetch_sub(1, Ordering::SeqCst);
                continue;
            }
            kind = Kind::DelaySpike;
        }
        fire(&link, b, kind, role, &shared, &mut rng).await;
    }
    restore(&link, b);
}

/// Briefly blackhole every peer so traffic must sit on the slow class,
/// then lift and expect failback. Slow links stay up.
async fn peer_collapse(
    peers: Vec<LinkHandle>,
    deadline: Instant,
    shared: Arc<Shared>,
    t0: Instant,
) {
    let mut rng = StdRng::from_entropy();
    let mut n = 0u32;
    while Instant::now() + Duration::from_secs(20) < deadline {
        tokio::time::sleep(Duration::from_secs(rng.gen_range(180..=280))).await;
        if Instant::now() + Duration::from_secs(8) >= deadline || n >= 2 {
            break;
        }
        shared.storm.store(true, Ordering::SeqCst);
        let wait_hard = Instant::now() + Duration::from_secs(4);
        while shared.hard.load(Ordering::SeqCst) > 0 && Instant::now() < wait_hard {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        n += 1;
        info!(n, "peer-class collapse");
        for l in &peers {
            l.set_blackhole(true);
        }
        {
            let mut g = shared.events.lock().unwrap();
            g.push(FaultEvent {
                t_ms: t0.elapsed().as_millis() as u64,
                link: "*".into(),
                kind: "peer_collapse",
                hold_ms: 0,
                note: format!("peers={}", peers.len()),
            });
        }
        tokio::time::sleep(Duration::from_millis(rng.gen_range(800..=1500))).await;
        for l in &peers {
            l.set_blackhole(false);
        }
        shared.storm.store(false, Ordering::SeqCst);
    }
    shared.storm.store(false, Ordering::SeqCst);
}

fn hist(stats: &WorkloadStats) -> String {
    let mut buckets = [0u64; 9];
    for us in stats.rtts_us() {
        let ms = us / 1000;
        let i = if ms < 20 {
            0
        } else if ms < 40 {
            1
        } else if ms < 80 {
            2
        } else if ms < 200 {
            3
        } else if ms < 500 {
            4
        } else if ms < 1000 {
            5
        } else if ms < 2000 {
            6
        } else {
            7
        };
        buckets[i] += 1;
    }
    buckets[8] = stats.timeouts;
    format!(
        "<20={} 20-40={} 40-80={} 80-200={} 200-500={} 500-1000={} 1000-2000={} ≥2000={} timeout={}",
        buckets[0],
        buckets[1],
        buckets[2],
        buckets[3],
        buckets[4],
        buckets[5],
        buckets[6],
        buckets[7],
        buckets[8]
    )
}

fn lat_line(st: &WorkloadStats) -> String {
    let ms = |p: f64| {
        st.percentile_us(p)
            .map(|u| format!("{:.1}", u as f64 / 1000.0))
            .unwrap_or_else(|| "-".into())
    };
    let avg = st
        .mean_us()
        .map(|u| format!("{:.1}", u / 1000.0))
        .unwrap_or_else(|| "-".into());
    format!(
        "n={}/{} ok={:.3} min={} avg={} p50={} p90={} p95={} p99={} max={}",
        st.n_ok(),
        st.n_samples(),
        st.success_rate(),
        ms(0.0),
        avg,
        ms(50.0),
        ms(90.0),
        ms(95.0),
        ms(99.0),
        st.max_us()
            .map(|u| format!("{:.1}", u as f64 / 1000.0))
            .unwrap_or_else(|| "-".into()),
    )
}

fn fail_report(name: &str, err: String) -> ScenarioReport {
    ScenarioReport {
        name: name.to_string(),
        stats: WorkloadStats::default(),
        snap: SessionSnapshot {
            path_added: 0,
            path_down: 0,
            migrates: 0,
            failbacks: 0,
            failbacks_upgrade: 0,
            failbacks_class_empty: 0,
            hol_rebalances: 0,
            stream_resets: 0,
            bytes_data_tx: 0,
            bytes_data_rx: 0,
            frame_send_drop: 0,
            paths: vec![],
        },
        sla: Sla::healthy(1),
        failover_observed_ms: None,
        notes: vec![err],
    }
}

pub async fn run_suite(opts: MixedOpts) -> Result<Vec<ScenarioReport>> {
    let bands = if opts.bands.is_empty() {
        vec![MixBand::Near]
    } else {
        opts.bands.clone()
    };
    let selected: Vec<MixCase> = suite_for(&bands)
        .into_iter()
        .filter(|c| {
            opts.filter
                .as_ref()
                .map(|f| c.name.contains(f))
                .unwrap_or(true)
        })
        .collect();
    let jobs = opts.jobs.max(1).min(selected.len().max(1));
    let band_tags: Vec<_> = bands.iter().map(|b| b.tag()).collect();
    info!(
        n = selected.len(),
        jobs,
        secs = opts.duration.as_secs(),
        bands = %band_tags.join(","),
        "mixed suite start"
    );
    let sem = Arc::new(tokio::sync::Semaphore::new(jobs));
    let mut joins = tokio::task::JoinSet::new();
    for (idx, case) in selected.into_iter().enumerate() {
        let sem = sem.clone();
        let duration = opts.duration;
        let harsh = opts.harsh;
        joins.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore");
            let name = case.name;
            let r = run_case(case, duration, harsh).await;
            (idx, name, r)
        });
    }
    let mut indexed = Vec::new();
    while let Some(joined) = joins.join_next().await {
        match joined {
            Ok((idx, _, Ok(r))) => indexed.push((idx, r)),
            Ok((idx, name, Err(e))) => {
                tracing::error!(name, error = %e, "mixed case error");
                indexed.push((idx, fail_report(&format!("{name} (error)"), e.to_string())));
            }
            Err(e) => indexed.push((usize::MAX, fail_report("mixed (panic)", e.to_string()))),
        }
    }
    indexed.sort_by_key(|(i, _)| *i);
    Ok(indexed.into_iter().map(|(_, r)| r).collect())
}

pub async fn run_case(case: MixCase, duration: Duration, harsh: bool) -> Result<ScenarioReport> {
    let h = start(case.spec()).await?;
    let shared = Arc::new(Shared {
        t0: Instant::now(),
        hard: AtomicU32::new(0),
        max_hard: case.max_hard.max(1),
        storm: AtomicBool::new(false),
        harsh,
        events: Mutex::new(Vec::new()),
        switches: Mutex::new(Vec::new()),
        covered: Mutex::new(BTreeMap::new()),
    });
    let deadline = Instant::now() + duration;

    let mut kinds: Vec<Kind> = Kind::all().to_vec();
    kinds.shuffle(&mut rand::thread_rng());
    let n_kinds = kinds.len();
    for (i, spec) in case.links.iter().enumerate() {
        let coverage = if spec.role == Role::Slow {
            Kind::mild().to_vec()
        } else {
            let mut cov = kinds.clone();
            cov.rotate_left(i * 3 % n_kinds);
            cov
        };
        let link = h.link(spec.name).clone();
        let b = Baseline::from(spec);
        let role = spec.role;
        let shared = shared.clone();
        tokio::spawn(async move {
            link_actor(link, b, role, coverage, deadline, shared).await;
        });
    }

    if case.has_slow() {
        let peers: Vec<_> = case.peers().map(|p| h.link(p.name).clone()).collect();
        let shared = shared.clone();
        let t0 = shared.t0;
        tokio::spawn(async move {
            peer_collapse(peers, deadline, shared, t0).await;
        });
    }

    let sess = h.session.clone();
    let sw_shared = shared.clone();
    let sw_dead = deadline;
    tokio::spawn(async move {
        let mut prev = sess.snapshot();
        while Instant::now() < sw_dead {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let now = sess.snapshot();
            let dm = now.migrates.saturating_sub(prev.migrates);
            let df = now.failbacks.saturating_sub(prev.failbacks);
            let du = now.failbacks_upgrade.saturating_sub(prev.failbacks_upgrade);
            let dc = now
                .failbacks_class_empty
                .saturating_sub(prev.failbacks_class_empty);
            let dh = now.hol_rebalances.saturating_sub(prev.hol_rebalances);
            let dd = now.path_down.saturating_sub(prev.path_down);
            let da = now.path_added.saturating_sub(prev.path_added);
            if dm + df + dd + da + dh > 0 {
                let mut rtts: Vec<_> = now
                    .paths
                    .iter()
                    .map(|p| {
                        format!(
                            "{}={}/{}/{}ms",
                            p.name,
                            p.rtt_us / 1000,
                            p.stable_rtt_us / 1000,
                            p.class_rtt_us / 1000
                        )
                    })
                    .collect();
                rtts.sort();
                sw_shared.switches.lock().unwrap().push(SwitchEvent {
                    t_ms: sw_shared.t0.elapsed().as_millis() as u64,
                    migrates: dm,
                    failbacks: df,
                    failbacks_upgrade: du,
                    failbacks_class_empty: dc,
                    hol_rebalances: dh,
                    path_down: dd,
                    path_added: da,
                    alive: now.paths.iter().filter(|p| p.alive).count(),
                    path_rtts: rtts.join(","),
                });
            }
            prev = now;
        }
    });

    let n_ping = case.ping_streams.max(1);
    let ping_to = case.ping_timeout();
    let mut joins = Vec::new();
    for i in 0..n_ping {
        let mut tcp = h.connect_forward().await?;
        let interval = if i == 0 {
            Duration::from_millis(80)
        } else {
            PING
        };
        joins.push(tokio::spawn(async move {
            ping_for(&mut tcp, duration, interval, ping_to).await
        }));
    }

    let mut bulk_joins = Vec::new();
    for _ in 0..case.bulk_streams {
        let fwd = h.forward;
        bulk_joins.push(tokio::spawn(async move {
            let mut ok = 0u64;
            let mut fail = 0u64;
            let stop = Instant::now() + duration;
            let mut tcp = match tokio::net::TcpStream::connect(fwd).await {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    s
                }
                Err(_) => return (0, 1),
            };
            while Instant::now() < stop {
                match bulk_echo(&mut tcp, 96 * 1024).await {
                    Ok((_, true)) => ok += 1,
                    _ => {
                        fail += 1;
                        tokio::time::sleep(Duration::from_millis(80)).await;
                        match tokio::net::TcpStream::connect(fwd).await {
                            Ok(s) => {
                                let _ = s.set_nodelay(true);
                                tcp = s;
                            }
                            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                        }
                    }
                }
            }
            (ok, fail)
        }));
    }

    let mut stats = WorkloadStats::default();
    let mut ping_fail = 0u64;
    for j in joins {
        match j.await {
            Ok(s) => stats.merge(&s),
            Err(_) => ping_fail += 1,
        }
    }
    let mut bulk_ok = 0u64;
    let mut bulk_fail = 0u64;
    for j in bulk_joins {
        match j.await {
            Ok((ok, fail)) => {
                bulk_ok += ok;
                bulk_fail += fail;
            }
            Err(_) => bulk_fail += 1,
        }
    }
    let bulk = if case.bulk_streams > 0 {
        Some((bulk_ok, bulk_fail))
    } else {
        None
    };
    tokio::time::sleep(Duration::from_millis(200)).await;

    let snap = h.session.snapshot();
    let events = shared.events.lock().unwrap().clone();
    let switches = shared.switches.lock().unwrap().clone();
    let covered = shared.covered.lock().unwrap().clone();

    let mut notes = build_report(
        &h, &case, &stats, &snap, &events, &switches, &covered, duration, n_ping, bulk, ping_fail,
        harsh,
    );
    let missing: Vec<_> = Kind::all()
        .iter()
        .filter(|k| !covered.keys().any(|(_, n)| *n == k.name()))
        .map(|k| k.name())
        .collect();
    if !missing.is_empty() {
        notes.push(format!("UNCOVERED kinds: {}", missing.join(",")));
    }

    let sla = Sla {
        must_survive: true,
        p99_ms: Some(case.p99_ms),
        failover_ms: None,
        min_success: case.min_success,
    };
    let mut r = ScenarioReport {
        name: format!("{}_{}", case.name, fmt_dur(duration)),
        stats,
        snap,
        sla,
        failover_observed_ms: Some(r_gap_ms(&notes)),
        notes,
    };
    let need_cover = duration >= Duration::from_secs(8 * 60);
    if !missing.is_empty() && need_cover {
        r.notes.push("coverage incomplete".into());
        r.sla.min_success = 2.0;
    }
    if let Some(p50) = r.stats.percentile_us(50.0) {
        if p50 / 1000 > case.p50_ms {
            r.notes.push(format!(
                "p50={}us left peer class (limit {}ms)",
                p50, case.p50_ms
            ));
            r.sla.min_success = 2.0;
        }
    }
    if ping_fail > 0 {
        r.notes.push(format!("ping tasks panicked: {ping_fail}"));
        r.sla.min_success = 2.0;
    }
    if let Some((_, fail)) = bulk {
        if fail > 0 && r.stats.disconnect {
            r.notes.push("bulk stream died with business TCP".into());
        }
    }
    if duration >= Duration::from_secs(300) {
        let mins = (duration.as_secs() as f64 / 60.0).max(1.0);
        let fb_per_min = r.snap.failbacks as f64 / mins;
        r.notes.push(format!("failbacks_per_min={fb_per_min:.1}"));
        r.notes.push(format!(
            "fb_upgrade_per_min={:.1}",
            r.snap.failbacks_upgrade as f64 / mins
        ));
        r.notes.push(format!(
            "fb_class_empty_per_min={:.1}",
            r.snap.failbacks_class_empty as f64 / mins
        ));
        r.notes
            .push(format!("hol_rebalances={}", r.snap.hol_rebalances));
        if fb_per_min >= 25.0 {
            r.notes
                .push(format!("failback chatter {fb_per_min:.1}/min (limit 25)"));
            r.sla.min_success = 2.0;
        }
        if case.bulk_streams > 0 {
            let peer: std::collections::BTreeSet<&str> = case
                .links
                .iter()
                .filter(|l| l.role == Role::Peer)
                .map(|l| l.name)
                .collect();
            let mut bytes: Vec<(String, u64)> = h
                .links
                .iter()
                .filter(|l| peer.contains(l.name.as_str()))
                .map(|l| {
                    let s = l.stats();
                    (s.name, s.bytes_fwd + s.bytes_rev)
                })
                .collect();
            let total: u64 = bytes.iter().map(|(_, n)| *n).sum();
            bytes.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
            let mixed_n = bytes
                .iter()
                .filter(|(_, n)| total > 0 && *n * 10 >= total)
                .count();
            r.notes
                .push(format!("peer_wan_10pct_names={mixed_n} total={total}"));
            if mixed_n < 2 {
                r.notes
                    .push("bulk did not mix across ≥2 peer names at 10% WAN bytes".into());
                r.sla.min_success = 2.0;
            }
        }
    }

    let dir = std::path::Path::new("e2e-reports");
    let _ = std::fs::create_dir_all(dir);
    let path = dir.join(format!("{}.txt", r.name));
    let _ = std::fs::write(&path, r.notes.join("\n") + "\n" + &format!("{r}\n"));
    Ok(r)
}

fn r_gap_ms(notes: &[String]) -> u64 {
    notes
        .iter()
        .find_map(|n| n.strip_prefix("max_ok_gap_ms="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn build_report(
    h: &Harness,
    case: &MixCase,
    stats: &WorkloadStats,
    snap: &SessionSnapshot,
    events: &[FaultEvent],
    switches: &[SwitchEvent],
    covered: &BTreeMap<(String, &'static str), u32>,
    duration: Duration,
    n_ping: usize,
    bulk: Option<(u64, u64)>,
    ping_fail: u64,
    harsh: bool,
) -> Vec<String> {
    let t0 = stats
        .samples
        .first()
        .map(|s| s.at)
        .unwrap_or_else(Instant::now);
    let mut out = Vec::new();
    let topo: String = h
        .links
        .iter()
        .map(|l| {
            let s = l.stats();
            let role = case
                .links
                .iter()
                .find(|x| x.name == s.name)
                .map(|x| x.role)
                .unwrap_or(Role::Peer);
            format!(
                "{}[{:?}]={}ms±{}ms/{:.1}%",
                s.name,
                role,
                s.rtt.as_millis(),
                s.jitter.as_millis(),
                s.loss * 100.0
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    out.push(format!(
        "duration={} case={} harsh={harsh} pings={n_ping} bulk={} ping_fail={ping_fail} {topo}",
        fmt_dur(duration),
        case.name,
        bulk.map(|(ok, f)| format!("ok={ok} fail={f}"))
            .unwrap_or_else(|| "off".into()),
    ));
    out.push(format!("TOP {}", lat_line(stats)));
    out.push(format!("hist {}", hist(stats)));
    out.push(format!(
        "survive={} disconnect={} timeouts={} io_err={} max_ok_gap_ms={}",
        !stats.disconnect,
        stats.disconnect,
        stats.timeouts,
        stats.io_errors,
        stats.max_ok_gap().as_millis()
    ));
    out.push(format!(
        "switch migrates={} failbacks={} fb_upgrade={} fb_class_empty={} hol={} path_down={} path_added={} stream_resets={} send_drops={}",
        snap.migrates,
        snap.failbacks,
        snap.failbacks_upgrade,
        snap.failbacks_class_empty,
        snap.hol_rebalances,
        snap.path_down,
        snap.path_added,
        snap.stream_resets,
        snap.frame_send_drop
    ));

    out.push("--- coverage kind×link ---".into());
    let names: Vec<&str> = case.links.iter().map(|l| l.name).collect();
    for k in Kind::all() {
        let cells: Vec<String> = names
            .iter()
            .map(|n| {
                let v = covered.get(&((*n).into(), k.name())).copied().unwrap_or(0);
                format!("{n}={v}")
            })
            .collect();
        let sum: u32 = names
            .iter()
            .map(|n| covered.get(&((*n).into(), k.name())).copied().unwrap_or(0))
            .sum();
        out.push(format!("  {:<18} {} sum={sum}", k.name(), cells.join(" ")));
    }
    out.push(format!(
        "fault_events={} switch_events={}",
        events.len(),
        switches.len()
    ));

    out.push("--- per-minute business RTT ---".into());
    let windows = ((duration.as_secs() + WINDOW.as_secs() - 1) / WINDOW.as_secs()).max(1) as u32;
    for i in 0..windows {
        let s = t0 + WINDOW * i;
        let e = s + WINDOW;
        let w = stats.slice_from(s, e);
        if w.n_samples() == 0 {
            continue;
        }
        out.push(format!("  min{:>02} {}", i, lat_line(&w)));
    }

    out.push("--- faults (t_ms link kind hold note) ---".into());
    for e in events {
        out.push(format!(
            "  t={:>7} {:<4} {:<18} hold={:<5} {}",
            e.t_ms, e.link, e.kind, e.hold_ms, e.note
        ));
    }
    out.push("--- switches (delta) ---".into());
    for s in switches {
        out.push(format!(
            "  t={:>7} mig=+{} fb=+{} up=+{} empty=+{} hol=+{} down=+{} add=+{} alive={} [{}]",
            s.t_ms,
            s.migrates,
            s.failbacks,
            s.failbacks_upgrade,
            s.failbacks_class_empty,
            s.hol_rebalances,
            s.path_down,
            s.path_added,
            s.alive,
            s.path_rtts
        ));
    }
    out.push("--- link wan ---".into());
    for l in &h.links {
        let st = l.stats();
        out.push(format!(
            "  {} rtt={}ms jitter={}ms loss={:.3} wan_drops={} retrans={} bytes={}/{} conns={}",
            st.name,
            st.rtt.as_millis(),
            st.jitter.as_millis(),
            st.loss,
            st.drops,
            st.retrans,
            st.bytes_fwd,
            st.bytes_rev,
            l.live_conn_count()
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_kinds_named() {
        assert_eq!(Kind::all().len(), 10);
        assert_eq!(Kind::mild().len(), 3);
        assert!(Kind::mild().iter().all(|k| !k.hard_down()));
    }

    #[test]
    fn suite_matches_typical_mix() {
        let s = suite();
        assert_eq!(s.len(), 4);
        let names: Vec<_> = s.iter().map(|c| c.name).collect();
        assert_eq!(names, ["peer3", "peer5", "peer3_slow1", "peer5_slow2"]);
        assert_eq!(peer3().ping_streams, 3);
        assert_eq!(peer5().ping_streams, 5);
        assert_eq!(peer3_slow1().bulk_streams, 1);
        assert_eq!(peer5_slow2().bulk_streams, 2);
        assert_eq!(
            peer3()
                .links
                .iter()
                .filter(|l| l.role == Role::Peer)
                .count(),
            3
        );
        assert_eq!(
            peer5()
                .links
                .iter()
                .filter(|l| l.role == Role::Peer)
                .count(),
            5
        );
        assert_eq!(
            peer3_slow1()
                .links
                .iter()
                .filter(|l| l.role == Role::Slow)
                .count(),
            1
        );
        assert_eq!(
            peer5_slow2()
                .links
                .iter()
                .filter(|l| l.role == Role::Slow)
                .count(),
            2
        );
    }

    fn peer_rtts(c: &MixCase) -> Vec<u64> {
        c.links
            .iter()
            .filter(|l| l.role == Role::Peer)
            .map(|l| l.rtt_ms)
            .collect()
    }

    fn in_window(c: &MixCase, lo: u64, hi: u64) {
        for r in peer_rtts(c) {
            assert!(
                (lo..=hi).contains(&r),
                "{} peer rtt {r} not in {lo}-{hi}",
                c.name
            );
        }
        for l in c.links.iter().filter(|l| l.role == Role::Slow) {
            assert!(
                l.rtt_ms > hi,
                "{} slow {}={}ms should sit above peer window {hi}",
                c.name,
                l.name,
                l.rtt_ms
            );
        }
    }

    #[test]
    fn all_bands_have_four_cases() {
        for b in MixBand::all() {
            assert_eq!(b.suite().len(), 4, "{}", b.tag());
        }
        assert_eq!(suite_for(MixBand::all()).len(), 16);
        assert_eq!(parse_bands("all").unwrap().len(), 4);
        assert_eq!(
            parse_bands("mid,far").unwrap(),
            vec![MixBand::Mid, MixBand::Far]
        );
        assert_eq!(parse_bands("60-100").unwrap(), vec![MixBand::Mid]);
        assert!(parse_bands("nope").is_none());
    }

    #[test]
    fn rtt_bands_match_requested_windows() {
        for c in MixBand::Near.suite() {
            in_window(&c, 11, 16);
        }
        for c in MixBand::Mid.suite() {
            in_window(&c, 60, 100);
        }
        for c in MixBand::High.suite() {
            in_window(&c, 120, 150);
        }
        for c in MixBand::Far.suite() {
            in_window(&c, 160, 200);
        }
    }

    #[test]
    fn ping_timeout_tracks_band() {
        assert_eq!(peer3().ping_timeout(), PING_TO_FLOOR);
        let far_to = MixBand::Far
            .suite()
            .iter()
            .map(|c| c.ping_timeout())
            .max()
            .unwrap();
        assert!(
            far_to >= Duration::from_millis(280 * 12),
            "far ping timeout {far_to:?}"
        );
    }

    #[test]
    fn delay_shift_never_speeds_up() {
        for base in [12u64, 16, 80, 96, 130, 148, 180, 198] {
            for harsh in [false, true] {
                let (lo, hi) = delay_shift_range(base, harsh);
                assert!(lo > base, "lo={lo} base={base} harsh={harsh}");
                assert!(hi >= lo);
            }
        }
        assert_eq!(delay_shift_range(12, false), (70, 110));
        assert_eq!(delay_shift_range(16, false), (70, 110));
    }

    #[test]
    fn high_rtt_faults_are_harsher() {
        let near_j = jitter_range(12, false, false);
        let far_j = jitter_range(180, false, false);
        assert!(far_j.0 > near_j.0 && far_j.1 > near_j.1);
        let near_l = loss_range(12, false, false);
        let far_l = loss_range(180, false, false);
        assert!(far_l.0 > near_l.0 && far_l.1 > near_l.1);
        let far_x = extra_range(180, false, false);
        assert!(far_x.0 >= 180 && far_x.1 >= 360);
    }
}
