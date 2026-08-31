use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;

use crate::harness::{start, Harness, HarnessSpec};
use crate::impair::ImpairConfig;
use crate::report::{ScenarioReport, Sla};
use crate::workload::{bulk_echo, ping_for, WorkloadStats};

const PING: Duration = Duration::from_millis(40);
const PING_TO: Duration = Duration::from_millis(1500);

fn three_paths(rtts: [u64; 3]) -> HarnessSpec {
    HarnessSpec {
        link_cfgs: vec![
            (
                "a".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(rtts[0]),
                    ..Default::default()
                },
            ),
            (
                "b".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(rtts[1]),
                    ..Default::default()
                },
            ),
            (
                "c".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(rtts[2]),
                    ..Default::default()
                },
            ),
        ],
        psk: "e2e-psk".into(),
        ..Default::default()
    }
}

async fn ping_stream(h: &Harness, dur: Duration) -> Result<WorkloadStats> {
    let mut tcp = h.connect_forward().await?;
    Ok(ping_for(&mut tcp, dur, PING, PING_TO).await)
}

fn finish(
    name: &str,
    h: &Harness,
    stats: WorkloadStats,
    sla: Sla,
    failover_ms: Option<u64>,
) -> ScenarioReport {
    ScenarioReport {
        name: name.to_string(),
        stats,
        snap: h.session.snapshot(),
        sla,
        failover_observed_ms: failover_ms,
        notes: Vec::new(),
    }
}

pub async fn baseline_10ms() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    let stats = ping_stream(&h, Duration::from_secs(2)).await?;
    Ok(finish("baseline_10ms", &h, stats, Sla::healthy(80), None))
}

pub async fn delay_matrix(ms: u64) -> Result<ScenarioReport> {
    let h = start(three_paths([ms, ms, ms])).await?;
    let stats = ping_stream(&h, Duration::from_secs(2)).await?;
    // budget: path RTT + overlay (scheduler/ping) + local. 4x RTT + 80ms floor.
    let budget = (ms * 4 + 80).max(ms + 80);
    Ok(finish(
        &format!("delay_{ms}ms"),
        &h,
        stats,
        Sla::healthy(budget),
        None,
    ))
}

pub async fn hetero_delay() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 60, 150])).await?;
    let stats = ping_stream(&h, Duration::from_secs(3)).await?;
    // sticky to fastest (~10ms); p99 should stay near that, not 150.
    Ok(finish(
        "hetero_10_60_150",
        &h,
        stats,
        Sla::healthy(100),
        None,
    ))
}

pub async fn jitter_on_fast() -> Result<ScenarioReport> {
    let mut spec = three_paths([10, 60, 150]);
    spec.link_cfgs[0].1.jitter = Duration::from_millis(15);
    let h = start(spec).await?;
    let stats = ping_stream(&h, Duration::from_secs(3)).await?;
    Ok(finish(
        "jitter_15ms_on_a",
        &h,
        stats,
        Sla::healthy(120),
        None,
    ))
}

pub async fn loss_on_one(pct: f64) -> Result<ScenarioReport> {
    let mut spec = three_paths([10, 60, 150]);
    spec.link_cfgs[0].1.loss = pct;
    let h = start(spec).await?;
    let stats = ping_stream(&h, Duration::from_secs(4)).await?;
    let name = if (pct * 1000.0).round() as u32 == 1 {
        "loss_a_0p1pct".into()
    } else {
        format!("loss_a_{}pct", (pct * 100.0).round() as u32)
    };
    // 1–3% stall-loss may add a single ~400ms sample before migrate.
    let budget = if pct >= 0.01 { 500 } else { 200 };
    Ok(finish(&name, &h, stats, Sla::healthy(budget), None))
}

pub async fn timed_spike() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 60, 150])).await?;
    let mut tcp = h.connect_forward().await?;
    let warm = ping_for(&mut tcp, Duration::from_millis(400), PING, PING_TO).await;
    let t0 = Instant::now();
    h.link("a")
        .spike(Duration::from_millis(400), Duration::from_millis(800));
    let rest = ping_for(&mut tcp, Duration::from_secs(3), PING, PING_TO).await;
    let mut stats = warm;
    stats.samples.extend(rest.samples);
    stats.timeouts += rest.timeouts;
    stats.io_errors += rest.io_errors;
    stats.bytes_ok += rest.bytes_ok;
    stats.disconnect |= rest.disconnect;
    let gap = stats.gap_around(t0).as_millis() as u64;
    Ok(finish(
        "timed_spike_a",
        &h,
        stats,
        Sla::failover(500, 1200),
        Some(gap),
    ))
}

pub async fn random_spikes() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 60, 150])).await?;
    let mut tcp = h.connect_forward().await?;
    let h2 = h.link("a").clone();
    tokio::spawn(async move {
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(400)).await;
            h2.spike(Duration::from_millis(250), Duration::from_millis(200));
        }
    });
    let stats = ping_for(&mut tcp, Duration::from_secs(4), PING, PING_TO).await;
    Ok(finish(
        "random_spikes_a",
        &h,
        stats,
        Sla::failover(500, 1500),
        None,
    ))
}

pub async fn blackhole_one(dur: Duration) -> Result<ScenarioReport> {
    let h = start(three_paths([10, 60, 150])).await?;
    let mut tcp = h.connect_forward().await?;
    let _warm = ping_for(&mut tcp, Duration::from_millis(500), PING, PING_TO).await;
    let t0 = Instant::now();
    h.link("a").set_blackhole(true);
    info!(?dur, "blackhole a start");
    let during = ping_for(&mut tcp, dur, PING, PING_TO).await;
    h.link("a").set_blackhole(false);
    let after = ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await;
    let mut stats = during;
    stats.samples.extend(after.samples.clone());
    stats.timeouts += after.timeouts;
    stats.io_errors += after.io_errors;
    stats.bytes_ok += after.bytes_ok;
    stats.disconnect |= after.disconnect;
    let gap = stats.gap_around(t0).as_millis() as u64;
    let name = format!("blackhole_a_{}s", dur.as_secs());
    // other two paths remain; must survive, failover < 1s
    Ok(finish(
        &name,
        &h,
        stats,
        Sla::failover(400, 1000),
        Some(gap),
    ))
}

/// Two 10ms links; blackhole one. Speculative migrate must hop at
/// DEGRADED (~50ms), not wait for down (~330ms). Hetero `blackhole_a_*`
/// does not hop 10→60 while the path is still alive.
pub async fn blackhole_same_class() -> Result<ScenarioReport> {
    let h = start(HarnessSpec {
        link_cfgs: vec![
            (
                "a".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(10),
                    ..Default::default()
                },
            ),
            (
                "b".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(10),
                    ..Default::default()
                },
            ),
        ],
        psk: "e2e-psk".into(),
        ..Default::default()
    })
    .await?;
    let mut tcp = h.connect_forward().await?;
    let _warm = ping_for(&mut tcp, Duration::from_millis(500), PING, PING_TO).await;
    let t0 = Instant::now();
    h.link("a").set_blackhole(true);
    let rest = ping_for(&mut tcp, Duration::from_secs(3), PING, PING_TO).await;
    let gap = rest.gap_around(t0).as_millis() as u64;
    Ok(finish(
        "blackhole_same_class",
        &h,
        rest,
        Sla::failover(80, 200),
        Some(gap),
    ))
}

pub async fn blackhole_all(dur: Duration) -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    let mut tcp = h.connect_forward().await?;
    let _warm = ping_for(&mut tcp, Duration::from_millis(400), PING, PING_TO).await;
    for l in &h.links {
        l.set_blackhole(true);
    }
    let during = ping_for(&mut tcp, dur, PING, Duration::from_millis(400)).await;
    for l in &h.links {
        l.set_blackhole(false);
    }
    // Give reconnect a moment once the hole lifts.
    let _ = h.session.wait_ready(Duration::from_secs(4)).await;
    let after = ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await;
    let name = format!("blackhole_all_{}s", dur.as_secs());
    if dur < Duration::from_secs(8) {
        // Application TCP must survive; recovery window must actually ping.
        let sla = Sla {
            must_survive: true,
            p99_ms: Some(200),
            failover_ms: None,
            min_success: 0.8,
        };
        let mut r = finish(&name, &h, after, sla, None);
        r.stats.disconnect |= during.disconnect;
        r.notes.push(format!(
            "during_hole timeouts={} survive={}",
            during.timeouts, !during.disconnect
        ));
        Ok(r)
    } else {
        let sla = Sla {
            must_survive: false,
            p99_ms: None,
            failover_ms: None,
            min_success: 0.0,
        };
        let mut r = finish(&name, &h, after, sla, None);
        r.stats.disconnect |= during.disconnect;
        r.notes
            .push("RST after all-down timeout is expected".into());
        Ok(r)
    }
}

fn fleet_3x10_2x60() -> HarnessSpec {
    HarnessSpec {
        link_cfgs: vec![
            (
                "f1".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(10),
                    ..Default::default()
                },
            ),
            (
                "f2".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(12),
                    ..Default::default()
                },
            ),
            (
                "f3".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(11),
                    ..Default::default()
                },
            ),
            (
                "b1".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(60),
                    ..Default::default()
                },
            ),
            (
                "b2".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(65),
                    ..Default::default()
                },
            ),
        ],
        psk: "e2e-psk".into(),
        ..Default::default()
    }
}

pub async fn fleet_baseline() -> Result<ScenarioReport> {
    let h = start(fleet_3x10_2x60()).await?;
    let stats = ping_stream(&h, Duration::from_secs(3)).await?;
    Ok(finish("fleet_3x10_2x60", &h, stats, Sla::healthy(50), None))
}

pub async fn failback_after_fast_blackhole() -> Result<ScenarioReport> {
    let h = start(fleet_3x10_2x60()).await?;
    let mut tcp = h.connect_forward().await?;
    let warm = ping_for(&mut tcp, Duration::from_secs(1), PING, PING_TO).await;
    let warm_p50 = warm.percentile_us(50.0).unwrap_or(u64::MAX);
    for name in ["f1", "f2", "f3"] {
        h.link(name).set_blackhole(true);
    }
    let mid = ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await;
    let mid_p50 = mid.percentile_us(50.0).unwrap_or(0);
    let mid_ok = mid.success_rate();
    for name in ["f1", "f2", "f3"] {
        h.link(name).set_blackhole(false);
    }
    // wait for failback_stable (~800ms) plus probes
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let after = ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await;
    let after_p50 = after.percentile_us(50.0).unwrap_or(u64::MAX);
    let mut stats = warm;
    stats.samples.extend(mid.samples);
    stats.samples.extend(after.samples.clone());
    stats.disconnect |= mid.disconnect || after.disconnect;
    stats.timeouts += mid.timeouts + after.timeouts;
    stats.io_errors += mid.io_errors + after.io_errors;
    stats.bytes_ok += mid.bytes_ok + after.bytes_ok;
    let sla = Sla {
        must_survive: true,
        p99_ms: Some(120),
        failover_ms: None,
        min_success: 0.85,
    };
    let mut r = finish("failback_fast_paths", &h, stats, sla, None);
    r.notes.push(format!(
        "p50_warm={warm_p50}us p50_backup={mid_p50}us p50_after={after_p50}us failbacks={}",
        r.snap.failbacks
    ));
    // After restore, p50 should return near the 10ms class, not stay on 60ms.
    if after_p50 > 35_000 {
        r.notes
            .push("FAILBACK_INCOMPLETE: still on high-rtt path".into());
        r.sla.min_success = 2.0; // force fail
    }
    if mid_p50 < 40_000 && mid_ok > 0.5 {
        // backup was used; good
    }
    Ok(r)
}

pub async fn chaos_independent() -> Result<ScenarioReport> {
    let h = start(fleet_3x10_2x60()).await?;
    let mut tcp = h.connect_forward().await?;
    let links: Vec<_> = h.links.clone();
    tokio::spawn(async move {
        // overlapping, independent faults
        tokio::time::sleep(Duration::from_millis(300)).await;
        links[0].set_loss(0.03);
        tokio::time::sleep(Duration::from_millis(400)).await;
        links[1].spike(Duration::from_millis(200), Duration::from_millis(500));
        tokio::time::sleep(Duration::from_millis(200)).await;
        links[2].set_blackhole(true);
        tokio::time::sleep(Duration::from_secs(1)).await;
        links[2].set_blackhole(false);
        tokio::time::sleep(Duration::from_millis(300)).await;
        links[0].set_loss(0.0);
        links[3].disconnect_all();
        tokio::time::sleep(Duration::from_millis(400)).await;
        links[1].set_rtt(Duration::from_millis(80));
        tokio::time::sleep(Duration::from_millis(500)).await;
        links[1].set_rtt(Duration::from_millis(12));
    });
    let stats = ping_for(&mut tcp, Duration::from_secs(5), PING, PING_TO).await;
    Ok(finish(
        "chaos_independent",
        &h,
        stats,
        Sla {
            must_survive: true,
            p99_ms: Some(800),
            failover_ms: None,
            min_success: 0.75,
        },
        None,
    ))
}

pub async fn ip_loss_retransmit() -> Result<ScenarioReport> {
    let spec = fleet_3x10_2x60();
    let h = start(spec).await?;
    h.link("f1").set_loss(0.03);
    h.link("f2").set_loss(0.01);
    let stats = ping_stream(&h, Duration::from_secs(4)).await?;
    let retrans: u64 = h.links.iter().map(|l| l.stats().retrans).sum();
    let drops: u64 = h.links.iter().map(|l| l.stats().drops).sum();
    let mut r = finish("ip_loss_retransmit", &h, stats, Sla::healthy(250), None);
    r.notes
        .push(format!("wan_drops={drops} wan_retrans={retrans}"));
    if drops > 0 && retrans == 0 {
        r.notes.push("no WAN retransmit observed".into());
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

pub async fn flash_and_return() -> Result<ScenarioReport> {
    let h = start(fleet_3x10_2x60()).await?;
    let mut tcp = h.connect_forward().await?;
    let _ = ping_for(&mut tcp, Duration::from_millis(400), PING, PING_TO).await;
    h.link("f1").disconnect_all();
    tokio::time::sleep(Duration::from_millis(200)).await;
    // f1 will reconnect via client supervisor; keep pinging
    let stats = ping_for(&mut tcp, Duration::from_secs(3), PING, PING_TO).await;
    Ok(finish(
        "flash_disconnect_f1",
        &h,
        stats,
        Sla::failover(80, 1000),
        None,
    ))
}

fn merge_stats(into: &mut WorkloadStats, add: &WorkloadStats) {
    into.samples.extend(add.samples.clone());
    into.timeouts += add.timeouts;
    into.io_errors += add.io_errors;
    into.bytes_ok += add.bytes_ok;
    into.disconnect |= add.disconnect;
}

/// All 10ms-class paths take a delay spike; traffic should sit on 60ms
/// backups, then fail back to the 10ms class once the spike lifts.
pub async fn failback_after_spike() -> Result<ScenarioReport> {
    let h = start(fleet_3x10_2x60()).await?;
    let mut tcp = h.connect_forward().await?;
    let warm = ping_for(&mut tcp, Duration::from_secs(1), PING, PING_TO).await;
    for name in ["f1", "f2", "f3"] {
        h.link(name)
            .spike(Duration::from_millis(400), Duration::from_millis(800));
    }
    let mid = ping_for(&mut tcp, Duration::from_millis(1200), PING, PING_TO).await;
    let mid_p50 = mid.percentile_us(50.0).unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let after = ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await;
    let after_p50 = after.percentile_us(50.0).unwrap_or(u64::MAX);
    let mut stats = warm;
    merge_stats(&mut stats, &mid);
    merge_stats(&mut stats, &after);
    let sla = Sla {
        must_survive: true,
        p99_ms: Some(500),
        failover_ms: None,
        min_success: 0.85,
    };
    let mut r = finish("failback_after_spike", &h, stats, sla, None);
    r.notes.push(format!(
        "p50_backup={mid_p50}us p50_after={after_p50}us failbacks={}",
        r.snap.failbacks
    ));
    if after_p50 > 35_000 {
        r.notes
            .push("FAILBACK_INCOMPLETE: still on high-rtt path".into());
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

/// Mutate all fast-path RTTs up into the backup class, then restore.
pub async fn delay_shift_and_restore() -> Result<ScenarioReport> {
    let h = start(fleet_3x10_2x60()).await?;
    let mut tcp = h.connect_forward().await?;
    let warm = ping_for(&mut tcp, Duration::from_secs(1), PING, PING_TO).await;
    for name in ["f1", "f2", "f3"] {
        h.link(name).set_rtt(Duration::from_millis(80));
    }
    let mid = ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await;
    let mid_p50 = mid.percentile_us(50.0).unwrap_or(0);
    h.link("f1").set_rtt(Duration::from_millis(10));
    h.link("f2").set_rtt(Duration::from_millis(12));
    h.link("f3").set_rtt(Duration::from_millis(11));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let after = ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await;
    let after_p50 = after.percentile_us(50.0).unwrap_or(u64::MAX);
    let mut stats = warm;
    merge_stats(&mut stats, &mid);
    merge_stats(&mut stats, &after);
    let sla = Sla {
        must_survive: true,
        p99_ms: Some(200),
        failover_ms: None,
        min_success: 0.90,
    };
    let mut r = finish("delay_shift_restore", &h, stats, sla, None);
    r.notes.push(format!(
        "p50_shifted={mid_p50}us p50_after={after_p50}us failbacks={}",
        r.snap.failbacks
    ));
    if after_p50 > 35_000 {
        r.notes
            .push("FAILBACK_INCOMPLETE: still on high-rtt path".into());
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

/// One fast path is RST'd for a couple of seconds; the other 10ms links
/// must keep the stream in the 10ms class, and the dead link returns.
pub async fn offline_then_return() -> Result<ScenarioReport> {
    let h = start(fleet_3x10_2x60()).await?;
    let mut tcp = h.connect_forward().await?;
    let warm = ping_for(&mut tcp, Duration::from_millis(400), PING, PING_TO).await;
    h.link("f1").disconnect_all();
    let mid = ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await;
    let after = ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await;
    let after_p50 = after.percentile_us(50.0).unwrap_or(u64::MAX);
    let mut stats = warm;
    merge_stats(&mut stats, &mid);
    merge_stats(&mut stats, &after);
    let sla = Sla {
        must_survive: true,
        p99_ms: Some(80),
        failover_ms: None,
        min_success: 0.90,
    };
    let mut r = finish("offline_then_return", &h, stats, sla, None);
    r.notes.push(format!(
        "p50_after={after_p50}us path_down={} migrates={}",
        r.snap.path_down, r.snap.migrates
    ));
    if after_p50 > 35_000 {
        r.notes
            .push("stayed on backup after other fast paths were up".into());
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

pub async fn disconnect_one() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 60, 150])).await?;
    let mut tcp = h.connect_forward().await?;
    let _warm = ping_for(&mut tcp, Duration::from_millis(400), PING, PING_TO).await;
    let t0 = Instant::now();
    h.link("a").disconnect_all();
    let rest = ping_for(&mut tcp, Duration::from_secs(3), PING, PING_TO).await;
    let gap = rest.gap_around(t0).as_millis() as u64;
    Ok(finish(
        "disconnect_a",
        &h,
        rest,
        Sla::failover(200, 1000),
        Some(gap),
    ))
}

fn one_link(ms: u64, connections: u32) -> HarnessSpec {
    HarnessSpec {
        link_cfgs: vec![(
            "a".into(),
            ImpairConfig {
                rtt: Duration::from_millis(ms),
                ..Default::default()
            },
        )],
        connections,
        psk: "e2e-psk".into(),
    }
}

/// One 10ms link with 3 TCP connections; traffic stays in the 10ms class.
pub async fn multi_conn_baseline() -> Result<ScenarioReport> {
    let h = start(one_link(10, 3)).await?;
    let stats = ping_stream(&h, Duration::from_secs(2)).await?;
    let n = h.session.alive_path_count();
    let mut r = finish("multi_conn_baseline", &h, stats, Sla::healthy(50), None);
    r.notes.push(format!(
        "alive_paths={n} impair_conns={}",
        h.link("a").live_conn_count()
    ));
    if n < 3 {
        r.notes
            .push("expected 3 overlay TCP connections on the link".into());
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

/// Blackhole 2 of 3 connections on a single link. The remaining conn
/// must keep the application TCP alive at ~10ms — not crash or hang.
pub async fn one_conn_blackhole() -> Result<ScenarioReport> {
    let h = start(one_link(10, 3)).await?;
    let mut tcp = h.connect_forward().await?;
    let _warm = ping_for(&mut tcp, Duration::from_millis(400), PING, PING_TO).await;
    let t0 = Instant::now();
    h.link("a").set_conn_blackhole(0, true);
    h.link("a").set_conn_blackhole(1, true);
    let rest = ping_for(&mut tcp, Duration::from_secs(3), PING, PING_TO).await;
    let gap = rest.gap_around(t0).as_millis() as u64;
    let p50 = rest.percentile_us(50.0).unwrap_or(u64::MAX);
    let mut r = finish(
        "one_conn_blackhole",
        &h,
        rest,
        Sla::failover(80, 1000),
        Some(gap),
    );
    r.notes.push(format!(
        "p50={p50}us migrates={} drops={} paths={}",
        r.snap.migrates,
        r.snap.frame_send_drop,
        h.session.alive_path_count()
    ));
    Ok(r)
}

/// Production shape: three named ISPs × two overlay TCPs, ~10 ms class.
fn prod_like_spec() -> HarnessSpec {
    let rtt = Duration::from_millis(10);
    HarnessSpec {
        link_cfgs: vec![
            (
                "akcdn".into(),
                ImpairConfig {
                    rtt,
                    ..Default::default()
                },
            ),
            (
                "soy".into(),
                ImpairConfig {
                    rtt,
                    ..Default::default()
                },
            ),
            (
                "nsix".into(),
                ImpairConfig {
                    rtt,
                    ..Default::default()
                },
            ),
        ],
        connections: 2,
        psk: "e2e-psk".into(),
    }
}

fn first_byte_sla(p99_ms: u64, min_success: f64) -> Sla {
    Sla {
        must_survive: true,
        p99_ms: Some(p99_ms),
        failover_ms: None,
        min_success,
    }
}

/// SOCKS CONNECT + first echo byte. Times the production 204/curl path.
async fn socks_first_byte(h: &Harness, payload: &[u8], to: Duration) -> Result<Duration, ()> {
    let t0 = Instant::now();
    let got = tokio::time::timeout(to, async {
        let mut tcp = h.connect_socks_echo().await?;
        tcp.write_all(payload).await?;
        let mut buf = vec![0u8; payload.len()];
        tcp.read_exact(&mut buf).await?;
        anyhow::Ok(t0.elapsed())
    })
    .await;
    match got {
        Ok(Ok(d)) => Ok(d),
        _ => Err(()),
    }
}

async fn collect_first_bytes(h: &Harness, n: usize, payload: &[u8], to: Duration) -> WorkloadStats {
    let mut stats = WorkloadStats::default();
    for _ in 0..n {
        let t0 = Instant::now();
        match socks_first_byte(h, payload, to).await {
            Ok(d) => {
                stats.samples.push(crate::workload::PingSample {
                    at: t0,
                    rtt: Some(d),
                });
                stats.bytes_ok += payload.len() as u64;
            }
            Err(()) => {
                stats.timeouts += 1;
                stats
                    .samples
                    .push(crate::workload::PingSample { at: t0, rtt: None });
            }
        }
    }
    stats
}

fn note_prod_like(r: &mut ScenarioReport, h: &Harness, baseline: Duration) {
    r.notes.push(format!(
        "baseline={:?} hedge={} rtx={} alive={}",
        baseline,
        r.snap.data_hedge,
        r.snap.data_retransmit,
        h.session.alive_path_count()
    ));
}

/// Three named links × two overlay TCPs (prod shape). One 5-tuple is
/// blackholed for many RTTs; new-stream first-byte must recover via
/// loss_timeout retry, not wait for path-down.
pub async fn prod_like_one_conn_hole_first_byte() -> Result<ScenarioReport> {
    let h = start(prod_like_spec()).await?;
    let payload = vec![0u8; 2048];
    let mut baseline = Duration::MAX;
    for _ in 0..3 {
        baseline = baseline.min(
            socks_first_byte(&h, &payload, Duration::from_millis(400))
                .await
                .map_err(|_| anyhow!("baseline first-byte timed out"))?,
        );
    }
    h.link("akcdn").set_conn_blackhole(0, true);
    let stats = collect_first_bytes(&h, 16, &payload, Duration::from_millis(250)).await;
    h.link("akcdn").set_conn_blackhole(0, false);
    let mut r = finish(
        "prod_like_one_conn_hole_first_byte",
        &h,
        stats,
        first_byte_sla(120, 0.95),
        None,
    );
    note_prod_like(&mut r, &h, baseline);
    if r.snap.session_all_down_resets != 0 || r.snap.stream_resets_timeout > 2 {
        r.sla.min_success = 2.0;
        r.notes.push(format!(
            "hygiene all_down={} timeout={}",
            r.snap.session_all_down_resets, r.snap.stream_resets_timeout
        ));
    }
    Ok(r)
}

/// Whole named link (both 5-tuples) blackholed; the other two ISPs stay up.
pub async fn prod_like_one_link_hole_first_byte() -> Result<ScenarioReport> {
    let h = start(prod_like_spec()).await?;
    let payload = vec![0u8; 2048];
    let mut baseline = Duration::MAX;
    for _ in 0..3 {
        baseline = baseline.min(
            socks_first_byte(&h, &payload, Duration::from_millis(400))
                .await
                .map_err(|_| anyhow!("baseline first-byte timed out"))?,
        );
    }
    h.link("akcdn").set_conn_blackhole(0, true);
    h.link("akcdn").set_conn_blackhole(1, true);
    let stats = collect_first_bytes(&h, 16, &payload, Duration::from_millis(250)).await;
    h.link("akcdn").set_conn_blackhole(0, false);
    h.link("akcdn").set_conn_blackhole(1, false);
    let mut r = finish(
        "prod_like_one_link_hole_first_byte",
        &h,
        stats,
        first_byte_sla(120, 0.95),
        None,
    );
    note_prod_like(&mut r, &h, baseline);
    if r.snap.session_all_down_resets != 0 || r.snap.stream_resets_timeout > 2 {
        r.sla.min_success = 2.0;
        r.notes.push(format!(
            "hygiene all_down={} timeout={}",
            r.snap.session_all_down_resets, r.snap.stream_resets_timeout
        ));
    }
    Ok(r)
}

/// Concurrent new SOCKS streams while one 5-tuple is blackholed.
pub async fn prod_like_concurrent_hole_first_byte() -> Result<ScenarioReport> {
    let h = start(prod_like_spec()).await?;
    let payload = vec![0u8; 2048];
    let mut baseline = Duration::MAX;
    for _ in 0..3 {
        baseline = baseline.min(
            socks_first_byte(&h, &payload, Duration::from_millis(400))
                .await
                .map_err(|_| anyhow!("baseline first-byte timed out"))?,
        );
    }
    h.link("akcdn").set_conn_blackhole(0, true);
    let socks = h.socks;
    let echo = h.echo;
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let payload = payload.clone();
        set.spawn(async move {
            let t0 = Instant::now();
            let got = tokio::time::timeout(Duration::from_millis(250), async {
                let mut tcp = crate::harness::socks5_connect(socks, echo).await?;
                tcp.write_all(&payload).await?;
                let mut buf = vec![0u8; payload.len()];
                tcp.read_exact(&mut buf).await?;
                anyhow::Ok(t0.elapsed())
            })
            .await;
            match got {
                Ok(Ok(d)) => (t0, Ok(d)),
                _ => (t0, Err(())),
            }
        });
    }
    let mut stats = WorkloadStats::default();
    while let Some(j) = set.join_next().await {
        match j {
            Ok((t0, Ok(d))) => {
                stats.samples.push(crate::workload::PingSample {
                    at: t0,
                    rtt: Some(d),
                });
                stats.bytes_ok += payload.len() as u64;
            }
            Ok((t0, Err(()))) => {
                stats.timeouts += 1;
                stats
                    .samples
                    .push(crate::workload::PingSample { at: t0, rtt: None });
            }
            Err(_) => {
                stats.timeouts += 1;
                stats.samples.push(crate::workload::PingSample {
                    at: Instant::now(),
                    rtt: None,
                });
            }
        }
    }
    h.link("akcdn").set_conn_blackhole(0, false);
    let mut r = finish(
        "prod_like_concurrent_hole_first_byte",
        &h,
        stats,
        first_byte_sla(180, 0.90),
        None,
    );
    note_prod_like(&mut r, &h, baseline);
    Ok(r)
}

/// Close queued into a silent-but-UP 5-tuple; FIN must arrive without linger Timeout.
pub async fn prod_like_close_swallowed() -> Result<ScenarioReport> {
    let h = start(prod_like_spec()).await?;
    let payload = vec![0u8; 256];
    let mut tcp = h.connect_socks_echo().await?;
    tcp.write_all(&payload).await?;
    let mut buf = vec![0u8; payload.len()];
    tcp.read_exact(&mut buf).await?;
    let to0 = h.session.snapshot().stream_resets_timeout;
    for (link, idx) in [
        ("akcdn", 0),
        ("akcdn", 1),
        ("soy", 0),
        ("soy", 1),
        ("nsix", 0),
    ] {
        h.link(link).set_conn_blackhole(idx, true);
    }
    tcp.write_all(&payload).await?;
    tcp.read_exact(&mut buf).await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    h.link("nsix").set_conn_blackhole(1, true);
    let t0 = Instant::now();
    tcp.shutdown().await?;
    for (link, idx) in [
        ("akcdn", 0),
        ("akcdn", 1),
        ("soy", 0),
        ("soy", 1),
        ("nsix", 0),
        ("nsix", 1),
    ] {
        h.link(link).set_conn_blackhole(idx, false);
    }
    let eof = tokio::time::timeout(Duration::from_millis(400), async {
        let mut one = [0u8; 1];
        tcp.read(&mut one).await
    })
    .await;
    let ok0 = matches!(eof, Ok(Ok(0)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let snap = h.session.snapshot();
    let mut stats = WorkloadStats::default();
    if ok0 {
        stats.samples.push(crate::workload::PingSample {
            at: t0,
            rtt: Some(t0.elapsed()),
        });
        stats.bytes_ok = 1;
    } else {
        stats.timeouts = 1;
        stats
            .samples
            .push(crate::workload::PingSample { at: t0, rtt: None });
    }
    let mut r = finish(
        "prod_like_close_swallowed",
        &h,
        stats,
        Sla {
            must_survive: true,
            p99_ms: Some(400),
            failover_ms: None,
            min_success: 1.0,
        },
        None,
    );
    r.notes.push(format!(
        "eof={ok0} close_retry={} timeout_delta={} all_down={}",
        snap.close_retry,
        snap.stream_resets_timeout.saturating_sub(to0),
        snap.session_all_down_resets
    ));
    if snap.stream_resets_timeout.saturating_sub(to0) != 0 || snap.session_all_down_resets != 0 {
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

/// Both akcdn and soy blackholed; nsix stays up (no A↔B ping-pong).
pub async fn prod_like_two_isp_hole_first_byte() -> Result<ScenarioReport> {
    let h = start(prod_like_spec()).await?;
    let payload = vec![0u8; 2048];
    let mut baseline = Duration::MAX;
    for _ in 0..3 {
        baseline = baseline.min(
            socks_first_byte(&h, &payload, Duration::from_millis(400))
                .await
                .map_err(|_| anyhow!("baseline first-byte timed out"))?,
        );
    }
    h.link("akcdn").set_conn_blackhole(0, true);
    h.link("akcdn").set_conn_blackhole(1, true);
    h.link("soy").set_conn_blackhole(0, true);
    h.link("soy").set_conn_blackhole(1, true);
    let stats = collect_first_bytes(&h, 16, &payload, Duration::from_millis(250)).await;
    h.link("akcdn").set_conn_blackhole(0, false);
    h.link("akcdn").set_conn_blackhole(1, false);
    h.link("soy").set_conn_blackhole(0, false);
    h.link("soy").set_conn_blackhole(1, false);
    let mut r = finish(
        "prod_like_two_isp_hole_first_byte",
        &h,
        stats,
        first_byte_sla(180, 0.95),
        None,
    );
    note_prod_like(&mut r, &h, baseline);
    if r.snap.session_all_down_resets != 0 {
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

/// All six overlay TCPs blackholed < all_down_timeout; session must not reset all streams.
pub async fn prod_like_all_path_blackhole() -> Result<ScenarioReport> {
    let h = start(prod_like_spec()).await?;
    let payload = vec![0u8; 256];
    socks_first_byte(&h, &payload, Duration::from_millis(400))
        .await
        .map_err(|_| anyhow!("baseline first-byte timed out"))?;
    for name in ["akcdn", "soy", "nsix"] {
        h.link(name).set_conn_blackhole(0, true);
        h.link(name).set_conn_blackhole(1, true);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let during = h.session.snapshot();
    for name in ["akcdn", "soy", "nsix"] {
        h.link(name).set_conn_blackhole(0, false);
        h.link(name).set_conn_blackhole(1, false);
    }
    let recovered = socks_first_byte(&h, &payload, Duration::from_millis(400)).await;
    let mut stats = WorkloadStats::default();
    match recovered {
        Ok(d) => {
            stats.samples.push(crate::workload::PingSample {
                at: Instant::now(),
                rtt: Some(d),
            });
            stats.bytes_ok = 1;
        }
        Err(()) => {
            stats.timeouts = 1;
            stats.samples.push(crate::workload::PingSample {
                at: Instant::now(),
                rtt: None,
            });
        }
    }
    let mut r = finish(
        "prod_like_all_path_blackhole",
        &h,
        stats,
        Sla {
            must_survive: true,
            p99_ms: Some(400),
            failover_ms: None,
            min_success: 1.0,
        },
        None,
    );
    r.notes.push(format!(
        "during_all_down={} corr={} recovered={}",
        during.session_all_down_resets,
        during.correlated_silence,
        recovered.is_ok()
    ));
    if during.session_all_down_resets != 0 {
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

/// Stall client→server on 2 of 3 connections (TCP send buffer / HOL).
/// Sibling connections must pick up the stream.
pub async fn one_conn_stall() -> Result<ScenarioReport> {
    let h = start(one_link(10, 3)).await?;
    let mut tcp = h.connect_forward().await?;
    let _warm = ping_for(&mut tcp, Duration::from_millis(400), PING, PING_TO).await;
    let t0 = Instant::now();
    h.link("a").set_conn_stall(0, true);
    h.link("a").set_conn_stall(1, true);
    let rest = ping_for(&mut tcp, Duration::from_secs(3), PING, PING_TO).await;
    h.link("a").set_conn_stall(0, false);
    h.link("a").set_conn_stall(1, false);
    let gap = rest.gap_around(t0).as_millis() as u64;
    let mut r = finish(
        "one_conn_stall",
        &h,
        rest,
        Sla::failover(120, 1500),
        Some(gap),
    );
    r.notes.push(format!(
        "migrates={} send_drops={} alive={}",
        r.snap.migrates,
        r.snap.frame_send_drop,
        h.session.alive_path_count()
    ));
    Ok(r)
}

/// RST 2 of 3 overlay TCP connections; session and app TCP stay up.
pub async fn one_conn_disconnect() -> Result<ScenarioReport> {
    let h = start(one_link(10, 3)).await?;
    let mut tcp = h.connect_forward().await?;
    let _warm = ping_for(&mut tcp, Duration::from_millis(400), PING, PING_TO).await;
    let t0 = Instant::now();
    h.link("a").disconnect_conn(0);
    h.link("a").disconnect_conn(0);
    let rest = ping_for(&mut tcp, Duration::from_secs(3), PING, PING_TO).await;
    let gap = rest.gap_around(t0).as_millis() as u64;
    Ok(finish(
        "one_conn_disconnect",
        &h,
        rest,
        Sla::failover(80, 1000),
        Some(gap),
    ))
}

/// Repeatedly kill a connection; reconnect must not take the session down.
pub async fn conn_churn() -> Result<ScenarioReport> {
    let h = start(one_link(10, 3)).await?;
    let mut tcp = h.connect_forward().await?;
    let link = h.link("a").clone();
    tokio::spawn(async move {
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(350)).await;
            link.disconnect_conn(0);
        }
    });
    let stats = ping_for(&mut tcp, Duration::from_secs(3), PING, PING_TO).await;
    let mut r = finish(
        "conn_churn",
        &h,
        stats,
        Sla {
            must_survive: true,
            p99_ms: Some(200),
            failover_ms: None,
            min_success: 0.80,
        },
        None,
    );
    r.notes.push(format!(
        "path_down={} path_added={} resets={}",
        r.snap.path_down, r.snap.path_added, r.snap.stream_resets
    ));
    Ok(r)
}

/// A stream that never reads must not stall other streams or the session.
pub async fn slow_consumer() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    let mut hog = h.connect_forward().await?;
    tokio::spawn(async move {
        let buf = vec![0x5au8; 32 * 1024];
        for _ in 0..64 {
            if hog.write_all(&buf).await.is_err() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(4)).await;
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    let stats = ping_stream(&h, Duration::from_secs(2)).await?;
    let mut r = finish("slow_consumer", &h, stats, Sla::healthy(120), None);
    r.notes.push(format!(
        "resets={} alive={}",
        r.snap.stream_resets,
        h.session.alive_path_count()
    ));
    Ok(r)
}

/// Many concurrent application TCPs: no crash, high success, session stays up.
pub async fn many_concurrent_streams() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    let mut joins = Vec::new();
    for _ in 0..24 {
        let mut tcp = h.connect_forward().await?;
        joins.push(tokio::spawn(async move {
            ping_for(&mut tcp, Duration::from_secs(2), PING, PING_TO).await
        }));
    }
    let mut stats = WorkloadStats::default();
    let mut failed = 0u64;
    for j in joins {
        match j.await {
            Ok(s) => merge_stats(&mut stats, &s),
            Err(_) => failed += 1,
        }
    }
    let mut r = finish(
        "many_concurrent_streams",
        &h,
        stats,
        Sla {
            must_survive: true,
            p99_ms: Some(150),
            failover_ms: None,
            min_success: 0.90,
        },
        None,
    );
    r.notes.push(format!(
        "join_fail={failed} resets={} paths={}",
        r.snap.stream_resets,
        h.session.alive_path_count()
    ));
    if failed > 0 {
        r.notes.push("a ping task panicked".into());
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

/// Bulk transfer on one stream must not kill a latency-sensitive neighbour.
pub async fn bulk_plus_ping() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    let mut bulk = h.connect_forward().await?;
    let bulk_task = tokio::spawn(async move { bulk_echo(&mut bulk, 256 * 1024).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let stats = ping_stream(&h, Duration::from_secs(2)).await?;
    let bulk_ok = match bulk_task.await {
        Ok(Ok((_, intact))) => intact,
        _ => false,
    };
    let mut r = finish("bulk_plus_ping", &h, stats, Sla::healthy(150), None);
    r.notes.push(format!("bulk_ok={bulk_ok}"));
    if !bulk_ok {
        r.notes.push("bulk transfer corrupted or failed".into());
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

fn link_key(name: &str) -> &str {
    name.rsplit_once('#').map(|(l, _)| l).unwrap_or(name)
}

/// 200ms extra on a 12ms path must migrate, not tear TCP.
pub async fn delay_spike_keeps_tcp() -> Result<ScenarioReport> {
    let h = start(three_paths([12, 12, 12])).await?;
    let mut tcp = h.connect_forward().await?;
    let warm = ping_for(&mut tcp, Duration::from_millis(500), PING, PING_TO).await;
    let snap0 = h.session.snapshot();
    h.link("a").set_extra(Duration::from_millis(200));
    let mid = ping_for(&mut tcp, Duration::from_millis(1200), PING, PING_TO).await;
    h.link("a").set_extra(Duration::ZERO);
    let after = ping_for(&mut tcp, Duration::from_millis(500), PING, PING_TO).await;
    let mut stats = warm;
    merge_stats(&mut stats, &mid);
    merge_stats(&mut stats, &after);
    let snap = h.session.snapshot();
    let after_p50 = after.percentile_us(50.0).unwrap_or(u64::MAX);
    let mut r = finish(
        "delay_spike_keeps_tcp",
        &h,
        stats,
        Sla {
            must_survive: true,
            p99_ms: Some(800),
            failover_ms: None,
            min_success: 0.90,
        },
        None,
    );
    r.notes.push(format!(
        "path_down {}→{} path_added {}→{} p50_after={after_p50}us",
        snap0.path_down, snap.path_down, snap0.path_added, snap.path_added
    ));
    if snap.path_down > snap0.path_down || snap.path_added > snap0.path_added {
        r.notes
            .push("delay spike tore TCP (path_down/path_added grew)".into());
        r.sla.min_success = 2.0;
    }
    if after_p50 / 1000 > 40 {
        r.notes.push("p50 after spike left 12ms class".into());
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

/// Three same-class links must hold stickies on ≥2 named links while streams are open.
pub async fn same_class_mix_warm() -> Result<ScenarioReport> {
    let h = start(three_paths([12, 14, 16])).await?;
    let mut joins = Vec::new();
    for _ in 0..3 {
        // Wait until this TCP is an overlay stream before opening the next,
        // otherwise three concurrent open_stream see sticky=0 and all pick a.
        let before = h.session.snapshot().streams_live;
        let mut tcp = h.connect_forward().await?;
        let deadline = Instant::now() + Duration::from_secs(1);
        while h.session.snapshot().streams_live <= before && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        joins.push(tokio::spawn(async move {
            ping_for(&mut tcp, Duration::from_secs(3), PING, PING_TO).await
        }));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let snap = h.session.snapshot();
    let mut stats = WorkloadStats::default();
    for j in joins {
        if let Ok(s) = j.await {
            merge_stats(&mut stats, &s);
        }
    }
    let mut by_link: std::collections::BTreeMap<String, u64> = Default::default();
    for p in &snap.paths {
        *by_link.entry(link_key(&p.name).to_string()).or_insert(0) += p.sticky;
    }
    let used: Vec<_> = by_link
        .iter()
        .filter(|(_, n)| **n > 0)
        .map(|(k, n)| format!("{k}={n}"))
        .collect();
    let mut r = finish("same_class_mix_warm", &h, stats, Sla::healthy(80), None);
    r.notes.push(format!("sticky_by_link {}", used.join(" ")));
    if snap.path_down != 0 {
        r.notes.push("unexpected path_down on quiet mix".into());
        r.sla.min_success = 2.0;
    }
    let n_links = by_link.values().filter(|n| **n > 0).count();
    let only_a = by_link.get("a").copied().unwrap_or(0) > 0
        && by_link.iter().all(|(k, n)| k == "a" || *n == 0);
    if n_links < 2 || only_a {
        r.notes
            .push("stickies did not mix across named links".into());
        r.sla.min_success = 2.0;
    }
    Ok(r)
}

async fn echo16(tcp: &mut TcpStream, seq: u64) -> Result<()> {
    let mut msg = [0u8; 16];
    msg[..8].copy_from_slice(&seq.to_be_bytes());
    tcp.write_all(&msg).await?;
    let mut got = [0u8; 16];
    tokio::time::timeout(Duration::from_millis(800), tcp.read_exact(&mut got)).await??;
    Ok(())
}

fn apply_table_invariants(
    r: &mut ScenarioReport,
    n: usize,
    after: &nya_core::SessionSnapshot,
    mig_on_flap: u64,
    io_fail: u64,
    fail_on_io: bool,
) {
    r.notes.push(format!(
        "churn={n} io_fail={io_fail} held={} live={} closed={} opened={} resets={} mig_on_flap={mig_on_flap}",
        after.streams_held,
        after.streams_live,
        after.streams_closed,
        after.streams_opened,
        after.stream_resets
    ));
    if after.streams_held > after.streams_live.saturating_add(2) {
        r.notes.push(format!(
            "stream table leak: held={} live={} after {n} short closes",
            after.streams_held, after.streams_live
        ));
        r.sla.min_success = 2.0;
    }
    if mig_on_flap > 8 {
        r.notes.push(format!(
            "migrate storm on closed streams: {mig_on_flap} (limit 8)"
        ));
        r.sla.min_success = 2.0;
    }
    if fail_on_io && io_fail > 0 {
        r.notes.push(format!("short-stream io failures: {io_fail}"));
        r.sla.min_success = 2.0;
    }
}

async fn settle_and_ping(
    h: &Harness,
    name: &str,
    n: usize,
    io_fail: u64,
    fail_on_io: bool,
) -> Result<ScenarioReport> {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after_close = h.session.snapshot();
    let mig0 = after_close.migrates;
    h.link("a").disconnect_conn(0);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let after_flap = h.session.snapshot();
    let mig_on_flap = after_flap.migrates.saturating_sub(mig0);
    let stats = ping_stream(h, Duration::from_secs(2)).await?;
    let mut r = finish(name, h, stats, Sla::healthy(120), None);
    apply_table_invariants(&mut r, n, &after_close, mig_on_flap, io_fail, fail_on_io);
    Ok(r)
}

/// Sequential short-lived application TCPs must leave the stream table empty.
///
/// Production 204 soak: each curl opened a stream, graceful close left the
/// HashMap entry, and `maintain` speculatively migrated every ghost on path
/// flap. Long-lived ping SLAs never see that.
pub async fn short_stream_churn() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    const N: usize = 64;
    let mut io_fail = 0u64;
    for i in 0..N {
        let mut tcp = h.connect_forward().await?;
        if echo16(&mut tcp, i as u64 + 1).await.is_err() {
            io_fail += 1;
        }
        drop(tcp);
    }
    settle_and_ping(&h, "short_stream_churn", N, io_fail, true).await
}

/// Same as short_stream_churn but through SOCKS5 CONNECT (curl --socks5-hostname).
pub async fn socks_short_churn() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    const N: usize = 32;
    let mut io_fail = 0u64;
    for i in 0..N {
        match h.connect_socks_echo().await {
            Ok(mut tcp) => {
                if echo16(&mut tcp, i as u64 + 1).await.is_err() {
                    io_fail += 1;
                }
            }
            Err(_) => io_fail += 1,
        }
    }
    settle_and_ping(&h, "socks_short_churn", N, io_fail, true).await
}

/// Overlapping short streams: open/close concurrently, not one-at-a-time.
pub async fn concurrent_short_churn() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    const N: usize = 24;
    let fwd = h.forward;
    let mut joins = Vec::new();
    for i in 0..N {
        joins.push(tokio::spawn(async move {
            let mut tcp = TcpStream::connect(fwd).await?;
            let _ = tcp.set_nodelay(true);
            echo16(&mut tcp, i as u64 + 1).await
        }));
    }
    let mut io_fail = 0u64;
    for j in joins {
        match j.await {
            Ok(Ok(())) => {}
            _ => io_fail += 1,
        }
    }
    settle_and_ping(&h, "concurrent_short_churn", N, io_fail, true).await
}

/// Write then drop without reading — RST/timeout style abort, not a clean echo.
pub async fn abort_unread_churn() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    const N: usize = 32;
    let mut io_fail = 0u64;
    for i in 0..N {
        match h.connect_forward().await {
            Ok(mut tcp) => {
                let mut msg = [0u8; 16];
                msg[..8].copy_from_slice(&(i as u64 + 1).to_be_bytes());
                if tcp.write_all(&msg).await.is_err() {
                    io_fail += 1;
                }
                drop(tcp);
            }
            Err(_) => io_fail += 1,
        }
    }
    settle_and_ping(&h, "abort_unread_churn", N, io_fail, false).await
}

/// Close handshake racing path flaps (StreamClose can be lost). After
/// `close_linger` the table must still drain; a new ping must be healthy.
pub async fn churn_during_path_flap() -> Result<ScenarioReport> {
    let h = start(three_paths([10, 10, 10])).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let a = h.link("a").clone();
    let b = h.link("b").clone();
    let stop_f = stop.clone();
    let flapper = tokio::spawn(async move {
        let mut i = 0u32;
        while !stop_f.load(Ordering::Relaxed) {
            if i % 2 == 0 {
                a.disconnect_conn(0);
            } else {
                b.disconnect_conn(0);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            i += 1;
        }
    });
    const N: usize = 32;
    let mut io_fail = 0u64;
    for i in 0..N {
        match h.connect_forward().await {
            Ok(mut tcp) => {
                if echo16(&mut tcp, i as u64 + 1).await.is_err() {
                    io_fail += 1;
                }
            }
            Err(_) => io_fail += 1,
        }
    }
    stop.store(true, Ordering::Relaxed);
    let _ = flapper.await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let after = h.session.snapshot();
    let stats = ping_stream(&h, Duration::from_secs(2)).await?;
    let mut r = finish("churn_during_path_flap", &h, stats, Sla::healthy(150), None);
    apply_table_invariants(&mut r, N, &after, 0, io_fail, false);
    Ok(r)
}

pub struct Scenario {
    pub name: &'static str,
    pub long: bool,
    pub run:
        fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ScenarioReport>> + Send>>,
}

macro_rules! sc {
    ($name:expr, $long:expr, $f:expr) => {
        Scenario {
            name: $name,
            long: $long,
            run: || Box::pin($f),
        }
    };
}

pub fn catalog() -> Vec<Scenario> {
    vec![
        sc!("baseline_10ms", false, baseline_10ms()),
        sc!("delay_10ms", false, delay_matrix(10)),
        sc!("delay_60ms", false, delay_matrix(60)),
        sc!("delay_150ms", false, delay_matrix(150)),
        sc!("delay_200ms", false, delay_matrix(200)),
        sc!("hetero_10_60_150", false, hetero_delay()),
        sc!("jitter_15ms_on_a", false, jitter_on_fast()),
        sc!("loss_a_0pct", false, loss_on_one(0.0)),
        sc!("loss_a_0p1pct", false, loss_on_one(0.001)),
        sc!("loss_a_1pct", false, loss_on_one(0.01)),
        sc!("loss_a_3pct", false, loss_on_one(0.03)),
        sc!("timed_spike_a", false, timed_spike()),
        sc!("random_spikes_a", false, random_spikes()),
        sc!("blackhole_same_class", false, blackhole_same_class()),
        sc!(
            "blackhole_a_5s",
            false,
            blackhole_one(Duration::from_secs(5))
        ),
        sc!(
            "blackhole_a_15s",
            false,
            blackhole_one(Duration::from_secs(15))
        ),
        sc!(
            "blackhole_a_30s",
            true,
            blackhole_one(Duration::from_secs(30))
        ),
        sc!(
            "blackhole_a_60s",
            true,
            blackhole_one(Duration::from_secs(60))
        ),
        sc!(
            "blackhole_a_300s",
            true,
            blackhole_one(Duration::from_secs(300))
        ),
        sc!(
            "blackhole_all_5s",
            false,
            blackhole_all(Duration::from_secs(5))
        ),
        sc!("disconnect_a", false, disconnect_one()),
        sc!("fleet_3x10_2x60", false, fleet_baseline()),
        sc!(
            "failback_fast_paths",
            false,
            failback_after_fast_blackhole()
        ),
        sc!("failback_after_spike", false, failback_after_spike()),
        sc!("delay_shift_restore", false, delay_shift_and_restore()),
        sc!("chaos_independent", false, chaos_independent()),
        sc!("ip_loss_retransmit", false, ip_loss_retransmit()),
        sc!("flash_disconnect_f1", false, flash_and_return()),
        sc!("offline_then_return", false, offline_then_return()),
        sc!("multi_conn_baseline", false, multi_conn_baseline()),
        sc!("one_conn_blackhole", false, one_conn_blackhole()),
        sc!(
            "prod_like_one_conn_hole_first_byte",
            false,
            prod_like_one_conn_hole_first_byte()
        ),
        sc!(
            "prod_like_one_link_hole_first_byte",
            false,
            prod_like_one_link_hole_first_byte()
        ),
        sc!(
            "prod_like_concurrent_hole_first_byte",
            false,
            prod_like_concurrent_hole_first_byte()
        ),
        sc!(
            "prod_like_close_swallowed",
            false,
            prod_like_close_swallowed()
        ),
        sc!(
            "prod_like_two_isp_hole_first_byte",
            false,
            prod_like_two_isp_hole_first_byte()
        ),
        sc!(
            "prod_like_all_path_blackhole",
            false,
            prod_like_all_path_blackhole()
        ),
        sc!("one_conn_stall", false, one_conn_stall()),
        sc!("one_conn_disconnect", false, one_conn_disconnect()),
        sc!("conn_churn", false, conn_churn()),
        sc!("slow_consumer", false, slow_consumer()),
        sc!("many_concurrent_streams", false, many_concurrent_streams()),
        sc!("bulk_plus_ping", false, bulk_plus_ping()),
        sc!("delay_spike_keeps_tcp", false, delay_spike_keeps_tcp()),
        sc!("same_class_mix_warm", false, same_class_mix_warm()),
    ]
}

fn error_report(name: &str, e: anyhow::Error) -> ScenarioReport {
    ScenarioReport {
        name: format!("{name} (error)"),
        stats: WorkloadStats::default(),
        snap: nya_core::SessionSnapshot::default(),
        sla: Sla::healthy(1),
        failover_observed_ms: None,
        notes: vec![e.to_string()],
    }
}

/// Run matching catalog entries concurrently. `jobs` is the max number of
/// independent harnesses in flight (each scenario is isolated).
pub async fn run_catalog(filter: Option<&str>, long: bool, jobs: usize) -> Vec<ScenarioReport> {
    let selected: Vec<Scenario> = catalog()
        .into_iter()
        .filter(|s| long || !s.long)
        .filter(|s| filter.map(|f| s.name.contains(f)).unwrap_or(true))
        .collect();
    run_selected(selected, jobs, "catalog").await
}

/// Isolated stream-lifecycle suite (SOCKS / concurrent / abort / flap).
/// Kept out of the p99 catalog so connect churn does not inflate neighbours.
pub async fn run_lifecycle(jobs: usize) -> Vec<ScenarioReport> {
    run_selected(
        vec![
            sc!("short_stream_churn", false, short_stream_churn()),
            sc!("socks_short_churn", false, socks_short_churn()),
            sc!("concurrent_short_churn", false, concurrent_short_churn()),
            sc!("abort_unread_churn", false, abort_unread_churn()),
            sc!("churn_during_path_flap", false, churn_during_path_flap()),
        ],
        jobs,
        "lifecycle",
    )
    .await
}

async fn run_selected(
    selected: Vec<Scenario>,
    jobs: usize,
    label: &'static str,
) -> Vec<ScenarioReport> {
    let jobs = jobs.max(1);
    info!(n = selected.len(), jobs, label, "suite start");
    let sem = Arc::new(tokio::sync::Semaphore::new(jobs));
    let mut joins = tokio::task::JoinSet::new();
    for (idx, s) in selected.into_iter().enumerate() {
        let sem = sem.clone();
        joins.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore");
            info!(name = s.name, "scenario start");
            let r = match (s.run)().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(name = s.name, error = %e, "scenario error");
                    error_report(s.name, e)
                }
            };
            info!(name = %r.name, pass = r.pass(), "scenario done");
            (idx, r)
        });
    }
    let mut indexed = Vec::new();
    while let Some(joined) = joins.join_next().await {
        match joined {
            Ok(pair) => indexed.push(pair),
            Err(e) => indexed.push((
                usize::MAX,
                error_report("join", anyhow!("task panicked: {e}")),
            )),
        }
    }
    indexed.sort_by_key(|(i, _)| *i);
    indexed.into_iter().map(|(_, r)| r).collect()
}
