use std::fmt;
use std::time::Duration;

use crate::workload::WorkloadStats;
use nya_core::{percentile, SessionSnapshot, FAILOVER_MS_BOUNDS, STALL_MS_BOUNDS};

#[derive(Clone, Debug)]
pub struct Sla {
    /// Application TCP must stay up.
    pub must_survive: bool,
    /// p99 RTT upper bound for the *steady* window (ms). None = don't check.
    pub p99_ms: Option<u64>,
    /// Max allowed gap around a fault (ms). None = don't check.
    pub failover_ms: Option<u64>,
    /// Minimum ping success rate 0–1.
    pub min_success: f64,
}

impl Sla {
    pub fn healthy(p99_ms: u64) -> Self {
        Self {
            must_survive: true,
            p99_ms: Some(p99_ms),
            failover_ms: None,
            min_success: 0.95,
        }
    }

    pub fn failover(p99_ms: u64, failover_ms: u64) -> Self {
        Self {
            must_survive: true,
            p99_ms: Some(p99_ms),
            failover_ms: Some(failover_ms),
            min_success: 0.70,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScenarioReport {
    pub name: String,
    pub stats: WorkloadStats,
    pub snap: SessionSnapshot,
    pub sla: Sla,
    pub failover_observed_ms: Option<u64>,
    pub notes: Vec<String>,
}

impl ScenarioReport {
    pub fn pass(&self) -> bool {
        if self.sla.must_survive && self.stats.disconnect {
            return false;
        }
        if self.stats.success_rate() + 1e-9 < self.sla.min_success {
            return false;
        }
        if let Some(lim) = self.sla.p99_ms {
            if let Some(p99) = self.stats.percentile_us(99.0) {
                if p99 / 1000 > lim {
                    return false;
                }
            } else if self.sla.must_survive {
                return false;
            }
        }
        if let (Some(lim), Some(obs)) = (self.sla.failover_ms, self.failover_observed_ms) {
            if obs > lim {
                return false;
            }
        }
        true
    }
}

fn fmt_ms_us(us: Option<u64>) -> String {
    match us {
        Some(u) => format!("{:.1}", u as f64 / 1000.0),
        None => "-".into(),
    }
}

fn fmt_ms_f(us: Option<f64>) -> String {
    match us {
        Some(u) => format!("{:.1}", u / 1000.0),
        None => "-".into(),
    }
}

impl ScenarioReport {
    /// One-line latency summary: min/avg/p50/p90/p99/max in milliseconds.
    pub fn latency_ms(&self) -> String {
        format!(
            "min={} avg={} p50={} p90={} p99={} max={}",
            fmt_ms_us(self.stats.min_us()),
            fmt_ms_f(self.stats.mean_us()),
            fmt_ms_us(self.stats.percentile_us(50.0)),
            fmt_ms_us(self.stats.percentile_us(90.0)),
            fmt_ms_us(self.stats.percentile_us(99.0)),
            fmt_ms_us(self.stats.max_us()),
        )
    }
}

impl ScenarioReport {
    pub fn line(&self) -> String {
        let fo = self
            .failover_observed_ms
            .map(|m| format!("{m}ms"))
            .unwrap_or_else(|| "-".into());
        let ok = if self.pass() { "PASS" } else { "FAIL" };
        format!(
            "{:<28} {ok:<5} surv={} n={}/{} ok_rate={:.3} {} gap={:<8} to={} err={} tx={} mig={} down={} drops={}",
            self.name,
            !self.stats.disconnect,
            self.stats.n_ok(),
            self.stats.n_samples(),
            self.stats.success_rate(),
            self.latency_ms(),
            fo,
            self.stats.timeouts,
            self.stats.io_errors,
            self.snap.bytes_data_tx,
            self.snap.migrates,
            self.snap.path_down,
            self.snap.frame_send_drop,
        )
    }

    fn overlay_notes(&self) -> Vec<String> {
        let stall = percentile(&self.snap.stall_ms, STALL_MS_BOUNDS, 99.0)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        let fail = percentile(&self.snap.failover_ms, FAILOVER_MS_BOUNDS, 99.0)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        let links = self
            .snap
            .links
            .iter()
            .map(|l| format!("{}:{}/{} {}ms", l.name, l.up, l.degraded, l.rtt_us / 1000))
            .collect::<Vec<_>>()
            .join(",");
        vec![
            format!(
                "overlay stall_p99={stall}ms failover_p99={fail}ms resets={} closed={} opened={} held={} live={}",
                self.snap.stream_resets,
                self.snap.streams_closed,
                self.snap.streams_opened,
                self.snap.streams_held,
                self.snap.streams_live
            ),
            format!(
                "links={links} mig spec={} down={} ens={} blk={} retransmit={} hedge={} probe_miss={} unk_pick={}/{}",
                self.snap.migrates_speculative,
                self.snap.migrates_path_down,
                self.snap.migrates_ensure_sticky,
                self.snap.migrates_send_blocked,
                self.snap.data_retransmit,
                self.snap.data_hedge,
                self.snap.probe_miss,
                self.snap.picks_unknown_over_known,
                self.snap.picks_unknown_rtt,
            ),
        ]
    }
}

impl fmt::Display for ScenarioReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.line())?;
        for n in self.overlay_notes() {
            write!(f, "\n    note: {n}")?;
        }
        for n in &self.notes {
            write!(f, "\n    note: {n}")?;
        }
        Ok(())
    }
}

/// Print the catalog / suite table used by the binary and the matrix test.
pub fn print_suite(reports: &[ScenarioReport]) {
    println!();
    println!("{:<28} {:<5} {}", "SCENARIO", "SLA", "DETAIL");
    for r in reports {
        println!("{}", r.line());
        for n in r.overlay_notes() {
            println!("    note: {n}");
        }
        for n in &r.notes {
            let show = !r.pass()
                && (n.starts_with("FAIL")
                    || n.starts_with("UNCOVERED")
                    || n.starts_with("p50=")
                    || n.contains("incomplete")
                    || n.contains("panicked")
                    || n.contains("failback chatter")
                    || n.contains("did not mix")
                    || n.contains("stream table leak")
                    || n.contains("migrate storm")
                    || n.contains("short-stream"));
            if show || n.contains("churn=") {
                println!("    note: {n}");
            }
        }
    }
    println!();
    println!(
        "{:<28} {:>5} {:>5} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}",
        "SCENARIO", "n", "ok%", "min", "avg", "p50", "p90", "p99", "max", "gap"
    );
    for r in reports {
        let pct = (r.stats.success_rate() * 100.0).round() as u64;
        let gap = r
            .failover_observed_ms
            .map(|m| format!("{m}"))
            .unwrap_or_else(|| "-".into());
        println!(
            "{:<28} {:>5} {:>5} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}",
            r.name,
            r.stats.n_samples(),
            pct,
            r.stats
                .min_us()
                .map(|u| format!("{:.1}", u as f64 / 1000.0))
                .unwrap_or_else(|| "-".into()),
            r.stats
                .mean_us()
                .map(|u| format!("{:.1}", u / 1000.0))
                .unwrap_or_else(|| "-".into()),
            r.stats
                .percentile_us(50.0)
                .map(|u| format!("{:.1}", u as f64 / 1000.0))
                .unwrap_or_else(|| "-".into()),
            r.stats
                .percentile_us(90.0)
                .map(|u| format!("{:.1}", u as f64 / 1000.0))
                .unwrap_or_else(|| "-".into()),
            r.stats
                .percentile_us(99.0)
                .map(|u| format!("{:.1}", u as f64 / 1000.0))
                .unwrap_or_else(|| "-".into()),
            r.stats
                .max_us()
                .map(|u| format!("{:.1}", u as f64 / 1000.0))
                .unwrap_or_else(|| "-".into()),
            gap,
        );
    }
    let fail = reports.iter().filter(|r| !r.pass()).count();
    println!();
    println!(
        "ran {}  pass {}  fail {fail}",
        reports.len(),
        reports.len() - fail
    );
}

pub fn fmt_dur(d: Duration) -> String {
    if d.as_secs() >= 60 {
        format!("{}m{}s", d.as_secs() / 60, d.as_secs() % 60)
    } else if d.as_millis() >= 1000 {
        format!("{}s", d.as_secs())
    } else {
        format!("{}ms", d.as_millis())
    }
}
