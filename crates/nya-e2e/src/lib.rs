//! End-to-end harness: userspace link impairment + SLA matrix.
//!
//! ```text
//! cargo test -p nya-e2e
//! cargo run -p nya-e2e --bin nya-e2e                    # short matrix, parallel
//! cargo run -p nya-e2e --bin nya-e2e -- --jobs 8
//! cargo run -p nya-e2e --bin nya-e2e -- --long          # + 30s/60s/5m blackholes
//! cargo run -p nya-e2e --bin nya-e2e -- --mixed              # 15min all bands (16 cases)
//! cargo run -p nya-e2e --bin nya-e2e -- --mixed --band near  # 4-case lab loop
//! cargo run -p nya-e2e --bin nya-e2e -- --mixed --band all --secs 480  # 8min soak ×1
//! cargo run -p nya-e2e --bin nya-e2e -- --mixed --band mid,high,far
//! cargo run -p nya-e2e --bin nya-e2e -- --mixed --case peer3
//! cargo run -p nya-e2e --bin nya-e2e -- --mixed --secs 45
//! cargo run -p nya-e2e --bin nya-e2e -- --filter delay
//! ```
//!
//! Impairment is a TCP proxy in front of each path (delay/jitter/loss-as-stall,
//! blackhole, disconnect, spikes). Packet loss is *not* byte-drops (that would
//! corrupt TLS); it extra-stalls a chunk like a TCP RTO.
//!
//! The 15-minute mixed suite models typical use: 3 or 5 same-class peer links
//! mixed together, optionally plus 1–2 slightly slower, mostly-stable links.
//! Matching 3/5 ping streams run concurrently, with one bulk stream per slow
//! link. Cases run in parallel so one wall-clock window covers several mixes.
//! `--band` selects RTT windows: near 11–16ms, mid 60–100ms,
//! high 120–150ms, far 160–200ms (`all` is the default mixed regression).
//! High-RTT bands use larger jitter/loss and delay_shift that is always
//! slower than the path baseline. Mixed jobs default to 4.
#![forbid(unsafe_code)]

pub mod harness;
pub mod impair;
pub mod mixed;
pub mod packet_wan;
pub mod report;
pub mod scenarios;
pub mod workload;

pub use harness::{start, Harness, HarnessSpec};
pub use impair::{ImpairConfig, LinkHandle};
pub use report::ScenarioReport;
pub use scenarios::{catalog, run_catalog, run_lifecycle};

/// Max concurrent isolated harnesses. Caps below host parallelism so
/// timer-heavy WAN sims do not inflate p99 of latency SLAs.
pub fn default_jobs() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
        .clamp(2, 16)
}

use std::sync::Once;

pub fn init_tracing() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                    "nya_e2e=info,nya_client=warn,nya_server=warn,nya_core=warn"
                        .parse()
                        .expect("static filter")
                }),
            )
            .try_init();
    });
}
