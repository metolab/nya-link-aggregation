use std::time::Duration;

use nya_e2e::init_tracing;
use nya_e2e::report::print_suite;
use nya_e2e::{default_jobs, run_catalog};

fn assert_all_pass(reports: Vec<nya_e2e::ScenarioReport>) {
    print_suite(&reports);
    let fail: Vec<_> = reports.iter().filter(|r| !r.pass()).collect();
    if !fail.is_empty() {
        let names: Vec<_> = fail.iter().map(|r| r.name.as_str()).collect();
        panic!("{} SLA FAIL: {}", fail.len(), names.join(", "));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn short_matrix() {
    init_tracing();
    let reports = run_catalog(None, false, default_jobs()).await;
    assert_all_pass(reports);
}

/// SOCKS / concurrent / abort / flap churn. Isolated from the p99 catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stream_lifecycle() {
    init_tracing();
    let reports = nya_e2e::run_lifecycle(4).await;
    assert_all_pass(reports);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "15min mixed suite; run with: cargo run -p nya-e2e --bin nya-e2e -- --mixed [--band all]"]
async fn mixed_15m() {
    init_tracing();
    let reports = nya_e2e::mixed::run_suite(nya_e2e::mixed::MixedOpts {
        duration: Duration::from_secs(15 * 60),
        harsh: false,
        jobs: 4,
        filter: None,
        bands: nya_e2e::mixed::MixBand::all().to_vec(),
    })
    .await
    .unwrap();
    assert_all_pass(reports);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long: 30s/60s/5m blackholes; run with nya-e2e --long"]
async fn long_blackholes() {
    init_tracing();
    let reports = run_catalog(Some("blackhole_a_"), true, 3).await;
    assert_all_pass(reports);
}
