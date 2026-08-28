use anyhow::Result;
use clap::Parser;
use nya_e2e::report::print_suite;
use nya_e2e::{default_jobs, init_tracing, run_catalog};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "nya-e2e", about = "nya-link-aggregation scenario runner")]
struct Args {
    /// Substring filter on scenario / mix-case name
    #[arg(long)]
    filter: Option<String>,
    /// Include 30s/60s/5m blackholes in the short matrix
    #[arg(long)]
    long: bool,
    /// 15-minute mixed suite: peer3, peer5, peer3+slow, peer5+2slow in parallel
    #[arg(long)]
    mixed: bool,
    /// Mix-case substring (only with --mixed), e.g. peer3
    #[arg(long)]
    case: Option<String>,
    /// RTT band for --mixed: near (11–16ms), mid (60–100), high (120–150), far (160–200), comma-list, or all
    #[arg(long, default_value = "all")]
    band: String,
    /// Duration in seconds (default 900 for --mixed)
    #[arg(long)]
    secs: Option<u64>,
    /// Heavier noise and longer/more frequent faults
    #[arg(long)]
    harsh: bool,
    /// Max concurrent isolated harnesses (default: min(16, nproc))
    #[arg(long, default_value_t = 0)]
    jobs: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let jobs = if args.jobs == 0 {
        // Mixed WAN sims are timer-heavy; 16-way parallel inflates p50 by 2×
        // and explodes migrate/failback counters. Four isolated harnesses
        // match the original near suite.
        if args.mixed {
            default_jobs().min(4)
        } else {
            default_jobs()
        }
    } else {
        args.jobs
    };
    if args.mixed {
        let duration = Duration::from_secs(args.secs.unwrap_or(15 * 60));
        let bands = nya_e2e::mixed::parse_bands(&args.band).ok_or_else(|| {
            anyhow::anyhow!("--band must be near|mid|high|far|all (or a comma-list)")
        })?;
        let reports = nya_e2e::mixed::run_suite(nya_e2e::mixed::MixedOpts {
            duration,
            harsh: args.harsh,
            jobs,
            filter: args.case.or(args.filter),
            bands,
        })
        .await?;
        print_suite(&reports);
        if reports.iter().any(|r| !r.pass()) {
            std::process::exit(1);
        }
        return Ok(());
    }
    let reports = run_catalog(args.filter.as_deref(), args.long, jobs).await;
    print_suite(&reports);
    if reports.iter().any(|r| !r.pass()) {
        std::process::exit(1);
    }
    Ok(())
}
