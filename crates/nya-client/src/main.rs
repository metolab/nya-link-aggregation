use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use nya_client::{run_with_inbounds, ClientConfig};
use nya_core::install_crypto;

#[derive(Parser)]
#[command(name = "nya-client", about = "nya-link-aggregation client")]
struct Args {
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("nya_client=info".parse()?)
                .add_directive("nya_core=info".parse()?),
        )
        .init();
    install_crypto();
    let args = Args::parse();
    let cfg = ClientConfig::load(&args.config)?;
    run_with_inbounds(cfg).await.context("client")?;
    Ok(())
}
