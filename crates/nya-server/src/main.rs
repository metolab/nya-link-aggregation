use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use nya_core::install_crypto;
use nya_server::{gen_cert, hex_encode, run, ServerConfig};

#[derive(Parser)]
#[command(name = "nya-server", about = "nya-link-aggregation server")]
struct Args {
    #[command(subcommand)]
    cmd: Option<Cmd>,
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a self-signed TLS certificate and print the SPKI pin.
    GenCert {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value = "nya.local")]
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "nya_server=info,nya_core=info"
                    .parse()
                    .expect("static filter")
            }),
        )
        .init();
    install_crypto();

    let args = Args::parse();
    match args.cmd {
        Some(Cmd::GenCert { out, name }) => {
            let pin = gen_cert(&out, &name)?;
            println!("wrote {}/server.crt and server.key", out.display());
            println!("pinned_spki_sha256 = \"{}\"", hex_encode(&pin));
            Ok(())
        }
        None => {
            let path = args.config.context("missing --config")?;
            let cfg = ServerConfig::load(&path)?;
            run(cfg).await
        }
    }
}
