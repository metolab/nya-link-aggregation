use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::net::TcpListener;
use tokio::sync::watch;

use nya_core::install_crypto;
use nya_server::{gen_cert, hex_encode, new_session_table, run_on_table, ServerConfig};

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

fn init_fmt() {
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
}

#[tokio::main]
async fn main() -> Result<()> {
    install_crypto();
    let args = Args::parse();
    match args.cmd {
        Some(Cmd::GenCert { out, name }) => {
            init_fmt();
            let pin = gen_cert(&out, &name)?;
            println!("wrote {}/server.crt and server.key", out.display());
            println!("pinned_spki_sha256 = \"{}\"", hex_encode(&pin));
            Ok(())
        }
        None => {
            let path = args.config.context("missing --config")?;
            let cfg = ServerConfig::load(&path)?;
            #[cfg(feature = "otel")]
            let guard = nya_obs::install("server", env!("CARGO_PKG_VERSION"), &cfg.obs)?;
            #[cfg(not(feature = "otel"))]
            init_fmt();

            #[cfg(feature = "otel")]
            let (listener, table) = {
                use tracing::Instrument;
                async {
                    let listener = TcpListener::bind(&cfg.listen)
                        .await
                        .with_context(|| format!("bind {}", cfg.listen))?;
                    let table = new_session_table(&cfg);
                    nya_obs::try_attach_table(&table);
                    Ok::<_, anyhow::Error>((listener, table))
                }
                .instrument(tracing::info_span!(
                    target: "nya_otel",
                    "nya.startup",
                    otel.kind = "internal",
                ))
                .await?
            };
            #[cfg(not(feature = "otel"))]
            let listener = TcpListener::bind(&cfg.listen)
                .await
                .with_context(|| format!("bind {}", cfg.listen))?;
            #[cfg(not(feature = "otel"))]
            let table = new_session_table(&cfg);
            let (stop_tx, stop_rx) = watch::channel(false);
            tokio::spawn(async move {
                wait_shutdown_signal().await;
                let _ = stop_tx.send(true);
            });
            let r = run_on_table(listener, cfg, stop_rx, table).await;
            #[cfg(feature = "otel")]
            guard.shutdown();
            r
        }
    }
}

async fn wait_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
