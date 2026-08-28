use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use nya_client::{serve_inbounds, start, ClientConfig};
use nya_core::install_crypto;

#[derive(Parser)]
#[command(name = "nya-client", about = "nya-link-aggregation client")]
struct Args {
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    install_crypto();
    let args = Args::parse();
    let cfg = ClientConfig::load(&args.config)?;

    #[cfg(feature = "otel")]
    let guard = nya_obs::install("client", env!("CARGO_PKG_VERSION"), &cfg.obs)?;
    #[cfg(not(feature = "otel"))]
    {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                    "nya_client=info,nya_core=info"
                        .parse()
                        .expect("static filter")
                }),
            )
            .init();
    }

    #[cfg(feature = "otel")]
    let session = {
        use tracing::Instrument;
        async {
            let session = start(cfg.clone()).await.context("client start")?;
            nya_obs::try_attach_session(&session);
            Ok::<_, anyhow::Error>(session)
        }
        .instrument(tracing::info_span!(
            target: "nya_otel",
            "nya.startup",
            otel.kind = "internal",
        ))
        .await?
    };
    #[cfg(not(feature = "otel"))]
    let session = start(cfg.clone()).await.context("client start")?;
    {
        let s = session.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            s.shutdown();
        });
    }
    let r = serve_inbounds(cfg.inbounds, session)
        .await
        .context("client");
    #[cfg(feature = "otel")]
    guard.shutdown();
    r
}
