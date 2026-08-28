use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use nya_core::{ObsOpts, SessionConfig, SessionOpts};

#[derive(Debug, Clone, Deserialize)]
pub struct ClientConfig {
    pub psk: String,
    pub pinned_spki_sha256: String,
    #[serde(default)]
    pub session: SessionOpts,
    #[serde(default)]
    pub obs: ObsOpts,
    pub links: Vec<Link>,
    pub inbounds: Vec<Inbound>,
}

fn default_connections() -> u32 {
    2
}

#[derive(Debug, Clone, Deserialize)]
pub struct Link {
    pub name: String,
    pub addr: String,
    /// Independent TCP+TLS connections on this link. Isolates HOL / congestion.
    #[serde(default = "default_connections")]
    pub connections: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Inbound {
    Socks5 { listen: String },
    Forward { listen: String, target: String },
}

impl ClientConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        toml::from_str(&text).context("parse client config")
    }

    pub fn session_config(&self) -> SessionConfig {
        self.session.apply(SessionConfig::default())
    }
}
