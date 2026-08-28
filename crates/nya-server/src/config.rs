use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use nya_core::{SessionConfig, SessionOpts};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub listen: String,
    pub psk: String,
    pub cert: PathBuf,
    pub key: PathBuf,
    #[serde(default)]
    pub session: SessionOpts,
}

impl ServerConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        toml::from_str(&text).context("parse server config")
    }

    pub fn session_config(&self) -> SessionConfig {
        self.session.apply(SessionConfig::default())
    }
}
