//! Module file `<data dir>/config.toml`. Arrays stay here.
//! `poll_interval_secs` and `timeout_ms` can also be overridden by node scalars.

use serde::{Deserialize, Serialize};

pub const DEFAULT_POLL_INTERVAL_SECS: u64 = 10;
pub const DEFAULT_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UpstreamConfig {
    pub poll_interval_secs: u64,
    pub timeout_ms: u64,
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub id: String,
    pub kind: String,
    pub base_url: String,
    #[serde(default)]
    pub fallback_base_url: Option<String>,
    #[serde(default)]
    pub rpc_user: Option<String>,
    #[serde(default)]
    pub rpc_password: Option<String>,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: DEFAULT_POLL_INTERVAL_SECS,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            sources: Vec::new(),
        }
    }
}

impl UpstreamConfig {
    pub fn load(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(path)?;
        let cfg: Self = toml::from_str(&text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Node `[modules.blvm-upstream]` scalars. Absent keys leave the file value.
    pub fn apply_scalar_overrides(&mut self, poll_interval_secs: Option<&str>, timeout_ms: Option<&str>) -> anyhow::Result<()> {
        if let Some(text) = poll_interval_secs {
            self.poll_interval_secs = text.parse().map_err(|_| {
                anyhow::anyhow!("poll_interval_secs must be an integer, got {text}")
            })?;
        }
        if let Some(text) = timeout_ms {
            self.timeout_ms = text
                .parse()
                .map_err(|_| anyhow::anyhow!("timeout_ms must be an integer, got {text}"))?;
        }
        self.validate()
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.poll_interval_secs == 0 {
            anyhow::bail!("poll_interval_secs must be at least 1, got 0");
        }
        if self.timeout_ms == 0 {
            anyhow::bail!("timeout_ms must be at least 1, got 0");
        }
        let mut seen = std::collections::BTreeSet::new();
        for source in &self.sources {
            if source.id.is_empty() || !seen.insert(source.id.clone()) {
                anyhow::bail!("source id must be unique and non-empty, got {}", source.id);
            }
            if !matches!(source.kind.as_str(), "mempool-space" | "esplora" | "bitcoind-rpc") {
                anyhow::bail!(
                    "source {} kind must be mempool-space, esplora, or bitcoind-rpc, got {}",
                    source.id,
                    source.kind
                );
            }
            if source.base_url.trim().is_empty() {
                anyhow::bail!("source {} base_url must not be empty", source.id);
            }
        }
        Ok(())
    }
}
