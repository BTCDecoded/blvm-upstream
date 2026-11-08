//! Cached sources and the `blvm.upstream.v1` JSON methods.

use crate::config::UpstreamConfig;
use crate::fetch::HttpFetch;
use crate::source::{SourceSnapshot, refresh_source};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

pub const CONTRACT: &str = "blvm.upstream.v1";
pub const METHODS: &[&str] = &["sources", "tip", "fee_table"];
/// Implemented now. Reserved names are not in this list.
pub const CAPABILITIES: &[&str] = &["tip", "fee_table"];

pub struct UpstreamService<H> {
    config: UpstreamConfig,
    http: H,
    cache: RwLock<HashMap<String, SourceSnapshot>>,
}

impl<H: HttpFetch + 'static> UpstreamService<H> {
    pub fn new(config: UpstreamConfig, http: H) -> Self {
        Self {
            config,
            http,
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn refresh(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let mut next = HashMap::new();
        for source in &self.config.sources {
            let snapshot = refresh_source(source, &self.http, self.config.timeout_ms, now).await;
            next.insert(source.id.clone(), snapshot);
        }
        *self.cache.write().await = next;
    }

    pub fn spawn_poll(self: &Arc<Self>) {
        let service = Arc::clone(self);
        let interval = self.config.poll_interval_secs.max(1);
        tokio::spawn(async move {
            loop {
                service.refresh().await;
                tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
            }
        });
    }

    pub async fn call(&self, method: &str, params: &[u8]) -> Result<Vec<u8>, String> {
        let value = match method {
            "sources" => self.sources().await,
            "tip" => self.tip(&source_param(params)?).await?,
            "fee_table" => self.fee_table(&source_param(params)?).await?,
            other => return Err(format!("unknown method {other}")),
        };
        serde_json::to_vec(&value).map_err(|err| err.to_string())
    }

    pub async fn sources(&self) -> Value {
        let cache = self.cache.read().await;
        let rows = self.config.sources.iter().map(|source| {
            let snapshot = cache.get(&source.id);
            json!({
                "id": source.id,
                "kind": source.kind,
                "healthy": snapshot.is_some_and(|item| item.healthy),
                "tip_height": snapshot.and_then(|item| item.tip_height),
                "tip_hash": snapshot.and_then(|item| item.tip_hash.clone()),
                "last_ok_unix": snapshot.and_then(|item| item.fetched_at_unix),
                "capabilities": CAPABILITIES,
            })
        });
        Value::Array(rows.collect())
    }

    pub async fn tip(&self, source: &str) -> Result<Value, String> {
        let snapshot = self.ready(source).await?;
        Ok(json!({
            "height": snapshot.tip_height,
            "hash": snapshot.tip_hash,
            "fetched_at_unix": snapshot.fetched_at_unix,
        }))
    }

    pub async fn fee_table(&self, source: &str) -> Result<Value, String> {
        let snapshot = self.ready(source).await?;
        let quotes: Vec<Vec<u64>> = snapshot
            .quotes
            .iter()
            .map(|(target, rate)| vec![u64::from(*target), *rate])
            .collect();
        Ok(json!({
            "source": snapshot.id,
            "tip_height": snapshot.tip_height,
            "tip_hash": snapshot.tip_hash,
            "fetched_at_unix": snapshot.fetched_at_unix,
            "quotes": quotes,
        }))
    }

    async fn ready(&self, source: &str) -> Result<SourceSnapshot, String> {
        let cache = self.cache.read().await;
        let Some(snapshot) = cache.get(source) else {
            return Err(format!("unknown source {source}"));
        };
        if !snapshot.healthy {
            return Err(snapshot
                .error
                .clone()
                .unwrap_or_else(|| format!("source {source} is unhealthy")));
        }
        Ok(snapshot.clone())
    }
}

fn source_param(params: &[u8]) -> Result<String, String> {
    let value: Value = serde_json::from_slice(params).map_err(|err| err.to_string())?;
    value
        .get("source")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "source is required".to_string())
}
