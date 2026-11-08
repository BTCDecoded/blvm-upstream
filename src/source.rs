//! One refresh of a configured source. A failed primary tries `fallback_base_url`.

use crate::config::SourceConfig;
use crate::convert::{
    basic_auth_header, btc_per_kvb_to_sat_per_kvb, join_url, sat_per_vb_to_sat_per_kvb,
    BITCOIND_FEE_TARGETS, MEMPOOL_SPACE_TARGETS,
};
use crate::fetch::HttpFetch;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct SourceSnapshot {
    pub id: String,
    pub healthy: bool,
    pub tip_height: Option<u64>,
    pub tip_hash: Option<String>,
    pub fetched_at_unix: Option<u64>,
    pub quotes: Vec<(u32, u64)>,
    pub error: Option<String>,
}

pub async fn refresh_source(
    source: &SourceConfig,
    http: &dyn HttpFetch,
    timeout_ms: u64,
    now_unix: u64,
) -> SourceSnapshot {
    match fetch_base(source, &source.base_url, http, timeout_ms, now_unix).await {
        Ok(snapshot) => snapshot,
        Err(primary) => {
            let Some(fallback) = source.fallback_base_url.as_deref() else {
                return failed(source, primary);
            };
            match fetch_base(source, fallback, http, timeout_ms, now_unix).await {
                Ok(snapshot) => snapshot,
                Err(fallback_error) => failed(source, format!("{primary}; fallback: {fallback_error}")),
            }
        }
    }
}

fn failed(source: &SourceConfig, error: String) -> SourceSnapshot {
    SourceSnapshot {
        id: source.id.clone(),
        healthy: false,
        tip_height: None,
        tip_hash: None,
        fetched_at_unix: None,
        quotes: Vec::new(),
        error: Some(error),
    }
}

async fn fetch_base(
    source: &SourceConfig,
    base: &str,
    http: &dyn HttpFetch,
    timeout_ms: u64,
    now_unix: u64,
) -> Result<SourceSnapshot, String> {
    let fetched = match source.kind.as_str() {
        "mempool-space" => fetch_rest(base, "/api/v1/fees/recommended", "/api/blocks/tip/height", "/api/blocks/tip/hash", true, http, timeout_ms).await?,
        "esplora" => fetch_rest(base, "/fee-estimates", "/blocks/tip/height", "/blocks/tip/hash", false, http, timeout_ms).await?,
        "bitcoind-rpc" => fetch_bitcoind(source, base, http, timeout_ms).await?,
        other => return Err(format!("unknown kind {other}")),
    };
    Ok(SourceSnapshot {
        id: source.id.clone(),
        healthy: !fetched.quotes.is_empty(),
        tip_height: Some(fetched.height),
        tip_hash: Some(fetched.hash),
        fetched_at_unix: Some(now_unix),
        quotes: fetched.quotes,
        error: None,
    })
}

struct Fetched {
    height: u64,
    hash: String,
    quotes: Vec<(u32, u64)>,
}

async fn fetch_rest(
    base: &str,
    fees_path: &str,
    height_path: &str,
    hash_path: &str,
    mempool_space: bool,
    http: &dyn HttpFetch,
    timeout_ms: u64,
) -> Result<Fetched, String> {
    let fees = get_ok(http, &join_url(base, fees_path), timeout_ms).await?;
    let height = get_ok(http, &join_url(base, height_path), timeout_ms).await?;
    let hash = get_ok(http, &join_url(base, hash_path), timeout_ms).await?;
    let quotes = if mempool_space {
        parse_mempool_space_fees(&fees)?
    } else {
        parse_esplora_fees(&fees)?
    };
    Ok(Fetched {
        height: parse_height(&height)?,
        hash: parse_hash(&hash)?,
        quotes,
    })
}

async fn get_ok(http: &dyn HttpFetch, url: &str, timeout_ms: u64) -> Result<Vec<u8>, String> {
    let response = http.get(url, timeout_ms).await?;
    if !(200..300).contains(&response.status) {
        return Err(format!("{url} returned {}", response.status));
    }
    Ok(response.body)
}

pub fn parse_mempool_space_fees(body: &[u8]) -> Result<Vec<(u32, u64)>, String> {
    let value: Value = serde_json::from_slice(body).map_err(|err| err.to_string())?;
    let mut quotes = Vec::new();
    for (name, target) in MEMPOOL_SPACE_TARGETS {
        let Some(rate) = value.get(*name).and_then(Value::as_f64) else {
            continue;
        };
        let Some(sat_per_kvb) = sat_per_vb_to_sat_per_kvb(rate) else {
            continue;
        };
        quotes.push((*target, sat_per_kvb));
    }
    quotes.sort_by_key(|(target, _)| *target);
    if quotes.is_empty() {
        return Err("mempool-space fees had no usable quote".to_string());
    }
    Ok(quotes)
}

pub fn parse_esplora_fees(body: &[u8]) -> Result<Vec<(u32, u64)>, String> {
    let value: Value = serde_json::from_slice(body).map_err(|err| err.to_string())?;
    let object = value
        .as_object()
        .ok_or_else(|| "esplora fee-estimates is not an object".to_string())?;
    let mut quotes = Vec::new();
    for (target, rate) in object {
        let Ok(target) = target.parse::<u32>() else {
            continue;
        };
        if target == 0 {
            continue;
        }
        let Some(rate) = rate.as_f64() else {
            continue;
        };
        let Some(sat_per_kvb) = sat_per_vb_to_sat_per_kvb(rate) else {
            continue;
        };
        quotes.push((target, sat_per_kvb));
    }
    quotes.sort_by_key(|(target, _)| *target);
    quotes.dedup_by_key(|(target, _)| *target);
    if quotes.is_empty() {
        return Err("esplora fee-estimates had no usable quote".to_string());
    }
    Ok(quotes)
}

async fn fetch_bitcoind(
    source: &SourceConfig,
    base: &str,
    http: &dyn HttpFetch,
    timeout_ms: u64,
) -> Result<Fetched, String> {
    let mut headers = Vec::new();
    let auth;
    if let (Some(user), Some(password)) = (source.rpc_user.as_deref(), source.rpc_password.as_deref()) {
        auth = basic_auth_header(user, password);
        headers.push(("Authorization", auth.as_str()));
    }
    let request = bitcoind_batch();
    let body = serde_json::to_vec(&request).map_err(|err| err.to_string())?;
    let response = http
        .post(base, "application/json", &body, &headers, timeout_ms)
        .await?;
    if !(200..300).contains(&response.status) {
        return Err(format!("{base} returned {}", response.status));
    }
    parse_bitcoind_batch(&response.body)
}

fn bitcoind_batch() -> Vec<Value> {
    let mut calls = Vec::new();
    for target in BITCOIND_FEE_TARGETS {
        calls.push(json!({
            "jsonrpc": "1.0",
            "id": format!("fee-{target}"),
            "method": "estimatesmartfee",
            "params": [target]
        }));
    }
    calls.push(json!({
        "jsonrpc": "1.0",
        "id": "mempool",
        "method": "getmempoolinfo",
        "params": []
    }));
    calls.push(json!({
        "jsonrpc": "1.0",
        "id": "count",
        "method": "getblockcount",
        "params": []
    }));
    calls.push(json!({
        "jsonrpc": "1.0",
        "id": "hash",
        "method": "getbestblockhash",
        "params": []
    }));
    calls
}

fn parse_bitcoind_batch(body: &[u8]) -> Result<Fetched, String> {
    let value: Value = serde_json::from_slice(body).map_err(|err| err.to_string())?;
    let rows = value
        .as_array()
        .ok_or_else(|| "bitcoind batch response is not an array".to_string())?;
    let mut by_id = std::collections::HashMap::new();
    for row in rows {
        if let Some(id) = row.get("id").and_then(Value::as_str) {
            by_id.insert(id.to_string(), row);
        }
    }
    let mempool = by_id
        .get("mempool")
        .ok_or_else(|| "bitcoind batch missing getmempoolinfo".to_string())?;
    if mempool.get("error").is_some_and(|err| !err.is_null()) {
        return Err("getmempoolinfo failed".to_string());
    }
    let height = by_id
        .get("count")
        .and_then(|row| row.get("result"))
        .and_then(Value::as_u64)
        .ok_or_else(|| "getblockcount missing".to_string())?;
    let hash = by_id
        .get("hash")
        .and_then(|row| row.get("result"))
        .and_then(Value::as_str)
        .ok_or_else(|| "getbestblockhash missing".to_string())?
        .to_string();
    let mut quotes = Vec::new();
    for target in BITCOIND_FEE_TARGETS {
        let Some(row) = by_id.get(&format!("fee-{target}")) else {
            continue;
        };
        if row.get("error").is_some_and(|err| !err.is_null()) {
            continue;
        }
        let Some(feerate) = row.pointer("/result/feerate").and_then(Value::as_f64) else {
            continue;
        };
        let Some(sat_per_kvb) = btc_per_kvb_to_sat_per_kvb(feerate) else {
            continue;
        };
        quotes.push((*target, sat_per_kvb));
    }
    if quotes.is_empty() {
        return Err("estimatesmartfee returned no usable quote".to_string());
    }
    Ok(Fetched { height, hash, quotes })
}

fn parse_height(body: &[u8]) -> Result<u64, String> {
    let text = String::from_utf8_lossy(body);
    let text = text.trim().trim_matches('"');
    text.parse::<u64>()
        .map_err(|_| format!("tip height is not an integer: {text}"))
}

fn parse_hash(body: &[u8]) -> Result<String, String> {
    let text = String::from_utf8_lossy(body);
    let text = text.trim().trim_matches('"').to_string();
    if text.is_empty() {
        return Err("tip hash is empty".to_string());
    }
    Ok(text)
}
