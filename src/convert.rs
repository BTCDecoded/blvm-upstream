//! Unit conversions named in design 21.4.
//!
//! mempool.space and Esplora report sat/vB. The contract uses sat/kvB: `ceil(x * 1000)`.
//! Bitcoin Core `estimatesmartfee` `feerate` is BTC per kvB, so sat/kvB is `ceil(feerate * 1e8)`.

/// Design 21.4 mempool.space map: `fastestFee` → 1, `halfHourFee` → 3, `hourFee` → 6.
pub const MEMPOOL_SPACE_TARGETS: &[(&str, u32)] =
    &[("fastestFee", 1), ("halfHourFee", 3), ("hourFee", 6)];

/// The same three targets, used for the bitcoind `estimatesmartfee` batch.
pub const BITCOIND_FEE_TARGETS: &[u32] = &[1, 3, 6];

pub fn sat_per_vb_to_sat_per_kvb(sat_per_vb: f64) -> Option<u64> {
    positive_ceil(sat_per_vb * 1000.0)
}

pub fn btc_per_kvb_to_sat_per_kvb(btc_per_kvb: f64) -> Option<u64> {
    positive_ceil(btc_per_kvb * 100_000_000.0)
}

fn positive_ceil(value: f64) -> Option<u64> {
    if !value.is_finite() || value <= 0.0 || value >= u64::MAX as f64 {
        return None;
    }
    // JSON numbers arrive as f64. A value that is a hair over an integer is that integer.
    let nearest = value.round();
    if (value - nearest).abs() < 1e-4 {
        return Some(nearest as u64);
    }
    Some(value.ceil() as u64)
}

pub fn join_url(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    let path = path.trim_start_matches('/');
    format!("{base}/{path}")
}

pub fn basic_auth_header(user: &str, password: &str) -> String {
    format!("Basic {}", base64(&format!("{user}:{password}")))
}

fn base64(input: &str) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out = String::new();
    let mut index = 0;
    while index + 3 <= bytes.len() {
        let n = ((bytes[index] as u32) << 16)
            | ((bytes[index + 1] as u32) << 8)
            | bytes[index + 2] as u32;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(TABLE[((n >> 6) & 63) as usize] as char);
        out.push(TABLE[(n & 63) as usize] as char);
        index += 3;
    }
    let rest = bytes.len() - index;
    if rest == 1 {
        let n = (bytes[index] as u32) << 16;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rest == 2 {
        let n = ((bytes[index] as u32) << 16) | ((bytes[index + 1] as u32) << 8);
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(TABLE[((n >> 6) & 63) as usize] as char);
        out.push('=');
    }
    out
}
