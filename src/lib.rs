//! Outbound fee and tip connectors. The node broker is the production HTTP path.
//! Tests use a local mock and do not call mempool.space or blockstream.

mod config;
mod convert;
mod fetch;
mod service;
mod source;

pub use config::UpstreamConfig;
pub use fetch::{HttpFetch, HttpResponse};
pub use service::{CAPABILITIES, CONTRACT, METHODS, UpstreamService};

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Mutex;

    struct Table {
        routes: Mutex<HashMap<String, (u16, Vec<u8>)>>,
    }

    #[async_trait]
    impl HttpFetch for Table {
        async fn get(&self, url: &str, _timeout_ms: u64) -> Result<HttpResponse, String> {
            self.lookup(&format!("GET {url}")).await
        }

        async fn post(
            &self,
            url: &str,
            _content_type: &str,
            body: &[u8],
            headers: &[(&str, &str)],
            _timeout_ms: u64,
        ) -> Result<HttpResponse, String> {
            if headers.iter().any(|(name, _)| *name == "Authorization") {
                let _ = body;
            }
            self.lookup(&format!("POST {url}")).await
        }
    }

    impl Table {
        async fn lookup(&self, key: &str) -> Result<HttpResponse, String> {
            let routes = self.routes.lock().await;
            let (status, body) = routes
                .get(key)
                .cloned()
                .ok_or_else(|| format!("no mock for {key}"))?;
            Ok(HttpResponse { status, body })
        }
    }

    fn source(id: &str, kind: &str, base: &str) -> crate::config::SourceConfig {
        crate::config::SourceConfig {
            id: id.to_string(),
            kind: kind.to_string(),
            base_url: base.to_string(),
            fallback_base_url: None,
            rpc_user: None,
            rpc_password: None,
        }
    }

    #[test]
    fn config_file_keeps_source_tables_and_scalars_can_override() {
        let dir = std::env::temp_dir().join(format!("blvm-upstream-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
poll_interval_secs = 10
timeout_ms = 5000

[[sources]]
id = "mempool-space"
kind = "mempool-space"
base_url = "http://127.0.0.1:1"

[[sources]]
id = "blockstream"
kind = "esplora"
base_url = "http://127.0.0.1:2"
"#,
        )
        .unwrap();
        let mut cfg = UpstreamConfig::load(&path).unwrap();
        assert_eq!(cfg.sources.len(), 2);
        cfg.apply_scalar_overrides(Some("15"), None).unwrap();
        assert_eq!(cfg.poll_interval_secs, 15);
        assert_eq!(cfg.timeout_ms, 5000);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn fractional_sat_per_vb_rounds_up_to_sat_per_kvb() {
        assert_eq!(
            crate::convert::sat_per_vb_to_sat_per_kvb(1.0001),
            Some(1001)
        );
        assert_eq!(
            crate::convert::sat_per_vb_to_sat_per_kvb(10.0),
            Some(10_000)
        );
        assert_eq!(
            crate::convert::btc_per_kvb_to_sat_per_kvb(0.00002),
            Some(2_000)
        );
    }

    #[tokio::test]
    async fn local_mock_feeds_every_kind_and_a_fallback() {
        let mempool = "http://mempool.local";
        let esplora = "http://esplora.local";
        let other = "http://other.local";
        let down = "http://down.local";
        let rpc = "http://bitcoind.local/";
        let mut routes = HashMap::new();
        routes.insert(
            format!("GET {mempool}/api/v1/fees/recommended"),
            (
                200,
                br#"{"fastestFee":1.0001,"halfHourFee":8.5,"hourFee":6,"economyFee":1}"#.to_vec(),
            ),
        );
        routes.insert(
            format!("GET {mempool}/api/blocks/tip/height"),
            (200, b"100".to_vec()),
        );
        routes.insert(
            format!("GET {mempool}/api/blocks/tip/hash"),
            (200, b"aa".to_vec()),
        );
        routes.insert(
            format!("GET {esplora}/fee-estimates"),
            (200, br#"{"6":1.5,"1":2}"#.to_vec()),
        );
        routes.insert(
            format!("GET {esplora}/blocks/tip/height"),
            (200, b"90".to_vec()),
        );
        routes.insert(
            format!("GET {esplora}/blocks/tip/hash"),
            (200, b"bb".to_vec()),
        );
        routes.insert(
            format!("GET {other}/fee-estimates"),
            (200, br#"{"2":3}"#.to_vec()),
        );
        routes.insert(
            format!("GET {other}/blocks/tip/height"),
            (200, b"80".to_vec()),
        );
        routes.insert(
            format!("GET {other}/blocks/tip/hash"),
            (200, b"cc".to_vec()),
        );
        routes.insert(
            format!("GET {down}/api/v1/fees/recommended"),
            (500, b"no".to_vec()),
        );
        routes.insert(
            format!("GET {esplora}/fallback/fee-estimates"),
            (200, br#"{"1":4}"#.to_vec()),
        );
        routes.insert(
            format!("GET {esplora}/fallback/blocks/tip/height"),
            (200, b"70".to_vec()),
        );
        routes.insert(
            format!("GET {esplora}/fallback/blocks/tip/hash"),
            (200, b"dd".to_vec()),
        );
        routes.insert(
            format!("POST {rpc}"),
            (
                200,
                br#"[{"id":"fee-1","result":{"feerate":0.00002},"error":null},{"id":"fee-3","result":{"errors":["no"]},"error":null},{"id":"fee-6","result":{"feerate":0.00001},"error":null},{"id":"mempool","result":{"size":1},"error":null},{"id":"count","result":10,"error":null},{"id":"hash","result":"ee","error":null}]"#.to_vec(),
            ),
        );
        let http = Table {
            routes: Mutex::new(routes),
        };
        let mut saved = source("saved", "esplora", "http://missing.local");
        saved.fallback_base_url = Some(format!("{esplora}/fallback"));
        let mut cfg = UpstreamConfig::default();
        cfg.sources = vec![
            source("mempool-space", "mempool-space", mempool),
            source("blockstream", "esplora", esplora),
            source("second", "esplora", other),
            source("down", "mempool-space", down),
            saved,
            source("core", "bitcoind-rpc", rpc),
        ];
        let service = UpstreamService::new(cfg, http);
        service.refresh().await;

        let sources = service.sources().await;
        let rows = sources.as_array().unwrap();
        assert_eq!(rows[0]["id"], "mempool-space");
        assert_eq!(rows[0]["healthy"], true);
        assert_eq!(rows[0]["capabilities"][0], "tip");
        assert_eq!(rows[3]["healthy"], false);

        let table = service.fee_table("mempool-space").await.unwrap();
        assert_eq!(table["quotes"][0][0], 1);
        assert_eq!(table["quotes"][0][1], 1001);
        assert_eq!(table["quotes"][1], serde_json::json!([3, 8500]));
        assert_eq!(table["quotes"][2], serde_json::json!([6, 6000]));
        assert_eq!(table["tip_height"], 100);
        assert_eq!(service.tip("mempool-space").await.unwrap()["hash"], "aa");

        let esplora = service.fee_table("blockstream").await.unwrap();
        assert_eq!(esplora["quotes"][0], serde_json::json!([1, 2000]));
        assert_eq!(esplora["quotes"][1], serde_json::json!([6, 1500]));
        let second = service.fee_table("second").await.unwrap();
        assert_eq!(second["quotes"][0], serde_json::json!([2, 3000]));
        assert_ne!(esplora["tip_hash"], second["tip_hash"]);

        let fallen = service.fee_table("saved").await.unwrap();
        assert_eq!(fallen["quotes"][0], serde_json::json!([1, 4000]));
        assert_eq!(fallen["tip_hash"], "dd");

        let core = service.fee_table("core").await.unwrap();
        assert_eq!(core["quotes"][0], serde_json::json!([1, 2000]));
        assert_eq!(core["quotes"][1], serde_json::json!([6, 1000]));
        assert_eq!(service.tip("core").await.unwrap()["height"], 10);
        assert!(service.fee_table("down").await.is_err());
        assert!(
            service
                .fee_table("missing")
                .await
                .unwrap_err()
                .contains("unknown")
        );
    }

    #[tokio::test]
    async fn a_local_tcp_mock_answers_the_esplora_paths() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let _ = socket.read(&mut buf).await.unwrap();
            let body = b"{\"1\":2.25}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        let request =
            format!("GET /fee-estimates HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        client.write_all(request.as_bytes()).await.unwrap();
        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        let text = String::from_utf8(received).unwrap();
        let json = text.split("\r\n\r\n").nth(1).unwrap();
        let quotes = crate::source::parse_esplora_fees(json.as_bytes()).unwrap();
        assert_eq!(quotes, vec![(1, 2250)]);
    }
}
