//! HTTP used by the connectors. Production goes through the node broker.
//! Tests point this at a local mock.

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait HttpFetch: Send + Sync {
    async fn get(&self, url: &str, timeout_ms: u64) -> Result<HttpResponse, String>;
    async fn post(
        &self,
        url: &str,
        content_type: &str,
        body: &[u8],
        headers: &[(&str, &str)],
        timeout_ms: u64,
    ) -> Result<HttpResponse, String>;
}
