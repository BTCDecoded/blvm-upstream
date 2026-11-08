//! blvm-upstream. The node spawns this process. HTTP goes through the broker.

use anyhow::Result;
use async_trait::async_trait;
use blvm_node::module::inter_module::api::ModuleAPI;
use blvm_node::module::ipc::protocol::CliSpec;
use blvm_node::module::traits::{ModuleError, NodeAPI};
use blvm_sdk::module::runner::run_module_with_setup_and_api;
use blvm_sdk::module::{ModuleBootstrap, ModuleDb};
use blvm_upstream::{HttpFetch, HttpResponse, METHODS, UpstreamService};
use std::sync::Arc;

const MODULE_NAME: &str = "blvm-upstream";

struct BrokerHttp {
    node: Arc<dyn NodeAPI>,
}

#[async_trait]
impl HttpFetch for BrokerHttp {
    async fn get(&self, url: &str, timeout_ms: u64) -> Result<HttpResponse, String> {
        let (status, body) = self
            .node
            .http_get(url, Some("application/json"), timeout_ms as u32)
            .await
            .map_err(|err| err.to_string())?;
        Ok(HttpResponse { status, body })
    }

    async fn post(
        &self,
        url: &str,
        content_type: &str,
        body: &[u8],
        headers: &[(&str, &str)],
        timeout_ms: u64,
    ) -> Result<HttpResponse, String> {
        let _ = headers;
        let authorization = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .map(|(_, value)| *value);
        let (status, body) = self
            .node
            .http_post(
                url,
                content_type,
                body.to_vec(),
                authorization,
                timeout_ms as u32,
            )
            .await
            .map_err(|err| err.to_string())?;
        Ok(HttpResponse { status, body })
    }
}

struct Contract {
    service: Arc<UpstreamService<BrokerHttp>>,
}

#[async_trait]
impl ModuleAPI for Contract {
    async fn handle_request(
        &self,
        method: &str,
        params: &[u8],
        _caller_module_id: &str,
    ) -> Result<Vec<u8>, ModuleError> {
        self.service
            .call(method, params)
            .await
            .map_err(ModuleError::Other)
    }

    fn list_methods(&self) -> Vec<String> {
        METHODS.iter().map(|method| (*method).to_string()).collect()
    }

    fn api_version(&self) -> u32 {
        1
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let bootstrap = ModuleBootstrap::init_module(MODULE_NAME);
    let db = ModuleDb::open_or_temp(&bootstrap.data_dir, MODULE_NAME)?;
    let data_dir = bootstrap.data_dir.clone();
    let setup = move |node_api: Arc<dyn NodeAPI>,
                      _db: Arc<dyn blvm_node::storage::database::Database>,
                      dir: &std::path::Path| {
        let dir = dir.to_path_buf();
        async move {
            let mut config = blvm_upstream::UpstreamConfig::load(dir.join("config.toml"))
                .map_err(|err| ModuleError::Other(err.to_string()))?;
            let poll = std::env::var("MODULE_CONFIG_POLL_INTERVAL_SECS").ok();
            let timeout = std::env::var("MODULE_CONFIG_TIMEOUT_MS").ok();
            config
                .apply_scalar_overrides(poll.as_deref(), timeout.as_deref())
                .map_err(|err| ModuleError::Other(err.to_string()))?;
            let service = Arc::new(UpstreamService::new(config, BrokerHttp { node: node_api }));
            service.spawn_poll();
            let api = Arc::new(Contract {
                service: Arc::clone(&service),
            });
            Ok(((), (), api as Arc<dyn ModuleAPI>))
        }
    };
    run_module_with_setup_and_api(
        &bootstrap.socket_path,
        &bootstrap.module_id,
        MODULE_NAME,
        env!("CARGO_PKG_VERSION"),
        CliSpec {
            version: 1,
            name: MODULE_NAME.to_string(),
            about: Some("Upstream fee and tip sources".to_string()),
            subcommands: Vec::new(),
        },
        &[],
        Vec::new(),
        |_invocation, _ctx, _module, _cli| {
            blvm_node::module::ipc::protocol::InvocationResultMessage {
                correlation_id: 0,
                success: false,
                payload: None,
                error: Some("no cli".to_string()),
            }
        },
        |_event, _module, _ctx| async { Ok::<(), ModuleError>(()) },
        setup,
        db.as_db(),
        &data_dir,
    )
    .await
    .map_err(|err| anyhow::anyhow!("{err}"))?;
    Ok(())
}
