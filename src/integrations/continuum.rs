use anyhow::{Result, anyhow};
use reqwest::Client;
use serde_json::Value;

use crate::{config::ExternalServiceConfig, models::ServiceSnapshot};

const CONTINUUM_MONITORING_PREFIX: &str = "/services/health/monitoring";

#[derive(Clone)]
pub struct ContinuumClient {
    client: Client,
    base_url: String,
    bearer_token: Option<String>,
}

impl ContinuumClient {
    pub async fn from_sources(
        config: &ExternalServiceConfig,
        service: Option<&ServiceSnapshot>,
    ) -> Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }
        let client = super::build_http_client(config.timeout_seconds.max(1))?;
        let base_url = select_live_base_url(&client, config, service)
            .await?
            .ok_or_else(|| anyhow!("no Continuum base URL configured or discovered"))?;
        Ok(Some(Self {
            client,
            base_url,
            bearer_token: config.bearer_token.clone(),
        }))
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn tracey_fleet_url(&self) -> String {
        format!("{}/tracey/fleet", self.base_url)
    }

    pub fn tracey_agents_url(&self) -> String {
        format!("{}/tracey/agents", self.base_url)
    }

    pub fn tracey_agent_analysis_url(&self, agent_id: &str) -> String {
        format!(
            "{}/tracey/agents/{}/analysis",
            self.base_url,
            agent_id.trim()
        )
    }

    pub fn tracey_agent_deepdive_url(&self, agent_id: &str) -> String {
        format!(
            "{}/tracey/agents/{}/deepdive",
            self.base_url,
            agent_id.trim()
        )
    }

    pub fn tracey_analytics_url(
        &self,
        window_seconds: u64,
        bucket_seconds: u64,
        log_limit: usize,
    ) -> String {
        format!(
            "{}/tracey/analytics?window_seconds={}&bucket_seconds={}&log_limit={}",
            self.base_url, window_seconds, bucket_seconds, log_limit
        )
    }

    pub fn tracey_assessment_fleet_url(&self) -> String {
        format!("{}/tracey/assessment/fleet", self.base_url)
    }

    pub async fn health(&self) -> Result<Value> {
        self.get_json("/health").await
    }

    pub async fn tracey_agents(&self) -> Result<Value> {
        self.get_json("/tracey/agents").await
    }

    pub async fn tracey_fleet(&self) -> Result<Value> {
        self.get_json("/tracey/fleet").await
    }

    pub async fn tracey_analytics(
        &self,
        window_seconds: u64,
        bucket_seconds: u64,
        log_limit: usize,
    ) -> Result<Value> {
        self.get_json(&format!(
            "/tracey/analytics?window_seconds={window_seconds}&bucket_seconds={bucket_seconds}&log_limit={log_limit}"
        ))
        .await
    }

    pub async fn tracey_assessment_fleet(&self) -> Result<Value> {
        self.get_json("/tracey/assessment/fleet").await
    }

    async fn get_json(&self, path: &str) -> Result<Value> {
        let mut request = self.client.get(format!("{}{}", self.base_url, path));
        if let Some(token) = self
            .bearer_token
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            request = request.bearer_auth(token);
        }
        let response = request.send().await?;
        super::decode_json(response).await
    }
}

pub fn candidate_base_urls(
    config: &ExternalServiceConfig,
    service: Option<&ServiceSnapshot>,
) -> Vec<String> {
    let mut candidates = Vec::new();
    push_candidate_variants(
        &mut candidates,
        config.base_url.as_deref(),
        is_continuum_public_edge_candidate,
    );
    if should_include_default_public_edges(config, service) {
        push_candidate_variants(
            &mut candidates,
            Some("https://api.neuralmimicry.ai"),
            is_continuum_public_edge_candidate,
        );
    }
    push_candidate_variants(
        &mut candidates,
        service.and_then(|item| item.public_url.as_deref()),
        is_continuum_public_edge_candidate,
    );
    push_candidate_variants(
        &mut candidates,
        service.and_then(|item| item.internal_url.as_deref()),
        is_continuum_public_edge_candidate,
    );
    candidates
}

pub async fn select_live_base_url(
    client: &Client,
    config: &ExternalServiceConfig,
    service: Option<&ServiceSnapshot>,
) -> Result<Option<String>> {
    if !config.enabled {
        return Ok(None);
    }
    let candidates = candidate_base_urls(config, service);
    if candidates.is_empty() {
        return Ok(None);
    }

    let mut attempts = Vec::new();
    for candidate in candidates {
        match super::get_json(
            client,
            &candidate,
            "/health",
            config.bearer_token.as_deref(),
        )
        .await
        {
            Ok(payload) if payload_looks_like_continuum_health(&payload) => {
                return Ok(Some(candidate));
            }
            Ok(_) => attempts.push(format!("{candidate}: unexpected health payload")),
            Err(error) => attempts.push(format!("{candidate}: {error}")),
        }
    }

    Err(anyhow!(
        "no reachable Continuum base URL available ({})",
        attempts.join("; ")
    ))
}

fn push_candidate_variants(
    candidates: &mut Vec<String>,
    raw: Option<&str>,
    should_add_monitoring_variant: fn(&str) -> bool,
) {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    let trimmed = raw.trim_end_matches('/');
    if trimmed.is_empty() {
        return;
    }
    if should_add_monitoring_variant(trimmed) {
        push_candidate(
            candidates,
            format!("{trimmed}{CONTINUUM_MONITORING_PREFIX}"),
        );
    }
    push_candidate(candidates, trimmed.to_string());
}

fn push_candidate(candidates: &mut Vec<String>, candidate: String) {
    if !candidates.iter().any(|value| value == &candidate) {
        candidates.push(candidate);
    }
}

fn should_include_default_public_edges(
    config: &ExternalServiceConfig,
    service: Option<&ServiceSnapshot>,
) -> bool {
    [
        config.base_url.as_deref(),
        service.and_then(|item| item.public_url.as_deref()),
        service.and_then(|item| item.internal_url.as_deref()),
    ]
    .into_iter()
    .flatten()
    .any(|value| value.contains("neuralmimicry.ai"))
}

fn is_continuum_public_edge_candidate(value: &str) -> bool {
    !value.contains(CONTINUUM_MONITORING_PREFIX) && value.contains("api.neuralmimicry.ai")
}

fn payload_looks_like_continuum_health(payload: &Value) -> bool {
    let payload = payload.get("data").unwrap_or(payload);
    payload
        .get("service")
        .and_then(Value::as_str)
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value.contains("nmc") || value.contains("continuum")
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::ExternalServiceConfig, models::ServiceHealth};
    use axum::{Json, Router, routing::get};
    use serde_json::json;
    use tokio::net::TcpListener;

    fn sample_service() -> ServiceSnapshot {
        ServiceSnapshot {
            service_key: "continuum".to_string(),
            display_name: "Continuum".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "continuum_tenant_server".to_string(),
            playbooks: vec![],
            host_targets: vec![],
            hosts: vec![],
            namespace: None,
            service_name: None,
            deployment_environment: None,
            internal_url: None,
            public_url: None,
            repo_path: None,
            repo_url: None,
            repo_branch: None,
            health: ServiceHealth::Healthy,
            capabilities: vec![],
            dependencies: vec![],
            storage_paths: vec![],
            raw_defaults: json!({}),
            probe: json!({}),
            discovered_at: crate::models::now_utc(),
            updated_at: crate::models::now_utc(),
        }
    }

    async fn spawn_mock_continuum() -> (String, tokio::task::JoinHandle<()>) {
        async fn health() -> Json<Value> {
            Json(json!({
                "success": true,
                "data": {"service": "nmc_server", "status": "ok"},
            }))
        }

        let app = Router::new().route("/health", get(health));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind continuum");
        let addr = listener.local_addr().expect("continuum local addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve continuum mock");
        });
        (format!("http://{}", addr), handle)
    }

    #[test]
    fn candidate_base_urls_add_monitoring_prefix_for_public_edge() {
        let mut config = ExternalServiceConfig::default();
        config.enabled = true;
        config.base_url = Some("https://api.neuralmimicry.ai".to_string());

        let candidates = candidate_base_urls(&config, None);

        assert_eq!(
            candidates,
            vec![
                "https://api.neuralmimicry.ai/services/health/monitoring".to_string(),
                "https://api.neuralmimicry.ai".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn select_live_base_url_uses_service_fallback_when_configured_primary_fails() {
        let (base_url, handle) = spawn_mock_continuum().await;
        let client = super::super::build_http_client(2).expect("http client");

        let mut config = ExternalServiceConfig::default();
        config.enabled = true;
        config.base_url = Some("http://127.0.0.1:9".to_string());

        let mut service = sample_service();
        service.public_url = Some(base_url.clone());

        let selected = select_live_base_url(&client, &config, Some(&service))
            .await
            .expect("select live base");

        assert_eq!(selected.as_deref(), Some(base_url.as_str()));
        handle.abort();
    }
}
