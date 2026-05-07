use anyhow::{Result, anyhow};
use reqwest::Client;
use serde_json::{Value, json};

use crate::{config::ExternalServiceConfig, models::ServiceSnapshot};

const REFINER_PUBLIC_ALIAS_URL: &str = "https://refiner.neuralmimicry.ai";
const REFINER_PUBLIC_EDGE_URL: &str = "https://api.neuralmimicry.ai";

#[derive(Clone)]
pub struct RefinerClient {
    client: Client,
    base_url: String,
    bearer_token: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

impl RefinerClient {
    pub async fn from_sources(
        config: &ExternalServiceConfig,
        service: Option<&ServiceSnapshot>,
    ) -> Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }
        let client = Client::builder()
            .use_rustls_tls()
            .cookie_store(true)
            .timeout(std::time::Duration::from_secs(
                config.timeout_seconds.max(1),
            ))
            .build()?;
        let base_url = select_live_base_url(&client, config, service)
            .await?
            .ok_or_else(|| anyhow!("no Refiner base URL configured or discovered"))?;
        Ok(Some(Self {
            client,
            base_url,
            bearer_token: config.bearer_token.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
        }))
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn job_url(&self, job_id: &str) -> String {
        format!("{}/api/jobs/{}", self.base_url, job_id.trim())
    }

    pub fn requirements_progress_url(&self, job_id: &str) -> String {
        format!(
            "{}/api/jobs/{}/requirements/progress",
            self.base_url,
            job_id.trim()
        )
    }

    pub fn requirements_summary_url(&self, job_id: &str) -> String {
        format!(
            "{}/api/jobs/{}/requirements/summary",
            self.base_url,
            job_id.trim()
        )
    }

    pub fn workspace_url(&self, job_id: &str) -> String {
        format!("{}/api/jobs/{}/workspace", self.base_url, job_id.trim())
    }

    pub async fn get_health(&self) -> Result<Value> {
        self.get_json("/api/health").await
    }

    pub async fn get_capabilities(&self) -> Result<Value> {
        self.get_json("/api/capabilities").await
    }

    pub async fn get_ai_orchestration(&self, limit: usize) -> Result<Value> {
        let limit = limit.clamp(1, 100);
        self.get_json(&format!("/api/admin/ai-orchestration?limit={limit}"))
            .await
    }

    pub async fn login_if_configured(&self) -> Result<()> {
        let (Some(username), Some(password)) = (
            self.username
                .as_deref()
                .filter(|value| !value.trim().is_empty()),
            self.password
                .as_deref()
                .filter(|value| !value.trim().is_empty()),
        ) else {
            return Ok(());
        };
        let response = self
            .client
            .post(format!("{}/api/login", self.base_url))
            .json(&json!({"username": username, "password": password}))
            .send()
            .await?;
        let status = response.status();
        let payload = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
        if status.is_success() {
            return Ok(());
        }
        let message = payload
            .get("details")
            .and_then(Value::as_str)
            .or_else(|| payload.get("error").and_then(Value::as_str))
            .or_else(|| payload.get("message").and_then(Value::as_str))
            .unwrap_or("refiner login failed");
        Err(anyhow!("refiner login failed: {}", message))
    }

    pub async fn get_job(&self, job_id: &str) -> Result<Value> {
        self.get_json(&format!("/api/jobs/{}", job_id.trim())).await
    }

    pub async fn get_requirements_progress(&self, job_id: &str) -> Result<Value> {
        self.get_json(&format!(
            "/api/jobs/{}/requirements/progress",
            job_id.trim()
        ))
        .await
    }

    pub async fn get_requirements_summary(&self, job_id: &str) -> Result<Value> {
        self.get_json(&format!("/api/jobs/{}/requirements/summary", job_id.trim()))
            .await
    }

    pub async fn get_workspace(&self, job_id: &str) -> Result<Value> {
        self.get_json(&format!("/api/jobs/{}/workspace", job_id.trim()))
            .await
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
    push_candidate(&mut candidates, config.base_url.as_deref());
    if should_include_default_public_edges(config, service) {
        push_candidate(&mut candidates, Some(REFINER_PUBLIC_ALIAS_URL));
    }
    push_candidate(
        &mut candidates,
        service.and_then(|item| item.public_url.as_deref()),
    );
    if should_include_default_public_edges(config, service) {
        push_candidate(&mut candidates, Some(REFINER_PUBLIC_EDGE_URL));
    }
    push_candidate(
        &mut candidates,
        service.and_then(|item| item.internal_url.as_deref()),
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
            "/api/health",
            config.bearer_token.as_deref(),
        )
        .await
        {
            Ok(_) => return Ok(Some(candidate)),
            Err(error) => attempts.push(format!("{candidate}: {error}")),
        }
    }

    Err(anyhow!(
        "no reachable Refiner base URL available ({})",
        attempts.join("; ")
    ))
}

fn push_candidate(candidates: &mut Vec<String>, raw: Option<&str>) {
    let Some(candidate) = raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.trim_end_matches('/').to_string())
    else {
        return;
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::ExternalServiceConfig, models::ServiceHealth};
    use axum::{Json, Router, routing::get};
    use serde_json::json;
    use tokio::net::TcpListener;

    fn sample_service() -> ServiceSnapshot {
        ServiceSnapshot {
            service_key: "refiner".to_string(),
            display_name: "Refiner".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "continuum_tenant_refiner".to_string(),
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

    async fn spawn_mock_refiner() -> (String, tokio::task::JoinHandle<()>) {
        async fn health() -> Json<Value> {
            Json(json!({"status": "ok"}))
        }

        let app = Router::new().route("/api/health", get(health));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind refiner");
        let addr = listener.local_addr().expect("refiner local addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve refiner mock");
        });
        (format!("http://{}", addr), handle)
    }

    #[test]
    fn candidate_base_urls_prioritize_alias_then_legacy_edge() {
        let mut config = ExternalServiceConfig::default();
        config.enabled = true;
        config.base_url = Some("https://refiner.neuralmimicry.ai".to_string());

        let mut service = sample_service();
        service.public_url = Some("https://api.neuralmimicry.ai".to_string());
        service.internal_url = Some("http://refiner.refiner.svc.cluster.local:5001".to_string());

        let candidates = candidate_base_urls(&config, Some(&service));

        assert_eq!(
            candidates,
            vec![
                "https://refiner.neuralmimicry.ai".to_string(),
                "https://api.neuralmimicry.ai".to_string(),
                "http://refiner.refiner.svc.cluster.local:5001".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn select_live_base_url_falls_back_to_service_when_primary_fails() {
        let (base_url, handle) = spawn_mock_refiner().await;
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
