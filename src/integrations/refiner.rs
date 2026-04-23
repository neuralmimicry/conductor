use anyhow::{Result, anyhow};
use reqwest::Client;
use serde_json::{Value, json};

use crate::{config::ExternalServiceConfig, models::ServiceSnapshot};

#[derive(Clone)]
pub struct RefinerClient {
    client: Client,
    base_url: String,
    bearer_token: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

impl RefinerClient {
    pub fn from_sources(
        config: &ExternalServiceConfig,
        service: Option<&ServiceSnapshot>,
    ) -> Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }
        let base_url = if let Some(base_url) = config
            .base_url
            .clone()
            .filter(|value| !value.trim().is_empty())
        {
            base_url.trim_end_matches('/').to_string()
        } else if let Some(service) = service {
            super::resolve_base_url(service, config)
                .ok_or_else(|| anyhow!("no Refiner base URL configured or discovered"))?
        } else {
            return Ok(None);
        };
        let client = Client::builder()
            .use_rustls_tls()
            .cookie_store(true)
            .timeout(std::time::Duration::from_secs(
                config.timeout_seconds.max(1),
            ))
            .build()?;
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
