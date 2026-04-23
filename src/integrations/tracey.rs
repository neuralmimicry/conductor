use anyhow::{Result, anyhow};
use reqwest::{Client, StatusCode};
use serde_json::Value;

use crate::{config::ExternalServiceConfig, models::ServiceSnapshot};

#[derive(Clone)]
pub struct TraceyClient {
    client: Client,
    base_url: String,
    bearer_token: Option<String>,
}

impl TraceyClient {
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
                .ok_or_else(|| anyhow!("no Tracey base URL configured or discovered"))?
        } else {
            return Ok(None);
        };
        let client = super::build_http_client(config.timeout_seconds.max(1))?;
        Ok(Some(Self {
            client,
            base_url,
            bearer_token: config.bearer_token.clone(),
        }))
    }

    pub fn status_url(&self) -> String {
        format!("{}/status", self.base_url)
    }

    pub fn loader_status_url(&self) -> String {
        format!("{}/loader/status", self.base_url)
    }

    pub async fn status(&self) -> Result<Value> {
        self.get_json("/status").await
    }

    pub async fn loader_status(&self) -> Result<Option<Value>> {
        let mut request = self
            .client
            .get(format!("{}{}", self.base_url, "/loader/status"));
        if let Some(token) = self
            .bearer_token
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            request = request.bearer_auth(token);
        }
        let response = request.send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(super::decode_json(response).await?))
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
