use std::time::Duration;

use anyhow::{Result, anyhow};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

use crate::{
    config::{ConductorConfig, ExternalServiceConfig},
    models::{ProbeResult, ServiceHealth, ServiceSnapshot},
};

pub mod atlassian;
pub mod continuum;
pub mod refiner;
pub mod tracey;

pub fn build_http_client(timeout_seconds: u64) -> Result<Client> {
    Ok(Client::builder()
        .use_rustls_tls()
        .timeout(Duration::from_secs(timeout_seconds.max(1)))
        .build()?)
}

pub async fn probe_service(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    match service.service_key.as_str() {
        "gail" => probe_gail(client, config, service).await,
        "tracey" => probe_tracey(client, config, service).await,
        "continuum" => probe_continuum(client, config, service).await,
        "refiner" => probe_refiner(client, config, service).await,
        "aarnn" => probe_aarnn(client, config, service).await,
        "ollama" => probe_ollama(client, config, service).await,
        _ => {
            probe_generic(
                client,
                resolve_external_config(config, service.service_key.as_str()),
                service,
            )
            .await
        }
    }
}

fn resolve_external_config<'a>(
    config: &'a ConductorConfig,
    service_key: &str,
) -> &'a ExternalServiceConfig {
    match service_key {
        "gail" => &config.integrations.gail,
        "tracey" => &config.integrations.tracey,
        "continuum" => &config.integrations.continuum,
        "refiner" => &config.integrations.refiner,
        "aarnn" => &config.integrations.aarnn,
        "ollama" => &config.integrations.ollama,
        _ => &config.integrations.continuum,
    }
}

pub(crate) fn resolve_base_url(
    service: &ServiceSnapshot,
    external: &ExternalServiceConfig,
) -> Option<String> {
    external
        .base_url
        .clone()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| service.public_url.clone())
        .or_else(|| service.internal_url.clone())
        .map(|value| value.trim_end_matches('/').to_string())
}

async fn probe_gail(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let external = &config.integrations.gail;
    let base_url =
        resolve_base_url(service, external).ok_or_else(|| anyhow!("no Gail base URL available"))?;
    let health = get_json(
        client,
        &base_url,
        "/healthz",
        external.bearer_token.as_deref(),
    )
    .await?;
    let orchestration = get_json(
        client,
        &base_url,
        "/v1/status/orchestration?limit=8",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    Ok(ProbeResult {
        endpoint: Some(base_url),
        summary: if orchestration.is_some() {
            "Gail health and orchestration status retrieved".to_string()
        } else {
            "Gail health retrieved; orchestration detail unavailable".to_string()
        },
        metrics: json!({
            "health": health,
            "orchestration": orchestration,
        }),
        health: ServiceHealth::Healthy,
    })
}

async fn probe_tracey(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let external = &config.integrations.tracey;
    let base_url = resolve_base_url(service, external)
        .ok_or_else(|| anyhow!("no Tracey base URL available"))?;
    let health = match get_json(
        client,
        &base_url,
        "/health",
        external.bearer_token.as_deref(),
    )
    .await
    {
        Ok(value) => value,
        Err(_) => {
            get_json(
                client,
                &base_url,
                "/ready",
                external.bearer_token.as_deref(),
            )
            .await?
        }
    };
    let status = get_json(
        client,
        &base_url,
        "/status",
        external.bearer_token.as_deref(),
    )
    .await?;
    let loader_status = get_json(
        client,
        &base_url,
        "/loader/status",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    Ok(ProbeResult {
        endpoint: Some(base_url),
        summary: if loader_status.is_some() {
            "Tracey health, runtime, and loader surfaces retrieved".to_string()
        } else {
            "Tracey health and runtime surfaces retrieved".to_string()
        },
        metrics: json!({
            "health": health,
            "status": status,
            "loader_status": loader_status,
            "continuum_loop": status.get("continuum_loop").cloned(),
            "resource_forecast": status.get("resource_forecast").cloned(),
        }),
        health: ServiceHealth::Healthy,
    })
}

async fn probe_continuum(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let external = &config.integrations.continuum;
    let base_url = continuum::select_live_base_url(client, external, Some(service))
        .await?
        .ok_or_else(|| anyhow!("no Continuum base URL available"))?;
    let health = get_json(
        client,
        &base_url,
        "/health",
        external.bearer_token.as_deref(),
    )
    .await?;
    let adaptive = get_json(
        client,
        &base_url,
        "/tracey/adaptive",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    let agents = get_json(
        client,
        &base_url,
        "/tracey/agents",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    let fleet = get_json(
        client,
        &base_url,
        "/tracey/fleet",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    let analytics = get_json(
        client,
        &base_url,
        "/tracey/analytics?window_seconds=7200&bucket_seconds=120&log_limit=25",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    let assessment = get_json(
        client,
        &base_url,
        "/tracey/assessment/fleet",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    let clusters = get_json(
        client,
        &base_url,
        "/k8s/list",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    let refiner = get_json(
        client,
        &base_url,
        "/k8s/refiner/status",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    Ok(ProbeResult {
        endpoint: Some(base_url),
        summary: "Continuum control-plane surfaces retrieved".to_string(),
        metrics: json!({
            "health": health,
            "tracey_adaptive": adaptive,
            "tracey_agents": agents,
            "tracey_fleet": fleet,
            "tracey_analytics": analytics,
            "tracey_assessment_fleet": assessment,
            "k8s_clusters": clusters,
            "refiner": refiner,
        }),
        health: ServiceHealth::Healthy,
    })
}

async fn probe_refiner(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let external = &config.integrations.refiner;
    let base_url = resolve_base_url(service, external)
        .ok_or_else(|| anyhow!("no Refiner base URL available"))?;
    let health = get_json(
        client,
        &base_url,
        "/api/health",
        external.bearer_token.as_deref(),
    )
    .await?;
    let capabilities = get_json(
        client,
        &base_url,
        "/api/capabilities",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    let orchestration = get_json(
        client,
        &base_url,
        "/api/admin/ai-orchestration?limit=8",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    Ok(ProbeResult {
        endpoint: Some(base_url),
        summary: "Refiner control-room API retrieved".to_string(),
        metrics: json!({
            "health": health,
            "capabilities": capabilities,
            "ai_orchestration": orchestration,
        }),
        health: ServiceHealth::Healthy,
    })
}

async fn probe_aarnn(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let external = &config.integrations.aarnn;
    let base_url = resolve_base_url(service, external)
        .ok_or_else(|| anyhow!("no AARNN base URL available"))?;
    let runtime_status = get_json(
        client,
        &base_url,
        "/api/runtime/status",
        external.bearer_token.as_deref(),
    )
    .await?;
    let cluster_status = get_json(
        client,
        &base_url,
        "/api/status",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    let activity = get_json(
        client,
        &base_url,
        "/api/activity",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    Ok(ProbeResult {
        endpoint: Some(base_url),
        summary: "AARNN runtime and cluster status retrieved".to_string(),
        metrics: json!({
            "runtime_status": runtime_status,
            "cluster_status": cluster_status,
            "activity": activity,
        }),
        health: ServiceHealth::Healthy,
    })
}

async fn probe_ollama(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let external = resolve_external_config(config, service.service_key.as_str());
    let base_url = resolve_base_url(service, external)
        .ok_or_else(|| anyhow!("no Ollama base URL available"))?;

    let tags = get_json(
        client,
        &base_url,
        "/api/tags",
        external.bearer_token.as_deref(),
    )
    .await?;

    let model_count = tags
        .get("models")
        .and_then(Value::as_array)
        .map(|models| models.len())
        .unwrap_or(0);

    Ok(ProbeResult {
        endpoint: Some(base_url),
        summary: format!(
            "Ollama availability confirmed via /api/tags ({} models visible)",
            model_count
        ),
        metrics: json!({
            "availability": tags,
            "model_count": model_count,
        }),
        health: ServiceHealth::Healthy,
    })
}
async fn probe_generic(
    client: &Client,
    external: &ExternalServiceConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let base_url =
        resolve_base_url(service, external).ok_or_else(|| anyhow!("no base URL available"))?;

    let (health, path_used) = match get_json(
        client,
        &base_url,
        "/healthz",
        external.bearer_token.as_deref(),
    )
    .await
    {
        Ok(value) => (value, "/healthz"),
        Err(_) => match get_json(
            client,
            &base_url,
            "/health",
            external.bearer_token.as_deref(),
        )
        .await
        {
            Ok(value) => (value, "/health"),
            Err(_) => {
                let value = get_json(
                    client,
                    &base_url,
                    "/api/tags",
                    external.bearer_token.as_deref(),
                )
                .await?;
                (value, "/api/tags")
            }
        },
    };

    Ok(ProbeResult {
        endpoint: Some(base_url),
        summary: format!(
            "{} health surface retrieved via {}",
            service.display_name, path_used
        ),
        metrics: json!({
            "health": health,
            "path_used": path_used,
        }),
        health: ServiceHealth::Healthy,
    })
}

pub async fn gail_plan_summary(
    client: &Client,
    config: &ConductorConfig,
    topology_summary: &Value,
    discovered_base_url: Option<&str>,
) -> Result<Option<Value>> {
    if !config.integrations.gail.enabled {
        return Ok(None);
    }
    let Some(base_url) = config
        .integrations
        .gail
        .base_url
        .clone()
        .or_else(|| discovered_base_url.map(ToString::to_string))
    else {
        return Ok(None);
    };
    let prompt = format!(
        "Summarize the three highest-leverage reliability, performance, and self-improvement actions for this topology. Use concise bullets only. Topology summary: {}",
        topology_summary
    );
    let completion = post_json(
        client,
        &base_url,
        "/v1/llm/complete",
        config.integrations.gail.bearer_token.as_deref(),
        &json!({
            "workflow": config.planning.gail_workflow,
            "role": "planner",
            "messages": [
                {"role": "system", "content": "You are the NeuralMimicry Conductor planner. Focus on reliability, latency, resource use, and safe self-improvement."},
                {"role": "user", "content": prompt}
            ],
            "include_configured": true,
            "selection_mode": "best",
            "max_candidates": 3,
            "timeout_seconds": 30
        }),
    )
    .await
    .ok();

    let neuromorphic = post_json(
        client,
        &base_url,
        "/v1/neuromorphic/analyze",
        config.integrations.gail.bearer_token.as_deref(),
        &json!({
            "workflow": config.planning.gail_workflow,
            "role": "researcher",
            "text": prompt,
        }),
    )
    .await
    .ok();

    if completion.is_none() && neuromorphic.is_none() {
        return Ok(None);
    }

    Ok(Some(json!({
        "completion": completion,
        "neuromorphic": neuromorphic,
    })))
}

pub(crate) async fn get_json(
    client: &Client,
    base_url: &str,
    path: &str,
    bearer_token: Option<&str>,
) -> Result<Value> {
    let mut request = client.get(format!("{}{}", base_url.trim_end_matches('/'), path));
    if let Some(token) = bearer_token.filter(|value| !value.trim().is_empty()) {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    decode_json(response).await
}

pub(crate) async fn post_json(
    client: &Client,
    base_url: &str,
    path: &str,
    bearer_token: Option<&str>,
    body: &Value,
) -> Result<Value> {
    let mut request = client
        .post(format!("{}{}", base_url.trim_end_matches('/'), path))
        .json(body);
    if let Some(token) = bearer_token.filter(|value| !value.trim().is_empty()) {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    decode_json(response).await
}

pub(crate) async fn decode_json(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
    if status.is_success() {
        return Ok(body);
    }
    let message = body
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| body.get("error").and_then(Value::as_str))
        .unwrap_or("request failed");
    let prefix = match status {
        StatusCode::UNAUTHORIZED => "unauthorized",
        StatusCode::FORBIDDEN => "forbidden",
        StatusCode::NOT_FOUND => "not_found",
        _ => "upstream_error",
    };
    Err(anyhow!("{}: {}", prefix, message))
}
