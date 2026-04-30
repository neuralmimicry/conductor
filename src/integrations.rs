use std::{fs, path::Path, time::Duration};

use anyhow::{Result, anyhow};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    config::{ConductorConfig, ExternalServiceConfig},
    models::{ProbeResult, ServiceHealth, ServiceSnapshot},
};

pub mod atlassian;
pub mod continuum;
pub mod refiner;
pub mod tracey;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitHubActionsRunEvidence {
    pub id: u64,
    pub name: Option<String>,
    pub status: Option<String>,
    pub conclusion: Option<String>,
    pub html_url: Option<String>,
    pub run_number: Option<u64>,
    pub head_sha: Option<String>,
    pub event: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitHubActionsEvidence {
    pub workflow_file: String,
    pub owner: Option<String>,
    pub repository: Option<String>,
    pub branch: Option<String>,
    pub succeeded: bool,
    pub reason: String,
    pub run: Option<GitHubActionsRunEvidence>,
}

#[derive(Debug, Deserialize)]
struct GitHubWorkflowRunsResponse {
    #[serde(default)]
    workflow_runs: Vec<GitHubWorkflowRun>,
}

#[derive(Debug, Deserialize)]
struct GitHubWorkflowRun {
    id: u64,
    name: Option<String>,
    status: Option<String>,
    conclusion: Option<String>,
    html_url: Option<String>,
    run_number: Option<u64>,
    head_sha: Option<String>,
    event: Option<String>,
    updated_at: Option<String>,
}

pub fn build_http_client(timeout_seconds: u64) -> Result<Client> {
    Ok(Client::builder()
        .use_rustls_tls()
        .timeout(Duration::from_secs(timeout_seconds.max(1)))
        .build()?)
}

pub fn github_repository_coordinate(url: &str) -> Option<(String, String)> {
    let trimmed = url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }

    let path = if let Some(rest) = trimmed.strip_prefix("git@") {
        rest.split_once(':')?.1
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        rest.split_once('/')?.1
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        rest.split_once('/')?.1
    } else {
        return None;
    };

    let mut parts = path.split('/');
    let owner = parts.next()?.trim();
    let repository = parts.next()?.trim().trim_end_matches(".git");
    if owner.is_empty() || repository.is_empty() {
        return None;
    }
    Some((owner.to_string(), repository.to_string()))
}

pub async fn fetch_latest_github_actions_evidence(
    client: &Client,
    config: &ConductorConfig,
    owner: &str,
    repository: &str,
    branch: &str,
    workflow_file: &str,
) -> Result<GitHubActionsEvidence> {
    let workflow_file = workflow_file.trim();
    let branch = branch.trim();
    let base_url = config.discovery.github.api_base_url.trim_end_matches('/');
    let url = format!(
        "{}/repos/{}/{}/actions/workflows/{}/runs",
        base_url, owner, repository, workflow_file
    );

    let mut request = client
        .get(url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "neuralmimicry-conductor");
    if let Some(token) = config
        .discovery
        .github
        .token
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        request = request.bearer_auth(token);
    }

    let response = request
        .query(&[
            ("branch", branch),
            ("status", "completed"),
            ("per_page", "1"),
        ])
        .send()
        .await?;
    let status = response.status();

    if status == StatusCode::NOT_FOUND {
        return Ok(GitHubActionsEvidence {
            workflow_file: workflow_file.to_string(),
            owner: Some(owner.to_string()),
            repository: Some(repository.to_string()),
            branch: Some(branch.to_string()),
            succeeded: false,
            reason: format!(
                "GitHub Actions workflow {} is missing or has no completed runs for {}/{} on branch {}",
                workflow_file, owner, repository, branch
            ),
            run: None,
        });
    }

    if !status.is_success() {
        let payload = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
        let message = payload
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| payload.get("error").and_then(Value::as_str))
            .unwrap_or("request failed");
        return Err(anyhow!(
            "GitHub Actions query failed with {} for {}/{} on branch {}: {}",
            status,
            owner,
            repository,
            branch,
            message
        ));
    }

    let payload = response.json::<GitHubWorkflowRunsResponse>().await?;
    let run = payload.workflow_runs.into_iter().next();
    let Some(run) = run else {
        return Ok(GitHubActionsEvidence {
            workflow_file: workflow_file.to_string(),
            owner: Some(owner.to_string()),
            repository: Some(repository.to_string()),
            branch: Some(branch.to_string()),
            succeeded: false,
            reason: format!(
                "GitHub Actions workflow {} has no completed runs for {}/{} on branch {}",
                workflow_file, owner, repository, branch
            ),
            run: None,
        });
    };

    let succeeded = run
        .conclusion
        .as_deref()
        .is_some_and(|conclusion| conclusion.eq_ignore_ascii_case("success"));
    let conclusion = run
        .conclusion
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let reason = if succeeded {
        format!(
            "GitHub Actions workflow {} succeeded for {}/{} on branch {}",
            workflow_file, owner, repository, branch
        )
    } else {
        format!(
            "GitHub Actions workflow {} concluded with {} for {}/{} on branch {}",
            workflow_file, conclusion, owner, repository, branch
        )
    };

    Ok(GitHubActionsEvidence {
        workflow_file: workflow_file.to_string(),
        owner: Some(owner.to_string()),
        repository: Some(repository.to_string()),
        branch: Some(branch.to_string()),
        succeeded,
        reason,
        run: Some(GitHubActionsRunEvidence {
            id: run.id,
            name: run.name,
            status: run.status,
            conclusion: run.conclusion,
            html_url: run.html_url,
            run_number: run.run_number,
            head_sha: run.head_sha,
            event: run.event,
            updated_at: run.updated_at,
        }),
    })
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
        "swarmhpc" => probe_swarmhpc(config, service).await,
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
    let trading = get_json(
        client,
        &base_url,
        "/v1/trading/status",
        external.bearer_token.as_deref(),
    )
    .await
    .ok();
    Ok(ProbeResult {
        endpoint: Some(base_url),
        summary: if orchestration.is_some() && trading.is_some() {
            "Gail health, orchestration, and trading status retrieved".to_string()
        } else if orchestration.is_some() {
            "Gail health and orchestration status retrieved".to_string()
        } else {
            "Gail health retrieved; orchestration detail unavailable".to_string()
        },
        metrics: json!({
            "health": health,
            "orchestration": orchestration,
            "trading": trading,
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

async fn probe_swarmhpc(
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let ansible_root = service
        .raw_defaults
        .get("ansible_root")
        .and_then(Value::as_str)
        .map(Path::new)
        .filter(|path| path.exists())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| config.discovery.ansible_root.clone());
    let config_path = ansible_root.join("ansible.cfg");
    let inventory_path = ansible_root.join("inventory").join("hosts.ini");
    let roles_path = ansible_root.join("roles");
    let secrets_path = ansible_root.join(".secrets");
    let playbook_count = fs::read_dir(&ansible_root)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_type()
                .ok()
                .is_some_and(|file_type| file_type.is_file())
        })
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| matches!(extension, "yml" | "yaml"))
        })
        .count();
    let healthy = config_path.exists() && inventory_path.exists() && roles_path.exists();
    Ok(ProbeResult {
        endpoint: None,
        summary: if healthy {
            format!(
                "SwarmHPC deployment automation is available with {} playbook(s)",
                playbook_count
            )
        } else {
            "SwarmHPC deployment automation is missing one or more core Ansible paths".to_string()
        },
        metrics: json!({
            "ansible_root": ansible_root.display().to_string(),
            "ansible_config_path": config_path.display().to_string(),
            "inventory_path": inventory_path.display().to_string(),
            "roles_path": roles_path.display().to_string(),
            "secrets_path": secrets_path.display().to_string(),
            "ansible_config_exists": config_path.exists(),
            "inventory_exists": inventory_path.exists(),
            "roles_path_exists": roles_path.exists(),
            "secrets_path_exists": secrets_path.exists(),
            "playbook_count": playbook_count,
            "playbooks": service.playbooks,
        }),
        health: if healthy {
            ServiceHealth::Healthy
        } else {
            ServiceHealth::Degraded
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::get};
    use tokio::net::TcpListener;

    async fn spawn_mock_github(
        run_conclusion: &'static str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/repos/neuralmimicry/conductor/actions/workflows/ci.yml/runs",
            get(move || async move {
                Json(json!({
                    "workflow_runs": [
                        {
                            "id": 42,
                            "name": "CI",
                            "status": "completed",
                            "conclusion": run_conclusion,
                            "html_url": "https://github.com/neuralmimicry/conductor/actions/runs/42",
                            "run_number": 7,
                            "head_sha": "abc123",
                            "event": "push",
                            "updated_at": "2026-04-30T10:00:00Z"
                        }
                    ]
                }))
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind github mock");
        let addr = listener.local_addr().expect("github mock addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve github mock");
        });
        (format!("http://{}", addr), handle)
    }

    #[tokio::test]
    async fn fetch_latest_github_actions_evidence_reports_success() {
        let (base_url, handle) = spawn_mock_github("success").await;
        let mut config = ConductorConfig::default();
        config.discovery.github.api_base_url = base_url;

        let client = build_http_client(5).expect("client");
        let evidence = fetch_latest_github_actions_evidence(
            &client,
            &config,
            "neuralmimicry",
            "conductor",
            "main",
            "ci.yml",
        )
        .await
        .expect("github evidence");

        assert!(evidence.succeeded);
        assert_eq!(
            evidence
                .run
                .as_ref()
                .and_then(|run| run.conclusion.as_deref()),
            Some("success")
        );

        handle.abort();
    }
}
