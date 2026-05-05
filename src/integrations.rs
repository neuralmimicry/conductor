use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    fs,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Result, anyhow};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Connection, Row, postgres::PgConnection};

use crate::{
    config::{
        ConductorConfig, ExternalServiceConfig, PostgresIntegrationConfig,
        SharedStorageIntegrationConfig,
    },
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

#[derive(Clone, Debug, Default)]
struct PrometheusJobSummary {
    service_key: Option<String>,
    total_targets: usize,
    down_targets: usize,
    healthy_targets: usize,
    last_errors: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct FilesystemProbeStats {
    bytes_total: u64,
    bytes_free: u64,
    bytes_available: u64,
    inode_total: u64,
    inode_free: u64,
    inode_available: u64,
    read_only: bool,
}

#[derive(Clone, Debug)]
struct MountInfo {
    source: String,
    target: String,
    fs_type: String,
    options: Vec<String>,
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
        "grafana" => probe_grafana(client, config, service).await,
        "prometheus" => probe_prometheus(client, config, service).await,
        "postgres" => probe_postgres(config, service).await,
        "shared-storage" => probe_shared_storage(config, service).await,
        _ => {
            probe_generic(
                client,
                resolve_http_external_config(config, service.service_key.as_str()),
                service,
            )
            .await
        }
    }
}

fn resolve_http_external_config<'a>(
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
        "grafana" => &config.integrations.grafana,
        "prometheus" => &config.integrations.prometheus,
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

fn base_url_candidates(service: &ServiceSnapshot, external: &ExternalServiceConfig) -> Vec<String> {
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();
    for value in [
        external.base_url.as_deref(),
        service.public_url.as_deref(),
        service.internal_url.as_deref(),
    ] {
        let Some(candidate) = value else {
            continue;
        };
        let candidate = candidate.trim_end_matches('/').trim();
        if candidate.is_empty() {
            continue;
        }
        if seen.insert(candidate.to_string()) {
            candidates.push(candidate.to_string());
        }
    }
    candidates
}

fn apply_external_auth(
    mut request: reqwest::RequestBuilder,
    external: &ExternalServiceConfig,
) -> reqwest::RequestBuilder {
    if let Some(token) = external
        .bearer_token
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        request = request.bearer_auth(token);
    } else if let Some(username) = external
        .username
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        request = request.basic_auth(username, external.password.as_deref());
    }
    request
}

async fn get_json_with_auth(
    client: &Client,
    base_url: &str,
    path: &str,
    external: &ExternalServiceConfig,
) -> Result<Value> {
    let request = client.get(format!("{}{}", base_url.trim_end_matches('/'), path));
    let response = apply_external_auth(request, external).send().await?;
    decode_json(response).await
}

async fn get_json_with_auth_query<T: Serialize + ?Sized>(
    client: &Client,
    base_url: &str,
    path: &str,
    query: &T,
    external: &ExternalServiceConfig,
) -> Result<Value> {
    let request = client
        .get(format!("{}{}", base_url.trim_end_matches('/'), path))
        .query(query);
    let response = apply_external_auth(request, external).send().await?;
    decode_json(response).await
}

fn prometheus_payload_data<'a>(payload: &'a Value, path: &str) -> Result<&'a Value> {
    if payload.get("status").and_then(Value::as_str) != Some("success") {
        return Err(anyhow!("invalid Prometheus response status"));
    }
    payload
        .get("data")
        .ok_or_else(|| anyhow!("Prometheus response is missing data for {}", path))
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

async fn probe_grafana(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let external = &config.integrations.grafana;
    let mut last_error = None;
    for base_url in base_url_candidates(service, external) {
        match get_json_with_auth(client, &base_url, "/api/health", external).await {
            Ok(health) => {
                if health.get("database").is_none() && health.get("version").is_none() {
                    last_error = Some(anyhow!("invalid Grafana health payload"));
                    continue;
                }

                let datasources =
                    get_json_with_auth(client, &base_url, "/api/datasources", external)
                        .await
                        .ok();
                let dashboards = get_json_with_auth_query(
                    client,
                    &base_url,
                    "/api/search",
                    &[("type", "dash-db"), ("limit", "1000")],
                    external,
                )
                .await
                .ok();

                let datasource_count = datasources
                    .as_ref()
                    .and_then(Value::as_array)
                    .map(|items| items.len())
                    .unwrap_or(0);
                let dashboard_count = dashboards
                    .as_ref()
                    .and_then(Value::as_array)
                    .map(|items| items.len())
                    .unwrap_or(0);
                let datasource_sample = datasources
                    .as_ref()
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .take(10)
                            .map(|item| {
                                json!({
                                    "uid": item.get("uid"),
                                    "name": item.get("name"),
                                    "type": item.get("type"),
                                    "is_default": item.get("isDefault"),
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let dashboard_sample = dashboards
                    .as_ref()
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .take(10)
                            .map(|item| {
                                json!({
                                    "uid": item.get("uid"),
                                    "title": item.get("title"),
                                    "folder": item.get("folderTitle"),
                                    "type": item.get("type"),
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let database_status = health
                    .get("database")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");

                return Ok(ProbeResult {
                    endpoint: Some(base_url),
                    summary: format!(
                        "Grafana health retrieved with {} datasource(s) and {} dashboard(s)",
                        datasource_count, dashboard_count
                    ),
                    metrics: json!({
                        "health": health,
                        "database_status": database_status,
                        "datasource_count": datasource_count,
                        "dashboard_count": dashboard_count,
                        "datasources": datasource_sample,
                        "dashboards": dashboard_sample,
                    }),
                    health: if database_status.eq_ignore_ascii_case("ok") {
                        ServiceHealth::Healthy
                    } else {
                        ServiceHealth::Degraded
                    },
                });
            }
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no Grafana base URL available")))
}

async fn probe_prometheus(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let external = &config.integrations.prometheus;
    let mut last_error = None;
    for base_url in base_url_candidates(service, external) {
        let runtime =
            match get_json_with_auth(client, &base_url, "/api/v1/status/runtimeinfo", external)
                .await
            {
                Ok(payload) => payload,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
        let runtime_data = match prometheus_payload_data(&runtime, "/api/v1/status/runtimeinfo") {
            Ok(value) => value.clone(),
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };

        let targets_payload = match get_json_with_auth_query(
            client,
            &base_url,
            "/api/v1/targets",
            &[("state", "any")],
            external,
        )
        .await
        {
            Ok(payload) => payload,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };
        let targets_data = match prometheus_payload_data(&targets_payload, "/api/v1/targets") {
            Ok(value) => value,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };

        let (jobs, active_targets_total, dropped_targets_total, down_targets_total) =
            summarize_prometheus_targets(targets_data);
        let jobs_with_failures = jobs.values().filter(|job| job.down_targets > 0).count();
        let job_summaries = jobs
            .into_iter()
            .map(|(job_name, summary)| {
                let down_ratio = if summary.total_targets > 0 {
                    summary.down_targets as f64 / summary.total_targets as f64
                } else {
                    0.0
                };
                json!({
                    "job": job_name,
                    "service_key": summary.service_key,
                    "total_targets": summary.total_targets,
                    "healthy_targets": summary.healthy_targets,
                    "down_targets": summary.down_targets,
                    "down_ratio": down_ratio,
                    "last_errors": summary.last_errors.into_iter().collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();

        return Ok(ProbeResult {
            endpoint: Some(base_url),
            summary: format!(
                "Prometheus runtime retrieved with {} active target(s); {} target(s) are down",
                active_targets_total, down_targets_total
            ),
            metrics: json!({
                "runtimeinfo": runtime_data,
                "targets": {
                    "active_targets_total": active_targets_total,
                    "dropped_targets_total": dropped_targets_total,
                    "down_targets_total": down_targets_total,
                    "jobs_with_failures": jobs_with_failures,
                    "jobs": job_summaries,
                }
            }),
            health: if active_targets_total == 0 {
                ServiceHealth::Degraded
            } else {
                ServiceHealth::Healthy
            },
        });
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no Prometheus base URL available")))
}

async fn probe_postgres(
    config: &ConductorConfig,
    _service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let integration = &config.integrations.postgres;
    let (connection_string, connection_source) =
        resolve_postgres_connection_string(config, integration)
            .ok_or_else(|| anyhow!("no Postgres connection string available"))?;
    let timeout_seconds = integration.timeout_seconds.max(1);
    let metrics = tokio::time::timeout(Duration::from_secs(timeout_seconds), async move {
        let mut connection = PgConnection::connect(&connection_string).await?;

        let current_database: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&mut connection)
            .await?;
        let max_connections: i64 =
            sqlx::query_scalar("SELECT current_setting('max_connections')::bigint")
                .fetch_one(&mut connection)
                .await?;

        let aggregate_row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(numbackends), 0)::bigint AS total_connections,
                COUNT(*)::bigint AS database_count,
                COALESCE(MAX(CASE WHEN datname = current_database() THEN numbackends END), 0)::bigint
                    AS current_database_connections
            FROM pg_stat_database
            WHERE datistemplate = false
            "#,
        )
        .fetch_one(&mut connection)
        .await?;

        let current_database_row = sqlx::query(
            r#"
            SELECT
                numbackends,
                xact_commit,
                xact_rollback,
                blks_read,
                blks_hit,
                deadlocks,
                temp_files,
                pg_database_size(current_database())::bigint AS size_bytes
            FROM pg_stat_database
            WHERE datname = current_database()
            "#,
        )
        .fetch_one(&mut connection)
        .await?;

        let activity_row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL)::bigint AS waiting_connections,
                COUNT(*) FILTER (WHERE state = 'idle in transaction')::bigint AS idle_in_transaction
            FROM pg_stat_activity
            WHERE datname = current_database()
            "#,
        )
        .fetch_optional(&mut connection)
        .await
        .ok()
        .flatten();

        let top_databases = sqlx::query(
            r#"
            SELECT datname, numbackends
            FROM pg_stat_database
            WHERE datistemplate = false
            ORDER BY numbackends DESC, datname ASC
            LIMIT 8
            "#,
        )
        .fetch_all(&mut connection)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            json!({
                "database": row.try_get::<String, _>("datname").ok(),
                "connections": row.try_get::<i64, _>("numbackends").ok(),
            })
        })
        .collect::<Vec<_>>();

        let total_connections = aggregate_row.try_get::<i64, _>("total_connections")?;
        let current_database_connections =
            aggregate_row.try_get::<i64, _>("current_database_connections")?;
        let database_count = aggregate_row.try_get::<i64, _>("database_count")?;
        let waiting_connections = activity_row
            .as_ref()
            .and_then(|row| row.try_get::<i64, _>("waiting_connections").ok())
            .unwrap_or(0);
        let idle_in_transaction = activity_row
            .as_ref()
            .and_then(|row| row.try_get::<i64, _>("idle_in_transaction").ok())
            .unwrap_or(0);
        let blks_read = current_database_row.try_get::<i64, _>("blks_read")?;
        let blks_hit = current_database_row.try_get::<i64, _>("blks_hit")?;
        let block_requests = blks_read.saturating_add(blks_hit);
        let cache_hit_ratio = if block_requests > 0 {
            blks_hit as f64 / block_requests as f64
        } else {
            1.0
        };
        let connection_utilization = if max_connections > 0 {
            total_connections as f64 / max_connections as f64
        } else {
            0.0
        };

        Result::<Value>::Ok(json!({
            "database": {
                "current_database": current_database,
                "connection_source": connection_source,
                "max_connections": max_connections,
                "total_connections": total_connections,
                "current_database_connections": current_database_connections,
                "database_count": database_count,
                "connection_utilization": connection_utilization,
                "waiting_connections": waiting_connections,
                "idle_in_transaction": idle_in_transaction,
                "numbackends": current_database_row.try_get::<i64, _>("numbackends")?,
                "xact_commit": current_database_row.try_get::<i64, _>("xact_commit")?,
                "xact_rollback": current_database_row.try_get::<i64, _>("xact_rollback")?,
                "deadlocks": current_database_row.try_get::<i64, _>("deadlocks")?,
                "temp_files": current_database_row.try_get::<i64, _>("temp_files")?,
                "size_bytes": current_database_row.try_get::<i64, _>("size_bytes")?,
                "cache_hit_ratio": cache_hit_ratio,
                "top_databases": top_databases,
            }
        }))
    })
    .await
    .map_err(|_| anyhow!("timeout querying postgres after {}s", timeout_seconds))??;

    let database = metrics
        .get("database")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let connection_utilization = database
        .get("connection_utilization")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let waiting_connections = database
        .get("waiting_connections")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let idle_in_transaction = database
        .get("idle_in_transaction")
        .and_then(Value::as_i64)
        .unwrap_or(0);

    Ok(ProbeResult {
        endpoint: None,
        summary: format!(
            "Postgres queried with {:.0}% connection utilisation and {} waiting session(s)",
            connection_utilization * 100.0,
            waiting_connections
        ),
        metrics,
        health: if connection_utilization >= 0.9
            || waiting_connections > 0
            || idle_in_transaction >= 4
        {
            ServiceHealth::Degraded
        } else {
            ServiceHealth::Healthy
        },
    })
}

async fn probe_shared_storage(
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let mount_path =
        resolve_shared_storage_mount_path(&config.integrations.shared_storage, service)
            .ok_or_else(|| anyhow!("not_found: no shared storage mount path available"))?;
    let metadata = fs::metadata(&mount_path).map_err(|error| {
        anyhow!(
            "not_found: shared storage path {} is not accessible: {}",
            mount_path.display(),
            error
        )
    })?;
    if !metadata.is_dir() {
        return Err(anyhow!(
            "not_found: shared storage path {} is not a directory",
            mount_path.display()
        ));
    }

    let filesystem = stat_filesystem(&mount_path)?;
    let expected_subdirectories = shared_storage_expected_subdirectories(service);
    let subdirectories = expected_subdirectories
        .iter()
        .map(|name| {
            let path = mount_path.join(name);
            let exists = path.is_dir();
            json!({
                "name": name,
                "path": path.display().to_string(),
                "exists": exists,
            })
        })
        .collect::<Vec<_>>();
    let missing_subdirectories = subdirectories
        .iter()
        .filter(|entry| entry.get("exists").and_then(Value::as_bool) != Some(true))
        .filter_map(|entry| {
            entry
                .get("name")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .collect::<Vec<_>>();
    let mount_info = mount_info_for_path(&mount_path);
    let usage_ratio = if filesystem.bytes_total > 0 {
        1.0 - (filesystem.bytes_available as f64 / filesystem.bytes_total as f64)
    } else {
        0.0
    };
    let inode_usage_ratio = if filesystem.inode_total > 0 {
        1.0 - (filesystem.inode_available as f64 / filesystem.inode_total as f64)
    } else {
        0.0
    };
    let expected_subdirectories_len = expected_subdirectories.len();
    let missing_subdirectories_len = missing_subdirectories.len();

    Ok(ProbeResult {
        endpoint: None,
        summary: format!(
            "Shared storage {} inspected at {:.0}% usage",
            mount_path.display(),
            usage_ratio * 100.0
        ),
        metrics: json!({
            "mount_path": mount_path.display().to_string(),
            "filesystem": {
                "bytes_total": filesystem.bytes_total,
                "bytes_free": filesystem.bytes_free,
                "bytes_available": filesystem.bytes_available,
                "usage_ratio": usage_ratio,
                "inode_total": filesystem.inode_total,
                "inode_free": filesystem.inode_free,
                "inode_available": filesystem.inode_available,
                "inode_usage_ratio": inode_usage_ratio,
                "read_only": filesystem.read_only,
                "mount_source": mount_info.as_ref().map(|info| info.source.clone()),
                "mount_target": mount_info.as_ref().map(|info| info.target.clone()),
                "fs_type": mount_info.as_ref().map(|info| info.fs_type.clone()),
                "mount_options": mount_info
                    .as_ref()
                    .map(|info| info.options.clone())
                    .unwrap_or_default(),
            },
            "expected_subdirectories": expected_subdirectories,
            "subdirectories": subdirectories,
            "missing_subdirectories": missing_subdirectories,
        }),
        health: if filesystem.read_only
            || usage_ratio >= 0.95
            || inode_usage_ratio >= 0.95
            || (expected_subdirectories_len > 0
                && expected_subdirectories_len == missing_subdirectories_len)
        {
            ServiceHealth::Degraded
        } else {
            ServiceHealth::Healthy
        },
    })
}

async fn probe_ollama(
    client: &Client,
    config: &ConductorConfig,
    service: &ServiceSnapshot,
) -> Result<ProbeResult> {
    let external = resolve_http_external_config(config, service.service_key.as_str());
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
    let mut last_error = None;
    for base_url in base_url_candidates(service, external) {
        let probe = match get_json_with_auth(client, &base_url, "/healthz", external).await {
            Ok(value) => Ok((value, "/healthz")),
            Err(error) => match get_json_with_auth(client, &base_url, "/health", external).await {
                Ok(value) => Ok((value, "/health")),
                Err(_) => {
                    match get_json_with_auth(client, &base_url, "/api/tags", external).await {
                        Ok(value) => Ok((value, "/api/tags")),
                        Err(_) => Err(error),
                    }
                }
            },
        };

        match probe {
            Ok((health, path_used)) => {
                return Ok(ProbeResult {
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
                });
            }
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no base URL available")))
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

fn summarize_prometheus_targets(
    data: &Value,
) -> (BTreeMap<String, PrometheusJobSummary>, usize, usize, usize) {
    let active_targets = data
        .get("activeTargets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let dropped_targets_total = data
        .get("droppedTargets")
        .and_then(Value::as_array)
        .map(|items| items.len())
        .unwrap_or(0);
    let mut jobs = BTreeMap::new();
    let mut down_targets_total = 0usize;

    for target in &active_targets {
        let labels = target.get("labels").and_then(Value::as_object);
        let discovered_labels = target.get("discoveredLabels").and_then(Value::as_object);
        let job_name = labels
            .and_then(|labels| labels.get("job"))
            .and_then(Value::as_str)
            .or_else(|| {
                discovered_labels
                    .and_then(|labels| labels.get("job"))
                    .and_then(Value::as_str)
            })
            .or_else(|| target.get("scrapePool").and_then(Value::as_str))
            .unwrap_or("unknown")
            .trim()
            .to_string();

        let summary = jobs
            .entry(job_name.clone())
            .or_insert_with(|| PrometheusJobSummary {
                service_key: infer_service_key_from_observability_name(&job_name),
                ..PrometheusJobSummary::default()
            });
        summary.total_targets += 1;
        match target.get("health").and_then(Value::as_str) {
            Some("up") => summary.healthy_targets += 1,
            _ => {
                summary.down_targets += 1;
                down_targets_total += 1;
            }
        }
        if let Some(last_error) = target
            .get("lastError")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            summary.last_errors.insert(last_error.to_string());
        }
    }

    (
        jobs,
        active_targets.len(),
        dropped_targets_total,
        down_targets_total,
    )
}

fn infer_service_key_from_observability_name(name: &str) -> Option<String> {
    let lowered = name.to_ascii_lowercase();
    for (pattern, service_key) in [
        ("tracey", "tracey"),
        ("continuum", "continuum"),
        ("conductor", "conductor"),
        ("gail", "gail"),
        ("refiner", "refiner"),
        ("aarnn", "aarnn"),
        ("ollama", "ollama"),
        ("customers", "customers"),
        ("billing", "billing"),
        ("nmchain", "nmchain"),
        ("postgres", "postgres"),
        ("prometheus", "prometheus"),
        ("grafana", "grafana"),
    ] {
        if lowered.contains(pattern) {
            return Some(service_key.to_string());
        }
    }
    None
}

fn resolve_postgres_connection_string(
    config: &ConductorConfig,
    integration: &PostgresIntegrationConfig,
) -> Option<(String, &'static str)> {
    if let Some(connection_string) = integration
        .connection_string
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return Some((connection_string.to_string(), "configured"));
    }
    if !config.database.url.trim().is_empty() {
        return Some((config.database.url.clone(), "conductor_database_fallback"));
    }
    None
}

fn resolve_shared_storage_mount_path(
    integration: &SharedStorageIntegrationConfig,
    service: &ServiceSnapshot,
) -> Option<PathBuf> {
    if let Some(path) = integration.mount_path.clone() {
        return Some(path);
    }

    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();

    for value in [
        service
            .raw_defaults
            .get("qc01_shared_storage_root")
            .and_then(Value::as_str),
        service
            .raw_defaults
            .get("rk1_shared_storage_nfs_path")
            .and_then(Value::as_str),
        service
            .raw_defaults
            .get("rk1_shared_build_storage_mount_path")
            .and_then(Value::as_str),
    ] {
        let Some(value) = value else {
            continue;
        };
        let path = PathBuf::from(value.trim());
        if !path.as_os_str().is_empty() && seen.insert(path.display().to_string()) {
            candidates.push(path);
        }
    }

    for path in service
        .storage_paths
        .iter()
        .filter(|path| path.contains("shared"))
        .chain(service.storage_paths.iter())
    {
        let candidate = PathBuf::from(path.trim());
        if !candidate.as_os_str().is_empty() && seen.insert(candidate.display().to_string()) {
            candidates.push(candidate);
        }
    }

    let fallback = PathBuf::from("/home/continuum-shared-storage");
    if seen.insert(fallback.display().to_string()) {
        candidates.push(fallback);
    }

    candidates.into_iter().next()
}

fn shared_storage_expected_subdirectories(service: &ServiceSnapshot) -> Vec<String> {
    let mut seen = BTreeSet::new();
    service
        .raw_defaults
        .get("qc01_shared_storage_subdirs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| seen.insert((*value).to_string()))
        .map(ToString::to_string)
        .collect()
}

fn stat_filesystem(path: &Path) -> Result<FilesystemProbeStats> {
    let raw_path = CString::new(path.as_os_str().as_bytes())?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(raw_path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return Err(anyhow!(
            "statvfs failed for {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        ));
    }
    let stats = unsafe { stats.assume_init() };
    let block_size = if stats.f_frsize > 0 {
        stats.f_frsize as u64
    } else {
        stats.f_bsize as u64
    };

    Ok(FilesystemProbeStats {
        bytes_total: (stats.f_blocks as u64).saturating_mul(block_size),
        bytes_free: (stats.f_bfree as u64).saturating_mul(block_size),
        bytes_available: (stats.f_bavail as u64).saturating_mul(block_size),
        inode_total: stats.f_files as u64,
        inode_free: stats.f_ffree as u64,
        inode_available: stats.f_favail as u64,
        read_only: (stats.f_flag & (libc::ST_RDONLY as libc::c_ulong)) != 0,
    })
}

fn mount_info_for_path(path: &Path) -> Option<MountInfo> {
    let path = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let path = path.to_string_lossy().to_string();
    let mounts = fs::read_to_string("/proc/mounts").ok()?;
    let mut best_match: Option<MountInfo> = None;

    for line in mounts.lines() {
        let mut parts = line.split_whitespace();
        let source = unescape_mount_field(parts.next()?);
        let target = unescape_mount_field(parts.next()?);
        let fs_type = parts.next()?.to_string();
        let options = parts
            .next()
            .unwrap_or_default()
            .split(',')
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        if !(path == target || path.starts_with(&(target.clone() + "/"))) {
            continue;
        }

        if best_match
            .as_ref()
            .is_none_or(|current| target.len() > current.target.len())
        {
            best_match = Some(MountInfo {
                source,
                target,
                fs_type,
                options,
            });
        }
    }

    best_match
}

fn unescape_mount_field(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
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
