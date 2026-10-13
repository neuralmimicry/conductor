//! Conductor owns immutable release qualification and asks Continuum to operate it.
use crate::{
    config::ConductorConfig,
    integrations::{build_http_client, continuum::ContinuumClient, github_repository_coordinate},
    models::{DeliveryStage, ExecutionStatus, ServiceSnapshot, WorkExecution, WorkItem},
    repository::ConductorRepository,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleasePolicy {
    pub workflow: String,
    pub required_jobs: BTreeMap<String, Vec<String>>,
    pub uat: DeploymentTarget,
    pub production: DeploymentTarget,
    pub acceptance: Acceptance,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseCandidate {
    pub repository: String,
    pub source_sha: String,
    pub workflow: String,
    pub run_id: u64,
    pub run_attempt: u64,
    /// Each group must name every required job for the applicable platform.
    pub required_jobs: BTreeMap<String, Vec<String>>,
    pub release_tag: String,
    pub asset_name: String,
    pub asset_sha256: String,
    pub image: String,
    pub uat: DeploymentTarget,
    pub production: DeploymentTarget,
    pub acceptance: Acceptance,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeploymentTarget {
    pub service_key: String,
    pub cluster_id: String,
    pub namespace: String,
    pub deployment: String,
    pub container: String,
    pub expected_image: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Acceptance {
    /// Named functional/authentication/state-safety checks from the independently fetched probe.
    pub required_checks: Vec<String>,
    /// Pointer into Conductor's independently fetched functional/health probe.
    pub metric_pointer: String,
    pub lower_is_better: bool,
    pub minimum_improvement_pct: f64,
    pub maximum_regression_pct: f64,
}
fn digest(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 120
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        && value != "."
        && value != ".."
}
fn immutable_image(image: &str, owner: &str) -> bool {
    image
        .strip_prefix(&format!("ghcr.io/{owner}/"))
        .and_then(|rest| rest.split_once("@sha256:"))
        .is_some_and(|(repository, hash)| name(repository) && digest(hash, 64))
}
impl ReleaseCandidate {
    pub fn validate(&self, config: &ConductorConfig, item: &WorkItem) -> Result<()> {
        let policy = config
            .delivery
            .qualified_releases
            .get(&self.repository)
            .context("no operator-owned qualified release policy for this repository")?;
        ensure!(
            policy.workflow == self.workflow
                && policy.required_jobs == self.required_jobs
                && policy.uat == self.uat
                && policy.production == self.production
                && policy.acceptance == self.acceptance,
            "candidate changed the trusted workflow, required jobs, targets or acceptance contract"
        );
        let owner = config.discovery.github.owner.trim();
        ensure!(
            !owner.is_empty() && name(owner),
            "release owner policy is missing"
        );
        let (repo_owner, repo_name) = self
            .repository
            .split_once('/')
            .context("release repository must be owner/name")?;
        ensure!(
            repo_owner == owner && name(repo_name),
            "release repository is outside the approved owner"
        );
        ensure!(
            digest(&self.source_sha, 40) && self.run_id > 0 && self.run_attempt > 0,
            "exact source/run/attempt required"
        );
        ensure!(
            name(&self.workflow)
                && (self.workflow.ends_with(".yml") || self.workflow.ends_with(".yaml")),
            "invalid trusted workflow name"
        );
        for group in ["validation", "build", "package"] {
            let jobs = self
                .required_jobs
                .get(group)
                .context("validation, build and package jobs must be specified")?;
            ensure!(
                !jobs.is_empty()
                    && jobs.len() <= 40
                    && jobs.iter().all(|j| !j.trim().is_empty() && j.len() <= 200),
                "invalid required job group"
            );
        }
        ensure!(self.required_jobs.len() == 3, "unknown required job group");
        ensure!(
            name(&self.release_tag) && name(&self.asset_name) && digest(&self.asset_sha256, 64),
            "release and package checksum required"
        );
        ensure!(
            immutable_image(&self.image, owner),
            "qualified image must be digest pinned"
        );
        for t in [&self.uat, &self.production] {
            ensure!(
                [
                    &t.service_key,
                    &t.cluster_id,
                    &t.namespace,
                    &t.deployment,
                    &t.container
                ]
                .iter()
                .all(|s| name(s)),
                "invalid delivery target"
            );
            ensure!(
                immutable_image(&t.expected_image, owner),
                "a verified immutable rollback image is required"
            );
        }
        ensure!(
            (
                &self.uat.cluster_id,
                &self.uat.namespace,
                &self.uat.deployment
            ) != (
                &self.production.cluster_id,
                &self.production.namespace,
                &self.production.deployment
            ),
            "UAT must be separate from production"
        );
        ensure!(
            item.target_service.as_deref() == Some(self.production.service_key.as_str()),
            "production target differs from the work item"
        );
        let a = &self.acceptance;
        ensure!(
            !a.required_checks.is_empty()
                && a.required_checks.len() <= 40
                && a.required_checks.iter().all(|c| name(c)),
            "required functional acceptance checks are missing"
        );
        ensure!(
            a.metric_pointer.starts_with('/') && a.metric_pointer.len() <= 256,
            "an independently measured benefit is required"
        );
        ensure!(
            a.minimum_improvement_pct.is_finite()
                && a.minimum_improvement_pct > 0.0
                && a.maximum_regression_pct.is_finite()
                && (0.0..=100.0).contains(&a.maximum_regression_pct),
            "invalid benefit/regression budget"
        );
        Ok(())
    }
    pub fn identity(&self) -> Result<String> {
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}

/// Reject a successful run for another source, another attempt, or another workflow.
pub fn verify_run(candidate: &ReleaseCandidate, run: &Value, jobs: &Value) -> Result<()> {
    ensure!(
        run["id"].as_u64() == Some(candidate.run_id)
            && run["run_attempt"].as_u64() == Some(candidate.run_attempt),
        "workflow run/attempt changed"
    );
    ensure!(
        run["head_sha"].as_str() == Some(&candidate.source_sha)
            && run["status"] == "completed"
            && run["conclusion"] == "success",
        "exact-source workflow has not succeeded"
    );
    ensure!(
        run["path"].as_str().and_then(|p| p.split('@').next())
            == Some(format!(".github/workflows/{}", candidate.workflow).as_str()),
        "workflow identity differs"
    );
    let rows = jobs["jobs"]
        .as_array()
        .context("required job observations missing")?;
    ensure!(
        jobs["total_count"].as_u64() == Some(rows.len() as u64) && rows.len() <= 100,
        "job list is truncated"
    );
    ensure!(
        rows.iter()
            .all(|j| j["run_id"].as_u64() == Some(candidate.run_id)
                && j["head_sha"].as_str() == Some(&candidate.source_sha)),
        "job source/run mismatch"
    );
    ensure!(
        !rows.iter().any(|j| matches!(
            j["conclusion"].as_str(),
            Some("failure" | "cancelled" | "timed_out" | "action_required")
        )),
        "release includes a failed job"
    );
    for job in candidate.required_jobs.values().flatten() {
        let matching: Vec<_> = rows
            .iter()
            .filter(|j| j["name"].as_str() == Some(job))
            .collect();
        ensure!(
            matching.len() == 1
                && matching[0]["status"] == "completed"
                && matching[0]["conclusion"] == "success",
            "required job {job} is missing, ambiguous, skipped or unsuccessful"
        );
    }
    Ok(())
}
pub fn verify_package(candidate: &ReleaseCandidate, release: &Value, commit: &Value) -> Result<()> {
    ensure!(
        commit["sha"].as_str() == Some(&candidate.source_sha),
        "release tag resolves to another source"
    );
    ensure!(
        release["tag_name"].as_str() == Some(&candidate.release_tag) && release["draft"] == false,
        "release is unavailable or draft"
    );
    let assets = release["assets"]
        .as_array()
        .context("release assets missing")?;
    let assets: Vec<_> = assets
        .iter()
        .filter(|a| a["name"].as_str() == Some(&candidate.asset_name))
        .collect();
    ensure!(
        assets.len() == 1
            && assets[0]["state"] == "uploaded"
            && assets[0]["size"].as_u64().is_some_and(|n| n > 0)
            && assets[0]["digest"].as_str()
                == Some(format!("sha256:{}", candidate.asset_sha256).as_str()),
        "published package digest/state differs"
    );
    Ok(())
}
/// Additional policy applied only to cryptographically verified gh output.
pub fn verify_attestation(candidate: &ReleaseCandidate, verified: &Value) -> Result<()> {
    let digest = candidate
        .image
        .split_once("@sha256:")
        .context("image digest missing")?
        .1;
    let invocation = format!(
        "https://github.com/{}/actions/runs/{}/attempts/{}",
        candidate.repository, candidate.run_id, candidate.run_attempt
    );
    ensure!(
        verified
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| {
                let statement = &row["verificationResult"]["statement"];
                statement["predicateType"] == "https://slsa.dev/provenance/v1"
                    && statement["predicate"]["runDetails"]["metadata"]["invocationId"]
                        == invocation
                    && statement["subject"].as_array().is_some_and(|subjects| {
                        subjects.iter().any(|s| s["digest"]["sha256"] == digest)
                    })
            })),
        "verified image provenance does not bind this exact build/run/attempt"
    );
    Ok(())
}
async fn attest_image(
    config: &ConductorConfig,
    candidate: &ReleaseCandidate,
    repo: &dyn ConductorRepository,
    execution: &mut WorkExecution,
) -> Result<Value> {
    use std::process::Stdio;
    use tokio::{io::AsyncReadExt, process::Command};
    let signer = format!(
        "{}/.github/workflows/{}",
        candidate.repository, candidate.workflow
    );
    let mut command = Command::new("/usr/bin/gh");
    command
        .args([
            "attestation",
            "verify",
            &format!("oci://{}", candidate.image),
            "--repo",
            &candidate.repository,
            "--signer-workflow",
            &signer,
            "--source-digest",
            &candidate.source_sha,
            "--format",
            "json",
            "--limit",
            "30",
        ])
        .env_remove("GH_TOKEN")
        .env_remove("GITHUB_TOKEN")
        .env("GH_PROMPT_DISABLED", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Some(token) = &config.discovery.github.token {
        command.env("GH_TOKEN", token);
    }
    let mut child = command
        .spawn()
        .context("trusted gh attestation verifier unavailable; qualification blocked")?;
    let stdout = child
        .stdout
        .take()
        .context("attestation output unavailable")?;
    let verification = async {
        let mut bytes = Vec::new();
        stdout
            .take(2 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .await?;
        ensure!(
            bytes.len() <= 2 * 1024 * 1024,
            "attestation verification output exceeds bound"
        );
        ensure!(
            child.wait().await?.success(),
            "image attestation signature/source/signer verification failed"
        );
        let verified: Value = serde_json::from_slice(&bytes)?;
        verify_attestation(candidate, &verified)?;
        Ok::<Value, anyhow::Error>(verified)
    };
    tokio::pin!(verification);
    let deadline = tokio::time::sleep(Duration::from_secs(120));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            result = &mut verification => return result,
            _ = &mut deadline => bail!("attestation verification deadline exceeded"),
            _ = tokio::time::sleep(Duration::from_secs(5)) => heartbeat(repo, execution).await?,
        }
    }
}
fn payload(mut value: Value) -> Result<Value> {
    for _ in 0..8 {
        if value.get("success").is_none() {
            ensure!(value.is_object(), "Continuum returned a non-object payload");
            if let Some(status) = value.get("upstream_status") {
                ensure!(
                    status.as_u64().is_some_and(|s| (200..300).contains(&s)),
                    "Continuum upstream operation failed"
                );
            }
            return Ok(value);
        }
        ensure!(value["success"] == true, "Continuum rejected the operation");
        value = value
            .get("data")
            .cloned()
            .context("Continuum response has no data")?;
    }
    bail!("Continuum response nesting exceeds limit")
}
fn measured(probe: &Value, acceptance: &Acceptance) -> Result<f64> {
    ensure!(probe["passed"] == true, "functional health probe failed");
    let at = probe["metrics"]["delivery_acceptance"]["observed_at_unix_ms"]
        .as_i64()
        .context("fresh functional acceptance observations missing")?;
    let age = crate::models::now_utc().timestamp_millis() - at;
    ensure!(
        (0..=60_000).contains(&age),
        "functional acceptance observation is stale or from the future"
    );
    let checks = probe["metrics"]["delivery_acceptance"]["checks"]
        .as_object()
        .context("required functional acceptance results missing")?;
    for check in &acceptance.required_checks {
        ensure!(
            checks.get(check) == Some(&json!(true)),
            "required functional check {check} failed or missing"
        );
    }
    let metric = probe
        .pointer(&acceptance.metric_pointer)
        .and_then(Value::as_f64)
        .context("benefit observation missing")?;
    ensure!(
        metric.is_finite() && metric > 0.0,
        "invalid benefit observation"
    );
    Ok(metric)
}
pub fn improvement(
    baseline: f64,
    current: f64,
    acceptance: &Acceptance,
    production: bool,
) -> Result<f64> {
    ensure!(
        baseline.is_finite() && baseline > 0.0 && current.is_finite() && current > 0.0,
        "invalid baseline comparison"
    );
    let change = if acceptance.lower_is_better {
        (baseline - current) / baseline * 100.0
    } else {
        (current - baseline) / baseline * 100.0
    };
    ensure!(
        change >= -acceptance.maximum_regression_pct,
        "candidate degraded beyond the regression budget"
    );
    if production {
        ensure!(
            change >= acceptance.minimum_improvement_pct,
            "measured benefit threshold not met"
        );
    }
    Ok(change)
}
fn running(status: &Value, target: &DeploymentTarget, image: &str, request: Option<&str>) -> bool {
    status["namespace"] == target.namespace
        && status["name"] == target.deployment
        && status["rollout_complete"] == true
        && status["ready_replicas"].as_u64().is_some_and(|n| n > 0)
        && status["ready_replicas"] == status["desired_replicas"]
        && request.is_none_or(|id| status["rollout_request_id"].as_str() == Some(id))
        && status["containers"].as_array().is_some_and(|rows| {
            rows.iter()
                .any(|c| c["name"] == target.container && c["image"] == image && c["ready"] == true)
        })
        && status["pods"].as_array().is_some_and(|pods| {
            !pods.is_empty()
                && pods.iter().all(|p| {
                    p["conditions"].as_array().is_some_and(|cs| {
                        cs.iter()
                            .any(|c| c["type"] == "Ready" && c["status"] == "True")
                    }) && p["containers"].as_array().is_some_and(|cs| {
                        cs.iter().any(|c| {
                            c["name"] == target.container
                                && c["image_id"] == image
                                && c["ready"] == true
                        })
                    })
                })
        })
}
async fn github(config: &ConductorConfig, repository: &str, path: &str) -> Result<Value> {
    let client = build_http_client(config.discovery.github.timeout_seconds)?;
    let mut request = client
        .get(format!(
            "{}/repos/{repository}/{path}",
            config.discovery.github.api_base_url.trim_end_matches('/')
        ))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "conductor-qualified-release");
    if let Some(token) = &config.discovery.github.token {
        request = request.bearer_auth(token);
    }
    let mut response = request.send().await?.error_for_status()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= 2 * 1024 * 1024,
            "GitHub observation exceeds bound"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
async fn heartbeat(repo: &dyn ConductorRepository, execution: &mut WorkExecution) -> Result<()> {
    execution.updated_at = crate::models::now_utc();
    repo.upsert_work_execution(execution).await
}
async fn status(client: &ContinuumClient, target: &DeploymentTarget) -> Result<Value> {
    payload(
        client
            .deployment_status(&target.cluster_id, &target.namespace, &target.deployment)
            .await?,
    )
}
async fn wait_ready(
    client: &ContinuumClient,
    config: &ConductorConfig,
    target: &DeploymentTarget,
    image: &str,
    request: &str,
    repo: &dyn ConductorRepository,
    execution: &mut WorkExecution,
) -> Result<Value> {
    let start = Instant::now();
    loop {
        heartbeat(repo, execution).await?;
        let observed = status(client, target).await?;
        if running(&observed, target, image, Some(request)) {
            return Ok(observed);
        }
        ensure!(
            start.elapsed().as_secs() < config.recovery.rollout_timeout_seconds,
            "qualified rollout readiness deadline exceeded"
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

pub async fn execute_release_stage(
    repo: &dyn ConductorRepository,
    config: &ConductorConfig,
    item: &WorkItem,
    execution: &mut WorkExecution,
    services: &[ServiceSnapshot],
) -> Result<Value> {
    let candidate: ReleaseCandidate = serde_json::from_value(
        item.plan
            .get("release_candidate")
            .cloned()
            .context("release stages require an exact qualified release_candidate manifest")?,
    )?;
    candidate.validate(config, item)?;
    let production = item.delivery_stage == DeliveryStage::Production;
    let identity = candidate.identity()?;
    if production {
        let history = repo.list_work_executions_for_item(item.id, 100).await?;
        let uat = history
            .iter()
            .filter(|e| e.delivery_stage == DeliveryStage::Uat)
            .max_by_key(|e| e.updated_at)
            .context("production has no independently verified UAT execution")?;
        ensure!(
            uat.status == ExecutionStatus::Success
                && uat.verification["candidate_id"] == identity
                && uat.verification["passed"] == true,
            "latest UAT is failed, incomplete or belongs to another candidate"
        );
        ensure!(
            (crate::models::now_utc() - uat.updated_at).num_seconds()
                <= config.execution.job_timeout_seconds as i64
                && uat.updated_at <= crate::models::now_utc(),
            "UAT evidence expired"
        );
    }
    let repository_info = github(config, &candidate.repository, "").await?;
    let default_branch = repository_info["default_branch"]
        .as_str()
        .context("repository default branch unavailable")?;
    ensure!(name(default_branch), "unsupported default branch identity");
    let default_head = github(
        config,
        &candidate.repository,
        &format!("commits/{default_branch}"),
    )
    .await?;
    ensure!(
        default_head["sha"] == candidate.source_sha,
        "candidate must be qualified on the current configured default branch source"
    );
    let run = github(
        config,
        &candidate.repository,
        &format!("actions/runs/{}", candidate.run_id),
    )
    .await?;
    let jobs = github(
        config,
        &candidate.repository,
        &format!(
            "actions/runs/{}/attempts/{}/jobs?per_page=100",
            candidate.run_id, candidate.run_attempt
        ),
    )
    .await?;
    verify_run(&candidate, &run, &jobs)?;
    let release = github(
        config,
        &candidate.repository,
        &format!("releases/tags/{}", candidate.release_tag),
    )
    .await?;
    let commit = github(
        config,
        &candidate.repository,
        &format!("commits/{}", candidate.release_tag),
    )
    .await?;
    verify_package(&candidate, &release, &commit)?;
    let provenance = attest_image(config, &candidate, repo, execution).await?;
    let target = if production {
        &candidate.production
    } else {
        &candidate.uat
    };
    let service = services
        .iter()
        .find(|s| s.service_key == target.service_key)
        .context("qualified target is absent from discovery")?;
    ensure!(
        service.namespace.as_deref() == Some(target.namespace.as_str()),
        "discovered target namespace differs"
    );
    ensure!(
        github_repository_coordinate(service.repo_url.as_deref().unwrap_or_default())
            .map(|(o, r)| format!("{o}/{r}"))
            == Some(candidate.repository.clone()),
        "discovered service repository differs"
    );
    ensure!(
        production || service.deployment_environment == Some(DeliveryStage::Uat),
        "validation target is not independently identified as UAT"
    );
    ensure!(
        (crate::models::now_utc() - service.updated_at).num_seconds()
            <= config.recovery.max_snapshot_age_seconds as i64,
        "target discovery is stale"
    );
    let control = ContinuumClient::from_sources(
        &config.integrations.continuum,
        services.iter().find(|s| s.service_key == "continuum"),
    )
    .await?
    .context("Continuum control is unavailable")?;
    let initial = status(&control, target).await?;
    ensure!(
        running(&initial, target, &target.expected_image, None),
        "known-good target readiness/digest is not verified"
    );
    ensure!(
        production || initial["validation_isolation_verified"] == true,
        "Continuum has no independently verified isolated UAT state/credential/network adapter for this target"
    );
    ensure!(
        !production || initial["zero_unavailable_rollout_ready"] == true,
        "Continuum cannot prove a supported zero-unavailability production transition"
    );
    let mut baseline_samples = Vec::new();
    for _ in 0..3 {
        heartbeat(repo, execution).await?;
        let probe = crate::executor::capture_safety_probe(config, Some(service)).await;
        measured(&probe, &candidate.acceptance)?;
        baseline_samples.push(probe);
        tokio::time::sleep(Duration::from_secs(
            config.safety.health_check_interval_seconds.max(1),
        ))
        .await;
    }
    let baseline_values = baseline_samples
        .iter()
        .map(|p| measured(p, &candidate.acceptance))
        .collect::<Result<Vec<_>>>()?;
    let baseline = baseline_values.iter().sum::<f64>() / baseline_values.len() as f64;
    let baseline_probe = json!({"samples": baseline_samples, "mean": baseline});
    let request = format!("sdlc-{}-{}", execution.id, item.delivery_stage.as_str());
    let change = format!("sdlc-{}", item.id);
    execution.mark_status(ExecutionStatus::Verifying);
    execution.request_payload = json!({"candidate_id": identity, "release_candidate": candidate, "request_id": request, "change_id": change, "baseline": baseline_probe});
    heartbeat(repo, execution).await?;
    // Persist intent before the mutation. Lost replies are reconciled by the same
    // request ID; never rebuild the package or blindly resubmit a mutation.
    let submitted = control
        .rollout_image(
            target,
            &candidate.image,
            &target.expected_image,
            &request,
            &change,
            production,
        )
        .await;
    execution.latest_payload = json!({"candidate_id":identity,"phase":"rollout_submitted",
        "request_id":request,"reply_received":submitted.is_ok(),"reconciliation_required":true});
    heartbeat(repo, execution).await?;
    let after = status(&control, target).await.context("rollout intent is durable; status unavailable, reconcile the recorded request before any further action")?;
    if submitted.is_err()
        && !(after["rollout_request_id"] == request
            && after["containers"].as_array().is_some_and(|cs| {
                cs.iter()
                    .any(|c| c["name"] == target.container && c["image"] == candidate.image)
            }))
    {
        return Err(submitted
            .err()
            .context("rollout response is ambiguous and intent cannot be reconciled")?);
    }
    if let Ok(response) = submitted {
        payload(response)?;
    }
    let outcome: Result<Value> = async {
        let deployed = wait_ready(&control, config, target, &candidate.image, &request, repo, execution).await?;
        let start = Instant::now();
        let mut samples = Vec::new();
        let mut values = Vec::new();
        loop {
            heartbeat(repo, execution).await?;
            ensure!(running(&status(&control, target).await?, target, &candidate.image, Some(&request)), "runtime identity or readiness changed during validation");
            let probe = crate::executor::capture_safety_probe(config, Some(service)).await;
            let current = measured(&probe, &candidate.acceptance)?;
            improvement(baseline, current, &candidate.acceptance, false)?;
            values.push(current);
            samples.push(probe);
            if samples.len() >= 3 && start.elapsed().as_secs() >= config.safety.health_check_window_seconds { break; }
            tokio::time::sleep(Duration::from_secs(config.safety.health_check_interval_seconds.max(1))).await;
        }
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let benefit = improvement(baseline, mean, &candidate.acceptance, true)?;
        Ok(json!({"passed":true,"candidate_id":identity,"source_sha":candidate.source_sha,"run_id":candidate.run_id,"run_attempt":candidate.run_attempt,"provenance":provenance,"asset_sha256":candidate.asset_sha256,"image":candidate.image,"stage":item.delivery_stage.as_str(),"request_id":request,"change_id":change,"deployed":deployed,"baseline":baseline_probe,"samples":samples,"improvement_pct":benefit,"observed_at":crate::models::now_utc()}))
    }.await;
    match outcome {
        Ok(report) => Ok(report),
        Err(error) => {
            let rollback_request = format!("{request}-rollback");
            let current = status(&control, target).await?;
            ensure!(
                current["rollout_request_id"] == request,
                "release failed and another operation owns the target; rollback requires reconciliation: {error}"
            );
            control
                .rollout_image(
                    target,
                    &target.expected_image,
                    &candidate.image,
                    &rollback_request,
                    &change,
                    production,
                )
                .await
                .and_then(payload)?;
            wait_ready(
                &control,
                config,
                target,
                &target.expected_image,
                &rollback_request,
                repo,
                execution,
            )
            .await?;
            let recovered = crate::executor::capture_safety_probe(config, Some(service)).await;
            improvement(
                baseline,
                measured(&recovered, &candidate.acceptance)?,
                &candidate.acceptance,
                false,
            )?;
            execution.latest_payload = json!({"candidate_id":identity,"failed":true,"error":error.to_string(),"rollback":{"request_id":rollback_request,"recovered":true,"probe":recovered}});
            heartbeat(repo, execution).await?;
            bail!(
                "release rejected; Continuum restored and verified the previous artifact: {error}"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn candidate() -> ReleaseCandidate {
        let target = |key: &str| DeploymentTarget {
            service_key: key.into(),
            cluster_id: "rk1".into(),
            namespace: key.into(),
            deployment: "app".into(),
            container: "app".into(),
            expected_image: format!("ghcr.io/neuralmimicry/app@sha256:{}", "b".repeat(64)),
        };
        ReleaseCandidate {
            repository: "neuralmimicry/app".into(),
            source_sha: "a".repeat(40),
            workflow: "release.yml".into(),
            run_id: 42,
            run_attempt: 2,
            required_jobs: BTreeMap::from([
                ("validation".into(), vec!["Test".into()]),
                ("build".into(), vec!["Build ARM64".into()]),
                ("package".into(), vec!["Publish".into()]),
            ]),
            release_tag: "v1.2.3".into(),
            asset_name: "app-arm64.deb".into(),
            asset_sha256: "c".repeat(64),
            image: format!("ghcr.io/neuralmimicry/app@sha256:{}", "d".repeat(64)),
            uat: target("app-uat"),
            production: target("app"),
            acceptance: Acceptance {
                required_checks: vec!["authenticated_read".into(), "state_preserved".into()],
                metric_pointer: "/metric".into(),
                lower_is_better: true,
                minimum_improvement_pct: 5.0,
                maximum_regression_pct: 0.0,
            },
        }
    }
    fn run(c: &ReleaseCandidate) -> Value {
        json!({"id":c.run_id,"run_attempt":c.run_attempt,"head_sha":c.source_sha,"path":".github/workflows/release.yml","status":"completed","conclusion":"success"})
    }
    fn jobs(c: &ReleaseCandidate) -> Value {
        json!({"total_count":3,"jobs":(["Test","Build ARM64","Publish"].iter().map(|name|json!({"name":name,"run_id":c.run_id,"head_sha":c.source_sha,"status":"completed","conclusion":"success"})).collect::<Vec<_>>())})
    }
    #[test]
    fn exact_success_cannot_cover_wrong_source_attempt_or_pending_work() {
        let c = candidate();
        assert!(verify_run(&c, &run(&c), &jobs(&c)).is_ok());
        for (key, value) in [
            ("head_sha", json!("e".repeat(40))),
            ("run_attempt", json!(3)),
            ("status", json!("in_progress")),
            ("path", json!(".github/workflows/ci.yml")),
        ] {
            let mut r = run(&c);
            r[key] = value;
            assert!(verify_run(&c, &r, &jobs(&c)).is_err());
        }
        for status in ["skipped", "cancelled", "failure"] {
            let mut j = jobs(&c);
            j["jobs"][2]["conclusion"] = json!(status);
            assert!(verify_run(&c, &run(&c), &j).is_err());
        }
        let mut j = jobs(&c);
        j["total_count"] = json!(4);
        assert!(verify_run(&c, &run(&c), &j).is_err());
    }
    #[test]
    fn release_digest_and_tag_are_independent_gates() {
        let c = candidate();
        let mut r = json!({"tag_name":c.release_tag,"draft":false,"assets":[{"name":c.asset_name,"state":"uploaded","size":123,"digest":format!("sha256:{}",c.asset_sha256)}]});
        assert!(verify_package(&c, &r, &json!({"sha":c.source_sha})).is_ok());
        assert!(verify_package(&c, &r, &json!({"sha":"e".repeat(40)})).is_err());
        r["assets"][0]["digest"] = json!(format!("sha256:{}", "e".repeat(64)));
        assert!(verify_package(&c, &r, &json!({"sha":c.source_sha})).is_err());
    }
    #[test]
    fn candidate_identity_changes_with_artifact_or_acceptance_contract() {
        let mut c = candidate();
        let before = c.identity().unwrap();
        c.image = format!("ghcr.io/neuralmimicry/app@sha256:{}", "e".repeat(64));
        assert_ne!(before, c.identity().unwrap());
        let before = c.identity().unwrap();
        c.acceptance.minimum_improvement_pct = 10.0;
        assert_ne!(before, c.identity().unwrap());
    }
    #[test]
    fn no_degradation_is_not_evidence_of_improvement() {
        let c = candidate();
        assert!(improvement(100.0, 90.0, &c.acceptance, true).is_ok());
        assert!(improvement(100.0, 100.0, &c.acceptance, true).is_err());
        assert!(improvement(100.0, 101.0, &c.acceptance, false).is_err());
        assert!(improvement(100.0, f64::NAN, &c.acceptance, true).is_err());
        assert!(measured(&json!({"passed":true}), &c.acceptance).is_err());
    }
    #[test]
    fn cached_container_ready_does_not_mask_an_unready_pod() {
        let c = candidate();
        let t = &c.production;
        let mut s = json!({"namespace":t.namespace,"name":t.deployment,"rollout_complete":true,"ready_replicas":1,"desired_replicas":1,"containers":[{"name":t.container,"image":c.image,"ready":true}],"pods":[{"conditions":[{"type":"Ready","status":"True"}],"containers":[{"name":t.container,"image_id":c.image,"ready":true}]}]});
        assert!(running(&s, t, &c.image, None));
        s["pods"][0]["conditions"][0]["status"] = json!("False");
        assert!(!running(&s, t, &c.image, None));
    }
    #[test]
    fn signed_provenance_must_name_the_exact_image_run_and_attempt() {
        let c = candidate();
        let digest = c.image.split_once("@sha256:").unwrap().1;
        let mut result = json!([{"verificationResult":{"statement":{
            "predicateType":"https://slsa.dev/provenance/v1",
            "subject":[{"digest":{"sha256":digest}}],
            "predicate":{"runDetails":{"metadata":{"invocationId":format!("https://github.com/{}/actions/runs/{}/attempts/{}", c.repository, c.run_id, c.run_attempt)}}}
        }}}]);
        assert!(verify_attestation(&c, &result).is_ok());
        result[0]["verificationResult"]["statement"]["subject"][0]["digest"]["sha256"] =
            json!("e".repeat(64));
        assert!(verify_attestation(&c, &result).is_err());
        assert!(verify_attestation(&c, &json!([])).is_err());
    }
    #[test]
    fn functional_acceptance_requires_fresh_complete_independent_results() {
        let c = candidate();
        let mut probe = json!({"passed":true,"metric":100.0,"metrics":{"delivery_acceptance":{
            "observed_at_unix_ms":crate::models::now_utc().timestamp_millis(),
            "checks":{"authenticated_read":true,"state_preserved":true}
        }}});
        assert!(measured(&probe, &c.acceptance).is_ok());
        probe["metrics"]["delivery_acceptance"]["checks"]["authenticated_read"] = json!(false);
        assert!(measured(&probe, &c.acceptance).is_err());
        probe["metrics"]["delivery_acceptance"]["checks"]["authenticated_read"] = json!(true);
        probe["metrics"]["delivery_acceptance"]["observed_at_unix_ms"] =
            json!(crate::models::now_utc().timestamp_millis() - 60001);
        assert!(measured(&probe, &c.acceptance).is_err());
    }
    #[test]
    fn candidate_cannot_choose_its_own_qualification_policy() {
        let mut c = candidate();
        let mut config = ConductorConfig::default();
        let item = WorkItem::from_new(
            serde_json::from_value(
                json!({"title":"release","summary":"release","target_service":"app"}),
            )
            .unwrap(),
        );
        assert!(c.validate(&config, &item).is_err());
        config.delivery.qualified_releases.insert(
            c.repository.clone(),
            ReleasePolicy {
                workflow: c.workflow.clone(),
                required_jobs: c.required_jobs.clone(),
                uat: c.uat.clone(),
                production: c.production.clone(),
                acceptance: c.acceptance.clone(),
            },
        );
        assert!(c.validate(&config, &item).is_ok());
        c.production.deployment = "another-workload".into();
        assert!(c.validate(&config, &item).is_err());
        c.production.deployment = "app".into();
        c.acceptance.minimum_improvement_pct = 0.1;
        assert!(c.validate(&config, &item).is_err());
    }
    #[test]
    fn nested_continuum_failure_is_never_a_successful_rollout() {
        assert!(payload(json!({"success":true,"data":{"success":false,"data":{}}})).is_err());
    }
}
