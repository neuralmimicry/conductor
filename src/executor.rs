use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    config::ConductorConfig,
    models::{
        ConductorEvent, ExecutionStatus, PolicyVerdict, ServiceSnapshot, WorkExecution, WorkItem,
        WorkStatus,
    },
    policy::{evaluate_work_item, policy_evaluation_to_value},
    repository::ConductorRepository,
};

pub type ExecutionEventCallback = Arc<dyn Fn(ConductorEvent) + Send + Sync>;

fn emit_event(callback: Option<&ExecutionEventCallback>, event: ConductorEvent) {
    if let Some(publish) = callback {
        publish(event);
    }
}

fn emit_execution_event(
    callback: Option<&ExecutionEventCallback>,
    event_type: &str,
    message: impl Into<String>,
    item: Option<&WorkItem>,
    execution: Option<&WorkExecution>,
    status: Option<&str>,
    payload: Value,
) {
    let mut event = ConductorEvent::new(event_type, message, payload);
    event.status = status.map(ToString::to_string);
    if let Some(item) = item {
        event.work_item_id = Some(item.id);
    }
    if let Some(execution) = execution {
        event.execution_id = Some(execution.id);
        event.refiner_job_id = execution.refiner_job_id.clone();
    }
    emit_event(callback, event);
}

pub async fn run_execution_cycle(
    repository: &dyn ConductorRepository,
    config: &ConductorConfig,
    event_callback: Option<&ExecutionEventCallback>,
) -> Result<Vec<WorkExecution>> {
    if !config.execution.enabled {
        return Ok(Vec::new());
    }

    let active = repository
        .list_work_executions(200)
        .await?
        .into_iter()
        .filter(|execution| !execution.status.is_terminal())
        .count();
    if active >= config.execution.max_concurrent_executions {
        return Ok(Vec::new());
    }

    let available_slots = config.execution.max_concurrent_executions - active;
    let work_items = repository.list_work_items().await?;
    let mut executed = Vec::new();
    for item in work_items
        .iter()
        .into_iter()
        .filter(|item| item.execution_approved && matches!(item.status, WorkStatus::Scheduled))
        .take(available_slots)
    {
        executed.push(
            execute_specific_work_item(repository, config, item.id, false, event_callback).await?,
        );
    }
    Ok(executed)
}

pub async fn execute_specific_work_item(
    repository: &dyn ConductorRepository,
    config: &ConductorConfig,
    work_item_id: Uuid,
    force_schedule: bool,
    event_callback: Option<&ExecutionEventCallback>,
) -> Result<WorkExecution> {
    let mut item = repository
        .get_work_item(work_item_id)
        .await?
        .ok_or_else(|| anyhow!("work item {} not found", work_item_id))?;
    if !item.execution_approved {
        return Err(anyhow!(
            "work item {} is not approved for execution",
            work_item_id
        ));
    }
    if force_schedule && !matches!(item.status, WorkStatus::Scheduled) {
        item.status = WorkStatus::Scheduled;
    }

    let work_items = repository.list_work_items().await?;
    let dependency_blockers = dependency_blockers(&item, &work_items);
    if !dependency_blockers.is_empty() {
        let message = format!(
            "execution blocked by dependency graph: {}",
            dependency_blockers.join("; ")
        );
        let policy = json!({
            "verdict": "blocked",
            "risk_level": "dependency_graph",
            "reasons": dependency_blockers,
        });
        let mut execution = WorkExecution::new(item.id, item.target_service.clone());
        execution.policy = policy.clone();
        execution.error = Some(message.clone());
        execution.mark_status(ExecutionStatus::Blocked);
        item.status = WorkStatus::OnHold;
        item.touch_execution(execution.id, policy.clone());
        item.notes.push(format!(
            "{} {}",
            crate::models::now_utc().to_rfc3339(),
            message
        ));
        emit_execution_event(
            event_callback,
            "execution.blocked",
            message.clone(),
            Some(&item),
            Some(&execution),
            Some("blocked"),
            json!({
                "reasons": dependency_blockers,
                "policy": policy,
            }),
        );
        repository.upsert_work_execution(&execution).await?;
        repository.upsert_work_item(&item).await?;
        return Ok(execution);
    }

    let services = repository.list_service_snapshots().await?;
    let target_service = item.target_service.as_deref().and_then(|target| {
        services
            .iter()
            .find(|service| service.service_key == target)
    });
    let policy = evaluate_work_item(config, &item, target_service);

    let mut execution = WorkExecution::new(item.id, item.target_service.clone());
    execution.policy = policy_evaluation_to_value(&policy);
    item.touch_execution(execution.id, execution.policy.clone());

    if !matches!(policy.verdict, PolicyVerdict::Allowed) {
        let message = policy.reasons.join("; ");
        execution.error = Some(message.clone());
        execution.mark_status(ExecutionStatus::Blocked);
        if matches!(
            policy.verdict,
            PolicyVerdict::NeedsApproval | PolicyVerdict::Blocked
        ) {
            item.status = WorkStatus::OnHold;
            item.notes.push(format!(
                "{} execution blocked by policy: {}",
                crate::models::now_utc().to_rfc3339(),
                message
            ));
        }
        emit_execution_event(
            event_callback,
            "execution.blocked",
            message.clone(),
            Some(&item),
            Some(&execution),
            Some("blocked"),
            json!({
                "policy": execution.policy.clone(),
                "reasons": policy.reasons,
            }),
        );
        repository.upsert_work_execution(&execution).await?;
        repository.upsert_work_item(&item).await?;
        return Ok(execution);
    }

    item.status = WorkStatus::InOperation;
    item.progress_pct = 5;
    item.notes.push(format!(
        "{} execution started through Refiner",
        crate::models::now_utc().to_rfc3339()
    ));
    emit_execution_event(
        event_callback,
        "execution.started",
        format!("execution started for {}", item.title),
        Some(&item),
        Some(&execution),
        Some(item.status.as_str()),
        json!({
            "target_service": item.target_service.clone(),
            "progress_pct": item.progress_pct,
        }),
    );
    repository.upsert_work_item(&item).await?;
    repository.upsert_work_execution(&execution).await?;

    let refiner_base_url = refiner_base_url(config, &services)?;
    let client = build_refiner_client(config.integrations.refiner.timeout_seconds.max(1))?;
    login_refiner_if_configured(&client, config, &refiner_base_url).await?;

    execution.mark_status(ExecutionStatus::Planning);
    let prompt = build_refiner_prompt(&item, target_service, &policy);
    let plan_response = post_refiner_json(
        &client,
        config,
        &refiner_base_url,
        "/api/playground/plan",
        &json!({
            "prompt": prompt,
            "provider": config.execution.llm_provider,
            "model": config.execution.llm_model,
            "codingagent": config.execution.coding_agent,
        }),
    )
    .await
    .context("failed to create Refiner execution plan")?;
    emit_execution_event(
        event_callback,
        "execution.planning_submitted",
        format!("planning response received for {}", item.title),
        Some(&item),
        Some(&execution),
        Some(execution.status.as_str()),
        json!({
            "plan_keys": plan_response
                .as_object()
                .map(|entries| entries.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default(),
        }),
    );
    let job_payload = build_job_payload(config, &item, target_service, &plan_response)?;
    execution.request_payload = job_payload.clone();
    execution.latest_payload = json!({"plan_response": plan_response});
    repository.upsert_work_execution(&execution).await?;

    item.progress_pct = 20;
    repository.upsert_work_item(&item).await?;

    let estimate_response = post_refiner_json(
        &client,
        config,
        &refiner_base_url,
        "/api/jobs/estimate",
        &job_payload,
    )
    .await
    .context("failed Refiner estimate gate")?;
    execution.latest_payload = json!({
        "plan_response": execution.latest_payload.get("plan_response").cloned().unwrap_or_else(|| json!({})),
        "estimate_response": estimate_response,
    });
    repository.upsert_work_execution(&execution).await?;

    let submit_response = post_refiner_json(
        &client,
        config,
        &refiner_base_url,
        "/api/jobs",
        &job_payload,
    )
    .await
    .context("failed to submit Refiner job")?;
    let refiner_job_id = submit_response
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| submit_response.get("job_id").and_then(Value::as_str))
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("Refiner job submission did not return an id"))?;
    execution.refiner_job_id = Some(refiner_job_id.clone());
    execution.mark_status(ExecutionStatus::Submitted);
    execution.latest_payload = json!({
        "plan_response": execution.latest_payload.get("plan_response").cloned().unwrap_or_else(|| json!({})),
        "estimate_response": execution.latest_payload.get("estimate_response").cloned().unwrap_or_else(|| json!({})),
        "submit_response": submit_response,
    });
    emit_execution_event(
        event_callback,
        "execution.job_submitted",
        format!("Refiner job {} submitted", refiner_job_id),
        Some(&item),
        Some(&execution),
        Some(execution.status.as_str()),
        json!({
            "refiner_job_id": refiner_job_id.clone(),
            "target_service": item.target_service.clone(),
        }),
    );
    repository.upsert_work_execution(&execution).await?;

    item.progress_pct = 35;
    item.notes.push(format!(
        "{} Refiner job {} accepted",
        crate::models::now_utc().to_rfc3339(),
        refiner_job_id
    ));
    repository.upsert_work_item(&item).await?;

    let terminal = poll_refiner_job(
        &client,
        config,
        &refiner_base_url,
        refiner_job_id.as_str(),
        &mut item,
        &mut execution,
        repository,
        event_callback,
    )
    .await?;

    execution.mark_status(ExecutionStatus::Verifying);
    let verification = verify_refiner_result(&terminal);
    execution.verification = verification.clone();
    execution.latest_payload = terminal.clone();

    let verification_passed = verification
        .get("passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if verification_passed {
        execution.mark_status(ExecutionStatus::Success);
        item.status = WorkStatus::Success;
        item.progress_pct = 100;
        item.finished_at = Some(crate::models::now_utc());
        item.notes.push(format!(
            "{} Refiner job {} completed and passed verification",
            crate::models::now_utc().to_rfc3339(),
            refiner_job_id
        ));
        emit_execution_event(
            event_callback,
            "execution.verification",
            format!("Refiner job {} passed verification", refiner_job_id),
            Some(&item),
            Some(&execution),
            Some("success"),
            verification.clone(),
        );
    } else {
        let failure_reason = verification
            .get("reasons")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "verification failed".to_string());
        execution.error = Some(failure_reason.clone());
        execution.mark_status(ExecutionStatus::Failure);
        item.status = WorkStatus::Failure;
        item.notes.push(format!(
            "{} Refiner job {} failed verification: {}",
            crate::models::now_utc().to_rfc3339(),
            refiner_job_id,
            failure_reason
        ));
        emit_execution_event(
            event_callback,
            "execution.verification",
            format!("Refiner job {} failed verification", refiner_job_id),
            Some(&item),
            Some(&execution),
            Some("failure"),
            verification.clone(),
        );
    }

    item.touch_execution(execution.id, execution.policy.clone());
    repository.upsert_work_execution(&execution).await?;
    repository.upsert_work_item(&item).await?;
    Ok(execution)
}

fn dependency_blockers(item: &WorkItem, work_items: &[WorkItem]) -> Vec<String> {
    let mut blockers = Vec::new();
    for reference in &item.depends_on {
        let reference = reference.trim();
        if reference.is_empty() {
            continue;
        }
        if item.matches_reference(reference) {
            blockers.push(format!("{reference} (self_dependency)"));
            continue;
        }
        let Some(dependency) = work_items
            .iter()
            .find(|candidate| candidate.matches_reference(reference))
        else {
            blockers.push(format!("{reference} (missing)"));
            continue;
        };
        if !matches!(dependency.status, WorkStatus::Success) {
            let label = dependency
                .dedupe_key
                .clone()
                .unwrap_or_else(|| dependency.id.to_string());
            blockers.push(format!("{label} ({})", dependency.status.as_str()));
        }
    }
    blockers
}

fn refiner_base_url(config: &ConductorConfig, services: &[ServiceSnapshot]) -> Result<String> {
    if let Some(base_url) = config.integrations.refiner.base_url.clone() {
        return Ok(base_url);
    }
    services
        .iter()
        .find(|service| service.service_key == "refiner")
        .and_then(|service| {
            service
                .public_url
                .clone()
                .or_else(|| service.internal_url.clone())
        })
        .ok_or_else(|| anyhow!("no Refiner base URL configured or discovered"))
}

fn build_refiner_client(timeout_seconds: u64) -> Result<Client> {
    Ok(Client::builder()
        .use_rustls_tls()
        .cookie_store(true)
        .timeout(Duration::from_secs(timeout_seconds.max(1)))
        .build()?)
}

async fn login_refiner_if_configured(
    client: &Client,
    config: &ConductorConfig,
    base_url: &str,
) -> Result<()> {
    let username = config.integrations.refiner.username.as_deref();
    let password = config.integrations.refiner.password.as_deref();
    let (Some(username), Some(password)) = (username, password) else {
        return Ok(());
    };
    let response = client
        .post(format!("{}/api/login", base_url.trim_end_matches('/')))
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
        .unwrap_or("refiner login failed");
    Err(anyhow!("refiner login failed: {}", message))
}

fn build_refiner_prompt(
    work_item: &WorkItem,
    service: Option<&ServiceSnapshot>,
    policy: &crate::models::PolicyEvaluation,
) -> String {
    let repo_context = service
        .and_then(|service| service.repo_path.as_deref().or(service.repo_url.as_deref()))
        .unwrap_or("no repository context discovered");
    let service_name = service
        .map(|service| service.display_name.as_str())
        .unwrap_or("target service");
    let verification = if policy.required_verifications.is_empty() {
        "project-native verification commands".to_string()
    } else {
        policy.required_verifications.join(", ")
    };
    format!(
        "Work item: {title}\nTarget: {target}\nSummary: {summary}\nPlan JSON: {plan}\nRepository context: {repo}\nConstraints: keep changes scoped, resilient, and secure; avoid destructive commands; leave unrelated files untouched.\nRequired verification: {verification}\nProduce a project-solver plan and job payload that implements the change with explicit verification.",
        title = work_item.title,
        target = service_name,
        summary = work_item.summary,
        plan = work_item.plan,
        repo = repo_context,
        verification = verification,
    )
}

fn build_job_payload(
    config: &ConductorConfig,
    work_item: &WorkItem,
    service: Option<&ServiceSnapshot>,
    plan_response: &Value,
) -> Result<Value> {
    let mut payload = plan_response
        .get("job_payload")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    payload.insert(
        "workflow".to_string(),
        json!(config.execution.refiner_workflow.clone()),
    );
    payload.insert(
        "project_name".to_string(),
        json!(format!(
            "{} :: {}",
            service
                .map(|service| service.display_name.clone())
                .unwrap_or_else(|| "Conductor Task".to_string()),
            work_item.title
        )),
    );
    payload.insert(
        "requirements_text".to_string(),
        json!(requirements_text(plan_response, work_item, service)),
    );
    payload.insert("project_run".to_string(), json!(true));
    payload.insert("dry_run".to_string(), json!(false));
    payload.insert(
        "token_scope".to_string(),
        json!(config.execution.token_scope.clone()),
    );
    payload.insert(
        "commit_message".to_string(),
        json!(format!("conductor: {}", work_item.title)),
    );
    if config.policy.require_refiner_strict_mode {
        payload.insert("solver_command_policy_mode".to_string(), json!("strict"));
    }
    if let Some(provider) = &config.execution.llm_provider {
        payload.insert("llm_provider".to_string(), json!(provider));
    }
    if let Some(model) = &config.execution.llm_model {
        payload.insert("llm_model".to_string(), json!(model));
    }
    if let Some(service) = service {
        if config.execution.use_local_project_root {
            if let Some(project_root) = service.repo_path.as_deref() {
                payload.insert("project_root".to_string(), json!(project_root));
            }
        }
        if let Some(repo_url) = service.repo_url.as_deref() {
            payload.insert("repo_url".to_string(), json!(repo_url));
        }
        if let Some(repo_branch) = service.repo_branch.as_deref() {
            payload.insert("repo_branch".to_string(), json!(repo_branch));
        }
        payload.insert(
            "work_branch".to_string(),
            json!(work_branch_name(work_item)),
        );
    }
    Ok(Value::Object(payload))
}

fn requirements_text(
    plan_response: &Value,
    work_item: &WorkItem,
    service: Option<&ServiceSnapshot>,
) -> String {
    if let Some(text) = plan_response
        .get("requirements_text")
        .and_then(Value::as_str)
    {
        return text.to_string();
    }

    let target = service
        .map(|service| service.display_name.as_str())
        .unwrap_or("target service");
    format!(
        "Overview: Improve {target} through the Conductor execution loop.\n\nRequirements Register:\n- REQ-001: Implement the scoped change described in the work item.\n- REQ-002: Preserve secure, resilient behaviour and avoid destructive commands.\n- REQ-003: Update or add tests covering the changed path.\n- REQ-004: Run verification commands and report the outcome.\n- REQ-005: Leave unrelated files untouched.\n\nWork Item Summary:\n{summary}\n\nPlan JSON:\n{plan}",
        target = target,
        summary = work_item.summary,
        plan = work_item.plan,
    )
}

fn work_branch_name(work_item: &WorkItem) -> String {
    let raw = work_item
        .dedupe_key
        .clone()
        .unwrap_or_else(|| work_item.id.to_string());
    let sanitized: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '/' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    format!("conductor/{}", sanitized.trim_matches('-'))
}

async fn poll_refiner_job(
    client: &Client,
    config: &ConductorConfig,
    base_url: &str,
    job_id: &str,
    item: &mut WorkItem,
    execution: &mut WorkExecution,
    repository: &dyn ConductorRepository,
    event_callback: Option<&ExecutionEventCallback>,
) -> Result<Value> {
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(config.execution.job_timeout_seconds.max(30));
    let poll_interval = Duration::from_secs(config.execution.poll_interval_seconds.max(1));
    let mut last_status = String::new();

    loop {
        let detail = get_refiner_json(
            client,
            config,
            base_url,
            format!("/api/jobs/{}", job_id).as_str(),
        )
        .await
        .with_context(|| format!("failed to poll Refiner job {}", job_id))?;
        execution.latest_payload = detail.clone();
        let status = detail
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_ascii_lowercase();
        let progress = detail
            .get("progress")
            .and_then(Value::as_i64)
            .map(|value| value.clamp(0, 100) as i32)
            .unwrap_or_else(|| match status.as_str() {
                "queued" => 40,
                "running" => 70,
                "completed" => 95,
                _ => item.progress_pct,
            });
        item.progress_pct = progress;
        execution.mark_status(match status.as_str() {
            "queued" => ExecutionStatus::Submitted,
            "running" | "paused" => ExecutionStatus::Running,
            "completed" => ExecutionStatus::Verifying,
            "cancelled" | "stopped" => ExecutionStatus::Cancelled,
            "failed" => ExecutionStatus::Failure,
            _ => execution.status,
        });
        if status != last_status {
            last_status = status.clone();
            emit_execution_event(
                event_callback,
                "execution.refiner_status",
                format!("Refiner job {} transitioned to {}", job_id, status),
                Some(item),
                Some(execution),
                Some(status.as_str()),
                json!({
                    "refiner_job_id": job_id,
                    "status": status,
                    "progress_pct": progress,
                }),
            );
        }
        repository.upsert_work_execution(execution).await?;
        repository.upsert_work_item(item).await?;

        if matches!(
            status.as_str(),
            "completed" | "failed" | "cancelled" | "stopped"
        ) {
            return Ok(detail);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!("Refiner job {} timed out", job_id));
        }
        tokio::time::sleep(poll_interval).await;
    }
}

fn verify_refiner_result(detail: &Value) -> Value {
    let status = detail
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_ascii_lowercase();
    let mut stage_failures = Vec::new();
    if let Some(stages) = detail.get("stages").and_then(Value::as_array) {
        for stage in stages {
            let name = stage.get("name").and_then(Value::as_str).unwrap_or("stage");
            let stage_status = stage
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_ascii_lowercase();
            if matches!(stage_status.as_str(), "failed" | "blocked" | "cancelled") {
                stage_failures.push(format!("{}:{}", name, stage_status));
            }
        }
    }

    let mut findings = Vec::new();
    collect_verification_findings(String::new(), detail, &mut findings);
    let passed = status == "completed" && stage_failures.is_empty() && findings.is_empty();
    json!({
        "passed": passed,
        "job_status": status,
        "stage_failures": stage_failures,
        "reasons": findings,
    })
}

fn collect_verification_findings(prefix: String, value: &Value, findings: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let next = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                let lowered = key.to_ascii_lowercase();
                let suspicious = lowered.contains("verification_failure")
                    || lowered.contains("verification_failures")
                    || lowered.contains("unresolved_verification_failures")
                    || lowered.contains("failed_tests")
                    || lowered.contains("failed_checks");
                if suspicious && value_is_problem(value) {
                    findings.push(format!("{} indicates a verification problem", next));
                }
                collect_verification_findings(next, value, findings);
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                collect_verification_findings(format!("{}[{}]", prefix, index), item, findings);
            }
        }
        _ => {}
    }
}

fn value_is_problem(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().map(|value| value > 0.0).unwrap_or(false),
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

async fn post_refiner_json(
    client: &Client,
    config: &ConductorConfig,
    base_url: &str,
    path: &str,
    body: &Value,
) -> Result<Value> {
    let mut request = client
        .post(format!("{}{}", base_url.trim_end_matches('/'), path))
        .json(body);
    if let Some(token) = config.integrations.refiner.bearer_token.as_deref() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    decode_response(response).await
}

async fn get_refiner_json(
    client: &Client,
    config: &ConductorConfig,
    base_url: &str,
    path: &str,
) -> Result<Value> {
    let mut request = client.get(format!("{}{}", base_url.trim_end_matches('/'), path));
    if let Some(token) = config.integrations.refiner.bearer_token.as_deref() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    decode_response(response).await
}

async fn decode_response(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let payload = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
    if status.is_success() {
        return Ok(payload);
    }
    let message = payload
        .get("details")
        .and_then(Value::as_str)
        .or_else(|| payload.get("error").and_then(Value::as_str))
        .or_else(|| payload.get("message").and_then(Value::as_str))
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
    use crate::{
        config::ConductorConfig,
        models::{NewWorkItem, ServiceHealth},
        storage::memory::MemoryRepository,
    };
    use std::sync::Arc;

    fn sample_service() -> ServiceSnapshot {
        ServiceSnapshot {
            service_key: "conductor".to_string(),
            display_name: "Conductor".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "continuum_tenant_conductor".to_string(),
            playbooks: vec![],
            hosts: vec![],
            namespace: None,
            service_name: None,
            internal_url: None,
            public_url: None,
            repo_path: Some("/tmp/conductor".to_string()),
            repo_url: Some("git@github.com:neuralmimicry/conductor.git".to_string()),
            repo_branch: Some("main".to_string()),
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

    #[test]
    fn job_payload_inherits_repo_context_and_strict_policy() {
        let config = ConductorConfig::default();
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("stabilize:conductor".to_string()),
            title: "Stabilize Conductor".to_string(),
            summary: "Improve executor reliability".to_string(),
            target_service: Some("conductor".to_string()),
            status: None,
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "stabilize_service"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        let payload = build_job_payload(
            &config,
            &item,
            Some(&sample_service()),
            &json!({"job_payload": {"workflow": "project_solver"}}),
        )
        .expect("payload");
        assert_eq!(
            payload.get("repo_url").and_then(Value::as_str),
            Some("git@github.com:neuralmimicry/conductor.git")
        );
        assert_eq!(
            payload
                .get("solver_command_policy_mode")
                .and_then(Value::as_str),
            Some("strict")
        );
    }

    #[test]
    fn verification_fails_when_failed_stage_exists() {
        let report = verify_refiner_result(&json!({
            "status": "completed",
            "stages": [
                {"name": "plan", "status": "completed"},
                {"name": "verify", "status": "failed"}
            ]
        }));
        assert_eq!(report.get("passed").and_then(Value::as_bool), Some(false));
    }

    #[tokio::test]
    async fn execution_cycle_skips_items_with_unsatisfied_dependencies() {
        let config = ConductorConfig::default();
        let repository = Arc::new(MemoryRepository::new());

        let dependency = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("stabilize:dependency".to_string()),
            title: "Stabilize Dependency".to_string(),
            summary: "Resolve dependency first".to_string(),
            target_service: Some("conductor".to_string()),
            status: Some(WorkStatus::Planned),
            priority: Some(90),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "stabilize_service"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        let blocked = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("followup:dependency".to_string()),
            title: "Follow-up".to_string(),
            summary: "Run after stabilization".to_string(),
            target_service: Some("conductor".to_string()),
            status: Some(WorkStatus::Scheduled),
            priority: Some(80),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "follow_up"}),
            depends_on: vec!["stabilize:dependency".to_string()],
            source: None,
            scheduled_for: None,
        });

        repository
            .upsert_work_item(&dependency)
            .await
            .expect("dependency");
        repository
            .upsert_work_item(&blocked)
            .await
            .expect("blocked item");

        let executed = run_execution_cycle(repository.as_ref(), &config, None)
            .await
            .expect("execution cycle");

        assert_eq!(executed.len(), 1);
        assert_eq!(executed[0].status, ExecutionStatus::Blocked);
    }

    #[tokio::test]
    async fn execute_specific_work_item_blocks_when_dependencies_are_not_satisfied() {
        let config = ConductorConfig::default();
        let repository = Arc::new(MemoryRepository::new());

        let dependency = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("stabilize:dependency".to_string()),
            title: "Stabilize Dependency".to_string(),
            summary: "Resolve dependency first".to_string(),
            target_service: Some("conductor".to_string()),
            status: Some(WorkStatus::Planned),
            priority: Some(90),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "stabilize_service"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        let blocked = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("followup:dependency".to_string()),
            title: "Follow-up".to_string(),
            summary: "Run after stabilization".to_string(),
            target_service: Some("conductor".to_string()),
            status: Some(WorkStatus::Scheduled),
            priority: Some(80),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "follow_up"}),
            depends_on: vec!["stabilize:dependency".to_string()],
            source: None,
            scheduled_for: None,
        });

        repository
            .upsert_work_item(&dependency)
            .await
            .expect("dependency");
        repository
            .upsert_work_item(&blocked)
            .await
            .expect("blocked item");

        let execution =
            execute_specific_work_item(repository.as_ref(), &config, blocked.id, false, None)
                .await
                .expect("execution");
        assert_eq!(execution.status, ExecutionStatus::Blocked);
        assert!(
            execution
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("dependency graph")
        );

        let refreshed = repository
            .get_work_item(blocked.id)
            .await
            .expect("refreshed item")
            .expect("stored item");
        assert_eq!(refreshed.status, WorkStatus::OnHold);
        assert_eq!(refreshed.last_execution_id, Some(execution.id));
    }
}
