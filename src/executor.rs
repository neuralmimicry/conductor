use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use chrono::Duration as ChronoDuration;
use futures::future;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    config::ConductorConfig,
    integrations::{
        GitHubActionsEvidence, build_http_client, fetch_latest_github_actions_evidence,
        github_repository_coordinate, probe_service,
    },
    models::{
        ConductorEvent, DeliveryStage, ExecutionStatus, PolicyVerdict, RepositorySnapshot,
        RolloutStrategy, ServiceHealth, ServiceSnapshot, WorkExecution, WorkItem, WorkItemPatch,
        WorkStatus,
    },
    policy::{apply_repository_safety_policy, evaluate_work_item, policy_evaluation_to_value},
    repository::ConductorRepository,
    validation::{failure_reasons, preview_independent_validation, run_independent_validation},
};

pub type ExecutionEventCallback = Arc<dyn Fn(ConductorEvent) + Send + Sync>;

const REFINER_EXECUTION_PLAN_PATH: &str = "/api/execution/plan";
// Discovery and health integrations use a short timeout, but Refiner job
// payloads include bounded logs and metrics and can take longer to read while
// the solver is busy. Keep lifecycle polling from failing on that unrelated
// short integration timeout. Five minutes remains bounded while allowing a
// busy ARM64 Refiner to serialize a job detail response without losing the
// otherwise healthy execution.
const REFINER_OPERATION_REQUEST_TIMEOUT_SECONDS: u64 = 300;
// Refiner persists terminal job metadata in more than one update.  A failed
// project run can therefore expose finished_at/exit_code before the produced
// commit is copied into repo_info.  Keep the terminal response open briefly
// so protected-target rollback has the exact commit identity it requires.
const REFINER_TERMINAL_COMMIT_REFRESH_ATTEMPTS: usize = 10;
const REFINER_TERMINAL_COMMIT_REFRESH_INTERVAL_SECONDS: u64 = 1;

async fn reconcile_stale_executions(
    repository: &dyn ConductorRepository,
    config: &ConductorConfig,
    event_callback: Option<&ExecutionEventCallback>,
) -> Result<usize> {
    let now = crate::models::now_utc();
    let stale_after = ChronoDuration::seconds(config.execution.claim_ttl_seconds.max(60) as i64);
    let executions = repository.list_work_executions(10_000).await?;
    let mut reconciled = 0;

    for mut execution in executions {
        if execution.status.is_terminal() || execution.updated_at + stale_after > now {
            continue;
        }

        let age_seconds = (now - execution.updated_at).num_seconds().max(0);
        let message = format!(
            "execution {} was reconciled as failed after {} seconds without a heartbeat",
            execution.id, age_seconds
        );
        execution.error = Some(message.clone());
        execution.mark_status(ExecutionStatus::Failure);
        repository.upsert_work_execution(&execution).await?;

        if let Some(item) = repository.get_work_item(execution.work_item_id).await?
            && item.last_execution_id == Some(execution.id)
        {
            let _ = repository
                .patch_work_item(
                    item.id,
                    WorkItemPatch {
                        status: Some(WorkStatus::Failure),
                        note: Some(message.clone()),
                        ..WorkItemPatch::default()
                    },
                )
                .await?;
        }

        emit_execution_event(
            event_callback,
            "execution.stale_reconciled",
            message.clone(),
            None,
            Some(&execution),
            Some("failure"),
            json!({
                "reason": "heartbeat_timeout",
                "age_seconds": age_seconds,
                "stale_after_seconds": stale_after.num_seconds(),
            }),
        );
        reconciled += 1;
    }

    Ok(reconciled)
}

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
    if config.execution.emergency_stop {
        emit_event(
            event_callback,
            ConductorEvent::new(
                "execution.cycle.skipped",
                "execution cycle skipped because emergency_stop is enabled",
                json!({"reason": "emergency_stop"}),
            ),
        );
        return Ok(Vec::new());
    }
    if config.execution.dry_run {
        emit_event(
            event_callback,
            ConductorEvent::new(
                "execution.cycle.skipped",
                "execution cycle skipped because dry_run is enabled",
                json!({"reason": "dry_run"}),
            ),
        );
        return Ok(Vec::new());
    }

    reconcile_stale_executions(repository, config, event_callback).await?;

    let work_items = repository
        .claim_scheduled_work_items(
            crate::models::now_utc(),
            execution_instance_id(config),
            config.execution.max_concurrent_executions,
            config.execution.claim_ttl_seconds,
        )
        .await?;
    let mut executed = Vec::new();
    for item in work_items {
        executed.push(dispatch_claimed_work_item(repository, config, item, event_callback).await?);
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
    if config.execution.emergency_stop {
        return Err(anyhow!(
            "execution is halted because execution.emergency_stop is enabled"
        ));
    }

    let item = repository
        .get_work_item(work_item_id)
        .await?
        .ok_or_else(|| anyhow!("work item {} not found", work_item_id))?;
    if !item.execution_approved {
        return Err(anyhow!(
            "work item {} is not approved for execution",
            work_item_id
        ));
    }
    if !force_schedule
        && item
            .scheduled_for
            .is_some_and(|scheduled_for| scheduled_for > crate::models::now_utc())
    {
        return Err(anyhow!(
            "work item {} is scheduled for the future and is not yet due",
            work_item_id
        ));
    }

    if config.execution.dry_run {
        return preview_work_item_execution(
            repository,
            config,
            item,
            force_schedule,
            event_callback,
        )
        .await;
    }

    reconcile_stale_executions(repository, config, event_callback).await?;

    let claimed = repository
        .claim_work_item_for_execution(
            work_item_id,
            crate::models::now_utc(),
            execution_instance_id(config),
            config.execution.claim_ttl_seconds,
            force_schedule,
            config.execution.max_concurrent_executions,
        )
        .await?
        .ok_or_else(|| {
            anyhow!(
                "work item {} is already claimed or no execution capacity is available",
                work_item_id
            )
        })?;
    dispatch_claimed_work_item(repository, config, claimed, event_callback).await
}

async fn dispatch_claimed_work_item(
    repository: &dyn ConductorRepository,
    config: &ConductorConfig,
    mut item: WorkItem,
    event_callback: Option<&ExecutionEventCallback>,
) -> Result<WorkExecution> {
    let claim_token = item
        .claim_token
        .ok_or_else(|| anyhow!("work item {} does not have an execution claim", item.id))?;

    let result = dispatch_claimed_work_item_inner(
        repository,
        config,
        &mut item,
        claim_token,
        event_callback,
    )
    .await;
    if item.claim_token.is_some() {
        let _ = repository
            .release_work_item_claim(item.id, claim_token)
            .await;
        item.clear_claim();
    }
    result
}

async fn dispatch_claimed_work_item_inner(
    repository: &dyn ConductorRepository,
    config: &ConductorConfig,
    item: &mut WorkItem,
    claim_token: Uuid,
    event_callback: Option<&ExecutionEventCallback>,
) -> Result<WorkExecution> {
    let work_items = repository.list_work_items().await?;
    let dependency_blockers = dependency_blockers(item, &work_items);
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
        let mut execution = WorkExecution::new(
            item.id,
            item.target_service.clone(),
            item.delivery_stage,
            item.rollout_strategy,
        );
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
        repository.upsert_work_execution(&execution).await?;
        repository.upsert_work_item(item).await?;
        emit_execution_event(
            event_callback,
            "execution.blocked",
            message.clone(),
            Some(item),
            Some(&execution),
            Some("blocked"),
            json!({
                "reasons": dependency_blockers,
                "policy": policy,
            }),
        );
        let _ = repository
            .release_work_item_claim(item.id, claim_token)
            .await;
        item.clear_claim();
        return Ok(execution);
    }

    let services = repository.list_service_snapshots().await?;
    let target_service = item.target_service.as_deref().and_then(|target| {
        services
            .iter()
            .find(|service| service.service_key == target)
    });
    // Repository snapshots are required for every delivery stage, not only
    // production.  Refiner must receive the actual GitHub repository context
    // before it creates a workspace; otherwise a service discovered from a
    // local checkout can silently fall back to a starter project and never
    // push the requested change to its owning repository.
    let repositories = repository.list_repository_snapshots().await?;
    let mut policy = evaluate_work_item(config, item, target_service);
    apply_repository_safety_policy(config, item, target_service, &repositories, &mut policy);
    let safety_contract = safety_contract(config, &policy, item);
    let safety_baseline = if policy.sensitive_targets.is_empty() {
        json!({"required": false})
    } else {
        capture_safety_probe(config, target_service).await
    };
    apply_safety_baseline_gate(&mut policy, &safety_baseline);
    let github_actions =
        production_github_actions_evidence(config, item, target_service, &repositories).await;
    apply_github_actions_gate(&mut policy, github_actions.as_ref());

    let mut execution = WorkExecution::new(
        item.id,
        item.target_service.clone(),
        item.delivery_stage,
        item.rollout_strategy,
    );
    execution.policy = policy_value_with_github_actions(&policy, github_actions.as_ref());
    execution.latest_payload = json!({
        "safety": {
            "contract": safety_contract.clone(),
            "baseline": safety_baseline.clone(),
        }
    });
    item.touch_execution(execution.id, execution.policy.clone());

    if !matches!(policy.verdict, PolicyVerdict::Allowed) {
        let message = policy.reasons.join("; ");
        execution.error = Some(message.clone());
        execution.mark_status(ExecutionStatus::Blocked);
        item.status = WorkStatus::OnHold;
        item.notes.push(format!(
            "{} execution blocked by policy: {}",
            crate::models::now_utc().to_rfc3339(),
            message
        ));
        repository.upsert_work_execution(&execution).await?;
        repository.upsert_work_item(item).await?;
        emit_execution_event(
            event_callback,
            "execution.blocked",
            message.clone(),
            Some(item),
            Some(&execution),
            Some("blocked"),
            json!({
                "policy": execution.policy.clone(),
                "reasons": policy.reasons,
            }),
        );
        let _ = repository
            .release_work_item_claim(item.id, claim_token)
            .await;
        item.clear_claim();
        return Ok(execution);
    }

    item.status = WorkStatus::InOperation;
    item.progress_pct = 5;
    if item.started_at.is_none() {
        item.started_at = Some(crate::models::now_utc());
    }
    item.finished_at = None;
    item.notes.push(format!(
        "{} execution started through Refiner",
        crate::models::now_utc().to_rfc3339()
    ));
    // Persist the execution before emitting any event that references it.  In
    // PostgreSQL conductor_events.execution_id has a foreign-key constraint;
    // publishing first makes the event write race the execution write and can
    // leave the execution invisible and permanently consume capacity.
    execution.mark_status(ExecutionStatus::Planning);
    repository.upsert_work_execution(&execution).await?;
    repository.upsert_work_item(item).await?;
    emit_execution_event(
        event_callback,
        "execution.started",
        format!("execution started for {}", item.title),
        Some(item),
        Some(&execution),
        Some(item.status.as_str()),
        json!({
            "target_service": item.target_service.clone(),
            "progress_pct": item.progress_pct,
            "claimed_by": item.claimed_by.clone(),
        }),
    );

    let _ = repository
        .release_work_item_claim(item.id, claim_token)
        .await;
    item.clear_claim();

    let client = match build_refiner_client(
        config
            .integrations
            .refiner
            .timeout_seconds
            .max(REFINER_OPERATION_REQUEST_TIMEOUT_SECONDS),
    ) {
        Ok(client) => client,
        Err(error) => {
            return finalize_execution_failure(
                repository,
                item,
                &mut execution,
                event_callback,
                error.to_string(),
            )
            .await;
        }
    };
    let refiner_base_url = match refiner_base_url(&client, config, &services).await {
        Ok(value) => value,
        Err(error) => {
            return finalize_execution_failure(
                repository,
                item,
                &mut execution,
                event_callback,
                error.to_string(),
            )
            .await;
        }
    };
    if let Err(error) = login_refiner_if_configured(&client, config, &refiner_base_url).await {
        return finalize_execution_failure(
            repository,
            item,
            &mut execution,
            event_callback,
            error.to_string(),
        )
        .await;
    }

    let prompt = build_refiner_prompt(config, item, target_service, &policy);
    let plan_response = match post_refiner_json(
        &client,
        config,
        &refiner_base_url,
        REFINER_EXECUTION_PLAN_PATH,
        &json!({
            "prompt": prompt,
            "provider": config.execution.llm_provider,
            "model": config.execution.llm_model,
            "codingagent": config.execution.coding_agent,
        }),
    )
    .await
    .context("failed to create Refiner execution plan")
    {
        Ok(response) => response,
        Err(error) => {
            return finalize_execution_failure(
                repository,
                item,
                &mut execution,
                event_callback,
                error.to_string(),
            )
            .await;
        }
    };
    if let Err(error) = validate_refiner_plan_response(&plan_response) {
        return finalize_execution_failure(
            repository,
            item,
            &mut execution,
            event_callback,
            format!("Refiner returned a low-quality execution plan: {error}"),
        )
        .await;
    }
    let mut job_payload =
        match build_job_payload(config, item, target_service, &repositories, &plan_response) {
            Ok(payload) => payload,
            Err(error) => {
                return finalize_execution_failure(
                    repository,
                    item,
                    &mut execution,
                    event_callback,
                    error.to_string(),
                )
                .await;
            }
        };
    if let Some(object) = job_payload.as_object_mut() {
        object.insert("protected_rollout".to_string(), safety_contract.clone());
        if !policy.sensitive_targets.is_empty() {
            let requirements = object
                .get("requirements_text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            object.insert(
                "requirements_text".to_string(),
                json!(format!(
                    "{requirements}\n\nProtected rollout contract (mandatory): capture a fresh readiness baseline before any change; use the selected canary or red_green strategy; verify health throughout the post-rollout window; if health or verification degrades, automatically revert the exact produced commit without rewriting history, rerun tests and GitHub Actions, and verify recovery."
                )),
            );
        }
    }
    execution.request_payload = job_payload.clone();
    execution.latest_payload = json!({"plan_response": plan_response});
    repository.upsert_work_execution(&execution).await?;

    item.progress_pct = 20;
    repository.upsert_work_item(item).await?;
    emit_execution_event(
        event_callback,
        "execution.planning_submitted",
        format!("planning response received for {}", item.title),
        Some(item),
        Some(&execution),
        Some(execution.status.as_str()),
        json!({
            "plan_keys": plan_response
                .as_object()
                .map(|entries| entries.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default(),
        }),
    );

    let estimate_response = match post_refiner_json(
        &client,
        config,
        &refiner_base_url,
        "/api/jobs/estimate",
        &job_payload,
    )
    .await
    .context("failed Refiner estimate gate")
    {
        Ok(response) => response,
        Err(error) => {
            return finalize_execution_failure(
                repository,
                item,
                &mut execution,
                event_callback,
                error.to_string(),
            )
            .await;
        }
    };
    execution.latest_payload = json!({
        "plan_response": execution.latest_payload.get("plan_response").cloned().unwrap_or_else(|| json!({})),
        "estimate_response": estimate_response,
    });
    repository.upsert_work_execution(&execution).await?;

    let submit_response = match post_refiner_json(
        &client,
        config,
        &refiner_base_url,
        "/api/jobs",
        &job_payload,
    )
    .await
    .context("failed to submit Refiner job")
    {
        Ok(response) => response,
        Err(error) => {
            return finalize_execution_failure(
                repository,
                item,
                &mut execution,
                event_callback,
                error.to_string(),
            )
            .await;
        }
    };
    let refiner_job_id = match submit_response
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| submit_response.get("job_id").and_then(Value::as_str))
        .map(ToString::to_string)
    {
        Some(job_id) => job_id,
        None => {
            return finalize_execution_failure(
                repository,
                item,
                &mut execution,
                event_callback,
                "Refiner job submission did not return an id",
            )
            .await;
        }
    };
    execution.refiner_job_id = Some(refiner_job_id.clone());
    execution.mark_status(ExecutionStatus::Submitted);
    execution.latest_payload = json!({
        "plan_response": execution.latest_payload.get("plan_response").cloned().unwrap_or_else(|| json!({})),
        "estimate_response": execution.latest_payload.get("estimate_response").cloned().unwrap_or_else(|| json!({})),
        "submit_response": submit_response,
    });
    repository.upsert_work_execution(&execution).await?;
    emit_execution_event(
        event_callback,
        "execution.job_submitted",
        format!("Refiner job {} submitted", refiner_job_id),
        Some(item),
        Some(&execution),
        Some(execution.status.as_str()),
        json!({
            "refiner_job_id": refiner_job_id.clone(),
            "target_service": item.target_service.clone(),
        }),
    );

    item.progress_pct = 35;
    item.notes.push(format!(
        "{} Refiner job {} accepted",
        crate::models::now_utc().to_rfc3339(),
        refiner_job_id
    ));
    repository.upsert_work_item(item).await?;

    let terminal = match poll_refiner_job(
        &client,
        config,
        &refiner_base_url,
        refiner_job_id.as_str(),
        item,
        &mut execution,
        repository,
        event_callback,
    )
    .await
    {
        Ok(detail) => detail,
        Err(error) => {
            return finalize_execution_failure(
                repository,
                item,
                &mut execution,
                event_callback,
                error.to_string(),
            )
            .await;
        }
    };

    execution.mark_status(ExecutionStatus::Verifying);
    let current_stage = item.delivery_stage;
    let current_rollout = item.rollout_strategy;
    let mut verification = verify_refiner_result(&terminal);
    if let Some(object) = verification.as_object_mut() {
        object.insert("delivery_stage".to_string(), json!(current_stage.as_str()));
        object.insert(
            "rollout_strategy".to_string(),
            json!(current_rollout.as_str()),
        );
    }
    let independent_validation = run_independent_validation_with_heartbeat(
        &config.validation,
        target_service,
        &policy.required_verifications,
        &mut execution,
        repository,
    )
    .await?;
    merge_independent_validation(
        &mut verification,
        &independent_validation,
        config.validation.require_success,
    );
    item.notes.push(format!(
        "{} {}",
        crate::models::now_utc().to_rfc3339(),
        independent_validation.summary
    ));
    execution.verification = verification.clone();
    execution.latest_payload = attach_independent_validation_payload(
        terminal.clone(),
        serde_json::to_value(&independent_validation).unwrap_or_else(|_| json!({})),
    );
    repository.upsert_work_execution(&execution).await?;
    repository.upsert_work_item(item).await?;
    emit_execution_event(
        event_callback,
        "execution.independent_validation",
        format!("independent validation completed for {}", item.title),
        Some(item),
        Some(&execution),
        Some(if independent_validation.passed {
            "success"
        } else {
            "failure"
        }),
        serde_json::to_value(&independent_validation).unwrap_or_else(|_| json!({})),
    );

    let mut safety_post_rollout = json!({"required": false});
    let mut rollback = json!({"attempted": false});
    let refiner_completed = refiner_effective_status(&terminal) == "completed";
    if !policy.sensitive_targets.is_empty() && refiner_completed {
        safety_post_rollout = run_safety_health_window(config, target_service).await;
        if let Some(object) = verification.as_object_mut() {
            object.insert(
                "post_rollout_health".to_string(),
                safety_post_rollout.clone(),
            );
        }
    }
    if !policy.sensitive_targets.is_empty() {
        let rollout_evidence =
            protected_rollout_evidence(item, &execution.request_payload, &terminal);
        if let Some(object) = verification.as_object_mut() {
            object.insert("rollout_evidence".to_string(), rollout_evidence.clone());
        }
        if rollout_evidence.get("passed").and_then(Value::as_bool) == Some(false) {
            if let Some(object) = verification.as_object_mut() {
                object.insert("passed".to_string(), json!(false));
                object
                    .entry("reasons".to_string())
                    .or_insert_with(|| json!([]));
                if let Some(reasons) = object.get_mut("reasons").and_then(Value::as_array_mut) {
                    reasons.push(json!(
                        "protected-target deployment lacks successful staged rollout evidence"
                    ));
                }
            }
        }
    }
    let mut verification_passed = verification
        .get("passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if policy.sensitive_targets.len() > 0
        && safety_post_rollout.get("passed").and_then(Value::as_bool) == Some(false)
    {
        verification_passed = false;
        if let Some(object) = verification.as_object_mut() {
            object.insert("passed".to_string(), json!(false));
            object
                .entry("reasons".to_string())
                .or_insert_with(|| json!([]));
            if let Some(reasons) = object.get_mut("reasons").and_then(Value::as_array_mut) {
                reasons.push(json!("protected-target post-rollout health degraded"));
            }
        }
    }
    if !verification_passed && !policy.sensitive_targets.is_empty() {
        if let Some(commit_sha) = extract_commit_sha(&terminal) {
            rollback = automatic_rollback(
                &client,
                config,
                &refiner_base_url,
                item,
                &execution,
                target_service,
                &terminal,
                &commit_sha,
                &safety_post_rollout,
            )
            .await;
            if rollback.get("succeeded").and_then(Value::as_bool) != Some(true) {
                verification_passed = false;
            }
        } else if let Some(object) = verification.as_object_mut() {
            object
                .entry("reasons".to_string())
                .or_insert_with(|| json!([]));
            if let Some(reasons) = object.get_mut("reasons").and_then(Value::as_array_mut) {
                reasons.push(json!(
                    "protected target failed without a rollback commit identity"
                ));
            }
        }
    }
    if let Some(object) = verification.as_object_mut() {
        object.insert("rollback".to_string(), rollback.clone());
    }
    execution.latest_payload = json!({
        "terminal": terminal,
        "independent_validation": serde_json::to_value(&independent_validation)
            .unwrap_or_else(|_| json!({})),
        "safety": {
            "contract": safety_contract,
            "baseline": safety_baseline,
            "post_rollout": safety_post_rollout,
            "rollback": rollback,
        }
    });
    execution.verification = verification.clone();
    repository.upsert_work_execution(&execution).await?;
    repository.upsert_work_item(item).await?;
    if rollback.get("attempted").and_then(Value::as_bool) == Some(true) {
        item.notes.push(format!(
            "{} automatic protected-target rollback {}",
            crate::models::now_utc().to_rfc3339(),
            if rollback.get("succeeded").and_then(Value::as_bool) == Some(true) {
                "succeeded"
            } else {
                "failed"
            }
        ));
        emit_execution_event(
            event_callback,
            "execution.rollback",
            format!(
                "automatic rollback evaluated for Refiner job {}",
                refiner_job_id
            ),
            Some(item),
            Some(&execution),
            Some(
                if rollback.get("succeeded").and_then(Value::as_bool) == Some(true) {
                    "success"
                } else {
                    "failure"
                },
            ),
            rollback.clone(),
        );
    }
    if verification_passed {
        execution.mark_status(ExecutionStatus::Success);
        item.mark_stage_validated(current_stage);
        repository.upsert_work_execution(&execution).await?;
        repository.upsert_work_item(item).await?;
        emit_execution_event(
            event_callback,
            "execution.verification",
            format!("Refiner job {} passed verification", refiner_job_id),
            Some(item),
            Some(&execution),
            Some("success"),
            verification.clone(),
        );
        if config.delivery.auto_advance {
            if let Some(next_stage) = current_stage.next() {
                item.delivery_stage = next_stage;
                item.rollout_strategy = RolloutStrategy::default_for_stage(next_stage);
                item.status = WorkStatus::Planned;
                item.execution_approved = false;
                item.finished_at = None;
                item.notes.push(format!(
                    "{} Refiner job {} validated {} and advanced the work item to {}",
                    crate::models::now_utc().to_rfc3339(),
                    refiner_job_id,
                    current_stage.as_str(),
                    next_stage.as_str()
                ));
                repository.upsert_work_execution(&execution).await?;
                repository.upsert_work_item(item).await?;
                emit_execution_event(
                    event_callback,
                    "execution.stage_promoted",
                    format!(
                        "Refiner job {} promoted {} to {}",
                        refiner_job_id,
                        current_stage.as_str(),
                        next_stage.as_str()
                    ),
                    Some(item),
                    Some(&execution),
                    Some("success"),
                    json!({
                        "completed_stage": current_stage.as_str(),
                        "next_stage": next_stage.as_str(),
                        "validated_stages": item
                            .validated_stages
                            .iter()
                            .map(|stage| stage.as_str())
                            .collect::<Vec<_>>(),
                        "rollout_strategy": item.rollout_strategy.as_str(),
                    }),
                );
            } else {
                item.status = WorkStatus::Success;
                item.progress_pct = 100;
                item.finished_at = Some(crate::models::now_utc());
                item.notes.push(format!(
                    "{} Refiner job {} completed the production stage and passed verification",
                    crate::models::now_utc().to_rfc3339(),
                    refiner_job_id
                ));
            }
        } else if current_stage.next().is_some() {
            item.status = WorkStatus::OnHold;
            item.finished_at = None;
            item.notes.push(format!(
                "{} Refiner job {} validated {}. Manual promotion is required for the next stage.",
                crate::models::now_utc().to_rfc3339(),
                refiner_job_id,
                current_stage.as_str()
            ));
        } else {
            item.status = WorkStatus::Success;
            item.progress_pct = 100;
            item.finished_at = Some(crate::models::now_utc());
            item.notes.push(format!(
                "{} Refiner job {} completed the production stage and passed verification",
                crate::models::now_utc().to_rfc3339(),
                refiner_job_id
            ));
        }
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
        item.finished_at = Some(crate::models::now_utc());
        item.notes.push(format!(
            "{} Refiner job {} failed verification: {}",
            crate::models::now_utc().to_rfc3339(),
            refiner_job_id,
            failure_reason
        ));
        repository.upsert_work_execution(&execution).await?;
        repository.upsert_work_item(item).await?;
        emit_execution_event(
            event_callback,
            "execution.verification",
            format!("Refiner job {} failed verification", refiner_job_id),
            Some(item),
            Some(&execution),
            Some("failure"),
            verification.clone(),
        );
    }

    item.touch_execution(execution.id, execution.policy.clone());
    repository.upsert_work_execution(&execution).await?;
    repository.upsert_work_item(item).await?;
    Ok(execution)
}

fn safety_contract(
    config: &ConductorConfig,
    policy: &crate::models::PolicyEvaluation,
    item: &WorkItem,
) -> Value {
    json!({
        "required": !policy.sensitive_targets.is_empty(),
        "targets": policy.sensitive_targets,
        "delivery_stage": item.delivery_stage.as_str(),
        "rollout_strategy": item.rollout_strategy.as_str(),
        "health_check_window_seconds": config.safety.health_check_window_seconds,
        "health_check_interval_seconds": config.safety.health_check_interval_seconds,
        "rollback": {
            "automatic": true,
            "max_attempts": config.safety.max_rollback_attempts,
            "trigger_conditions": [
                "post-rollout readiness degradation",
                "verification failure after a produced commit",
                "missing or failed protected-target health window"
            ],
            "procedure": "refiner_revert_exact_commit_and_verify"
        }
    })
}

async fn capture_safety_probe(
    config: &ConductorConfig,
    service: Option<&ServiceSnapshot>,
) -> Value {
    let Some(service) = service else {
        return json!({
            "passed": false,
            "status": "missing",
            "reason": "service snapshot is unavailable"
        });
    };
    let started = Instant::now();
    let timeout = config.safety.health_check_timeout_seconds.max(1);
    let client = match build_http_client(timeout) {
        Ok(client) => client,
        Err(error) => {
            return json!({
                "passed": false,
                "status": "client_error",
                "reason": error.to_string(),
                "service": service.service_key,
            });
        }
    };
    match probe_service(&client, config, service).await {
        Ok(probe) => {
            let passed =
                probe.health == ServiceHealth::Healthy && health_payload_is_healthy(&probe.metrics);
            json!({
                "passed": passed,
                "status": if passed { "healthy" } else { "degraded" },
                "service": service.service_key,
                "endpoint": probe.endpoint,
                "summary": probe.summary,
                "health": probe.health.as_str(),
                "metrics": probe.metrics,
                "latency_ms": started.elapsed().as_millis(),
            })
        }
        Err(error) => json!({
            "passed": false,
            "status": "unreachable",
            "service": service.service_key,
            "reason": error.to_string(),
            "latency_ms": started.elapsed().as_millis(),
        }),
    }
}

fn health_payload_is_healthy(metrics: &Value) -> bool {
    let health = metrics.get("health").unwrap_or(metrics);
    if health.get("ok").and_then(Value::as_bool) == Some(false)
        || health.get("healthy").and_then(Value::as_bool) == Some(false)
        || health.get("ready").and_then(Value::as_bool) == Some(false)
    {
        return false;
    }
    for key in ["status", "state"] {
        if let Some(value) = health.get(key).and_then(Value::as_str) {
            let value = value.to_ascii_lowercase();
            if [
                "failed",
                "failure",
                "error",
                "unhealthy",
                "degraded",
                "unready",
            ]
            .iter()
            .any(|candidate| value.contains(candidate))
            {
                return false;
            }
        }
    }
    true
}

fn apply_safety_baseline_gate(policy: &mut crate::models::PolicyEvaluation, baseline: &Value) {
    if policy.sensitive_targets.is_empty()
        || baseline.get("passed").and_then(Value::as_bool) == Some(true)
    {
        return;
    }
    let reason = "protected target readiness baseline failed; rollout is blocked".to_string();
    if !policy.reasons.contains(&reason) {
        policy.reasons.push(reason);
    }
    policy.verdict = PolicyVerdict::Blocked;
    policy.risk_level = "critical".to_string();
}

async fn run_safety_health_window(
    config: &ConductorConfig,
    service: Option<&ServiceSnapshot>,
) -> Value {
    let Some(service) = service else {
        return json!({
            "required": true,
            "passed": false,
            "status": "missing",
            "reason": "service snapshot is unavailable"
        });
    };
    let started = Instant::now();
    let window = Duration::from_secs(config.safety.health_check_window_seconds.max(1));
    let interval = Duration::from_secs(config.safety.health_check_interval_seconds.max(1));
    let mut samples = Vec::new();
    loop {
        let sample = capture_safety_probe(config, Some(service)).await;
        let passed = sample.get("passed").and_then(Value::as_bool) == Some(true);
        samples.push(sample);
        if !passed || started.elapsed() >= window {
            break;
        }
        let remaining = window.saturating_sub(started.elapsed());
        tokio::time::sleep(interval.min(remaining)).await;
    }
    let passed = samples
        .iter()
        .all(|sample| sample.get("passed").and_then(Value::as_bool) == Some(true));
    json!({
        "required": true,
        "passed": passed,
        "status": if passed { "healthy" } else { "degraded" },
        "window_seconds": started.elapsed().as_secs(),
        "sample_count": samples.len(),
        "samples": samples,
    })
}

fn extract_commit_sha(detail: &Value) -> Option<String> {
    for candidate in [
        detail.pointer("/repo_info/commit_sha"),
        detail.pointer("/repo_info/github_actions/expected_sha"),
        detail.pointer("/repo_info/github_actions/head_sha"),
        detail.pointer("/vcs/commit_sha"),
    ] {
        let Some(value) = candidate.and_then(Value::as_str) else {
            continue;
        };
        let value = value.trim();
        if value.len() >= 7
            && value.len() <= 64
            && value.chars().all(|character| character.is_ascii_hexdigit())
        {
            return Some(value.to_string());
        }
    }
    None
}

fn protected_rollout_evidence(item: &WorkItem, request_payload: &Value, terminal: &Value) -> Value {
    let action_text = format!("{} {} {}", item.title, item.summary, item.plan).to_ascii_lowercase();
    let deployment_requested = request_payload
        .get("delivery_run")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || ["deploy", "rollout", "release", "restart", "upgrade"]
            .iter()
            .any(|keyword| action_text.contains(keyword));
    if !deployment_requested {
        return json!({
            "required": false,
            "passed": true,
            "reason": "no deployment operation was requested"
        });
    }
    let staged = terminal
        .get("stages")
        .and_then(Value::as_array)
        .map(|stages| {
            stages.iter().any(|stage| {
                let name = stage
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let kind = stage
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let status = stage
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                (name.contains("deploy")
                    || name.contains("rollout")
                    || kind.contains("deploy")
                    || kind.contains("rollout"))
                    && matches!(status.as_str(), "completed" | "ok" | "success")
            })
        })
        .unwrap_or(false);
    json!({
        "required": true,
        "passed": staged,
        "reason": if staged { "successful staged rollout evidence recorded" } else { "no successful staged rollout stage recorded" }
    })
}

async fn automatic_rollback(
    client: &Client,
    config: &ConductorConfig,
    base_url: &str,
    item: &WorkItem,
    execution: &WorkExecution,
    service: Option<&ServiceSnapshot>,
    terminal: &Value,
    commit_sha: &str,
    degraded_health: &Value,
) -> Value {
    let repo_url = execution
        .request_payload
        .get("repo_url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let branch = execution
        .request_payload
        .get("work_branch")
        .and_then(Value::as_str)
        .or_else(|| {
            execution
                .request_payload
                .get("repo_branch")
                .and_then(Value::as_str)
        })
        .unwrap_or("main");
    if repo_url.trim().is_empty() {
        return json!({
            "attempted": false,
            "succeeded": false,
            "reason": "rollback requires repository context"
        });
    }
    let owner = github_repository_coordinate(repo_url)
        .map(|(owner, _)| owner)
        .unwrap_or_default();
    let requirements_text = format!(
        "AUTOMATIC ROLLBACK for Conductor execution {}. Restore the last known-good state by reverting the exact commit {} on branch {} using the rollback_commit control. Do not rewrite history. Run the repository-native tests and required checks, push the revert commit to the same branch, and verify the configured GitHub Actions workflow for the resulting commit. Treat any failed check as a rollback failure. This rollback was triggered because the protected-target post-rollout health evidence was: {}.",
        execution.id, commit_sha, branch, degraded_health
    );
    let mut payload = json!({
        "workflow": config.execution.refiner_workflow,
        "project_name": format!("{} automatic rollback", item.title),
        "repo_url": repo_url,
        // Clone the delivered branch itself so the exact commit is present in
        // the shallow workspace used by Refiner's deterministic revert.
        "repo_branch": branch,
        "work_branch": branch,
        "fork_org": owner,
        "skip_fork": true,
        "rollback_commit": commit_sha,
        // Refiner must start from the exact delivered tip.  A concurrent
        // rollback or branch writer must be rejected instead of reverting a
        // different tree and later being reported as a valid recovery.
        "rollback_expected_head": commit_sha,
        "rollback_of_execution_id": execution.id,
        "rollback_reason": "protected-target degradation or verification failure",
        "requirements_text": requirements_text,
        "project_run": true,
        "github_actions_required": true,
        "github_actions_timeout_sec": config.execution.job_timeout_seconds,
        "token_scope": config.execution.token_scope,
        "llm_provider": config.execution.llm_provider,
        "llm_model": config.execution.llm_model,
        "codingagent": config.execution.coding_agent,
        "commit_message": format!("Rollback protected execution {}", execution.id),
    });
    if let Some(project_root) = execution.request_payload.get("project_root") {
        payload["project_root"] = project_root.clone();
    }

    let submit = match post_refiner_json(client, config, base_url, "/api/jobs", &payload).await {
        Ok(value) => value,
        Err(error) => {
            return json!({
                "attempted": true,
                "succeeded": false,
                "reason": format!("rollback submission failed: {}", error),
                "commit_sha": commit_sha,
            });
        }
    };
    let Some(job_id) = submit
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| submit.get("job_id").and_then(Value::as_str))
    else {
        return json!({
            "attempted": true,
            "succeeded": false,
            "reason": "rollback submission did not return a job id",
            "commit_sha": commit_sha,
        });
    };
    let detail = match poll_rollback_job(client, config, base_url, job_id).await {
        Ok(value) => value,
        Err(error) => {
            return json!({
                "attempted": true,
                "succeeded": false,
                "job_id": job_id,
                "reason": error.to_string(),
                "commit_sha": commit_sha,
            });
        }
    };
    let completed = detail
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("completed"));
    let actions_passed = detail
        .pointer("/repo_info/github_actions/succeeded")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let verification = verify_refiner_result(&detail);
    let verification_passed = verification.get("passed").and_then(Value::as_bool) == Some(true);
    // A successful revert commit and CI run are not sufficient proof that the
    // running service recovered. Probe the same discovered endpoint after the
    // rollback job so a stale process/container cannot be reported as safely
    // restored.
    let recovery_health = if completed && actions_passed && verification_passed {
        run_safety_health_window(config, service).await
    } else {
        json!({
            "required": true,
            "passed": false,
            "status": "not_checked",
            "reason": "rollback code or verification gates did not pass"
        })
    };
    let recovery_health_passed =
        recovery_health.get("passed").and_then(Value::as_bool) == Some(true);
    let succeeded = completed && actions_passed && verification_passed && recovery_health_passed;
    json!({
        "attempted": true,
        "succeeded": succeeded,
        "job_id": job_id,
        "commit_sha": commit_sha,
        "job_status": detail.get("status"),
        "github_actions_passed": actions_passed,
        "verification": verification,
        "recovery_health": recovery_health,
        "reason": if succeeded { "rollback completed and was verified" } else { "rollback job did not pass all recovery gates, including recovery readiness" },
        "terminal": terminal.get("status"),
    })
}

async fn poll_rollback_job(
    client: &Client,
    config: &ConductorConfig,
    base_url: &str,
    job_id: &str,
) -> Result<Value> {
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(config.execution.job_timeout_seconds.max(30));
    let poll_interval = Duration::from_secs(config.execution.poll_interval_seconds.max(1));
    loop {
        let detail = get_refiner_json(
            client,
            config,
            base_url,
            format!("/api/jobs/{}", job_id).as_str(),
        )
        .await
        .with_context(|| format!("failed to poll rollback Refiner job {}", job_id))?;
        let status = detail
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if matches!(status, "completed" | "failed" | "cancelled" | "stopped") {
            return Ok(detail);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!("rollback Refiner job {} timed out", job_id));
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn preview_work_item_execution(
    repository: &dyn ConductorRepository,
    config: &ConductorConfig,
    mut item: WorkItem,
    force_schedule: bool,
    event_callback: Option<&ExecutionEventCallback>,
) -> Result<WorkExecution> {
    if force_schedule && !matches!(item.status, WorkStatus::Scheduled) {
        item.status = WorkStatus::Scheduled;
    }

    let work_items = repository.list_work_items().await?;
    let dependency_blockers = dependency_blockers(&item, &work_items);
    if !dependency_blockers.is_empty() {
        let policy = json!({
            "verdict": "blocked",
            "risk_level": "dependency_graph",
            "reasons": dependency_blockers,
        });
        let mut execution = WorkExecution::new(
            item.id,
            item.target_service.clone(),
            item.delivery_stage,
            item.rollout_strategy,
        );
        execution.policy = policy.clone();
        execution.error = Some("dry-run preview blocked by dependency graph".to_string());
        execution.verification = json!({
            "passed": false,
            "mode": "dry_run",
            "reasons": dependency_blockers,
        });
        execution.mark_status(ExecutionStatus::Blocked);
        item.status = WorkStatus::OnHold;
        item.touch_execution(execution.id, policy);
        repository.upsert_work_execution(&execution).await?;
        repository.upsert_work_item(&item).await?;
        return Ok(execution);
    }

    let services = repository.list_service_snapshots().await?;
    let repositories = repository.list_repository_snapshots().await?;
    let target_service = item.target_service.as_deref().and_then(|target| {
        services
            .iter()
            .find(|service| service.service_key == target)
    });
    let mut policy = evaluate_work_item(config, &item, target_service);
    // A dry run never reaches Refiner or a deployment target.  Preserve the
    // safety findings in the preview, but allow the preview itself to be
    // generated so operators can see exactly which gates would be required.
    if !policy.sensitive_targets.is_empty() && matches!(policy.verdict, PolicyVerdict::Blocked) {
        policy.verdict = PolicyVerdict::Allowed;
        policy.reasons.push(
            "dry-run only: protected-target rollout gates are previewed, not executed".to_string(),
        );
    }
    let mut execution = WorkExecution::new(
        item.id,
        item.target_service.clone(),
        item.delivery_stage,
        item.rollout_strategy,
    );
    execution.policy = policy_evaluation_to_value(&policy);
    item.touch_execution(execution.id, execution.policy.clone());

    if !matches!(policy.verdict, PolicyVerdict::Allowed) {
        execution.error = Some(policy.reasons.join("; "));
        execution.verification = json!({
            "passed": false,
            "mode": "dry_run",
            "reasons": policy.reasons,
        });
        execution.mark_status(ExecutionStatus::Blocked);
        item.status = WorkStatus::OnHold;
        repository.upsert_work_execution(&execution).await?;
        repository.upsert_work_item(&item).await?;
        return Ok(execution);
    }

    let prompt = build_refiner_prompt(config, &item, target_service, &policy);
    let preview_payload =
        build_job_payload(config, &item, target_service, &repositories, &json!({}))?;
    let independent_validation = preview_independent_validation(
        &config.validation,
        target_service,
        &policy.required_verifications,
    );
    execution.request_payload = preview_payload.clone();
    execution.latest_payload = json!({
        "mode": "dry_run",
        "prompt": prompt,
        "job_payload": preview_payload,
        "independent_validation": independent_validation,
    });
    execution.verification = json!({
        "passed": false,
        "mode": "dry_run",
        "reason": "execution.dry_run is enabled",
        "independent_validation": execution.latest_payload.get("independent_validation").cloned().unwrap_or_else(|| json!({})),
    });
    execution.mark_status(ExecutionStatus::Cancelled);
    item.notes.push(format!(
        "{} dry-run preview generated; no external execution was started",
        crate::models::now_utc().to_rfc3339()
    ));
    repository.upsert_work_execution(&execution).await?;
    repository.upsert_work_item(&item).await?;
    emit_execution_event(
        event_callback,
        "execution.dry_run",
        format!("dry-run preview generated for {}", item.title),
        Some(&item),
        Some(&execution),
        Some(execution.status.as_str()),
        execution.latest_payload.clone(),
    );
    Ok(execution)
}

async fn finalize_execution_failure(
    repository: &dyn ConductorRepository,
    item: &mut WorkItem,
    execution: &mut WorkExecution,
    event_callback: Option<&ExecutionEventCallback>,
    message: impl Into<String>,
) -> Result<WorkExecution> {
    let message = message.into();
    execution.error = Some(message.clone());
    execution.mark_status(ExecutionStatus::Failure);
    item.status = WorkStatus::Failure;
    if item.started_at.is_none() {
        item.started_at = Some(crate::models::now_utc());
    }
    item.finished_at = Some(crate::models::now_utc());
    item.notes.push(format!(
        "{} execution failed: {}",
        crate::models::now_utc().to_rfc3339(),
        message
    ));
    item.touch_execution(execution.id, execution.policy.clone());
    repository.upsert_work_execution(execution).await?;
    repository.upsert_work_item(item).await?;
    emit_execution_event(
        event_callback,
        "execution.failed",
        message,
        Some(item),
        Some(execution),
        Some("failure"),
        json!({
            "policy": execution.policy.clone(),
            "target_service": item.target_service.clone(),
        }),
    );
    Ok(execution.clone())
}

fn execution_instance_id(config: &ConductorConfig) -> &str {
    config
        .execution
        .instance_id
        .as_deref()
        .unwrap_or("conductor")
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

async fn refiner_base_url(
    client: &Client,
    config: &ConductorConfig,
    services: &[ServiceSnapshot],
) -> Result<String> {
    crate::integrations::refiner::select_live_base_url(
        client,
        &config.integrations.refiner,
        services
            .iter()
            .find(|service| service.service_key == "refiner"),
    )
    .await?
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
    config: &ConductorConfig,
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
    let rollout_context = deployment_automation_context(config, service)
        .map(|context| format!("\nAnsible rollout context: {}", context))
        .unwrap_or_default();
    let validated_stages = if work_item.validated_stages.is_empty() {
        "none".to_string()
    } else {
        work_item
            .validated_stages
            .iter()
            .map(|stage| stage.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "Work item: {title}\nTarget: {target}\nDelivery stage: {delivery_stage}\nValidated stages: {validated_stages}\nRollout strategy: {rollout_strategy}\nSummary: {summary}\nPlan JSON: {plan}\nRepository context: {repo}{rollout_context}\n\nExecution method: inspect the current repository/runtime/job state first; state observed evidence and uncertainty; then choose the smallest safe operation. The operation may be a code change, an Ansible/job update, a progress-monitoring action, or a deliberate no-change decision when evidence is insufficient. Do not infer success from a healthy process alone.\n\nRequired plan sections: evidence and hypothesis, affected files/services/jobs, ordered implementation or operational steps, tests and live checks, rollback/recovery, and the acceptance signal that proves the gap is closed. Keep changes scoped, resilient, and secure; avoid destructive commands; leave unrelated files untouched; do not bypass staged delivery gates.\nRequired verification: {verification}\nReturn a structured project-solver plan. Include non-empty `summary` and `steps` (or a non-empty `requirements_text`), and a `job_payload` object only when it is valid. Never return prose-only, empty, or schema-echo output.",
        title = work_item.title,
        target = service_name,
        delivery_stage = work_item.delivery_stage.as_str(),
        validated_stages = validated_stages,
        rollout_strategy = work_item.rollout_strategy.as_str(),
        summary = work_item.summary,
        plan = work_item.plan,
        repo = repo_context,
        rollout_context = rollout_context,
        verification = verification,
    )
}

fn validate_refiner_plan_response(response: &Value) -> Result<()> {
    let object = response
        .as_object()
        .ok_or_else(|| anyhow!("response must be a JSON object"))?;
    if let Some(job_payload) = object.get("job_payload")
        && !job_payload.is_object()
    {
        return Err(anyhow!("job_payload must be a JSON object"));
    }

    let has_summary = object
        .get("summary")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    let has_steps = object
        .get("steps")
        .and_then(Value::as_array)
        .is_some_and(|steps| {
            !steps.is_empty()
                && steps.iter().all(|step| match step {
                    Value::String(value) => !value.trim().is_empty(),
                    Value::Object(value) => ["title", "action", "description"].iter().any(|key| {
                        value
                            .get(*key)
                            .and_then(Value::as_str)
                            .is_some_and(|text| !text.trim().is_empty())
                    }),
                    _ => false,
                })
        });
    let has_requirements = object
        .get("requirements_text")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    if !(has_requirements || (has_summary && has_steps)) {
        return Err(anyhow!(
            "response needs non-empty requirements_text or summary plus ordered steps"
        ));
    }
    Ok(())
}

fn build_job_payload(
    config: &ConductorConfig,
    work_item: &WorkItem,
    service: Option<&ServiceSnapshot>,
    repositories: &[RepositorySnapshot],
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
        json!(requirements_text(config, plan_response, work_item, service)),
    );
    payload.insert("project_run".to_string(), json!(true));
    // Conductor supplies an authoritative, evidence-backed requirements
    // document.  Do not let Refiner add unrelated repository-wide sequence
    // or helper-module findings to this execution; those belong to a
    // separately planned discovery task and can otherwise exhaust the local
    // solver context before the requested work starts.
    payload.insert("requirements_only".to_string(), json!(true));
    payload.insert("dry_run".to_string(), json!(config.execution.dry_run));
    payload.insert(
        "token_scope".to_string(),
        json!(config.execution.token_scope.clone()),
    );
    payload.insert(
        "commit_message".to_string(),
        json!(format!("conductor: {}", work_item.title)),
    );
    payload.insert(
        "delivery_stage".to_string(),
        json!(work_item.delivery_stage.as_str()),
    );
    payload.insert(
        "validated_stages".to_string(),
        json!(
            work_item
                .validated_stages
                .iter()
                .map(|stage| stage.as_str())
                .collect::<Vec<_>>()
        ),
    );
    payload.insert(
        "rollout_strategy".to_string(),
        json!(work_item.rollout_strategy.as_str()),
    );
    payload.insert(
        "rollout".to_string(),
        json!({
            "strategy": work_item.rollout_strategy.as_str(),
            "canary_percentage": if matches!(work_item.rollout_strategy, RolloutStrategy::Canary) {
                config.delivery.production_canary_percentage
            } else {
                0
            },
            "staggered_cutover": matches!(work_item.rollout_strategy, RolloutStrategy::RedGreen),
        }),
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
        // Prefer an explicit planner value, then enrich the service snapshot
        // from the repository inventory.  The latter is essential when the
        // Ansible checkout is a copied/mounted tree without a Git remote.
        if !payload.contains_key("repo_url") {
            if let Some((repo_url, branches)) =
                rollout_repository_context(Some(service), repositories)
            {
                payload.insert("repo_url".to_string(), json!(repo_url));
                if !payload.contains_key("repo_branch") {
                    if let Some(repo_branch) = branches.first() {
                        payload.insert("repo_branch".to_string(), json!(repo_branch));
                    }
                }
            }
        }
        if !payload.contains_key("repo_branch") {
            if let Some(repo_branch) = service.repo_branch.as_deref() {
                payload.insert("repo_branch".to_string(), json!(repo_branch));
            }
        }
        payload.insert(
            "work_branch".to_string(),
            json!(work_branch_name(work_item)),
        );

        // A repository-delivery work item targets the owning repository.  Do
        // not ask Refiner to fork a repository into the same GitHub
        // organisation: GitHub rejects that operation (typically as 404),
        // even when the caller has valid repository permissions.  External
        // repositories retain Refiner's normal fork behaviour.
        let is_repository_delivery = work_item
            .plan
            .get("action")
            .and_then(Value::as_str)
            .is_some_and(|action| action.eq_ignore_ascii_case("repository_delivery"));
        if is_repository_delivery {
            if let Some(repo_url) = payload.get("repo_url").and_then(Value::as_str) {
                if let Some((owner, _repository)) = github_repository_coordinate(repo_url) {
                    let configured_owner = config.discovery.github.owner.trim();
                    if !configured_owner.is_empty() && owner.eq_ignore_ascii_case(configured_owner)
                    {
                        payload.insert("fork_org".to_string(), json!(owner));
                        payload.insert("skip_fork".to_string(), json!(true));
                    }
                }
            }
        }
    }
    if let Some(deployment_automation) = deployment_automation_context(config, service) {
        payload.insert("deployment_automation".to_string(), deployment_automation);
    }
    Ok(Value::Object(payload))
}

fn requirements_text(
    config: &ConductorConfig,
    plan_response: &Value,
    work_item: &WorkItem,
    service: Option<&ServiceSnapshot>,
) -> String {
    let planner_guidance = plan_response
        .get("requirements_text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty());

    let target = service
        .map(|service| service.display_name.as_str())
        .unwrap_or("target service");
    let rollout_requirement = if let Some(context) = deployment_automation_context(config, service)
    {
        format!(
            "\n- REQ-013: When runtime rollout or restart work is needed, use the available Ansible automation context: {}.",
            context
        )
    } else {
        String::new()
    };
    let protected_rollout_requirements = if service.is_some() {
        "\n- REQ-009: Capture a fresh protected-target readiness baseline before any change.\n- REQ-010: Use the selected canary or red-green rollout strategy and verify the post-rollout health window.\n- REQ-011: Automatically revert the exact produced commit without rewriting history if health or verification degrades.\n- REQ-012: Verify rollback readiness and recovery before finalising the delivery."
            .to_string()
    } else {
        String::new()
    };
    let authoritative_delivery_constraints =
        format_authoritative_delivery_constraints(&work_item.plan);
    let authoritative = format!(
        "Overview: Improve {target} through the Conductor execution loop.\n\nDelivery Context:\n- Current stage: {delivery_stage}\n- Validated stages: {validated_stages}\n- Rollout strategy: {rollout_strategy}\n\nRequirements Register:\n- REQ-001: Inspect and record current repository, runtime, or job evidence before selecting an operation.\n- REQ-002: Implement only the scoped change, job update, or progress-monitoring action supported by that evidence.\n- REQ-003: Preserve secure, resilient behaviour and avoid destructive commands.\n- REQ-004: Update or add tests covering the changed path, or provide the relevant live operational check.\n- REQ-005: Run verification commands and report the outcome.\n- REQ-006: Leave unrelated files untouched.\n- REQ-007: Record rollback/recovery steps and the acceptance signal proving the gap is closed.\n- REQ-008: Preserve staged progression and rollout governance metadata.{protected_rollout_requirements}{rollout_requirement}\n\nWork Item Summary:\n{summary}\n\nAuthoritative delivery constraints (mandatory; implement and verify these, do not merely describe them):\n{authoritative_delivery_constraints}\n\nPlan JSON:\n{plan}",
        target = target,
        delivery_stage = work_item.delivery_stage.as_str(),
        validated_stages = if work_item.validated_stages.is_empty() {
            "none".to_string()
        } else {
            work_item
                .validated_stages
                .iter()
                .map(|stage| stage.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        },
        rollout_strategy = work_item.rollout_strategy.as_str(),
        protected_rollout_requirements = protected_rollout_requirements,
        rollout_requirement = rollout_requirement,
        authoritative_delivery_constraints = authoritative_delivery_constraints,
        summary = work_item.summary,
        plan = work_item.plan,
    );

    // The execution-plan endpoint is advisory: a model can return a valid
    // looking but semantically stale or read-only plan for a work item that
    // explicitly requires a repository change.  Keep the governed work item
    // as the source of truth and retain planner guidance only as context.
    planner_guidance
        .map(|guidance| {
            format!(
                "{authoritative}\n\nPlanner guidance (advisory; it must not weaken or contradict the authoritative work-item requirements):\n{guidance}\n"
            )
        })
        .unwrap_or(authoritative)
}

fn format_authoritative_delivery_constraints(plan: &Value) -> String {
    let Some(plan) = plan.as_object() else {
        return "- No structured delivery constraints were supplied; follow the work-item summary exactly.".to_string();
    };

    let mut lines = Vec::new();
    if let Some(files) = plan.get("required_files").and_then(Value::as_array) {
        for path in files
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|path| !path.is_empty())
        {
            lines.push(format!(
                "- Required file: {} (must exist in the delivered repository).",
                path
            ));
        }
    }
    if let Some(commands) = plan.get("required_verifications").and_then(Value::as_array) {
        for command in commands
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|command| !command.is_empty())
        {
            lines.push(format!(
                "- Required verification command: {} (must be run and pass).",
                command
            ));
        }
    }
    if lines.is_empty() {
        lines.push("- No structured delivery constraints were supplied; follow the work-item summary exactly.".to_string());
    }
    lines.join("\n")
}

fn deployment_automation_context(
    config: &ConductorConfig,
    service: Option<&ServiceSnapshot>,
) -> Option<Value> {
    let ansible_root = &config.discovery.ansible_root;
    // A container image creates the configured workspace paths before the
    // read-only Ansible checkout is mounted.  Treat such an empty placeholder
    // as unavailable; otherwise execution plans advertise automation context
    // that cannot actually be used.
    if ansible_root.as_os_str().is_empty()
        || !ansible_root.join("ansible.cfg").is_file()
        || !ansible_root.join("inventory").join("hosts.ini").is_file()
    {
        return None;
    }
    let repo_root = ansible_root.parent().unwrap_or(ansible_root);
    Some(json!({
        "repo_root": repo_root.display().to_string(),
        "ansible_root": ansible_root.display().to_string(),
        "config_path": ansible_root.join("ansible.cfg").display().to_string(),
        "inventory_path": ansible_root.join("inventory").join("hosts.ini").display().to_string(),
        "roles_path": ansible_root.join("roles").display().to_string(),
        "secrets_root": ansible_root.join(".secrets").display().to_string(),
        "playbooks": service.map(|service| service.playbooks.clone()).unwrap_or_default(),
        "host_targets": service.map(|service| service.host_targets.clone()).unwrap_or_default(),
        "hosts": service.map(|service| service.hosts.clone()).unwrap_or_default(),
    }))
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

fn policy_value_with_github_actions(
    policy: &crate::models::PolicyEvaluation,
    github_actions: Option<&GitHubActionsEvidence>,
) -> Value {
    let mut value = policy_evaluation_to_value(policy);
    if let Some(evidence) = github_actions {
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "github_actions".to_string(),
                serde_json::to_value(evidence).unwrap_or_else(|_| json!({})),
            );
        }
    }
    value
}

fn apply_github_actions_gate(
    policy: &mut crate::models::PolicyEvaluation,
    github_actions: Option<&GitHubActionsEvidence>,
) {
    let Some(evidence) = github_actions else {
        return;
    };
    if evidence.succeeded {
        return;
    }

    policy
        .reasons
        .retain(|reason| reason != "policy checks passed");
    if !policy.reasons.contains(&evidence.reason) {
        policy.reasons.push(evidence.reason.clone());
    }
    policy.verdict = PolicyVerdict::Blocked;
    policy.risk_level = "critical".to_string();
}

async fn production_github_actions_evidence(
    config: &ConductorConfig,
    item: &WorkItem,
    service: Option<&ServiceSnapshot>,
    repositories: &[RepositorySnapshot],
) -> Option<GitHubActionsEvidence> {
    if !config
        .policy
        .require_successful_github_actions_for_production
        || !matches!(item.delivery_stage, DeliveryStage::Production)
    {
        return None;
    }

    let workflow_file = config.policy.github_actions_workflow_file.clone();
    let context = rollout_repository_context(service, repositories);
    let Some((repo_url, branches)) = context else {
        return Some(GitHubActionsEvidence {
            workflow_file,
            owner: None,
            repository: None,
            branch: None,
            succeeded: false,
            reason: "production rollout requires repository and branch context for GitHub Actions verification".to_string(),
            run: None,
        });
    };

    let Some((owner, repository)) = github_repository_coordinate(&repo_url) else {
        return Some(GitHubActionsEvidence {
            workflow_file,
            owner: None,
            repository: None,
            branch: branches.first().cloned(),
            succeeded: false,
            reason: format!(
                "production rollout requires a GitHub repository URL, but '{}' could not be parsed",
                repo_url
            ),
            run: None,
        });
    };

    let client = match build_http_client(config.discovery.github.timeout_seconds.max(1)) {
        Ok(client) => client,
        Err(error) => {
            return Some(GitHubActionsEvidence {
                workflow_file,
                owner: Some(owner),
                repository: Some(repository),
                branch: branches.first().cloned(),
                succeeded: false,
                reason: format!("unable to create GitHub Actions client: {}", error),
                run: None,
            });
        }
    };

    let branch_checks = future::join_all(branches.iter().cloned().map(|branch| async {
        let result = fetch_latest_github_actions_evidence(
            &client,
            config,
            &owner,
            &repository,
            &branch,
            &workflow_file,
        )
        .await;
        (branch, result)
    }))
    .await;

    let mut last_missing = None;
    for (branch, result) in branch_checks {
        match result {
            Ok(evidence) if evidence.succeeded => return Some(evidence),
            Ok(evidence) if evidence.run.is_some() => return Some(evidence),
            Ok(evidence) => last_missing = Some(evidence),
            Err(error) => {
                return Some(GitHubActionsEvidence {
                    workflow_file: workflow_file.clone(),
                    owner: Some(owner.clone()),
                    repository: Some(repository.clone()),
                    branch: Some(branch),
                    succeeded: false,
                    reason: format!(
                        "unable to verify GitHub Actions workflow {}: {}",
                        workflow_file, error
                    ),
                    run: None,
                });
            }
        }
    }

    last_missing.or_else(|| {
        Some(GitHubActionsEvidence {
            workflow_file,
            owner: Some(owner),
            repository: Some(repository),
            branch: None,
            succeeded: false,
            reason: "production rollout requires a branch to verify GitHub Actions CI".to_string(),
            run: None,
        })
    })
}

fn rollout_repository_context(
    service: Option<&ServiceSnapshot>,
    repositories: &[RepositorySnapshot],
) -> Option<(String, Vec<String>)> {
    let service = service?;
    let matched = service
        .repo_path
        .as_deref()
        .and_then(|repo_path| {
            repositories
                .iter()
                .find(|repository| repository.local_path.as_deref() == Some(repo_path))
        })
        .or_else(|| {
            service.repo_url.as_deref().and_then(|repo_url| {
                repositories
                    .iter()
                    .find(|repository| repository.repo_url.as_deref() == Some(repo_url))
            })
        })
        .or_else(|| {
            repositories
                .iter()
                .find(|repository| repository.linked_services.contains(&service.service_key))
        });

    let repo_url = service
        .repo_url
        .clone()
        .or_else(|| matched.and_then(|repository| repository.repo_url.clone()))?;
    let mut branches = Vec::new();
    for branch in [
        service.repo_branch.clone(),
        matched.and_then(|repository| repository.current_branch.clone()),
        matched.and_then(|repository| repository.default_branch.clone()),
    ]
    .into_iter()
    .flatten()
    {
        let branch = branch.trim();
        if !branch.is_empty() && !branches.iter().any(|candidate| candidate == branch) {
            branches.push(branch.to_string());
        }
    }

    Some((repo_url, branches))
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
        let status = refiner_effective_status(&detail);
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
            // Keep the durable execution state ahead of its event so event
            // persistence cannot race the execution foreign-key insert, and
            // so a restart can resume from the observed Refiner state.
            repository.upsert_work_execution(execution).await?;
            repository.upsert_work_item(item).await?;
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
        } else {
            repository.upsert_work_execution(execution).await?;
            repository.upsert_work_item(item).await?;
        }

        if matches!(
            status.as_str(),
            "completed" | "failed" | "cancelled" | "stopped"
        ) {
            if needs_terminal_commit_refresh(&detail) {
                let mut refreshed = detail;
                for _ in 0..REFINER_TERMINAL_COMMIT_REFRESH_ATTEMPTS {
                    tokio::time::sleep(Duration::from_secs(
                        REFINER_TERMINAL_COMMIT_REFRESH_INTERVAL_SECONDS,
                    ))
                    .await;
                    let Ok(candidate) = get_refiner_json(
                        client,
                        config,
                        base_url,
                        format!("/api/jobs/{}", job_id).as_str(),
                    )
                    .await
                    else {
                        continue;
                    };
                    refreshed = candidate;
                    if extract_commit_sha(&refreshed).is_some() {
                        break;
                    }
                }
                return Ok(refreshed);
            }
            return Ok(detail);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!("Refiner job {} timed out", job_id));
        }
        tokio::time::sleep(poll_interval).await;
    }
}

fn refiner_effective_status(detail: &Value) -> String {
    let status = detail
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_ascii_lowercase();
    if matches!(
        status.as_str(),
        "completed" | "failed" | "cancelled" | "stopped"
    ) {
        return status;
    }

    // Refiner can publish terminal metadata before updating its top-level
    // status. Treat a finished job with a non-zero exit code or failed stage
    // as failed instead of waiting until the Conductor job timeout.
    let finished = detail
        .get("finished_at")
        .is_some_and(|value| value.as_str().is_some_and(|text| !text.trim().is_empty()));
    if finished {
        let exit_failed = detail
            .get("exit_code")
            .and_then(Value::as_i64)
            .is_some_and(|code| code != 0);
        let stage_failed = detail
            .get("stages")
            .and_then(Value::as_array)
            .is_some_and(|stages| {
                stages.iter().any(|stage| {
                    stage
                        .get("status")
                        .and_then(Value::as_str)
                        .is_some_and(|stage_status| {
                            matches!(
                                stage_status.to_ascii_lowercase().as_str(),
                                "failed" | "blocked" | "cancelled"
                            )
                        })
                })
            });
        return if exit_failed || stage_failed {
            "failed".to_string()
        } else {
            "completed".to_string()
        };
    }

    status
}

fn needs_terminal_commit_refresh(detail: &Value) -> bool {
    refiner_effective_status(detail) == "failed"
        && detail.get("repo_info").is_some()
        && extract_commit_sha(detail).is_none()
}

fn verify_refiner_result(detail: &Value) -> Value {
    let status = refiner_effective_status(detail);
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

/// Keep the durable execution timestamp fresh while independent checks run.
/// Validation commands are bounded individually, but a project can have
/// several checks and the aggregate duration may exceed the stale-execution
/// threshold.  Without this heartbeat a healthy, long-running verification is
/// indistinguishable from a crashed Conductor process after a restart.
async fn run_independent_validation_with_heartbeat(
    config: &crate::config::ValidationConfig,
    service: Option<&ServiceSnapshot>,
    required_checks: &[String],
    execution: &mut WorkExecution,
    repository: &dyn ConductorRepository,
) -> Result<crate::validation::IndependentValidationReport> {
    let validation = run_independent_validation(config, service, required_checks);
    tokio::pin!(validation);
    let heartbeat_seconds = (config.timeout_seconds.max(1).min(60)).min(30).max(5);
    let heartbeat_interval = Duration::from_secs(heartbeat_seconds);

    loop {
        tokio::select! {
            report = &mut validation => return Ok(report),
            _ = tokio::time::sleep(heartbeat_interval) => {
                execution.updated_at = crate::models::now_utc();
                repository.upsert_work_execution(execution).await?;
            }
        }
    }
}

fn merge_independent_validation(
    verification: &mut Value,
    report: &crate::validation::IndependentValidationReport,
    require_success: bool,
) {
    let payload = serde_json::to_value(report).unwrap_or_else(|_| json!({}));
    let failure_reasons = failure_reasons(report);
    if let Some(object) = verification.as_object_mut() {
        object.insert("independent_validation".to_string(), payload);
        object.insert(
            "independent_validation_enforced".to_string(),
            json!(require_success),
        );
        if require_success && !report.passed {
            object.insert("passed".to_string(), json!(false));
            let reasons = object
                .entry("reasons".to_string())
                .or_insert_with(|| json!([]));
            if let Some(items) = reasons.as_array_mut() {
                if failure_reasons.is_empty() {
                    items.push(json!(report.summary.clone()));
                } else {
                    for reason in failure_reasons {
                        items.push(json!(reason));
                    }
                }
            }
        }
    }
}

fn attach_independent_validation_payload(payload: Value, report: Value) -> Value {
    match payload {
        Value::Object(mut object) => {
            object.insert("independent_validation".to_string(), report);
            Value::Object(object)
        }
        other => json!({
            "refiner_result": other,
            "independent_validation": report,
        }),
    }
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
        models::{DeliveryStage, NewWorkItem, ServiceHealth},
        storage::memory::MemoryRepository,
        validation::{IndependentValidationReport, ValidationCommandResult},
    };
    use axum::{
        Json, Router,
        routing::{get, post},
    };
    use chrono::Duration as ChronoDuration;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    fn sample_service() -> ServiceSnapshot {
        ServiceSnapshot {
            service_key: "conductor".to_string(),
            display_name: "Conductor".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "continuum_tenant_conductor".to_string(),
            playbooks: vec![],
            host_targets: vec![],
            hosts: vec![],
            namespace: None,
            service_name: None,
            deployment_environment: Some(DeliveryStage::Production),
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

    async fn spawn_mock_github(conclusion: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/repos/neuralmimicry/conductor/actions/workflows/ci.yml/runs",
            get(move || async move {
                Json(json!({
                    "workflow_runs": [
                        {
                            "id": 100,
                            "name": "CI",
                            "status": "completed",
                            "conclusion": conclusion,
                            "html_url": "https://github.com/neuralmimicry/conductor/actions/runs/100",
                            "run_number": 12,
                            "head_sha": "abc123",
                            "event": "push",
                            "updated_at": "2026-04-30T10:00:00Z"
                        }
                    ]
                }))
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind github");
        let addr = listener.local_addr().expect("github addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve github");
        });
        (format!("http://{}", addr), handle)
    }

    async fn spawn_mock_refiner_execution_surface() -> (String, tokio::task::JoinHandle<()>) {
        async fn health() -> Json<Value> {
            Json(json!({"status": "ok"}))
        }

        async fn execution_plan() -> Json<Value> {
            Json(json!({
                "summary": "Stabilise the release gate.",
                "steps": [
                    "Audit the failing verification seam.",
                    "Extract the delivery helper.",
                    "Update the targeted tests.",
                    "Run the focused verification suite."
                ],
                "requirements_text": "Overview: Stabilise the release gate.\n\nRequirements Register:\n- REQ-001: Fix the failing verification path.\n- REQ-002: Preserve rollout metadata.\n- REQ-003: Add or update targeted tests.\n- REQ-004: Document the execution boundary.\n",
                "project_name": "Release Stabiliser",
                "job_payload": {
                    "project_iterations": 4,
                    "project_max_steps": 12,
                    "source": "execution"
                }
            }))
        }

        async fn estimate() -> Json<Value> {
            Json(json!({"estimated_tokens": 512}))
        }

        async fn submit_job() -> Json<Value> {
            Json(json!({"job_id": "job-123"}))
        }

        async fn job_detail(_: axum::extract::Path<String>) -> Json<Value> {
            Json(json!({
                "status": "completed",
                "stages": [
                    {"name": "plan", "status": "completed"},
                    {"name": "apply", "status": "completed"},
                    {"name": "verify", "status": "completed"}
                ]
            }))
        }

        let app = Router::new()
            .route("/api/health", get(health))
            .route(REFINER_EXECUTION_PLAN_PATH, post(execution_plan))
            .route("/api/jobs/estimate", post(estimate))
            .route("/api/jobs", post(submit_job))
            .route("/api/jobs/{job_id}", get(job_detail));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind refiner execution surface");
        let addr = listener.local_addr().expect("refiner execution addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve refiner execution surface");
        });
        (format!("http://{}", addr), handle)
    }

    async fn spawn_mock_rollback_surface(
        successful: bool,
    ) -> (String, tokio::task::JoinHandle<()>) {
        async fn submit_job() -> Json<Value> {
            Json(json!({"job_id": "rollback-job"}))
        }
        let status = if successful { "completed" } else { "failed" };
        let actions_succeeded = successful;
        let app = Router::new()
            .route("/api/jobs", post(submit_job))
            .route(
                "/api/jobs/{job_id}",
                get(move |_: axum::extract::Path<String>| async move {
                    Json(json!({
                        "status": status,
                        "stages": [],
                        "repo_info": {
                            "github_actions": {"succeeded": actions_succeeded}
                        }
                    }))
                }),
            )
            .route("/healthz", get(|| async { Json(json!({"status": "ok"})) }));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind rollback surface");
        let addr = listener.local_addr().expect("rollback addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve rollback surface");
        });
        (format!("http://{}", addr), handle)
    }

    fn rollback_service(base_url: &str) -> ServiceSnapshot {
        let mut service = sample_service();
        service.service_key = "rollback-test".to_string();
        service.display_name = "Rollback test".to_string();
        service.internal_url = Some(base_url.to_string());
        service.repo_url = None;
        service
    }

    #[test]
    fn job_payload_inherits_repo_context_and_strict_policy() {
        let config = ConductorConfig::default();
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("stabilize:conductor".to_string()),
            title: "Stabilize Conductor".to_string(),
            summary: "Improve executor reliability".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
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
            &[],
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
        assert_eq!(payload.get("requirements_only"), Some(&json!(true)));
    }

    #[test]
    fn job_payload_recovers_repo_context_from_inventory_when_service_remote_is_missing() {
        let config = ConductorConfig::default();
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("inventory-context:conductor".to_string()),
            title: "Validate repository delivery".to_string(),
            summary: "Ensure the change is pushed to the owning repository".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: None,
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "repository_delivery"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        let mut service = sample_service();
        service.repo_url = None;
        service.repo_branch = None;
        let repository = RepositorySnapshot {
            repo_key: "neuralmimicry/conductor".to_string(),
            name: "conductor".to_string(),
            owner: Some("neuralmimicry".to_string()),
            repo_url: Some("git@github.com:neuralmimicry/conductor.git".to_string()),
            local_path: Some("/tmp/conductor".to_string()),
            default_branch: Some("main".to_string()),
            current_branch: None,
            language: Some("Rust".to_string()),
            frameworks: vec![],
            build_systems: vec!["cargo".to_string()],
            package_managers: vec![],
            runtime_type: Some("service".to_string()),
            deployment_type: Some("kubernetes".to_string()),
            purpose: None,
            criticality: "critical".to_string(),
            visibility: Some("public".to_string()),
            archived: false,
            linked_services: vec!["conductor".to_string()],
            dependencies: vec![],
            capabilities: vec![],
            inventory_sources: vec!["github_api".to_string()],
            metadata: json!({}),
            discovered_at: crate::models::now_utc(),
            updated_at: crate::models::now_utc(),
        };

        let payload = build_job_payload(
            &config,
            &item,
            Some(&service),
            &[repository],
            &json!({"job_payload": {"workflow": "project_solver"}}),
        )
        .expect("payload");

        assert_eq!(
            payload.get("repo_url").and_then(Value::as_str),
            Some("git@github.com:neuralmimicry/conductor.git")
        );
        assert_eq!(
            payload.get("repo_branch").and_then(Value::as_str),
            Some("main")
        );
        assert_eq!(
            payload.get("fork_org").and_then(Value::as_str),
            Some("neuralmimicry")
        );
        assert_eq!(
            payload.get("skip_fork").and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn refiner_plan_quality_gate_accepts_structured_plan() {
        validate_refiner_plan_response(&json!({
            "summary": "Inspect and repair the readiness gate",
            "steps": ["Inspect the current probe", "Add a focused test"],
            "job_payload": {"project_iterations": 2}
        }))
        .expect("structured plan");
        validate_refiner_plan_response(&json!({
            "summary": "Inspect and repair the readiness gate",
            "steps": [{"action": "Inspect the current probe"}],
        }))
        .expect("object-step plan");
    }

    #[test]
    fn refiner_plan_quality_gate_rejects_empty_or_malformed_plan() {
        assert!(validate_refiner_plan_response(&json!({})).is_err());
        assert!(
            validate_refiner_plan_response(&json!({
                "summary": "plan",
                "steps": ["inspect"],
                "job_payload": []
            }))
            .is_err()
        );
    }

    #[test]
    fn requirements_text_makes_structured_delivery_constraints_authoritative() {
        let constraints = format_authoritative_delivery_constraints(&json!({
            "required_files": ["docs/refiner-supported-languages.md"],
            "required_verifications": ["cargo fmt --check", "cargo test"],
        }));
        assert!(constraints.contains(
            "Required file: docs/refiner-supported-languages.md (must exist in the delivered repository)."
        ));
        assert!(
            constraints
                .contains("Required verification command: cargo test (must be run and pass).")
        );
    }

    #[test]
    fn safety_health_payload_rejects_explicit_degradation() {
        assert!(health_payload_is_healthy(
            &json!({"health": {"status": "ok"}})
        ));
        assert!(!health_payload_is_healthy(
            &json!({"health": {"status": "degraded"}})
        ));
        assert!(!health_payload_is_healthy(
            &json!({"health": {"ok": false}})
        ));
    }

    #[test]
    fn commit_extraction_accepts_only_safe_hexadecimal_ids() {
        assert_eq!(
            extract_commit_sha(&json!({"repo_info": {"commit_sha": "abcdef1234567"}})),
            Some("abcdef1234567".to_string())
        );
        assert_eq!(
            extract_commit_sha(&json!({"repo_info": {"commit_sha": "--danger"}})),
            None
        );
    }

    #[tokio::test]
    async fn automatic_rollback_reports_successful_recovery() {
        let (base_url, handle) = spawn_mock_rollback_surface(true).await;
        let mut config = ConductorConfig::default();
        config.safety.health_check_window_seconds = 1;
        config.safety.health_check_interval_seconds = 1;
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("rollback:success".to_string()),
            title: "Protected change".to_string(),
            summary: "Rollback test".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: Some(DeliveryStage::Development),
            validated_stages: vec![],
            rollout_strategy: Some(RolloutStrategy::Canary),
            status: Some(WorkStatus::InOperation),
            priority: Some(90),
            progress_pct: Some(50),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "change"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        let mut execution = WorkExecution::new(
            item.id,
            item.target_service.clone(),
            item.delivery_stage,
            item.rollout_strategy,
        );
        execution.request_payload = json!({
            "repo_url": "https://github.com/neuralmimicry/conductor",
            "repo_branch": "main",
            "work_branch": "conductor/rollback-success"
        });
        let service = rollback_service(&base_url);
        let result = automatic_rollback(
            &build_http_client(2).expect("client"),
            &config,
            &base_url,
            &item,
            &execution,
            Some(&service),
            &json!({"status": "completed"}),
            "abcdef1234567",
            &json!({"passed": false}),
        )
        .await;
        assert_eq!(result["attempted"], json!(true));
        assert_eq!(result["succeeded"], json!(true));
        assert_eq!(result["job_id"], json!("rollback-job"));
        assert_eq!(result["recovery_health"]["passed"], json!(true));
        handle.abort();
    }

    #[tokio::test]
    async fn automatic_rollback_reports_failed_recovery_without_claiming_success() {
        let (base_url, handle) = spawn_mock_rollback_surface(false).await;
        let mut config = ConductorConfig::default();
        config.safety.health_check_window_seconds = 1;
        config.safety.health_check_interval_seconds = 1;
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("rollback:failure".to_string()),
            title: "Protected change".to_string(),
            summary: "Rollback failure test".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: Some(DeliveryStage::Development),
            validated_stages: vec![],
            rollout_strategy: Some(RolloutStrategy::Canary),
            status: Some(WorkStatus::InOperation),
            priority: Some(90),
            progress_pct: Some(50),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "change"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        let mut execution = WorkExecution::new(
            item.id,
            item.target_service.clone(),
            item.delivery_stage,
            item.rollout_strategy,
        );
        execution.request_payload = json!({
            "repo_url": "https://github.com/neuralmimicry/conductor",
            "repo_branch": "main",
            "work_branch": "conductor/rollback-failure"
        });
        let service = rollback_service(&base_url);
        let result = automatic_rollback(
            &build_http_client(2).expect("client"),
            &config,
            &base_url,
            &item,
            &execution,
            Some(&service),
            &json!({"status": "completed"}),
            "abcdef1234567",
            &json!({"passed": false}),
        )
        .await;
        assert_eq!(result["attempted"], json!(true));
        assert_eq!(result["succeeded"], json!(false));
        handle.abort();
    }

    #[test]
    fn job_payload_uses_configured_canary_percentage() {
        let mut config = ConductorConfig::default();
        config.delivery.production_canary_percentage = 25;
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("promote:conductor".to_string()),
            title: "Promote Conductor".to_string(),
            summary: "Promote the release candidate to production".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: Some(DeliveryStage::Production),
            validated_stages: vec![
                DeliveryStage::Development,
                DeliveryStage::Testing,
                DeliveryStage::Integration,
                DeliveryStage::IntegrationTesting,
                DeliveryStage::Uat,
            ],
            rollout_strategy: Some(RolloutStrategy::Canary),
            status: Some(WorkStatus::Scheduled),
            priority: Some(90),
            progress_pct: Some(90),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "promote_release"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        let payload = build_job_payload(
            &config,
            &item,
            Some(&sample_service()),
            &[],
            &json!({"job_payload": {"workflow": "project_solver"}}),
        )
        .expect("payload");
        assert_eq!(
            payload
                .get("rollout")
                .and_then(Value::as_object)
                .and_then(|rollout| rollout.get("canary_percentage"))
                .and_then(Value::as_u64),
            Some(25)
        );
    }

    #[test]
    fn job_payload_includes_deployment_automation_context() {
        let temp = tempdir().expect("tempdir");
        let ansible_root = temp.path().join("ansible");
        std::fs::create_dir_all(ansible_root.join("inventory")).expect("inventory");
        std::fs::create_dir_all(ansible_root.join("roles")).expect("roles");
        std::fs::write(ansible_root.join("ansible.cfg"), "[defaults]\n").expect("cfg");
        std::fs::write(
            ansible_root.join("inventory").join("hosts.ini"),
            "[all]\nlocalhost ansible_connection=local\n",
        )
        .expect("hosts");

        let mut config = ConductorConfig::default();
        config.discovery.ansible_root = ansible_root.clone();
        let mut service = sample_service();
        service.playbooks = vec!["continuum_tenant_conductor_site.yml".to_string()];
        service.host_targets = vec!["rk1".to_string()];
        service.hosts = vec!["spirit".to_string()];

        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("rollout:conductor".to_string()),
            title: "Roll out Conductor".to_string(),
            summary: "Apply a controlled runtime restart".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: Some(DeliveryStage::IntegrationTesting),
            validated_stages: vec![DeliveryStage::Development, DeliveryStage::Testing],
            rollout_strategy: Some(RolloutStrategy::Canary),
            status: Some(WorkStatus::Scheduled),
            priority: Some(80),
            progress_pct: Some(50),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "rollout_restart"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });

        let payload =
            build_job_payload(&config, &item, Some(&service), &[], &json!({})).expect("payload");
        let deployment = payload
            .get("deployment_automation")
            .and_then(Value::as_object)
            .expect("deployment automation");
        assert_eq!(
            deployment.get("ansible_root").and_then(Value::as_str),
            Some(ansible_root.to_string_lossy().as_ref())
        );
        assert_eq!(
            deployment
                .get("playbooks")
                .and_then(Value::as_array)
                .and_then(|playbooks| playbooks.first())
                .and_then(Value::as_str),
            Some("continuum_tenant_conductor_site.yml")
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

    #[test]
    fn terminal_refiner_metadata_overrides_stale_running_status() {
        let detail = json!({
            "status": "running",
            "progress": 100,
            "finished_at": "01/09/2026 13:26:24",
            "exit_code": 2,
            "stages": [
                {"name": "execute", "status": "failed"},
                {"name": "finalize", "status": "failed"}
            ]
        });

        assert_eq!(refiner_effective_status(&detail), "failed");
        let report = verify_refiner_result(&detail);
        assert_eq!(
            report.get("job_status").and_then(Value::as_str),
            Some("failed")
        );
        assert_eq!(report.get("passed").and_then(Value::as_bool), Some(false));
    }

    #[test]
    fn failed_terminal_job_refreshes_when_commit_metadata_is_late() {
        let detail = json!({
            "status": "running",
            "finished_at": "01/09/2026 13:26:24",
            "exit_code": 2,
            "repo_info": {"branch": "conductor/change"},
        });

        assert!(needs_terminal_commit_refresh(&detail));
        assert!(!needs_terminal_commit_refresh(&json!({
            "status": "running",
            "finished_at": "01/09/2026 13:26:24",
            "exit_code": 2,
            "repo_info": {"commit_sha": "abcdef1234567"},
        })));
    }

    #[test]
    fn finished_refiner_metadata_without_failures_is_completed() {
        let detail = json!({
            "status": "running",
            "progress": 100,
            "finished_at": "01/09/2026 13:26:24",
            "exit_code": 0,
            "stages": [{"name": "execute", "status": "completed"}]
        });

        assert_eq!(refiner_effective_status(&detail), "completed");
    }

    #[test]
    fn merge_independent_validation_marks_verification_failed_when_enforced() {
        let mut verification = json!({
            "passed": true,
            "reasons": [],
        });
        let report = IndependentValidationReport {
            enabled: true,
            enforced: true,
            passed: false,
            completeness: "full".to_string(),
            repo_path: Some("/tmp/conductor".to_string()),
            required_checks: vec!["cargo test".to_string()],
            planned_commands: vec!["cargo test".to_string()],
            commands: vec![ValidationCommandResult {
                command: "cargo test".to_string(),
                status: "failed".to_string(),
                exit_code: Some(1),
                duration_ms: 42,
                stdout_excerpt: None,
                stderr_excerpt: Some("failure".to_string()),
                reason: Some("cargo test exited with status 1".to_string()),
            }],
            summary: "independent validation failed".to_string(),
        };

        merge_independent_validation(&mut verification, &report, true);

        assert_eq!(
            verification.get("passed").and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            verification
                .get("independent_validation_enforced")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(
            verification
                .get("reasons")
                .and_then(Value::as_array)
                .is_some_and(|items| items
                    .iter()
                    .any(|item| item == "cargo test exited with status 1"))
        );
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
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
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
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
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
    async fn execute_specific_work_item_uses_execution_plan_surface() {
        let (refiner_url, refiner_handle) = spawn_mock_refiner_execution_surface().await;
        let mut config = ConductorConfig::default();
        config.integrations.refiner.base_url = Some(refiner_url);
        config.integrations.refiner.timeout_seconds = 2;
        config.validation.enabled = false;

        let repository = Arc::new(MemoryRepository::new());
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("stabilize:release-gate".to_string()),
            title: "Stabilise release gate".to_string(),
            summary: "Fix the failing verification seam".to_string(),
            target_service: None,
            delivery_stage: Some(DeliveryStage::Development),
            validated_stages: vec![],
            rollout_strategy: Some(RolloutStrategy::Canary),
            status: Some(WorkStatus::Scheduled),
            priority: Some(90),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "stabilize_release_gate"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        repository
            .upsert_work_item(&item)
            .await
            .expect("insert work item");

        let execution =
            execute_specific_work_item(repository.as_ref(), &config, item.id, false, None)
                .await
                .expect("execution");

        assert_eq!(execution.status, ExecutionStatus::Success);
        assert_eq!(
            execution
                .request_payload
                .get("requirements_text")
                .and_then(Value::as_str),
            Some(
                "Overview: Improve target service through the Conductor execution loop.\n\nDelivery Context:\n- Current stage: development\n- Validated stages: none\n- Rollout strategy: canary\n\nRequirements Register:\n- REQ-001: Inspect and record current repository, runtime, or job evidence before selecting an operation.\n- REQ-002: Implement only the scoped change, job update, or progress-monitoring action supported by that evidence.\n- REQ-003: Preserve secure, resilient behaviour and avoid destructive commands.\n- REQ-004: Update or add tests covering the changed path, or provide the relevant live operational check.\n- REQ-005: Run verification commands and report the outcome.\n- REQ-006: Leave unrelated files untouched.\n- REQ-007: Record rollback/recovery steps and the acceptance signal proving the gap is closed.\n- REQ-008: Preserve staged progression and rollout governance metadata.\n\nWork Item Summary:\nFix the failing verification seam\n\nAuthoritative delivery constraints (mandatory; implement and verify these, do not merely describe them):\n- No structured delivery constraints were supplied; follow the work-item summary exactly.\n\nPlan JSON:\n{\"action\":\"stabilize_release_gate\"}\n\nPlanner guidance (advisory; it must not weaken or contradict the authoritative work-item requirements):\nOverview: Stabilise the release gate.\n\nRequirements Register:\n- REQ-001: Fix the failing verification path.\n- REQ-002: Preserve rollout metadata.\n- REQ-003: Add or update targeted tests.\n- REQ-004: Document the execution boundary.\n"
            )
        );
        assert_eq!(
            execution
                .request_payload
                .get("source")
                .and_then(Value::as_str),
            Some("execution")
        );
        assert_eq!(
            execution
                .request_payload
                .get("project_iterations")
                .and_then(Value::as_u64),
            Some(4)
        );

        refiner_handle.abort();
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
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
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
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
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

    #[tokio::test]
    async fn execution_cycle_respects_future_schedule() {
        let config = ConductorConfig::default();
        let repository = Arc::new(MemoryRepository::new());

        let scheduled = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("future:scheduled".to_string()),
            title: "Future".to_string(),
            summary: "Do not run yet".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: Some(WorkStatus::Scheduled),
            priority: Some(80),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "wait"}),
            depends_on: vec![],
            source: None,
            scheduled_for: Some(crate::models::now_utc() + ChronoDuration::hours(2)),
        });
        repository
            .upsert_work_item(&scheduled)
            .await
            .expect("scheduled item");

        let executed = run_execution_cycle(repository.as_ref(), &config, None)
            .await
            .expect("execution cycle");

        assert!(executed.is_empty());
        let stored = repository
            .get_work_item(scheduled.id)
            .await
            .expect("stored")
            .expect("item");
        assert_eq!(stored.status, WorkStatus::Scheduled);
        assert!(stored.claim_token.is_none());
    }

    #[tokio::test]
    async fn execution_cycle_reconciles_stale_execution_and_releases_capacity() {
        let repository = Arc::new(MemoryRepository::new());
        let mut config = ConductorConfig::default();
        config.execution.enabled = true;
        config.execution.dry_run = false;
        config.execution.claim_ttl_seconds = 60;

        let mut item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("stale:execution".to_string()),
            title: "Stale execution".to_string(),
            summary: "Recover an execution left behind by a stopped process".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: Some(WorkStatus::InOperation),
            priority: Some(50),
            progress_pct: Some(20),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        let mut execution = WorkExecution::new(
            item.id,
            item.target_service.clone(),
            item.delivery_stage,
            item.rollout_strategy,
        );
        let stale_at = crate::models::now_utc() - ChronoDuration::seconds(120);
        execution.started_at = stale_at;
        execution.updated_at = stale_at;
        item.last_execution_id = Some(execution.id);

        repository.upsert_work_item(&item).await.expect("work item");
        repository
            .upsert_work_execution(&execution)
            .await
            .expect("execution");

        let executed = run_execution_cycle(repository.as_ref(), &config, None)
            .await
            .expect("execution cycle");

        assert!(executed.is_empty());
        let recovered = repository
            .list_work_executions(10)
            .await
            .expect("execution list")
            .pop()
            .expect("recovered execution");
        assert_eq!(recovered.status, ExecutionStatus::Failure);
        assert!(
            recovered
                .error
                .as_deref()
                .is_some_and(|error| error.contains("without a heartbeat"))
        );
        let recovered_item = repository
            .get_work_item(item.id)
            .await
            .expect("work item lookup")
            .expect("recovered work item");
        assert_eq!(recovered_item.status, WorkStatus::Failure);
    }

    #[tokio::test]
    async fn manual_execution_returns_dry_run_preview_when_enabled() {
        let mut config = ConductorConfig::default();
        config.execution.dry_run = true;
        let repository = Arc::new(MemoryRepository::new());

        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("dryrun:preview".to_string()),
            title: "Preview".to_string(),
            summary: "Generate a dry-run payload".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: Some(WorkStatus::Scheduled),
            priority: Some(70),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "preview"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        repository.upsert_work_item(&item).await.expect("item");
        repository
            .replace_service_snapshots(&[sample_service()])
            .await
            .expect("services");

        let execution =
            execute_specific_work_item(repository.as_ref(), &config, item.id, false, None)
                .await
                .expect("dry-run execution");

        assert_eq!(execution.status, ExecutionStatus::Cancelled);
        assert_eq!(
            execution.latest_payload.get("mode").and_then(Value::as_str),
            Some("dry_run")
        );
        assert_eq!(
            execution
                .request_payload
                .get("dry_run")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(
            execution
                .latest_payload
                .get("independent_validation")
                .is_some()
        );
        assert!(
            execution
                .verification
                .get("independent_validation")
                .is_some()
        );
    }

    #[tokio::test]
    async fn execution_cycle_skips_when_emergency_stop_is_enabled() {
        let mut config = ConductorConfig::default();
        config.execution.emergency_stop = true;
        let repository = Arc::new(MemoryRepository::new());

        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("emergency:stop".to_string()),
            title: "Stop".to_string(),
            summary: "Should not run".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: Some(WorkStatus::Scheduled),
            priority: Some(90),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "noop"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        repository.upsert_work_item(&item).await.expect("item");

        let executed = run_execution_cycle(repository.as_ref(), &config, None)
            .await
            .expect("execution cycle");

        assert!(executed.is_empty());
        assert!(
            repository
                .list_work_executions(10)
                .await
                .expect("executions")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn production_execution_blocks_when_github_actions_ci_failed() {
        let (github_base_url, github_handle) = spawn_mock_github("failure").await;
        let mut config = ConductorConfig::default();
        config.discovery.github.api_base_url = github_base_url;

        let repository = Arc::new(MemoryRepository::new());
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("promote:conductor".to_string()),
            title: "Promote Conductor".to_string(),
            summary: "Promote the candidate to production".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: Some(DeliveryStage::Production),
            validated_stages: vec![DeliveryStage::Uat],
            rollout_strategy: Some(RolloutStrategy::Canary),
            status: Some(WorkStatus::Scheduled),
            priority: Some(95),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "promote_release"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });

        repository.upsert_work_item(&item).await.expect("item");
        repository
            .replace_service_snapshots(&[sample_service()])
            .await
            .expect("services");

        let execution =
            execute_specific_work_item(repository.as_ref(), &config, item.id, false, None)
                .await
                .expect("execution");

        assert_eq!(execution.status, ExecutionStatus::Blocked);
        assert!(
            execution
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("GitHub Actions workflow ci.yml concluded with failure")
        );
        assert_eq!(
            execution
                .policy
                .get("github_actions")
                .and_then(|value| value.get("succeeded"))
                .and_then(Value::as_bool),
            Some(false)
        );

        github_handle.abort();
    }
}
