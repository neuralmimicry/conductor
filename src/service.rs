use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use axum::http::HeaderMap;
use chrono::Duration as ChronoDuration;
use reqwest::Client;
use serde_json::json;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    config::ConductorConfig,
    discovery::discover_and_probe,
    error::ApiError,
    executor::{ExecutionEventCallback, execute_specific_work_item, run_execution_cycle},
    models::{
        ConductorEvent, DashboardSummary, DeliveryStage, DiscoveryRun, DoraMetricsSummary,
        FindingEvidence, FindingProvenance, FindingRecord, ImprovementCycle, RepositorySnapshot,
        ServiceSnapshot, WorkExecution, WorkItem, WorkItemTraceability, WorkStatus, now_utc,
        topology_from_services,
    },
    planner::run_planning_cycle,
    repository::ConductorRepository,
    trends::collect_metric_samples,
};

#[derive(Clone)]
pub struct ConductorService {
    pub config: Arc<ConductorConfig>,
    pub repository: Arc<dyn ConductorRepository>,
    pub http: Client,
    pub events: broadcast::Sender<ConductorEvent>,
}

impl ConductorService {
    pub fn new(
        config: ConductorConfig,
        repository: Arc<dyn ConductorRepository>,
        http: Client,
    ) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            config: Arc::new(config),
            repository,
            http,
            events,
        }
    }

    pub fn publish_event(&self, event: ConductorEvent) {
        let _ = self.events.send(event.clone());
        let repository = self.repository.clone();
        tokio::spawn(async move {
            if let Err(error) = repository.insert_conductor_event(&event).await {
                tracing::warn!(error = %error, "failed to persist conductor event");
            }
        });
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<ConductorEvent> {
        self.events.subscribe()
    }

    fn event_callback(&self) -> ExecutionEventCallback {
        let sender = self.events.clone();
        let repository = self.repository.clone();
        Arc::new(move |event: ConductorEvent| {
            let _ = sender.send(event.clone());
            let repository = repository.clone();
            tokio::spawn(async move {
                if let Err(error) = repository.insert_conductor_event(&event).await {
                    tracing::warn!(error = %error, "failed to persist conductor event");
                }
            });
        })
    }

    pub fn authorize_read(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        self.authorize_read_with_token(headers, None)
    }

    pub fn authorize_read_with_token(
        &self,
        headers: &HeaderMap,
        token_override: Option<&str>,
    ) -> Result<(), ApiError> {
        if self.config.security.allow_dashboard_without_token {
            return Ok(());
        }
        self.authorize_admin_with_token(headers, token_override)
    }

    pub fn authorize_admin(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        self.authorize_admin_with_token(headers, None)
    }

    pub fn authorize_admin_with_token(
        &self,
        headers: &HeaderMap,
        token_override: Option<&str>,
    ) -> Result<(), ApiError> {
        let Some(expected) = self
            .config
            .security
            .admin_token
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(());
        };
        let supplied = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim);
        let supplied = supplied.or_else(|| token_override.map(str::trim));
        match supplied {
            Some(token) if token == expected => Ok(()),
            _ => Err(ApiError::unauthorized()),
        }
    }

    pub async fn run_discovery_cycle(&self) -> Result<DiscoveryRun> {
        let discovery = discover_and_probe(&self.config, &self.http).await?;
        let metric_samples = collect_metric_samples(discovery.run.id, &discovery.services);
        self.repository
            .replace_service_snapshots(&discovery.services)
            .await?;
        self.repository
            .replace_repository_snapshots(&discovery.repositories)
            .await?;
        self.repository.insert_discovery_run(&discovery.run).await?;
        if !metric_samples.is_empty() {
            self.repository
                .insert_service_metric_samples(&metric_samples)
                .await?;
        }
        let mut event = ConductorEvent::new(
            "discovery.completed",
            format!(
                "discovery cycle completed with {} services and {} repositories",
                discovery.services.len(),
                discovery.repositories.len()
            ),
            json!({
                "discovery_run_id": discovery.run.id.to_string(),
                "services_count": discovery.services.len(),
                "repositories_count": discovery.repositories.len(),
                "status": discovery.run.status.as_str(),
            }),
        );
        event.status = Some(discovery.run.status.as_str().to_string());
        self.publish_event(event);
        Ok(discovery.run)
    }

    pub async fn run_planning_cycle(&self) -> Result<ImprovementCycle> {
        let cycle = run_planning_cycle(self.repository.as_ref(), &self.http, &self.config).await?;
        let mut event = ConductorEvent::new(
            "planning.completed",
            cycle.summary.clone(),
            json!({
                "improvement_cycle_id": cycle.id.to_string(),
                "status": cycle.status.as_str(),
                "source_services": cycle.source_services.clone(),
            }),
        );
        event.status = Some(cycle.status.as_str().to_string());
        self.publish_event(event);
        Ok(cycle)
    }

    pub async fn services(&self) -> Result<Vec<crate::models::ServiceSnapshot>> {
        self.repository.list_service_snapshots().await
    }

    pub async fn repositories(&self) -> Result<Vec<RepositorySnapshot>> {
        self.repository.list_repository_snapshots().await
    }

    pub async fn findings(&self) -> Result<Vec<FindingRecord>> {
        self.repository.list_findings().await
    }

    pub async fn finding(&self, id: Uuid) -> Result<Option<FindingRecord>> {
        self.repository.get_finding(id).await
    }

    pub async fn finding_evidence(&self, id: Uuid) -> Result<Vec<FindingEvidence>> {
        self.repository.list_finding_evidence(id).await
    }

    pub async fn finding_provenance(&self, id: Uuid) -> Result<Vec<FindingProvenance>> {
        self.repository.list_finding_provenance(id).await
    }

    pub async fn work_items(&self) -> Result<Vec<WorkItem>> {
        self.repository.list_work_items().await
    }

    pub async fn work_item(&self, id: Uuid) -> Result<Option<WorkItem>> {
        self.repository.get_work_item(id).await
    }

    pub async fn executions(&self, limit: usize) -> Result<Vec<WorkExecution>> {
        self.repository.list_work_executions(limit).await
    }

    pub async fn events(&self, limit: usize) -> Result<Vec<ConductorEvent>> {
        self.repository.list_conductor_events(limit).await
    }

    pub async fn work_item_executions(
        &self,
        work_item_id: Uuid,
        limit: usize,
    ) -> Result<Vec<WorkExecution>> {
        self.repository
            .list_work_executions_for_item(work_item_id, limit)
            .await
    }

    pub async fn work_item_traceability(
        &self,
        work_item_id: Uuid,
    ) -> Result<Option<WorkItemTraceability>> {
        let Some(work_item) = self.repository.get_work_item(work_item_id).await? else {
            return Ok(None);
        };
        let services = self.repository.list_service_snapshots().await?;
        let repositories = self.repository.list_repository_snapshots().await?;
        let findings = self.repository.list_findings().await?;
        let mut executions = self
            .repository
            .list_work_executions_for_item(work_item_id, 100)
            .await?;
        executions.sort_by_key(execution_sort_key);
        executions.reverse();

        let finding = traceability_finding(&work_item, &findings);
        let evidence = if let Some(finding) = &finding {
            self.repository.list_finding_evidence(finding.id).await?
        } else {
            Vec::new()
        };
        let provenance = if let Some(finding) = &finding {
            self.repository.list_finding_provenance(finding.id).await?
        } else {
            Vec::new()
        };
        let target_service = traceability_service(&work_item, &services);
        let target_repository =
            traceability_repository(&finding, target_service.as_ref(), &repositories);
        let latest_execution = executions.first().cloned();
        let latest_verification = latest_execution
            .as_ref()
            .map(|execution| execution.verification.clone())
            .unwrap_or_else(|| json!({}));
        let independent_validation = latest_execution
            .as_ref()
            .and_then(traceability_independent_validation)
            .unwrap_or_else(|| json!({}));

        Ok(Some(WorkItemTraceability {
            work_item,
            finding,
            target_service,
            target_repository,
            evidence,
            provenance,
            executions,
            latest_execution,
            latest_verification,
            independent_validation,
        }))
    }

    pub async fn run_execution_cycle(&self) -> Result<Vec<WorkExecution>> {
        self.publish_event(ConductorEvent::new(
            "execution.cycle.started",
            "execution cycle started",
            json!({}),
        ));
        let callback = self.event_callback();
        let result =
            run_execution_cycle(self.repository.as_ref(), &self.config, Some(&callback)).await;
        match &result {
            Ok(executions) => {
                let mut event = ConductorEvent::new(
                    "execution.cycle.completed",
                    format!(
                        "execution cycle completed with {} execution(s)",
                        executions.len()
                    ),
                    json!({
                        "executions_started": executions.len(),
                    }),
                );
                event.status = Some("success".to_string());
                self.publish_event(event);
            }
            Err(error) => {
                let mut event = ConductorEvent::new(
                    "execution.cycle.failed",
                    format!("execution cycle failed: {}", error),
                    json!({"error": error.to_string()}),
                );
                event.status = Some("failure".to_string());
                self.publish_event(event);
            }
        }
        result
    }

    pub async fn execute_work_item(
        &self,
        work_item_id: Uuid,
        force_schedule: bool,
    ) -> Result<WorkExecution> {
        self.publish_event(ConductorEvent::new(
            "execution.manual.started",
            format!("manual execution requested for work item {}", work_item_id),
            json!({
                "work_item_id": work_item_id.to_string(),
                "force_schedule": force_schedule,
            }),
        ));
        let callback = self.event_callback();
        execute_specific_work_item(
            self.repository.as_ref(),
            &self.config,
            work_item_id,
            force_schedule,
            Some(&callback),
        )
        .await
    }

    pub async fn summary(&self) -> Result<DashboardSummary> {
        let services = self.repository.list_service_snapshots().await?;
        let repositories = self.repository.list_repository_snapshots().await?;
        let findings = self.repository.list_findings().await?;
        let work_items = self.repository.list_work_items().await?;
        let latest_discovery = self
            .repository
            .list_discovery_runs(1)
            .await?
            .into_iter()
            .next();
        let latest_cycle = self
            .repository
            .list_improvement_cycles(1)
            .await?
            .into_iter()
            .next();
        let cycles_total = self.repository.list_improvement_cycles(200).await?.len();
        let executions = self.repository.list_work_executions(1000).await?;

        let mut work_by_status = BTreeMap::new();
        for item in &work_items {
            *work_by_status
                .entry(item.status.as_str().to_string())
                .or_insert(0usize) += 1;
        }

        let mut delivery_stage_totals = BTreeMap::new();
        let mut rollout_strategy_totals = BTreeMap::new();
        for item in &work_items {
            *delivery_stage_totals
                .entry(item.delivery_stage.as_str().to_string())
                .or_insert(0usize) += 1;
            *rollout_strategy_totals
                .entry(item.rollout_strategy.as_str().to_string())
                .or_insert(0usize) += 1;
        }

        let mut findings_by_severity = BTreeMap::new();
        for finding in &findings {
            *findings_by_severity
                .entry(finding.severity.as_str().to_string())
                .or_insert(0usize) += 1;
        }

        let dora_metrics = compute_dora_metrics(
            &work_items,
            &executions,
            self.config.delivery.dora_window_days,
        );

        Ok(DashboardSummary {
            generated_at: now_utc(),
            services_total: services.len(),
            repositories_total: repositories.len(),
            findings_total: findings.len(),
            findings_by_severity,
            services_healthy: services
                .iter()
                .filter(|service| matches!(service.health, crate::models::ServiceHealth::Healthy))
                .count(),
            services_degraded: services
                .iter()
                .filter(|service| matches!(service.health, crate::models::ServiceHealth::Degraded))
                .count(),
            services_unreachable: services
                .iter()
                .filter(|service| {
                    matches!(
                        service.health,
                        crate::models::ServiceHealth::Unreachable
                            | crate::models::ServiceHealth::Missing
                    )
                })
                .count(),
            work_items_total: work_items.len(),
            work_by_status,
            delivery_stage_totals,
            rollout_strategy_totals,
            cycles_total,
            executions_total: executions.len(),
            executions_running: executions
                .iter()
                .filter(|execution| !execution.status.is_terminal())
                .count(),
            approvals_waiting: work_items
                .iter()
                .filter(|item| {
                    !item.execution_approved
                        && matches!(
                            item.status,
                            WorkStatus::Planned | WorkStatus::Scheduled | WorkStatus::OnHold
                        )
                })
                .count(),
            dora_metrics,
            latest_discovery,
            latest_cycle,
        })
    }

    pub async fn topology(&self) -> Result<crate::models::TopologyGraph> {
        let services = self.repository.list_service_snapshots().await?;
        Ok(topology_from_services(&services))
    }
}

fn compute_dora_metrics(
    work_items: &[WorkItem],
    executions: &[WorkExecution],
    window_days: i64,
) -> DoraMetricsSummary {
    let window_days = window_days.max(1);
    let cutoff = now_utc() - ChronoDuration::days(window_days);
    let work_items_by_id = work_items
        .iter()
        .map(|item| (item.id, item))
        .collect::<HashMap<_, _>>();

    let mut production_executions = executions
        .iter()
        .filter(|execution| matches!(execution.delivery_stage, DeliveryStage::Production))
        .filter(|execution| execution.finished_at.unwrap_or(execution.updated_at) >= cutoff)
        .collect::<Vec<_>>();
    production_executions
        .sort_by_key(|execution| execution.finished_at.unwrap_or(execution.updated_at));

    let attempted_production_deployments = production_executions.len();
    let successful_executions = production_executions
        .iter()
        .copied()
        .filter(|execution| matches!(execution.status, crate::models::ExecutionStatus::Success))
        .collect::<Vec<_>>();
    let failed_executions = production_executions
        .iter()
        .copied()
        .filter(|execution| {
            matches!(
                execution.status,
                crate::models::ExecutionStatus::Failure
                    | crate::models::ExecutionStatus::Blocked
                    | crate::models::ExecutionStatus::Cancelled
            )
        })
        .collect::<Vec<_>>();

    let mut lead_times = successful_executions
        .iter()
        .filter_map(|execution| {
            let item = work_items_by_id.get(&execution.work_item_id)?;
            let finished_at = execution.finished_at?;
            Some((finished_at - item.created_at).num_minutes() as f64 / 60.0)
        })
        .collect::<Vec<_>>();
    lead_times.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));

    let mut mttr_values = Vec::new();
    for failed in &failed_executions {
        let failed_at = failed.finished_at.unwrap_or(failed.updated_at);
        if let Some(recovery) = successful_executions.iter().find(|candidate| {
            let candidate_finished = candidate.finished_at.unwrap_or(candidate.updated_at);
            candidate_finished > failed_at && same_recovery_scope(candidate, failed)
        }) {
            let recovered_at = recovery.finished_at.unwrap_or(recovery.updated_at);
            mttr_values.push((recovered_at - failed_at).num_minutes() as f64 / 60.0);
        }
    }

    DoraMetricsSummary {
        window_days,
        attempted_production_deployments,
        successful_production_deployments: successful_executions.len(),
        deployment_frequency_per_day: successful_executions.len() as f64 / window_days as f64,
        lead_time_hours_average: average(&lead_times),
        lead_time_hours_median: median(&lead_times),
        change_failure_rate_pct: if attempted_production_deployments == 0 {
            0.0
        } else {
            (failed_executions.len() as f64 / attempted_production_deployments as f64) * 100.0
        },
        mean_time_to_restore_hours: average(&mttr_values),
    }
}

fn same_recovery_scope(candidate: &WorkExecution, failed: &WorkExecution) -> bool {
    if candidate.work_item_id == failed.work_item_id {
        return true;
    }
    match (
        candidate.target_service.as_deref(),
        failed.target_service.as_deref(),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

fn average(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        Some((values[mid - 1] + values[mid]) / 2.0)
    } else {
        Some(values[mid])
    }
}

pub fn spawn_background_loops(service: ConductorService) {
    let discovery_interval =
        Duration::from_secs(service.config.discovery.refresh_interval_seconds.max(30));
    let planning_interval =
        Duration::from_secs(service.config.planning.refresh_interval_seconds.max(30));
    let execution_interval =
        Duration::from_secs(service.config.execution.refresh_interval_seconds.max(5));

    let discovery_service = service.clone();
    tokio::spawn(async move {
        if let Err(error) = discovery_service.run_discovery_cycle().await {
            tracing::warn!(error = %error, "initial discovery cycle failed");
            let mut event = ConductorEvent::new(
                "discovery.failed",
                format!("initial discovery cycle failed: {}", error),
                json!({"error": error.to_string()}),
            );
            event.status = Some("failure".to_string());
            discovery_service.publish_event(event);
        }
        let mut ticker = tokio::time::interval(discovery_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = discovery_service.run_discovery_cycle().await {
                tracing::warn!(error = %error, "discovery cycle failed");
                let mut event = ConductorEvent::new(
                    "discovery.failed",
                    format!("discovery cycle failed: {}", error),
                    json!({"error": error.to_string()}),
                );
                event.status = Some("failure".to_string());
                discovery_service.publish_event(event);
            }
        }
    });

    let planning_service = service.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        if let Err(error) = planning_service.run_planning_cycle().await {
            tracing::warn!(error = %error, "initial planning cycle failed");
            let mut event = ConductorEvent::new(
                "planning.failed",
                format!("initial planning cycle failed: {}", error),
                json!({"error": error.to_string()}),
            );
            event.status = Some("failure".to_string());
            planning_service.publish_event(event);
        }
        let mut ticker = tokio::time::interval(planning_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = planning_service.run_planning_cycle().await {
                tracing::warn!(error = %error, "planning cycle failed");
                let mut event = ConductorEvent::new(
                    "planning.failed",
                    format!("planning cycle failed: {}", error),
                    json!({"error": error.to_string()}),
                );
                event.status = Some("failure".to_string());
                planning_service.publish_event(event);
            }
        }
    });

    let execution_service = service.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        if let Err(error) = execution_service.run_execution_cycle().await {
            tracing::warn!(error = %error, "initial execution cycle failed");
            let mut event = ConductorEvent::new(
                "execution.cycle.failed",
                format!("initial execution cycle failed: {}", error),
                json!({"error": error.to_string()}),
            );
            event.status = Some("failure".to_string());
            execution_service.publish_event(event);
        }
        let mut ticker = tokio::time::interval(execution_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = execution_service.run_execution_cycle().await {
                tracing::warn!(error = %error, "execution cycle failed");
                let mut event = ConductorEvent::new(
                    "execution.cycle.failed",
                    format!("execution cycle failed: {}", error),
                    json!({"error": error.to_string()}),
                );
                event.status = Some("failure".to_string());
                execution_service.publish_event(event);
            }
        }
    });
}

fn traceability_finding(work_item: &WorkItem, findings: &[FindingRecord]) -> Option<FindingRecord> {
    let finding_id = work_item
        .plan
        .get("finding_id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok());
    let finding_key = work_item
        .plan
        .get("finding_key")
        .and_then(|value| value.as_str());

    findings
        .iter()
        .find(|finding| {
            finding_id.is_some_and(|id| finding.id == id)
                || finding_key.is_some_and(|key| finding.finding_key == key)
        })
        .cloned()
}

fn traceability_service(
    work_item: &WorkItem,
    services: &[ServiceSnapshot],
) -> Option<ServiceSnapshot> {
    work_item
        .target_service
        .as_deref()
        .and_then(|target| {
            services
                .iter()
                .find(|service| service.service_key == target)
        })
        .cloned()
}

fn traceability_repository(
    finding: &Option<FindingRecord>,
    service: Option<&ServiceSnapshot>,
    repositories: &[RepositorySnapshot],
) -> Option<RepositorySnapshot> {
    if let Some(repository_key) = finding
        .as_ref()
        .and_then(|finding| finding.target_repository.as_deref())
    {
        if let Some(repository) = repositories
            .iter()
            .find(|repository| repository.repo_key == repository_key)
        {
            return Some(repository.clone());
        }
    }

    let Some(service) = service else {
        return None;
    };

    if let Some(repo_path) = service.repo_path.as_deref() {
        if let Some(repository) = repositories
            .iter()
            .find(|repository| repository.local_path.as_deref() == Some(repo_path))
        {
            return Some(repository.clone());
        }
    }

    repositories
        .iter()
        .find(|repository| repository.linked_services.contains(&service.service_key))
        .cloned()
}

fn traceability_independent_validation(execution: &WorkExecution) -> Option<serde_json::Value> {
    execution
        .verification
        .get("independent_validation")
        .cloned()
        .or_else(|| {
            execution
                .latest_payload
                .get("independent_validation")
                .cloned()
        })
}

fn execution_sort_key(execution: &WorkExecution) -> chrono::DateTime<chrono::Utc> {
    execution.finished_at.unwrap_or(execution.updated_at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ExecutionStatus, NewWorkItem, RolloutStrategy};
    use serde_json::json;

    #[test]
    fn dora_metrics_use_production_stage_history() {
        let mut production_item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("promote:gail".to_string()),
            title: "Promote Gail".to_string(),
            summary: "Advance Gail to production".to_string(),
            target_service: Some("gail".to_string()),
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
            priority: Some(80),
            progress_pct: Some(80),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        production_item.created_at = now_utc() - ChronoDuration::hours(48);

        let mut success = WorkExecution::new(
            production_item.id,
            Some("gail".to_string()),
            DeliveryStage::Production,
            RolloutStrategy::Canary,
        );
        success.status = ExecutionStatus::Success;
        success.started_at = now_utc() - ChronoDuration::hours(2);
        success.updated_at = now_utc() - ChronoDuration::hours(1);
        success.finished_at = Some(now_utc() - ChronoDuration::hours(1));

        let mut failed_item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("promote:tracey".to_string()),
            title: "Promote Tracey".to_string(),
            summary: "Advance Tracey to production".to_string(),
            target_service: Some("tracey".to_string()),
            delivery_stage: Some(DeliveryStage::Production),
            validated_stages: vec![
                DeliveryStage::Development,
                DeliveryStage::Testing,
                DeliveryStage::Integration,
                DeliveryStage::IntegrationTesting,
                DeliveryStage::Uat,
            ],
            rollout_strategy: Some(RolloutStrategy::RedGreen),
            status: Some(WorkStatus::Scheduled),
            priority: Some(80),
            progress_pct: Some(80),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        failed_item.created_at = now_utc() - ChronoDuration::hours(72);

        let mut failed = WorkExecution::new(
            failed_item.id,
            Some("tracey".to_string()),
            DeliveryStage::Production,
            RolloutStrategy::RedGreen,
        );
        failed.status = ExecutionStatus::Failure;
        failed.started_at = now_utc() - ChronoDuration::hours(10);
        failed.updated_at = now_utc() - ChronoDuration::hours(9);
        failed.finished_at = Some(now_utc() - ChronoDuration::hours(9));

        let mut recovery = WorkExecution::new(
            failed_item.id,
            Some("tracey".to_string()),
            DeliveryStage::Production,
            RolloutStrategy::Canary,
        );
        recovery.status = ExecutionStatus::Success;
        recovery.started_at = now_utc() - ChronoDuration::hours(4);
        recovery.updated_at = now_utc() - ChronoDuration::hours(3);
        recovery.finished_at = Some(now_utc() - ChronoDuration::hours(3));

        let metrics = compute_dora_metrics(
            &[production_item, failed_item],
            &[success, failed, recovery],
            30,
        );

        assert_eq!(metrics.attempted_production_deployments, 3);
        assert_eq!(metrics.successful_production_deployments, 2);
        assert!(metrics.deployment_frequency_per_day > 0.0);
        assert!((metrics.change_failure_rate_pct - 33.33333333333333).abs() < 0.0001);
        assert_eq!(metrics.mean_time_to_restore_hours, Some(6.0));
        assert!(metrics.lead_time_hours_average.is_some());
        assert!(metrics.lead_time_hours_median.is_some());
    }
}
