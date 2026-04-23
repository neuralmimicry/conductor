use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::Result;
use axum::http::HeaderMap;
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
        ConductorEvent, DashboardSummary, DiscoveryRun, FindingEvidence, FindingProvenance,
        FindingRecord, ImprovementCycle, RepositorySnapshot, WorkExecution, WorkItem, WorkStatus,
        now_utc, topology_from_services,
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
        let executions = self.repository.list_work_executions(200).await?;

        let mut work_by_status = BTreeMap::new();
        for item in &work_items {
            *work_by_status
                .entry(item.status.as_str().to_string())
                .or_insert(0usize) += 1;
        }

        let mut findings_by_severity = BTreeMap::new();
        for finding in &findings {
            *findings_by_severity
                .entry(finding.severity.as_str().to_string())
                .or_insert(0usize) += 1;
        }

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
            latest_discovery,
            latest_cycle,
        })
    }

    pub async fn topology(&self) -> Result<crate::models::TopologyGraph> {
        let services = self.repository.list_service_snapshots().await?;
        Ok(topology_from_services(&services))
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
