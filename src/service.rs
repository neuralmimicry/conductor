use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::Result;
use axum::http::HeaderMap;
use reqwest::Client;

use crate::{
    config::ConductorConfig,
    discovery::discover_and_probe,
    error::ApiError,
    models::{
        DashboardSummary, DiscoveryRun, ImprovementCycle, WorkItem, WorkStatus, now_utc,
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
}

impl ConductorService {
    pub fn new(
        config: ConductorConfig,
        repository: Arc<dyn ConductorRepository>,
        http: Client,
    ) -> Self {
        Self {
            config: Arc::new(config),
            repository,
            http,
        }
    }

    pub fn authorize_read(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        if self.config.security.allow_dashboard_without_token {
            return Ok(());
        }
        self.authorize_admin(headers)
    }

    pub fn authorize_admin(&self, headers: &HeaderMap) -> Result<(), ApiError> {
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
        match supplied {
            Some(token) if token == expected => Ok(()),
            _ => Err(ApiError::unauthorized()),
        }
    }

    pub async fn run_discovery_cycle(&self) -> Result<DiscoveryRun> {
        let (services, run) = discover_and_probe(&self.config, &self.http).await?;
        let metric_samples = collect_metric_samples(run.id, &services);
        self.repository.replace_service_snapshots(&services).await?;
        self.repository.insert_discovery_run(&run).await?;
        if !metric_samples.is_empty() {
            self.repository
                .insert_service_metric_samples(&metric_samples)
                .await?;
        }
        Ok(run)
    }

    pub async fn run_planning_cycle(&self) -> Result<ImprovementCycle> {
        run_planning_cycle(self.repository.as_ref(), &self.http, &self.config).await
    }

    pub async fn services(&self) -> Result<Vec<crate::models::ServiceSnapshot>> {
        self.repository.list_service_snapshots().await
    }

    pub async fn work_items(&self) -> Result<Vec<WorkItem>> {
        self.repository.list_work_items().await
    }

    pub async fn summary(&self) -> Result<DashboardSummary> {
        let services = self.repository.list_service_snapshots().await?;
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

        Ok(DashboardSummary {
            generated_at: now_utc(),
            services_total: services.len(),
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

    let discovery_service = service.clone();
    tokio::spawn(async move {
        if let Err(error) = discovery_service.run_discovery_cycle().await {
            tracing::warn!(error = %error, "initial discovery cycle failed");
        }
        let mut ticker = tokio::time::interval(discovery_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = discovery_service.run_discovery_cycle().await {
                tracing::warn!(error = %error, "discovery cycle failed");
            }
        }
    });

    let planning_service = service.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        if let Err(error) = planning_service.run_planning_cycle().await {
            tracing::warn!(error = %error, "initial planning cycle failed");
        }
        let mut ticker = tokio::time::interval(planning_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = planning_service.run_planning_cycle().await {
                tracing::warn!(error = %error, "planning cycle failed");
            }
        }
    });
}
