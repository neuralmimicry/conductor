use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::{
    Pool, Postgres, Row,
    migrate::Migrator,
    postgres::{PgPoolOptions, PgRow},
    types::Json,
};
use uuid::Uuid;

use crate::{
    config::DatabaseConfig,
    models::{
        DiscoveryRun, ExecutionStatus, ImprovementCycle, RunStatus, ServiceHealth,
        ServiceMetricSample, ServiceSnapshot, WorkExecution, WorkItem, WorkItemPatch, WorkStatus,
    },
    repository::ConductorRepository,
};

pub struct PostgresRepository {
    pool: Pool<Postgres>,
}

impl PostgresRepository {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.url)
            .await
            .with_context(|| "failed to connect to postgres")?;

        if config.run_migrations {
            Migrator::new(migrations_path().as_path())
                .await?
                .run(&pool)
                .await
                .with_context(|| "failed to run migrations")?;
        }

        Ok(Self { pool })
    }
}

fn migrations_path() -> std::path::PathBuf {
    let cwd_path = std::path::PathBuf::from("migrations");
    if cwd_path.exists() {
        return cwd_path;
    }

    if let Ok(executable) = std::env::current_exe() {
        if let Some(parent) = executable.parent() {
            let sibling = parent.join("migrations");
            if sibling.exists() {
                return sibling;
            }
        }
    }

    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

#[async_trait]
impl ConductorRepository for PostgresRepository {
    async fn list_service_snapshots(&self) -> Result<Vec<ServiceSnapshot>> {
        let rows = sqlx::query("SELECT * FROM service_snapshots ORDER BY service_key")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_service_snapshot).collect()
    }

    async fn replace_service_snapshots(&self, services: &[ServiceSnapshot]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM service_snapshots")
            .execute(&mut *tx)
            .await?;

        for service in services {
            sqlx::query(
                r#"
                INSERT INTO service_snapshots (
                    service_key, display_name, kind, role_name, playbooks, hosts, namespace,
                    service_name, internal_url, public_url, repo_path, repo_url, repo_branch,
                    health, capabilities, dependencies, storage_paths, raw_defaults, probe,
                    discovered_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13,
                    $14, $15, $16, $17, $18, $19,
                    $20, $21
                )
                "#,
            )
            .bind(&service.service_key)
            .bind(&service.display_name)
            .bind(&service.kind)
            .bind(&service.role_name)
            .bind(Json(service.playbooks.clone()))
            .bind(Json(service.hosts.clone()))
            .bind(&service.namespace)
            .bind(&service.service_name)
            .bind(&service.internal_url)
            .bind(&service.public_url)
            .bind(&service.repo_path)
            .bind(&service.repo_url)
            .bind(&service.repo_branch)
            .bind(service.health.as_str())
            .bind(Json(service.capabilities.clone()))
            .bind(Json(service.dependencies.clone()))
            .bind(Json(service.storage_paths.clone()))
            .bind(Json(service.raw_defaults.clone()))
            .bind(Json(service.probe.clone()))
            .bind(service.discovered_at)
            .bind(service.updated_at)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn insert_discovery_run(&self, run: &DiscoveryRun) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO discovery_runs (
                id, status, services_count, issues, topology, started_at, finished_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(run.id)
        .bind(run.status.as_str())
        .bind(run.services_count as i32)
        .bind(Json(run.issues.clone()))
        .bind(Json(run.topology.clone()))
        .bind(run.started_at)
        .bind(run.finished_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_discovery_runs(&self, limit: usize) -> Result<Vec<DiscoveryRun>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query("SELECT * FROM discovery_runs ORDER BY finished_at DESC LIMIT $1")
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_discovery_run).collect()
    }

    async fn insert_service_metric_samples(&self, samples: &[ServiceMetricSample]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        for sample in samples {
            sqlx::query(
                r#"
                INSERT INTO service_metric_samples (
                    id, discovery_run_id, service_key, metric_source, metrics, sampled_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO UPDATE SET
                    discovery_run_id = EXCLUDED.discovery_run_id,
                    service_key = EXCLUDED.service_key,
                    metric_source = EXCLUDED.metric_source,
                    metrics = EXCLUDED.metrics,
                    sampled_at = EXCLUDED.sampled_at
                "#,
            )
            .bind(sample.id)
            .bind(sample.discovery_run_id)
            .bind(&sample.service_key)
            .bind(&sample.metric_source)
            .bind(Json(sample.metrics.clone()))
            .bind(sample.sampled_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_service_metric_samples(
        &self,
        service_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ServiceMetricSample>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let rows = if let Some(service_key) =
            service_key.map(str::trim).filter(|value| !value.is_empty())
        {
            sqlx::query(
                "SELECT * FROM service_metric_samples WHERE service_key = $1 ORDER BY sampled_at DESC LIMIT $2",
            )
            .bind(service_key)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query("SELECT * FROM service_metric_samples ORDER BY sampled_at DESC LIMIT $1")
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
        };

        rows.into_iter().map(map_service_metric_sample).collect()
    }

    async fn upsert_work_item(&self, item: &WorkItem) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO work_items (
                id, dedupe_key, title, summary, target_service, status, priority,
                progress_pct, admin_override, execution_approved, verification_required,
                source, tags, plan, notes, scheduled_for, started_at, finished_at,
                last_execution_id, last_policy, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, $18,
                $19, $20, $21, $22
            )
            ON CONFLICT (id) DO UPDATE SET
                dedupe_key = EXCLUDED.dedupe_key,
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                target_service = EXCLUDED.target_service,
                status = EXCLUDED.status,
                priority = EXCLUDED.priority,
                progress_pct = EXCLUDED.progress_pct,
                admin_override = EXCLUDED.admin_override,
                execution_approved = EXCLUDED.execution_approved,
                verification_required = EXCLUDED.verification_required,
                source = EXCLUDED.source,
                tags = EXCLUDED.tags,
                plan = EXCLUDED.plan,
                notes = EXCLUDED.notes,
                scheduled_for = EXCLUDED.scheduled_for,
                started_at = EXCLUDED.started_at,
                finished_at = EXCLUDED.finished_at,
                last_execution_id = EXCLUDED.last_execution_id,
                last_policy = EXCLUDED.last_policy,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(item.id)
        .bind(&item.dedupe_key)
        .bind(&item.title)
        .bind(&item.summary)
        .bind(&item.target_service)
        .bind(item.status.as_str())
        .bind(item.priority)
        .bind(item.progress_pct)
        .bind(item.admin_override)
        .bind(item.execution_approved)
        .bind(item.verification_required)
        .bind(&item.source)
        .bind(Json(item.tags.clone()))
        .bind(Json(item.plan.clone()))
        .bind(Json(item.notes.clone()))
        .bind(item.scheduled_for)
        .bind(item.started_at)
        .bind(item.finished_at)
        .bind(item.last_execution_id)
        .bind(Json(item.last_policy.clone()))
        .bind(item.created_at)
        .bind(item.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_work_items(&self) -> Result<Vec<WorkItem>> {
        let rows = sqlx::query("SELECT * FROM work_items ORDER BY priority DESC, updated_at DESC")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_work_item).collect()
    }

    async fn get_work_item(&self, id: Uuid) -> Result<Option<WorkItem>> {
        let row = sqlx::query("SELECT * FROM work_items WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(map_work_item).transpose()
    }

    async fn patch_work_item(&self, id: Uuid, patch: WorkItemPatch) -> Result<Option<WorkItem>> {
        if let Some(mut item) = self.get_work_item(id).await? {
            item.apply_patch(patch);
            self.upsert_work_item(&item).await?;
            return Ok(Some(item));
        }
        Ok(None)
    }

    async fn find_work_item_by_dedupe_key(&self, dedupe_key: &str) -> Result<Option<WorkItem>> {
        let trimmed = dedupe_key.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let row = sqlx::query("SELECT * FROM work_items WHERE dedupe_key = $1")
            .bind(trimmed)
            .fetch_optional(&self.pool)
            .await?;
        row.map(map_work_item).transpose()
    }

    async fn upsert_work_execution(&self, execution: &WorkExecution) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO work_executions (
                id, work_item_id, target_service, status, refiner_job_id, policy,
                request_payload, latest_payload, verification, error, started_at,
                updated_at, finished_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11,
                $12, $13
            )
            ON CONFLICT (id) DO UPDATE SET
                work_item_id = EXCLUDED.work_item_id,
                target_service = EXCLUDED.target_service,
                status = EXCLUDED.status,
                refiner_job_id = EXCLUDED.refiner_job_id,
                policy = EXCLUDED.policy,
                request_payload = EXCLUDED.request_payload,
                latest_payload = EXCLUDED.latest_payload,
                verification = EXCLUDED.verification,
                error = EXCLUDED.error,
                started_at = EXCLUDED.started_at,
                updated_at = EXCLUDED.updated_at,
                finished_at = EXCLUDED.finished_at
            "#,
        )
        .bind(execution.id)
        .bind(execution.work_item_id)
        .bind(&execution.target_service)
        .bind(execution.status.as_str())
        .bind(&execution.refiner_job_id)
        .bind(Json(execution.policy.clone()))
        .bind(Json(execution.request_payload.clone()))
        .bind(Json(execution.latest_payload.clone()))
        .bind(Json(execution.verification.clone()))
        .bind(&execution.error)
        .bind(execution.started_at)
        .bind(execution.updated_at)
        .bind(execution.finished_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_work_executions(&self, limit: usize) -> Result<Vec<WorkExecution>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query("SELECT * FROM work_executions ORDER BY updated_at DESC LIMIT $1")
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_work_execution).collect()
    }

    async fn list_work_executions_for_item(
        &self,
        work_item_id: Uuid,
        limit: usize,
    ) -> Result<Vec<WorkExecution>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT * FROM work_executions WHERE work_item_id = $1 ORDER BY updated_at DESC LIMIT $2",
        )
        .bind(work_item_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_work_execution).collect()
    }

    async fn insert_improvement_cycle(&self, cycle: &ImprovementCycle) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO improvement_cycles (
                id, status, summary, source_services, recommendations, gail_response,
                started_at, finished_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(cycle.id)
        .bind(cycle.status.as_str())
        .bind(&cycle.summary)
        .bind(Json(cycle.source_services.clone()))
        .bind(Json(cycle.recommendations.clone()))
        .bind(Json(cycle.gail_response.clone()))
        .bind(cycle.started_at)
        .bind(cycle.finished_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_improvement_cycles(&self, limit: usize) -> Result<Vec<ImprovementCycle>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows =
            sqlx::query("SELECT * FROM improvement_cycles ORDER BY finished_at DESC LIMIT $1")
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter().map(map_improvement_cycle).collect()
    }
}

fn map_work_item(row: PgRow) -> Result<WorkItem> {
    Ok(WorkItem {
        id: row.try_get("id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        title: row.try_get("title")?,
        summary: row.try_get("summary")?,
        target_service: row.try_get("target_service")?,
        status: WorkStatus::from_db(row.try_get::<String, _>("status")?.as_str()),
        priority: row.try_get("priority")?,
        progress_pct: row.try_get("progress_pct")?,
        admin_override: row.try_get("admin_override")?,
        execution_approved: row.try_get("execution_approved")?,
        verification_required: row.try_get("verification_required")?,
        source: row.try_get("source")?,
        tags: row.try_get::<Json<Vec<String>>, _>("tags")?.0,
        plan: row.try_get::<Json<serde_json::Value>, _>("plan")?.0,
        notes: row.try_get::<Json<Vec<String>>, _>("notes")?.0,
        scheduled_for: row.try_get("scheduled_for")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        last_execution_id: row.try_get("last_execution_id")?,
        last_policy: row.try_get::<Json<serde_json::Value>, _>("last_policy")?.0,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_service_snapshot(row: PgRow) -> Result<ServiceSnapshot> {
    Ok(ServiceSnapshot {
        service_key: row.try_get("service_key")?,
        display_name: row.try_get("display_name")?,
        kind: row.try_get("kind")?,
        role_name: row.try_get("role_name")?,
        playbooks: row.try_get::<Json<Vec<String>>, _>("playbooks")?.0,
        hosts: row.try_get::<Json<Vec<String>>, _>("hosts")?.0,
        namespace: row.try_get("namespace")?,
        service_name: row.try_get("service_name")?,
        internal_url: row.try_get("internal_url")?,
        public_url: row.try_get("public_url")?,
        repo_path: row.try_get("repo_path")?,
        repo_url: row.try_get("repo_url")?,
        repo_branch: row.try_get("repo_branch")?,
        health: ServiceHealth::from_db(row.try_get::<String, _>("health")?.as_str()),
        capabilities: row.try_get::<Json<Vec<String>>, _>("capabilities")?.0,
        dependencies: row.try_get::<Json<Vec<String>>, _>("dependencies")?.0,
        storage_paths: row.try_get::<Json<Vec<String>>, _>("storage_paths")?.0,
        raw_defaults: row.try_get::<Json<serde_json::Value>, _>("raw_defaults")?.0,
        probe: row.try_get::<Json<serde_json::Value>, _>("probe")?.0,
        discovered_at: row.try_get("discovered_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_discovery_run(row: PgRow) -> Result<DiscoveryRun> {
    Ok(DiscoveryRun {
        id: row.try_get("id")?,
        status: RunStatus::from_db(row.try_get::<String, _>("status")?.as_str()),
        services_count: row.try_get::<i32, _>("services_count")? as usize,
        issues: row.try_get::<Json<Vec<String>>, _>("issues")?.0,
        topology: row.try_get::<Json<serde_json::Value>, _>("topology")?.0,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn map_service_metric_sample(row: PgRow) -> Result<ServiceMetricSample> {
    Ok(ServiceMetricSample {
        id: row.try_get("id")?,
        discovery_run_id: row.try_get("discovery_run_id")?,
        service_key: row.try_get("service_key")?,
        metric_source: row.try_get("metric_source")?,
        metrics: row.try_get::<Json<serde_json::Value>, _>("metrics")?.0,
        sampled_at: row.try_get("sampled_at")?,
    })
}

fn map_work_execution(row: PgRow) -> Result<WorkExecution> {
    Ok(WorkExecution {
        id: row.try_get("id")?,
        work_item_id: row.try_get("work_item_id")?,
        target_service: row.try_get("target_service")?,
        status: ExecutionStatus::from_db(row.try_get::<String, _>("status")?.as_str()),
        refiner_job_id: row.try_get("refiner_job_id")?,
        policy: row.try_get::<Json<serde_json::Value>, _>("policy")?.0,
        request_payload: row
            .try_get::<Json<serde_json::Value>, _>("request_payload")?
            .0,
        latest_payload: row
            .try_get::<Json<serde_json::Value>, _>("latest_payload")?
            .0,
        verification: row.try_get::<Json<serde_json::Value>, _>("verification")?.0,
        error: row.try_get("error")?,
        started_at: row.try_get("started_at")?,
        updated_at: row.try_get("updated_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn map_improvement_cycle(row: PgRow) -> Result<ImprovementCycle> {
    Ok(ImprovementCycle {
        id: row.try_get("id")?,
        status: RunStatus::from_db(row.try_get::<String, _>("status")?.as_str()),
        summary: row.try_get("summary")?,
        source_services: row.try_get::<Json<Vec<String>>, _>("source_services")?.0,
        recommendations: row
            .try_get::<Json<Vec<serde_json::Value>>, _>("recommendations")?
            .0,
        gail_response: row
            .try_get::<Json<Option<serde_json::Value>>, _>("gail_response")?
            .0,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}
