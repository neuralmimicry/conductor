use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
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
        ConductorEvent, DeliveryStage, DiscoveryRun, ExecutionStatus, FindingEvidence,
        FindingProvenance, FindingRecord, FindingSeverity, FindingStatus, ImprovementCycle,
        RepositorySnapshot, RolloutStrategy, RunStatus, ServiceHealth, ServiceMetricSample,
        ServiceSnapshot, TraceabilityLink, WorkExecution, WorkItem, WorkItemPatch, WorkStatus,
    },
    repository::ConductorRepository,
};

pub struct PostgresRepository {
    pool: Pool<Postgres>,
}

const EXECUTION_CLAIM_LOCK_KEY: i64 = 4_620_260_001;

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
                    service_key, display_name, kind, role_name, playbooks, host_targets, hosts, namespace,
                    service_name, deployment_environment, internal_url, public_url, repo_path, repo_url, repo_branch,
                    health, capabilities, dependencies, storage_paths, raw_defaults, probe,
                    discovered_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13, $14, $15,
                    $16, $17, $18, $19, $20, $21,
                    $22, $23
                )
                "#,
            )
            .bind(&service.service_key)
            .bind(&service.display_name)
            .bind(&service.kind)
            .bind(&service.role_name)
            .bind(Json(service.playbooks.clone()))
            .bind(Json(service.host_targets.clone()))
            .bind(Json(service.hosts.clone()))
            .bind(&service.namespace)
            .bind(&service.service_name)
            .bind(service.deployment_environment.map(|stage| stage.as_str().to_string()))
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

    async fn list_repository_snapshots(&self) -> Result<Vec<RepositorySnapshot>> {
        let rows = sqlx::query("SELECT * FROM repository_snapshots ORDER BY repo_key")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_repository_snapshot).collect()
    }

    async fn replace_repository_snapshots(
        &self,
        repositories: &[RepositorySnapshot],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM repository_snapshots")
            .execute(&mut *tx)
            .await?;

        for repository in repositories {
            sqlx::query(
                r#"
                INSERT INTO repository_snapshots (
                    repo_key, name, owner, repo_url, local_path, default_branch, current_branch,
                    language, frameworks, build_systems, package_managers, runtime_type,
                    deployment_type, purpose, criticality, visibility, archived, linked_services,
                    dependencies, capabilities, inventory_sources, metadata, discovered_at,
                    updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18,
                    $19, $20, $21, $22, $23,
                    $24
                )
                "#,
            )
            .bind(&repository.repo_key)
            .bind(&repository.name)
            .bind(&repository.owner)
            .bind(&repository.repo_url)
            .bind(&repository.local_path)
            .bind(&repository.default_branch)
            .bind(&repository.current_branch)
            .bind(&repository.language)
            .bind(Json(repository.frameworks.clone()))
            .bind(Json(repository.build_systems.clone()))
            .bind(Json(repository.package_managers.clone()))
            .bind(&repository.runtime_type)
            .bind(&repository.deployment_type)
            .bind(&repository.purpose)
            .bind(&repository.criticality)
            .bind(&repository.visibility)
            .bind(repository.archived)
            .bind(Json(repository.linked_services.clone()))
            .bind(Json(repository.dependencies.clone()))
            .bind(Json(repository.capabilities.clone()))
            .bind(Json(repository.inventory_sources.clone()))
            .bind(Json(repository.metadata.clone()))
            .bind(repository.discovered_at)
            .bind(repository.updated_at)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn list_findings(&self) -> Result<Vec<FindingRecord>> {
        let rows =
            sqlx::query("SELECT * FROM findings ORDER BY last_seen_at DESC, finding_key ASC")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter().map(map_finding_record).collect()
    }

    async fn get_finding(&self, id: Uuid) -> Result<Option<FindingRecord>> {
        let row = sqlx::query("SELECT * FROM findings WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(map_finding_record).transpose()
    }

    async fn replace_findings(
        &self,
        findings: &[FindingRecord],
        evidence: &[FindingEvidence],
        provenance: &[FindingProvenance],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM finding_provenance")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM finding_evidence")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM findings")
            .execute(&mut *tx)
            .await?;

        for finding in findings {
            sqlx::query(
                r#"
                INSERT INTO findings (
                    id, finding_key, title, summary, category, severity, status,
                    target_service, target_repository, source_run_id, confidence_score,
                    tags, details, first_seen_at, last_seen_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11,
                    $12, $13, $14, $15, $16
                )
                "#,
            )
            .bind(finding.id)
            .bind(&finding.finding_key)
            .bind(&finding.title)
            .bind(&finding.summary)
            .bind(&finding.category)
            .bind(finding.severity.as_str())
            .bind(finding.status.as_str())
            .bind(&finding.target_service)
            .bind(&finding.target_repository)
            .bind(finding.source_run_id)
            .bind(finding.confidence_score)
            .bind(Json(finding.tags.clone()))
            .bind(Json(finding.details.clone()))
            .bind(finding.first_seen_at)
            .bind(finding.last_seen_at)
            .bind(finding.updated_at)
            .execute(&mut *tx)
            .await?;
        }

        for item in evidence {
            sqlx::query(
                r#"
                INSERT INTO finding_evidence (
                    id, finding_id, evidence_type, source_kind, source_ref,
                    summary, payload, collected_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
            )
            .bind(item.id)
            .bind(item.finding_id)
            .bind(&item.evidence_type)
            .bind(&item.source_kind)
            .bind(&item.source_ref)
            .bind(&item.summary)
            .bind(Json(item.payload.clone()))
            .bind(item.collected_at)
            .execute(&mut *tx)
            .await?;
        }

        for item in provenance {
            sqlx::query(
                r#"
                INSERT INTO finding_provenance (
                    id, finding_id, stage, origin, component, detail,
                    confidence_score, payload, recorded_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
            )
            .bind(item.id)
            .bind(item.finding_id)
            .bind(&item.stage)
            .bind(&item.origin)
            .bind(&item.component)
            .bind(&item.detail)
            .bind(item.confidence_score)
            .bind(Json(item.payload.clone()))
            .bind(item.recorded_at)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn list_finding_evidence(&self, finding_id: Uuid) -> Result<Vec<FindingEvidence>> {
        let rows = sqlx::query(
            "SELECT * FROM finding_evidence WHERE finding_id = $1 ORDER BY summary ASC, collected_at ASC",
        )
        .bind(finding_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_finding_evidence).collect()
    }

    async fn list_finding_provenance(&self, finding_id: Uuid) -> Result<Vec<FindingProvenance>> {
        let rows = sqlx::query(
            "SELECT * FROM finding_provenance WHERE finding_id = $1 ORDER BY recorded_at ASC, stage ASC",
        )
        .bind(finding_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_finding_provenance).collect()
    }

    async fn upsert_traceability_link(&self, link: &TraceabilityLink) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO traceability_links (
                id, link_key, work_item_id, execution_id, finding_key, system,
                reference_type, reference_key, title, status, url, metadata,
                created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12,
                $13, $14
            )
            ON CONFLICT (link_key) DO UPDATE SET
                work_item_id = EXCLUDED.work_item_id,
                execution_id = EXCLUDED.execution_id,
                finding_key = EXCLUDED.finding_key,
                system = EXCLUDED.system,
                reference_type = EXCLUDED.reference_type,
                reference_key = EXCLUDED.reference_key,
                title = EXCLUDED.title,
                status = EXCLUDED.status,
                url = EXCLUDED.url,
                metadata = EXCLUDED.metadata,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(link.id)
        .bind(&link.link_key)
        .bind(link.work_item_id)
        .bind(link.execution_id)
        .bind(&link.finding_key)
        .bind(&link.system)
        .bind(&link.reference_type)
        .bind(&link.reference_key)
        .bind(&link.title)
        .bind(&link.status)
        .bind(&link.url)
        .bind(Json(link.metadata.clone()))
        .bind(link.created_at)
        .bind(link.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_traceability_links(
        &self,
        work_item_id: Option<Uuid>,
        execution_id: Option<Uuid>,
        finding_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TraceabilityLink>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            r#"
            SELECT * FROM traceability_links
            WHERE ($1::uuid IS NULL OR work_item_id = $1)
              AND ($2::uuid IS NULL OR execution_id = $2)
              AND ($3::text IS NULL OR finding_key = $3)
            ORDER BY updated_at DESC, link_key ASC
            LIMIT $4
            "#,
        )
        .bind(work_item_id)
        .bind(execution_id)
        .bind(finding_key.map(str::trim).filter(|value| !value.is_empty()))
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_traceability_link).collect()
    }

    async fn insert_discovery_run(&self, run: &DiscoveryRun) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO discovery_runs (
                id, status, services_count, repositories_count, issues, topology, started_at, finished_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(run.id)
        .bind(run.status.as_str())
        .bind(run.services_count as i32)
        .bind(run.repositories_count as i32)
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
                id, dedupe_key, title, summary, target_service, delivery_stage, validated_stages,
                rollout_strategy, status, priority,
                progress_pct, admin_override, execution_approved, verification_required,
                source, tags, plan, approval_metadata, depends_on, notes, scheduled_for, claimed_by,
                claim_expires_at, claim_token, started_at, finished_at, last_execution_id,
                last_policy, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17, $18, $19, $20, $21,
                $22, $23, $24, $25, $26, $27,
                $28, $29, $30
            )
            ON CONFLICT (id) DO UPDATE SET
                dedupe_key = EXCLUDED.dedupe_key,
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                target_service = EXCLUDED.target_service,
                delivery_stage = EXCLUDED.delivery_stage,
                validated_stages = EXCLUDED.validated_stages,
                rollout_strategy = EXCLUDED.rollout_strategy,
                status = EXCLUDED.status,
                priority = EXCLUDED.priority,
                progress_pct = EXCLUDED.progress_pct,
                admin_override = EXCLUDED.admin_override,
                execution_approved = EXCLUDED.execution_approved,
                verification_required = EXCLUDED.verification_required,
                source = EXCLUDED.source,
                tags = EXCLUDED.tags,
                plan = EXCLUDED.plan,
                approval_metadata = EXCLUDED.approval_metadata,
                depends_on = EXCLUDED.depends_on,
                notes = EXCLUDED.notes,
                scheduled_for = EXCLUDED.scheduled_for,
                claimed_by = EXCLUDED.claimed_by,
                claim_expires_at = EXCLUDED.claim_expires_at,
                claim_token = EXCLUDED.claim_token,
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
        .bind(item.delivery_stage.as_str())
        .bind(Json(
            item.validated_stages
                .iter()
                .map(|stage| stage.as_str().to_string())
                .collect::<Vec<_>>(),
        ))
        .bind(item.rollout_strategy.as_str())
        .bind(item.status.as_str())
        .bind(item.priority)
        .bind(item.progress_pct)
        .bind(item.admin_override)
        .bind(item.execution_approved)
        .bind(item.verification_required)
        .bind(&item.source)
        .bind(Json(item.tags.clone()))
        .bind(Json(item.plan.clone()))
        .bind(Json(item.approval_metadata.clone()))
        .bind(Json(item.depends_on.clone()))
        .bind(Json(item.notes.clone()))
        .bind(item.scheduled_for)
        .bind(&item.claimed_by)
        .bind(item.claim_expires_at)
        .bind(item.claim_token)
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

    async fn claim_scheduled_work_items(
        &self,
        now: DateTime<Utc>,
        claimed_by: &str,
        max_concurrent_executions: usize,
        claim_ttl_seconds: u64,
    ) -> Result<Vec<WorkItem>> {
        if max_concurrent_executions == 0 {
            return Ok(Vec::new());
        }

        let mut tx = self.pool.begin().await?;
        let lock_row = sqlx::query("SELECT pg_try_advisory_xact_lock($1) AS locked")
            .bind(EXECUTION_CLAIM_LOCK_KEY)
            .fetch_one(&mut *tx)
            .await?;
        let locked: bool = lock_row.try_get("locked")?;
        if !locked {
            tx.rollback().await?;
            return Ok(Vec::new());
        }

        let active_row = sqlx::query(
            "SELECT COUNT(*) AS active FROM work_executions WHERE status IN ('pending', 'planning', 'submitted', 'running', 'verifying')",
        )
        .fetch_one(&mut *tx)
        .await?;
        let active: i64 = active_row.try_get("active")?;
        if active >= max_concurrent_executions as i64 {
            tx.commit().await?;
            return Ok(Vec::new());
        }

        let available_slots = (max_concurrent_executions as i64 - active).max(0);
        if available_slots == 0 {
            tx.commit().await?;
            return Ok(Vec::new());
        }

        let candidate_rows = sqlx::query(
            r#"
            SELECT id
            FROM work_items
            WHERE execution_approved = TRUE
              AND status = 'scheduled'
              AND (scheduled_for IS NULL OR scheduled_for <= $1)
              AND (claim_expires_at IS NULL OR claim_expires_at <= $1)
            ORDER BY priority DESC, updated_at DESC
            FOR UPDATE SKIP LOCKED
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(available_slots)
        .fetch_all(&mut *tx)
        .await?;

        let claim_expires_at = now + ChronoDuration::seconds(claim_ttl_seconds as i64);
        let mut claimed = Vec::new();
        for row in candidate_rows {
            let id: Uuid = row.try_get("id")?;
            let claim_token = Uuid::new_v4();
            let row = sqlx::query(
                r#"
                UPDATE work_items
                SET claimed_by = $2,
                    claim_expires_at = $3,
                    claim_token = $4,
                    updated_at = $5
                WHERE id = $1
                RETURNING *
                "#,
            )
            .bind(id)
            .bind(claimed_by)
            .bind(claim_expires_at)
            .bind(claim_token)
            .bind(now)
            .fetch_one(&mut *tx)
            .await?;
            claimed.push(map_work_item(row)?);
        }

        tx.commit().await?;
        Ok(claimed)
    }

    async fn claim_work_item_for_execution(
        &self,
        id: Uuid,
        now: DateTime<Utc>,
        claimed_by: &str,
        claim_ttl_seconds: u64,
        force_schedule: bool,
        max_concurrent_executions: usize,
    ) -> Result<Option<WorkItem>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(EXECUTION_CLAIM_LOCK_KEY)
            .execute(&mut *tx)
            .await?;

        let active_row = sqlx::query(
            "SELECT COUNT(*) AS active FROM work_executions WHERE status IN ('pending', 'planning', 'submitted', 'running', 'verifying')",
        )
        .fetch_one(&mut *tx)
        .await?;
        let active: i64 = active_row.try_get("active")?;
        if active >= max_concurrent_executions as i64 {
            tx.commit().await?;
            return Ok(None);
        }

        if force_schedule {
            sqlx::query(
                r#"
                UPDATE work_items
                SET status = 'scheduled',
                    updated_at = $2
                WHERE id = $1
                  AND status <> 'scheduled'
                "#,
            )
            .bind(id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }

        let row = sqlx::query(
            r#"
            SELECT *
            FROM work_items
            WHERE id = $1
              AND status = 'scheduled'
              AND (claim_expires_at IS NULL OR claim_expires_at <= $2)
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(id)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(_) = row else {
            tx.commit().await?;
            return Ok(None);
        };

        let claim_token = Uuid::new_v4();
        let claim_expires_at = now + ChronoDuration::seconds(claim_ttl_seconds as i64);
        let row = sqlx::query(
            r#"
            UPDATE work_items
            SET claimed_by = $2,
                claim_expires_at = $3,
                claim_token = $4,
                updated_at = $5
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(claimed_by)
        .bind(claim_expires_at)
        .bind(claim_token)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(map_work_item(row)?))
    }

    async fn release_work_item_claim(&self, id: Uuid, claim_token: Uuid) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE work_items
            SET claimed_by = NULL,
                claim_expires_at = NULL,
                claim_token = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND claim_token = $2
            "#,
        )
        .bind(id)
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn upsert_work_execution(&self, execution: &WorkExecution) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO work_executions (
                id, work_item_id, target_service, delivery_stage, rollout_strategy, status, refiner_job_id, policy,
                request_payload, latest_payload, verification, error, started_at,
                updated_at, finished_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13,
                $14, $15
            )
            ON CONFLICT (id) DO UPDATE SET
                work_item_id = EXCLUDED.work_item_id,
                target_service = EXCLUDED.target_service,
                delivery_stage = EXCLUDED.delivery_stage,
                rollout_strategy = EXCLUDED.rollout_strategy,
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
        .bind(execution.delivery_stage.as_str())
        .bind(execution.rollout_strategy.as_str())
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

    async fn insert_conductor_event(&self, event: &ConductorEvent) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO conductor_events (
                id, event_type, message, status, work_item_id, execution_id,
                refiner_job_id, created_at, payload
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9
            )
            ON CONFLICT (id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                message = EXCLUDED.message,
                status = EXCLUDED.status,
                work_item_id = EXCLUDED.work_item_id,
                execution_id = EXCLUDED.execution_id,
                refiner_job_id = EXCLUDED.refiner_job_id,
                created_at = EXCLUDED.created_at,
                payload = EXCLUDED.payload
            "#,
        )
        .bind(event.id)
        .bind(&event.event_type)
        .bind(&event.message)
        .bind(&event.status)
        .bind(event.work_item_id)
        .bind(event.execution_id)
        .bind(&event.refiner_job_id)
        .bind(event.created_at)
        .bind(Json(event.payload.clone()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_conductor_events(&self, limit: usize) -> Result<Vec<ConductorEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query("SELECT * FROM conductor_events ORDER BY created_at DESC LIMIT $1")
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_conductor_event).collect()
    }
}

fn map_work_item(row: PgRow) -> Result<WorkItem> {
    Ok(WorkItem {
        id: row.try_get("id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        title: row.try_get("title")?,
        summary: row.try_get("summary")?,
        target_service: row.try_get("target_service")?,
        delivery_stage: DeliveryStage::from_db(
            row.try_get::<String, _>("delivery_stage")?.as_str(),
        ),
        validated_stages: row
            .try_get::<Json<Vec<String>>, _>("validated_stages")?
            .0
            .into_iter()
            .map(|value| DeliveryStage::from_db(value.as_str()))
            .collect(),
        rollout_strategy: RolloutStrategy::from_db(
            row.try_get::<String, _>("rollout_strategy")?.as_str(),
        ),
        status: WorkStatus::from_db(row.try_get::<String, _>("status")?.as_str()),
        priority: row.try_get("priority")?,
        progress_pct: row.try_get("progress_pct")?,
        admin_override: row.try_get("admin_override")?,
        execution_approved: row.try_get("execution_approved")?,
        verification_required: row.try_get("verification_required")?,
        source: row.try_get("source")?,
        tags: row.try_get::<Json<Vec<String>>, _>("tags")?.0,
        plan: row.try_get::<Json<serde_json::Value>, _>("plan")?.0,
        approval_metadata: row
            .try_get::<Json<serde_json::Value>, _>("approval_metadata")?
            .0,
        depends_on: row.try_get::<Json<Vec<String>>, _>("depends_on")?.0,
        notes: row.try_get::<Json<Vec<String>>, _>("notes")?.0,
        scheduled_for: row.try_get("scheduled_for")?,
        claimed_by: row.try_get("claimed_by")?,
        claim_expires_at: row.try_get("claim_expires_at")?,
        claim_token: row.try_get("claim_token")?,
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
        host_targets: row.try_get::<Json<Vec<String>>, _>("host_targets")?.0,
        hosts: row.try_get::<Json<Vec<String>>, _>("hosts")?.0,
        namespace: row.try_get("namespace")?,
        service_name: row.try_get("service_name")?,
        deployment_environment: row
            .try_get::<Option<String>, _>("deployment_environment")?
            .map(|value| DeliveryStage::from_db(value.as_str())),
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

fn map_repository_snapshot(row: PgRow) -> Result<RepositorySnapshot> {
    Ok(RepositorySnapshot {
        repo_key: row.try_get("repo_key")?,
        name: row.try_get("name")?,
        owner: row.try_get("owner")?,
        repo_url: row.try_get("repo_url")?,
        local_path: row.try_get("local_path")?,
        default_branch: row.try_get("default_branch")?,
        current_branch: row.try_get("current_branch")?,
        language: row.try_get("language")?,
        frameworks: row.try_get::<Json<Vec<String>>, _>("frameworks")?.0,
        build_systems: row.try_get::<Json<Vec<String>>, _>("build_systems")?.0,
        package_managers: row.try_get::<Json<Vec<String>>, _>("package_managers")?.0,
        runtime_type: row.try_get("runtime_type")?,
        deployment_type: row.try_get("deployment_type")?,
        purpose: row.try_get("purpose")?,
        criticality: row.try_get("criticality")?,
        visibility: row.try_get("visibility")?,
        archived: row.try_get("archived")?,
        linked_services: row.try_get::<Json<Vec<String>>, _>("linked_services")?.0,
        dependencies: row.try_get::<Json<Vec<String>>, _>("dependencies")?.0,
        capabilities: row.try_get::<Json<Vec<String>>, _>("capabilities")?.0,
        inventory_sources: row.try_get::<Json<Vec<String>>, _>("inventory_sources")?.0,
        metadata: row.try_get::<Json<serde_json::Value>, _>("metadata")?.0,
        discovered_at: row.try_get("discovered_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_finding_record(row: PgRow) -> Result<FindingRecord> {
    Ok(FindingRecord {
        id: row.try_get("id")?,
        finding_key: row.try_get("finding_key")?,
        title: row.try_get("title")?,
        summary: row.try_get("summary")?,
        category: row.try_get("category")?,
        severity: FindingSeverity::from_db(row.try_get::<String, _>("severity")?.as_str()),
        status: FindingStatus::from_db(row.try_get::<String, _>("status")?.as_str()),
        target_service: row.try_get("target_service")?,
        target_repository: row.try_get("target_repository")?,
        source_run_id: row.try_get("source_run_id")?,
        confidence_score: row.try_get("confidence_score")?,
        tags: row.try_get::<Json<Vec<String>>, _>("tags")?.0,
        details: row.try_get::<Json<serde_json::Value>, _>("details")?.0,
        first_seen_at: row.try_get("first_seen_at")?,
        last_seen_at: row.try_get("last_seen_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_finding_evidence(row: PgRow) -> Result<FindingEvidence> {
    Ok(FindingEvidence {
        id: row.try_get("id")?,
        finding_id: row.try_get("finding_id")?,
        evidence_type: row.try_get("evidence_type")?,
        source_kind: row.try_get("source_kind")?,
        source_ref: row.try_get("source_ref")?,
        summary: row.try_get("summary")?,
        payload: row.try_get::<Json<serde_json::Value>, _>("payload")?.0,
        collected_at: row.try_get("collected_at")?,
    })
}

fn map_finding_provenance(row: PgRow) -> Result<FindingProvenance> {
    Ok(FindingProvenance {
        id: row.try_get("id")?,
        finding_id: row.try_get("finding_id")?,
        stage: row.try_get("stage")?,
        origin: row.try_get("origin")?,
        component: row.try_get("component")?,
        detail: row.try_get("detail")?,
        confidence_score: row.try_get("confidence_score")?,
        payload: row.try_get::<Json<serde_json::Value>, _>("payload")?.0,
        recorded_at: row.try_get("recorded_at")?,
    })
}

fn map_traceability_link(row: PgRow) -> Result<TraceabilityLink> {
    Ok(TraceabilityLink {
        id: row.try_get("id")?,
        link_key: row.try_get("link_key")?,
        work_item_id: row.try_get("work_item_id")?,
        execution_id: row.try_get("execution_id")?,
        finding_key: row.try_get("finding_key")?,
        system: row.try_get("system")?,
        reference_type: row.try_get("reference_type")?,
        reference_key: row.try_get("reference_key")?,
        title: row.try_get("title")?,
        status: row.try_get("status")?,
        url: row.try_get("url")?,
        metadata: row.try_get::<Json<serde_json::Value>, _>("metadata")?.0,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_discovery_run(row: PgRow) -> Result<DiscoveryRun> {
    Ok(DiscoveryRun {
        id: row.try_get("id")?,
        status: RunStatus::from_db(row.try_get::<String, _>("status")?.as_str()),
        services_count: row.try_get::<i32, _>("services_count")? as usize,
        repositories_count: row.try_get::<i32, _>("repositories_count")? as usize,
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
        delivery_stage: DeliveryStage::from_db(
            row.try_get::<String, _>("delivery_stage")?.as_str(),
        ),
        rollout_strategy: RolloutStrategy::from_db(
            row.try_get::<String, _>("rollout_strategy")?.as_str(),
        ),
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

fn map_conductor_event(row: PgRow) -> Result<ConductorEvent> {
    Ok(ConductorEvent {
        id: row.try_get("id")?,
        event_type: row.try_get("event_type")?,
        message: row.try_get("message")?,
        status: row.try_get("status")?,
        work_item_id: row.try_get("work_item_id")?,
        execution_id: row.try_get("execution_id")?,
        refiner_job_id: row.try_get("refiner_job_id")?,
        created_at: row.try_get("created_at")?,
        payload: row.try_get::<Json<serde_json::Value>, _>("payload")?.0,
    })
}
