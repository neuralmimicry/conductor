use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::models::{
    ConductorEvent, DiscoveryRun, FindingEvidence, FindingProvenance, FindingRecord,
    ImprovementCycle, RepositorySnapshot, ServiceMetricSample, ServiceSnapshot, TraceabilityLink,
    WorkExecution, WorkItem, WorkItemPatch,
};

#[async_trait]
pub trait ConductorRepository: Send + Sync {
    async fn list_service_snapshots(&self) -> anyhow::Result<Vec<ServiceSnapshot>>;
    async fn replace_service_snapshots(&self, services: &[ServiceSnapshot]) -> anyhow::Result<()>;
    async fn list_repository_snapshots(&self) -> anyhow::Result<Vec<RepositorySnapshot>>;
    async fn replace_repository_snapshots(
        &self,
        repositories: &[RepositorySnapshot],
    ) -> anyhow::Result<()>;
    async fn list_findings(&self) -> anyhow::Result<Vec<FindingRecord>>;
    async fn get_finding(&self, id: uuid::Uuid) -> anyhow::Result<Option<FindingRecord>>;
    async fn replace_findings(
        &self,
        findings: &[FindingRecord],
        evidence: &[FindingEvidence],
        provenance: &[FindingProvenance],
    ) -> anyhow::Result<()>;
    async fn list_finding_evidence(
        &self,
        finding_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<FindingEvidence>>;
    async fn list_finding_provenance(
        &self,
        finding_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<FindingProvenance>>;
    async fn upsert_traceability_link(&self, link: &TraceabilityLink) -> anyhow::Result<()>;
    async fn list_traceability_links(
        &self,
        work_item_id: Option<uuid::Uuid>,
        execution_id: Option<uuid::Uuid>,
        finding_key: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<TraceabilityLink>>;

    async fn insert_discovery_run(&self, run: &DiscoveryRun) -> anyhow::Result<()>;
    async fn list_discovery_runs(&self, limit: usize) -> anyhow::Result<Vec<DiscoveryRun>>;

    async fn insert_service_metric_samples(
        &self,
        samples: &[ServiceMetricSample],
    ) -> anyhow::Result<()>;
    async fn list_service_metric_samples(
        &self,
        service_key: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<ServiceMetricSample>>;

    async fn upsert_work_item(&self, item: &WorkItem) -> anyhow::Result<()>;
    async fn list_work_items(&self) -> anyhow::Result<Vec<WorkItem>>;
    async fn get_work_item(&self, id: uuid::Uuid) -> anyhow::Result<Option<WorkItem>>;
    async fn patch_work_item(
        &self,
        id: uuid::Uuid,
        patch: WorkItemPatch,
    ) -> anyhow::Result<Option<WorkItem>>;
    async fn find_work_item_by_dedupe_key(
        &self,
        dedupe_key: &str,
    ) -> anyhow::Result<Option<WorkItem>>;
    async fn claim_scheduled_work_items(
        &self,
        now: DateTime<Utc>,
        claimed_by: &str,
        max_concurrent_executions: usize,
        claim_ttl_seconds: u64,
    ) -> anyhow::Result<Vec<WorkItem>>;
    async fn claim_work_item_for_execution(
        &self,
        id: uuid::Uuid,
        now: DateTime<Utc>,
        claimed_by: &str,
        claim_ttl_seconds: u64,
        force_schedule: bool,
        max_concurrent_executions: usize,
    ) -> anyhow::Result<Option<WorkItem>>;
    async fn release_work_item_claim(
        &self,
        id: uuid::Uuid,
        claim_token: uuid::Uuid,
    ) -> anyhow::Result<bool>;

    async fn upsert_work_execution(&self, execution: &WorkExecution) -> anyhow::Result<()>;
    async fn list_work_executions(&self, limit: usize) -> anyhow::Result<Vec<WorkExecution>>;
    async fn list_work_executions_for_item(
        &self,
        work_item_id: uuid::Uuid,
        limit: usize,
    ) -> anyhow::Result<Vec<WorkExecution>>;

    async fn insert_improvement_cycle(&self, cycle: &ImprovementCycle) -> anyhow::Result<()>;
    async fn list_improvement_cycles(&self, limit: usize) -> anyhow::Result<Vec<ImprovementCycle>>;
    async fn count_improvement_cycles(&self) -> anyhow::Result<usize>;
    async fn insert_conductor_event(&self, event: &ConductorEvent) -> anyhow::Result<()>;
    async fn list_conductor_events(&self, limit: usize) -> anyhow::Result<Vec<ConductorEvent>>;
}
