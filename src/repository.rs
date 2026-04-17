use async_trait::async_trait;

use crate::models::{
    DiscoveryRun, ImprovementCycle, ServiceMetricSample, ServiceSnapshot, WorkExecution, WorkItem,
    WorkItemPatch,
};

#[async_trait]
pub trait ConductorRepository: Send + Sync {
    async fn list_service_snapshots(&self) -> anyhow::Result<Vec<ServiceSnapshot>>;
    async fn replace_service_snapshots(&self, services: &[ServiceSnapshot]) -> anyhow::Result<()>;

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

    async fn upsert_work_execution(&self, execution: &WorkExecution) -> anyhow::Result<()>;
    async fn list_work_executions(&self, limit: usize) -> anyhow::Result<Vec<WorkExecution>>;
    async fn list_work_executions_for_item(
        &self,
        work_item_id: uuid::Uuid,
        limit: usize,
    ) -> anyhow::Result<Vec<WorkExecution>>;

    async fn insert_improvement_cycle(&self, cycle: &ImprovementCycle) -> anyhow::Result<()>;
    async fn list_improvement_cycles(&self, limit: usize) -> anyhow::Result<Vec<ImprovementCycle>>;
}
