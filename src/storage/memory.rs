use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    models::{DiscoveryRun, ImprovementCycle, ServiceSnapshot, WorkItem, WorkItemPatch},
    repository::ConductorRepository,
};

#[derive(Default)]
pub struct MemoryRepository {
    services: RwLock<Vec<ServiceSnapshot>>,
    discoveries: RwLock<Vec<DiscoveryRun>>,
    work_items: RwLock<HashMap<Uuid, WorkItem>>,
    cycles: RwLock<Vec<ImprovementCycle>>,
}

impl MemoryRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ConductorRepository for MemoryRepository {
    async fn list_service_snapshots(&self) -> Result<Vec<ServiceSnapshot>> {
        let mut services = self.services.read().await.clone();
        services.sort_by(|left, right| left.service_key.cmp(&right.service_key));
        Ok(services)
    }

    async fn replace_service_snapshots(&self, services: &[ServiceSnapshot]) -> Result<()> {
        *self.services.write().await = services.to_vec();
        Ok(())
    }

    async fn insert_discovery_run(&self, run: &DiscoveryRun) -> Result<()> {
        self.discoveries.write().await.push(run.clone());
        Ok(())
    }

    async fn list_discovery_runs(&self, limit: usize) -> Result<Vec<DiscoveryRun>> {
        let mut runs = self.discoveries.read().await.clone();
        runs.sort_by(|left, right| right.finished_at.cmp(&left.finished_at));
        runs.truncate(limit);
        Ok(runs)
    }

    async fn upsert_work_item(&self, item: &WorkItem) -> Result<()> {
        self.work_items.write().await.insert(item.id, item.clone());
        Ok(())
    }

    async fn list_work_items(&self) -> Result<Vec<WorkItem>> {
        let mut items: Vec<_> = self.work_items.read().await.values().cloned().collect();
        items.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| right.updated_at.cmp(&left.updated_at))
        });
        Ok(items)
    }

    async fn get_work_item(&self, id: Uuid) -> Result<Option<WorkItem>> {
        Ok(self.work_items.read().await.get(&id).cloned())
    }

    async fn patch_work_item(&self, id: Uuid, patch: WorkItemPatch) -> Result<Option<WorkItem>> {
        let mut guard = self.work_items.write().await;
        if let Some(item) = guard.get_mut(&id) {
            item.apply_patch(patch);
            return Ok(Some(item.clone()));
        }
        Ok(None)
    }

    async fn find_work_item_by_dedupe_key(&self, dedupe_key: &str) -> Result<Option<WorkItem>> {
        let trimmed = dedupe_key.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let items = self.work_items.read().await;
        Ok(items
            .values()
            .find(|item| item.dedupe_key.as_deref() == Some(trimmed))
            .cloned())
    }

    async fn insert_improvement_cycle(&self, cycle: &ImprovementCycle) -> Result<()> {
        self.cycles.write().await.push(cycle.clone());
        Ok(())
    }

    async fn list_improvement_cycles(&self, limit: usize) -> Result<Vec<ImprovementCycle>> {
        let mut cycles = self.cycles.read().await.clone();
        cycles.sort_by(|left, right| right.finished_at.cmp(&left.finished_at));
        cycles.truncate(limit);
        Ok(cycles)
    }
}
