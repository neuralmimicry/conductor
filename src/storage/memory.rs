use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::{
    models::{
        ConductorEvent, DiscoveryRun, FindingEvidence, FindingProvenance, FindingRecord,
        ImprovementCycle, RepositorySnapshot, ServiceMetricSample, ServiceSnapshot,
        TraceabilityLink, WorkExecution, WorkItem, WorkItemPatch,
    },
    repository::ConductorRepository,
};

#[derive(Default)]
pub struct MemoryRepository {
    services: RwLock<Vec<ServiceSnapshot>>,
    repositories: RwLock<Vec<RepositorySnapshot>>,
    findings: RwLock<HashMap<Uuid, FindingRecord>>,
    finding_evidence: RwLock<Vec<FindingEvidence>>,
    finding_provenance: RwLock<Vec<FindingProvenance>>,
    traceability_links: RwLock<HashMap<String, TraceabilityLink>>,
    discoveries: RwLock<Vec<DiscoveryRun>>,
    metric_samples: RwLock<Vec<ServiceMetricSample>>,
    work_items: RwLock<HashMap<Uuid, WorkItem>>,
    executions: RwLock<HashMap<Uuid, WorkExecution>>,
    cycles: RwLock<Vec<ImprovementCycle>>,
    events: RwLock<Vec<ConductorEvent>>,
    claim_guard: Mutex<()>,
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

    async fn list_repository_snapshots(&self) -> Result<Vec<RepositorySnapshot>> {
        let mut repositories = self.repositories.read().await.clone();
        repositories.sort_by(|left, right| left.repo_key.cmp(&right.repo_key));
        Ok(repositories)
    }

    async fn replace_repository_snapshots(
        &self,
        repositories: &[RepositorySnapshot],
    ) -> Result<()> {
        *self.repositories.write().await = repositories.to_vec();
        Ok(())
    }

    async fn list_findings(&self) -> Result<Vec<FindingRecord>> {
        let mut findings: Vec<_> = self.findings.read().await.values().cloned().collect();
        findings.sort_by(|left, right| {
            right
                .last_seen_at
                .cmp(&left.last_seen_at)
                .then_with(|| left.finding_key.cmp(&right.finding_key))
        });
        Ok(findings)
    }

    async fn get_finding(&self, id: Uuid) -> Result<Option<FindingRecord>> {
        Ok(self.findings.read().await.get(&id).cloned())
    }

    async fn replace_findings(
        &self,
        findings: &[FindingRecord],
        evidence: &[FindingEvidence],
        provenance: &[FindingProvenance],
    ) -> Result<()> {
        let mut finding_guard = self.findings.write().await;
        finding_guard.clear();
        for finding in findings {
            finding_guard.insert(finding.id, finding.clone());
        }
        *self.finding_evidence.write().await = evidence.to_vec();
        *self.finding_provenance.write().await = provenance.to_vec();
        Ok(())
    }

    async fn list_finding_evidence(&self, finding_id: Uuid) -> Result<Vec<FindingEvidence>> {
        let mut evidence: Vec<_> = self
            .finding_evidence
            .read()
            .await
            .iter()
            .filter(|item| item.finding_id == finding_id)
            .cloned()
            .collect();
        evidence.sort_by(|left, right| left.summary.cmp(&right.summary));
        Ok(evidence)
    }

    async fn list_finding_provenance(&self, finding_id: Uuid) -> Result<Vec<FindingProvenance>> {
        let mut provenance: Vec<_> = self
            .finding_provenance
            .read()
            .await
            .iter()
            .filter(|item| item.finding_id == finding_id)
            .cloned()
            .collect();
        provenance.sort_by(|left, right| left.recorded_at.cmp(&right.recorded_at));
        Ok(provenance)
    }

    async fn upsert_traceability_link(&self, link: &TraceabilityLink) -> Result<()> {
        let mut guard = self.traceability_links.write().await;
        if let Some(existing) = guard.get_mut(&link.link_key) {
            let created_at = existing.created_at;
            let id = existing.id;
            *existing = link.clone();
            existing.created_at = created_at;
            existing.id = id;
            return Ok(());
        }
        guard.insert(link.link_key.clone(), link.clone());
        Ok(())
    }

    async fn list_traceability_links(
        &self,
        work_item_id: Option<Uuid>,
        execution_id: Option<Uuid>,
        finding_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TraceabilityLink>> {
        let finding_key = finding_key.map(str::trim).filter(|value| !value.is_empty());
        let mut links: Vec<_> = self
            .traceability_links
            .read()
            .await
            .values()
            .filter(|link| work_item_id.is_none_or(|value| link.work_item_id == Some(value)))
            .filter(|link| execution_id.is_none_or(|value| link.execution_id == Some(value)))
            .filter(|link| {
                finding_key.is_none_or(|value| link.finding_key.as_deref() == Some(value))
            })
            .cloned()
            .collect();
        links.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.link_key.cmp(&right.link_key))
        });
        links.truncate(limit);
        Ok(links)
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

    async fn insert_service_metric_samples(&self, samples: &[ServiceMetricSample]) -> Result<()> {
        let mut guard = self.metric_samples.write().await;
        guard.extend(samples.iter().cloned());
        guard.sort_by(|left, right| right.sampled_at.cmp(&left.sampled_at));
        Ok(())
    }

    async fn list_service_metric_samples(
        &self,
        service_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ServiceMetricSample>> {
        let trimmed = service_key.map(str::trim).filter(|value| !value.is_empty());
        let mut samples: Vec<_> = self
            .metric_samples
            .read()
            .await
            .iter()
            .filter(|sample| trimmed.is_none_or(|expected| sample.service_key == expected))
            .cloned()
            .collect();
        samples.sort_by(|left, right| right.sampled_at.cmp(&left.sampled_at));
        samples.truncate(limit);
        Ok(samples)
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

        let _guard = self.claim_guard.lock().await;
        let active = self
            .executions
            .read()
            .await
            .values()
            .filter(|execution| !execution.status.is_terminal())
            .count();
        if active >= max_concurrent_executions {
            return Ok(Vec::new());
        }

        let available_slots = max_concurrent_executions - active;
        let expires_at = now + ChronoDuration::seconds(claim_ttl_seconds as i64);
        let mut items: Vec<_> = self.work_items.read().await.values().cloned().collect();
        items.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| right.updated_at.cmp(&left.updated_at))
        });

        let mut claimed_ids = Vec::new();
        for item in items {
            if claimed_ids.len() >= available_slots {
                break;
            }
            if !item.execution_approved
                || !matches!(item.status, crate::models::WorkStatus::Scheduled)
            {
                continue;
            }
            if item
                .scheduled_for
                .is_some_and(|scheduled_for| scheduled_for > now)
            {
                continue;
            }
            if item
                .claim_expires_at
                .is_some_and(|claim_expires_at| claim_expires_at > now)
            {
                continue;
            }
            claimed_ids.push(item.id);
        }

        if claimed_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut guard = self.work_items.write().await;
        let mut claimed = Vec::new();
        for id in claimed_ids {
            if let Some(item) = guard.get_mut(&id) {
                item.claimed_by = Some(claimed_by.to_string());
                item.claim_expires_at = Some(expires_at);
                item.claim_token = Some(Uuid::new_v4());
                item.updated_at = now;
                claimed.push(item.clone());
            }
        }
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
        let _guard = self.claim_guard.lock().await;
        let active = self
            .executions
            .read()
            .await
            .values()
            .filter(|execution| !execution.status.is_terminal())
            .count();
        if active >= max_concurrent_executions {
            return Ok(None);
        }

        let mut guard = self.work_items.write().await;
        let Some(item) = guard.get_mut(&id) else {
            return Ok(None);
        };
        if !force_schedule && !matches!(item.status, crate::models::WorkStatus::Scheduled) {
            return Ok(None);
        }
        if force_schedule && !matches!(item.status, crate::models::WorkStatus::Scheduled) {
            item.status = crate::models::WorkStatus::Scheduled;
        }
        if item
            .claim_expires_at
            .is_some_and(|claim_expires_at| claim_expires_at > now)
        {
            return Ok(None);
        }
        item.claimed_by = Some(claimed_by.to_string());
        item.claim_expires_at = Some(now + ChronoDuration::seconds(claim_ttl_seconds as i64));
        item.claim_token = Some(Uuid::new_v4());
        item.updated_at = now;
        Ok(Some(item.clone()))
    }

    async fn release_work_item_claim(&self, id: Uuid, claim_token: Uuid) -> Result<bool> {
        let mut guard = self.work_items.write().await;
        let Some(item) = guard.get_mut(&id) else {
            return Ok(false);
        };
        if item.claim_token != Some(claim_token) {
            return Ok(false);
        }
        item.clear_claim();
        Ok(true)
    }

    async fn upsert_work_execution(&self, execution: &WorkExecution) -> Result<()> {
        self.executions
            .write()
            .await
            .insert(execution.id, execution.clone());
        Ok(())
    }

    async fn list_work_executions(&self, limit: usize) -> Result<Vec<WorkExecution>> {
        let mut items: Vec<_> = self.executions.read().await.values().cloned().collect();
        items.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        items.truncate(limit);
        Ok(items)
    }

    async fn list_work_executions_for_item(
        &self,
        work_item_id: Uuid,
        limit: usize,
    ) -> Result<Vec<WorkExecution>> {
        let mut items: Vec<_> = self
            .executions
            .read()
            .await
            .values()
            .filter(|execution| execution.work_item_id == work_item_id)
            .cloned()
            .collect();
        items.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        items.truncate(limit);
        Ok(items)
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

    async fn insert_conductor_event(&self, event: &ConductorEvent) -> Result<()> {
        let mut guard = self.events.write().await;
        guard.push(event.clone());
        guard.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        Ok(())
    }

    async fn list_conductor_events(&self, limit: usize) -> Result<Vec<ConductorEvent>> {
        let mut events = self.events.read().await.clone();
        events.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        events.truncate(limit);
        Ok(events)
    }
}
