use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub fn now_utc() -> DateTime<Utc> {
    Utc::now()
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkStatus {
    #[default]
    Planned,
    Scheduled,
    InOperation,
    Success,
    Failure,
    Aborted,
    OnHold,
}

impl WorkStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Scheduled => "scheduled",
            Self::InOperation => "in_operation",
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Aborted => "aborted",
            Self::OnHold => "on_hold",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.trim() {
            "planned" => Self::Planned,
            "scheduled" => Self::Scheduled,
            "in_operation" => Self::InOperation,
            "success" => Self::Success,
            "failure" => Self::Failure,
            "aborted" => Self::Aborted,
            "on_hold" => Self::OnHold,
            _ => Self::Planned,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ServiceHealth {
    Healthy,
    Degraded,
    Unreachable,
    Missing,
    #[default]
    Unknown,
}

impl ServiceHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unreachable => "unreachable",
            Self::Missing => "missing",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.trim() {
            "healthy" => Self::Healthy,
            "degraded" => Self::Degraded,
            "unreachable" => Self::Unreachable,
            "missing" => Self::Missing,
            _ => Self::Unknown,
        }
    }

    pub fn severity(self) -> i32 {
        match self {
            Self::Healthy => 0,
            Self::Unknown => 1,
            Self::Degraded => 2,
            Self::Unreachable => 3,
            Self::Missing => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    #[default]
    Success,
    PartialFailure,
    Failed,
    Running,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::PartialFailure => "partial_failure",
            Self::Failed => "failed",
            Self::Running => "running",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.trim() {
            "success" => Self::Success,
            "partial_failure" => Self::PartialFailure,
            "failed" => Self::Failed,
            "running" => Self::Running,
            _ => Self::Success,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NewWorkItem {
    pub dedupe_key: Option<String>,
    pub title: String,
    pub summary: String,
    pub target_service: Option<String>,
    pub status: Option<WorkStatus>,
    pub priority: Option<i32>,
    pub progress_pct: Option<i32>,
    #[serde(default)]
    pub admin_override: bool,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub plan: Value,
    pub source: Option<String>,
    pub scheduled_for: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct WorkItemPatch {
    pub summary: Option<String>,
    pub status: Option<WorkStatus>,
    pub priority: Option<i32>,
    pub progress_pct: Option<i32>,
    pub admin_override: Option<bool>,
    pub scheduled_for: Option<DateTime<Utc>>,
    #[serde(default)]
    pub clear_schedule: bool,
    pub note: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkItem {
    pub id: Uuid,
    pub dedupe_key: Option<String>,
    pub title: String,
    pub summary: String,
    pub target_service: Option<String>,
    pub status: WorkStatus,
    pub priority: i32,
    pub progress_pct: i32,
    pub admin_override: bool,
    pub source: String,
    pub tags: Vec<String>,
    pub plan: Value,
    pub notes: Vec<String>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl WorkItem {
    pub fn from_new(input: NewWorkItem) -> Self {
        let now = now_utc();
        Self {
            id: Uuid::new_v4(),
            dedupe_key: input
                .dedupe_key
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            title: input.title,
            summary: input.summary,
            target_service: input.target_service,
            status: input.status.unwrap_or_default(),
            priority: input.priority.unwrap_or(50),
            progress_pct: input.progress_pct.unwrap_or(0).clamp(0, 100),
            admin_override: input.admin_override,
            source: input.source.unwrap_or_else(|| "manual".to_string()),
            tags: unique_strings(input.tags),
            plan: input.plan,
            notes: Vec::new(),
            scheduled_for: input.scheduled_for,
            started_at: None,
            finished_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn apply_patch(&mut self, patch: WorkItemPatch) {
        if let Some(summary) = patch.summary {
            self.summary = summary;
        }
        if let Some(status) = patch.status {
            self.status = status;
            match status {
                WorkStatus::InOperation if self.started_at.is_none() => {
                    self.started_at = Some(now_utc());
                    self.finished_at = None;
                }
                WorkStatus::Success | WorkStatus::Failure | WorkStatus::Aborted => {
                    if self.started_at.is_none() {
                        self.started_at = Some(now_utc());
                    }
                    self.finished_at = Some(now_utc());
                    if matches!(status, WorkStatus::Success) {
                        self.progress_pct = 100;
                    }
                }
                _ => {}
            }
        }
        if let Some(priority) = patch.priority {
            self.priority = priority;
        }
        if let Some(progress_pct) = patch.progress_pct {
            self.progress_pct = progress_pct.clamp(0, 100);
        }
        if let Some(admin_override) = patch.admin_override {
            self.admin_override = admin_override;
        }
        if patch.clear_schedule {
            self.scheduled_for = None;
        } else if let Some(scheduled_for) = patch.scheduled_for {
            self.scheduled_for = Some(scheduled_for);
        }
        if let Some(note) = patch
            .note
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        {
            self.notes
                .push(format!("{} {}", now_utc().to_rfc3339(), note));
        }
        self.updated_at = now_utc();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceSnapshot {
    pub service_key: String,
    pub display_name: String,
    pub kind: String,
    pub role_name: String,
    pub playbooks: Vec<String>,
    pub hosts: Vec<String>,
    pub namespace: Option<String>,
    pub service_name: Option<String>,
    pub internal_url: Option<String>,
    pub public_url: Option<String>,
    pub repo_path: Option<String>,
    pub health: ServiceHealth,
    pub capabilities: Vec<String>,
    pub dependencies: Vec<String>,
    pub storage_paths: Vec<String>,
    pub raw_defaults: Value,
    pub probe: Value,
    pub discovered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveryRun {
    pub id: Uuid,
    pub status: RunStatus,
    pub services_count: usize,
    pub issues: Vec<String>,
    pub topology: Value,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImprovementCycle {
    pub id: Uuid,
    pub status: RunStatus,
    pub summary: String,
    pub source_services: Vec<String>,
    pub recommendations: Vec<Value>,
    pub gail_response: Option<Value>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopologyEdge {
    pub from: String,
    pub to: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopologyGraph {
    pub services: Vec<ServiceSnapshot>,
    pub edges: Vec<TopologyEdge>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DashboardSummary {
    pub generated_at: DateTime<Utc>,
    pub services_total: usize,
    pub services_healthy: usize,
    pub services_degraded: usize,
    pub services_unreachable: usize,
    pub work_items_total: usize,
    pub work_by_status: BTreeMap<String, usize>,
    pub cycles_total: usize,
    pub latest_discovery: Option<DiscoveryRun>,
    pub latest_cycle: Option<ImprovementCycle>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeResult {
    pub endpoint: Option<String>,
    pub summary: String,
    pub metrics: Value,
    pub health: ServiceHealth,
}

pub fn unique_strings(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

pub fn topology_from_services(services: &[ServiceSnapshot]) -> TopologyGraph {
    let mut edges = Vec::new();
    for service in services {
        for dependency in &service.dependencies {
            edges.push(TopologyEdge {
                from: service.service_key.clone(),
                to: dependency.clone(),
                reason: "dependency".to_string(),
            });
        }
    }
    TopologyGraph {
        services: services.to_vec(),
        edges,
    }
}
