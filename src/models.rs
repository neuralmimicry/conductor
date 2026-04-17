use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
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

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Success | Self::Failure | Self::Aborted)
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    #[default]
    Pending,
    Planning,
    Submitted,
    Running,
    Verifying,
    Success,
    Failure,
    Blocked,
    Cancelled,
}

impl ExecutionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Planning => "planning",
            Self::Submitted => "submitted",
            Self::Running => "running",
            Self::Verifying => "verifying",
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.trim() {
            "pending" => Self::Pending,
            "planning" => Self::Planning,
            "submitted" => Self::Submitted,
            "running" => Self::Running,
            "verifying" => Self::Verifying,
            "success" => Self::Success,
            "failure" => Self::Failure,
            "blocked" => Self::Blocked,
            "cancelled" => Self::Cancelled,
            _ => Self::Pending,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failure | Self::Blocked | Self::Cancelled
        )
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PolicyVerdict {
    #[default]
    Allowed,
    NeedsApproval,
    Blocked,
}

impl PolicyVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::NeedsApproval => "needs_approval",
            Self::Blocked => "blocked",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.trim() {
            "allowed" => Self::Allowed,
            "needs_approval" => Self::NeedsApproval,
            "blocked" => Self::Blocked,
            _ => Self::Allowed,
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
    pub execution_approved: bool,
    pub verification_required: Option<bool>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub plan: Value,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub source: Option<String>,
    pub scheduled_for: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct WorkItemPatch {
    pub title: Option<String>,
    pub summary: Option<String>,
    pub target_service: Option<String>,
    pub status: Option<WorkStatus>,
    pub priority: Option<i32>,
    pub progress_pct: Option<i32>,
    pub admin_override: Option<bool>,
    pub execution_approved: Option<bool>,
    pub verification_required: Option<bool>,
    pub tags: Option<Vec<String>>,
    pub plan: Option<Value>,
    pub depends_on: Option<Vec<String>>,
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
    pub execution_approved: bool,
    pub verification_required: bool,
    pub source: String,
    pub tags: Vec<String>,
    pub plan: Value,
    pub depends_on: Vec<String>,
    pub notes: Vec<String>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub last_execution_id: Option<Uuid>,
    pub last_policy: Value,
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
            execution_approved: input.execution_approved,
            verification_required: input.verification_required.unwrap_or(true),
            source: input.source.unwrap_or_else(|| "manual".to_string()),
            tags: unique_strings(input.tags),
            plan: input.plan,
            depends_on: unique_strings(input.depends_on),
            notes: Vec::new(),
            scheduled_for: input.scheduled_for,
            started_at: None,
            finished_at: None,
            last_execution_id: None,
            last_policy: json!({}),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn apply_patch(&mut self, patch: WorkItemPatch) {
        if let Some(title) = patch.title {
            self.title = title;
        }
        if let Some(summary) = patch.summary {
            self.summary = summary;
        }
        if let Some(target_service) = patch.target_service {
            self.target_service = Some(target_service);
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
        if let Some(execution_approved) = patch.execution_approved {
            self.execution_approved = execution_approved;
        }
        if let Some(verification_required) = patch.verification_required {
            self.verification_required = verification_required;
        }
        if let Some(tags) = patch.tags {
            self.tags = unique_strings(tags);
        }
        if let Some(plan) = patch.plan {
            self.plan = plan;
        }
        if let Some(depends_on) = patch.depends_on {
            self.depends_on = unique_strings(depends_on);
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

    pub fn touch_execution(&mut self, execution_id: Uuid, policy: Value) {
        self.last_execution_id = Some(execution_id);
        self.last_policy = policy;
        self.updated_at = now_utc();
    }

    pub fn matches_reference(&self, reference: &str) -> bool {
        let reference = reference.trim();
        !reference.is_empty()
            && (self.id.to_string() == reference
                || self
                    .dedupe_key
                    .as_deref()
                    .is_some_and(|value| value == reference))
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
    pub repo_url: Option<String>,
    pub repo_branch: Option<String>,
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
    pub executions_total: usize,
    pub executions_running: usize,
    pub approvals_waiting: usize,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyEvaluation {
    pub verdict: PolicyVerdict,
    pub risk_level: String,
    pub protected_targets: Vec<String>,
    pub external_repos: Vec<String>,
    pub required_verifications: Vec<String>,
    pub reasons: Vec<String>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkExecution {
    pub id: Uuid,
    pub work_item_id: Uuid,
    pub target_service: Option<String>,
    pub status: ExecutionStatus,
    pub refiner_job_id: Option<String>,
    pub policy: Value,
    pub request_payload: Value,
    pub latest_payload: Value,
    pub verification: Value,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl WorkExecution {
    pub fn new(work_item_id: Uuid, target_service: Option<String>) -> Self {
        let now = now_utc();
        Self {
            id: Uuid::new_v4(),
            work_item_id,
            target_service,
            status: ExecutionStatus::Pending,
            refiner_job_id: None,
            policy: json!({}),
            request_payload: json!({}),
            latest_payload: json!({}),
            verification: json!({}),
            error: None,
            started_at: now,
            updated_at: now,
            finished_at: None,
        }
    }

    pub fn mark_status(&mut self, status: ExecutionStatus) {
        self.status = status;
        self.updated_at = now_utc();
        if status.is_terminal() {
            self.finished_at = Some(self.updated_at);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConductorEvent {
    pub id: Uuid,
    pub event_type: String,
    pub message: String,
    pub status: Option<String>,
    pub work_item_id: Option<Uuid>,
    pub execution_id: Option<Uuid>,
    pub refiner_job_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub payload: Value,
}

impl ConductorEvent {
    pub fn new(event_type: impl Into<String>, message: impl Into<String>, payload: Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            event_type: event_type.into(),
            message: message.into(),
            status: None,
            work_item_id: None,
            execution_id: None,
            refiner_job_id: None,
            created_at: now_utc(),
            payload,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceMetricSample {
    pub id: Uuid,
    pub discovery_run_id: Uuid,
    pub service_key: String,
    pub metric_source: String,
    pub metrics: Value,
    pub sampled_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricTrend {
    pub metric_name: String,
    pub latest: f64,
    pub average: f64,
    pub slope: f64,
    pub direction: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceTrendSummary {
    pub service_key: String,
    pub sample_count: usize,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub direction: String,
    pub headline: String,
    pub metrics: Vec<MetricTrend>,
    pub raw_latest: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicySummary {
    pub protected_services: Vec<String>,
    pub protected_repo_roots: Vec<String>,
    pub require_admin_approval: bool,
    pub require_verification: bool,
    pub require_refiner_strict_mode: bool,
    pub allow_external_repo_execution: bool,
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
