use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ConductorConfig {
    pub server: ServerConfig,
    pub security: SecurityConfig,
    pub storage: StorageConfig,
    pub database: DatabaseConfig,
    pub discovery: DiscoveryConfig,
    pub delivery: DeliveryConfig,
    pub validation: ValidationConfig,
    pub self_test: SelfTestConfig,
    pub integrations: IntegrationsConfig,
    pub planning: PlanningConfig,
    pub execution: ExecutionConfig,
    pub policy: PolicyConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub public_base_url: Option<String>,
    pub dashboard_title: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    pub admin_token: Option<String>,
    pub allow_dashboard_without_token: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub root_dir: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub run_migrations: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct DiscoveryConfig {
    pub ansible_root: PathBuf,
    pub local_repo_root: PathBuf,
    pub refresh_interval_seconds: u64,
    pub probe_services: bool,
    pub service_timeout_seconds: u64,
    pub github: GitHubDiscoveryConfig,
    pub repo_hints: RepoHints,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct GitHubDiscoveryConfig {
    pub enabled: bool,
    pub api_base_url: String,
    pub owner: String,
    pub token: Option<String>,
    pub timeout_seconds: u64,
    pub max_repositories: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RepoHints {
    pub conductor_repo: PathBuf,
    pub gail_repo: PathBuf,
    pub tracey_repo: PathBuf,
    pub continuum_repo: PathBuf,
    pub refiner_repo: PathBuf,
    pub aarnn_repo: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct IntegrationsConfig {
    pub gail: ExternalServiceConfig,
    pub tracey: ExternalServiceConfig,
    pub continuum: ExternalServiceConfig,
    pub refiner: ExternalServiceConfig,
    pub aarnn: ExternalServiceConfig,
    pub ollama: ExternalServiceConfig,
    pub atlassian: AtlassianConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct DeliveryConfig {
    pub auto_advance: bool,
    pub dora_window_days: i64,
    pub require_uat_before_production: bool,
    pub production_canary_percentage: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ValidationConfig {
    pub enabled: bool,
    pub require_success: bool,
    pub allow_missing_tooling: bool,
    pub timeout_seconds: u64,
    pub max_output_bytes: usize,
    pub max_commands: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SelfTestConfig {
    pub enabled: bool,
    pub refresh_interval_seconds: u64,
    pub auto_queue_regression_work_item: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ExternalServiceConfig {
    pub enabled: bool,
    pub base_url: Option<String>,
    pub bearer_token: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub timeout_seconds: u64,
    pub sync_interval_seconds: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct AtlassianConfig {
    pub enabled: bool,
    pub base_url: Option<String>,
    pub username: Option<String>,
    pub api_token: Option<String>,
    pub timeout_seconds: u64,
    pub sync_interval_seconds: u64,
    pub jira_project_key: Option<String>,
    pub jira_issue_type: String,
    pub confluence_space_key: Option<String>,
    pub confluence_parent_page_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PlanningConfig {
    pub refresh_interval_seconds: u64,
    pub auto_queue: bool,
    pub gail_workflow: String,
    pub minimum_priority: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    pub enabled: bool,
    pub dry_run: bool,
    pub emergency_stop: bool,
    pub refresh_interval_seconds: u64,
    pub poll_interval_seconds: u64,
    pub job_timeout_seconds: u64,
    pub claim_ttl_seconds: u64,
    pub max_concurrent_executions: usize,
    pub instance_id: Option<String>,
    pub use_local_project_root: bool,
    pub refiner_workflow: String,
    pub token_scope: String,
    pub llm_provider: Option<String>,
    pub llm_model: Option<String>,
    pub coding_agent: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PolicyConfig {
    pub enabled: bool,
    pub require_admin_approval: bool,
    pub require_verification: bool,
    pub require_refiner_strict_mode: bool,
    pub allow_external_repo_execution: bool,
    pub require_successful_github_actions_for_production: bool,
    pub github_actions_workflow_file: String,
    pub ai_approvals_enabled: bool,
    pub ai_approval_interval_seconds: u64,
    pub ai_approval_workflow: String,
    pub ai_approval_min_confidence: f64,
    pub ai_approval_max_items_per_cycle: usize,
    pub protected_services: Vec<String>,
    pub protected_repo_roots: Vec<PathBuf>,
    pub blocked_action_keywords: Vec<String>,
}

impl Default for ConductorConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            security: SecurityConfig::default(),
            storage: StorageConfig::default(),
            database: DatabaseConfig::default(),
            discovery: DiscoveryConfig::default(),
            delivery: DeliveryConfig::default(),
            validation: ValidationConfig::default(),
            self_test: SelfTestConfig::default(),
            integrations: IntegrationsConfig::default(),
            planning: PlanningConfig::default(),
            execution: ExecutionConfig::default(),
            policy: PolicyConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:8091".to_string(),
            public_base_url: None,
            dashboard_title: "NeuralMimicry Conductor".to_string(),
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            admin_token: None,
            allow_dashboard_without_token: false,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("data"),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgres://conductor:conductor@127.0.0.1:5432/conductor".to_string(),
            max_connections: 10,
            run_migrations: true,
        }
    }
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            ansible_root: PathBuf::from("/home/pbisaacs/Developer/swarmhpc/swarmhpc/ansible"),
            local_repo_root: PathBuf::from("/home/pbisaacs/Developer/neuralmimicry"),
            refresh_interval_seconds: 180,
            probe_services: true,
            service_timeout_seconds: 5,
            github: GitHubDiscoveryConfig::default(),
            repo_hints: RepoHints::default(),
        }
    }
}

impl Default for GitHubDiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            api_base_url: "https://api.github.com".to_string(),
            owner: "neuralmimicry".to_string(),
            token: None,
            timeout_seconds: 10,
            max_repositories: 200,
        }
    }
}

impl Default for RepoHints {
    fn default() -> Self {
        Self {
            conductor_repo: PathBuf::from("/home/pbisaacs/Developer/neuralmimicry/conductor"),
            gail_repo: PathBuf::from("/home/pbisaacs/Developer/neuralmimicry/gail"),
            tracey_repo: PathBuf::from("/home/pbisaacs/Developer/neuralmimicry/tracey"),
            continuum_repo: PathBuf::from("/home/pbisaacs/Developer/neuralmimicry/nmc"),
            refiner_repo: PathBuf::from("/home/pbisaacs/Developer/neuralmimicry/rag_demo"),
            aarnn_repo: PathBuf::from("/home/pbisaacs/Developer/neuralmimicry/aarnn_rust"),
        }
    }
}

impl Default for IntegrationsConfig {
    fn default() -> Self {
        Self {
            gail: ExternalServiceConfig::default(),
            tracey: ExternalServiceConfig::default(),
            continuum: ExternalServiceConfig::default(),
            refiner: ExternalServiceConfig::default(),
            aarnn: ExternalServiceConfig::default(),
            ollama: ExternalServiceConfig::default(),
            atlassian: AtlassianConfig::default(),
        }
    }
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            auto_advance: true,
            dora_window_days: 30,
            require_uat_before_production: true,
            production_canary_percentage: 10,
        }
    }
}

impl Default for ExternalServiceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_url: None,
            bearer_token: None,
            username: None,
            password: None,
            timeout_seconds: 5,
            sync_interval_seconds: 0,
        }
    }
}

impl Default for AtlassianConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_url: None,
            username: None,
            api_token: None,
            timeout_seconds: 15,
            sync_interval_seconds: 600,
            jira_project_key: None,
            jira_issue_type: "Task".to_string(),
            confluence_space_key: None,
            confluence_parent_page_id: None,
        }
    }
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            require_success: true,
            allow_missing_tooling: true,
            timeout_seconds: 600,
            max_output_bytes: 4096,
            max_commands: 6,
        }
    }
}

impl Default for SelfTestConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_interval_seconds: 900,
            auto_queue_regression_work_item: true,
        }
    }
}

impl Default for PlanningConfig {
    fn default() -> Self {
        Self {
            refresh_interval_seconds: 240,
            auto_queue: true,
            gail_workflow: "conductor_improvement_planner".to_string(),
            minimum_priority: 40,
        }
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dry_run: false,
            emergency_stop: false,
            refresh_interval_seconds: 120,
            poll_interval_seconds: 5,
            job_timeout_seconds: 900,
            claim_ttl_seconds: 1200,
            max_concurrent_executions: 1,
            instance_id: None,
            use_local_project_root: true,
            refiner_workflow: "project_solver".to_string(),
            token_scope: "personal".to_string(),
            llm_provider: None,
            llm_model: None,
            coding_agent: Some("project_solver".to_string()),
        }
    }
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            require_admin_approval: true,
            require_verification: true,
            require_refiner_strict_mode: true,
            allow_external_repo_execution: true,
            require_successful_github_actions_for_production: true,
            github_actions_workflow_file: "ci.yml".to_string(),
            ai_approvals_enabled: true,
            ai_approval_interval_seconds: 60,
            ai_approval_workflow: "conductor_safe_approval".to_string(),
            ai_approval_min_confidence: 0.7,
            ai_approval_max_items_per_cycle: 20,
            protected_services: vec![
                "conductor".to_string(),
                "gail".to_string(),
                "refiner".to_string(),
                "aarnn".to_string(),
                "swarmhpc".to_string(),
                "ollama".to_string(),
            ],
            protected_repo_roots: Vec::new(),
            blocked_action_keywords: vec![
                "reset --hard".to_string(),
                "rm -rf".to_string(),
                "wipe".to_string(),
                "destroy".to_string(),
            ],
        }
    }
}

impl ConductorConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let rendered = interpolate_env(&raw);
        let mut config: Self = serde_yaml::from_str(&rendered)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config.normalize()?;
        Ok(config)
    }

    pub fn normalize(&mut self) -> Result<()> {
        if self.server.bind_addr.trim().is_empty() {
            return Err(anyhow!("server.bind_addr must not be empty"));
        }
        if self.database.url.trim().is_empty() {
            return Err(anyhow!("database.url must not be empty"));
        }
        normalize_optional_string(&mut self.server.public_base_url);
        normalize_optional_string(&mut self.security.admin_token);
        if self.discovery.refresh_interval_seconds == 0 {
            self.discovery.refresh_interval_seconds = 180;
        }
        if self.discovery.ansible_root.as_os_str().is_empty() {
            self.discovery.ansible_root =
                PathBuf::from("/home/pbisaacs/Developer/swarmhpc/swarmhpc/ansible");
        }
        if self.discovery.local_repo_root.as_os_str().is_empty() {
            self.discovery.local_repo_root =
                PathBuf::from("/home/pbisaacs/Developer/neuralmimicry");
        }
        if self
            .discovery
            .repo_hints
            .conductor_repo
            .as_os_str()
            .is_empty()
        {
            self.discovery.repo_hints.conductor_repo =
                self.discovery.local_repo_root.join("conductor");
        }
        if self.discovery.repo_hints.gail_repo.as_os_str().is_empty() {
            self.discovery.repo_hints.gail_repo = self.discovery.local_repo_root.join("gail");
        }
        if self.discovery.repo_hints.tracey_repo.as_os_str().is_empty() {
            self.discovery.repo_hints.tracey_repo = self.discovery.local_repo_root.join("tracey");
        }
        if self
            .discovery
            .repo_hints
            .continuum_repo
            .as_os_str()
            .is_empty()
        {
            self.discovery.repo_hints.continuum_repo = self.discovery.local_repo_root.join("nmc");
        }
        if self
            .discovery
            .repo_hints
            .refiner_repo
            .as_os_str()
            .is_empty()
        {
            self.discovery.repo_hints.refiner_repo =
                self.discovery.local_repo_root.join("rag_demo");
        }
        if self.discovery.repo_hints.aarnn_repo.as_os_str().is_empty() {
            self.discovery.repo_hints.aarnn_repo =
                self.discovery.local_repo_root.join("aarnn_rust");
        }
        if self.delivery.dora_window_days <= 0 {
            self.delivery.dora_window_days = 30;
        }
        if self.delivery.production_canary_percentage == 0 {
            self.delivery.production_canary_percentage = 10;
        }
        self.delivery.production_canary_percentage =
            self.delivery.production_canary_percentage.clamp(1, 100);
        if self.validation.timeout_seconds == 0 {
            self.validation.timeout_seconds = 600;
        }
        self.validation.max_output_bytes = self.validation.max_output_bytes.clamp(256, 65_536);
        self.validation.max_commands = self.validation.max_commands.clamp(1, 20);
        if self.self_test.refresh_interval_seconds == 0 {
            self.self_test.refresh_interval_seconds = 900;
        }
        self.self_test.refresh_interval_seconds = self.self_test.refresh_interval_seconds.max(60);
        if self.planning.refresh_interval_seconds == 0 {
            self.planning.refresh_interval_seconds = 240;
        }
        if self.execution.refresh_interval_seconds == 0 {
            self.execution.refresh_interval_seconds = 120;
        }
        if self.execution.poll_interval_seconds == 0 {
            self.execution.poll_interval_seconds = 5;
        }
        if self.execution.job_timeout_seconds == 0 {
            self.execution.job_timeout_seconds = 900;
        }
        if self.execution.claim_ttl_seconds == 0 {
            self.execution.claim_ttl_seconds =
                self.execution.job_timeout_seconds.saturating_add(300);
        }
        self.execution.claim_ttl_seconds = self.execution.claim_ttl_seconds.max(60);
        if self.discovery.service_timeout_seconds == 0 {
            self.discovery.service_timeout_seconds = 5;
        }
        if self.discovery.github.api_base_url.trim().is_empty() {
            self.discovery.github.api_base_url = "https://api.github.com".to_string();
        } else {
            self.discovery.github.api_base_url = self
                .discovery
                .github
                .api_base_url
                .trim_end_matches('/')
                .to_string();
        }
        self.discovery.github.owner = self.discovery.github.owner.trim().to_string();
        normalize_optional_string(&mut self.discovery.github.token);
        if self.discovery.github.timeout_seconds == 0 {
            self.discovery.github.timeout_seconds = 10;
        }
        if self.discovery.github.max_repositories == 0 {
            self.discovery.github.max_repositories = 200;
        }
        if self.execution.max_concurrent_executions == 0 {
            self.execution.max_concurrent_executions = 1;
        }
        if self.storage.root_dir.as_os_str().is_empty() {
            self.storage.root_dir = PathBuf::from("data");
        }
        if self.planning.gail_workflow.trim().is_empty() {
            self.planning.gail_workflow = "conductor_improvement_planner".to_string();
        }
        if self.execution.refiner_workflow.trim().is_empty() {
            self.execution.refiner_workflow = "project_solver".to_string();
        }
        if self.execution.token_scope.trim().is_empty() {
            self.execution.token_scope = "personal".to_string();
        }
        normalize_optional_string(&mut self.execution.instance_id);
        if self.execution.instance_id.is_none() {
            let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "local".to_string());
            self.execution.instance_id =
                Some(format!("conductor-{}-{}", host.trim(), std::process::id()));
        }
        normalize_optional_string(&mut self.execution.llm_provider);
        normalize_optional_string(&mut self.execution.llm_model);
        normalize_optional_string(&mut self.execution.coding_agent);
        normalize_external_service(&mut self.integrations.gail);
        normalize_external_service(&mut self.integrations.tracey);
        normalize_external_service(&mut self.integrations.continuum);
        normalize_external_service(&mut self.integrations.refiner);
        normalize_external_service(&mut self.integrations.aarnn);
        normalize_external_service(&mut self.integrations.ollama);
        normalize_atlassian(&mut self.integrations.atlassian);
        if self.policy.ai_approval_interval_seconds == 0 {
            self.policy.ai_approval_interval_seconds = 60;
        }
        self.policy.ai_approval_interval_seconds = self.policy.ai_approval_interval_seconds.max(15);
        if self.policy.github_actions_workflow_file.trim().is_empty() {
            self.policy.github_actions_workflow_file = "ci.yml".to_string();
        } else {
            self.policy.github_actions_workflow_file =
                self.policy.github_actions_workflow_file.trim().to_string();
        }
        if self.policy.ai_approval_workflow.trim().is_empty() {
            self.policy.ai_approval_workflow = "conductor_safe_approval".to_string();
        } else {
            self.policy.ai_approval_workflow = self.policy.ai_approval_workflow.trim().to_string();
        }
        if !self.policy.ai_approval_min_confidence.is_finite() {
            self.policy.ai_approval_min_confidence = 0.7;
        }
        self.policy.ai_approval_min_confidence =
            self.policy.ai_approval_min_confidence.clamp(0.0, 1.0);
        if self.policy.ai_approval_max_items_per_cycle == 0 {
            self.policy.ai_approval_max_items_per_cycle = 20;
        }
        normalize_unique_strings(&mut self.policy.protected_services);
        normalize_unique_strings(&mut self.policy.blocked_action_keywords);
        normalize_paths(&mut self.policy.protected_repo_roots);
        if self.policy.protected_repo_roots.is_empty() {
            let mut protected_roots = vec![
                self.discovery.repo_hints.conductor_repo.clone(),
                self.discovery.repo_hints.gail_repo.clone(),
                self.discovery.repo_hints.tracey_repo.clone(),
                self.discovery.repo_hints.continuum_repo.clone(),
                self.discovery.repo_hints.refiner_repo.clone(),
                self.discovery.repo_hints.aarnn_repo.clone(),
            ];
            if let Some(ansible_repo_root) = self
                .discovery
                .ansible_root
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .map(Path::to_path_buf)
            {
                protected_roots.push(ansible_repo_root);
            }
            self.policy.protected_repo_roots = protected_roots;
            normalize_paths(&mut self.policy.protected_repo_roots);
        }
        Ok(())
    }
}

fn interpolate_env(input: &str) -> String {
    static ENV_PATTERN: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let regex = ENV_PATTERN.get_or_init(|| Regex::new(r"\$\{([A-Z0-9_]+)\}").expect("regex"));
    regex
        .replace_all(input, |caps: &regex::Captures<'_>| {
            std::env::var(&caps[1]).unwrap_or_default()
        })
        .into_owned()
}

fn normalize_external_service(config: &mut ExternalServiceConfig) {
    normalize_optional_string(&mut config.base_url);
    normalize_optional_string(&mut config.bearer_token);
    normalize_optional_string(&mut config.username);
    normalize_optional_string(&mut config.password);
    if let Some(base_url) = &mut config.base_url {
        *base_url = base_url.trim_end_matches('/').to_string();
    }
    if config.timeout_seconds == 0 {
        config.timeout_seconds = 5;
    }
    if config.sync_interval_seconds > 0 {
        config.sync_interval_seconds = config.sync_interval_seconds.max(60);
    }
}

fn normalize_atlassian(config: &mut AtlassianConfig) {
    normalize_optional_string(&mut config.base_url);
    normalize_optional_string(&mut config.username);
    normalize_optional_string(&mut config.api_token);
    normalize_optional_string(&mut config.jira_project_key);
    normalize_optional_string(&mut config.confluence_space_key);
    normalize_optional_string(&mut config.confluence_parent_page_id);
    if let Some(base_url) = &mut config.base_url {
        *base_url = base_url.trim_end_matches('/').to_string();
    }
    if config.timeout_seconds == 0 {
        config.timeout_seconds = 15;
    }
    if config.sync_interval_seconds > 0 {
        config.sync_interval_seconds = config.sync_interval_seconds.max(60);
    }
    if config.jira_issue_type.trim().is_empty() {
        config.jira_issue_type = "Task".to_string();
    } else {
        config.jira_issue_type = config.jira_issue_type.trim().to_string();
    }
}

fn normalize_optional_string(value: &mut Option<String>) {
    *value = value
        .take()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty());
}

fn normalize_unique_strings(values: &mut Vec<String>) {
    let mut seen = std::collections::BTreeSet::new();
    *values = values
        .drain(..)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .filter(|value| seen.insert(value.clone()))
        .collect();
}

fn normalize_paths(values: &mut Vec<PathBuf>) {
    let mut seen = std::collections::BTreeSet::new();
    *values = values
        .drain(..)
        .filter(|value| !value.as_os_str().is_empty())
        .filter(|value| seen.insert(value.display().to_string()))
        .collect();
}
