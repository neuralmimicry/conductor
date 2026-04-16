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
    pub integrations: IntegrationsConfig,
    pub planning: PlanningConfig,
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
    pub refresh_interval_seconds: u64,
    pub probe_services: bool,
    pub service_timeout_seconds: u64,
    pub repo_hints: RepoHints,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RepoHints {
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ExternalServiceConfig {
    pub enabled: bool,
    pub base_url: Option<String>,
    pub bearer_token: Option<String>,
    pub timeout_seconds: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PlanningConfig {
    pub refresh_interval_seconds: u64,
    pub auto_queue: bool,
    pub gail_workflow: String,
    pub minimum_priority: i32,
}

impl Default for ConductorConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            security: SecurityConfig::default(),
            storage: StorageConfig::default(),
            database: DatabaseConfig::default(),
            discovery: DiscoveryConfig::default(),
            integrations: IntegrationsConfig::default(),
            planning: PlanningConfig::default(),
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
            allow_dashboard_without_token: true,
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
            refresh_interval_seconds: 180,
            probe_services: true,
            service_timeout_seconds: 5,
            repo_hints: RepoHints::default(),
        }
    }
}

impl Default for RepoHints {
    fn default() -> Self {
        Self {
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
        }
    }
}

impl Default for ExternalServiceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_url: None,
            bearer_token: None,
            timeout_seconds: 5,
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
        if self.planning.refresh_interval_seconds == 0 {
            self.planning.refresh_interval_seconds = 240;
        }
        if self.discovery.service_timeout_seconds == 0 {
            self.discovery.service_timeout_seconds = 5;
        }
        if self.storage.root_dir.as_os_str().is_empty() {
            self.storage.root_dir = PathBuf::from("data");
        }
        if self.planning.gail_workflow.trim().is_empty() {
            self.planning.gail_workflow = "conductor_improvement_planner".to_string();
        }
        normalize_external_service(&mut self.integrations.gail);
        normalize_external_service(&mut self.integrations.tracey);
        normalize_external_service(&mut self.integrations.continuum);
        normalize_external_service(&mut self.integrations.refiner);
        normalize_external_service(&mut self.integrations.aarnn);
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
    if let Some(base_url) = &mut config.base_url {
        *base_url = base_url.trim_end_matches('/').to_string();
    }
    if config.timeout_seconds == 0 {
        config.timeout_seconds = 5;
    }
}

fn normalize_optional_string(value: &mut Option<String>) {
    *value = value
        .take()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty());
}
