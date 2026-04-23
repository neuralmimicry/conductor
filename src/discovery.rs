use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use anyhow::{Result, anyhow};
use regex::Regex;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use walkdir::WalkDir;

use crate::{
    config::{ConductorConfig, GitHubDiscoveryConfig},
    integrations::probe_service,
    models::{
        DiscoveryRun, ProbeResult, RepositorySnapshot, RunStatus, ServiceHealth, ServiceSnapshot,
        now_utc, topology_from_services, unique_strings,
    },
};

pub struct DiscoveryOutput {
    pub services: Vec<ServiceSnapshot>,
    pub repositories: Vec<RepositorySnapshot>,
    pub run: DiscoveryRun,
}

#[derive(Clone, Debug)]
struct PlaybookDocument {
    file_name: String,
    hosts: Vec<String>,
    roles: Vec<String>,
    vars: Map<String, Value>,
    import_playbook: Option<String>,
}

#[derive(Clone, Debug)]
struct MutableService {
    service_key: String,
    display_name: String,
    kind: String,
    role_name: String,
    playbooks: BTreeSet<String>,
    host_targets: BTreeSet<String>,
    repo_path: Option<String>,
    capabilities: BTreeSet<String>,
    dependencies: BTreeSet<String>,
    storage_paths: BTreeSet<String>,
    vars: Map<String, Value>,
}

#[derive(Clone, Debug, Default)]
struct RepoMetadata {
    url: Option<String>,
    branch: Option<String>,
    default_branch: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct InventoryIndex {
    groups: BTreeMap<String, BTreeSet<String>>,
    children: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Clone, Debug)]
struct LocalRepository {
    name: String,
    local_path: PathBuf,
    remote_url: Option<String>,
    current_branch: Option<String>,
    default_branch: Option<String>,
    coordinate: Option<RepoCoordinate>,
    signals: RepositorySignals,
}

#[derive(Clone, Debug, Default)]
struct RepositorySignals {
    language: Option<String>,
    frameworks: Vec<String>,
    build_systems: Vec<String>,
    package_managers: Vec<String>,
    runtime_type: Option<String>,
    purpose: Option<String>,
    capabilities: Vec<String>,
    manifests: Vec<String>,
    has_container: bool,
    has_kubernetes_manifests: bool,
    has_tests: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RepoCoordinate {
    owner: Option<String>,
    name: String,
}

#[derive(Clone, Debug, Deserialize)]
struct GitHubRepository {
    name: String,
    full_name: String,
    html_url: Option<String>,
    clone_url: Option<String>,
    ssh_url: Option<String>,
    default_branch: Option<String>,
    language: Option<String>,
    archived: bool,
    private: bool,
    description: Option<String>,
    topics: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
struct MutableRepository {
    repo_key: String,
    name: String,
    owner: Option<String>,
    repo_url: Option<String>,
    local_path: Option<String>,
    default_branch: Option<String>,
    current_branch: Option<String>,
    language: Option<String>,
    frameworks: BTreeSet<String>,
    build_systems: BTreeSet<String>,
    package_managers: BTreeSet<String>,
    runtime_type: Option<String>,
    deployment_type: Option<String>,
    purpose: Option<String>,
    criticality: String,
    visibility: Option<String>,
    archived: bool,
    linked_services: BTreeSet<String>,
    dependencies: BTreeSet<String>,
    capabilities: BTreeSet<String>,
    inventory_sources: BTreeSet<String>,
    metadata: Map<String, Value>,
    has_container: bool,
    has_kubernetes_manifests: bool,
    has_tests: bool,
}

impl MutableService {
    fn new(
        service_key: impl Into<String>,
        display_name: impl Into<String>,
        kind: impl Into<String>,
        role_name: impl Into<String>,
    ) -> Self {
        Self {
            service_key: service_key.into(),
            display_name: display_name.into(),
            kind: kind.into(),
            role_name: role_name.into(),
            playbooks: BTreeSet::new(),
            host_targets: BTreeSet::new(),
            repo_path: None,
            capabilities: BTreeSet::new(),
            dependencies: BTreeSet::new(),
            storage_paths: BTreeSet::new(),
            vars: Map::new(),
        }
    }

    fn absorb(
        &mut self,
        playbook: &str,
        hosts: &[String],
        vars: &Map<String, Value>,
        imported_dependencies: &[String],
    ) {
        self.playbooks.insert(playbook.to_string());
        self.host_targets.extend(hosts.iter().cloned());
        overlay_map(&mut self.vars, vars);
        self.dependencies
            .extend(imported_dependencies.iter().cloned());
    }

    fn finalize(
        mut self,
        config: &ConductorConfig,
        inventory: &InventoryIndex,
        local_repositories: &[LocalRepository],
    ) -> ServiceSnapshot {
        let now = now_utc();
        let repo_path =
            resolve_repo_path(config, &self.service_key, &self.vars, local_repositories)
                .filter(|path| path.exists());
        let repo_metadata = repo_path.as_deref().map(repo_metadata).unwrap_or_default();
        self.repo_path = repo_path.map(|path| path.display().to_string());

        let namespace = pick_string(
            &self.vars,
            &[
                &format!("continuum_tenant_{}_namespace", self.service_key),
                "continuum_tenant_k8s_app_namespace",
            ],
        )
        .or_else(|| Some(self.service_key.clone()));

        let service_name = pick_string(
            &self.vars,
            &[
                &format!("continuum_tenant_{}_service_name", self.service_key),
                "continuum_tenant_k8s_app_service_name",
                "continuum_tenant_k8s_app_name",
            ],
        )
        .or_else(|| Some(self.service_key.clone()));
        let deployment_environment = infer_delivery_environment(&self.vars, &self.service_key);

        let port = pick_port(
            &self.vars,
            &[
                &format!("continuum_tenant_{}_port", self.service_key),
                &format!("continuum_tenant_{}_host_port", self.service_key),
                "continuum_tenant_k8s_app_ingress_service_port",
                "ollama_service_port",
            ],
        );

        let tls_enabled = pick_bool(
            &self.vars,
            &[
                &format!("continuum_tenant_{}_enable_tls", self.service_key),
                "continuum_tenant_k8s_app_enable_tls",
            ],
        )
        .unwrap_or(true);

        let public_host = pick_string(
            &self.vars,
            &[
                &format!("continuum_tenant_{}_ingress_hostname", self.service_key),
                &format!("continuum_shared_{}_public_hostname", self.service_key),
                "continuum_tenant_k8s_app_ingress_hostname",
                "continuum_shared_auth_api_public_base_url",
                &format!("{}_public_base_url", self.service_key),
            ],
        );

        let public_url = public_host
            .as_ref()
            .and_then(|raw| normalize_url(raw, tls_enabled));

        let internal_url = pick_string(
            &self.vars,
            &[
                &format!("continuum_tenant_{}_internal_url", self.service_key),
                &format!(
                    "continuum_shared_{}_api_internal_base_url",
                    self.service_key
                ),
                "continuum_public_base_url",
            ],
        )
        .and_then(|raw| normalize_url(&raw, false))
        .or_else(|| {
            if let (Some(service_name), Some(namespace), Some(port)) =
                (service_name.clone(), namespace.clone(), port)
            {
                Some(format!(
                    "http://{}.{}.svc.cluster.local:{}",
                    service_name, namespace, port
                ))
            } else {
                None
            }
        });

        let inferred_dependencies = infer_dependencies(&self.vars);
        self.dependencies.extend(inferred_dependencies);

        infer_storage_paths(&self.vars, &mut self.storage_paths);
        infer_capabilities(
            &self.service_key,
            &self.vars,
            &mut self.capabilities,
            public_url.is_some(),
            self.repo_path.is_some(),
        );

        let host_targets = self.host_targets.into_iter().collect::<Vec<_>>();
        let hosts = inventory.resolve_targets(&host_targets);
        let repo_url = repo_metadata
            .url
            .or_else(|| extract_repo_url(&self.vars, &self.service_key));
        let repo_branch = repo_metadata
            .branch
            .or_else(|| extract_repo_branch(&self.vars, &self.service_key));
        let mut raw_defaults = self.vars;
        if let Some(default_branch) = repo_metadata.default_branch {
            raw_defaults.insert(
                "conductor_repo_default_branch".to_string(),
                Value::String(default_branch),
            );
        }

        ServiceSnapshot {
            service_key: self.service_key,
            display_name: self.display_name,
            kind: self.kind,
            role_name: self.role_name,
            playbooks: self.playbooks.into_iter().collect(),
            host_targets,
            hosts,
            namespace,
            service_name,
            deployment_environment,
            internal_url,
            public_url,
            repo_path: self.repo_path,
            repo_url,
            repo_branch,
            health: ServiceHealth::Unknown,
            capabilities: self.capabilities.into_iter().collect(),
            dependencies: self.dependencies.into_iter().collect(),
            storage_paths: self.storage_paths.into_iter().collect(),
            raw_defaults: Value::Object(raw_defaults),
            probe: json!({}),
            discovered_at: now,
            updated_at: now,
        }
    }
}

fn infer_delivery_environment(
    vars: &Map<String, Value>,
    service_key: &str,
) -> Option<crate::models::DeliveryStage> {
    let explicit = pick_string(
        vars,
        &[
            &format!("continuum_tenant_{}_environment", service_key),
            &format!("continuum_{}_tenant_environment", service_key),
            "continuum_tenant_environment",
        ],
    );
    if let Some(value) = explicit {
        return Some(crate::models::DeliveryStage::from_db(&value));
    }

    for (key, value) in vars {
        if !key.ends_with("_tenant_environment") {
            continue;
        }
        if !key.contains(service_key) {
            continue;
        }
        if let Some(text) = value.as_str() {
            return Some(crate::models::DeliveryStage::from_db(text));
        }
    }
    None
}

impl InventoryIndex {
    fn resolve_targets(&self, targets: &[String]) -> Vec<String> {
        let mut resolved = BTreeSet::new();
        for target in targets {
            for token in target.split([',', ':']) {
                let token = token.trim();
                if token.is_empty() || token.contains("{{") {
                    continue;
                }
                if self.groups.contains_key(token) || self.children.contains_key(token) {
                    self.expand_group(token, &mut BTreeSet::new(), &mut resolved);
                } else {
                    resolved.insert(token.to_string());
                }
            }
        }
        resolved.into_iter().collect()
    }

    fn expand_group(
        &self,
        group: &str,
        seen: &mut BTreeSet<String>,
        resolved: &mut BTreeSet<String>,
    ) {
        if !seen.insert(group.to_string()) {
            return;
        }
        if let Some(hosts) = self.groups.get(group) {
            resolved.extend(hosts.iter().cloned());
        }
        if let Some(children) = self.children.get(group) {
            for child in children {
                self.expand_group(child, seen, resolved);
            }
        }
    }
}

impl MutableRepository {
    fn new(repo_key: String, name: String) -> Self {
        let criticality = criticality_for_repository_name(&name);
        let purpose = purpose_for_repository_name(&name);
        let runtime_type = runtime_type_for_repository_name(&name);
        Self {
            repo_key,
            name,
            owner: None,
            repo_url: None,
            local_path: None,
            default_branch: None,
            current_branch: None,
            language: None,
            frameworks: BTreeSet::new(),
            build_systems: BTreeSet::new(),
            package_managers: BTreeSet::new(),
            runtime_type,
            deployment_type: None,
            purpose,
            criticality,
            visibility: None,
            archived: false,
            linked_services: BTreeSet::new(),
            dependencies: BTreeSet::new(),
            capabilities: BTreeSet::new(),
            inventory_sources: BTreeSet::new(),
            metadata: Map::new(),
            has_container: false,
            has_kubernetes_manifests: false,
            has_tests: false,
        }
    }

    fn absorb_local(&mut self, repository: &LocalRepository) {
        self.owner = self.owner.clone().or_else(|| {
            repository
                .coordinate
                .as_ref()
                .and_then(|coordinate| coordinate.owner.clone())
        });
        self.repo_url = self
            .repo_url
            .clone()
            .or_else(|| repository.remote_url.clone());
        self.local_path = Some(repository.local_path.display().to_string());
        self.default_branch = self
            .default_branch
            .clone()
            .or_else(|| repository.default_branch.clone());
        self.current_branch = self
            .current_branch
            .clone()
            .or_else(|| repository.current_branch.clone());
        self.language = self
            .language
            .clone()
            .or_else(|| repository.signals.language.clone());
        self.frameworks
            .extend(repository.signals.frameworks.iter().cloned());
        self.build_systems
            .extend(repository.signals.build_systems.iter().cloned());
        self.package_managers
            .extend(repository.signals.package_managers.iter().cloned());
        self.runtime_type = self
            .runtime_type
            .clone()
            .or_else(|| repository.signals.runtime_type.clone());
        self.purpose = self
            .purpose
            .clone()
            .or_else(|| repository.signals.purpose.clone());
        self.capabilities
            .extend(repository.signals.capabilities.iter().cloned());
        self.inventory_sources.insert("local_git".to_string());
        self.has_container |= repository.signals.has_container;
        self.has_kubernetes_manifests |= repository.signals.has_kubernetes_manifests;
        self.has_tests |= repository.signals.has_tests;
        self.metadata.insert(
            "manifests".to_string(),
            Value::Array(
                repository
                    .signals
                    .manifests
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
        self.metadata
            .insert("has_container".to_string(), Value::Bool(self.has_container));
        self.metadata.insert(
            "has_kubernetes_manifests".to_string(),
            Value::Bool(self.has_kubernetes_manifests),
        );
        self.metadata
            .insert("has_tests".to_string(), Value::Bool(self.has_tests));
        promote_criticality(
            &mut self.criticality,
            &criticality_for_repository_name(&self.name),
        );
    }

    fn absorb_github(&mut self, repository: &GitHubRepository) {
        self.owner = self
            .owner
            .clone()
            .or_else(|| owner_from_full_name(&repository.full_name));
        self.repo_url = self
            .repo_url
            .clone()
            .or_else(|| repository.ssh_url.clone())
            .or_else(|| repository.clone_url.clone())
            .or_else(|| repository.html_url.clone());
        self.default_branch = self
            .default_branch
            .clone()
            .or_else(|| repository.default_branch.clone());
        self.language = self
            .language
            .clone()
            .or_else(|| repository.language.clone());
        self.visibility = Some(if repository.private {
            "private".to_string()
        } else {
            "public".to_string()
        });
        self.archived = repository.archived;
        self.inventory_sources.insert("github_api".to_string());
        if let Some(description) = &repository.description {
            self.metadata.insert(
                "description".to_string(),
                Value::String(description.clone()),
            );
        }
        if let Some(html_url) = &repository.html_url {
            self.metadata
                .insert("html_url".to_string(), Value::String(html_url.clone()));
        }
        if let Some(topics) = &repository.topics {
            self.metadata.insert(
                "topics".to_string(),
                Value::Array(topics.iter().cloned().map(Value::String).collect()),
            );
        }
        promote_criticality(
            &mut self.criticality,
            &criticality_for_repository_name(&self.name),
        );
    }

    fn absorb_service(&mut self, service: &ServiceSnapshot) {
        self.linked_services.insert(service.service_key.clone());
        self.inventory_sources.insert("ansible_service".to_string());
        self.repo_url = self.repo_url.clone().or_else(|| service.repo_url.clone());
        self.local_path = self
            .local_path
            .clone()
            .or_else(|| service.repo_path.clone());
        if self.deployment_type.is_none() {
            self.deployment_type = Some(deployment_type_for_service(service));
        }
        if self.runtime_type.is_none() {
            self.runtime_type = Some(runtime_type_for_service(service));
        }
        if self.purpose.is_none() {
            self.purpose = Some(purpose_for_service(service));
        }
        self.capabilities
            .extend(service.capabilities.iter().cloned());
        promote_criticality(&mut self.criticality, &criticality_for_service(service));
        self.metadata.insert(
            "service_kind".to_string(),
            Value::String(service.kind.clone()),
        );
        if let Some(public_url) = &service.public_url {
            self.metadata.insert(
                format!("service_public_url:{}", service.service_key),
                Value::String(public_url.clone()),
            );
        }
        if let Some(internal_url) = &service.internal_url {
            self.metadata.insert(
                format!("service_internal_url:{}", service.service_key),
                Value::String(internal_url.clone()),
            );
        }
    }

    fn finalize(mut self) -> RepositorySnapshot {
        if self.purpose.is_none() {
            self.purpose = Some(purpose_for_repository(&self));
        }
        if self.deployment_type.is_none() {
            self.deployment_type = Some(deployment_type_for_repository(&self));
        }
        if self.runtime_type.is_none() {
            self.runtime_type = Some(runtime_type_for_repository(&self));
        }
        if self.has_container {
            self.capabilities.insert("containerised".to_string());
        }
        if self.has_kubernetes_manifests {
            self.capabilities.insert("kubernetes".to_string());
        }
        if self.has_tests {
            self.capabilities.insert("tests".to_string());
        }

        RepositorySnapshot {
            repo_key: self.repo_key,
            name: self.name,
            owner: self.owner,
            repo_url: self.repo_url,
            local_path: self.local_path,
            default_branch: self.default_branch,
            current_branch: self.current_branch,
            language: self.language,
            frameworks: self.frameworks.into_iter().collect(),
            build_systems: self.build_systems.into_iter().collect(),
            package_managers: self.package_managers.into_iter().collect(),
            runtime_type: self.runtime_type,
            deployment_type: self.deployment_type,
            purpose: self.purpose,
            criticality: self.criticality,
            visibility: self.visibility,
            archived: self.archived,
            linked_services: self.linked_services.into_iter().collect(),
            dependencies: self.dependencies.into_iter().collect(),
            capabilities: self.capabilities.into_iter().collect(),
            inventory_sources: self.inventory_sources.into_iter().collect(),
            metadata: Value::Object(self.metadata),
            discovered_at: now_utc(),
            updated_at: now_utc(),
        }
    }
}

pub async fn discover_and_probe(
    config: &ConductorConfig,
    client: &Client,
) -> Result<DiscoveryOutput> {
    let started_at = now_utc();
    let ansible_root = &config.discovery.ansible_root;
    if !ansible_root.exists() {
        return Err(anyhow!(
            "ansible root {} does not exist",
            ansible_root.display()
        ));
    }

    let mut issues = Vec::new();
    let inventory = match load_inventory_hosts(ansible_root) {
        Ok(inventory) => inventory,
        Err(error) => {
            issues.push(format!("inventory host parsing failed: {}", error));
            InventoryIndex::default()
        }
    };
    let local_repositories = match discover_local_repositories(&config.discovery.local_repo_root) {
        Ok(repositories) => repositories,
        Err(error) => {
            issues.push(format!("local repository inventory failed: {}", error));
            Vec::new()
        }
    };

    let role_defaults = load_role_defaults(ansible_root)?;
    let global_vars = load_global_vars(ansible_root)?;
    let playbooks = load_playbooks(ansible_root)?;
    let mut services = collect_services(
        config,
        &playbooks,
        &role_defaults,
        &global_vars,
        &inventory,
        &local_repositories,
    );

    if config.discovery.probe_services {
        for service in &mut services {
            match probe_service(client, config, service).await {
                Ok(probe) => apply_probe(service, probe),
                Err(error) => {
                    let error_text = error.to_string();
                    service.health = classify_probe_error(&error_text);
                    service.updated_at = now_utc();
                    service.probe = json!({"error": error_text});
                }
            }
        }

        for service in &services {
            if matches!(
                service.health,
                ServiceHealth::Degraded | ServiceHealth::Unreachable | ServiceHealth::Missing
            ) {
                issues.push(format!(
                    "{} probe issue: {}",
                    service.service_key, service.probe
                ));
            }
        }
    }

    let github_repositories = match load_github_repositories(client, &config.discovery.github).await
    {
        Ok(repositories) => repositories,
        Err(error) => {
            issues.push(format!("github inventory failed: {}", error));
            Vec::new()
        }
    };
    let repositories =
        collect_repository_snapshots(&services, &local_repositories, &github_repositories);

    let topology = json!({
        "services": topology_from_services(&services),
        "repositories": repositories,
    });
    let run = DiscoveryRun {
        id: uuid::Uuid::new_v4(),
        status: if issues.is_empty() {
            RunStatus::Success
        } else {
            RunStatus::PartialFailure
        },
        services_count: services.len(),
        repositories_count: repositories.len(),
        issues,
        topology,
        started_at,
        finished_at: now_utc(),
    };

    Ok(DiscoveryOutput {
        services,
        repositories,
        run,
    })
}

fn apply_probe(service: &mut ServiceSnapshot, probe: ProbeResult) {
    service.health = probe.health;
    service.updated_at = now_utc();
    service.probe = json!({
        "endpoint": probe.endpoint,
        "summary": probe.summary,
        "metrics": probe.metrics,
        "health": probe.health.as_str(),
    });
}

fn collect_services(
    config: &ConductorConfig,
    playbooks: &[PlaybookDocument],
    role_defaults: &BTreeMap<String, Map<String, Value>>,
    global_vars: &BTreeMap<String, Value>,
    inventory: &InventoryIndex,
    local_repositories: &[LocalRepository],
) -> Vec<ServiceSnapshot> {
    let mut entries: BTreeMap<String, MutableService> = BTreeMap::new();
    let mut imported_by_file: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for playbook in playbooks {
        if let Some(import_playbook) = &playbook.import_playbook {
            if let Some(service_key) = service_key_from_playbook(import_playbook) {
                imported_by_file
                    .entry(playbook.file_name.clone())
                    .or_default()
                    .push(service_key);
            }
            continue;
        }

        let imported = imported_by_file
            .get(&playbook.file_name)
            .cloned()
            .unwrap_or_default();

        if playbook
            .roles
            .iter()
            .any(|role| role == "continuum_tenant_k8s_app")
        {
            if let Some(service_key) =
                pick_string(&playbook.vars, &["continuum_tenant_k8s_app_name"])
            {
                let display_name = friendly_name(&service_key);
                let entry = entries.entry(service_key.clone()).or_insert_with(|| {
                    MutableService::new(
                        service_key.clone(),
                        display_name,
                        kind_for_service(&service_key, "continuum_tenant_k8s_app"),
                        "continuum_tenant_k8s_app",
                    )
                });
                let mut vars = relevant_global_vars(global_vars, &service_key);
                overlay_map(&mut vars, &playbook.vars);
                entry.absorb(&playbook.file_name, &playbook.hosts, &vars, &imported);
            }
        }

        for role in &playbook.roles {
            if role == "continuum_tenant_k8s_app" {
                continue;
            }
            if role == "tracey_host_agent" {
                let entry = entries.entry("tracey".to_string()).or_insert_with(|| {
                    MutableService::new(
                        "tracey",
                        "Tracey",
                        kind_for_service("tracey", "tracey_host_agent"),
                        "tracey_host_agent",
                    )
                });
                let vars = relevant_global_vars(global_vars, "tracey");
                entry.absorb(&playbook.file_name, &playbook.hosts, &vars, &imported);
                continue;
            }

            if let Some(service_key) = service_key_from_role(role) {
                let entry = entries.entry(service_key.clone()).or_insert_with(|| {
                    MutableService::new(
                        service_key.clone(),
                        friendly_name(&service_key),
                        kind_for_service(&service_key, role),
                        role.clone(),
                    )
                });
                let mut vars = relevant_global_vars(global_vars, &service_key);
                if let Some(defaults) = role_defaults.get(role) {
                    overlay_map(&mut vars, defaults);
                }
                overlay_map(&mut vars, &playbook.vars);
                entry.absorb(&playbook.file_name, &playbook.hosts, &vars, &imported);
            }
        }
    }

    if let Some(base_url) = pick_string_in_map(
        global_vars,
        &["nmc_server_url", "continuum_tenant_server_url"],
    ) {
        let entry = entries.entry("continuum".to_string()).or_insert_with(|| {
            MutableService::new(
                "continuum",
                "Continuum",
                kind_for_service("continuum", "nmc_server"),
                "nmc_server",
            )
        });
        let mut vars = relevant_global_vars(global_vars, "continuum");
        vars.insert(
            "continuum_public_base_url".to_string(),
            Value::String(base_url),
        );
        entry.absorb("host_vars", &[], &vars, &[]);
    }

    entries
        .into_values()
        .map(|entry| entry.finalize(config, inventory, local_repositories))
        .collect()
}

fn collect_repository_snapshots(
    services: &[ServiceSnapshot],
    local_repositories: &[LocalRepository],
    github_repositories: &[GitHubRepository],
) -> Vec<RepositorySnapshot> {
    let mut repositories: BTreeMap<String, MutableRepository> = BTreeMap::new();

    for repository in local_repositories {
        let key = repository_key_from_local(repository);
        let entry = repositories
            .entry(key.clone())
            .or_insert_with(|| MutableRepository::new(key.clone(), repository.name.clone()));
        entry.absorb_local(repository);
    }

    for repository in github_repositories {
        let coordinate =
            repo_coordinate_from_full_name(&repository.full_name).unwrap_or(RepoCoordinate {
                owner: None,
                name: repository.name.clone(),
            });
        let key = repository_key(coordinate.owner.as_deref(), &coordinate.name);
        let entry = repositories
            .entry(key.clone())
            .or_insert_with(|| MutableRepository::new(key.clone(), repository.name.clone()));
        entry.absorb_github(repository);
    }

    let mut repository_keys_by_service = BTreeMap::new();
    for service in services {
        let key = service_repository_key(service, local_repositories, github_repositories).or_else(
            || {
                service
                    .repo_path
                    .as_deref()
                    .and_then(|path| Path::new(path).file_name().and_then(|name| name.to_str()))
                    .map(|name| repository_key(None, name))
            },
        );
        let Some(key) = key else {
            continue;
        };
        let name = repository_name_from_key(&key);
        let entry = repositories
            .entry(key.clone())
            .or_insert_with(|| MutableRepository::new(key.clone(), name));
        entry.absorb_service(service);
        repository_keys_by_service.insert(service.service_key.clone(), key);
    }

    let services_by_key = services
        .iter()
        .map(|service| (service.service_key.as_str(), service))
        .collect::<BTreeMap<_, _>>();

    let mut snapshots = Vec::new();
    for mut repository in repositories.into_values() {
        let linked_services = repository
            .linked_services
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for service_key in linked_services {
            let Some(service) = services_by_key.get(service_key.as_str()) else {
                continue;
            };
            for dependency in &service.dependencies {
                if let Some(repo_key) = repository_keys_by_service.get(dependency) {
                    repository.dependencies.insert(repo_key.clone());
                }
            }
        }
        snapshots.push(repository.finalize());
    }

    snapshots.sort_by(|left, right| left.repo_key.cmp(&right.repo_key));
    snapshots
}

fn load_role_defaults(root: &Path) -> Result<BTreeMap<String, Map<String, Value>>> {
    let roles_dir = root.join("roles");
    if !roles_dir.exists() {
        return Ok(BTreeMap::new());
    }
    let mut output = BTreeMap::new();
    for entry in fs::read_dir(roles_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let defaults_path = entry.path().join("defaults").join("main.yml");
        if !defaults_path.exists() {
            continue;
        }
        let value = load_yaml_file_to_json(&defaults_path)?;
        if let Value::Object(map) = value {
            output.insert(entry.file_name().to_string_lossy().to_string(), map);
        }
    }
    Ok(output)
}

fn load_global_vars(root: &Path) -> Result<BTreeMap<String, Value>> {
    let mut files = Vec::new();
    for subdir in [root.join("group_vars"), root.join("host_vars")] {
        if !subdir.exists() {
            continue;
        }
        for entry in WalkDir::new(subdir).min_depth(1).max_depth(2) {
            let entry = entry?;
            if entry.file_type().is_file() && is_yaml_file(entry.path()) {
                files.push(entry.into_path());
            }
        }
    }
    files.sort();

    let mut vars = BTreeMap::new();
    for path in files {
        let value = load_yaml_file_to_json(&path)?;
        if let Value::Object(map) = value {
            for (key, value) in map {
                vars.insert(key, value);
            }
        }
    }
    Ok(vars)
}

fn load_playbooks(root: &Path) -> Result<Vec<PlaybookDocument>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(root).min_depth(1).max_depth(3) {
        let entry = entry?;
        if !entry.file_type().is_file() || !is_yaml_file(entry.path()) {
            continue;
        }
        if should_skip_playbook_path(root, entry.path()) {
            continue;
        }
        files.push(entry.into_path());
    }
    files.sort();

    let mut docs = Vec::new();
    for file in files {
        let raw = fs::read_to_string(&file)?;
        for document in serde_yaml::Deserializer::from_str(&raw) {
            let value = Value::deserialize(document)?;
            append_playbook_documents(root, &file, &value, &mut docs);
        }
    }
    Ok(docs)
}

fn append_playbook_documents(
    root: &Path,
    file: &Path,
    value: &Value,
    docs: &mut Vec<PlaybookDocument>,
) {
    match value {
        Value::Array(items) => {
            for item in items {
                append_playbook_documents(root, file, item, docs);
            }
        }
        Value::Object(_) => {
            if let Some(document) = playbook_document_from_value(root, file, value) {
                docs.push(document);
            }
        }
        _ => {}
    }
}

fn playbook_document_from_value(
    root: &Path,
    file: &Path,
    value: &Value,
) -> Option<PlaybookDocument> {
    let file_name = relative_path(root, file);
    let import_playbook = value
        .get("import_playbook")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let hosts = extract_hosts(value);
    let roles = extract_roles(value);
    let vars = value
        .get("vars")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    if import_playbook.is_none() && hosts.is_empty() && roles.is_empty() {
        return None;
    }

    Some(PlaybookDocument {
        file_name,
        hosts,
        roles,
        vars,
        import_playbook,
    })
}

fn load_inventory_hosts(root: &Path) -> Result<InventoryIndex> {
    let inventory_path = root.join("inventory").join("hosts.ini");
    if !inventory_path.exists() {
        return Ok(InventoryIndex::default());
    }

    enum Section {
        Group(String),
        Children(String),
        Ignore,
    }

    let raw = fs::read_to_string(inventory_path)?;
    let mut section = Section::Ignore;
    let mut inventory = InventoryIndex::default();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            let header = &line[1..line.len() - 1];
            if let Some(group) = header.strip_suffix(":children") {
                section = Section::Children(group.to_string());
            } else if header.ends_with(":vars") {
                section = Section::Ignore;
            } else {
                section = Section::Group(header.to_string());
            }
            continue;
        }

        match &section {
            Section::Group(group) => {
                let host = line
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                if !host.is_empty() {
                    inventory
                        .groups
                        .entry(group.clone())
                        .or_default()
                        .insert(host);
                }
            }
            Section::Children(group) => {
                let child = line
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                if !child.is_empty() {
                    inventory
                        .children
                        .entry(group.clone())
                        .or_default()
                        .insert(child);
                }
            }
            Section::Ignore => {}
        }
    }
    Ok(inventory)
}

fn discover_local_repositories(root: &Path) -> Result<Vec<LocalRepository>> {
    if !root.exists() {
        return Err(anyhow!("local repo root {} does not exist", root.display()));
    }

    let mut repositories = Vec::new();
    let mut entries = fs::read_dir(root)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    entries.sort();

    for path in entries {
        if !looks_like_git_repo(&path) {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let remote_url = git_output(&path, ["config", "--get", "remote.origin.url"]);
        let current_branch = git_output(&path, ["rev-parse", "--abbrev-ref", "HEAD"]);
        let default_branch = git_output(
            &path,
            [
                "symbolic-ref",
                "--quiet",
                "--short",
                "refs/remotes/origin/HEAD",
            ],
        )
        .map(|branch| branch.trim_start_matches("origin/").to_string());
        let coordinate = remote_url
            .as_deref()
            .and_then(repo_coordinate_from_url)
            .or_else(|| {
                Some(RepoCoordinate {
                    owner: None,
                    name: name.to_string(),
                })
            });
        repositories.push(LocalRepository {
            name: name.to_string(),
            local_path: path.clone(),
            remote_url,
            current_branch,
            default_branch,
            coordinate,
            signals: analyze_repository(&path, name),
        });
    }

    repositories.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(repositories)
}

async fn load_github_repositories(
    client: &Client,
    config: &GitHubDiscoveryConfig,
) -> Result<Vec<GitHubRepository>> {
    if !config.enabled || config.owner.trim().is_empty() || config.max_repositories == 0 {
        return Ok(Vec::new());
    }

    let base_url = config.api_base_url.trim_end_matches('/');
    let timeout = Duration::from_secs(config.timeout_seconds.max(1));
    let per_page = config.max_repositories.min(100);

    for scope in ["orgs", "users"] {
        let mut page = 1usize;
        let mut repositories = Vec::new();
        loop {
            let mut request = client
                .get(format!(
                    "{}/{}/{}/repos",
                    base_url,
                    scope,
                    config.owner.trim()
                ))
                .query(&[
                    ("per_page", per_page.to_string()),
                    ("page", page.to_string()),
                    ("type", "all".to_string()),
                ])
                .header("Accept", "application/vnd.github+json")
                .header("User-Agent", "neuralmimicry-conductor/0.1")
                .timeout(timeout);
            if let Some(token) = config
                .token
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                request = request.bearer_auth(token);
            }
            let response = request.send().await?;
            if response.status() == StatusCode::NOT_FOUND {
                repositories.clear();
                break;
            }
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "GitHub {} inventory failed with {}: {}",
                    scope,
                    status,
                    body.trim()
                ));
            }
            let chunk = response.json::<Vec<GitHubRepository>>().await?;
            if chunk.is_empty() {
                break;
            }
            repositories.extend(chunk);
            if repositories.len() >= config.max_repositories || repositories.len() < page * per_page
            {
                break;
            }
            page += 1;
        }
        if !repositories.is_empty() {
            repositories.truncate(config.max_repositories);
            repositories.sort_by(|left, right| left.full_name.cmp(&right.full_name));
            return Ok(repositories);
        }
    }

    Ok(Vec::new())
}

fn analyze_repository(path: &Path, name: &str) -> RepositorySignals {
    let mut signals = RepositorySignals::default();
    let mut frameworks = BTreeSet::new();
    let mut build_systems = BTreeSet::new();
    let mut package_managers = BTreeSet::new();
    let mut capabilities = BTreeSet::new();
    let mut manifests = BTreeSet::new();

    let cargo_toml = read_text_if_exists(&path.join("Cargo.toml"));
    let pyproject_toml = read_text_if_exists(&path.join("pyproject.toml"));
    let requirements_txt = read_text_if_exists(&path.join("requirements.txt"));
    let package_json = read_text_if_exists(&path.join("package.json"));
    let go_mod = read_text_if_exists(&path.join("go.mod"));
    let cmake_lists = read_text_if_exists(&path.join("CMakeLists.txt"))
        .or_else(|| read_text_if_exists(&path.join("nmc_client").join("CMakeLists.txt")));

    if let Some(cargo) = cargo_toml.as_deref() {
        signals.language = Some("Rust".to_string());
        build_systems.insert("cargo".to_string());
        package_managers.insert("cargo".to_string());
        manifests.insert("Cargo.toml".to_string());
        if cargo.contains("axum") {
            frameworks.insert("axum".to_string());
        }
        if cargo.contains("sqlx") {
            frameworks.insert("sqlx".to_string());
        }
        if cargo.contains("tokio") {
            frameworks.insert("tokio".to_string());
        }
    }
    if let Some(pyproject) = pyproject_toml.as_deref() {
        if signals.language.is_none() {
            signals.language = Some("Python".to_string());
        }
        build_systems.insert("pyproject".to_string());
        manifests.insert("pyproject.toml".to_string());
        if pyproject.contains("poetry") {
            package_managers.insert("poetry".to_string());
        } else {
            package_managers.insert("pip".to_string());
        }
        if pyproject.contains("fastapi") {
            frameworks.insert("fastapi".to_string());
        }
        if pyproject.contains("flask") {
            frameworks.insert("flask".to_string());
        }
        if pyproject.contains("django") {
            frameworks.insert("django".to_string());
        }
        if pyproject.contains("pytest") {
            capabilities.insert("tests".to_string());
        }
    }
    if let Some(requirements) = requirements_txt.as_deref() {
        if signals.language.is_none() {
            signals.language = Some("Python".to_string());
        }
        package_managers.insert("pip".to_string());
        manifests.insert("requirements.txt".to_string());
        if requirements.contains("fastapi") {
            frameworks.insert("fastapi".to_string());
        }
        if requirements.contains("flask") {
            frameworks.insert("flask".to_string());
        }
        if requirements.contains("django") {
            frameworks.insert("django".to_string());
        }
        if requirements.contains("pytest") {
            capabilities.insert("tests".to_string());
        }
    }
    if let Some(package) = package_json.as_deref() {
        signals.language = Some(
            if path.join("tsconfig.json").exists() || package.contains("typescript") {
                "TypeScript".to_string()
            } else {
                "JavaScript".to_string()
            },
        );
        build_systems.insert("node".to_string());
        package_managers.insert("npm".to_string());
        manifests.insert("package.json".to_string());
        if package.contains("react") {
            frameworks.insert("react".to_string());
        }
        if package.contains("next") {
            frameworks.insert("nextjs".to_string());
        }
        if package.contains("vite") {
            frameworks.insert("vite".to_string());
        }
        if package.contains("express") {
            frameworks.insert("express".to_string());
        }
    }
    if go_mod.is_some() {
        if signals.language.is_none() {
            signals.language = Some("Go".to_string());
        }
        build_systems.insert("go".to_string());
        package_managers.insert("go_modules".to_string());
        manifests.insert("go.mod".to_string());
    }
    if cmake_lists.is_some() {
        if signals.language.is_none() {
            signals.language = Some("C++".to_string());
        }
        build_systems.insert("cmake".to_string());
        manifests.insert("CMakeLists.txt".to_string());
    }

    signals.has_container = path.join("Containerfile").exists() || path.join("Dockerfile").exists();
    signals.has_tests = path.join("tests").exists() || path.join("test").exists();
    signals.has_kubernetes_manifests = path.join("k8s").exists()
        || path.join("charts").exists()
        || WalkDir::new(path)
            .min_depth(1)
            .max_depth(2)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
            .any(|entry| {
                let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
                name.contains("k8s")
                    || name.contains("kubernetes")
                    || name.contains("deployment")
                    || name.contains("statefulset")
                    || name.contains("daemonset")
            });
    if signals.has_container {
        capabilities.insert("containerised".to_string());
    }
    if signals.has_tests {
        capabilities.insert("tests".to_string());
    }
    if signals.has_kubernetes_manifests {
        capabilities.insert("kubernetes".to_string());
    }
    capabilities.extend(repository_capabilities_by_name(name));

    signals.runtime_type =
        if path.join("src").join("lib.rs").exists() && !path.join("src").join("main.rs").exists() {
            Some("library".to_string())
        } else if path.join("src").join("main.rs").exists()
            || path.join("main.py").exists()
            || path.join("app.py").exists()
            || path.join("src").join("main.cpp").exists()
        {
            Some("application".to_string())
        } else {
            None
        };
    signals.purpose = purpose_for_repository_name(name);
    signals.frameworks = frameworks.into_iter().collect();
    signals.build_systems = build_systems.into_iter().collect();
    signals.package_managers = package_managers.into_iter().collect();
    signals.capabilities = capabilities.into_iter().collect();
    signals.manifests = manifests.into_iter().collect();
    signals
}

fn read_text_if_exists(path: &Path) -> Option<String> {
    fs::read_to_string(path).ok()
}

fn should_skip_playbook_path(root: &Path, path: &Path) -> bool {
    let relative = match path.strip_prefix(root) {
        Ok(relative) => relative,
        Err(_) => return true,
    };
    let Some(first) = relative
        .components()
        .next()
        .and_then(|component| component.as_os_str().to_str())
    else {
        return true;
    };
    matches!(
        first,
        "roles" | "group_vars" | "host_vars" | "inventory" | ".secrets"
    )
}

fn is_yaml_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("yml") | Some("yaml")
    )
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn load_yaml_file_to_json(path: &Path) -> Result<Value> {
    let raw = fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&raw)?)
}

fn extract_hosts(value: &Value) -> Vec<String> {
    match value.get("hosts") {
        Some(Value::String(hosts)) => hosts
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(ToString::to_string)
            .collect(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

fn extract_roles(value: &Value) -> Vec<String> {
    match value.get("roles") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| match item {
                Value::String(role) => Some(role.clone()),
                Value::Object(map) => map
                    .get("role")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn service_key_from_role(role: &str) -> Option<String> {
    if let Some(suffix) = role.strip_prefix("continuum_tenant_") {
        if suffix != "k8s_app" {
            return Some(suffix.to_string());
        }
    }
    match role {
        "rk1_k3s" => Some("k3s-control-plane".to_string()),
        "qc01_k3s_worker" => Some("k3s-worker".to_string()),
        "rk1_shared_storage" | "qc01_shared_storage" => Some("shared-storage".to_string()),
        "rk1_dhcp_tftp" | "pi_dhcp_tftp" => Some("pxe-control".to_string()),
        _ => None,
    }
}

fn service_key_from_playbook(playbook: &str) -> Option<String> {
    static PLAYBOOK_PATTERN: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let regex = PLAYBOOK_PATTERN
        .get_or_init(|| Regex::new(r"continuum_tenant_([a-z0-9_]+)_site\.ya?ml$").expect("regex"));
    regex
        .captures(playbook)
        .and_then(|captures| captures.get(1).map(|matched| matched.as_str().to_string()))
        .filter(|service| service != "observability")
}

fn kind_for_service(service_key: &str, role: &str) -> String {
    match service_key {
        "conductor" | "continuum" => "control_plane".to_string(),
        "tracey" => "host_agent".to_string(),
        "postgres" => "database".to_string(),
        "prometheus" | "grafana" => "observability".to_string(),
        "ollama" => "llm_runtime".to_string(),
        _ if role.starts_with("continuum_tenant_") => "tenant_service".to_string(),
        _ => "infrastructure".to_string(),
    }
}

fn friendly_name(service_key: &str) -> String {
    match service_key {
        "gail" => "Gail".to_string(),
        "tracey" => "Tracey".to_string(),
        "continuum" => "Continuum".to_string(),
        "refiner" => "Refiner".to_string(),
        "aarnn" => "AARNN".to_string(),
        "nmchain" => "NMChain".to_string(),
        "nmstt" => "NMSTT".to_string(),
        "postgres" => "Postgres".to_string(),
        other => other
            .split(['-', '_'])
            .map(|part| {
                let mut chars = part.chars();
                match chars.next() {
                    Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                    None => String::new(),
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
    }
}

fn relevant_global_vars(all: &BTreeMap<String, Value>, service_key: &str) -> Map<String, Value> {
    let mut selected = Map::new();
    let mut prefixes = vec![
        format!("continuum_tenant_{}", service_key),
        format!("continuum_shared_{}", service_key),
    ];
    match service_key {
        "tracey" => prefixes.extend([
            "tracey".to_string(),
            "continuum_tracey".to_string(),
            "nmc_tracey".to_string(),
        ]),
        "continuum" => prefixes.extend([
            "nmc".to_string(),
            "continuum_tenant_server".to_string(),
            "continuum_tenant_nmc".to_string(),
        ]),
        "refiner" | "customers" | "billing" | "nmchain" | "aarnn" => {
            prefixes.extend([
                "continuum_shared_auth".to_string(),
                "continuum_shared_local_auth".to_string(),
            ]);
        }
        "gail" => prefixes.push("continuum_shared_ollama".to_string()),
        _ => {}
    }

    for (key, value) in all {
        if prefixes.iter().any(|prefix| key.starts_with(prefix))
            || matches!(service_key, "continuum") && key == "nmc_server_url"
        {
            selected.insert(key.clone(), value.clone());
        }
    }

    selected
}

fn overlay_map(target: &mut Map<String, Value>, source: &Map<String, Value>) {
    for (key, value) in source {
        target.insert(key.clone(), value.clone());
    }
}

fn pick_string(map: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    let converted: BTreeMap<String, Value> = map
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    pick_string_in_map(&converted, keys)
}

fn pick_string_in_map(map: &BTreeMap<String, Value>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(Value::String(value)) = map.get(*key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() && !trimmed.contains("{{") {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn pick_bool(map: &Map<String, Value>, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(value) = map.get(*key) {
            match value {
                Value::Bool(flag) => return Some(*flag),
                Value::String(raw) if !raw.contains("{{") => {
                    match raw.trim().to_ascii_lowercase().as_str() {
                        "true" | "1" | "yes" => return Some(true),
                        "false" | "0" | "no" => return Some(false),
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn pick_port(map: &Map<String, Value>, keys: &[&str]) -> Option<u16> {
    for key in keys {
        if let Some(value) = map.get(*key) {
            match value {
                Value::Number(number) => {
                    if let Some(port) = number.as_u64().and_then(|value| u16::try_from(value).ok())
                    {
                        return Some(port);
                    }
                }
                Value::String(raw) if !raw.contains("{{") => {
                    if let Ok(port) = raw.trim().parse::<u16>() {
                        return Some(port);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn normalize_url(raw: &str, tls_enabled: bool) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.contains("{{") {
        return None;
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Some(trimmed.trim_end_matches('/').to_string());
    }
    let scheme = if tls_enabled { "https" } else { "http" };
    Some(format!("{}://{}", scheme, trimmed.trim_end_matches('/')))
}

fn classify_probe_error(message: &str) -> ServiceHealth {
    let message = message.to_ascii_lowercase();
    if message.contains("not_found") {
        ServiceHealth::Missing
    } else if message.contains("timeout")
        || message.contains("timed out")
        || message.contains("dns")
        || message.contains("connect")
        || message.contains("connection")
        || message.contains("unreachable")
    {
        ServiceHealth::Unreachable
    } else {
        ServiceHealth::Degraded
    }
}

fn infer_dependencies(vars: &Map<String, Value>) -> Vec<String> {
    static SERVICE_REGEX: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let regex = SERVICE_REGEX.get_or_init(|| {
        Regex::new(r"https?://([a-z0-9-]+)\.([a-z0-9-]+)\.svc\.cluster\.local").expect("regex")
    });
    let mut dependencies = BTreeSet::new();
    for value in flatten_strings(vars) {
        for captures in regex.captures_iter(&value) {
            if let Some(service) = captures.get(1) {
                dependencies.insert(service.as_str().to_string());
            }
        }
        for candidate in [
            "gail",
            "tracey",
            "ollama",
            "customers",
            "postgres",
            "nmchain",
            "billing",
            "refiner",
            "aarnn",
            "grafana",
            "prometheus",
            "conductor",
        ] {
            if value.contains(candidate) {
                dependencies.insert(candidate.to_string());
            }
        }
    }
    dependencies.into_iter().collect()
}

fn infer_storage_paths(vars: &Map<String, Value>, storage_paths: &mut BTreeSet<String>) {
    for (key, value) in vars {
        if !(key.contains("storage") || key.contains("mount_path") || key.contains("data_pvc")) {
            continue;
        }
        match value {
            Value::String(text) if !text.contains("{{") => {
                storage_paths.insert(text.trim().to_string());
            }
            Value::Array(items) => {
                for item in items.iter().filter_map(Value::as_str) {
                    storage_paths.insert(item.to_string());
                }
            }
            _ => {}
        }
    }
}

fn infer_capabilities(
    service_key: &str,
    vars: &Map<String, Value>,
    capabilities: &mut BTreeSet<String>,
    has_public_url: bool,
    has_repo: bool,
) {
    if has_public_url {
        capabilities.insert("ingress".to_string());
    }
    if has_repo {
        capabilities.insert("local_repo".to_string());
    }
    if vars
        .keys()
        .any(|key| key.contains("storage") || key.contains("pvc"))
    {
        capabilities.insert("persistent_storage".to_string());
    }
    if vars.keys().any(|key| key.contains("replicas")) {
        capabilities.insert("replication".to_string());
    }
    if vars
        .keys()
        .any(|key| key.contains("oidc") || key.contains("auth_"))
    {
        capabilities.insert("auth".to_string());
    }
    match service_key {
        "gail" => capabilities.extend(
            [
                "ai_gateway",
                "llm_orchestration",
                "neuromorphic_routing",
                "aer",
            ]
            .into_iter()
            .map(ToString::to_string),
        ),
        "tracey" => capabilities.extend(
            [
                "resource_insights",
                "security_posture",
                "telemetry",
                "adaptive_loop",
            ]
            .into_iter()
            .map(ToString::to_string),
        ),
        "continuum" => capabilities.extend(
            [
                "control_plane",
                "node_recruitment",
                "adaptive_scaling",
                "k8s_orchestration",
            ]
            .into_iter()
            .map(ToString::to_string),
        ),
        "refiner" => capabilities.extend(
            ["code_generation", "project_solver", "workflow_api"]
                .into_iter()
                .map(ToString::to_string),
        ),
        "aarnn" => capabilities.extend(
            [
                "neuromorphic_runtime",
                "distributed_engine",
                "self_improvement_target",
            ]
            .into_iter()
            .map(ToString::to_string),
        ),
        "conductor" => capabilities.extend(
            [
                "improvement_planning",
                "workflow_governance",
                "execution_control",
                "dashboard",
            ]
            .into_iter()
            .map(ToString::to_string),
        ),
        "prometheus" | "grafana" => {
            capabilities.insert("observability".to_string());
        }
        "postgres" => {
            capabilities.insert("relational_storage".to_string());
        }
        "ollama" => {
            capabilities.insert("model_runtime".to_string());
        }
        _ => {}
    }
}

fn flatten_strings(map: &Map<String, Value>) -> Vec<String> {
    let mut output = Vec::new();
    for value in map.values() {
        flatten_value(value, &mut output);
    }
    unique_strings(output)
}

fn flatten_value(value: &Value, output: &mut Vec<String>) {
    match value {
        Value::String(text) => output.push(text.clone()),
        Value::Array(items) => items.iter().for_each(|item| flatten_value(item, output)),
        Value::Object(map) => map.values().for_each(|item| flatten_value(item, output)),
        _ => {}
    }
}

fn resolve_repo_path(
    config: &ConductorConfig,
    service_key: &str,
    vars: &Map<String, Value>,
    local_repositories: &[LocalRepository],
) -> Option<PathBuf> {
    if let Some(path) = repo_hint(config, service_key).filter(|path| path.exists()) {
        return Some(path);
    }

    if let Some(repo_url) = extract_repo_url(vars, service_key) {
        if let Some(repository) = local_repository_by_url(local_repositories, &repo_url) {
            return Some(repository.local_path.clone());
        }
    }

    for alias in service_repo_aliases(service_key) {
        if let Some(repository) = local_repository_by_name(local_repositories, alias.as_str()) {
            return Some(repository.local_path.clone());
        }
    }

    None
}

fn extract_repo_url(vars: &Map<String, Value>, service_key: &str) -> Option<String> {
    extract_repo_field(vars, service_key, &["_repo_url"])
}

fn extract_repo_branch(vars: &Map<String, Value>, service_key: &str) -> Option<String> {
    extract_repo_field(vars, service_key, &["_repo_version", "_repo_branch"])
}

fn extract_repo_field(
    vars: &Map<String, Value>,
    service_key: &str,
    suffixes: &[&str],
) -> Option<String> {
    let aliases = service_repo_aliases(service_key)
        .into_iter()
        .map(|alias| alias.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut candidates = Vec::new();
    for (key, value) in vars {
        let key_lower = key.to_ascii_lowercase();
        if !suffixes.iter().any(|suffix| key_lower.ends_with(suffix)) {
            continue;
        }
        let Some(text) = value.as_str() else {
            continue;
        };
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed.contains("{{") {
            continue;
        }
        let score = aliases
            .iter()
            .filter(|alias| key_lower.contains(alias.as_str()))
            .count();
        candidates.push((score, trimmed.to_string()));
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    match candidates.first() {
        Some((score, value)) if *score > 0 || candidates.len() == 1 => Some(value.clone()),
        _ => None,
    }
}

fn repo_hint(config: &ConductorConfig, service_key: &str) -> Option<PathBuf> {
    match service_key {
        "conductor" => Some(config.discovery.repo_hints.conductor_repo.clone()),
        "gail" => Some(config.discovery.repo_hints.gail_repo.clone()),
        "tracey" => Some(config.discovery.repo_hints.tracey_repo.clone()),
        "continuum" => Some(config.discovery.repo_hints.continuum_repo.clone()),
        "refiner" => Some(config.discovery.repo_hints.refiner_repo.clone()),
        "aarnn" => Some(config.discovery.repo_hints.aarnn_repo.clone()),
        _ => None,
    }
}

fn repo_metadata(repo_path: &Path) -> RepoMetadata {
    RepoMetadata {
        url: git_output(repo_path, ["config", "--get", "remote.origin.url"]),
        branch: git_output(repo_path, ["rev-parse", "--abbrev-ref", "HEAD"]),
        default_branch: git_output(
            repo_path,
            [
                "symbolic-ref",
                "--quiet",
                "--short",
                "refs/remotes/origin/HEAD",
            ],
        )
        .map(|branch| branch.trim_start_matches("origin/").to_string()),
    }
}

fn git_output<const N: usize>(repo_path: &Path, args: [&str; N]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "HEAD" {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn looks_like_git_repo(path: &Path) -> bool {
    path.join(".git").exists()
        || Command::new("git")
            .arg("-C")
            .arg(path)
            .arg("rev-parse")
            .arg("--is-inside-work-tree")
            .output()
            .ok()
            .is_some_and(|output| output.status.success())
}

fn repo_coordinate_from_url(url: &str) -> Option<RepoCoordinate> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return None;
    }
    static URL_PATTERNS: std::sync::OnceLock<Vec<Regex>> = std::sync::OnceLock::new();
    let patterns = URL_PATTERNS.get_or_init(|| {
        vec![
            Regex::new(r"^https?://[^/]+/(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$")
                .expect("regex"),
            Regex::new(r"^git@[^:]+:(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?$").expect("regex"),
        ]
    });
    for pattern in patterns {
        if let Some(captures) = pattern.captures(trimmed) {
            return Some(RepoCoordinate {
                owner: captures
                    .name("owner")
                    .map(|matched| matched.as_str().to_string()),
                name: captures.name("repo")?.as_str().to_string(),
            });
        }
    }
    None
}

fn repo_coordinate_from_full_name(full_name: &str) -> Option<RepoCoordinate> {
    let mut parts = full_name.split('/');
    let owner = parts.next()?.trim();
    let name = parts.next()?.trim();
    if owner.is_empty() || name.is_empty() {
        return None;
    }
    Some(RepoCoordinate {
        owner: Some(owner.to_string()),
        name: name.to_string(),
    })
}

fn owner_from_full_name(full_name: &str) -> Option<String> {
    repo_coordinate_from_full_name(full_name).and_then(|coordinate| coordinate.owner)
}

fn repository_key(owner: Option<&str>, name: &str) -> String {
    let normalized_name = normalize_repo_name(name);
    match owner.map(str::trim).filter(|owner| !owner.is_empty()) {
        Some(owner) => format!("{}/{}", owner.to_ascii_lowercase(), normalized_name),
        None => normalized_name,
    }
}

fn repository_key_from_local(repository: &LocalRepository) -> String {
    match &repository.coordinate {
        Some(coordinate) => repository_key(coordinate.owner.as_deref(), &coordinate.name),
        None => repository_key(None, &repository.name),
    }
}

fn repository_name_from_key(repo_key: &str) -> String {
    repo_key
        .rsplit('/')
        .next()
        .map(ToString::to_string)
        .unwrap_or_else(|| repo_key.to_string())
}

fn normalize_repo_name(name: &str) -> String {
    name.trim().trim_end_matches(".git").to_ascii_lowercase()
}

fn local_repository_by_url<'a>(
    repositories: &'a [LocalRepository],
    repo_url: &str,
) -> Option<&'a LocalRepository> {
    let coordinate = repo_coordinate_from_url(repo_url)?;
    repositories.iter().find(|repository| {
        repository
            .coordinate
            .as_ref()
            .is_some_and(|candidate| candidate == &coordinate)
    })
}

fn local_repository_by_name<'a>(
    repositories: &'a [LocalRepository],
    name: &str,
) -> Option<&'a LocalRepository> {
    repositories.iter().find(|repository| {
        repository.name.eq_ignore_ascii_case(name)
            || repository_key_from_local(repository).ends_with(&normalize_repo_name(name))
    })
}

fn service_repository_key(
    service: &ServiceSnapshot,
    local_repositories: &[LocalRepository],
    github_repositories: &[GitHubRepository],
) -> Option<String> {
    if let Some(path) = service.repo_path.as_deref() {
        if let Some(repository) = local_repositories
            .iter()
            .find(|repository| repository.local_path == Path::new(path))
        {
            return Some(repository_key_from_local(repository));
        }
    }
    if let Some(repo_url) = service.repo_url.as_deref() {
        if let Some(coordinate) = repo_coordinate_from_url(repo_url) {
            return Some(repository_key(
                coordinate.owner.as_deref(),
                &coordinate.name,
            ));
        }
    }
    for alias in service_repo_aliases(&service.service_key) {
        if let Some(repository) = local_repository_by_name(local_repositories, alias.as_str()) {
            return Some(repository_key_from_local(repository));
        }
        if let Some(repository) = github_repositories
            .iter()
            .find(|repository| repository.name.eq_ignore_ascii_case(alias.as_str()))
        {
            let coordinate =
                repo_coordinate_from_full_name(&repository.full_name).unwrap_or(RepoCoordinate {
                    owner: None,
                    name: repository.name.clone(),
                });
            return Some(repository_key(
                coordinate.owner.as_deref(),
                &coordinate.name,
            ));
        }
    }
    None
}

fn service_repo_aliases(service_key: &str) -> Vec<String> {
    let mut aliases = vec![service_key.to_string()];
    match service_key {
        "continuum" => aliases.push("nmc".to_string()),
        "refiner" => aliases.push("rag_demo".to_string()),
        "aarnn" => aliases.push("aarnn_rust".to_string()),
        _ => {}
    }
    unique_strings(aliases)
}

fn deployment_type_for_service(service: &ServiceSnapshot) -> String {
    match service.kind.as_str() {
        "host_agent" => "host_managed".to_string(),
        "control_plane" if service.namespace.is_some() => "kubernetes_control_plane".to_string(),
        _ if service.namespace.is_some() || service.service_name.is_some() => {
            "kubernetes".to_string()
        }
        _ => "infrastructure".to_string(),
    }
}

fn runtime_type_for_service(service: &ServiceSnapshot) -> String {
    match service.service_key.as_str() {
        "postgres" => "database".to_string(),
        "ollama" => "llm_runtime".to_string(),
        _ if service.kind == "host_agent" => "host_agent".to_string(),
        _ if service.public_url.is_some() || service.internal_url.is_some() => {
            "http_service".to_string()
        }
        _ => "service".to_string(),
    }
}

fn purpose_for_service(service: &ServiceSnapshot) -> String {
    match service.service_key.as_str() {
        "conductor" => "governed_improvement_control_plane".to_string(),
        "continuum" => "cluster_control_plane".to_string(),
        "refiner" => "code_generation_executor".to_string(),
        "gail" => "llm_orchestration_gateway".to_string(),
        "tracey" => "telemetry_and_runtime_analysis".to_string(),
        "aarnn" => "neuromorphic_runtime".to_string(),
        "customers" => "identity_and_session_service".to_string(),
        "billing" => "commercial_service".to_string(),
        _ => "runtime_service".to_string(),
    }
}

fn criticality_for_service(service: &ServiceSnapshot) -> String {
    match service.service_key.as_str() {
        "conductor" | "continuum" | "refiner" | "gail" | "tracey" | "postgres" | "customers"
        | "billing" | "aarnn" => "critical".to_string(),
        _ if service.public_url.is_some() || service.capabilities.contains(&"auth".to_string()) => {
            "high".to_string()
        }
        _ => "medium".to_string(),
    }
}

fn runtime_type_for_repository_name(name: &str) -> Option<String> {
    match normalize_repo_name(name).as_str() {
        "jirastats" => Some("analysis_tooling".to_string()),
        "rag_demo" => Some("workflow_service".to_string()),
        "tracey" => Some("host_agent".to_string()),
        "conductor" => Some("control_plane".to_string()),
        "nmc" => Some("control_plane".to_string()),
        _ => None,
    }
}

fn purpose_for_repository_name(name: &str) -> Option<String> {
    match normalize_repo_name(name).as_str() {
        "conductor" => Some("governed_improvement_control_plane".to_string()),
        "rag_demo" => Some("code_generation_executor".to_string()),
        "jirastats" => Some("atlassian_research_and_reporting".to_string()),
        "gail" => Some("llm_orchestration_gateway".to_string()),
        "tracey" => Some("telemetry_and_runtime_analysis".to_string()),
        "nmc" => Some("cluster_control_plane".to_string()),
        "aarnn_rust" | "aarnn-network" | "aarnn-nsys" => Some("neuromorphic_runtime".to_string()),
        _ => None,
    }
}

fn criticality_for_repository_name(name: &str) -> String {
    match normalize_repo_name(name).as_str() {
        "conductor" | "rag_demo" | "gail" | "tracey" | "nmc" | "aarnn_rust" | "customers"
        | "billing" | "nmchain" => "critical".to_string(),
        "jirastats" | "nmstt" | "oshift" => "high".to_string(),
        _ => "medium".to_string(),
    }
}

fn repository_capabilities_by_name(name: &str) -> BTreeSet<String> {
    match normalize_repo_name(name).as_str() {
        "conductor" => ["governance", "inventory", "execution_control"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        "rag_demo" => ["code_generation", "git_workflow", "atlassian_actions"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        "jirastats" => ["atlassian_read", "research", "reporting"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        "gail" => ["llm_orchestration", "neuromorphic_routing"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        "tracey" => ["telemetry", "observability", "runtime_analysis"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        "nmc" => ["cluster_control_plane", "kubernetes_orchestration"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        name if name.starts_with("aarnn") => ["neuromorphic_runtime", "adaptive_inference"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        _ => BTreeSet::new(),
    }
}

fn purpose_for_repository(repository: &MutableRepository) -> String {
    repository
        .purpose
        .clone()
        .or_else(|| purpose_for_repository_name(&repository.name))
        .unwrap_or_else(|| {
            if !repository.linked_services.is_empty() {
                "runtime_service".to_string()
            } else if repository.runtime_type.as_deref() == Some("library") {
                "shared_library".to_string()
            } else {
                "engineering_tooling".to_string()
            }
        })
}

fn deployment_type_for_repository(repository: &MutableRepository) -> String {
    repository.deployment_type.clone().unwrap_or_else(|| {
        if repository.linked_services.is_empty() {
            if repository.has_kubernetes_manifests {
                "kubernetes_candidate".to_string()
            } else if repository.has_container {
                "containerised".to_string()
            } else {
                "local_tooling".to_string()
            }
        } else if repository.has_kubernetes_manifests || repository.has_container {
            "kubernetes".to_string()
        } else {
            "runtime_managed".to_string()
        }
    })
}

fn runtime_type_for_repository(repository: &MutableRepository) -> String {
    repository.runtime_type.clone().unwrap_or_else(|| {
        if !repository.linked_services.is_empty() {
            "service".to_string()
        } else {
            "tooling".to_string()
        }
    })
}

fn promote_criticality(current: &mut String, candidate: &str) {
    if criticality_rank(candidate) > criticality_rank(current.as_str()) {
        *current = candidate.to_string();
    }
}

fn criticality_rank(value: &str) -> usize {
    match value {
        "critical" => 3,
        "high" => 2,
        "medium" => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConductorConfig;
    use tempfile::tempdir;

    #[test]
    fn service_key_is_extracted_from_playbook_name() {
        assert_eq!(
            service_key_from_playbook("continuum_tenant_gail_site.yml"),
            Some("gail".to_string())
        );
        assert_eq!(
            service_key_from_playbook("playbooks/continuum_tenant_gail_site.yaml"),
            Some("gail".to_string())
        );
        assert_eq!(
            service_key_from_playbook("continuum_tenant_observability_site.yml"),
            None
        );
    }

    #[test]
    fn infer_dependencies_extracts_service_names() {
        let vars = serde_json::from_value::<Map<String, Value>>(json!({
            "endpoint": "http://gail.gail.svc.cluster.local:8080",
            "auth": "http://customers.customers.svc.cluster.local:5010/api/session"
        }))
        .expect("map");
        let dependencies = infer_dependencies(&vars);
        assert!(dependencies.contains(&"gail".to_string()));
        assert!(dependencies.contains(&"customers".to_string()));
    }

    #[test]
    fn inventory_resolution_expands_group_targets() {
        let temp = tempdir().expect("tempdir");
        let inventory_dir = temp.path().join("inventory");
        fs::create_dir_all(&inventory_dir).expect("inventory dir");
        fs::write(
            inventory_dir.join("hosts.ini"),
            "[rk1]\nspirit\nqc01\n\n[tracey]\nvega\n\n[edge:children]\nrk1\ntracey\n",
        )
        .expect("inventory file");

        let inventory = load_inventory_hosts(temp.path()).expect("inventory");
        let hosts = inventory.resolve_targets(&["edge".to_string()]);
        assert_eq!(
            hosts,
            vec!["qc01".to_string(), "spirit".to_string(), "vega".to_string()]
        );
    }

    #[test]
    fn extract_repo_url_prefers_service_specific_variables() {
        let vars = serde_json::from_value::<Map<String, Value>>(json!({
            "continuum_tenant_nmc_repo_url": "git@github.com:neuralmimicry/nmc.git",
            "continuum_tenant_refiner_repo_url": "git@github.com:neuralmimicry/rag_demo.git"
        }))
        .expect("vars");

        assert_eq!(
            extract_repo_url(&vars, "continuum"),
            Some("git@github.com:neuralmimicry/nmc.git".to_string())
        );
        assert_eq!(
            extract_repo_url(&vars, "refiner"),
            Some("git@github.com:neuralmimicry/rag_demo.git".to_string())
        );
    }

    #[test]
    fn mutable_service_finalizes_urls_and_capabilities() {
        let mut entry =
            MutableService::new("gail", "Gail", "tenant_service", "continuum_tenant_gail");
        let vars = serde_json::from_value::<Map<String, Value>>(json!({
            "continuum_tenant_gail_namespace": "gail",
            "continuum_tenant_gail_service_name": "gail",
            "continuum_tenant_gail_environment": "prod",
            "continuum_tenant_gail_port": 8080,
            "continuum_tenant_gail_ingress_hostname": "gail.neuralmimicry.ai",
            "continuum_tenant_gail_enable_tls": true,
            "continuum_tenant_gail_data_storage_size": "5Gi"
        }))
        .expect("map");
        entry.absorb(
            "continuum_tenant_gail_site.yml",
            &["rk1".to_string()],
            &vars,
            &[],
        );
        let service = entry.finalize(&ConductorConfig::default(), &InventoryIndex::default(), &[]);
        assert_eq!(service.host_targets, vec!["rk1".to_string()]);
        assert_eq!(service.hosts, vec!["rk1".to_string()]);
        assert_eq!(
            service.public_url.as_deref(),
            Some("https://gail.neuralmimicry.ai")
        );
        assert_eq!(
            service.internal_url.as_deref(),
            Some("http://gail.gail.svc.cluster.local:8080")
        );
        assert_eq!(
            service.deployment_environment,
            Some(crate::models::DeliveryStage::Production)
        );
        assert!(
            service
                .capabilities
                .contains(&"persistent_storage".to_string())
        );
    }

    #[test]
    fn infer_delivery_environment_uses_service_specific_tenant_environment() {
        let vars = serde_json::from_value::<Map<String, Value>>(json!({
            "continuum_tenant_refiner_environment": "integration_testing"
        }))
        .expect("vars");

        assert_eq!(
            infer_delivery_environment(&vars, "refiner"),
            Some(crate::models::DeliveryStage::IntegrationTesting)
        );
    }

    #[test]
    fn analyze_repository_detects_rust_service_signals() {
        let temp = tempdir().expect("tempdir");
        fs::write(
            temp.path().join("Cargo.toml"),
            "[package]\nname='demo'\n[dependencies]\naxum='0.8'\n",
        )
        .expect("cargo");
        fs::create_dir_all(temp.path().join("src")).expect("src");
        fs::write(temp.path().join("src/main.rs"), "fn main() {}\n").expect("main");
        fs::write(temp.path().join("Containerfile"), "FROM scratch\n").expect("container");
        let signals = analyze_repository(temp.path(), "demo");
        assert_eq!(signals.language.as_deref(), Some("Rust"));
        assert!(signals.frameworks.contains(&"axum".to_string()));
        assert!(signals.has_container);
    }
}
