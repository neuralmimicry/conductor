use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow};
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use walkdir::WalkDir;

use crate::{
    config::ConductorConfig,
    integrations::probe_service,
    models::{
        DiscoveryRun, ProbeResult, RunStatus, ServiceHealth, ServiceSnapshot, now_utc,
        topology_from_services, unique_strings,
    },
};

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
    hosts: BTreeSet<String>,
    repo_path: Option<String>,
    capabilities: BTreeSet<String>,
    dependencies: BTreeSet<String>,
    storage_paths: BTreeSet<String>,
    vars: Map<String, Value>,
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
            hosts: BTreeSet::new(),
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
        self.hosts.extend(hosts.iter().cloned());
        overlay_map(&mut self.vars, vars);
        self.dependencies
            .extend(imported_dependencies.iter().cloned());
    }

    fn finalize(mut self, config: &ConductorConfig) -> ServiceSnapshot {
        let now = now_utc();
        self.repo_path = repo_hint(config, &self.service_key)
            .filter(|path| path.exists())
            .map(|path| path.display().to_string());

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

        ServiceSnapshot {
            service_key: self.service_key,
            display_name: self.display_name,
            kind: self.kind,
            role_name: self.role_name,
            playbooks: self.playbooks.into_iter().collect(),
            hosts: self.hosts.into_iter().collect(),
            namespace,
            service_name,
            internal_url,
            public_url,
            repo_path: self.repo_path,
            health: ServiceHealth::Unknown,
            capabilities: self.capabilities.into_iter().collect(),
            dependencies: self.dependencies.into_iter().collect(),
            storage_paths: self.storage_paths.into_iter().collect(),
            raw_defaults: Value::Object(self.vars),
            probe: json!({}),
            discovered_at: now,
            updated_at: now,
        }
    }
}

pub async fn discover_and_probe(
    config: &ConductorConfig,
    client: &Client,
) -> Result<(Vec<ServiceSnapshot>, DiscoveryRun)> {
    let started_at = now_utc();
    let ansible_root = &config.discovery.ansible_root;
    if !ansible_root.exists() {
        return Err(anyhow!(
            "ansible root {} does not exist",
            ansible_root.display()
        ));
    }

    let role_defaults = load_role_defaults(ansible_root)?;
    let global_vars = load_global_vars(ansible_root)?;
    let playbooks = load_playbooks(ansible_root)?;
    let mut services = collect_services(config, &playbooks, &role_defaults, &global_vars);

    let mut issues = Vec::new();
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

    let topology = serde_json::to_value(topology_from_services(&services))?;
    let run = DiscoveryRun {
        id: uuid::Uuid::new_v4(),
        status: if issues.is_empty() {
            RunStatus::Success
        } else {
            RunStatus::PartialFailure
        },
        services_count: services.len(),
        issues,
        topology,
        started_at,
        finished_at: now_utc(),
    };

    Ok((services, run))
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
                        "tenant_service",
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
                    MutableService::new("tracey", "Tracey", "host_agent", "tracey_host_agent")
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
                        kind_for_role(role),
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
            MutableService::new("continuum", "Continuum", "control_plane", "nmc_server")
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
        .map(|entry| entry.finalize(config))
        .collect()
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
            if entry.file_type().is_file()
                && entry.path().extension().and_then(|ext| ext.to_str()) == Some("yml")
            {
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
    let mut files: Vec<PathBuf> = fs::read_dir(root)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("yml"))
        .collect();
    files.sort();

    let mut docs = Vec::new();
    for file in files {
        let raw = fs::read_to_string(&file)?;
        for document in serde_yaml::Deserializer::from_str(&raw) {
            let value = Value::deserialize(document)?;
            append_playbook_documents(&file, &value, &mut docs);
        }
    }
    Ok(docs)
}

fn append_playbook_documents(file: &Path, value: &Value, docs: &mut Vec<PlaybookDocument>) {
    match value {
        Value::Array(items) => {
            for item in items {
                append_playbook_documents(file, item, docs);
            }
        }
        Value::Object(_) => {
            if let Some(document) = playbook_document_from_value(file, value) {
                docs.push(document);
            }
        }
        _ => {}
    }
}

fn playbook_document_from_value(file: &Path, value: &Value) -> Option<PlaybookDocument> {
    let file_name = file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
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
        .get_or_init(|| Regex::new(r"continuum_tenant_([a-z0-9_]+)_site\.yml$").expect("regex"));
    regex
        .captures(playbook)
        .and_then(|captures| captures.get(1).map(|matched| matched.as_str().to_string()))
        .filter(|service| service != "observability")
}

fn kind_for_role(role: &str) -> String {
    if role.starts_with("continuum_tenant_") {
        return "tenant_service".to_string();
    }
    "infrastructure".to_string()
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
        "continuum" => prefixes.extend(["nmc".to_string(), "continuum_tenant_server".to_string()]),
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
        if value.contains("gail") {
            dependencies.insert("gail".to_string());
        }
        if value.contains("tracey") {
            dependencies.insert("tracey".to_string());
        }
        if value.contains("ollama") {
            dependencies.insert("ollama".to_string());
        }
        if value.contains("customers") {
            dependencies.insert("customers".to_string());
        }
        if value.contains("postgres") {
            dependencies.insert("postgres".to_string());
        }
        if value.contains("nmchain") {
            dependencies.insert("nmchain".to_string());
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
        "prometheus" | "grafana" => {
            capabilities.insert("observability".to_string());
        }
        "postgres" => {
            capabilities.insert("relational_storage".to_string());
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

fn repo_hint(config: &ConductorConfig, service_key: &str) -> Option<PathBuf> {
    match service_key {
        "gail" => Some(config.discovery.repo_hints.gail_repo.clone()),
        "tracey" => Some(config.discovery.repo_hints.tracey_repo.clone()),
        "continuum" => Some(config.discovery.repo_hints.continuum_repo.clone()),
        "refiner" => Some(config.discovery.repo_hints.refiner_repo.clone()),
        "aarnn" => Some(config.discovery.repo_hints.aarnn_repo.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConductorConfig;

    #[test]
    fn service_key_is_extracted_from_playbook_name() {
        assert_eq!(
            service_key_from_playbook("continuum_tenant_gail_site.yml"),
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
    fn mutable_service_finalizes_urls_and_capabilities() {
        let mut entry =
            MutableService::new("gail", "Gail", "tenant_service", "continuum_tenant_gail");
        let vars = serde_json::from_value::<Map<String, Value>>(json!({
            "continuum_tenant_gail_namespace": "gail",
            "continuum_tenant_gail_service_name": "gail",
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
        let service = entry.finalize(&ConductorConfig::default());
        assert_eq!(
            service.public_url.as_deref(),
            Some("https://gail.neuralmimicry.ai")
        );
        assert_eq!(
            service.internal_url.as_deref(),
            Some("http://gail.gail.svc.cluster.local:8080")
        );
        assert!(
            service
                .capabilities
                .contains(&"persistent_storage".to_string())
        );
    }
}
