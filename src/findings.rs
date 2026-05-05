use std::collections::BTreeMap;

use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    models::{
        FindingEvidence, FindingProvenance, FindingRecord, FindingSeverity, FindingStatus,
        RepositorySnapshot, ServiceSnapshot, ServiceTrendSummary, now_utc, unique_strings,
    },
    trends::pressure_score,
};

#[derive(Clone, Debug)]
pub struct RecommendationSeed {
    pub dedupe_key: String,
    pub title: String,
    pub summary: String,
    pub target_service: Option<String>,
    pub priority: i32,
    pub tags: Vec<String>,
    pub plan: Value,
    pub depends_on: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct DetectedFinding {
    pub finding: FindingRecord,
    pub evidence: Vec<FindingEvidence>,
    pub provenance: Vec<FindingProvenance>,
    pub recommendation: RecommendationSeed,
}

#[derive(Clone, Debug)]
struct EvidenceSeed {
    evidence_type: String,
    source_kind: String,
    source_ref: String,
    summary: String,
    payload: Value,
}

#[derive(Clone, Debug)]
struct ProvenanceSeed {
    stage: String,
    origin: String,
    component: String,
    detail: String,
    confidence_score: Option<f64>,
    payload: Value,
}

pub fn detect_findings(
    services: &[ServiceSnapshot],
    repositories: &[RepositorySnapshot],
    trends: &[ServiceTrendSummary],
    source_run_id: Option<Uuid>,
    existing_findings: &[FindingRecord],
) -> Vec<DetectedFinding> {
    let existing_by_key = existing_findings
        .iter()
        .map(|finding| (finding.finding_key.clone(), finding))
        .collect::<BTreeMap<_, _>>();
    let trends_by_service = trends
        .iter()
        .map(|trend| (trend.service_key.as_str(), trend))
        .collect::<BTreeMap<_, _>>();

    let mut findings = Vec::new();

    for service in services {
        if matches!(
            service.health,
            crate::models::ServiceHealth::Degraded
                | crate::models::ServiceHealth::Unreachable
                | crate::models::ServiceHealth::Missing
        ) {
            let severity = match service.health {
                crate::models::ServiceHealth::Degraded => FindingSeverity::High,
                crate::models::ServiceHealth::Unreachable
                | crate::models::ServiceHealth::Missing => FindingSeverity::Critical,
                _ => FindingSeverity::High,
            };
            findings.push(build_detected_finding(
                existing_by_key.get(&format!("service_health:{}", service.service_key)),
                source_run_id,
                &format!("service_health:{}", service.service_key),
                &format!("{} health requires stabilisation", service.display_name),
                &format!(
                    "{} is currently {}. Restore its control-plane visibility, probe path, or runtime health before deeper optimisation.",
                    service.display_name,
                    service.health.as_str()
                ),
                "reliability",
                severity,
                Some(service.service_key.clone()),
                None,
                0.98,
                vec!["reliability".to_string(), "health".to_string()],
                json!({
                    "rule": "service_health_not_healthy",
                    "health": service.health.as_str(),
                    "service_kind": service.kind,
                }),
                vec![EvidenceSeed {
                    evidence_type: "service_snapshot".to_string(),
                    source_kind: "runtime_probe".to_string(),
                    source_ref: service.service_key.clone(),
                    summary: format!(
                        "{} is reporting {}",
                        service.display_name,
                        service.health.as_str()
                    ),
                    payload: service_evidence_payload(service),
                }],
                vec![
                    provenance_seed(
                        "analysis",
                        "deterministic_rule",
                        "conductor.findings",
                        "service_health_not_healthy",
                        Some(0.98),
                        json!({"service": service.service_key}),
                    ),
                    provenance_seed(
                        "recommendation",
                        "deterministic_rule",
                        "conductor.planner",
                        "stabilize_service",
                        Some(0.98),
                        json!({"service": service.service_key, "priority": 90}),
                    ),
                ],
                RecommendationSeed {
                    dedupe_key: format!("stabilize:{}", service.service_key),
                    title: format!("Stabilize {}", service.display_name),
                    summary: format!(
                        "{} is currently {}. Restore its control-plane visibility, probe path, or runtime health before deeper optimisation.",
                        service.display_name,
                        service.health.as_str()
                    ),
                    target_service: Some(service.service_key.clone()),
                    priority: 90,
                    tags: vec!["reliability".to_string(), "health".to_string()],
                    plan: json!({
                        "action": "stabilize_service",
                        "service": service.service_key,
                    }),
                    depends_on: Vec::new(),
                },
            ));
        }

        if service.service_key == "gail"
            && !service.capabilities.contains(&"local_repo".to_string())
        {
            findings.push(build_detected_finding(
                existing_by_key.get("repository_visibility:gail"),
                source_run_id,
                "repository_visibility:gail",
                "Gail repository visibility is incomplete",
                "Conductor should improve the live Gail gateway from source, but the local Gail repository path is missing or unreadable.",
                "governance",
                FindingSeverity::Medium,
                Some("gail".to_string()),
                None,
                0.92,
                vec![
                    "governance".to_string(),
                    "source_of_truth".to_string(),
                ],
                json!({
                    "rule": "gail_missing_local_repo",
                    "service": "gail",
                }),
                vec![EvidenceSeed {
                    evidence_type: "service_snapshot".to_string(),
                    source_kind: "inventory".to_string(),
                    source_ref: service.service_key.clone(),
                    summary: "Gail service snapshot has no readable local repository path".to_string(),
                    payload: service_evidence_payload(service),
                }],
                vec![
                    provenance_seed(
                        "analysis",
                        "deterministic_rule",
                        "conductor.findings",
                        "gail_missing_local_repo",
                        Some(0.92),
                        json!({"service": "gail"}),
                    ),
                    provenance_seed(
                        "recommendation",
                        "deterministic_rule",
                        "conductor.planner",
                        "verify_repo_hint",
                        Some(0.92),
                        json!({"service": "gail", "priority": 80}),
                    ),
                ],
                RecommendationSeed {
                    dedupe_key: "gail:repo_visibility".to_string(),
                    title: "Restore Gail repository visibility".to_string(),
                    summary: "Conductor should improve the live Gail gateway from source, but the local Gail repository path is missing or unreadable.".to_string(),
                    target_service: Some("gail".to_string()),
                    priority: 80,
                    tags: vec![
                        "governance".to_string(),
                        "source_of_truth".to_string(),
                    ],
                    plan: json!({"action": "verify_repo_hint", "service": "gail"}),
                    depends_on: Vec::new(),
                },
            ));
        }

        if service.service_key == "aarnn" {
            let replicas = extract_i64(
                &service.raw_defaults,
                "continuum_tenant_aarnn_web_ui_replicas",
            )
            .unwrap_or(1);
            if replicas == 1 {
                findings.push(build_detected_finding(
                    existing_by_key.get("aarnn_singleton_web_ui"),
                    source_run_id,
                    "aarnn_singleton_web_ui",
                    "AARNN web UI remains singleton-bound",
                    "AARNN is configured as a singleton web UI. Externalize session and runtime coordination before scaling the control surface horizontally.",
                    "scalability",
                    FindingSeverity::Medium,
                    Some("aarnn".to_string()),
                    None,
                    0.88,
                    vec!["scalability".to_string(), "aarnn".to_string()],
                    json!({
                        "rule": "aarnn_singleton_web_ui",
                        "replicas": replicas,
                    }),
                    vec![EvidenceSeed {
                        evidence_type: "configuration".to_string(),
                        source_kind: "inventory".to_string(),
                        source_ref: service.service_key.clone(),
                        summary: "AARNN web UI replica count is still 1".to_string(),
                        payload: json!({
                            "replicas": replicas,
                            "service": service.service_key,
                            "raw_defaults": service.raw_defaults,
                        }),
                    }],
                    vec![
                        provenance_seed(
                            "analysis",
                            "deterministic_rule",
                            "conductor.findings",
                            "aarnn_singleton_web_ui",
                            Some(0.88),
                            json!({"service": "aarnn", "replicas": replicas}),
                        ),
                        provenance_seed(
                            "recommendation",
                            "deterministic_rule",
                            "conductor.planner",
                            "externalize_aarnn_sessions",
                            Some(0.88),
                            json!({"service": "aarnn", "priority": 78}),
                        ),
                    ],
                    RecommendationSeed {
                        dedupe_key: "aarnn:singleton_web_ui".to_string(),
                        title: "Externalize AARNN web-session coordination".to_string(),
                        summary: "AARNN is configured as a singleton web UI. Externalize session and runtime coordination before scaling the control surface horizontally.".to_string(),
                        target_service: Some("aarnn".to_string()),
                        priority: 78,
                        tags: vec!["scalability".to_string(), "aarnn".to_string()],
                        plan: json!({"action": "externalize_aarnn_sessions"}),
                        depends_on: Vec::new(),
                    },
                ));
            }
        }

        if service.service_key == "tracey" {
            let pressure = pressure_score(&service.probe);
            if pressure >= 0.75 {
                findings.push(build_detected_finding(
                    existing_by_key.get("tracey_pressure_hotspot"),
                    source_run_id,
                    "tracey_pressure_hotspot",
                    "Tracey is surfacing a pressure hotspot",
                    "Tracey is surfacing elevated pressure or latency. Rebalance workloads, trim noisy loops, or increase capacity before the hotspot becomes a bottleneck.",
                    "performance",
                    FindingSeverity::High,
                    Some("tracey".to_string()),
                    None,
                    0.9,
                    vec![
                        "performance".to_string(),
                        "resource_utilisation".to_string(),
                    ],
                    json!({
                        "rule": "tracey_pressure_hotspot",
                        "pressure_score": pressure,
                    }),
                    vec![EvidenceSeed {
                        evidence_type: "runtime_probe".to_string(),
                        source_kind: "runtime".to_string(),
                        source_ref: service.service_key.clone(),
                        summary: format!("Tracey pressure score is {:.2}", pressure),
                        payload: json!({
                            "pressure_score": pressure,
                            "probe": service.probe,
                        }),
                    }],
                    vec![
                        provenance_seed(
                            "analysis",
                            "deterministic_rule",
                            "conductor.findings",
                            "tracey_pressure_hotspot",
                            Some(0.9),
                            json!({"service": "tracey", "pressure_score": pressure}),
                        ),
                        provenance_seed(
                            "recommendation",
                            "deterministic_rule",
                            "conductor.planner",
                            "investigate_resource_hotspot",
                            Some(0.9),
                            json!({"service": "tracey", "priority": 85}),
                        ),
                    ],
                    RecommendationSeed {
                        dedupe_key: "tracey:resource_hotspot".to_string(),
                        title: "Investigate Tracey-reported resource hotspots".to_string(),
                        summary: "Tracey is surfacing elevated pressure or latency. Rebalance workloads, trim noisy loops, or increase capacity before the hotspot becomes a bottleneck.".to_string(),
                        target_service: Some("tracey".to_string()),
                        priority: 85,
                        tags: vec![
                            "performance".to_string(),
                            "resource_utilisation".to_string(),
                        ],
                        plan: json!({
                            "action": "investigate_resource_hotspot",
                            "pressure_score": pressure,
                        }),
                        depends_on: Vec::new(),
                    },
                ));
            }
        }

        if service.service_key == "refiner" {
            findings.push(build_detected_finding(
                existing_by_key.get("refiner_controlled_executor"),
                source_run_id,
                "refiner_controlled_executor",
                "Refiner should remain the controlled change executor",
                "Route concrete code-change proposals into Refiner's project-solver and job APIs so Conductor can stage self-improvement work with verification and audit trails.",
                "automation",
                FindingSeverity::Medium,
                Some("refiner".to_string()),
                None,
                0.87,
                vec![
                    "automation".to_string(),
                    "code_generation".to_string(),
                ],
                json!({
                    "rule": "refiner_controlled_executor",
                    "service": "refiner",
                }),
                vec![EvidenceSeed {
                    evidence_type: "service_snapshot".to_string(),
                    source_kind: "inventory".to_string(),
                    source_ref: service.service_key.clone(),
                    summary: "Refiner remains the available governed execution surface".to_string(),
                    payload: service_evidence_payload(service),
                }],
                vec![
                    provenance_seed(
                        "analysis",
                        "deterministic_rule",
                        "conductor.findings",
                        "refiner_controlled_executor",
                        Some(0.87),
                        json!({"service": "refiner"}),
                    ),
                    provenance_seed(
                        "recommendation",
                        "deterministic_rule",
                        "conductor.planner",
                        "integrate_refiner_jobs",
                        Some(0.87),
                        json!({"service": "refiner", "priority": 72}),
                    ),
                ],
                RecommendationSeed {
                    dedupe_key: "refiner:solver_loop".to_string(),
                    title: "Use Refiner as the controlled code-improvement executor".to_string(),
                    summary: "Route concrete code-change proposals into Refiner's project-solver/job APIs so Conductor can stage self-improvement work with verification and audit trails.".to_string(),
                    target_service: Some("refiner".to_string()),
                    priority: 72,
                    tags: vec![
                        "automation".to_string(),
                        "code_generation".to_string(),
                    ],
                    plan: json!({
                        "action": "integrate_refiner_jobs",
                        "paths": ["/api/jobs", "/api/execution/plan"],
                    }),
                    depends_on: Vec::new(),
                },
            ));
        }

        if service.service_key == "continuum"
            && !service
                .capabilities
                .contains(&"adaptive_scaling".to_string())
        {
            findings.push(build_detected_finding(
                existing_by_key.get("continuum_adaptive_loop_gap"),
                source_run_id,
                "continuum_adaptive_loop_gap",
                "Continuum adaptive loop wiring is incomplete",
                "Conductor expects Continuum to feed plan/ramp/optimise/repeat signals. Ensure the Tracey fleet and recruitment paths are visible through the Continuum control plane.",
                "control_plane",
                FindingSeverity::Medium,
                Some("continuum".to_string()),
                None,
                0.85,
                vec!["control_plane".to_string(), "autoscaling".to_string()],
                json!({
                    "rule": "continuum_adaptive_loop_gap",
                    "service": "continuum",
                }),
                vec![EvidenceSeed {
                    evidence_type: "service_snapshot".to_string(),
                    source_kind: "inventory".to_string(),
                    source_ref: service.service_key.clone(),
                    summary: "Continuum capabilities do not currently include adaptive scaling".to_string(),
                    payload: service_evidence_payload(service),
                }],
                vec![
                    provenance_seed(
                        "analysis",
                        "deterministic_rule",
                        "conductor.findings",
                        "continuum_adaptive_loop_gap",
                        Some(0.85),
                        json!({"service": "continuum"}),
                    ),
                    provenance_seed(
                        "recommendation",
                        "deterministic_rule",
                        "conductor.planner",
                        "verify_continuum_adaptive_loop",
                        Some(0.85),
                        json!({"service": "continuum", "priority": 70}),
                    ),
                ],
                RecommendationSeed {
                    dedupe_key: "continuum:adaptive_loop".to_string(),
                    title: "Tighten Continuum adaptive loop wiring".to_string(),
                    summary: "Conductor expects Continuum to feed plan/ramp/optimise/repeat signals. Ensure the Tracey fleet and recruitment paths are visible through the Continuum control plane.".to_string(),
                    target_service: Some("continuum".to_string()),
                    priority: 70,
                    tags: vec!["control_plane".to_string(), "autoscaling".to_string()],
                    plan: json!({"action": "verify_continuum_adaptive_loop"}),
                    depends_on: Vec::new(),
                },
            ));
        }

        if service.dependencies.contains(&"postgres".to_string())
            && !service
                .capabilities
                .contains(&"persistent_storage".to_string())
        {
            findings.push(build_detected_finding(
                existing_by_key.get(&format!("storage_profile:{}", service.service_key)),
                source_run_id,
                &format!("storage_profile:{}", service.service_key),
                &format!("{} persistent state strategy is unclear", service.display_name),
                &format!("{} depends on shared data services but no clear persistent-storage profile was inferred from Ansible. Confirm PVCs or durable mounts before further automation.", service.display_name),
                "durability",
                FindingSeverity::Medium,
                Some(service.service_key.clone()),
                None,
                0.82,
                vec!["storage".to_string(), "durability".to_string()],
                json!({
                    "rule": "storage_profile_missing",
                    "dependencies": service.dependencies,
                }),
                vec![EvidenceSeed {
                    evidence_type: "service_snapshot".to_string(),
                    source_kind: "inventory".to_string(),
                    source_ref: service.service_key.clone(),
                    summary: "Service depends on Postgres without inferred persistent-storage capability".to_string(),
                    payload: service_evidence_payload(service),
                }],
                vec![
                    provenance_seed(
                        "analysis",
                        "deterministic_rule",
                        "conductor.findings",
                        "storage_profile_missing",
                        Some(0.82),
                        json!({"service": service.service_key}),
                    ),
                    provenance_seed(
                        "recommendation",
                        "deterministic_rule",
                        "conductor.planner",
                        "verify_persistent_storage",
                        Some(0.82),
                        json!({"service": service.service_key, "priority": 68}),
                    ),
                ],
                RecommendationSeed {
                    dedupe_key: format!("storage:{}", service.service_key),
                    title: format!(
                        "Verify persistent state strategy for {}",
                        service.display_name
                    ),
                    summary: format!("{} depends on shared data services but no clear persistent-storage profile was inferred from Ansible. Confirm PVCs or durable mounts before further automation.", service.display_name),
                    target_service: Some(service.service_key.clone()),
                    priority: 68,
                    tags: vec!["storage".to_string(), "durability".to_string()],
                    plan: json!({
                        "action": "verify_persistent_storage",
                        "service": service.service_key,
                    }),
                    depends_on: Vec::new(),
                },
            ));
        }

        if service.service_key == "prometheus" {
            for (observed_service, down_targets, total_targets, errors) in
                prometheus_target_failures(service)
            {
                let target_service = services
                    .iter()
                    .find(|candidate| candidate.service_key == observed_service)
                    .map(|candidate| candidate.service_key.clone())
                    .or_else(|| Some(observed_service.clone()));
                let observed_display_name = services
                    .iter()
                    .find(|candidate| candidate.service_key == observed_service)
                    .map(|candidate| candidate.display_name.clone())
                    .unwrap_or_else(|| observed_service.clone());
                let severity = if down_targets >= total_targets.max(1) {
                    FindingSeverity::High
                } else {
                    FindingSeverity::Medium
                };
                let priority = if down_targets >= total_targets.max(1) {
                    83
                } else {
                    75
                };

                findings.push(build_detected_finding(
                    existing_by_key
                        .get(&format!("prometheus_target_health:{}", observed_service)),
                    source_run_id,
                    &format!("prometheus_target_health:{}", observed_service),
                    &format!("{} is losing Prometheus scrape coverage", observed_display_name),
                    &format!(
                        "Prometheus reports {}/{} scrape target(s) down for {}. Restore exporter coverage or scrape reachability so Conductor can weigh live runtime evidence when prioritising improvement work.",
                        down_targets,
                        total_targets.max(1),
                        observed_display_name
                    ),
                    "observability",
                    severity,
                    target_service,
                    None,
                    0.9,
                    vec![
                        "observability".to_string(),
                        "telemetry".to_string(),
                        "coverage".to_string(),
                    ],
                    json!({
                        "rule": "prometheus_target_down",
                        "observed_service": observed_service,
                        "down_targets": down_targets,
                        "total_targets": total_targets,
                        "errors": errors,
                    }),
                    vec![EvidenceSeed {
                        evidence_type: "runtime_probe".to_string(),
                        source_kind: "observability".to_string(),
                        source_ref: "prometheus".to_string(),
                        summary: format!(
                            "Prometheus reports {}/{} target(s) down for {}",
                            down_targets,
                            total_targets.max(1),
                            observed_display_name
                        ),
                        payload: service_evidence_payload(service),
                    }],
                    vec![
                        provenance_seed(
                            "analysis",
                            "deterministic_rule",
                            "conductor.findings",
                            "prometheus_target_down",
                            Some(0.9),
                            json!({"service": observed_service, "down_targets": down_targets}),
                        ),
                        provenance_seed(
                            "recommendation",
                            "deterministic_rule",
                            "conductor.planner",
                            "restore_observability_coverage",
                            Some(0.9),
                            json!({"service": observed_service, "priority": priority}),
                        ),
                    ],
                    RecommendationSeed {
                        dedupe_key: format!("prometheus:coverage:{}", observed_service),
                        title: format!(
                            "Restore observability coverage for {}",
                            observed_display_name
                        ),
                        summary: format!(
                            "Prometheus reports {}/{} scrape target(s) down for {}. Restore exporter coverage or scrape reachability so Conductor can weigh live runtime evidence when prioritising improvement work.",
                            down_targets,
                            total_targets.max(1),
                            observed_display_name
                        ),
                        target_service: Some(observed_service.clone()),
                        priority,
                        tags: vec![
                            "observability".to_string(),
                            "telemetry".to_string(),
                            "coverage".to_string(),
                        ],
                        plan: json!({
                            "action": "restore_observability_coverage",
                            "service": observed_service,
                            "down_targets": down_targets,
                            "total_targets": total_targets,
                        }),
                        depends_on: Vec::new(),
                    },
                ));
            }
        }

        if service.service_key == "grafana" {
            let metrics = probe_metrics(service);
            let database_status = metrics
                .get("database_status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let datasource_count = extract_i64(&metrics, "datasource_count").unwrap_or(0);
            let dashboard_count = extract_i64(&metrics, "dashboard_count").unwrap_or(0);
            if !database_status.eq_ignore_ascii_case("ok")
                || datasource_count == 0
                || dashboard_count == 0
            {
                let severity = if !database_status.eq_ignore_ascii_case("ok") {
                    FindingSeverity::High
                } else {
                    FindingSeverity::Medium
                };
                let priority = if matches!(severity, FindingSeverity::High) {
                    79
                } else {
                    71
                };
                findings.push(build_detected_finding(
                    existing_by_key.get("grafana_visibility_gap"),
                    source_run_id,
                    "grafana_visibility_gap",
                    "Grafana visibility surface needs attention",
                    &format!(
                        "Grafana health is reporting database='{}' with {} datasource(s) and {} dashboard(s). Restore dashboard coverage and backing state so estate operators retain a usable observability cockpit.",
                        database_status,
                        datasource_count,
                        dashboard_count
                    ),
                    "observability",
                    severity,
                    Some("grafana".to_string()),
                    None,
                    0.88,
                    vec!["observability".to_string(), "dashboard".to_string()],
                    json!({
                        "rule": "grafana_visibility_gap",
                        "database_status": database_status,
                        "datasource_count": datasource_count,
                        "dashboard_count": dashboard_count,
                    }),
                    vec![EvidenceSeed {
                        evidence_type: "runtime_probe".to_string(),
                        source_kind: "observability".to_string(),
                        source_ref: "grafana".to_string(),
                        summary: "Grafana health or coverage is incomplete".to_string(),
                        payload: service_evidence_payload(service),
                    }],
                    vec![
                        provenance_seed(
                            "analysis",
                            "deterministic_rule",
                            "conductor.findings",
                            "grafana_visibility_gap",
                            Some(0.88),
                            json!({"service": "grafana"}),
                        ),
                        provenance_seed(
                            "recommendation",
                            "deterministic_rule",
                            "conductor.planner",
                            "restore_grafana_visibility",
                            Some(0.88),
                            json!({"service": "grafana", "priority": priority}),
                        ),
                    ],
                    RecommendationSeed {
                        dedupe_key: "grafana:visibility".to_string(),
                        title: "Restore Grafana visibility coverage".to_string(),
                        summary: format!(
                            "Grafana health is reporting database='{}' with {} datasource(s) and {} dashboard(s). Restore dashboard coverage and backing state so estate operators retain a usable observability cockpit.",
                            database_status,
                            datasource_count,
                            dashboard_count
                        ),
                        target_service: Some("grafana".to_string()),
                        priority,
                        tags: vec!["observability".to_string(), "dashboard".to_string()],
                        plan: json!({
                            "action": "restore_grafana_visibility",
                            "database_status": database_status,
                            "datasource_count": datasource_count,
                            "dashboard_count": dashboard_count,
                        }),
                        depends_on: Vec::new(),
                    },
                ));
            }
        }

        if service.service_key == "postgres" {
            let metrics = probe_metrics(service);
            let database = metrics
                .get("database")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let connection_utilization =
                extract_f64(&database, "connection_utilization").unwrap_or(0.0);
            let waiting_connections = extract_i64(&database, "waiting_connections").unwrap_or(0);
            let idle_in_transaction = extract_i64(&database, "idle_in_transaction").unwrap_or(0);
            let current_database = database
                .get("current_database")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            if connection_utilization >= 0.75 || waiting_connections > 0 || idle_in_transaction > 0
            {
                let severity = if connection_utilization >= 0.9 || waiting_connections > 0 {
                    FindingSeverity::High
                } else {
                    FindingSeverity::Medium
                };
                let priority = if matches!(severity, FindingSeverity::High) {
                    84
                } else {
                    76
                };
                findings.push(build_detected_finding(
                    existing_by_key.get("postgres_pressure_hotspot"),
                    source_run_id,
                    "postgres_pressure_hotspot",
                    "Postgres is surfacing shared-state pressure",
                    &format!(
                        "Postgres reports {:.0}% connection utilisation on database '{}' with {} waiting session(s) and {} idle-in-transaction session(s). Reduce query pressure or connection sprawl before shared state becomes the bottleneck.",
                        connection_utilization * 100.0,
                        current_database,
                        waiting_connections,
                        idle_in_transaction
                    ),
                    "durability",
                    severity,
                    Some("postgres".to_string()),
                    None,
                    0.91,
                    vec![
                        "database".to_string(),
                        "durability".to_string(),
                        "performance".to_string(),
                    ],
                    json!({
                        "rule": "postgres_pressure_hotspot",
                        "connection_utilization": connection_utilization,
                        "waiting_connections": waiting_connections,
                        "idle_in_transaction": idle_in_transaction,
                        "current_database": current_database,
                    }),
                    vec![EvidenceSeed {
                        evidence_type: "runtime_probe".to_string(),
                        source_kind: "database".to_string(),
                        source_ref: "postgres".to_string(),
                        summary: format!(
                            "Postgres connection utilisation is {:.0}%",
                            connection_utilization * 100.0
                        ),
                        payload: service_evidence_payload(service),
                    }],
                    vec![
                        provenance_seed(
                            "analysis",
                            "deterministic_rule",
                            "conductor.findings",
                            "postgres_pressure_hotspot",
                            Some(0.91),
                            json!({"service": "postgres"}),
                        ),
                        provenance_seed(
                            "recommendation",
                            "deterministic_rule",
                            "conductor.planner",
                            "reduce_postgres_pressure",
                            Some(0.91),
                            json!({"service": "postgres", "priority": priority}),
                        ),
                    ],
                    RecommendationSeed {
                        dedupe_key: "postgres:pressure".to_string(),
                        title: "Reduce shared Postgres pressure".to_string(),
                        summary: format!(
                            "Postgres reports {:.0}% connection utilisation on database '{}' with {} waiting session(s) and {} idle-in-transaction session(s). Reduce query pressure or connection sprawl before shared state becomes the bottleneck.",
                            connection_utilization * 100.0,
                            current_database,
                            waiting_connections,
                            idle_in_transaction
                        ),
                        target_service: Some("postgres".to_string()),
                        priority,
                        tags: vec![
                            "database".to_string(),
                            "durability".to_string(),
                            "performance".to_string(),
                        ],
                        plan: json!({
                            "action": "reduce_postgres_pressure",
                            "connection_utilization": connection_utilization,
                            "waiting_connections": waiting_connections,
                            "idle_in_transaction": idle_in_transaction,
                        }),
                        depends_on: Vec::new(),
                    },
                ));
            }
        }

        if service.service_key == "shared-storage" {
            let metrics = probe_metrics(service);
            let filesystem = metrics
                .get("filesystem")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let usage_ratio = extract_f64(&filesystem, "usage_ratio").unwrap_or(0.0);
            let inode_usage_ratio = extract_f64(&filesystem, "inode_usage_ratio").unwrap_or(0.0);
            let read_only = filesystem
                .get("read_only")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let missing_subdirectories = metrics
                .get("missing_subdirectories")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            if read_only
                || usage_ratio >= 0.8
                || inode_usage_ratio >= 0.8
                || !missing_subdirectories.is_empty()
            {
                let severity = if read_only || usage_ratio >= 0.95 || inode_usage_ratio >= 0.95 {
                    FindingSeverity::High
                } else {
                    FindingSeverity::Medium
                };
                let priority = if matches!(severity, FindingSeverity::High) {
                    82
                } else {
                    74
                };
                findings.push(build_detected_finding(
                    existing_by_key.get("shared_storage_pressure"),
                    source_run_id,
                    "shared_storage_pressure",
                    "Shared persistent storage needs attention",
                    &format!(
                        "Shared storage is at {:.0}% byte usage and {:.0}% inode usage{}{}. Protect capacity and mount health before persistent services lose room to operate.",
                        usage_ratio * 100.0,
                        inode_usage_ratio * 100.0,
                        if read_only { ", and the mount is read-only" } else { "" },
                        if missing_subdirectories.is_empty() {
                            String::new()
                        } else {
                            format!(
                                ", missing expected paths: {}",
                                missing_subdirectories.join(", ")
                            )
                        }
                    ),
                    "durability",
                    severity,
                    Some("shared-storage".to_string()),
                    None,
                    0.9,
                    vec!["storage".to_string(), "durability".to_string()],
                    json!({
                        "rule": "shared_storage_pressure",
                        "usage_ratio": usage_ratio,
                        "inode_usage_ratio": inode_usage_ratio,
                        "read_only": read_only,
                        "missing_subdirectories": missing_subdirectories,
                    }),
                    vec![EvidenceSeed {
                        evidence_type: "runtime_probe".to_string(),
                        source_kind: "storage".to_string(),
                        source_ref: "shared-storage".to_string(),
                        summary: format!(
                            "Shared storage usage is {:.0}% with {:.0}% inode usage",
                            usage_ratio * 100.0,
                            inode_usage_ratio * 100.0
                        ),
                        payload: service_evidence_payload(service),
                    }],
                    vec![
                        provenance_seed(
                            "analysis",
                            "deterministic_rule",
                            "conductor.findings",
                            "shared_storage_pressure",
                            Some(0.9),
                            json!({"service": "shared-storage"}),
                        ),
                        provenance_seed(
                            "recommendation",
                            "deterministic_rule",
                            "conductor.planner",
                            "protect_shared_storage_capacity",
                            Some(0.9),
                            json!({"service": "shared-storage", "priority": priority}),
                        ),
                    ],
                    RecommendationSeed {
                        dedupe_key: "shared-storage:pressure".to_string(),
                        title: "Protect shared storage capacity".to_string(),
                        summary: format!(
                            "Shared storage is at {:.0}% byte usage and {:.0}% inode usage{}{}. Protect capacity and mount health before persistent services lose room to operate.",
                            usage_ratio * 100.0,
                            inode_usage_ratio * 100.0,
                            if read_only { ", and the mount is read-only" } else { "" },
                            if missing_subdirectories.is_empty() {
                                String::new()
                            } else {
                                format!(
                                    ", missing expected paths: {}",
                                    missing_subdirectories.join(", ")
                                )
                            }
                        ),
                        target_service: Some("shared-storage".to_string()),
                        priority,
                        tags: vec!["storage".to_string(), "durability".to_string()],
                        plan: json!({
                            "action": "protect_shared_storage_capacity",
                            "usage_ratio": usage_ratio,
                            "inode_usage_ratio": inode_usage_ratio,
                            "read_only": read_only,
                            "missing_subdirectories": missing_subdirectories,
                        }),
                        depends_on: Vec::new(),
                    },
                ));
            }
        }

        if let Some(trend) = trends_by_service.get(service.service_key.as_str()) {
            findings.extend(trend_findings(
                &existing_by_key,
                source_run_id,
                service,
                trend,
            ));
        }
    }

    for repository in repositories {
        if repository.archived && !repository.linked_services.is_empty() {
            findings.push(build_detected_finding(
                existing_by_key.get(&format!("archived_live_repo:{}", repository.repo_key)),
                source_run_id,
                &format!("archived_live_repo:{}", repository.repo_key),
                &format!("{} is archived but still linked to live services", repository.name),
                &format!(
                    "{} is marked archived while still linked to live services. Confirm that the runtime has moved to a supported source or unarchive the repository before further automation.",
                    repository.name
                ),
                "governance",
                FindingSeverity::High,
                repository.linked_services.first().cloned(),
                Some(repository.repo_key.clone()),
                0.9,
                vec![
                    "repository".to_string(),
                    "governance".to_string(),
                    "lifecycle".to_string(),
                ],
                json!({
                    "rule": "archived_live_repository",
                    "linked_services": repository.linked_services,
                }),
                vec![EvidenceSeed {
                    evidence_type: "repository_snapshot".to_string(),
                    source_kind: "inventory".to_string(),
                    source_ref: repository.repo_key.clone(),
                    summary: "Archived repository remains linked to active services".to_string(),
                    payload: repository_evidence_payload(repository),
                }],
                vec![
                    provenance_seed(
                        "analysis",
                        "deterministic_rule",
                        "conductor.findings",
                        "archived_live_repository",
                        Some(0.9),
                        json!({"repository": repository.repo_key}),
                    ),
                    provenance_seed(
                        "recommendation",
                        "deterministic_rule",
                        "conductor.planner",
                        "verify_repository_support_status",
                        Some(0.9),
                        json!({"repository": repository.repo_key, "priority": 82}),
                    ),
                ],
                RecommendationSeed {
                    dedupe_key: format!("repo:archived_live:{}", repository.repo_key),
                    title: format!(
                        "Resolve archived-source risk for {}",
                        repository.name
                    ),
                    summary: format!(
                        "{} is marked archived while still linked to live services. Confirm that the runtime has moved to a supported source or unarchive the repository before further automation.",
                        repository.name
                    ),
                    target_service: repository.linked_services.first().cloned(),
                    priority: 82,
                    tags: vec![
                        "repository".to_string(),
                        "governance".to_string(),
                        "lifecycle".to_string(),
                    ],
                    plan: json!({
                        "action": "verify_repository_support_status",
                        "repository": repository.repo_key,
                        "linked_services": repository.linked_services,
                    }),
                    depends_on: Vec::new(),
                },
            ));
        }

        if !repository.capabilities.contains(&"tests".to_string())
            && !repository.linked_services.is_empty()
        {
            let priority = if repository.criticality == "high" {
                72
            } else {
                66
            };
            let severity = if repository.criticality == "high" {
                FindingSeverity::High
            } else {
                FindingSeverity::Medium
            };
            findings.push(build_detected_finding(
                existing_by_key.get(&format!("repository_test_baseline:{}", repository.repo_key)),
                source_run_id,
                &format!("repository_test_baseline:{}", repository.repo_key),
                &format!("{} lacks an obvious test baseline", repository.name),
                &format!(
                    "{} is linked to live services but no obvious test capability was discovered in the repository inventory. Establish at least a minimal regression or smoke-test baseline before deeper autonomous changes.",
                    repository.name
                ),
                "testability",
                severity,
                repository.linked_services.first().cloned(),
                Some(repository.repo_key.clone()),
                0.84,
                vec![
                    "repository".to_string(),
                    "tests".to_string(),
                    "quality".to_string(),
                ],
                json!({
                    "rule": "repository_missing_tests_capability",
                    "criticality": repository.criticality,
                    "linked_services": repository.linked_services,
                }),
                vec![EvidenceSeed {
                    evidence_type: "repository_snapshot".to_string(),
                    source_kind: "inventory".to_string(),
                    source_ref: repository.repo_key.clone(),
                    summary: "Repository capabilities do not include tests while linked services are present".to_string(),
                    payload: repository_evidence_payload(repository),
                }],
                vec![
                    provenance_seed(
                        "analysis",
                        "deterministic_rule",
                        "conductor.findings",
                        "repository_missing_tests_capability",
                        Some(0.84),
                        json!({"repository": repository.repo_key}),
                    ),
                    provenance_seed(
                        "recommendation",
                        "deterministic_rule",
                        "conductor.planner",
                        "establish_repository_test_baseline",
                        Some(0.84),
                        json!({"repository": repository.repo_key, "priority": priority}),
                    ),
                ],
                RecommendationSeed {
                    dedupe_key: format!("repo:test_baseline:{}", repository.repo_key),
                    title: format!(
                        "Establish a test baseline for {}",
                        repository.name
                    ),
                    summary: format!(
                        "{} is linked to live services but no obvious test capability was discovered in the repository inventory. Establish at least a minimal regression or smoke-test baseline before deeper autonomous changes.",
                        repository.name
                    ),
                    target_service: repository.linked_services.first().cloned(),
                    priority,
                    tags: vec![
                        "repository".to_string(),
                        "tests".to_string(),
                        "quality".to_string(),
                    ],
                    plan: json!({
                        "action": "establish_repository_test_baseline",
                        "repository": repository.repo_key,
                        "linked_services": repository.linked_services,
                    }),
                    depends_on: Vec::new(),
                },
            ));
        }
    }

    findings
}

fn trend_findings(
    existing_by_key: &BTreeMap<String, &FindingRecord>,
    source_run_id: Option<Uuid>,
    service: &ServiceSnapshot,
    trend: &ServiceTrendSummary,
) -> Vec<DetectedFinding> {
    if trend.direction != "worsening" {
        return Vec::new();
    }

    let sustained = trend.sample_count >= 3;
    let metrics_summary = if trend.metrics.is_empty() {
        trend.headline.clone()
    } else {
        trend
            .metrics
            .iter()
            .take(3)
            .map(|metric| {
                format!(
                    "{}={:.2} ({})",
                    metric.metric_name, metric.latest, metric.direction
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    match service.service_key.as_str() {
        "tracey" => vec![build_detected_finding(
            existing_by_key.get("tracey_worsening_trend"),
            source_run_id,
            "tracey_worsening_trend",
            "Tracey telemetry trend is worsening",
            &format!(
                "Tracey shows a {} worsening telemetry trend across {} samples. Focus on the highest-pressure signals before the adaptive loop starts making poorer placement decisions. Current headline: {}",
                if sustained { "sustained" } else { "emerging" },
                trend.sample_count,
                metrics_summary
            ),
            "performance",
            if sustained {
                FindingSeverity::High
            } else {
                FindingSeverity::Medium
            },
            Some("tracey".to_string()),
            None,
            if sustained { 0.9 } else { 0.78 },
            vec![
                "performance".to_string(),
                "telemetry".to_string(),
                "trend".to_string(),
            ],
            json!({
                "rule": "tracey_worsening_trend",
                "sample_count": trend.sample_count,
                "headline": trend.headline,
            }),
            vec![EvidenceSeed {
                evidence_type: "metric_trend".to_string(),
                source_kind: "runtime".to_string(),
                source_ref: trend.service_key.clone(),
                summary: trend.headline.clone(),
                payload: json!({
                    "sample_count": trend.sample_count,
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "raw_latest": trend.raw_latest,
                }),
            }],
            vec![
                provenance_seed(
                    "analysis",
                    "deterministic_rule",
                    "conductor.findings",
                    "tracey_worsening_trend",
                    Some(if sustained { 0.9 } else { 0.78 }),
                    json!({"service": "tracey"}),
                ),
                provenance_seed(
                    "recommendation",
                    "deterministic_rule",
                    "conductor.planner",
                    "stabilize_tracey_trend",
                    Some(if sustained { 0.9 } else { 0.78 }),
                    json!({"service": "tracey", "priority": if sustained { 88 } else { 81 }}),
                ),
            ],
            RecommendationSeed {
                dedupe_key: "tracey:worsening_trend".to_string(),
                title: "Stabilize Tracey pressure trend".to_string(),
                summary: format!(
                    "Tracey shows a {} worsening telemetry trend across {} samples. Focus on the highest-pressure signals before the adaptive loop starts making poorer placement decisions. Current headline: {}",
                    if sustained { "sustained" } else { "emerging" },
                    trend.sample_count,
                    metrics_summary
                ),
                target_service: Some("tracey".to_string()),
                priority: if sustained { 88 } else { 81 },
                tags: vec![
                    "performance".to_string(),
                    "telemetry".to_string(),
                    "trend".to_string(),
                ],
                plan: json!({
                    "action": "stabilize_tracey_trend",
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "sample_count": trend.sample_count,
                }),
                depends_on: Vec::new(),
            },
        )],
        "continuum" => vec![build_detected_finding(
            existing_by_key.get("continuum_worsening_trend"),
            source_run_id,
            "continuum_worsening_trend",
            "Continuum control-plane trend is worsening",
            &format!(
                "Continuum shows a {} worsening control-plane trend across {} samples. Tighten recruitment, placement, or backlog handling before orchestration latency compounds. Current headline: {}",
                if sustained { "sustained" } else { "emerging" },
                trend.sample_count,
                metrics_summary
            ),
            "control_plane",
            if sustained {
                FindingSeverity::High
            } else {
                FindingSeverity::Medium
            },
            Some("continuum".to_string()),
            None,
            if sustained { 0.88 } else { 0.76 },
            vec![
                "control_plane".to_string(),
                "adaptive_loop".to_string(),
                "trend".to_string(),
            ],
            json!({
                "rule": "continuum_worsening_trend",
                "sample_count": trend.sample_count,
                "headline": trend.headline,
            }),
            vec![EvidenceSeed {
                evidence_type: "metric_trend".to_string(),
                source_kind: "runtime".to_string(),
                source_ref: trend.service_key.clone(),
                summary: trend.headline.clone(),
                payload: json!({
                    "sample_count": trend.sample_count,
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "raw_latest": trend.raw_latest,
                }),
            }],
            vec![
                provenance_seed(
                    "analysis",
                    "deterministic_rule",
                    "conductor.findings",
                    "continuum_worsening_trend",
                    Some(if sustained { 0.88 } else { 0.76 }),
                    json!({"service": "continuum"}),
                ),
                provenance_seed(
                    "recommendation",
                    "deterministic_rule",
                    "conductor.planner",
                    "correct_continuum_trend",
                    Some(if sustained { 0.88 } else { 0.76 }),
                    json!({"service": "continuum", "priority": if sustained { 84 } else { 77 }}),
                ),
            ],
            RecommendationSeed {
                dedupe_key: "continuum:worsening_trend".to_string(),
                title: "Correct Continuum adaptive drift".to_string(),
                summary: format!(
                    "Continuum shows a {} worsening control-plane trend across {} samples. Tighten recruitment, placement, or backlog handling before orchestration latency compounds. Current headline: {}",
                    if sustained { "sustained" } else { "emerging" },
                    trend.sample_count,
                    metrics_summary
                ),
                target_service: Some("continuum".to_string()),
                priority: if sustained { 84 } else { 77 },
                tags: vec![
                    "control_plane".to_string(),
                    "adaptive_loop".to_string(),
                    "trend".to_string(),
                ],
                plan: json!({
                    "action": "correct_continuum_trend",
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "sample_count": trend.sample_count,
                }),
                depends_on: Vec::new(),
            },
        )],
        "prometheus" => vec![build_detected_finding(
            existing_by_key.get("prometheus_worsening_trend"),
            source_run_id,
            "prometheus_worsening_trend",
            "Prometheus scrape coverage trend is worsening",
            &format!(
                "Prometheus shows a {} worsening observability trend across {} samples. Recover scrape coverage before the planner starts reasoning over stale runtime evidence. Current headline: {}",
                if sustained { "sustained" } else { "emerging" },
                trend.sample_count,
                metrics_summary
            ),
            "observability",
            if sustained {
                FindingSeverity::High
            } else {
                FindingSeverity::Medium
            },
            Some("prometheus".to_string()),
            None,
            if sustained { 0.86 } else { 0.74 },
            vec![
                "observability".to_string(),
                "telemetry".to_string(),
                "trend".to_string(),
            ],
            json!({
                "rule": "prometheus_worsening_trend",
                "sample_count": trend.sample_count,
                "headline": trend.headline,
            }),
            vec![EvidenceSeed {
                evidence_type: "metric_trend".to_string(),
                source_kind: "observability".to_string(),
                source_ref: trend.service_key.clone(),
                summary: trend.headline.clone(),
                payload: json!({
                    "sample_count": trend.sample_count,
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "raw_latest": trend.raw_latest,
                }),
            }],
            vec![
                provenance_seed(
                    "analysis",
                    "deterministic_rule",
                    "conductor.findings",
                    "prometheus_worsening_trend",
                    Some(if sustained { 0.86 } else { 0.74 }),
                    json!({"service": "prometheus"}),
                ),
                provenance_seed(
                    "recommendation",
                    "deterministic_rule",
                    "conductor.planner",
                    "stabilize_prometheus_coverage",
                    Some(if sustained { 0.86 } else { 0.74 }),
                    json!({"service": "prometheus", "priority": if sustained { 79 } else { 72 }}),
                ),
            ],
            RecommendationSeed {
                dedupe_key: "prometheus:worsening_trend".to_string(),
                title: "Stabilize Prometheus scrape coverage".to_string(),
                summary: format!(
                    "Prometheus shows a {} worsening observability trend across {} samples. Recover scrape coverage before the planner starts reasoning over stale runtime evidence. Current headline: {}",
                    if sustained { "sustained" } else { "emerging" },
                    trend.sample_count,
                    metrics_summary
                ),
                target_service: Some("prometheus".to_string()),
                priority: if sustained { 79 } else { 72 },
                tags: vec![
                    "observability".to_string(),
                    "telemetry".to_string(),
                    "trend".to_string(),
                ],
                plan: json!({
                    "action": "stabilize_prometheus_coverage",
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "sample_count": trend.sample_count,
                }),
                depends_on: Vec::new(),
            },
        )],
        "postgres" => vec![build_detected_finding(
            existing_by_key.get("postgres_worsening_trend"),
            source_run_id,
            "postgres_worsening_trend",
            "Postgres pressure trend is worsening",
            &format!(
                "Postgres shows a {} worsening shared-state trend across {} samples. Reduce query, connection, or transaction pressure before persistence becomes the bottleneck. Current headline: {}",
                if sustained { "sustained" } else { "emerging" },
                trend.sample_count,
                metrics_summary
            ),
            "durability",
            if sustained {
                FindingSeverity::High
            } else {
                FindingSeverity::Medium
            },
            Some("postgres".to_string()),
            None,
            if sustained { 0.88 } else { 0.76 },
            vec![
                "database".to_string(),
                "durability".to_string(),
                "trend".to_string(),
            ],
            json!({
                "rule": "postgres_worsening_trend",
                "sample_count": trend.sample_count,
                "headline": trend.headline,
            }),
            vec![EvidenceSeed {
                evidence_type: "metric_trend".to_string(),
                source_kind: "database".to_string(),
                source_ref: trend.service_key.clone(),
                summary: trend.headline.clone(),
                payload: json!({
                    "sample_count": trend.sample_count,
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "raw_latest": trend.raw_latest,
                }),
            }],
            vec![
                provenance_seed(
                    "analysis",
                    "deterministic_rule",
                    "conductor.findings",
                    "postgres_worsening_trend",
                    Some(if sustained { 0.88 } else { 0.76 }),
                    json!({"service": "postgres"}),
                ),
                provenance_seed(
                    "recommendation",
                    "deterministic_rule",
                    "conductor.planner",
                    "stabilize_postgres_trend",
                    Some(if sustained { 0.88 } else { 0.76 }),
                    json!({"service": "postgres", "priority": if sustained { 82 } else { 75 }}),
                ),
            ],
            RecommendationSeed {
                dedupe_key: "postgres:worsening_trend".to_string(),
                title: "Stabilize Postgres shared-state pressure".to_string(),
                summary: format!(
                    "Postgres shows a {} worsening shared-state trend across {} samples. Reduce query, connection, or transaction pressure before persistence becomes the bottleneck. Current headline: {}",
                    if sustained { "sustained" } else { "emerging" },
                    trend.sample_count,
                    metrics_summary
                ),
                target_service: Some("postgres".to_string()),
                priority: if sustained { 82 } else { 75 },
                tags: vec![
                    "database".to_string(),
                    "durability".to_string(),
                    "trend".to_string(),
                ],
                plan: json!({
                    "action": "stabilize_postgres_trend",
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "sample_count": trend.sample_count,
                }),
                depends_on: Vec::new(),
            },
        )],
        "shared-storage" => vec![build_detected_finding(
            existing_by_key.get("shared_storage_worsening_trend"),
            source_run_id,
            "shared_storage_worsening_trend",
            "Shared storage pressure trend is worsening",
            &format!(
                "Shared storage shows a {} worsening durability trend across {} samples. Recover capacity or mount health before persistent services start competing for scarce storage. Current headline: {}",
                if sustained { "sustained" } else { "emerging" },
                trend.sample_count,
                metrics_summary
            ),
            "durability",
            if sustained {
                FindingSeverity::High
            } else {
                FindingSeverity::Medium
            },
            Some("shared-storage".to_string()),
            None,
            if sustained { 0.87 } else { 0.75 },
            vec![
                "storage".to_string(),
                "durability".to_string(),
                "trend".to_string(),
            ],
            json!({
                "rule": "shared_storage_worsening_trend",
                "sample_count": trend.sample_count,
                "headline": trend.headline,
            }),
            vec![EvidenceSeed {
                evidence_type: "metric_trend".to_string(),
                source_kind: "storage".to_string(),
                source_ref: trend.service_key.clone(),
                summary: trend.headline.clone(),
                payload: json!({
                    "sample_count": trend.sample_count,
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "raw_latest": trend.raw_latest,
                }),
            }],
            vec![
                provenance_seed(
                    "analysis",
                    "deterministic_rule",
                    "conductor.findings",
                    "shared_storage_worsening_trend",
                    Some(if sustained { 0.87 } else { 0.75 }),
                    json!({"service": "shared-storage"}),
                ),
                provenance_seed(
                    "recommendation",
                    "deterministic_rule",
                    "conductor.planner",
                    "protect_shared_storage_trend",
                    Some(if sustained { 0.87 } else { 0.75 }),
                    json!({"service": "shared-storage", "priority": if sustained { 80 } else { 73 }}),
                ),
            ],
            RecommendationSeed {
                dedupe_key: "shared-storage:worsening_trend".to_string(),
                title: "Stabilize shared storage durability trend".to_string(),
                summary: format!(
                    "Shared storage shows a {} worsening durability trend across {} samples. Recover capacity or mount health before persistent services start competing for scarce storage. Current headline: {}",
                    if sustained { "sustained" } else { "emerging" },
                    trend.sample_count,
                    metrics_summary
                ),
                target_service: Some("shared-storage".to_string()),
                priority: if sustained { 80 } else { 73 },
                tags: vec![
                    "storage".to_string(),
                    "durability".to_string(),
                    "trend".to_string(),
                ],
                plan: json!({
                    "action": "protect_shared_storage_trend",
                    "headline": trend.headline,
                    "metrics": trend.metrics,
                    "sample_count": trend.sample_count,
                }),
                depends_on: Vec::new(),
            },
        )],
        _ => Vec::new(),
    }
}

fn build_detected_finding(
    existing: Option<&&FindingRecord>,
    source_run_id: Option<Uuid>,
    finding_key: &str,
    title: &str,
    summary: &str,
    category: &str,
    severity: FindingSeverity,
    target_service: Option<String>,
    target_repository: Option<String>,
    confidence_score: f64,
    tags: Vec<String>,
    details: Value,
    evidence_seeds: Vec<EvidenceSeed>,
    provenance_seeds: Vec<ProvenanceSeed>,
    recommendation: RecommendationSeed,
) -> DetectedFinding {
    let now = now_utc();
    let existing = existing.copied();
    let finding_id = existing.map(|item| item.id).unwrap_or_else(Uuid::new_v4);
    let finding = FindingRecord {
        id: finding_id,
        finding_key: finding_key.to_string(),
        title: title.to_string(),
        summary: summary.to_string(),
        category: category.to_string(),
        severity,
        status: existing
            .map(|item| item.status)
            .unwrap_or(FindingStatus::Open),
        target_service,
        target_repository,
        source_run_id,
        confidence_score,
        tags: unique_strings(tags),
        details,
        first_seen_at: existing.map(|item| item.first_seen_at).unwrap_or(now),
        last_seen_at: now,
        updated_at: now,
    };

    let evidence = evidence_seeds
        .into_iter()
        .map(|seed| FindingEvidence {
            id: Uuid::new_v4(),
            finding_id,
            evidence_type: seed.evidence_type,
            source_kind: seed.source_kind,
            source_ref: seed.source_ref,
            summary: seed.summary,
            payload: seed.payload,
            collected_at: now,
        })
        .collect();

    let provenance = provenance_seeds
        .into_iter()
        .map(|seed| FindingProvenance {
            id: Uuid::new_v4(),
            finding_id,
            stage: seed.stage,
            origin: seed.origin,
            component: seed.component,
            detail: seed.detail,
            confidence_score: seed.confidence_score,
            payload: seed.payload,
            recorded_at: now,
        })
        .collect();

    DetectedFinding {
        finding,
        evidence,
        provenance,
        recommendation,
    }
}

fn probe_metrics(service: &ServiceSnapshot) -> Value {
    service
        .probe
        .get("metrics")
        .cloned()
        .unwrap_or_else(|| json!({}))
}

fn extract_f64(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|item| match item {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    })
}

fn prometheus_target_failures(service: &ServiceSnapshot) -> Vec<(String, i64, i64, Vec<String>)> {
    let metrics = probe_metrics(service);
    let Some(jobs) = metrics
        .get("targets")
        .and_then(|value| value.get("jobs"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };

    let mut by_service = BTreeMap::<String, (i64, i64, Vec<String>)>::new();
    for job in jobs {
        let down_targets = extract_i64(job, "down_targets").unwrap_or(0);
        if down_targets <= 0 {
            continue;
        }
        let service_key = job
            .get("service_key")
            .and_then(Value::as_str)
            .or_else(|| job.get("job").and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(service_key) = service_key else {
            continue;
        };
        let total_targets = extract_i64(job, "total_targets").unwrap_or(down_targets);
        let errors = job
            .get("last_errors")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let entry = by_service
            .entry(service_key.to_string())
            .or_insert((0, 0, Vec::new()));
        entry.0 += down_targets;
        entry.1 += total_targets.max(down_targets);
        for error in errors {
            if !entry.2.contains(&error) {
                entry.2.push(error);
            }
        }
    }

    by_service
        .into_iter()
        .map(|(service_key, (down_targets, total_targets, errors))| {
            (service_key, down_targets, total_targets, errors)
        })
        .collect()
}

fn provenance_seed(
    stage: &str,
    origin: &str,
    component: &str,
    detail: &str,
    confidence_score: Option<f64>,
    payload: Value,
) -> ProvenanceSeed {
    ProvenanceSeed {
        stage: stage.to_string(),
        origin: origin.to_string(),
        component: component.to_string(),
        detail: detail.to_string(),
        confidence_score,
        payload,
    }
}

fn service_evidence_payload(service: &ServiceSnapshot) -> Value {
    json!({
        "service_key": service.service_key,
        "display_name": service.display_name,
        "health": service.health.as_str(),
        "capabilities": service.capabilities,
        "dependencies": service.dependencies,
        "public_url": service.public_url,
        "internal_url": service.internal_url,
        "repo_path": service.repo_path,
        "probe": service.probe,
    })
}

fn repository_evidence_payload(repository: &RepositorySnapshot) -> Value {
    json!({
        "repo_key": repository.repo_key,
        "name": repository.name,
        "archived": repository.archived,
        "criticality": repository.criticality,
        "capabilities": repository.capabilities,
        "linked_services": repository.linked_services,
        "deployment_type": repository.deployment_type,
        "runtime_type": repository.runtime_type,
        "inventory_sources": repository.inventory_sources,
    })
}

fn extract_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|item| match item {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{DeliveryStage, ServiceHealth, now_utc};

    fn base_service(service_key: &str) -> ServiceSnapshot {
        ServiceSnapshot {
            service_key: service_key.to_string(),
            display_name: service_key.to_uppercase(),
            kind: "tenant_service".to_string(),
            role_name: format!("role_{}", service_key),
            playbooks: vec![format!("{}_site.yml", service_key)],
            host_targets: vec!["rk1".to_string()],
            hosts: vec!["rk1".to_string()],
            namespace: Some(service_key.to_string()),
            service_name: Some(service_key.to_string()),
            deployment_environment: Some(DeliveryStage::Production),
            internal_url: Some(format!(
                "http://{}.{}.svc.cluster.local:8080",
                service_key, service_key
            )),
            public_url: Some(format!("https://{}.neuralmimicry.ai", service_key)),
            repo_path: Some(format!("/tmp/{}", service_key)),
            repo_url: None,
            repo_branch: None,
            health: ServiceHealth::Healthy,
            capabilities: vec!["persistent_storage".to_string()],
            dependencies: vec![],
            storage_paths: vec![],
            raw_defaults: json!({}),
            probe: json!({}),
            discovered_at: now_utc(),
            updated_at: now_utc(),
        }
    }

    fn base_repository(repo_key: &str) -> RepositorySnapshot {
        RepositorySnapshot {
            repo_key: repo_key.to_string(),
            name: repo_key.to_string(),
            owner: Some("neuralmimicry".to_string()),
            repo_url: Some(format!("https://github.com/neuralmimicry/{}", repo_key)),
            local_path: Some(format!("/tmp/{}", repo_key)),
            default_branch: Some("main".to_string()),
            current_branch: Some("main".to_string()),
            language: Some("rust".to_string()),
            frameworks: vec![],
            build_systems: vec!["cargo".to_string()],
            package_managers: vec!["cargo".to_string()],
            runtime_type: Some("service".to_string()),
            deployment_type: Some("kubernetes".to_string()),
            purpose: Some("application".to_string()),
            criticality: "high".to_string(),
            visibility: Some("private".to_string()),
            archived: false,
            linked_services: vec!["gail".to_string()],
            dependencies: vec![],
            capabilities: vec!["containerised".to_string()],
            inventory_sources: vec!["local".to_string()],
            metadata: json!({}),
            discovered_at: now_utc(),
            updated_at: now_utc(),
        }
    }

    #[test]
    fn detect_findings_flags_unhealthy_service() {
        let mut service = base_service("gail");
        service.health = ServiceHealth::Degraded;

        let detected = detect_findings(&[service], &[], &[], None, &[]);

        assert!(
            detected
                .iter()
                .any(|item| item.recommendation.dedupe_key == "stabilize:gail")
        );
    }

    #[test]
    fn detect_findings_flags_repository_without_tests() {
        let repository = base_repository("gail");

        let detected = detect_findings(&[], &[repository], &[], None, &[]);

        assert!(
            detected
                .iter()
                .any(|item| item.finding.finding_key == "repository_test_baseline:gail")
        );
    }

    #[test]
    fn detect_findings_preserves_existing_first_seen_timestamp() {
        let repository = base_repository("tracey");
        let existing = FindingRecord {
            id: Uuid::new_v4(),
            finding_key: "repository_test_baseline:tracey".to_string(),
            title: "Existing".to_string(),
            summary: "Existing".to_string(),
            category: "testability".to_string(),
            severity: FindingSeverity::Medium,
            status: FindingStatus::Open,
            target_service: Some("tracey".to_string()),
            target_repository: Some("tracey".to_string()),
            source_run_id: None,
            confidence_score: 0.5,
            tags: vec![],
            details: json!({}),
            first_seen_at: now_utc(),
            last_seen_at: now_utc(),
            updated_at: now_utc(),
        };

        let detected = detect_findings(&[], &[repository], &[], None, &[existing.clone()]);
        let finding = detected
            .iter()
            .find(|item| item.finding.finding_key == existing.finding_key)
            .expect("finding");

        assert_eq!(finding.finding.id, existing.id);
        assert_eq!(finding.finding.first_seen_at, existing.first_seen_at);
    }

    #[test]
    fn detect_findings_flags_prometheus_target_failures() {
        let mut prometheus = base_service("prometheus");
        prometheus.probe = json!({
            "metrics": {
                "targets": {
                    "jobs": [
                        {
                            "job": "tracey",
                            "service_key": "tracey",
                            "total_targets": 2,
                            "healthy_targets": 1,
                            "down_targets": 1,
                            "last_errors": ["connection refused"]
                        }
                    ]
                }
            }
        });

        let tracey = base_service("tracey");
        let detected = detect_findings(&[prometheus, tracey], &[], &[], None, &[]);

        assert!(detected.iter().any(|item| {
            item.finding.finding_key == "prometheus_target_health:tracey"
                && item.recommendation.dedupe_key == "prometheus:coverage:tracey"
        }));
    }

    #[test]
    fn detect_findings_flags_shared_storage_pressure() {
        let mut storage = base_service("shared-storage");
        storage.kind = "storage".to_string();
        storage.probe = json!({
            "metrics": {
                "filesystem": {
                    "usage_ratio": 0.92,
                    "inode_usage_ratio": 0.15,
                    "read_only": false
                },
                "missing_subdirectories": ["postgres"]
            }
        });

        let detected = detect_findings(&[storage], &[], &[], None, &[]);

        assert!(
            detected
                .iter()
                .any(|item| item.finding.finding_key == "shared_storage_pressure")
        );
    }
}
