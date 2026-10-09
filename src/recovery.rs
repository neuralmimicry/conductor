use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::{
    config::RecoveryTargetConfig,
    models::{ServiceHealth, ServiceSnapshot},
};

#[derive(Clone, Debug, Serialize)]
pub struct RecoveryAssessment {
    pub service_key: String,
    pub namespace: String,
    pub deployment: String,
    pub affected_services: Vec<String>,
    pub blockers: Vec<String>,
    pub eligible: bool,
}

/// Build a conservative view of the dependency component that can be affected
/// by restarting one workload. Any missing, duplicated, stale, or dangling
/// service record makes the graph unsafe to use for an automated decision.
pub fn assess_recovery_target(
    target: &RecoveryTargetConfig,
    services: &[ServiceSnapshot],
    now: DateTime<Utc>,
    max_age_seconds: u64,
) -> RecoveryAssessment {
    let mut assessment = RecoveryAssessment {
        service_key: target.service_key.clone(),
        namespace: target.namespace.clone(),
        deployment: target.deployment.clone(),
        affected_services: Vec::new(),
        blockers: Vec::new(),
        eligible: false,
    };

    let mut by_key = BTreeMap::new();
    for service in services {
        if by_key
            .insert(service.service_key.as_str(), service)
            .is_some()
        {
            assessment.blockers.push(format!(
                "service graph contains duplicate key `{}`",
                service.service_key
            ));
        }
        if !snapshot_is_fresh(service, now, max_age_seconds) {
            assessment.blockers.push(format!(
                "service graph record `{}` is stale or from the future",
                service.service_key
            ));
        }
    }

    let Some(target_service) = by_key.get(target.service_key.as_str()).copied() else {
        assessment
            .blockers
            .push("configured service key is absent from the discovered service graph".to_string());
        assessment.blockers.sort();
        assessment.blockers.dedup();
        return assessment;
    };

    if target_service.namespace.as_deref() != Some(target.namespace.as_str()) {
        assessment.blockers.push(format!(
            "configured namespace `{}` does not exactly match discovered namespace `{}`",
            target.namespace,
            target_service.namespace.as_deref().unwrap_or("unknown")
        ));
    }
    if !matches!(
        target_service.health,
        ServiceHealth::Degraded | ServiceHealth::Unreachable
    ) {
        assessment.blockers.push(format!(
            "target health is `{}`; recovery is considered only for degraded or unreachable workloads",
            target_service.health.as_str()
        ));
    }

    let mut graph = BTreeMap::<&str, BTreeSet<&str>>::new();
    let mut dangling_dependencies = Vec::new();
    for service in services {
        graph.entry(service.service_key.as_str()).or_default();
        for dependency in &service.dependencies {
            let dependency = dependency.trim();
            if dependency.is_empty() || !by_key.contains_key(dependency) {
                dangling_dependencies.push((service.service_key.as_str(), dependency.to_string()));
                continue;
            }
            graph
                .entry(service.service_key.as_str())
                .or_default()
                .insert(dependency);
            graph
                .entry(dependency)
                .or_default()
                .insert(service.service_key.as_str());
        }
    }

    let mut visited = BTreeSet::new();
    let mut pending = VecDeque::from([target.service_key.as_str()]);
    while let Some(key) = pending.pop_front() {
        if !visited.insert(key) {
            continue;
        }
        if let Some(neighbors) = graph.get(key) {
            pending.extend(neighbors.iter().copied());
        }
    }
    for (source, dependency) in dangling_dependencies {
        if visited.contains(source) {
            assessment.blockers.push(format!(
                "affected service `{source}` references missing dependency `{dependency}`"
            ));
        }
    }
    assessment.affected_services = visited.iter().map(|key| (*key).to_string()).collect();

    // The services coupled to the target must be healthy before a restart is
    // considered: otherwise this action could compound an existing incident.
    for key in visited
        .iter()
        .copied()
        .filter(|key| *key != target.service_key)
    {
        match by_key.get(key).copied() {
            Some(service) if service.health == ServiceHealth::Healthy => {}
            Some(service) => assessment.blockers.push(format!(
                "affected prerequisite/dependent `{key}` is `{}`",
                service.health.as_str()
            )),
            None => assessment.blockers.push(format!(
                "affected service `{key}` is missing from the current graph"
            )),
        }
    }

    for required in ["prometheus", "grafana"] {
        match by_key.get(required).copied() {
            Some(service)
                if snapshot_is_fresh(service, now, max_age_seconds)
                    && service.health == ServiceHealth::Healthy => {}
            Some(service) => assessment.blockers.push(format!(
                "{required} evidence is unavailable, stale, or `{}`",
                service.health.as_str()
            )),
            None => assessment.blockers.push(format!(
                "{required} is absent from the discovered service graph"
            )),
        }
    }

    if !prometheus_confirms_target_down(by_key.get("prometheus").copied(), &target.service_key) {
        assessment.blockers.push(format!(
            "Prometheus has no current down-target evidence for `{}`",
            target.service_key
        ));
    }

    assessment.blockers.sort();
    assessment.blockers.dedup();
    assessment.eligible = assessment.blockers.is_empty();
    assessment
}

/// Require a fresh dependency component and known host placement before any
/// live rollout. The target may be degraded because the work can be corrective;
/// its prerequisites and transitive dependents must be healthy.
pub fn deployment_dependency_blockers(
    target_key: &str,
    services: &[ServiceSnapshot],
    now: DateTime<Utc>,
    max_age_seconds: u64,
) -> Vec<String> {
    let mut blockers = Vec::new();
    let mut by_key = BTreeMap::new();
    for service in services {
        if by_key
            .insert(service.service_key.as_str(), service)
            .is_some()
        {
            blockers.push(format!("duplicate service key `{}`", service.service_key));
        }
        if !snapshot_is_fresh(service, now, max_age_seconds) {
            blockers.push(format!(
                "service graph record `{}` is stale",
                service.service_key
            ));
        }
    }
    let Some(target) = by_key.get(target_key).copied() else {
        blockers.push(format!(
            "target service `{target_key}` is absent from the current graph"
        ));
        blockers.sort();
        blockers.dedup();
        return blockers;
    };
    if target.hosts.is_empty() {
        blockers.push(format!(
            "target service `{target_key}` has no resolved live host placement"
        ));
    }

    let mut graph = BTreeMap::<&str, BTreeSet<&str>>::new();
    let mut dangling_dependencies = Vec::new();
    for service in services {
        graph.entry(service.service_key.as_str()).or_default();
        for dependency in &service.dependencies {
            let dependency = dependency.trim();
            if dependency.is_empty() || !by_key.contains_key(dependency) {
                dangling_dependencies.push((service.service_key.as_str(), dependency.to_string()));
                continue;
            }
            graph
                .entry(service.service_key.as_str())
                .or_default()
                .insert(dependency);
            graph
                .entry(dependency)
                .or_default()
                .insert(service.service_key.as_str());
        }
    }
    let mut visited = BTreeSet::new();
    let mut pending = VecDeque::from([target_key]);
    while let Some(key) = pending.pop_front() {
        if !visited.insert(key) {
            continue;
        }
        if let Some(neighbors) = graph.get(key) {
            pending.extend(neighbors.iter().copied());
        }
    }
    for (source, dependency) in dangling_dependencies {
        if visited.contains(source) {
            blockers.push(format!(
                "affected service `{source}` references missing dependency `{dependency}`"
            ));
        }
    }
    for key in visited.iter().copied().filter(|key| *key != target_key) {
        match by_key.get(key).copied() {
            Some(service) if service.health == ServiceHealth::Healthy => {}
            Some(service) => blockers.push(format!(
                "transitive prerequisite/dependent `{key}` is `{}`",
                service.health.as_str()
            )),
            None => blockers.push(format!("affected service `{key}` is absent")),
        }
    }
    for required in ["prometheus", "grafana"] {
        match by_key.get(required).copied() {
            Some(service)
                if snapshot_is_fresh(service, now, max_age_seconds)
                    && service.health == ServiceHealth::Healthy => {}
            Some(service) => blockers.push(format!(
                "{required} deployment evidence is unavailable, stale, or `{}`",
                service.health.as_str()
            )),
            None => blockers.push(format!("{required} deployment evidence is absent")),
        }
    }
    blockers.sort();
    blockers.dedup();
    blockers
}

fn snapshot_is_fresh(service: &ServiceSnapshot, now: DateTime<Utc>, max_age_seconds: u64) -> bool {
    let max_age = Duration::seconds(max_age_seconds.min(i64::MAX as u64) as i64);
    service.updated_at <= now
        && service.discovered_at <= now
        && now - service.updated_at <= max_age
        && now - service.discovered_at <= max_age
}

fn prometheus_confirms_target_down(prometheus: Option<&ServiceSnapshot>, target: &str) -> bool {
    let Some(prometheus) = prometheus else {
        return false;
    };
    prometheus
        .probe
        .pointer("/metrics/targets/jobs")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|jobs| {
            jobs.iter().any(|job| {
                job.get("service_key").and_then(serde_json::Value::as_str) == Some(target)
                    && job
                        .get("down_targets")
                        .and_then(serde_json::Value::as_u64)
                        .is_some_and(|count| count > 0)
            })
        })
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use serde_json::json;

    use super::*;
    use crate::models::now_utc;

    fn service(key: &str, dependencies: &[&str], health: ServiceHealth) -> ServiceSnapshot {
        let now = now_utc();
        ServiceSnapshot {
            service_key: key.to_string(),
            display_name: key.to_string(),
            kind: "tenant_service".to_string(),
            role_name: format!("{key}_role"),
            playbooks: vec![],
            host_targets: vec![],
            hosts: vec!["qc04".to_string()],
            namespace: Some(if key == "api" { "apps" } else { "monitoring" }.to_string()),
            service_name: Some(key.to_string()),
            deployment_environment: None,
            internal_url: Some(format!("http://{key}.svc/health")),
            public_url: None,
            repo_path: None,
            repo_url: None,
            repo_branch: None,
            health,
            capabilities: vec![],
            dependencies: dependencies
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            storage_paths: vec![],
            raw_defaults: json!({}),
            probe: if key == "prometheus" {
                json!({"metrics": {"targets": {"jobs": [{"service_key": "api", "down_targets": 1}]}}})
            } else {
                json!({"metrics": {}})
            },
            discovered_at: now,
            updated_at: now,
        }
    }

    fn target() -> RecoveryTargetConfig {
        RecoveryTargetConfig {
            service_key: "api".to_string(),
            namespace: "apps".to_string(),
            deployment: "api".to_string(),
        }
    }

    fn graph(target_health: ServiceHealth) -> Vec<ServiceSnapshot> {
        vec![
            service("api", &["postgres"], target_health),
            service("postgres", &[], ServiceHealth::Healthy),
            service("consumer", &["api"], ServiceHealth::Healthy),
            service("prometheus", &[], ServiceHealth::Healthy),
            service("grafana", &["prometheus"], ServiceHealth::Healthy),
        ]
    }

    #[test]
    fn allows_only_degraded_target_with_fresh_healthy_impact_graph_and_metrics() {
        let services = graph(ServiceHealth::Degraded);
        let result = assess_recovery_target(&target(), &services, now_utc(), 300);

        assert!(result.eligible, "{:?}", result.blockers);
        assert_eq!(
            result.affected_services,
            vec!["api", "consumer", "postgres"]
        );
    }

    #[test]
    fn blocks_when_a_dependent_is_unhealthy_or_unknown() {
        let mut services = graph(ServiceHealth::Unreachable);
        services[2].health = ServiceHealth::Degraded;

        let result = assess_recovery_target(&target(), &services, now_utc(), 300);

        assert!(!result.eligible);
        assert!(
            result
                .blockers
                .iter()
                .any(|reason| reason.contains("consumer"))
        );
    }

    #[test]
    fn blocks_stale_graph_and_missing_metric_evidence() {
        let mut services = graph(ServiceHealth::Degraded);
        services[0].updated_at = now_utc() - Duration::minutes(10);
        services[3].probe = json!({"metrics": {"targets": {"jobs": []}}});

        let result = assess_recovery_target(&target(), &services, now_utc(), 300);

        assert!(!result.eligible);
        assert!(
            result
                .blockers
                .iter()
                .any(|reason| reason.contains("stale"))
        );
        assert!(
            result
                .blockers
                .iter()
                .any(|reason| reason.contains("Prometheus"))
        );
    }

    #[test]
    fn blocks_unknown_dependency_and_namespace_mismatch() {
        let mut services = graph(ServiceHealth::Degraded);
        services[0]
            .dependencies
            .push("unknown-database".to_string());
        let mut wrong_target = target();
        wrong_target.namespace = "default".to_string();

        let result = assess_recovery_target(&wrong_target, &services, now_utc(), 300);

        assert!(!result.eligible);
        assert!(
            result
                .blockers
                .iter()
                .any(|reason| reason.contains("unknown-database"))
        );
        assert!(
            result
                .blockers
                .iter()
                .any(|reason| reason.contains("namespace"))
        );
    }

    #[test]
    fn deployment_gate_checks_transitive_dependents_and_host_placement() {
        let mut services = graph(ServiceHealth::Degraded);
        services[0].hosts = vec!["qc04".to_string()];
        services.push(service(
            "consumer-ui",
            &["consumer"],
            ServiceHealth::Healthy,
        ));

        assert!(deployment_dependency_blockers("api", &services, now_utc(), 300).is_empty());

        services[5].health = ServiceHealth::Degraded;
        let result = deployment_dependency_blockers("api", &services, now_utc(), 300);
        assert!(result.iter().any(|reason| reason.contains("consumer-ui")));
    }

    #[test]
    fn deployment_gate_blocks_unknown_host_or_stale_inventory() {
        let mut services = graph(ServiceHealth::Healthy);
        services[0].hosts.clear();
        services[2].discovered_at = now_utc() - Duration::minutes(10);

        let result = deployment_dependency_blockers("api", &services, now_utc(), 300);

        assert!(
            result
                .iter()
                .any(|reason| reason.contains("host placement"))
        );
        assert!(result.iter().any(|reason| reason.contains("stale")));
    }
}
