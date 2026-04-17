use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use serde_json::{Value, json};

use crate::{
    integrations::gail_plan_summary,
    models::{
        ImprovementCycle, NewWorkItem, RunStatus, ServiceHealth, ServiceSnapshot,
        ServiceTrendSummary, WorkItem, WorkStatus, now_utc,
    },
    repository::ConductorRepository,
    trends::{pressure_score, summarize_trends},
};

#[derive(Clone, Debug)]
pub struct ImprovementRecommendation {
    pub dedupe_key: String,
    pub title: String,
    pub summary: String,
    pub target_service: Option<String>,
    pub priority: i32,
    pub tags: Vec<String>,
    pub plan: Value,
    pub depends_on: Vec<String>,
}

pub async fn run_planning_cycle(
    repository: &dyn ConductorRepository,
    client: &reqwest::Client,
    config: &crate::config::ConductorConfig,
) -> Result<ImprovementCycle> {
    let started_at = now_utc();
    let services = repository.list_service_snapshots().await?;
    let metric_samples = repository
        .list_service_metric_samples(None, services.len().saturating_mul(24).max(64))
        .await?;
    let trends = summarize_trends(&metric_samples);
    let recommendations =
        derive_recommendations(&services, &trends, config.planning.minimum_priority);

    if config.planning.auto_queue {
        for recommendation in &recommendations {
            upsert_recommendation(repository, recommendation).await?;
        }
    }

    let topology_summary = json!({
        "services": services.iter().map(|service| json!({
            "service_key": service.service_key,
            "health": service.health.as_str(),
            "dependencies": service.dependencies,
            "capabilities": service.capabilities,
            "probe": service.probe,
        })).collect::<Vec<_>>(),
        "trends": trends.iter().map(|trend| json!({
            "service_key": trend.service_key,
            "direction": trend.direction,
            "sample_count": trend.sample_count,
            "headline": trend.headline,
            "metrics": trend.metrics,
        })).collect::<Vec<_>>(),
        "recommendation_count": recommendations.len(),
    });

    let gail_base_url = services
        .iter()
        .find(|service| service.service_key == "gail")
        .and_then(|service| {
            service
                .public_url
                .as_deref()
                .or(service.internal_url.as_deref())
        });
    let gail_response = gail_plan_summary(client, config, &topology_summary, gail_base_url).await?;
    let cycle = ImprovementCycle {
        id: uuid::Uuid::new_v4(),
        status: RunStatus::Success,
        summary: if recommendations.is_empty() {
            "No new improvement items were queued; topology is stable or awaiting admin action."
                .to_string()
        } else if !config.planning.auto_queue {
            format!(
                "Identified {} improvement items across {} services; auto-queue is disabled.",
                recommendations.len(),
                unique_service_targets(&recommendations).len()
            )
        } else {
            format!(
                "Queued {} improvement items across {} services.",
                recommendations.len(),
                unique_service_targets(&recommendations).len()
            )
        },
        source_services: unique_service_targets(&recommendations),
        recommendations: recommendations
            .iter()
            .map(recommendation_to_value)
            .collect(),
        gail_response,
        started_at,
        finished_at: now_utc(),
    };

    repository.insert_improvement_cycle(&cycle).await?;
    Ok(cycle)
}

fn derive_recommendations(
    services: &[ServiceSnapshot],
    trends: &[ServiceTrendSummary],
    minimum_priority: i32,
) -> Vec<ImprovementRecommendation> {
    let mut recommendations = Vec::new();
    let trends_by_service = trends
        .iter()
        .map(|trend| (trend.service_key.as_str(), trend))
        .collect::<BTreeMap<_, _>>();

    for service in services {
        if matches!(
            service.health,
            ServiceHealth::Degraded | ServiceHealth::Unreachable | ServiceHealth::Missing
        ) {
            recommendations.push(ImprovementRecommendation {
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
                plan: json!({"action": "stabilize_service", "service": service.service_key}),
                depends_on: Vec::new(),
            });
        }

        if service.service_key == "gail"
            && !service.capabilities.contains(&"local_repo".to_string())
        {
            recommendations.push(ImprovementRecommendation {
                dedupe_key: "gail:repo_visibility".to_string(),
                title: "Restore Gail repository visibility".to_string(),
                summary: "Conductor should improve the live Gail gateway from source, but the local Gail repository path is missing or unreadable.".to_string(),
                target_service: Some("gail".to_string()),
                priority: 80,
                tags: vec!["governance".to_string(), "source_of_truth".to_string()],
                plan: json!({"action": "verify_repo_hint", "service": "gail"}),
                depends_on: Vec::new(),
            });
        }

        if service.service_key == "aarnn" {
            let replicas = extract_i64(
                &service.raw_defaults,
                "continuum_tenant_aarnn_web_ui_replicas",
            )
            .unwrap_or(1);
            if replicas == 1 {
                recommendations.push(ImprovementRecommendation {
                    dedupe_key: "aarnn:singleton_web_ui".to_string(),
                    title: "Externalize AARNN web-session coordination".to_string(),
                    summary: "AARNN is configured as a singleton web UI. Externalize session and runtime coordination before scaling the control surface horizontally.".to_string(),
                    target_service: Some("aarnn".to_string()),
                    priority: 78,
                    tags: vec!["scalability".to_string(), "aarnn".to_string()],
                    plan: json!({"action": "externalize_aarnn_sessions"}),
                    depends_on: Vec::new(),
                });
            }
        }

        if service.service_key == "refiner" {
            recommendations.push(ImprovementRecommendation {
                dedupe_key: "refiner:solver_loop".to_string(),
                title: "Use Refiner as the controlled code-improvement executor".to_string(),
                summary: "Route concrete code-change proposals into Refiner's project-solver/job APIs so Conductor can stage self-improvement work with verification and audit trails.".to_string(),
                target_service: Some("refiner".to_string()),
                priority: 72,
                tags: vec!["automation".to_string(), "code_generation".to_string()],
                plan: json!({"action": "integrate_refiner_jobs", "paths": ["/api/jobs", "/api/playground/plan"]}),
                depends_on: Vec::new(),
            });
        }

        if service.service_key == "tracey" {
            let pressure = pressure_score(&service.probe);
            if pressure >= 0.75 {
                recommendations.push(ImprovementRecommendation {
                    dedupe_key: "tracey:resource_hotspot".to_string(),
                    title: "Investigate Tracey-reported resource hotspots".to_string(),
                    summary: "Tracey is surfacing elevated pressure or latency. Rebalance workloads, trim noisy loops, or increase capacity before the hotspot becomes a bottleneck.".to_string(),
                    target_service: Some("tracey".to_string()),
                    priority: 85,
                    tags: vec!["performance".to_string(), "resource_utilisation".to_string()],
                    plan: json!({"action": "investigate_resource_hotspot", "pressure_score": pressure}),
                    depends_on: Vec::new(),
                });
            }
        }

        if service.service_key == "continuum"
            && !service
                .capabilities
                .contains(&"adaptive_scaling".to_string())
        {
            recommendations.push(ImprovementRecommendation {
                dedupe_key: "continuum:adaptive_loop".to_string(),
                title: "Tighten Continuum adaptive loop wiring".to_string(),
                summary: "Conductor expects Continuum to feed plan/ramp/optimise/repeat signals. Ensure the Tracey fleet and recruitment paths are visible through the Continuum control plane.".to_string(),
                target_service: Some("continuum".to_string()),
                priority: 70,
                tags: vec!["control_plane".to_string(), "autoscaling".to_string()],
                plan: json!({"action": "verify_continuum_adaptive_loop"}),
                depends_on: Vec::new(),
            });
        }

        if service.dependencies.contains(&"postgres".to_string())
            && !service
                .capabilities
                .contains(&"persistent_storage".to_string())
        {
            recommendations.push(ImprovementRecommendation {
                dedupe_key: format!("storage:{}", service.service_key),
                title: format!("Verify persistent state strategy for {}", service.display_name),
                summary: format!("{} depends on shared data services but no clear persistent-storage profile was inferred from Ansible. Confirm PVCs or durable mounts before further automation.", service.display_name),
                target_service: Some(service.service_key.clone()),
                priority: 68,
                tags: vec!["storage".to_string(), "durability".to_string()],
                plan: json!({"action": "verify_persistent_storage", "service": service.service_key}),
                depends_on: Vec::new(),
            });
        }

        if let Some(trend) = trends_by_service.get(service.service_key.as_str()) {
            push_trend_recommendations(service, trend, &mut recommendations);
        }
    }

    let stabilization_keys = recommendations
        .iter()
        .filter(|recommendation| recommendation.dedupe_key.starts_with("stabilize:"))
        .map(|recommendation| recommendation.dedupe_key.clone())
        .collect::<BTreeSet<_>>();
    for recommendation in &mut recommendations {
        let Some(service) = recommendation.target_service.as_deref() else {
            continue;
        };
        let stabilize_key = format!("stabilize:{}", service);
        if recommendation.dedupe_key != stabilize_key && stabilization_keys.contains(&stabilize_key)
        {
            recommendation.depends_on = vec![stabilize_key];
        }
    }

    recommendations.retain(|item| item.priority >= minimum_priority);
    recommendations.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.dedupe_key.cmp(&right.dedupe_key))
    });
    recommendations
}

fn push_trend_recommendations(
    service: &ServiceSnapshot,
    trend: &ServiceTrendSummary,
    recommendations: &mut Vec<ImprovementRecommendation>,
) {
    if trend.direction != "worsening" {
        return;
    }

    let sustained = trend.sample_count >= 3;
    let qualifier = if sustained { "sustained" } else { "emerging" };
    let top_metrics = trend
        .metrics
        .iter()
        .take(3)
        .map(|metric| {
            format!(
                "{}={:.2} ({})",
                metric.metric_name, metric.latest, metric.direction
            )
        })
        .collect::<Vec<_>>();
    let metrics_summary = if top_metrics.is_empty() {
        trend.headline.clone()
    } else {
        top_metrics.join(", ")
    };

    match service.service_key.as_str() {
        "tracey" => recommendations.push(ImprovementRecommendation {
            dedupe_key: "tracey:worsening_trend".to_string(),
            title: "Stabilize Tracey pressure trend".to_string(),
            summary: format!(
                "Tracey shows a {} worsening telemetry trend across {} samples. Focus on the highest-pressure signals before the adaptive loop starts making poorer placement decisions. Current headline: {}",
                qualifier, trend.sample_count, metrics_summary
            ),
            target_service: Some(service.service_key.clone()),
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
        }),
        "continuum" => recommendations.push(ImprovementRecommendation {
            dedupe_key: "continuum:worsening_trend".to_string(),
            title: "Correct Continuum adaptive drift".to_string(),
            summary: format!(
                "Continuum shows a {} worsening control-plane trend across {} samples. Tighten recruitment, placement, or backlog handling before orchestration latency compounds. Current headline: {}",
                qualifier, trend.sample_count, metrics_summary
            ),
            target_service: Some(service.service_key.clone()),
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
        }),
        _ => {}
    }
}

async fn upsert_recommendation(
    repository: &dyn ConductorRepository,
    recommendation: &ImprovementRecommendation,
) -> Result<()> {
    if let Some(existing) = repository
        .find_work_item_by_dedupe_key(&recommendation.dedupe_key)
        .await?
    {
        if existing.admin_override {
            return Ok(());
        }
        repository
            .patch_work_item(
                existing.id,
                crate::models::WorkItemPatch {
                    title: Some(recommendation.title.clone()),
                    summary: Some(recommendation.summary.clone()),
                    target_service: recommendation.target_service.clone(),
                    priority: Some(recommendation.priority),
                    tags: Some(recommendation.tags.clone()),
                    plan: Some(recommendation.plan.clone()),
                    depends_on: Some(recommendation.depends_on.clone()),
                    note: Some("planner refreshed recommendation".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        return Ok(());
    }

    let item = WorkItem::from_new(NewWorkItem {
        dedupe_key: Some(recommendation.dedupe_key.clone()),
        title: recommendation.title.clone(),
        summary: recommendation.summary.clone(),
        target_service: recommendation.target_service.clone(),
        status: Some(WorkStatus::Planned),
        priority: Some(recommendation.priority),
        progress_pct: Some(0),
        admin_override: false,
        execution_approved: false,
        verification_required: Some(true),
        tags: recommendation.tags.clone(),
        plan: recommendation.plan.clone(),
        depends_on: recommendation.depends_on.clone(),
        source: Some("planner".to_string()),
        scheduled_for: None,
    });
    repository.upsert_work_item(&item).await
}

fn recommendation_to_value(recommendation: &ImprovementRecommendation) -> Value {
    json!({
        "dedupe_key": recommendation.dedupe_key,
        "title": recommendation.title,
        "summary": recommendation.summary,
        "target_service": recommendation.target_service,
        "priority": recommendation.priority,
        "tags": recommendation.tags,
        "plan": recommendation.plan,
        "depends_on": recommendation.depends_on,
    })
}

fn unique_service_targets(recommendations: &[ImprovementRecommendation]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for recommendation in recommendations {
        if let Some(service) = &recommendation.target_service {
            set.insert(service.clone());
        }
    }
    set.into_iter().collect()
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
    use crate::models::ServiceSnapshot;

    #[test]
    fn planner_flags_degraded_services() {
        let service = ServiceSnapshot {
            service_key: "gail".to_string(),
            display_name: "Gail".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "continuum_tenant_gail".to_string(),
            playbooks: vec!["continuum_tenant_gail_site.yml".to_string()],
            hosts: vec!["rk1".to_string()],
            namespace: Some("gail".to_string()),
            service_name: Some("gail".to_string()),
            internal_url: Some("http://gail.gail.svc.cluster.local:8080".to_string()),
            public_url: Some("https://gail.neuralmimicry.ai".to_string()),
            repo_path: Some("/tmp/gail".to_string()),
            repo_url: None,
            repo_branch: None,
            health: ServiceHealth::Degraded,
            capabilities: vec!["ai_gateway".to_string()],
            dependencies: vec![],
            storage_paths: vec![],
            raw_defaults: json!({}),
            probe: json!({"error": "timeout"}),
            discovered_at: now_utc(),
            updated_at: now_utc(),
        };

        let recommendations = derive_recommendations(&[service], &[], 0);
        assert!(
            recommendations
                .iter()
                .any(|item| item.dedupe_key == "stabilize:gail")
        );
    }

    #[test]
    fn planner_adds_dependency_edges_for_follow_up_work_on_degraded_services() {
        let service = ServiceSnapshot {
            service_key: "gail".to_string(),
            display_name: "Gail".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "continuum_tenant_gail".to_string(),
            playbooks: vec!["continuum_tenant_gail_site.yml".to_string()],
            hosts: vec!["rk1".to_string()],
            namespace: Some("gail".to_string()),
            service_name: Some("gail".to_string()),
            internal_url: Some("http://gail.gail.svc.cluster.local:8080".to_string()),
            public_url: Some("https://gail.neuralmimicry.ai".to_string()),
            repo_path: None,
            repo_url: None,
            repo_branch: None,
            health: ServiceHealth::Degraded,
            capabilities: vec!["ai_gateway".to_string()],
            dependencies: vec![],
            storage_paths: vec![],
            raw_defaults: json!({}),
            probe: json!({"error": "timeout"}),
            discovered_at: now_utc(),
            updated_at: now_utc(),
        };

        let recommendations = derive_recommendations(&[service], &[], 0);
        let repo_visibility = recommendations
            .iter()
            .find(|item| item.dedupe_key == "gail:repo_visibility")
            .expect("repo visibility follow-up");
        assert_eq!(
            repo_visibility.depends_on,
            vec!["stabilize:gail".to_string()]
        );
    }

    #[test]
    fn planner_flags_worsening_tracey_trend() {
        let service = ServiceSnapshot {
            service_key: "tracey".to_string(),
            display_name: "Tracey".to_string(),
            kind: "host_agent".to_string(),
            role_name: "tracey_host_agent".to_string(),
            playbooks: vec!["tracey_host_agent.yml".to_string()],
            hosts: vec!["qc01".to_string()],
            namespace: None,
            service_name: None,
            internal_url: None,
            public_url: None,
            repo_path: Some("/tmp/tracey".to_string()),
            repo_url: None,
            repo_branch: None,
            health: ServiceHealth::Healthy,
            capabilities: vec!["resource_insights".to_string()],
            dependencies: vec![],
            storage_paths: vec![],
            raw_defaults: json!({}),
            probe: json!({"metrics": {"status": {"pressure_score": 0.6}}}),
            discovered_at: now_utc(),
            updated_at: now_utc(),
        };
        let trend = ServiceTrendSummary {
            service_key: "tracey".to_string(),
            sample_count: 4,
            window_start: Some(now_utc()),
            window_end: Some(now_utc()),
            direction: "worsening".to_string(),
            headline: "tracey trend is worsening via pressure_score".to_string(),
            metrics: vec![],
            raw_latest: json!({"pressure_score": 0.8}),
        };

        let recommendations = derive_recommendations(&[service], &[trend], 0);
        assert!(
            recommendations
                .iter()
                .any(|item| item.dedupe_key == "tracey:worsening_trend")
        );
    }
}
