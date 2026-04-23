use std::collections::BTreeSet;

use anyhow::Result;
use serde_json::{Value, json};

use crate::{
    findings::{DetectedFinding, detect_findings},
    integrations::gail_plan_summary,
    models::{
        DeliveryStage, ImprovementCycle, NewWorkItem, RolloutStrategy, RunStatus, WorkItem,
        WorkStatus, now_utc,
    },
    repository::ConductorRepository,
    trends::summarize_trends,
};

#[derive(Clone, Debug)]
pub struct ImprovementRecommendation {
    pub finding_id: uuid::Uuid,
    pub finding_key: String,
    pub dedupe_key: String,
    pub title: String,
    pub summary: String,
    pub target_service: Option<String>,
    pub delivery_stage: DeliveryStage,
    pub rollout_strategy: RolloutStrategy,
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
    let repositories = repository.list_repository_snapshots().await?;
    let existing_findings = repository.list_findings().await?;
    let latest_discovery = repository.list_discovery_runs(1).await?.into_iter().next();
    let metric_samples = repository
        .list_service_metric_samples(None, services.len().saturating_mul(24).max(64))
        .await?;
    let trends = summarize_trends(&metric_samples);
    let detected_findings = detect_findings(
        &services,
        &repositories,
        &trends,
        latest_discovery.as_ref().map(|run| run.id),
        &existing_findings,
    );
    let findings = detected_findings
        .iter()
        .map(|item| item.finding.clone())
        .collect::<Vec<_>>();
    let evidence = detected_findings
        .iter()
        .flat_map(|item| item.evidence.clone())
        .collect::<Vec<_>>();
    let provenance = detected_findings
        .iter()
        .flat_map(|item| item.provenance.clone())
        .collect::<Vec<_>>();
    repository
        .replace_findings(&findings, &evidence, &provenance)
        .await?;
    let recommendations =
        derive_recommendations(&detected_findings, config.planning.minimum_priority);

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
        "repositories": repositories.iter().map(|repository| json!({
            "repo_key": repository.repo_key,
            "linked_services": repository.linked_services,
            "criticality": repository.criticality,
            "capabilities": repository.capabilities,
            "archived": repository.archived,
        })).collect::<Vec<_>>(),
        "findings": findings.iter().map(|finding| json!({
            "finding_key": finding.finding_key,
            "category": finding.category,
            "severity": finding.severity.as_str(),
            "target_service": finding.target_service,
            "target_repository": finding.target_repository,
            "confidence_score": finding.confidence_score,
        })).collect::<Vec<_>>(),
        "trends": trends.iter().map(|trend| json!({
            "service_key": trend.service_key,
            "direction": trend.direction,
            "sample_count": trend.sample_count,
            "headline": trend.headline,
            "metrics": trend.metrics,
        })).collect::<Vec<_>>(),
        "finding_count": findings.len(),
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
            format!(
                "No new improvement items were queued; {} evidence-backed findings remain visible for review.",
                findings.len()
            )
        } else if !config.planning.auto_queue {
            format!(
                "Identified {} improvement items from {} findings across {} services; auto-queue is disabled.",
                recommendations.len(),
                findings.len(),
                unique_service_targets(&recommendations).len()
            )
        } else {
            format!(
                "Queued {} improvement items from {} findings across {} services.",
                recommendations.len(),
                findings.len(),
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
    detected_findings: &[DetectedFinding],
    minimum_priority: i32,
) -> Vec<ImprovementRecommendation> {
    let mut recommendations = detected_findings
        .iter()
        .map(|item| {
            let mut plan = item.recommendation.plan.clone();
            if let Some(object) = plan.as_object_mut() {
                object.insert(
                    "finding_id".to_string(),
                    Value::String(item.finding.id.to_string()),
                );
                object.insert(
                    "finding_key".to_string(),
                    Value::String(item.finding.finding_key.clone()),
                );
            }

            ImprovementRecommendation {
                finding_id: item.finding.id,
                finding_key: item.finding.finding_key.clone(),
                dedupe_key: item.recommendation.dedupe_key.clone(),
                title: item.recommendation.title.clone(),
                summary: item.recommendation.summary.clone(),
                target_service: item.recommendation.target_service.clone(),
                delivery_stage: DeliveryStage::Development,
                rollout_strategy: RolloutStrategy::default_for_stage(DeliveryStage::Development),
                priority: item.recommendation.priority,
                tags: item.recommendation.tags.clone(),
                plan,
                depends_on: item.recommendation.depends_on.clone(),
            }
        })
        .collect::<Vec<_>>();

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
                    delivery_stage: Some(recommendation.delivery_stage),
                    rollout_strategy: Some(recommendation.rollout_strategy),
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
        delivery_stage: Some(recommendation.delivery_stage),
        validated_stages: Vec::new(),
        rollout_strategy: Some(recommendation.rollout_strategy),
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
        "finding_id": recommendation.finding_id,
        "finding_key": recommendation.finding_key,
        "dedupe_key": recommendation.dedupe_key,
        "title": recommendation.title,
        "summary": recommendation.summary,
        "target_service": recommendation.target_service,
        "delivery_stage": recommendation.delivery_stage.as_str(),
        "rollout_strategy": recommendation.rollout_strategy.as_str(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        findings::detect_findings,
        models::{DeliveryStage, ServiceHealth, ServiceSnapshot, ServiceTrendSummary},
    };

    #[test]
    fn planner_flags_degraded_services() {
        let service = ServiceSnapshot {
            service_key: "gail".to_string(),
            display_name: "Gail".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "continuum_tenant_gail".to_string(),
            playbooks: vec!["continuum_tenant_gail_site.yml".to_string()],
            host_targets: vec!["rk1".to_string()],
            hosts: vec!["rk1".to_string()],
            namespace: Some("gail".to_string()),
            service_name: Some("gail".to_string()),
            deployment_environment: Some(DeliveryStage::Production),
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

        let detected = detect_findings(&[service], &[], &[], None, &[]);
        let recommendations = derive_recommendations(&detected, 0);
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
            host_targets: vec!["rk1".to_string()],
            hosts: vec!["rk1".to_string()],
            namespace: Some("gail".to_string()),
            service_name: Some("gail".to_string()),
            deployment_environment: Some(DeliveryStage::Production),
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

        let detected = detect_findings(&[service], &[], &[], None, &[]);
        let recommendations = derive_recommendations(&detected, 0);
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
            host_targets: vec!["qc01".to_string()],
            hosts: vec!["qc01".to_string()],
            namespace: None,
            service_name: None,
            deployment_environment: Some(DeliveryStage::Production),
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

        let detected = detect_findings(&[service], &[], &[trend], None, &[]);
        let recommendations = derive_recommendations(&detected, 0);
        assert!(
            recommendations
                .iter()
                .any(|item| item.dedupe_key == "tracey:worsening_trend")
        );
    }
}
