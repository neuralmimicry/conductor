use std::collections::BTreeSet;

use anyhow::Result;
use serde_json::{Map, Value, json};

use crate::{
    findings::{DetectedFinding, detect_findings},
    improvement_catalog::{ImprovementGap, KNOWN_GAPS},
    integrations::gail_plan_summary,
    models::{
        DeliveryStage, FindingSeverity, ImprovementCycle, NewWorkItem, RepositorySnapshot,
        RolloutStrategy, RunStatus, ServiceSnapshot, ServiceTrendSummary, WorkItem, WorkStatus,
        now_utc,
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

    // Formal gaps are the durable programme backlog and must remain visible
    // even when operators temporarily disable dynamic finding auto-queueing.
    for gap in KNOWN_GAPS {
        upsert_catalogue_gap(repository, gap).await?;
    }
    if config.planning.auto_queue {
        for recommendation in &recommendations {
            upsert_recommendation(repository, recommendation).await?;
        }
    }

    let topology_summary = build_planner_context(
        &services,
        &repositories,
        &detected_findings,
        recommendations.len(),
        &trends,
        config,
    );

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
                "Reconciled {} known platform gaps; {} evidence-backed findings remain visible for review.",
                KNOWN_GAPS.len(),
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
                "Reconciled {} known platform gaps and queued {} finding-driven improvement items from {} findings across {} services.",
                KNOWN_GAPS.len(),
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

/// Build a bounded, evidence-first view for Gail. The complete snapshots stay
/// in Postgres for the dashboard and audit trail; a small local model receives
/// only the records it can usefully reason over. Findings retain their
/// recommendation and evidence summaries so Gail does not have to infer the
/// actual action from opaque IDs alone.
fn build_planner_context(
    services: &[ServiceSnapshot],
    repositories: &[RepositorySnapshot],
    detected_findings: &[DetectedFinding],
    recommendation_count: usize,
    trends: &[ServiceTrendSummary],
    config: &crate::config::ConductorConfig,
) -> Value {
    let mut relevant_repositories = repositories
        .iter()
        .filter(|repository| {
            !repository.linked_services.is_empty()
                || matches!(repository.criticality.as_str(), "critical" | "high")
                || matches!(
                    repository.repo_key.as_str(),
                    "conductor" | "swarmhpc" | "gail"
                )
        })
        .collect::<Vec<_>>();
    relevant_repositories.sort_by(|left, right| {
        repository_rank(right)
            .cmp(&repository_rank(left))
            .then_with(|| left.repo_key.cmp(&right.repo_key))
    });

    let mut ordered_findings = detected_findings.iter().collect::<Vec<_>>();
    ordered_findings.sort_by(|left, right| {
        severity_rank(right.finding.severity)
            .cmp(&severity_rank(left.finding.severity))
            .then_with(|| {
                right
                    .finding
                    .confidence_score
                    .partial_cmp(&left.finding.confidence_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.finding.finding_key.cmp(&right.finding.finding_key))
    });

    let context = json!({
        "services": services.iter().map(|service| json!({
            "service_key": service.service_key,
            "health": service.health.as_str(),
            "dependencies": service.dependencies,
            "capabilities": service.capabilities,
            // Probes can contain Prometheus payloads or logs. Preserve a
            // shape/sample only; the typed finding evidence is authoritative.
            "probe": compact_prompt_value(&service.probe, 0),
        })).collect::<Vec<_>>(),
        "repositories": relevant_repositories
            .into_iter()
            .take(config.planning.max_repositories)
            .map(|repository| json!({
                "repo_key": repository.repo_key,
                "linked_services": repository.linked_services,
                "criticality": repository.criticality,
                "capabilities": repository.capabilities,
                "archived": repository.archived,
            }))
            .collect::<Vec<_>>(),
        "findings": ordered_findings
            .into_iter()
            .take(config.planning.max_findings)
            .map(|item| json!({
                "finding_key": item.finding.finding_key,
                "title": planner_text(&item.finding.title),
                "summary": planner_text(&item.finding.summary),
                "category": item.finding.category,
                "severity": item.finding.severity.as_str(),
                "target_service": item.finding.target_service,
                "target_repository": item.finding.target_repository,
                "confidence_score": item.finding.confidence_score,
                "recommendation": {
                    "title": planner_text(&item.recommendation.title),
                    "summary": planner_text(&item.recommendation.summary),
                    "priority": item.recommendation.priority,
                    "depends_on": item.recommendation.depends_on,
                },
                "evidence": item.evidence.iter().take(3).map(|evidence| json!({
                    "type": evidence.evidence_type,
                    "source": evidence.source_ref,
                    "summary": planner_text(&evidence.summary),
                })).collect::<Vec<_>>(),
            }))
            .collect::<Vec<_>>(),
        "trends": trends.iter().take(config.planning.max_trends).map(|trend| json!({
            "service_key": trend.service_key,
            "direction": trend.direction,
            "sample_count": trend.sample_count,
            "headline": planner_text(&trend.headline),
            "metrics": trend
                .metrics
                .iter()
                .take(8)
                .map(|metric| json!({
                    "name": metric.metric_name,
                    "latest": metric.latest,
                    "average": metric.average,
                    "slope": metric.slope,
                    "direction": metric.direction,
                }))
                .collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "finding_count": detected_findings.len(),
        "recommendation_count": recommendation_count,
        "catalogue_gap_count": KNOWN_GAPS.len(),
    });

    fit_planner_context(context, config.planning.max_prompt_chars)
}

fn planner_text(text: &str) -> String {
    const MAX_CHARS: usize = 320;
    let truncated = text.chars().take(MAX_CHARS).collect::<String>();
    if text.chars().count() > MAX_CHARS {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn repository_rank(repository: &RepositorySnapshot) -> u8 {
    if repository.criticality == "critical" {
        3
    } else if repository.criticality == "high" {
        2
    } else if !repository.linked_services.is_empty() {
        1
    } else {
        0
    }
}

fn severity_rank(severity: FindingSeverity) -> u8 {
    match severity {
        FindingSeverity::Critical => 5,
        FindingSeverity::High => 4,
        FindingSeverity::Medium => 3,
        FindingSeverity::Low => 2,
        FindingSeverity::Info => 1,
    }
}

/// Reduce optional breadth until the serialized JSON fits the configured
/// budget. This is deliberately deterministic: the most important findings
/// survive, while low-value repository/trend breadth is removed first.
fn fit_planner_context(mut context: Value, max_chars: usize) -> Value {
    let serialized_len = |value: &Value| {
        serde_json::to_string(value)
            .map(|text| text.len())
            .unwrap_or(usize::MAX)
    };
    if serialized_len(&context) <= max_chars {
        return context;
    }

    if let Some(services) = context.get_mut("services").and_then(Value::as_array_mut) {
        for service in services {
            if let Some(object) = service.as_object_mut() {
                object.remove("probe");
            }
        }
    }

    for key in ["repositories", "trends", "services", "findings"] {
        loop {
            if serialized_len(&context) <= max_chars {
                return context;
            }
            let removed = context
                .get_mut(key)
                .and_then(Value::as_array_mut)
                .and_then(|items| if items.len() > 1 { items.pop() } else { None });
            if removed.is_none() {
                break;
            }
        }
    }

    // A very small configured budget should still produce valid, explainable
    // JSON rather than slicing a JSON string in the middle of a token.
    json!({
        "services": context.get("services").cloned().unwrap_or_else(|| json!([])),
        "findings": context.get("findings").cloned().unwrap_or_else(|| json!([])),
        "finding_count": context.get("finding_count").cloned().unwrap_or_else(|| json!(0)),
        "catalogue_gap_count": context.get("catalogue_gap_count").cloned().unwrap_or_else(|| json!(0)),
    })
}

/// Keep planner context bounded even when a service probe contains an
/// accidentally unbounded response body. Findings and trend summaries carry
/// the evidence needed for planning; this view only preserves the shape and a
/// small sample of the probe for context.
fn compact_prompt_value(value: &Value, depth: usize) -> Value {
    const MAX_OBJECT_ENTRIES: usize = 24;
    const MAX_ARRAY_ITEMS: usize = 6;
    const MAX_STRING_CHARS: usize = 320;

    match value {
        Value::Object(object) if depth < 2 => {
            let mut compact = Map::new();
            for (key, child) in object.iter().take(MAX_OBJECT_ENTRIES) {
                compact.insert(key.clone(), compact_prompt_value(child, depth + 1));
            }
            if object.len() > MAX_OBJECT_ENTRIES {
                compact.insert(
                    "_omitted_keys".to_string(),
                    json!(object.len() - MAX_OBJECT_ENTRIES),
                );
            }
            Value::Object(compact)
        }
        Value::Object(object) => json!({
            "_kind": "object",
            "_keys": object.len(),
        }),
        Value::Array(items) if depth < 2 => {
            let compact = items
                .iter()
                .take(MAX_ARRAY_ITEMS)
                .map(|child| compact_prompt_value(child, depth + 1))
                .collect::<Vec<_>>();
            if items.len() > MAX_ARRAY_ITEMS {
                json!({
                    "_kind": "array",
                    "_length": items.len(),
                    "_sample": compact,
                })
            } else {
                Value::Array(compact)
            }
        }
        Value::Array(items) => json!({
            "_kind": "array",
            "_length": items.len(),
        }),
        Value::String(text) => {
            let truncated = text.chars().take(MAX_STRING_CHARS).collect::<String>();
            if text.chars().count() > MAX_STRING_CHARS {
                json!(format!("{truncated}…"))
            } else {
                Value::String(truncated)
            }
        }
        other => other.clone(),
    }
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

fn catalogue_gap_plan(gap: &ImprovementGap) -> Value {
    json!({
        "kind": "formal_gap",
        "gap_id": gap.id,
        "repositories": gap.repositories,
        "outcomes": gap.outcomes,
        "validation": gap.validation,
    })
}

async fn upsert_catalogue_gap(
    repository: &dyn ConductorRepository,
    gap: &ImprovementGap,
) -> Result<()> {
    let dedupe_key = format!("formal-gap:{}", gap.id);
    let tags = gap
        .tags
        .iter()
        .map(|tag| (*tag).to_string())
        .collect::<Vec<_>>();
    let dependencies = gap
        .depends_on
        .iter()
        .map(|key| format!("formal-gap:{key}"))
        .collect::<Vec<_>>();
    let plan = catalogue_gap_plan(gap);

    if let Some(existing) = repository.find_work_item_by_dedupe_key(&dedupe_key).await? {
        if existing.admin_override {
            return Ok(());
        }
        let changed = existing.title != gap.title
            || existing.summary != gap.summary
            || existing.target_service.as_deref() != Some(gap.target_service)
            || existing.priority != gap.priority
            || existing.tags != tags
            || existing.plan != plan
            || existing.depends_on != dependencies;
        if !changed {
            return Ok(());
        }
        let mut updated = existing;
        updated.title = gap.title.to_string();
        updated.summary = gap.summary.to_string();
        updated.target_service = Some(gap.target_service.to_string());
        updated.priority = gap.priority;
        updated.tags = tags;
        updated.plan = plan;
        updated.depends_on = dependencies;
        updated.updated_at = now_utc();
        updated.notes.push(format!(
            "{} reconciled formal gap catalogue entry {}",
            now_utc().to_rfc3339(),
            gap.id
        ));
        repository.upsert_work_item(&updated).await?;
        return Ok(());
    }

    let item = WorkItem::from_new(NewWorkItem {
        dedupe_key: Some(dedupe_key),
        title: gap.title.to_string(),
        summary: gap.summary.to_string(),
        target_service: Some(gap.target_service.to_string()),
        delivery_stage: Some(DeliveryStage::Development),
        validated_stages: Vec::new(),
        rollout_strategy: Some(RolloutStrategy::default_for_stage(
            DeliveryStage::Development,
        )),
        status: Some(WorkStatus::Planned),
        priority: Some(gap.priority),
        progress_pct: Some(0),
        admin_override: false,
        execution_approved: false,
        verification_required: Some(true),
        tags,
        plan,
        depends_on: dependencies,
        source: Some("formal_gap_catalogue".to_string()),
        scheduled_for: None,
    });
    repository.upsert_work_item(&item).await
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
        let material_change = existing.title != recommendation.title
            || existing.summary != recommendation.summary
            || existing.target_service != recommendation.target_service
            || existing.delivery_stage != recommendation.delivery_stage
            || existing.rollout_strategy != recommendation.rollout_strategy
            || existing.priority != recommendation.priority
            || existing.tags != recommendation.tags
            || existing.plan != recommendation.plan
            || existing.depends_on != recommendation.depends_on;

        let mut updated = existing.clone();
        updated.title = recommendation.title.clone();
        updated.summary = recommendation.summary.clone();
        updated.target_service = recommendation.target_service.clone();
        updated.delivery_stage = recommendation.delivery_stage;
        updated.rollout_strategy = recommendation.rollout_strategy;
        updated.priority = recommendation.priority;
        updated.tags = recommendation.tags.clone();
        updated.plan = recommendation.plan.clone();
        updated.depends_on = recommendation.depends_on.clone();
        updated.updated_at = now_utc();
        updated.notes.push(format!(
            "{} {}",
            now_utc().to_rfc3339(),
            if material_change {
                "planner refreshed recommendation"
            } else {
                "planner confirmed recommendation"
            }
        ));
        if material_change && (updated.execution_approved || updated.approval_metadata != json!({}))
        {
            updated.execution_approved = false;
            updated.approval_metadata = json!({});
            if matches!(
                updated.status,
                WorkStatus::Planned | WorkStatus::Scheduled | WorkStatus::OnHold
            ) {
                updated.status = WorkStatus::Planned;
            }
            updated.notes.push(format!(
                "{} planner reset approval because the recommended change changed",
                now_utc().to_rfc3339()
            ));
        }
        repository.upsert_work_item(&updated).await?;
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
        models::{
            DeliveryStage, NewWorkItem, RolloutStrategy, ServiceHealth, ServiceSnapshot,
            ServiceTrendSummary, WorkStatus,
        },
        storage::memory::MemoryRepository,
    };
    use std::sync::Arc;

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

    #[tokio::test]
    async fn formal_gap_catalogue_is_persisted_once_and_reconciled() {
        let repository = crate::storage::memory::MemoryRepository::new();
        for gap in KNOWN_GAPS {
            upsert_catalogue_gap(&repository, gap)
                .await
                .expect("initial gap upsert");
        }
        for gap in KNOWN_GAPS {
            upsert_catalogue_gap(&repository, gap)
                .await
                .expect("repeat gap reconciliation");
        }
        let items = repository.list_work_items().await.expect("work items");
        assert_eq!(items.len(), KNOWN_GAPS.len());
        assert!(
            items
                .iter()
                .all(|item| item.source == "formal_gap_catalogue")
        );
        assert!(items.iter().all(|item| {
            item.dedupe_key
                .as_deref()
                .is_some_and(|key| key.starts_with("formal-gap:"))
        }));
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

    #[test]
    fn planner_context_budget_is_valid_json_and_preserves_findings() {
        let context = json!({
            "services": (0..20)
                .map(|index| json!({
                    "service_key": format!("service-{index}"),
                    "health": "healthy",
                    "probe": {"logs": "x".repeat(1000)},
                }))
                .collect::<Vec<_>>(),
            "repositories": (0..20)
                .map(|index| json!({
                    "repo_key": format!("repo-{index}"),
                    "criticality": "medium",
                }))
                .collect::<Vec<_>>(),
            "findings": [{
                "finding_key": "service_health:gail",
                "severity": "critical",
                "summary": "Gail is unavailable",
            }],
            "trends": (0..20)
                .map(|index| json!({"service_key": format!("service-{index}"), "headline": "trend"}))
                .collect::<Vec<_>>(),
            "finding_count": 1,
            "catalogue_gap_count": 10,
        });

        let bounded = fit_planner_context(context, 4_096);
        let serialized = serde_json::to_string(&bounded).expect("valid planner JSON");
        assert!(serialized.len() <= 4_096);
        assert_eq!(bounded["findings"][0]["finding_key"], "service_health:gail");
    }

    #[tokio::test]
    async fn planner_resets_execution_approval_when_recommendation_changes() {
        let repository = Arc::new(MemoryRepository::new());
        let mut item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("gail:trading".to_string()),
            title: "Improve Gail trading".to_string(),
            summary: "Tighten the trading loop".to_string(),
            target_service: Some("gail".to_string()),
            delivery_stage: Some(DeliveryStage::Development),
            validated_stages: Vec::new(),
            rollout_strategy: Some(RolloutStrategy::Direct),
            status: Some(WorkStatus::Scheduled),
            priority: Some(90),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec!["gail".to_string(), "trading".to_string()],
            plan: json!({"action": "improve_trading", "scope": "signals"}),
            depends_on: Vec::new(),
            source: Some("planner".to_string()),
            scheduled_for: None,
        });
        item.approval_metadata = json!({"verdict": "approved"});
        repository.upsert_work_item(&item).await.expect("work item");

        let recommendation = ImprovementRecommendation {
            finding_id: uuid::Uuid::new_v4(),
            finding_key: "gail_trading".to_string(),
            dedupe_key: "gail:trading".to_string(),
            title: "Improve Gail trading".to_string(),
            summary: "Tighten the trading loop".to_string(),
            target_service: Some("gail".to_string()),
            delivery_stage: DeliveryStage::Development,
            rollout_strategy: RolloutStrategy::Direct,
            priority: 90,
            tags: vec!["gail".to_string(), "trading".to_string()],
            plan: json!({"action": "improve_trading", "scope": "risk_controls"}),
            depends_on: Vec::new(),
        };

        upsert_recommendation(repository.as_ref(), &recommendation)
            .await
            .expect("upsert");

        let updated = repository
            .find_work_item_by_dedupe_key("gail:trading")
            .await
            .expect("lookup")
            .expect("updated item");
        assert!(!updated.execution_approved);
        assert_eq!(updated.approval_metadata, json!({}));
        assert_eq!(updated.status, WorkStatus::Planned);
    }
}
