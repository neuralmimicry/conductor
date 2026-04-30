use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    config::ConductorConfig,
    integrations::post_json,
    models::{PolicyEvaluation, ServiceSnapshot, WorkItem, now_utc, unique_strings},
    policy::policy_evaluation_to_value,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AiApprovalDecision {
    pub approved: bool,
    pub confidence: f64,
    pub risk_level: String,
    pub reason: String,
    #[serde(default)]
    pub required_actions: Vec<String>,
    #[serde(default)]
    pub schedule_now: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

pub async fn request_ai_approval(
    client: &Client,
    config: &ConductorConfig,
    item: &WorkItem,
    service: Option<&ServiceSnapshot>,
    policy: &PolicyEvaluation,
    dependency_blockers: &[String],
    discovered_gail_base_url: Option<&str>,
) -> Result<Option<AiApprovalDecision>> {
    if !config.policy.ai_approvals_enabled || !config.integrations.gail.enabled {
        return Ok(None);
    }
    let Some(base_url) = config
        .integrations
        .gail
        .base_url
        .clone()
        .or_else(|| discovered_gail_base_url.map(ToString::to_string))
    else {
        return Ok(None);
    };

    let prompt_context = json!({
        "work_item": {
            "id": item.id,
            "dedupe_key": item.dedupe_key,
            "title": item.title,
            "summary": item.summary,
            "target_service": item.target_service,
            "delivery_stage": item.delivery_stage.as_str(),
            "validated_stages": item.validated_stages.iter().map(|stage| stage.as_str()).collect::<Vec<_>>(),
            "rollout_strategy": item.rollout_strategy.as_str(),
            "status": item.status.as_str(),
            "priority": item.priority,
            "verification_required": item.verification_required,
            "tags": item.tags,
            "plan": item.plan,
            "depends_on": item.depends_on,
        },
        "target_service": service.map(|service| json!({
            "service_key": service.service_key,
            "display_name": service.display_name,
            "health": service.health.as_str(),
            "capabilities": service.capabilities,
            "dependencies": service.dependencies,
            "repo_path": service.repo_path,
            "repo_url": service.repo_url,
            "repo_branch": service.repo_branch,
        })),
        "policy": policy_evaluation_to_value(policy),
        "dependency_blockers": dependency_blockers,
        "approval_constraints": {
            "minimum_confidence": config.policy.ai_approval_min_confidence,
            "blocked_keywords": config.policy.blocked_action_keywords,
            "protected_services": config.policy.protected_services,
            "require_verification": config.policy.require_verification,
            "require_refiner_strict_mode": config.policy.require_refiner_strict_mode,
        },
    });

    let completion = post_json(
        client,
        &base_url,
        "/v1/llm/complete",
        config.integrations.gail.bearer_token.as_deref(),
        &json!({
            "workflow": config.policy.ai_approval_workflow,
            "role": "reviewer",
            "include_configured": true,
            "selection_mode": "best",
            "max_candidates": 5,
            "timeout_seconds": 45,
            "reasoning_effort": "high",
            "request_category": "approval_review",
            "messages": [
                {
                    "role": "system",
                    "content": "You are the NeuralMimicry Conductor approval reviewer. Approve only narrowly scoped, non-destructive changes with clear repository context, adequate verification, satisfied rollout governance, and no unresolved blockers. Deny ambiguous or unsafe work. Return strict JSON only with keys approved, confidence, risk_level, reason, required_actions, schedule_now."
                },
                {
                    "role": "user",
                    "content": format!(
                        "Review this work item for safe automated execution approval. Confidence must be between 0.0 and 1.0.\n\nContext JSON:\n{}",
                        prompt_context
                    )
                }
            ]
        }),
    )
    .await?;

    let text = completion
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Gail approval response did not include completion text"))?;
    let decision_value = extract_json_value(text)?;
    let mut decision = serde_json::from_value::<AiApprovalDecision>(decision_value)
        .context("failed to parse Gail approval decision JSON")?;

    decision.confidence = decision.confidence.clamp(0.0, 1.0);
    decision.reason = normalize_reason(decision.approved, decision.reason);
    decision.required_actions = unique_strings(decision.required_actions);
    decision.provider = completion
        .get("provider")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    decision.model = completion
        .get("model")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    decision.request_id = completion
        .get("request_id")
        .and_then(Value::as_str)
        .map(ToString::to_string);

    Ok(Some(decision))
}

pub fn work_item_approval_fingerprint(item: &WorkItem) -> String {
    let mut hasher = DefaultHasher::new();
    item.title.hash(&mut hasher);
    item.summary.hash(&mut hasher);
    item.target_service.hash(&mut hasher);
    item.delivery_stage.as_str().hash(&mut hasher);
    item.rollout_strategy.as_str().hash(&mut hasher);
    item.priority.hash(&mut hasher);
    item.verification_required.hash(&mut hasher);
    serde_json::to_string(&item.tags)
        .unwrap_or_default()
        .hash(&mut hasher);
    serde_json::to_string(&item.plan)
        .unwrap_or_default()
        .hash(&mut hasher);
    serde_json::to_string(&item.depends_on)
        .unwrap_or_default()
        .hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub fn build_approval_metadata(
    item: &WorkItem,
    policy: &PolicyEvaluation,
    decision: &AiApprovalDecision,
    effective_approval: bool,
) -> Value {
    json!({
        "reviewed_at": now_utc().to_rfc3339(),
        "reviewed_by": "gail",
        "workflow": "ai_approval",
        "verdict": if effective_approval { "approved" } else { "denied" },
        "schedule_now": decision.schedule_now,
        "confidence": decision.confidence,
        "risk_level": decision.risk_level,
        "reason": decision.reason,
        "required_actions": decision.required_actions,
        "provider": decision.provider,
        "model": decision.model,
        "request_id": decision.request_id,
        "fingerprint": work_item_approval_fingerprint(item),
        "policy": policy_evaluation_to_value(policy),
    })
}

pub fn metadata_verdict(metadata: &Value) -> Option<&str> {
    metadata.get("verdict").and_then(Value::as_str)
}

pub fn metadata_schedule_now(metadata: &Value) -> bool {
    if metadata_verdict(metadata) != Some("approved") {
        return true;
    }
    metadata
        .get("schedule_now")
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

pub fn metadata_matches_item(metadata: &Value, item: &WorkItem) -> bool {
    metadata
        .get("fingerprint")
        .and_then(Value::as_str)
        .is_some_and(|fingerprint| fingerprint == work_item_approval_fingerprint(item))
}

fn normalize_reason(approved: bool, reason: String) -> String {
    let trimmed = reason.trim();
    if !trimmed.is_empty() {
        return trimmed.to_string();
    }
    if approved {
        "approved by Gail reviewer".to_string()
    } else {
        "denied by Gail reviewer".to_string()
    }
}

fn extract_json_value(text: &str) -> Result<Value> {
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Ok(value);
    }

    let stripped = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim);
    if let Some(stripped) = stripped {
        if let Ok(value) = serde_json::from_str::<Value>(stripped) {
            return Ok(value);
        }
    }

    let mut start = None;
    let mut depth = 0usize;
    for (idx, ch) in trimmed.char_indices() {
        if ch == '{' {
            if start.is_none() {
                start = Some(idx);
            }
            depth += 1;
        } else if ch == '}' {
            if depth == 0 {
                continue;
            }
            depth -= 1;
            if depth == 0 {
                if let Some(start_idx) = start {
                    let candidate = &trimmed[start_idx..=idx];
                    if let Ok(value) = serde_json::from_str::<Value>(candidate) {
                        return Ok(value);
                    }
                }
            }
        }
    }

    Err(anyhow!("approval reviewer did not return valid JSON"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{DeliveryStage, NewWorkItem, RolloutStrategy, WorkStatus};

    #[test]
    fn extracts_json_from_code_fence() {
        let value = extract_json_value(
            "```json\n{\"approved\":true,\"confidence\":0.9,\"risk_level\":\"low\",\"reason\":\"ok\",\"required_actions\":[],\"schedule_now\":true}\n```",
        )
        .expect("json");
        assert_eq!(value.get("approved").and_then(Value::as_bool), Some(true));
    }

    #[test]
    fn fingerprint_changes_when_plan_changes() {
        let mut item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("gail:improve".to_string()),
            title: "Improve Gail".to_string(),
            summary: "Tighten the trading loop".to_string(),
            target_service: Some("gail".to_string()),
            delivery_stage: Some(DeliveryStage::Development),
            validated_stages: vec![],
            rollout_strategy: Some(RolloutStrategy::Direct),
            status: Some(WorkStatus::Planned),
            priority: Some(80),
            progress_pct: Some(0),
            admin_override: false,
            execution_approved: false,
            verification_required: Some(true),
            tags: vec!["gail".to_string()],
            plan: json!({"action": "improve_trading"}),
            depends_on: vec![],
            source: Some("planner".to_string()),
            scheduled_for: None,
        });
        let first = work_item_approval_fingerprint(&item);
        item.plan = json!({"action": "improve_trading", "scope": "risk_controls"});
        let second = work_item_approval_fingerprint(&item);
        assert_ne!(first, second);
    }
}
