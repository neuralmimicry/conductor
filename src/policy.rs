use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::{
    config::ConductorConfig,
    models::{PolicyEvaluation, PolicySummary, PolicyVerdict, ServiceSnapshot, WorkItem, now_utc},
};

pub fn evaluate_work_item(
    config: &ConductorConfig,
    work_item: &WorkItem,
    service: Option<&ServiceSnapshot>,
) -> PolicyEvaluation {
    if !config.policy.enabled {
        return PolicyEvaluation {
            verdict: PolicyVerdict::Allowed,
            risk_level: "low".to_string(),
            protected_targets: Vec::new(),
            external_repos: Vec::new(),
            required_verifications: Vec::new(),
            reasons: vec!["policy engine disabled".to_string()],
            generated_at: now_utc(),
        };
    }

    let mut protected_targets = Vec::new();
    let mut external_repos = Vec::new();
    let mut reasons = Vec::new();

    if let Some(service) = service {
        if config
            .policy
            .protected_services
            .iter()
            .any(|candidate| candidate == &service.service_key)
        {
            protected_targets.push(service.service_key.clone());
            reasons.push(format!(
                "{} is marked as a protected service target",
                service.service_key
            ));
        }
        if let Some(repo_path) = service
            .repo_path
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            let repo_path_buf = PathBuf::from(repo_path);
            let conductor_root = &config.discovery.repo_hints.conductor_repo;
            if !path_starts_with(&repo_path_buf, conductor_root) {
                external_repos.push(repo_path.to_string());
                reasons.push(format!(
                    "{} is outside the Conductor repository root",
                    repo_path
                ));
            }
            if config
                .policy
                .protected_repo_roots
                .iter()
                .any(|root| path_starts_with(&repo_path_buf, root))
            {
                if !external_repos.contains(&repo_path.to_string()) {
                    external_repos.push(repo_path.to_string());
                }
                reasons.push(format!(
                    "{} is covered by a protected repo policy",
                    repo_path
                ));
            }
        }
    }

    let action_text = format!(
        "{} {} {}",
        work_item.title, work_item.summary, work_item.plan
    )
    .to_ascii_lowercase();
    if let Some(keyword) = config
        .policy
        .blocked_action_keywords
        .iter()
        .find(|keyword| {
            !keyword.trim().is_empty() && action_text.contains(&keyword.to_ascii_lowercase())
        })
    {
        reasons.push(format!(
            "work item contains blocked action keyword '{}'",
            keyword
        ));
    }

    let required_verifications = required_verifications(service);
    if config.policy.require_verification && !work_item.verification_required {
        reasons.push("verification gate is required for execution".to_string());
    }

    let verdict = if reasons
        .iter()
        .any(|reason| reason.contains("blocked action keyword"))
    {
        PolicyVerdict::Blocked
    } else if !config.policy.allow_external_repo_execution && !external_repos.is_empty() {
        reasons.push("external repository execution is disabled by policy".to_string());
        PolicyVerdict::Blocked
    } else if config.policy.require_verification && !work_item.verification_required {
        PolicyVerdict::Blocked
    } else if config.policy.require_admin_approval
        && (!protected_targets.is_empty() || !external_repos.is_empty())
        && !work_item.execution_approved
    {
        reasons.push("explicit admin approval is required before execution".to_string());
        PolicyVerdict::NeedsApproval
    } else {
        if reasons.is_empty() {
            reasons.push("policy checks passed".to_string());
        }
        PolicyVerdict::Allowed
    };

    let risk_level = if matches!(verdict, PolicyVerdict::Blocked) {
        "critical"
    } else if !protected_targets.is_empty() || !external_repos.is_empty() {
        "high"
    } else if work_item.verification_required {
        "medium"
    } else {
        "low"
    }
    .to_string();

    PolicyEvaluation {
        verdict,
        risk_level,
        protected_targets,
        external_repos,
        required_verifications,
        reasons,
        generated_at: now_utc(),
    }
}

pub fn policy_summary(config: &ConductorConfig) -> PolicySummary {
    PolicySummary {
        protected_services: config.policy.protected_services.clone(),
        protected_repo_roots: config
            .policy
            .protected_repo_roots
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        require_admin_approval: config.policy.require_admin_approval,
        require_verification: config.policy.require_verification,
        require_refiner_strict_mode: config.policy.require_refiner_strict_mode,
        allow_external_repo_execution: config.policy.allow_external_repo_execution,
    }
}

fn path_starts_with(candidate: &Path, root: &Path) -> bool {
    if root.as_os_str().is_empty() {
        return false;
    }
    let candidate = candidate.components().collect::<Vec<_>>();
    let root = root.components().collect::<Vec<_>>();
    candidate.starts_with(&root)
}

fn required_verifications(service: Option<&ServiceSnapshot>) -> Vec<String> {
    let Some(service) = service else {
        return vec!["project-native verification commands".to_string()];
    };
    let Some(repo_path) = service.repo_path.as_deref() else {
        return vec!["project-native verification commands".to_string()];
    };
    let repo = Path::new(repo_path);
    if repo.join("Cargo.toml").exists() {
        return vec![
            "cargo fmt --check".to_string(),
            "cargo check".to_string(),
            "cargo test".to_string(),
        ];
    }
    if repo.join("pyproject.toml").exists() || repo.join("requirements.txt").exists() {
        return vec!["python -m pytest".to_string(), "pytest".to_string()];
    }
    if repo.join("package.json").exists() {
        return vec!["npm test".to_string(), "npm run lint".to_string()];
    }
    vec!["project-native verification commands".to_string()]
}

pub fn policy_evaluation_to_value(evaluation: &PolicyEvaluation) -> Value {
    serde_json::to_value(evaluation).unwrap_or_else(|_| serde_json::json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ConductorConfig,
        models::{NewWorkItem, ServiceHealth, WorkItem},
    };
    use serde_json::json;

    #[test]
    fn protected_target_requires_approval() {
        let config = ConductorConfig::default();
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: None,
            title: "Improve Gail".to_string(),
            summary: "Tighten Gail execution path".to_string(),
            target_service: Some("gail".to_string()),
            status: None,
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: false,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "improve"}),
            source: None,
            scheduled_for: None,
        });
        let service = crate::models::ServiceSnapshot {
            service_key: "gail".to_string(),
            display_name: "Gail".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "continuum_tenant_gail".to_string(),
            playbooks: vec![],
            hosts: vec![],
            namespace: None,
            service_name: None,
            internal_url: None,
            public_url: None,
            repo_path: Some("/home/pbisaacs/Developer/neuralmimicry/gail".to_string()),
            repo_url: None,
            repo_branch: None,
            health: ServiceHealth::Healthy,
            capabilities: vec![],
            dependencies: vec![],
            storage_paths: vec![],
            raw_defaults: json!({}),
            probe: json!({}),
            discovered_at: now_utc(),
            updated_at: now_utc(),
        };

        let evaluation = evaluate_work_item(&config, &item, Some(&service));
        assert_eq!(evaluation.verdict, PolicyVerdict::NeedsApproval);
    }

    #[test]
    fn blocked_keyword_is_rejected() {
        let config = ConductorConfig::default();
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: None,
            title: "Danger".to_string(),
            summary: "Run rm -rf on repo".to_string(),
            target_service: None,
            status: None,
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "rm -rf"}),
            source: None,
            scheduled_for: None,
        });

        let evaluation = evaluate_work_item(&config, &item, None);
        assert_eq!(evaluation.verdict, PolicyVerdict::Blocked);
    }
}
