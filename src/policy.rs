use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::{
    config::ConductorConfig,
    models::{
        DeliveryStage, PolicyEvaluation, PolicySummary, PolicyVerdict, RolloutStrategy,
        ServiceSnapshot, WorkItem, now_utc,
    },
};

pub fn evaluate_work_item(
    config: &ConductorConfig,
    work_item: &WorkItem,
    service: Option<&ServiceSnapshot>,
) -> PolicyEvaluation {
    let required_previous_stage = required_previous_stage(
        work_item.delivery_stage,
        config.delivery.require_uat_before_production,
    );

    if !config.policy.enabled {
        return PolicyEvaluation {
            verdict: PolicyVerdict::Allowed,
            risk_level: "low".to_string(),
            delivery_stage: work_item.delivery_stage,
            validated_stages: work_item.validated_stages.clone(),
            required_previous_stage,
            rollout_strategy: work_item.rollout_strategy,
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

    if let Some(previous_stage) = required_previous_stage {
        if !work_item.stage_is_validated(previous_stage)
            && !work_item.stage_is_validated(work_item.delivery_stage)
        {
            reasons.push(format!(
                "{} promotion requires {} to be validated first",
                work_item.delivery_stage.as_str(),
                previous_stage.as_str()
            ));
        }
    }

    if matches!(work_item.delivery_stage, DeliveryStage::Production)
        && matches!(work_item.rollout_strategy, RolloutStrategy::Direct)
    {
        reasons
            .push("production stage requires a canary or red_green rollout strategy".to_string());
    }

    let required_verifications = required_verifications(
        service,
        work_item.delivery_stage,
        work_item.rollout_strategy,
    );
    if config.policy.require_verification && !work_item.verification_required {
        reasons.push("verification gate is required for execution".to_string());
    }
    let stage_requires_approval = config.policy.require_admin_approval
        && work_item.delivery_stage.is_release_gate()
        && !work_item.execution_approved;

    let verdict = if reasons.iter().any(|reason| {
        reason.contains("blocked action keyword")
            || reason.contains("requires a canary or red_green rollout strategy")
            || reason.contains("requires") && reason.contains("to be validated first")
    }) {
        PolicyVerdict::Blocked
    } else if !config.policy.allow_external_repo_execution && !external_repos.is_empty() {
        reasons.push("external repository execution is disabled by policy".to_string());
        PolicyVerdict::Blocked
    } else if config.policy.require_verification && !work_item.verification_required {
        PolicyVerdict::Blocked
    } else if stage_requires_approval {
        reasons.push(format!(
            "{} stage requires explicit admin approval before execution",
            work_item.delivery_stage.as_str()
        ));
        PolicyVerdict::NeedsApproval
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
    } else if matches!(work_item.delivery_stage, DeliveryStage::Production) {
        "critical"
    } else if matches!(work_item.delivery_stage, DeliveryStage::Uat) {
        "high"
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
        delivery_stage: work_item.delivery_stage,
        validated_stages: work_item.validated_stages.clone(),
        required_previous_stage,
        rollout_strategy: work_item.rollout_strategy,
        protected_targets,
        external_repos,
        required_verifications,
        reasons,
        generated_at: now_utc(),
    }
}

fn required_previous_stage(
    stage: DeliveryStage,
    require_uat_before_production: bool,
) -> Option<DeliveryStage> {
    if matches!(stage, DeliveryStage::Production) && !require_uat_before_production {
        return Some(DeliveryStage::IntegrationTesting);
    }
    stage.previous()
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
        require_successful_github_actions_for_production: config
            .policy
            .require_successful_github_actions_for_production,
        github_actions_workflow_file: config.policy.github_actions_workflow_file.clone(),
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

fn required_verifications(
    service: Option<&ServiceSnapshot>,
    delivery_stage: DeliveryStage,
    rollout_strategy: RolloutStrategy,
) -> Vec<String> {
    let mut commands = project_native_verification_commands(service);
    if commands.is_empty() {
        commands.push("project-native verification commands".to_string());
    }
    match delivery_stage {
        DeliveryStage::Development => {}
        DeliveryStage::Testing => {
            commands.push("unit and component tests".to_string());
        }
        DeliveryStage::Integration => {
            commands.push("cross-service integration checks".to_string());
        }
        DeliveryStage::IntegrationTesting => {
            commands.push("integration-test suite".to_string());
            commands.push("regression verification".to_string());
        }
        DeliveryStage::Uat => {
            commands.push("user acceptance verification".to_string());
            commands.push("release candidate sign-off".to_string());
        }
        DeliveryStage::Production => {
            commands.push(format!(
                "{} rollout verification",
                rollout_strategy.as_str()
            ));
            commands.push("rollback readiness check".to_string());
            commands.push("production smoke and health verification".to_string());
        }
    }
    commands
}

pub(crate) fn project_native_verification_commands(
    service: Option<&ServiceSnapshot>,
) -> Vec<String> {
    let Some(service) = service else {
        return Vec::new();
    };
    let Some(repo_path) = service.repo_path.as_deref() else {
        return Vec::new();
    };
    let repo = Path::new(repo_path);
    if let Some(ansible_root) = ansible_root(repo) {
        let inventory = ansible_root.join("inventory").join("hosts.ini");
        let primary_playbook = preferred_ansible_playbook(&ansible_root, service);
        let ansible_config = relative_command_path(repo, &ansible_root.join("ansible.cfg"));
        let mut commands = Vec::new();
        if let Some(playbook) = primary_playbook {
            commands.push(format!(
                "env ANSIBLE_CONFIG={} ansible-playbook --syntax-check {}",
                ansible_config,
                relative_command_path(repo, &playbook)
            ));
        }
        if inventory.exists() {
            commands.push(format!(
                "env ANSIBLE_CONFIG={} ansible-inventory -i {} --list",
                ansible_config,
                relative_command_path(repo, &inventory)
            ));
        }
        return commands;
    }
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
    Vec::new()
}

fn ansible_root(repo: &Path) -> Option<std::path::PathBuf> {
    if repo.join("ansible.cfg").exists() && repo.join("inventory").exists() {
        return Some(repo.to_path_buf());
    }
    let nested = repo.join("ansible");
    if nested.join("ansible.cfg").exists() && nested.join("inventory").exists() {
        return Some(nested);
    }
    None
}

fn preferred_ansible_playbook(
    ansible_root: &Path,
    service: &ServiceSnapshot,
) -> Option<std::path::PathBuf> {
    for preferred in ["playbook.yml", "site.yml"] {
        let candidate = ansible_root.join(preferred);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    service
        .playbooks
        .iter()
        .map(|playbook| ansible_root.join(playbook))
        .find(|playbook| playbook.exists())
}

fn relative_command_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

pub fn policy_evaluation_to_value(evaluation: &PolicyEvaluation) -> Value {
    serde_json::to_value(evaluation).unwrap_or_else(|_| serde_json::json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ConductorConfig,
        models::{DeliveryStage, NewWorkItem, RolloutStrategy, ServiceHealth, WorkItem},
    };
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn protected_target_requires_approval() {
        let config = ConductorConfig::default();
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: None,
            title: "Improve Gail".to_string(),
            summary: "Tighten Gail execution path".to_string(),
            target_service: Some("gail".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: None,
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: false,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "improve"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        let service = crate::models::ServiceSnapshot {
            service_key: "gail".to_string(),
            display_name: "Gail".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "continuum_tenant_gail".to_string(),
            playbooks: vec![],
            host_targets: vec![],
            hosts: vec![],
            namespace: None,
            service_name: None,
            deployment_environment: Some(DeliveryStage::Production),
            internal_url: None,
            public_url: None,
            repo_path: Some(std::env::temp_dir().join("test_nm_gail").display().to_string()),
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
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: None,
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "rm -rf"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });

        let evaluation = evaluate_work_item(&config, &item, None);
        assert_eq!(evaluation.verdict, PolicyVerdict::Blocked);
    }

    #[test]
    fn production_stage_requires_release_rollout_strategy() {
        let config = ConductorConfig::default();
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: None,
            title: "Promote".to_string(),
            summary: "Promote to production".to_string(),
            target_service: Some("gail".to_string()),
            delivery_stage: Some(DeliveryStage::Production),
            validated_stages: vec![
                DeliveryStage::Development,
                DeliveryStage::Testing,
                DeliveryStage::Integration,
                DeliveryStage::IntegrationTesting,
                DeliveryStage::Uat,
            ],
            rollout_strategy: Some(RolloutStrategy::Direct),
            status: Some(crate::models::WorkStatus::Scheduled),
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"action": "promote"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });

        let evaluation = evaluate_work_item(&config, &item, None);
        assert_eq!(evaluation.verdict, PolicyVerdict::Blocked);
    }

    #[test]
    fn project_native_verification_commands_detect_ansible_repo() {
        let temp = tempdir().expect("tempdir");
        std::fs::create_dir_all(temp.path().join("inventory")).expect("inventory");
        std::fs::create_dir_all(temp.path().join("roles")).expect("roles");
        std::fs::write(temp.path().join("ansible.cfg"), "[defaults]\n").expect("cfg");
        std::fs::write(
            temp.path().join("inventory").join("hosts.ini"),
            "[all]\nlocalhost ansible_connection=local\n",
        )
        .expect("hosts");
        std::fs::write(temp.path().join("playbook.yml"), "---\n- hosts: all\n").expect("playbook");

        let service = crate::models::ServiceSnapshot {
            service_key: "swarmhpc".to_string(),
            display_name: "SwarmHPC".to_string(),
            kind: "deployment_automation".to_string(),
            role_name: "ansible_control_plane".to_string(),
            playbooks: vec!["playbook.yml".to_string()],
            host_targets: vec!["all".to_string()],
            hosts: vec!["localhost".to_string()],
            namespace: None,
            service_name: Some("swarmhpc-ansible".to_string()),
            deployment_environment: Some(DeliveryStage::Production),
            internal_url: None,
            public_url: None,
            repo_path: Some(temp.path().display().to_string()),
            repo_url: None,
            repo_branch: None,
            health: ServiceHealth::Healthy,
            capabilities: vec!["ansible".to_string()],
            dependencies: vec![],
            storage_paths: vec![],
            raw_defaults: json!({}),
            probe: json!({}),
            discovered_at: now_utc(),
            updated_at: now_utc(),
        };

        let commands = project_native_verification_commands(Some(&service));
        assert!(
            commands
                .iter()
                .any(|command| command.contains("ansible-playbook --syntax-check playbook.yml"))
        );
        assert!(
            commands
                .iter()
                .any(|command| command.contains("ANSIBLE_CONFIG=ansible.cfg"))
        );
        assert!(
            commands
                .iter()
                .any(|command| command.contains("ansible-inventory -i inventory/hosts.ini --list"))
        );
    }
}
