use std::{io::ErrorKind, path::Path, process::Stdio, time::Instant};

use serde::{Deserialize, Serialize};
use tokio::{
    process::Command,
    time::{Duration, timeout},
};

use crate::{
    config::ValidationConfig, models::ServiceSnapshot, policy::project_native_verification_commands,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationCommandResult {
    pub command: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub stdout_excerpt: Option<String>,
    pub stderr_excerpt: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndependentValidationReport {
    pub enabled: bool,
    pub enforced: bool,
    pub passed: bool,
    pub completeness: String,
    pub repo_path: Option<String>,
    pub required_checks: Vec<String>,
    pub planned_commands: Vec<String>,
    pub commands: Vec<ValidationCommandResult>,
    pub summary: String,
}

pub fn preview_independent_validation(
    config: &ValidationConfig,
    service: Option<&ServiceSnapshot>,
    required_checks: &[String],
) -> IndependentValidationReport {
    let repo_path = repo_path(service);
    let planned_commands = planned_commands(config, service);

    if !config.enabled {
        return IndependentValidationReport {
            enabled: false,
            enforced: config.require_success,
            passed: true,
            completeness: "disabled".to_string(),
            repo_path: repo_path.map(display_path),
            required_checks: required_checks.to_vec(),
            planned_commands,
            commands: Vec::new(),
            summary: "independent validation is disabled".to_string(),
        };
    }

    if repo_path.is_none() {
        return IndependentValidationReport {
            enabled: true,
            enforced: config.require_success,
            passed: true,
            completeness: "skipped".to_string(),
            repo_path: None,
            required_checks: required_checks.to_vec(),
            planned_commands,
            commands: Vec::new(),
            summary: "no local repository path is available for independent validation".to_string(),
        };
    }

    if planned_commands.is_empty() {
        return IndependentValidationReport {
            enabled: true,
            enforced: config.require_success,
            passed: true,
            completeness: "skipped".to_string(),
            repo_path: repo_path.map(display_path),
            required_checks: required_checks.to_vec(),
            planned_commands,
            commands: Vec::new(),
            summary: "no executable project-native validation commands were discovered".to_string(),
        };
    }

    IndependentValidationReport {
        enabled: true,
        enforced: config.require_success,
        passed: true,
        completeness: "planned".to_string(),
        repo_path: repo_path.map(display_path),
        required_checks: required_checks.to_vec(),
        planned_commands: planned_commands.clone(),
        commands: planned_commands
            .into_iter()
            .map(|command| ValidationCommandResult {
                command,
                status: "planned".to_string(),
                exit_code: None,
                duration_ms: 0,
                stdout_excerpt: None,
                stderr_excerpt: None,
                reason: None,
            })
            .collect(),
        summary: "independent validation commands are planned but not executed in dry-run mode"
            .to_string(),
    }
}

pub async fn run_independent_validation(
    config: &ValidationConfig,
    service: Option<&ServiceSnapshot>,
    required_checks: &[String],
) -> IndependentValidationReport {
    let repo_path = repo_path(service);
    let planned_commands = planned_commands(config, service);

    if !config.enabled {
        return IndependentValidationReport {
            enabled: false,
            enforced: config.require_success,
            passed: true,
            completeness: "disabled".to_string(),
            repo_path: repo_path.map(display_path),
            required_checks: required_checks.to_vec(),
            planned_commands,
            commands: Vec::new(),
            summary: "independent validation is disabled".to_string(),
        };
    }

    let Some(repo_path) = repo_path else {
        return IndependentValidationReport {
            enabled: true,
            enforced: config.require_success,
            passed: true,
            completeness: "skipped".to_string(),
            repo_path: None,
            required_checks: required_checks.to_vec(),
            planned_commands,
            commands: Vec::new(),
            summary: "no local repository path is available for independent validation".to_string(),
        };
    };

    if planned_commands.is_empty() {
        return IndependentValidationReport {
            enabled: true,
            enforced: config.require_success,
            passed: true,
            completeness: "skipped".to_string(),
            repo_path: Some(display_path(repo_path)),
            required_checks: required_checks.to_vec(),
            planned_commands,
            commands: Vec::new(),
            summary: "no executable project-native validation commands were discovered".to_string(),
        };
    }

    run_commands_in_dir(
        config,
        Some(repo_path),
        planned_commands,
        required_checks.to_vec(),
    )
    .await
}

pub fn failure_reasons(report: &IndependentValidationReport) -> Vec<String> {
    report
        .commands
        .iter()
        .filter(|result| matches!(result.status.as_str(), "failed" | "timeout"))
        .map(|result| {
            result
                .reason
                .clone()
                .unwrap_or_else(|| format!("{} {}", result.command, result.status))
        })
        .collect()
}

fn repo_path(service: Option<&ServiceSnapshot>) -> Option<&Path> {
    service
        .and_then(|service| service.repo_path.as_deref())
        .map(Path::new)
        .filter(|path| !path.as_os_str().is_empty())
}

fn planned_commands(config: &ValidationConfig, service: Option<&ServiceSnapshot>) -> Vec<String> {
    project_native_verification_commands(service)
        .into_iter()
        .take(config.max_commands)
        .collect()
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}

async fn run_commands_in_dir(
    config: &ValidationConfig,
    repo_path: Option<&Path>,
    planned_commands: Vec<String>,
    required_checks: Vec<String>,
) -> IndependentValidationReport {
    let repo_path_buf = repo_path.map(Path::to_path_buf);
    let mut results = Vec::new();
    let mut has_unavailable = false;
    let mut has_failure = false;

    for command in &planned_commands {
        let result = run_command(config, repo_path_buf.as_deref(), command).await;
        if result.status == "unavailable" {
            has_unavailable = true;
        }
        if matches!(result.status.as_str(), "failed" | "timeout") {
            has_failure = true;
        }
        results.push(result);
    }

    let completeness = if has_unavailable { "partial" } else { "full" }.to_string();
    let passed_count = results
        .iter()
        .filter(|result| result.status == "passed")
        .count();
    let unavailable_count = results
        .iter()
        .filter(|result| result.status == "unavailable")
        .count();
    let failed_count = results
        .iter()
        .filter(|result| matches!(result.status.as_str(), "failed" | "timeout"))
        .count();
    let summary = if has_failure {
        format!(
            "independent validation failed: {} passed, {} failed, {} unavailable",
            passed_count, failed_count, unavailable_count
        )
    } else if has_unavailable {
        format!(
            "independent validation partially completed: {} passed, {} unavailable",
            passed_count, unavailable_count
        )
    } else {
        format!(
            "independent validation passed: {} command checks succeeded",
            passed_count
        )
    };

    IndependentValidationReport {
        enabled: true,
        enforced: config.require_success,
        passed: !has_failure,
        completeness,
        repo_path: repo_path_buf.as_deref().map(display_path),
        required_checks,
        planned_commands,
        commands: results,
        summary,
    }
}

async fn run_command(
    config: &ValidationConfig,
    repo_path: Option<&Path>,
    command: &str,
) -> ValidationCommandResult {
    let started = Instant::now();
    let (program, args) = match split_command(command) {
        Some(parts) => parts,
        None => {
            return ValidationCommandResult {
                command: command.to_string(),
                status: "failed".to_string(),
                exit_code: None,
                duration_ms: 0,
                stdout_excerpt: None,
                stderr_excerpt: None,
                reason: Some("validation command is empty".to_string()),
            };
        }
    };

    let mut process = Command::new(&program);
    process
        .kill_on_drop(true)
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(path) = repo_path {
        process.current_dir(path);
    }

    let child = match process.spawn() {
        Ok(child) => child,
        Err(error) if error.kind() == ErrorKind::NotFound && config.allow_missing_tooling => {
            return ValidationCommandResult {
                command: command.to_string(),
                status: "unavailable".to_string(),
                exit_code: None,
                duration_ms: started.elapsed().as_millis() as u64,
                stdout_excerpt: None,
                stderr_excerpt: None,
                reason: Some(format!(
                    "{} is not available in the current runtime",
                    program
                )),
            };
        }
        Err(error) => {
            return ValidationCommandResult {
                command: command.to_string(),
                status: "failed".to_string(),
                exit_code: None,
                duration_ms: started.elapsed().as_millis() as u64,
                stdout_excerpt: None,
                stderr_excerpt: None,
                reason: Some(error.to_string()),
            };
        }
    };

    match timeout(
        Duration::from_secs(config.timeout_seconds.max(1)),
        child.wait_with_output(),
    )
    .await
    {
        Ok(Ok(output)) => {
            let duration_ms = started.elapsed().as_millis() as u64;
            let stdout_excerpt = trim_output(&output.stdout, config.max_output_bytes);
            let stderr_excerpt = trim_output(&output.stderr, config.max_output_bytes);
            if output.status.success() {
                ValidationCommandResult {
                    command: command.to_string(),
                    status: "passed".to_string(),
                    exit_code: output.status.code(),
                    duration_ms,
                    stdout_excerpt,
                    stderr_excerpt,
                    reason: None,
                }
            } else {
                ValidationCommandResult {
                    command: command.to_string(),
                    status: "failed".to_string(),
                    exit_code: output.status.code(),
                    duration_ms,
                    stdout_excerpt,
                    stderr_excerpt,
                    reason: Some(match output.status.code() {
                        Some(code) => format!("{} exited with status {}", command, code),
                        None => format!("{} exited without a status code", command),
                    }),
                }
            }
        }
        Ok(Err(error)) => ValidationCommandResult {
            command: command.to_string(),
            status: "failed".to_string(),
            exit_code: None,
            duration_ms: started.elapsed().as_millis() as u64,
            stdout_excerpt: None,
            stderr_excerpt: None,
            reason: Some(error.to_string()),
        },
        Err(_) => ValidationCommandResult {
            command: command.to_string(),
            status: "timeout".to_string(),
            exit_code: None,
            duration_ms: started.elapsed().as_millis() as u64,
            stdout_excerpt: None,
            stderr_excerpt: None,
            reason: Some(format!(
                "{} exceeded the validation timeout of {} seconds",
                command, config.timeout_seconds
            )),
        },
    }
}

fn split_command(command: &str) -> Option<(String, Vec<String>)> {
    let mut parts = command.split_whitespace();
    let program = parts.next()?.trim();
    if program.is_empty() {
        return None;
    }
    Some((
        program.to_string(),
        parts.map(|part| part.to_string()).collect(),
    ))
}

fn trim_output(bytes: &[u8], max_output_bytes: usize) -> Option<String> {
    let rendered = String::from_utf8_lossy(bytes).trim().to_string();
    if rendered.is_empty() {
        return None;
    }
    if rendered.len() <= max_output_bytes {
        return Some(rendered);
    }
    let mut trimmed = String::new();
    for ch in rendered.chars() {
        if trimmed.len() + ch.len_utf8() > max_output_bytes {
            break;
        }
        trimmed.push(ch);
    }
    Some(format!("{}...[truncated]", trimmed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn independent_validation_reports_command_failures() {
        let repo = tempdir().expect("tempdir");
        let mut config = ValidationConfig::default();
        config.timeout_seconds = 5;
        let report = run_commands_in_dir(
            &config,
            Some(repo.path()),
            vec!["true".to_string(), "false".to_string()],
            vec!["true".to_string(), "false".to_string()],
        )
        .await;

        assert!(!report.passed);
        assert_eq!(report.completeness, "full");
        assert_eq!(report.commands.len(), 2);
        assert_eq!(report.commands[0].status, "passed");
        assert_eq!(report.commands[1].status, "failed");
    }

    #[tokio::test]
    async fn independent_validation_tolerates_missing_tooling_when_allowed() {
        let repo = tempdir().expect("tempdir");
        let report = run_commands_in_dir(
            &ValidationConfig::default(),
            Some(repo.path()),
            vec!["definitely-not-a-real-validation-tool".to_string()],
            vec![],
        )
        .await;

        assert!(report.passed);
        assert_eq!(report.completeness, "partial");
        assert_eq!(report.commands.len(), 1);
        assert_eq!(report.commands[0].status, "unavailable");
    }

    #[test]
    fn preview_marks_commands_as_planned() {
        let repo = tempdir().expect("tempdir");
        std::fs::write(
            repo.path().join("Cargo.toml"),
            "[package]\nname=\"x\"\nversion=\"0.1.0\"\nedition=\"2024\"\n",
        )
        .expect("cargo");
        let service = ServiceSnapshot {
            service_key: "demo".to_string(),
            display_name: "Demo".to_string(),
            kind: "tenant_service".to_string(),
            role_name: "demo".to_string(),
            playbooks: vec![],
            host_targets: vec![],
            hosts: vec![],
            namespace: None,
            service_name: None,
            deployment_environment: None,
            internal_url: None,
            public_url: None,
            repo_path: Some(repo.path().display().to_string()),
            repo_url: None,
            repo_branch: None,
            health: crate::models::ServiceHealth::Healthy,
            capabilities: vec![],
            dependencies: vec![],
            storage_paths: vec![],
            raw_defaults: serde_json::json!({}),
            probe: serde_json::json!({}),
            discovered_at: crate::models::now_utc(),
            updated_at: crate::models::now_utc(),
        };

        let report =
            preview_independent_validation(&ValidationConfig::default(), Some(&service), &[]);
        assert_eq!(report.completeness, "planned");
        assert!(!report.commands.is_empty());
        assert!(
            report
                .commands
                .iter()
                .all(|result| result.status == "planned")
        );
    }
}
