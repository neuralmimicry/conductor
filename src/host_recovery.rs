use serde_json::Value;

#[derive(Clone, Debug, Default)]
pub struct HostRecoveryInput {
    pub device_id: String,
    pub controller_id: String,
    pub inventory_fresh: bool,
    pub identity_verified: bool,
    pub power_actions_enabled: bool,
    pub host_reported_offline: bool,
    pub offline_age_seconds: Option<u64>,
    pub tracey_observation_age_seconds: Option<u64>,
    pub dhcp_fresh: bool,
    pub target_has_dhcp_lease: Option<bool>,
    pub chassis_powered: Option<bool>,
    pub power_faults: Vec<String>,
    pub critical_sensors: Vec<String>,
    pub hosted_services_known: bool,
    pub hosted_services_unavailable: bool,
    pub dependency_graph_complete: bool,
    pub dependencies_healthy: bool,
    pub scheduler_status_fresh: bool,
    pub scheduler_blockers: Vec<String>,
    pub monitoring_state: Option<String>,
}

#[derive(Clone, Debug)]
pub struct HostRecoveryAssessment {
    pub eligible: bool,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
}

pub fn assess_host_recovery(
    input: &HostRecoveryInput,
    action: &str,
    minimum_offline_seconds: u64,
    maximum_observation_age_seconds: u64,
) -> HostRecoveryAssessment {
    let mut blockers = Vec::new();
    let mut warnings = Vec::new();
    if !matches!(action, "warm_restart" | "cold_restart") {
        blockers.push("host recovery action is unsupported".to_string());
    }
    if input.device_id.trim().is_empty() || input.controller_id.trim().is_empty() {
        blockers.push("host or controller identity is missing".to_string());
    }
    if !input.inventory_fresh {
        blockers.push("Continuum device inventory is stale or malformed".to_string());
    }
    if !input.identity_verified {
        blockers.push("device-to-controller identity is not uniquely verified".to_string());
    }
    if !input.power_actions_enabled {
        blockers.push("BMC power actions are disabled by inventory policy".to_string());
    }
    if !input.host_reported_offline {
        blockers.push("Tracey does not freshly report the exact host as offline".to_string());
    }
    match input.offline_age_seconds {
        Some(age) if age >= minimum_offline_seconds => {}
        Some(age) => blockers.push(format!(
            "host has been offline for {age}s; {minimum_offline_seconds}s is required"
        )),
        None => blockers.push("host offline duration is unknown".to_string()),
    }
    match input.tracey_observation_age_seconds {
        Some(age) if age <= maximum_observation_age_seconds => {}
        Some(age) => blockers.push(format!("Tracey host observation is stale ({age}s old)")),
        None => blockers.push("fresh Tracey host observation is unavailable".to_string()),
    }
    if !input.dhcp_fresh {
        blockers.push("fresh Continuum DHCP evidence is unavailable".to_string());
    }
    match input.target_has_dhcp_lease {
        Some(false) => {}
        Some(true) => blockers.push("target still has a fresh DHCP lease".to_string()),
        None => blockers.push("target DHCP lease state is unknown".to_string()),
    }
    if input.chassis_powered != Some(true) {
        blockers.push("BMC did not freshly confirm the chassis is powered on".to_string());
    }
    blockers.extend(input.power_faults.iter().cloned());
    for sensor in &input.critical_sensors {
        if sensor.eq_ignore_ascii_case("SYS_V3_Battery") {
            if action == "warm_restart" {
                // A warm IPMI reset does not remove chassis power. A cold
                // power-cycle remains blocked until this critical sensor has
                // been diagnosed.
                warnings.push(format!("critical BMC battery sensor: {sensor}"));
            } else {
                blockers.push(format!("critical BMC sensor blocks cold restart: {sensor}"));
            }
        } else {
            blockers.push(format!("critical BMC sensor blocks restart: {sensor}"));
        }
    }
    if !input.hosted_services_known {
        blockers.push("hosted service inventory is missing or ambiguous".to_string());
    }
    if !input.hosted_services_unavailable {
        blockers.push(
            "one or more hosted services are still healthy or their outage is unproven".to_string(),
        );
    }
    if !input.dependency_graph_complete {
        blockers.push(
            "host dependency graph is empty or not fully mapped to live services".to_string(),
        );
    }
    if !input.dependencies_healthy {
        blockers.push("one or more declared host dependencies are not freshly healthy".to_string());
    }
    if !input.scheduler_status_fresh {
        blockers.push("Continuum could not provide fresh Gail/Slurm status".to_string());
    }
    blockers.extend(input.scheduler_blockers.iter().cloned());
    if !matches!(
        input.monitoring_state.as_deref(),
        Some("healthy" | "unavailable")
    ) {
        blockers.push("monitoring health is unknown or degraded".to_string());
    }
    blockers.sort();
    blockers.dedup();
    warnings.sort();
    warnings.dedup();
    HostRecoveryAssessment {
        eligible: blockers.is_empty(),
        blockers,
        warnings,
    }
}

/// Unwrap the standard Continuum envelope while accepting already-unwrapped
/// payloads used by older NMC releases. The bounded depth avoids following an
/// arbitrary nested user value.
pub fn continuum_data(mut value: &Value) -> &Value {
    for _ in 0..4 {
        let Some(next) = value.get("data").filter(|value| value.is_object()) else {
            break;
        };
        value = next;
    }
    value
}

pub fn inventory_is_fresh(payload: &Value, now_epoch_ms: i64) -> bool {
    let data = continuum_data(payload);
    let Some(generated_at) = data.get("generated_at_unix_ms").and_then(Value::as_i64) else {
        return false;
    };
    let Some(max_age_ms) = data.get("max_age_ms").and_then(Value::as_i64) else {
        return false;
    };
    let age_ms = now_epoch_ms.saturating_sub(generated_at);
    age_ms >= -30_000 && max_age_ms > 0 && age_ms <= max_age_ms
}

pub fn inventory_device<'a>(payload: &'a Value, device_id: &str) -> Option<&'a Value> {
    continuum_data(payload)
        .get("devices")?
        .as_array()?
        .iter()
        .find(|device| device.get("id").and_then(Value::as_str) == Some(device_id))
}

pub fn inventory_controller<'a>(payload: &'a Value, controller_id: &str) -> Option<&'a Value> {
    continuum_data(payload)
        .get("controllers")?
        .as_array()?
        .iter()
        .find(|controller| controller.get("id").and_then(Value::as_str) == Some(controller_id))
}

/// Continuum normally materialises the enabled-by-default controller policy.
/// A consumer must nevertheless fail closed when an old or malformed
/// inventory omits that explicit field.
pub fn controller_power_actions_enabled(controller: Option<&Value>) -> bool {
    controller
        .and_then(|item| item.get("power_actions_enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

pub fn tracey_host<'a>(payload: &'a Value, device_id: &str) -> Option<&'a Value> {
    let agents = continuum_data(payload).get("agents")?.as_array()?;
    let expected_agent = format!("tracey-{device_id}");
    agents
        .iter()
        .filter(|agent| {
            agent.get("agent_id").and_then(Value::as_str) == Some(expected_agent.as_str())
                || agent.get("host").and_then(Value::as_str) == Some(device_id)
        })
        .filter(|agent| agent.get("link_state").is_some())
        .max_by_key(|agent| {
            agent
                .get("last_query_epoch_ms")
                .and_then(Value::as_i64)
                .unwrap_or_default()
        })
}

pub fn tracey_observation_age_seconds(agent: &Value) -> Option<u64> {
    agent
        .get("last_query_seconds_ago")
        .and_then(Value::as_u64)
        .or_else(|| {
            agent
                .get("last_query_epoch_ms")
                .and_then(Value::as_i64)
                .map(|observed| {
                    (chrono::Utc::now().timestamp_millis() - observed).max(0) as u64 / 1_000
                })
        })
}

pub fn tracey_offline_age_seconds(agent: &Value) -> Option<u64> {
    agent
        .get("last_seen_seconds_ago")
        .and_then(Value::as_u64)
        .or_else(|| {
            agent
                .get("last_seen_epoch_ms")
                .and_then(Value::as_i64)
                .map(|seen| (chrono::Utc::now().timestamp_millis() - seen).max(0) as u64 / 1_000)
        })
}

pub fn diagnostic_powered(diagnostics: &Value) -> Option<bool> {
    let data = continuum_data(diagnostics);
    let output = data.get("chassis_status").and_then(Value::as_str)?;
    let normalized = output.trim().to_ascii_lowercase();
    if normalized.lines().any(|line| {
        line.split_once(':')
            .is_some_and(|(name, value)| name.trim() == "system power" && value.trim() == "on")
    }) || normalized.contains("chassis power is on")
    {
        Some(true)
    } else if normalized.lines().any(|line| {
        line.split_once(':')
            .is_some_and(|(name, value)| name.trim() == "system power" && value.trim() == "off")
    }) || normalized.contains("chassis power is off")
    {
        Some(false)
    } else {
        None
    }
}

pub fn diagnostic_safety_faults(diagnostics: &Value) -> Vec<String> {
    let data = continuum_data(diagnostics);
    let mut faults = Vec::new();
    if data.get("inventory_stale").and_then(Value::as_bool) != Some(false) {
        faults.push("BMC diagnostics report stale device inventory".to_string());
    }
    if data
        .pointer("/system_event_log/available")
        .and_then(Value::as_bool)
        != Some(true)
    {
        faults.push("BMC system event log is unavailable".to_string());
    }
    faults
}

pub fn diagnostic_observation_age_seconds(diagnostics: &Value, now_epoch_ms: i64) -> Option<u64> {
    continuum_data(diagnostics)
        .get("observed_at_unix_ms")
        .and_then(Value::as_i64)
        .map(|observed| now_epoch_ms.saturating_sub(observed).max(0) as u64 / 1_000)
}

pub fn trainer_jobs_for_host(payload: &Value, host: &str) -> Vec<String> {
    const ACTIVE: &[&str] = &[
        "pending",
        "queued",
        "configuring",
        "running",
        "completing",
        "suspended",
        "resizing",
        "requeue_fed",
        "requeued",
    ];
    let root = continuum_data(payload)
        .get("payload")
        .unwrap_or_else(|| continuum_data(payload));
    let mut jobs = Vec::new();
    collect_trainer_jobs(root, host, ACTIVE, 0, &mut jobs);
    jobs.sort();
    jobs.dedup();
    jobs
}

fn collect_trainer_jobs(
    value: &Value,
    host: &str,
    active_states: &[&str],
    depth: usize,
    jobs: &mut Vec<String>,
) {
    if depth > 8 || jobs.len() >= 256 {
        return;
    }
    match value {
        Value::Array(values) => {
            for child in values.iter().take(512) {
                collect_trainer_jobs(child, host, active_states, depth + 1, jobs);
            }
        }
        Value::Object(object) => {
            let status = ["state", "status", "job_state", "slurm_state"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_str))
                .unwrap_or_default()
                .to_ascii_lowercase();
            let node_list = [
                "nodelist",
                "node_list",
                "requested_nodelist",
                "requested_node_list",
                "requested_nodes",
                "nodes",
            ]
            .iter()
            .find_map(|key| object.get(*key));
            let includes_target = match node_list {
                Some(Value::String(value)) => slurm_node_list_contains(value, host),
                Some(Value::Array(values)) => values
                    .iter()
                    .any(|value| value.as_str().is_some_and(|name| name == host)),
                _ => false,
            };
            if active_states.contains(&status.as_str()) && includes_target {
                let id = ["job_id", "id", "slurm_job_id"]
                    .iter()
                    .find_map(|key| object.get(*key).and_then(Value::as_str))
                    .unwrap_or("unknown-id");
                jobs.push(format!(
                    "active Gail/Slurm job {id} requests {host} ({status})"
                ));
            }
            for child in object.values() {
                collect_trainer_jobs(child, host, active_states, depth + 1, jobs);
            }
        }
        _ => {}
    }
}

pub fn dhcp_has_device_lease(payload: &Value, device_id: &str) -> Option<bool> {
    let data = continuum_data(payload);
    if data.get("inventory_stale").and_then(Value::as_bool) != Some(false) {
        return None;
    }
    let servers = data.get("servers")?.as_object()?;
    let available = servers.values().any(|server| {
        server.get("available").and_then(Value::as_bool) == Some(true)
            && server.get("stale").and_then(Value::as_bool) == Some(false)
            && server.get("leases").and_then(Value::as_array).is_some()
    });
    if !available {
        return None;
    }
    let has_lease = servers.values().any(|server| {
        server
            .get("leases")
            .and_then(Value::as_array)
            .is_some_and(|leases| {
                leases.iter().any(|lease| {
                    lease.get("observation_fresh").and_then(Value::as_bool) == Some(true)
                        && lease.get("device_id").and_then(Value::as_str) == Some(device_id)
                })
            })
    });
    Some(has_lease)
}

/// Match Slurm's compact host-list notation (for example `qc[00,02-04]`) to
/// one exact host. Unknown syntax is treated as no match only by this helper;
/// callers still fail closed when the active job's node list is unavailable.
pub fn slurm_node_list_contains(nodelist: &str, host: &str) -> bool {
    if nodelist.trim().is_empty() || host.trim().is_empty() {
        return false;
    }
    for group in split_outside_brackets(nodelist, ',') {
        if group == host {
            return true;
        }
        let Some(open) = group.find('[') else {
            continue;
        };
        let Some(close) = group[open + 1..].find(']') else {
            continue;
        };
        let close = open + 1 + close;
        let prefix = &group[..open];
        let suffix = &group[close + 1..];
        let Some(host_middle) = host
            .strip_prefix(prefix)
            .and_then(|value| value.strip_suffix(suffix))
        else {
            continue;
        };
        if host_middle.is_empty() || !host_middle.chars().all(|ch| ch.is_ascii_digit()) {
            continue;
        }
        let host_number = match host_middle.parse::<u32>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        if split_outside_brackets(&group[open + 1..close], ',')
            .into_iter()
            .any(|part| {
                if let Some((start, end)) = part.split_once('-') {
                    matches!((start.parse::<u32>(), end.parse::<u32>()), (Ok(a), Ok(b)) if a <= host_number && host_number <= b)
                } else {
                    part.parse::<u32>() == Ok(host_number)
                }
            })
        {
            return true;
        }
    }
    false
}

pub fn critical_sensor_names(diagnostics: &Value) -> Vec<String> {
    let data = continuum_data(diagnostics);
    let Some(output) = data.pointer("/sensors/output").and_then(Value::as_str) else {
        return Vec::new();
    };
    let mut names = output
        .lines()
        .filter_map(|line| {
            let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
            (fields.len() > 3
                && matches!(fields[3].to_ascii_lowercase().as_str(), "cr" | "critical"))
            .then(|| fields[0].to_string())
        })
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn split_outside_brackets(value: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, character) in value.char_indices() {
        match character {
            '[' => depth = depth.saturating_add(1),
            ']' => depth = depth.saturating_sub(1),
            _ if character == delimiter && depth == 0 => {
                parts.push(&value[start..index]);
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&value[start..]);
    parts
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn selects_fresh_tracey_host_observation_and_age() {
        let payload = json!({
            "success": true,
            "data": {"agents": [
                {"agent_id":"tracey-qc04","host":"qc04","link_state":"offline","last_query_epoch_ms":100,"last_query_seconds_ago":20,"last_seen_seconds_ago":248451},
                {"agent_id":"tracey-qc04","host":"qc04","latency_ms":1}
            ]}
        });
        let agent = tracey_host(&payload, "qc04").expect("host observation");
        assert_eq!(agent["link_state"], "offline");
        assert_eq!(tracey_observation_age_seconds(agent), Some(20));
        assert_eq!(tracey_offline_age_seconds(agent), Some(248451));
    }

    #[test]
    fn requires_a_fresh_dhcp_collection_and_matches_registered_identity() {
        let payload = json!({
            "data": {"inventory_stale": false, "servers": {
                "vega": {"available":true,"stale":false,"leases":[
                    {"device_id":"qc01","observation_fresh":true},
                    {"device_id":"qc04","observation_fresh":true}
                ]}
            }}
        });
        assert_eq!(dhcp_has_device_lease(&payload, "qc04"), Some(true));
        assert_eq!(dhcp_has_device_lease(&payload, "qc05"), Some(false));
        let stale = json!({"data":{"inventory_stale":true,"servers":{}}});
        assert_eq!(dhcp_has_device_lease(&stale, "qc04"), None);
    }

    #[test]
    fn missing_controller_action_policy_fails_closed() {
        assert!(!controller_power_actions_enabled(None));
        assert!(!controller_power_actions_enabled(Some(&json!({}))));
        assert!(!controller_power_actions_enabled(Some(
            &json!({"power_actions_enabled": false})
        )));
        assert!(controller_power_actions_enabled(Some(
            &json!({"power_actions_enabled": true})
        )));
    }

    #[test]
    fn expands_slurm_node_lists_without_substring_matches() {
        assert!(slurm_node_list_contains("qc[00,02-04],sm[00-01]", "qc04"));
        assert!(slurm_node_list_contains("qc[00-05]", "qc04"));
        assert!(!slurm_node_list_contains("qc[00-03]", "qc04"));
        assert!(!slurm_node_list_contains("qc[00-05]", "qc040"));
        assert!(slurm_node_list_contains("qc04,sm00", "qc04"));
    }

    #[test]
    fn extracts_only_critical_bmc_sensor_lines() {
        let diagnostics = json!({"data":{"sensors":{"output":"SYS_V3.3 | 3.3 | Volts | ok | x\nSYS_V3_Battery | 0.0 | Volts | cr | x\n"}}});
        assert_eq!(critical_sensor_names(&diagnostics), ["SYS_V3_Battery"]);
    }

    #[test]
    fn validates_inventory_and_diagnostic_freshness_and_power_state() {
        let now = 1_790_000_000_000_i64;
        let inventory = json!({"data":{"generated_at_unix_ms":now-1000,"max_age_ms":60000}});
        assert!(inventory_is_fresh(&inventory, now));
        assert!(!inventory_is_fresh(&inventory, now + 60_000));

        let diagnostics = json!({"data":{
            "observed_at_unix_ms":now-500,
            "inventory_stale":false,
            "chassis_status":"System Power         : on\n",
            "system_event_log":{"available":false}
        }});
        assert_eq!(diagnostic_powered(&diagnostics), Some(true));
        assert_eq!(
            diagnostic_observation_age_seconds(&diagnostics, now),
            Some(0)
        );
        assert_eq!(
            diagnostic_safety_faults(&diagnostics),
            ["BMC system event log is unavailable"]
        );
    }

    #[test]
    fn blocks_only_active_jobs_that_request_the_exact_host() {
        let status = json!({"data":{"payload":{"jobs":[
            {"job_id":"4479","state":"PENDING","nodelist":"qc[02-04]"},
            {"job_id":"4480","state":"RUNNING","nodelist":"qc[00-03]"},
            {"job_id":"4481","state":"COMPLETED","nodelist":"qc04"}
        ]}}});
        assert_eq!(
            trainer_jobs_for_host(&status, "qc04"),
            ["active Gail/Slurm job 4479 requests qc04 (pending)"]
        );
    }

    #[test]
    fn blocks_host_action_when_scheduler_or_hardware_evidence_is_missing() {
        let mut input = HostRecoveryInput {
            device_id: "qc04".to_string(),
            controller_id: "qc04-bmc".to_string(),
            inventory_fresh: true,
            identity_verified: true,
            power_actions_enabled: true,
            host_reported_offline: true,
            offline_age_seconds: Some(248_451),
            tracey_observation_age_seconds: Some(31),
            dhcp_fresh: true,
            target_has_dhcp_lease: Some(false),
            chassis_powered: Some(true),
            hosted_services_known: true,
            dependencies_healthy: true,
            scheduler_status_fresh: false,
            monitoring_state: Some("healthy".to_string()),
            critical_sensors: vec!["SYS_V3_Battery".to_string()],
            ..HostRecoveryInput::default()
        };
        let assessment = assess_host_recovery(&input, "warm_restart", 3_600, 120);
        assert!(!assessment.eligible);
        assert!(
            assessment
                .blockers
                .iter()
                .any(|blocker| blocker.contains("Gail/Slurm"))
        );
        assert_eq!(
            assessment.warnings,
            ["critical BMC battery sensor: SYS_V3_Battery"]
        );

        input.scheduler_status_fresh = true;
        input.scheduler_blockers = vec!["active Slurm job requests qc04".to_string()];
        let assessment = assess_host_recovery(&input, "warm_restart", 3_600, 120);
        assert!(!assessment.eligible);
        assert!(
            assessment
                .blockers
                .iter()
                .any(|blocker| blocker.contains("requests qc04"))
        );
    }

    #[test]
    fn allows_only_fully_evidenced_warm_recovery_facts() {
        let input = HostRecoveryInput {
            device_id: "qc04".to_string(),
            controller_id: "qc04-bmc".to_string(),
            inventory_fresh: true,
            identity_verified: true,
            power_actions_enabled: true,
            host_reported_offline: true,
            offline_age_seconds: Some(248_451),
            tracey_observation_age_seconds: Some(31),
            dhcp_fresh: true,
            target_has_dhcp_lease: Some(false),
            chassis_powered: Some(true),
            hosted_services_known: true,
            hosted_services_unavailable: true,
            dependency_graph_complete: true,
            dependencies_healthy: true,
            scheduler_status_fresh: true,
            monitoring_state: Some("healthy".to_string()),
            critical_sensors: vec!["SYS_V3_Battery".to_string()],
            ..HostRecoveryInput::default()
        };
        let assessment = assess_host_recovery(&input, "warm_restart", 3_600, 120);
        assert!(assessment.eligible, "{:?}", assessment.blockers);
        assert_eq!(assessment.warnings.len(), 1);

        let cold = assess_host_recovery(&input, "cold_restart", 3_600, 120);
        assert!(!cold.eligible);
        assert!(
            cold.blockers
                .iter()
                .any(|blocker| { blocker.contains("critical BMC sensor blocks cold restart") })
        );
    }
}

/// These are investigation signals, never a claimed root cause or permission
/// to reset a host again. Ignore unrelated timestamps and redact risky excerpts.
pub fn post_restart_log_findings(logs: &[Value], restart_ms: i64, now_ms: i64) -> Value {
    let mut findings = Vec::new();
    let mut examined = 0usize;
    for log in logs.iter().take(200) {
        let Some(at) = log["ts_ms"].as_i64() else {
            continue;
        };
        if at < restart_ms.saturating_sub(3_600_000)
            || at > now_ms.min(restart_ms.saturating_add(3_600_000))
        {
            continue;
        }
        examined += 1;
        let message = log["message"].as_str().unwrap_or_default();
        let lower = message.to_ascii_lowercase();
        let signals: Vec<_> = [
            ("memory", "out of memory"),
            ("memory", "oom"),
            ("kernel", "panic"),
            ("storage", "i/o error"),
            ("storage", "corrupt"),
            ("storage", "failed to mount"),
            ("database", "connection pool"),
            ("database", "postgres"),
            ("authentication", "auth request"),
            ("availability", "503"),
            ("availability", "500"),
            ("service", "crashloop"),
            ("service", "segfault"),
        ]
        .iter()
        .filter(|(_, term)| lower.contains(term))
        .map(|(kind, _)| *kind)
        .collect();
        if !signals.is_empty()
            || matches!(log["level"].as_str(), Some("error" | "critical" | "fatal"))
        {
            let excerpt = safe_review_excerpt(message);
            findings.push(serde_json::json!({"ts_ms":at,"signals":signals,"category":log["category"],"excerpt":excerpt}));
        }
    }
    serde_json::json!({"examined":examined,"findings":findings,
        "bounded":true,"truncated":logs.len() >= 200,"root_cause_confirmed":false,
        "previous_boot_coverage_verified":false,"investigation_required":true})
}

fn safe_review_excerpt(message: &str) -> String {
    let lower = message.to_ascii_lowercase();
    if [
        "password",
        "authorization",
        "bearer ",
        "token",
        "secret",
        "cookie",
        "private key",
        "://",
    ]
    .iter()
    .any(|term| lower.contains(term))
    {
        "[sensitive log excerpt redacted]".into()
    } else {
        message.chars().take(480).collect()
    }
}

pub fn restart_workload_findings(payload: &Value, restart_ms: i64, now_ms: i64) -> Value {
    let items = payload["items"].as_array();
    let mut logs = Vec::new();
    let mut timestamp_gaps = 0usize;
    for item in items.into_iter().flatten().take(16) {
        for line in item["logs"].as_str().unwrap_or_default().lines().take(100) {
            let Some((timestamp, message)) = line.split_once(' ') else {
                timestamp_gaps += 1;
                continue;
            };
            let Ok(at) = chrono::DateTime::parse_from_rfc3339(timestamp) else {
                timestamp_gaps += 1;
                continue;
            };
            logs.push(serde_json::json!({"ts_ms":at.timestamp_millis(),"message":message,"category":"workload",
                "level":if message.to_ascii_lowercase().contains("error"){"error"}else{"info"}}));
        }
    }
    let findings = post_restart_log_findings(&logs, restart_ms, now_ms);
    serde_json::json!({"findings":findings,"timestamp_gaps":timestamp_gaps,"upstream_errors":payload["error_count"],
        "truncated":payload["truncated"] == true || items.is_some_and(|rows|rows.len()>16) || logs.len()>=200,
        "review_complete":false,"raw_logs_retained":false})
}

/// Only operator-owned SSH coordinates that match a fresh unique inventory IP
/// may be used. No agent-supplied host, command, key or SSH configuration executes.
fn review_ssh_args(
    settings: &crate::config::RestartReviewSshConfig,
    device: &Value,
    restart_ms: i64,
) -> anyhow::Result<Vec<String>> {
    use anyhow::{anyhow, ensure};
    ensure!(
        device["id"] == settings.device_id
            && device["addresses"]
                .as_array()
                .is_some_and(|addresses| addresses
                    .iter()
                    .any(|a| a.as_str() == Some(settings.address.as_str()))),
        "SSH address is not inventory-pinned"
    );
    settings
        .address
        .parse::<std::net::IpAddr>()
        .map_err(|_| anyhow!("SSH address must be an IP"))?;
    ensure!(
        !settings.username.is_empty()
            && settings.username.len() <= 64
            && settings
                .username
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
        "SSH username is invalid"
    );
    for path in [&settings.identity_file, &settings.known_hosts_file] {
        ensure!(
            std::path::Path::new(path).is_absolute() && !path.contains(['\n', '\r', '\0']),
            "SSH evidence requires absolute key and known-host paths"
        );
    }
    ensure!(restart_ms > 3_600_000, "invalid restart timestamp");
    Ok(vec![
        "-F".into(),
        "/dev/null".into(),
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "StrictHostKeyChecking=yes".into(),
        "-o".into(),
        format!("UserKnownHostsFile={}", settings.known_hosts_file),
        "-o".into(),
        "GlobalKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "IdentitiesOnly=yes".into(),
        "-o".into(),
        "ForwardAgent=no".into(),
        "-o".into(),
        "ClearAllForwardings=yes".into(),
        "-o".into(),
        "ConnectTimeout=5".into(),
        "-o".into(),
        "ConnectionAttempts=1".into(),
        "-i".into(),
        settings.identity_file.clone(),
        "-l".into(),
        settings.username.clone(),
        settings.address.clone(),
    ])
}

pub async fn restart_ssh_evidence(
    settings: Option<&crate::config::RestartReviewSshConfig>,
    device: Option<&Value>,
    inventory_fresh: bool,
    restart_ms: i64,
) -> Value {
    use tokio::io::AsyncReadExt;
    let unavailable = |gap: &str| serde_json::json!({"available":false,"gap":gap,"logs":[],"review_complete":false});
    let (Some(settings), Some(device)) = (settings, device) else {
        return unavailable("Approved SSH evidence settings or inventory device unavailable");
    };
    if !inventory_fresh {
        return unavailable("Inventory is stale; SSH collection held");
    }
    let Ok(args) = review_ssh_args(settings, device, restart_ms) else {
        return unavailable("SSH coordinates/key/known-host policy failed inventory validation");
    };
    let mut sources = Vec::new();
    let mut logs = Vec::new();
    for (source, flags) in [
        ("previous_boot", "-b -1 -p warning"),
        ("previous_kernel", "-b -1 -k"),
        ("current_services", "-b 0 -p warning"),
    ] {
        // The remote shell receives only fixed words and validated integer times.
        let command = format!(
            "sudo -n /usr/bin/journalctl --no-pager --output=json -n 200 {flags} --since=@{} --until=@{}",
            (restart_ms - 3_600_000) / 1000,
            (restart_ms + 3_600_000) / 1000
        );
        let result = async {
            let mut child = tokio::process::Command::new("/usr/bin/ssh")
                .args(&args)
                .arg(command)
                .stdin(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()?;
            let mut bytes = Vec::new();
            child
                .stdout
                .take()
                .ok_or_else(|| anyhow::anyhow!("no stdout"))?
                .take(262_145)
                .read_to_end(&mut bytes)
                .await?;
            anyhow::ensure!(bytes.len() <= 262_144, "journal exceeds evidence bound");
            anyhow::ensure!(child.wait().await?.success(), "journal command failed");
            let text = std::str::from_utf8(&bytes)?;
            let mut rows = Vec::new();
            for line in text.lines() {
                let entry: Value = serde_json::from_str(line)?;
                let at = entry["__REALTIME_TIMESTAMP"]
                    .as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .ok_or_else(|| anyhow::anyhow!("journal timestamp missing"))?
                    / 1000;
                if at < restart_ms - 3_600_000 || at > restart_ms + 3_600_000 {
                    continue;
                }
                rows.push(serde_json::json!({"ts_ms":at,"message":safe_review_excerpt(entry["MESSAGE"].as_str().unwrap_or_default()),"level":if matches!(entry["PRIORITY"].as_str(),Some("0"|"1"|"2"|"3")) {"error"} else {"warning"},"category":source,"boot_id":entry["_BOOT_ID"]}));
                if rows.len() >= 200 {
                    break;
                }
            }
            Ok::<_, anyhow::Error>(rows)
        };
        match tokio::time::timeout(std::time::Duration::from_secs(10), result).await {
            Ok(Ok(rows)) => {
                sources.push(serde_json::json!({"source":source,"available":!rows.is_empty(),"records":rows.len(),"truncated":rows.len() >= 200,"findings":post_restart_log_findings(&rows,restart_ms,restart_ms+3_600_000),"boot_ids":rows.iter().filter_map(|r|r["boot_id"].as_str()).collect::<std::collections::BTreeSet<_>>(),"gap":if rows.is_empty(){Some("No correlated journal entries; previous-boot coverage unproven")}else{None}}));
                logs.extend(rows);
            }
            _ => sources.push(serde_json::json!({"source":source,"available":false,"gap":"SSH/journal collection unavailable or exceeded time/output bounds"})),
        }
    }
    let findings = post_restart_log_findings(&logs, restart_ms, restart_ms + 3_600_000);
    serde_json::json!({"available":!logs.is_empty(),"sources":sources,"findings":findings,"bounded":true,"review_complete":false})
}

async fn bounded_monitoring_json(
    external: &crate::config::ExternalServiceConfig,
    path: &str,
    params: &[(String, String)],
) -> anyhow::Result<Value> {
    anyhow::ensure!(external.enabled, "monitor disabled");
    let base = external
        .base_url
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("monitor URL missing"))?;
    let url = reqwest::Url::parse(base)?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.username().is_empty()
            && url.password().is_none(),
        "invalid monitoring URL"
    );
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(
            external.timeout_seconds.clamp(1, 10),
        ))
        .build()?;
    let mut response = crate::integrations::apply_external_auth(
        client
            .get(format!("{}{path}", base.trim_end_matches('/')))
            .query(params),
        external,
    )
    .send()
    .await?
    .error_for_status()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        anyhow::ensure!(
            bytes.len() + chunk.len() <= 262_144,
            "monitor response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn range_summary(value: &Value, start: i64, end: i64) -> anyhow::Result<Value> {
    anyhow::ensure!(
        value["status"] == "success" && value["data"]["resultType"] == "matrix",
        "range response invalid"
    );
    let rows = value["data"]["result"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing series"))?;
    anyhow::ensure!(
        !rows.is_empty() && rows.len() <= 64,
        "range series missing/excessive"
    );
    let mut samples = Vec::new();
    let mut coverage_verified = true;
    for row in rows {
        let mut times = Vec::new();
        for sample in row["values"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("missing samples"))?
        {
            let at = sample[0]
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("bad timestamp"))?;
            let reading = sample[1]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or_else(|| anyhow::anyhow!("bad sample"))?;
            anyhow::ensure!(
                at.is_finite() && at >= start as f64 && at <= end as f64 && reading.is_finite(),
                "out-of-window/non-finite sample"
            );
            samples.push((at, reading));
            times.push(at);
            anyhow::ensure!(samples.len() <= 8192, "too many samples");
        }
        times.sort_by(f64::total_cmp);
        coverage_verified &= times.first().is_some_and(|at| *at <= start as f64 + 60.0)
            && times.last().is_some_and(|at| *at >= end as f64 - 60.0)
            && times.windows(2).all(|w| w[1] - w[0] <= 120.0);
    }
    anyhow::ensure!(!samples.is_empty(), "no samples");
    Ok(
        serde_json::json!({"series":rows.len(),"samples":samples.len(),"coverage_verified":coverage_verified,"gap":if coverage_verified{None}else{Some("Incomplete sample coverage around the restart; available samples cannot establish outage health")},"first_sample":samples.iter().map(|s|s.0).fold(f64::INFINITY,f64::min),"last_sample":samples.iter().map(|s|s.0).fold(f64::NEG_INFINITY,f64::max),"minimum":samples.iter().map(|s|s.1).fold(f64::INFINITY,f64::min),"maximum":samples.iter().map(|s|s.1).fold(f64::NEG_INFINITY,f64::max),"root_cause_confirmed":false}),
    )
}

pub async fn restart_monitoring_evidence(
    config: &crate::config::ConductorConfig,
    host: &str,
    restart_ms: i64,
    now_ms: i64,
) -> Value {
    if host.is_empty()
        || host.len() > 128
        || !host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        || restart_ms <= 3_600_000
        || now_ms < restart_ms
    {
        return serde_json::json!({"available":false,"gap":"Invalid host or restart window","review_complete":false});
    }
    let start = (restart_ms - 3_600_000) / 1000;
    let end = now_ms.min(restart_ms + 3_600_000) / 1000;
    let queries = [
        (
            "host_probe_success",
            format!("probe_success{{device=\"{host}\"}}"),
        ),
        (
            "host_memory_available",
            format!("node_memory_MemAvailable_bytes{{host=\"{host}\"}}"),
        ),
        (
            "host_filesystem_available",
            format!("node_filesystem_avail_bytes{{host=\"{host}\",fstype!~\"tmpfs|overlay\"}}"),
        ),
        (
            "host_boot_time",
            format!("node_boot_time_seconds{{host=\"{host}\"}}"),
        ),
        (
            "host_readiness",
            format!(
                "kube_node_status_condition{{node=\"{host}\",condition=\"Ready\",status=\"true\"}}"
            ),
        ),
        (
            "host_memory_pressure",
            format!(
                "kube_node_status_condition{{node=\"{host}\",condition=\"MemoryPressure\",status=\"true\"}}"
            ),
        ),
        (
            "host_disk_pressure",
            format!(
                "kube_node_status_condition{{node=\"{host}\",condition=\"DiskPressure\",status=\"true\"}}"
            ),
        ),
        (
            "estate_ingress_5xx",
            "sum(rate(nginx_ingress_controller_requests{status=~\"5..\"}[5m]))".into(),
        ),
    ];
    let prometheus = futures::future::join_all(queries.into_iter().map(|(name,query)| async move {
        let params = vec![("query".into(),query),("start".into(),start.to_string()),("end".into(),end.to_string()),("step".into(),"60".into())];
        let result = bounded_monitoring_json(&config.integrations.prometheus,"/api/v1/query_range",&params).await.and_then(|v|range_summary(&v,start,end));
        match result { Ok(summary) => serde_json::json!({"query":name,"available":true,"summary":summary}), Err(_) => serde_json::json!({"query":name,"available":false,"gap":"Monitoring unavailable, missing series or invalid/oversized time-window evidence"}) }
    })).await;
    let params = vec![
        ("from".into(), (start * 1000).to_string()),
        ("to".into(), (end * 1000).to_string()),
        ("limit".into(), "100".into()),
        ("tags".into(), format!("host:{host}")),
    ];
    let grafana = match bounded_monitoring_json(
        &config.integrations.grafana,
        "/api/annotations",
        &params,
    )
    .await
    {
        Ok(value) if value.is_array() && value.as_array().unwrap().len() <= 100 => {
            let rows: Vec<_> = value.as_array().unwrap().iter().filter(|v|v["time"].as_i64().is_some_and(|t|t>=start*1000 && t<=end*1000) && v["tags"].as_array().is_some_and(|tags|tags.iter().any(|t|t.as_str()==Some(&format!("host:{host}")))))
                .map(|v|serde_json::json!({"id":v["id"],"time":v["time"],"text":safe_review_excerpt(v["text"].as_str().unwrap_or_default())})).collect();
            serde_json::json!({"available":!rows.is_empty(),"annotations":rows,"truncated":value.as_array().unwrap().len()==100,"gap":if rows.is_empty(){Some("No correlated host annotations; this does not prove an uneventful restart")}else{None}})
        }
        _ => {
            serde_json::json!({"available":false,"gap":"Grafana annotation evidence unavailable or invalid"})
        }
    };
    serde_json::json!({"window_start":start,"window_end":end,"prometheus":prometheus,"grafana":grafana,"review_complete":false,"root_cause_confirmed":false})
}

#[cfg(test)]
mod post_restart_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn workload_review_examines_only_correlated_lines_and_records_coverage_gaps() {
        let restart = 10_000_000;
        let report = restart_workload_findings(
            &json!({"error_count":1,"truncated":true,"items":[{"logs":
            "1970-01-01T02:46:40Z ERROR postgres connection pool password=private\n1970-01-01T00:00:01Z old kernel panic\nno_timestamp ERROR 503"}]}),
            restart,
            restart + 100_000,
        );
        assert_eq!(report["findings"]["examined"], 1);
        assert_eq!(
            report["findings"]["findings"][0]["excerpt"],
            "[sensitive log excerpt redacted]"
        );
        assert_eq!(report["timestamp_gaps"], 1);
        assert_eq!(report["truncated"], true);
        assert_eq!(report["review_complete"], false);
    }
    #[test]
    fn ssh_review_refuses_unpinned_hosts_and_command_injection() {
        let mut settings = crate::config::RestartReviewSshConfig {
            device_id: "qc01".into(),
            address: "192.168.1.61".into(),
            username: "pbisaacs".into(),
            identity_file: "/run/secrets/ssh-key".into(),
            known_hosts_file: "/etc/conductor/known_hosts".into(),
        };
        let device = json!({"id":"qc01","addresses":["192.168.1.61"]});
        let args = review_ssh_args(&settings, &device, 10_000_000).unwrap();
        assert!(args.iter().any(|a| a == "StrictHostKeyChecking=yes"));
        assert!(args.iter().any(|a| a == "ForwardAgent=no"));
        settings.address = "192.168.1.62".into();
        assert!(review_ssh_args(&settings, &device, 10_000_000).is_err());
        settings.address = "192.168.1.61".into();
        settings.username = "root; reboot".into();
        assert!(review_ssh_args(&settings, &device, 10_000_000).is_err());
    }
    #[test]
    fn monitoring_rejects_missing_nonfinite_and_unrelated_samples() {
        assert!(
            range_summary(
                &json!({"status":"success","data":{"resultType":"matrix","result":[]}}),
                100,
                200
            )
            .is_err()
        );
        for sample in [json!([150, "NaN"]), json!([99, "1"])] {
            assert!(range_summary(&json!({"status":"success","data":{"resultType":"matrix","result":[{"values":[sample]}]}}),100,200).is_err());
        }
        assert_eq!(range_summary(&json!({"status":"success","data":{"resultType":"matrix","result":[{"values":[[100,"1"],[160,"0"]]}]}}),100,200).unwrap()["minimum"],0.0);
    }
    #[tokio::test]
    async fn monitoring_review_collects_correlated_evidence_without_accepting_it_as_root_cause() {
        use axum::{Json, Router, extract::Query, routing::get};
        use std::collections::HashMap;
        let app = Router::new().route("/api/v1/query_range",get(|Query(query):Query<HashMap<String,String>>|async move {
            let at = query["start"].parse::<i64>().unwrap();
            Json(json!({"status":"success","data":{"resultType":"matrix","result":[{"values":[[at,"1"]]}]}}))
        })).route("/api/annotations",get(||async {Json(json!([]))}));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut config = crate::config::ConductorConfig::default();
        config.integrations.prometheus.enabled = true;
        config.integrations.prometheus.base_url = Some(base.clone());
        config.integrations.grafana.enabled = true;
        config.integrations.grafana.base_url = Some(base);
        let report = restart_monitoring_evidence(&config, "qc01", 10_000_000, 10_100_000).await;
        assert_eq!(report["prometheus"][0]["available"], true);
        assert_eq!(report["grafana"]["available"], false);
        assert_eq!(report["review_complete"], false);
        assert_eq!(report["root_cause_confirmed"], false);
        let malicious =
            restart_monitoring_evidence(&config, "qc01\"}", 10_000_000, 10_100_000).await;
        assert_eq!(malicious["available"], false);
        server.abort();
    }
    #[test]
    fn startup_review_correlates_faults_and_redacts_sensitive_log_excerpts() {
        let report = post_restart_log_findings(
            &[
                json!({"ts_ms":900_000,"message":"kernel OOM killed postgres","level":"error","category":"journal"}),
                json!({"ts_ms":900_001,"message":"postgres password=private","level":"error"}),
                json!({"ts_ms":-10_000_000,"message":"old kernel panic","level":"error"}),
            ],
            1_000_000,
            1_100_000,
        );
        assert_eq!(report["examined"], 2);
        assert_eq!(report["findings"].as_array().unwrap().len(), 2);
        assert_eq!(
            report["findings"][1]["excerpt"],
            "[sensitive log excerpt redacted]"
        );
        assert_eq!(report["root_cause_confirmed"], false);
    }
    #[test]
    fn missing_logs_or_quiet_root_page_do_not_close_recurrence_investigation() {
        let report = post_restart_log_findings(&[], 1_000_000, 1_100_000);
        assert_eq!(report["examined"], 0);
        assert_eq!(report["investigation_required"], true);
        assert_eq!(report["previous_boot_coverage_verified"], false);
    }
}
