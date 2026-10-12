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
    minimum_offline_seconds: u64,
    maximum_observation_age_seconds: u64,
) -> HostRecoveryAssessment {
    let mut blockers = Vec::new();
    let mut warnings = Vec::new();
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
            // A warm IPMI reset does not remove chassis power. Record the
            // degraded RTC battery, but never escalate this automatic path to
            // cold power-cycle while the sensor is critical.
            warnings.push(format!("critical BMC battery sensor: {sensor}"));
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
        let assessment = assess_host_recovery(&input, 3_600, 120);
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
        let assessment = assess_host_recovery(&input, 3_600, 120);
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
        let assessment = assess_host_recovery(&input, 3_600, 120);
        assert!(assessment.eligible, "{:?}", assessment.blockers);
        assert_eq!(assessment.warnings.len(), 1);
    }
}
