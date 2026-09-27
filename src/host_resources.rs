use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::models::ServiceSnapshot;

const MISSING_HOST_RETENTION_DAYS: i64 = 7;

/// Build stable per-node resource records from the Kubernetes core Node API.
/// Volatile fields such as resourceVersion, readiness transition times, and
/// node addresses are deliberately excluded from the fingerprint.
pub fn node_snapshots_from_api(payload: &Value, observed_at: DateTime<Utc>) -> Option<Vec<Value>> {
    let items = payload.get("items")?.as_array()?;
    let observed_at = observed_at.to_rfc3339();
    let mut hosts = items
        .iter()
        .filter_map(|node| node_snapshot(node, &observed_at))
        .collect::<Vec<_>>();
    hosts.sort_by(|left, right| host_sort_key(left, right));
    Some(hosts)
}

fn node_snapshot(node: &Value, observed_at: &str) -> Option<Value> {
    let metadata = node.get("metadata")?;
    let host = metadata.get("name")?.as_str()?.trim();
    if host.is_empty() {
        return None;
    }

    let status = node.get("status").cloned().unwrap_or_else(|| json!({}));
    let resource_profile = resource_profile(&metadata["labels"], &status);
    let encoded = serde_json::to_vec(&resource_profile).ok()?;
    let fingerprint = sha256_hex(&encoded);
    let conditions = selected_conditions(&status["conditions"]);
    let ready = conditions
        .get("Ready")
        .and_then(Value::as_str)
        .is_some_and(|value| value == "True");

    Some(json!({
        "host": host,
        "node_uid": metadata.get("uid"),
        "resource_version": metadata.get("resourceVersion"),
        "fingerprint": fingerprint,
        "resource_profile": resource_profile,
        "conditions": conditions,
        "ready": ready,
        "presence": "present",
        "first_seen_at": observed_at,
        "last_seen_at": observed_at,
        "observed_at": observed_at,
    }))
}

fn resource_profile(labels: &Value, status: &Value) -> Value {
    let mut selected_labels = Map::new();
    if let Some(labels) = labels.as_object() {
        for (name, value) in labels {
            let stable_label = matches!(
                name.as_str(),
                "kubernetes.io/arch"
                    | "kubernetes.io/os"
                    | "kubernetes.io/hostname"
                    | "node.kubernetes.io/instance-type"
                    | "topology.kubernetes.io/region"
                    | "topology.kubernetes.io/zone"
            ) || name.starts_with("nvidia.com/")
                || name.starts_with("amd.com/")
                || name.starts_with("gpu.intel.com/")
                || name.starts_with("feature.node.kubernetes.io/pci-")
                || name.starts_with("feature.node.kubernetes.io/cpu-")
                || name.starts_with("feature.node.kubernetes.io/memory-");
            if stable_label {
                selected_labels.insert(name.clone(), value.clone());
            }
        }
    }

    let mut node_info = Map::new();
    for field in [
        "architecture",
        "operatingSystem",
        "osImage",
        "kernelVersion",
        "containerRuntimeVersion",
        "kubeletVersion",
        "machineID",
        "systemUUID",
    ] {
        if let Some(value) = status
            .get("nodeInfo")
            .and_then(|info| info.get(field))
            .filter(|value| !value.is_null())
        {
            node_info.insert(field.to_string(), value.clone());
        }
    }

    json!({
        "capacity": status.get("capacity").cloned().unwrap_or_else(|| json!({})),
        "allocatable": status.get("allocatable").cloned().unwrap_or_else(|| json!({})),
        "labels": selected_labels,
        "node_info": node_info,
    })
}

fn selected_conditions(value: &Value) -> Map<String, Value> {
    const CONDITION_TYPES: &[&str] = &[
        "Ready",
        "MemoryPressure",
        "DiskPressure",
        "PIDPressure",
        "NetworkUnavailable",
    ];
    let mut conditions = Map::new();
    if let Some(entries) = value.as_array() {
        for condition in entries {
            let Some(kind) = condition.get("type").and_then(Value::as_str) else {
                continue;
            };
            if CONDITION_TYPES.contains(&kind) {
                if let Some(state) = condition.get("status") {
                    conditions.insert(kind.to_string(), state.clone());
                }
            }
        }
    }
    conditions
}

fn sha256_hex(input: &[u8]) -> String {
    let digest = Sha256::digest(input);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

/// Compare the latest successful node list with the last persisted snapshot.
/// Unavailable API reads preserve the previous baseline and never manufacture
/// missing-node findings. Missing nodes remain visible for a bounded period,
/// and a returning node produces an explicit reintegration event.
pub fn reconcile_inventory(previous: Option<&Value>, current: &mut Value, now: DateTime<Utc>) {
    let status = current
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let previous_hosts = previous
        .and_then(|inventory| inventory.get("hosts"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    if status != "available" {
        if let Some(object) = current.as_object_mut() {
            object.insert("hosts".to_string(), json!(previous_hosts));
            if let Some(last_successful) = previous
                .and_then(|inventory| inventory.get("last_successful_observed_at"))
                .or_else(|| previous.and_then(|inventory| inventory.get("observed_at")))
            {
                object.insert(
                    "last_successful_observed_at".to_string(),
                    last_successful.clone(),
                );
            }
            object.insert("events".to_string(), json!([]));
        }
        return;
    }

    let observed_at = current
        .get("observed_at")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| now.to_rfc3339());
    let mut current_hosts = current
        .get("hosts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    current_hosts.sort_by(host_sort_key);

    let previous_by_host = previous_hosts
        .into_iter()
        .filter_map(|host| {
            let name = host.get("host").and_then(Value::as_str)?.to_string();
            Some((name, host))
        })
        .collect::<BTreeMap<_, _>>();
    let current_names = current_hosts
        .iter()
        .filter_map(|host| host.get("host").and_then(Value::as_str))
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();

    let mut events = Vec::new();
    for host in &mut current_hosts {
        let Some(name) = host
            .get("host")
            .and_then(Value::as_str)
            .map(ToString::to_string)
        else {
            continue;
        };
        let Some(previous_host) = previous_by_host.get(&name) else {
            continue;
        };

        let previous_fingerprint = previous_host
            .get("fingerprint")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let current_fingerprint = host
            .get("fingerprint")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let previous_ready = previous_host
            .get("ready")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let current_ready = host.get("ready").and_then(Value::as_bool).unwrap_or(false);
        let was_missing = previous_host.get("presence").and_then(Value::as_str) == Some("missing");
        let identity_changed = previous_host.get("node_uid") != host.get("node_uid");
        let resource_changed = previous_fingerprint != current_fingerprint;
        let recovered = was_missing || (!previous_ready && current_ready) || identity_changed;

        if recovered || resource_changed {
            events.push(json!({
                "host": name,
                "type": if recovered { "host_reintegrated" } else { "resource_drift" },
                "previous_fingerprint": previous_fingerprint,
                "current_fingerprint": current_fingerprint,
                "previous_ready": previous_ready,
                "current_ready": current_ready,
                "identity_changed": identity_changed,
                "resource_changed": resource_changed,
                "resource_profile": host.get("resource_profile"),
                "node_uid": host.get("node_uid"),
                "resource_version": host.get("resource_version"),
                "source": "kubernetes_nodes_api",
                "observed_at": observed_at,
            }));
        }

        if let Some(object) = host.as_object_mut() {
            object.insert(
                "first_seen_at".to_string(),
                previous_host
                    .get("first_seen_at")
                    .cloned()
                    .unwrap_or_else(|| json!(observed_at)),
            );
            object.insert("last_seen_at".to_string(), json!(observed_at));
        }
    }

    let retain_after = now - Duration::days(MISSING_HOST_RETENTION_DAYS);
    for (name, previous_host) in &previous_by_host {
        if current_names.contains(name) {
            continue;
        }
        let missing_since = previous_host
            .get("missing_since")
            .and_then(Value::as_str)
            .or_else(|| previous_host.get("last_seen_at").and_then(Value::as_str))
            .unwrap_or_default();
        let still_retain = DateTime::parse_from_rfc3339(missing_since)
            .map(|timestamp| timestamp.with_timezone(&Utc) >= retain_after)
            .unwrap_or(true);
        if !still_retain {
            continue;
        }

        let first_missing =
            previous_host.get("presence").and_then(Value::as_str) != Some("missing");
        let mut missing_host = previous_host.clone();
        if let Some(object) = missing_host.as_object_mut() {
            object.insert("presence".to_string(), json!("missing"));
            object.insert("ready".to_string(), json!(false));
            object.insert(
                "missing_since".to_string(),
                previous_host
                    .get("missing_since")
                    .cloned()
                    .unwrap_or_else(|| json!(observed_at)),
            );
        }
        current_hosts.push(missing_host.clone());
        events.push(json!({
            "host": name,
            "type": "host_missing",
            "previous_fingerprint": previous_host.get("fingerprint"),
            "current_fingerprint": Value::Null,
            "previous_ready": previous_host.get("ready"),
            "current_ready": false,
            "resource_changed": false,
            "first_missing_observation": first_missing,
            "resource_profile": previous_host.get("resource_profile"),
            "node_uid": previous_host.get("node_uid"),
            "resource_version": previous_host.get("resource_version"),
            "source": "kubernetes_nodes_api",
            "observed_at": observed_at,
        }));
    }

    current_hosts.sort_by(host_sort_key);
    events.sort_by(|left, right| {
        left.get("host")
            .and_then(Value::as_str)
            .cmp(&right.get("host").and_then(Value::as_str))
    });
    if let Some(object) = current.as_object_mut() {
        object.insert("hosts".to_string(), json!(current_hosts));
        object.insert("events".to_string(), json!(events));
        object.insert(
            "last_successful_observed_at".to_string(),
            json!(observed_at),
        );
    }
}

fn host_sort_key(left: &Value, right: &Value) -> std::cmp::Ordering {
    left.get("host")
        .and_then(Value::as_str)
        .cmp(&right.get("host").and_then(Value::as_str))
}

pub fn reconcile_service_host_resources(
    previous: &[ServiceSnapshot],
    current: &mut [ServiceSnapshot],
) {
    let previous_inventory = previous
        .iter()
        .find(|service| service.service_key == "swarmhpc")
        .and_then(|service| service.probe.pointer("/metrics/host_resource_inventory"));
    let Some(service) = current
        .iter_mut()
        .find(|service| service.service_key == "swarmhpc")
    else {
        return;
    };
    let Some(inventory) = service
        .probe
        .pointer_mut("/metrics/host_resource_inventory")
    else {
        return;
    };
    reconcile_inventory(previous_inventory, inventory, Utc::now());
}

#[cfg(test)]
mod tests {
    use super::{node_snapshots_from_api, reconcile_inventory};
    use chrono::{Duration, Utc};
    use serde_json::{Value, json};

    fn node(memory: &str, gpu_count: &str, ready: &str, resource_version: &str) -> Value {
        json!({
            "metadata": {
                "name": "qc01",
                "uid": "node-uid",
                "resourceVersion": resource_version,
                "labels": {
                    "kubernetes.io/hostname": "qc01",
                    "kubernetes.io/arch": "amd64",
                    "nvidia.com/gpu.product": "NVIDIA-A10"
                }
            },
            "status": {
                "capacity": {"cpu": "32", "memory": memory, "nvidia.com/gpu": gpu_count},
                "allocatable": {"cpu": "30", "memory": memory, "nvidia.com/gpu": gpu_count},
                "nodeInfo": {"architecture": "amd64", "operatingSystem": "linux", "kernelVersion": "6.8"},
                "conditions": [
                    {"type": "Ready", "status": ready, "lastTransitionTime": resource_version},
                    {"type": "DiskPressure", "status": "False"}
                ]
            }
        })
    }

    fn inventory(status: &str, hosts: Vec<Value>, observed_at: &str) -> Value {
        json!({"status": status, "observed_at": observed_at, "hosts": hosts, "events": []})
    }

    #[test]
    fn fingerprint_ignores_readiness_and_resource_version_but_tracks_capacity_and_gpu() {
        let now = Utc::now();
        let first =
            node_snapshots_from_api(&json!({"items": [node("128Gi", "2", "True", "1")]}), now)
                .unwrap();
        let unchanged =
            node_snapshots_from_api(&json!({"items": [node("128Gi", "2", "False", "2")]}), now)
                .unwrap();
        let changed =
            node_snapshots_from_api(&json!({"items": [node("256Gi", "4", "True", "3")]}), now)
                .unwrap();
        assert_eq!(first[0]["fingerprint"], unchanged[0]["fingerprint"]);
        assert_ne!(first[0]["fingerprint"], changed[0]["fingerprint"]);
    }

    #[test]
    fn unavailable_inventory_preserves_baseline_without_reporting_false_loss() {
        let now = Utc::now();
        let previous = inventory(
            "available",
            vec![json!({"host":"qc01","fingerprint":"abc","last_seen_at":now.to_rfc3339()})],
            &now.to_rfc3339(),
        );
        let mut current = inventory("unavailable", Vec::new(), &now.to_rfc3339());
        reconcile_inventory(Some(&previous), &mut current, now + Duration::minutes(5));
        assert_eq!(current["hosts"].as_array().unwrap().len(), 1);
        assert!(current["events"].as_array().unwrap().is_empty());
    }

    #[test]
    fn changed_capacity_creates_drift_and_missing_host_stays_recoverable() {
        let now = Utc::now();
        let old =
            node_snapshots_from_api(&json!({"items": [node("128Gi", "2", "True", "1")]}), now)
                .unwrap();
        let mut changed = inventory(
            "available",
            node_snapshots_from_api(&json!({"items": [node("256Gi", "4", "True", "2")]}), now)
                .unwrap(),
            &now.to_rfc3339(),
        );
        reconcile_inventory(
            Some(&inventory("available", old, &now.to_rfc3339())),
            &mut changed,
            now,
        );
        assert_eq!(changed["events"][0]["type"], "resource_drift");

        let previous = changed.clone();
        let mut missing = inventory(
            "available",
            Vec::new(),
            &(now + Duration::minutes(1)).to_rfc3339(),
        );
        reconcile_inventory(Some(&previous), &mut missing, now + Duration::minutes(1));
        assert_eq!(missing["events"][0]["type"], "host_missing");
        assert_eq!(missing["hosts"][0]["presence"], "missing");

        let mut recovered = inventory(
            "available",
            node_snapshots_from_api(
                &json!({"items": [node("512Gi", "4", "True", "3")]}),
                now + Duration::minutes(2),
            )
            .unwrap(),
            &(now + Duration::minutes(2)).to_rfc3339(),
        );
        reconcile_inventory(Some(&missing), &mut recovered, now + Duration::minutes(2));
        assert_eq!(recovered["events"][0]["type"], "host_reintegrated");
        assert_eq!(recovered["events"][0]["resource_changed"], true);
    }

    #[test]
    fn replacing_a_node_with_the_same_name_is_reported_as_reintegration() {
        let now = Utc::now();
        let old =
            node_snapshots_from_api(&json!({"items": [node("128Gi", "2", "True", "1")]}), now)
                .unwrap();
        let mut replacement_payload = json!({
            "items": [node("128Gi", "2", "True", "2")]
        });
        replacement_payload["items"][0]["metadata"]["uid"] = json!("replacement-node-uid");
        let mut replacement = inventory(
            "available",
            node_snapshots_from_api(&replacement_payload, now + Duration::minutes(1)).unwrap(),
            &(now + Duration::minutes(1)).to_rfc3339(),
        );
        reconcile_inventory(
            Some(&inventory("available", old, &now.to_rfc3339())),
            &mut replacement,
            now + Duration::minutes(1),
        );
        assert_eq!(replacement["events"][0]["type"], "host_reintegrated");
        assert_eq!(replacement["events"][0]["identity_changed"], true);
        assert_eq!(replacement["events"][0]["resource_changed"], false);
    }
}
