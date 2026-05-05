use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::models::{
    MetricTrend, ServiceMetricSample, ServiceSnapshot, ServiceTrendSummary, now_utc,
};

pub fn collect_metric_samples(
    discovery_run_id: Uuid,
    services: &[ServiceSnapshot],
) -> Vec<ServiceMetricSample> {
    services
        .iter()
        .filter_map(|service| {
            let metrics = service_metrics(service);
            if metrics.is_empty() {
                return None;
            }
            Some(ServiceMetricSample {
                id: uuid::Uuid::new_v4(),
                discovery_run_id,
                service_key: service.service_key.clone(),
                metric_source: "probe".to_string(),
                metrics: Value::Object(metrics),
                sampled_at: now_utc(),
            })
        })
        .collect()
}

pub fn summarize_trends(samples: &[ServiceMetricSample]) -> Vec<ServiceTrendSummary> {
    let mut grouped: BTreeMap<String, Vec<&ServiceMetricSample>> = BTreeMap::new();
    for sample in samples {
        grouped
            .entry(sample.service_key.clone())
            .or_default()
            .push(sample);
    }

    let mut summaries = Vec::new();
    for (service_key, mut group) in grouped {
        group.sort_by(|left, right| left.sampled_at.cmp(&right.sampled_at));
        let mut metrics_by_name: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        for sample in &group {
            if let Value::Object(map) = &sample.metrics {
                for (name, value) in map {
                    if let Some(number) = value.as_f64() {
                        metrics_by_name
                            .entry(name.clone())
                            .or_default()
                            .push(number);
                    }
                }
            }
        }

        let mut metrics = Vec::new();
        for (metric_name, values) in metrics_by_name {
            if values.is_empty() {
                continue;
            }
            let latest = *values.last().unwrap_or(&0.0);
            let first = *values.first().unwrap_or(&latest);
            let average = values.iter().sum::<f64>() / values.len() as f64;
            let slope = latest - first;
            metrics.push(MetricTrend {
                metric_name: metric_name.clone(),
                latest,
                average,
                slope,
                direction: trend_direction(metric_name.as_str(), slope),
            });
        }
        metrics.sort_by(|left, right| {
            right
                .slope
                .abs()
                .partial_cmp(&left.slope.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.metric_name.cmp(&right.metric_name))
        });

        let direction = overall_direction(&metrics);
        let headline = build_headline(&service_key, &metrics, &direction);
        summaries.push(ServiceTrendSummary {
            service_key,
            sample_count: group.len(),
            window_start: group.first().map(|sample| sample.sampled_at),
            window_end: group.last().map(|sample| sample.sampled_at),
            direction,
            headline,
            metrics,
            raw_latest: group
                .last()
                .map(|sample| sample.metrics.clone())
                .unwrap_or_else(|| json!({})),
        });
    }

    summaries.sort_by(|left, right| left.service_key.cmp(&right.service_key));
    summaries
}

pub fn pressure_score(value: &Value) -> f64 {
    match value {
        Value::Object(map) => map
            .iter()
            .map(|(key, value)| {
                let base = pressure_score(value);
                if key.contains("cpu")
                    || key.contains("memory")
                    || key.contains("pressure")
                    || key.contains("load")
                    || key.contains("latency")
                    || key.contains("queue")
                    || key.contains("risk")
                {
                    base.max(extract_number(value))
                } else {
                    base
                }
            })
            .fold(0.0, f64::max),
        Value::Array(items) => items.iter().map(pressure_score).fold(0.0, f64::max),
        _ => extract_number(value),
    }
}

pub fn normalize_signal(value: f64) -> f64 {
    if value > 1.0 {
        (value / 100.0).clamp(0.0, 1.0)
    } else {
        value.clamp(0.0, 1.0)
    }
}

pub fn extract_number(value: &Value) -> f64 {
    match value {
        Value::Number(number) => number.as_f64().map(normalize_signal).unwrap_or(0.0),
        Value::String(text) => text.parse::<f64>().map(normalize_signal).unwrap_or(0.0),
        _ => 0.0,
    }
}

fn service_metrics(service: &ServiceSnapshot) -> Map<String, Value> {
    match service.service_key.as_str() {
        "tracey" => tracey_metrics(service),
        "continuum" => continuum_metrics(service),
        "prometheus" => prometheus_metrics(service),
        "grafana" => grafana_metrics(service),
        "postgres" => postgres_metrics(service),
        "shared-storage" => shared_storage_metrics(service),
        _ => generic_metrics(service),
    }
}

fn tracey_metrics(service: &ServiceSnapshot) -> Map<String, Value> {
    let root = service
        .probe
        .get("metrics")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let status = root.get("status").cloned().unwrap_or_else(|| root.clone());
    let flattened = flatten_numeric_fields(&status);
    let mut metrics = generic_metrics(service);
    metrics.insert("pressure_score".to_string(), json!(pressure_score(&status)));
    insert_optional_metric(
        &mut metrics,
        "continuum_overall_score",
        pick_metric(&flattened, &["continuum_loop", "overall", "score"]),
    );
    insert_optional_metric(
        &mut metrics,
        "continuum_readiness_score",
        pick_metric(&flattened, &["continuum_loop", "readiness", "score"]),
    );
    insert_optional_metric(
        &mut metrics,
        "continuum_placement_score",
        pick_metric(&flattened, &["continuum_loop", "placement", "score"]),
    );
    insert_optional_metric(
        &mut metrics,
        "network_latency_pressure",
        pick_metric(&flattened, &["latency", "pressure"]),
    );
    insert_optional_metric(
        &mut metrics,
        "network_queue_pressure",
        pick_metric(&flattened, &["queue", "pressure"]),
    );
    insert_optional_metric(
        &mut metrics,
        "forecast_15m_pressure",
        pick_metric(&flattened, &["15m"]),
    );
    insert_optional_metric(
        &mut metrics,
        "query_failures",
        pick_metric(&flattened, &["query", "failure"])
            .or_else(|| pick_metric(&flattened, &["query", "failures"])),
    );
    metrics
}

fn continuum_metrics(service: &ServiceSnapshot) -> Map<String, Value> {
    let root = service
        .probe
        .get("metrics")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let adaptive = root
        .get("tracey_adaptive")
        .and_then(|value| value.get("data").cloned().or_else(|| Some(value.clone())))
        .unwrap_or_else(|| json!({}));
    let agents = root
        .get("tracey_agents")
        .and_then(|value| value.get("data").cloned().or_else(|| Some(value.clone())))
        .unwrap_or_else(|| json!({}));
    let adaptive_flattened = flatten_numeric_fields(&adaptive);
    let agents_flattened = flatten_numeric_fields(&agents);
    let mut metrics = generic_metrics(service);
    metrics.insert(
        "pressure_score".to_string(),
        json!(pressure_score(
            &json!({"adaptive": adaptive, "agents": agents})
        )),
    );
    insert_optional_metric(
        &mut metrics,
        "adaptive_overall_score",
        pick_metric(&adaptive_flattened, &["overall", "score"]),
    );
    insert_optional_metric(
        &mut metrics,
        "adaptive_readiness_score",
        pick_metric(&adaptive_flattened, &["readiness", "score"]),
    );
    insert_optional_metric(
        &mut metrics,
        "adaptive_placement_score",
        pick_metric(&adaptive_flattened, &["placement", "score"]),
    );
    insert_optional_metric(
        &mut metrics,
        "pressure_signal_count",
        pick_metric(&adaptive_flattened, &["pressure", "signal", "count"]),
    );
    insert_optional_metric(
        &mut metrics,
        "requested_remote_nodes",
        pick_metric(&adaptive_flattened, &["requested", "remote", "nodes"]),
    );
    insert_optional_metric(
        &mut metrics,
        "active_remote_nodes",
        pick_metric(&adaptive_flattened, &["active", "remote", "nodes"]),
    );
    insert_optional_metric(
        &mut metrics,
        "avg_network_latency_pressure",
        pick_metric(&agents_flattened, &["latency", "pressure"]),
    );
    insert_optional_metric(
        &mut metrics,
        "avg_network_queue_pressure",
        pick_metric(&agents_flattened, &["queue", "pressure"]),
    );
    insert_optional_metric(
        &mut metrics,
        "preferred_hosts",
        pick_metric(&adaptive_flattened, &["preferred", "host"]),
    );
    insert_optional_metric(
        &mut metrics,
        "preferred_gpus",
        pick_metric(&adaptive_flattened, &["preferred", "gpu"]),
    );
    metrics
}

fn prometheus_metrics(service: &ServiceSnapshot) -> Map<String, Value> {
    let root = service
        .probe
        .get("metrics")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let targets = root.get("targets").cloned().unwrap_or_else(|| json!({}));
    let active_targets_total = targets
        .get("active_targets_total")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let down_targets_total = targets
        .get("down_targets_total")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let jobs_with_failures = targets
        .get("jobs_with_failures")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let job_count = targets
        .get("jobs")
        .and_then(Value::as_array)
        .map(|jobs| jobs.len() as u64)
        .unwrap_or(0);
    let down_target_ratio = if active_targets_total > 0 {
        down_targets_total as f64 / active_targets_total as f64
    } else {
        0.0
    };
    let failing_job_ratio = if job_count > 0 {
        jobs_with_failures as f64 / job_count as f64
    } else {
        0.0
    };

    let mut metrics = generic_metrics(service);
    metrics.insert(
        "pressure_score".to_string(),
        json!(down_target_ratio.max(failing_job_ratio)),
    );
    metrics.insert("down_target_ratio".to_string(), json!(down_target_ratio));
    metrics.insert("failing_job_ratio".to_string(), json!(failing_job_ratio));
    metrics.insert(
        "active_target_count".to_string(),
        json!(normalize_signal(active_targets_total as f64)),
    );
    metrics
}

fn grafana_metrics(service: &ServiceSnapshot) -> Map<String, Value> {
    let root = service
        .probe
        .get("metrics")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let database_status_ok = root
        .get("database_status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("ok"));
    let datasource_count = root
        .get("datasource_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let dashboard_count = root
        .get("dashboard_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let coverage_gap = if datasource_count == 0 || dashboard_count == 0 {
        1.0
    } else {
        0.0
    };

    let mut metrics = generic_metrics(service);
    metrics.insert(
        "pressure_score".to_string(),
        json!(if database_status_ok {
            coverage_gap
        } else {
            1.0
        }),
    );
    metrics.insert(
        "database_status_severity".to_string(),
        json!(if database_status_ok { 0.0 } else { 1.0 }),
    );
    metrics.insert(
        "datasource_count".to_string(),
        json!(normalize_signal(datasource_count as f64)),
    );
    metrics.insert(
        "dashboard_count".to_string(),
        json!(normalize_signal(dashboard_count as f64)),
    );
    metrics
}

fn postgres_metrics(service: &ServiceSnapshot) -> Map<String, Value> {
    let root = service
        .probe
        .get("metrics")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let database = root.get("database").cloned().unwrap_or_else(|| json!({}));
    let connection_utilization = database
        .get("connection_utilization")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let waiting_connections = database
        .get("waiting_connections")
        .map(extract_number)
        .unwrap_or(0.0);
    let idle_in_transaction = database
        .get("idle_in_transaction")
        .map(extract_number)
        .unwrap_or(0.0);
    let deadlocks = database.get("deadlocks").map(extract_number).unwrap_or(0.0);

    let mut metrics = generic_metrics(service);
    metrics.insert(
        "pressure_score".to_string(),
        json!(
            connection_utilization
                .max(waiting_connections)
                .max(idle_in_transaction)
        ),
    );
    metrics.insert(
        "connection_utilization".to_string(),
        json!(connection_utilization),
    );
    metrics.insert(
        "waiting_connection_pressure".to_string(),
        json!(waiting_connections),
    );
    metrics.insert(
        "idle_in_transaction_pressure".to_string(),
        json!(idle_in_transaction),
    );
    metrics.insert("deadlock_pressure".to_string(), json!(deadlocks));
    metrics
}

fn shared_storage_metrics(service: &ServiceSnapshot) -> Map<String, Value> {
    let root = service
        .probe
        .get("metrics")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let filesystem = root.get("filesystem").cloned().unwrap_or_else(|| json!({}));
    let usage_ratio = filesystem
        .get("usage_ratio")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let inode_usage_ratio = filesystem
        .get("inode_usage_ratio")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let read_only = filesystem
        .get("read_only")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let expected_subdirectories = root
        .get("expected_subdirectories")
        .and_then(Value::as_array)
        .map(|items| items.len())
        .unwrap_or(0);
    let missing_subdirectories = root
        .get("missing_subdirectories")
        .and_then(Value::as_array)
        .map(|items| items.len())
        .unwrap_or(0);
    let missing_subdirectory_ratio = if expected_subdirectories > 0 {
        missing_subdirectories as f64 / expected_subdirectories as f64
    } else {
        0.0
    };

    let mut metrics = generic_metrics(service);
    metrics.insert(
        "pressure_score".to_string(),
        json!(
            usage_ratio
                .max(inode_usage_ratio)
                .max(missing_subdirectory_ratio)
                .max(if read_only { 1.0 } else { 0.0 })
        ),
    );
    metrics.insert("storage_usage_pressure".to_string(), json!(usage_ratio));
    metrics.insert("inode_usage_pressure".to_string(), json!(inode_usage_ratio));
    metrics.insert(
        "missing_subdirectory_ratio".to_string(),
        json!(missing_subdirectory_ratio),
    );
    metrics.insert(
        "read_only_severity".to_string(),
        json!(if read_only { 1.0 } else { 0.0 }),
    );
    metrics
}

fn generic_metrics(service: &ServiceSnapshot) -> Map<String, Value> {
    let mut metrics = Map::new();
    metrics.insert(
        "health_severity".to_string(),
        json!(service.health.severity() as f64 / 4.0),
    );
    metrics.insert(
        "dependency_count".to_string(),
        json!(normalize_signal(service.dependencies.len() as f64)),
    );
    metrics.insert(
        "capability_count".to_string(),
        json!(normalize_signal(service.capabilities.len() as f64)),
    );
    metrics
}

fn insert_optional_metric(metrics: &mut Map<String, Value>, name: &str, value: Option<f64>) {
    if let Some(value) = value {
        metrics.insert(name.to_string(), json!(value));
    }
}

fn flatten_numeric_fields(value: &Value) -> Vec<(String, f64)> {
    let mut output = Vec::new();
    flatten_numeric_value(String::new(), value, &mut output);
    output
}

fn flatten_numeric_value(prefix: String, value: &Value, output: &mut Vec<(String, f64)>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let next = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_numeric_value(next, value, output);
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let next = if prefix.is_empty() {
                    index.to_string()
                } else {
                    format!("{}.{}", prefix, index)
                };
                flatten_numeric_value(next, item, output);
            }
        }
        Value::Number(number) => {
            if let Some(value) = number.as_f64() {
                output.push((prefix, normalize_signal(value)));
            }
        }
        Value::String(text) => {
            if let Ok(value) = text.parse::<f64>() {
                output.push((prefix, normalize_signal(value)));
            }
        }
        _ => {}
    }
}

fn pick_metric(entries: &[(String, f64)], patterns: &[&str]) -> Option<f64> {
    entries
        .iter()
        .filter(|(path, _)| {
            let lowered = path.to_ascii_lowercase();
            patterns
                .iter()
                .all(|pattern| lowered.contains(&pattern.to_ascii_lowercase()))
        })
        .map(|(_, value)| *value)
        .reduce(f64::max)
}

fn metric_is_risk(metric_name: &str) -> bool {
    let lowered = metric_name.to_ascii_lowercase();
    lowered.contains("pressure")
        || lowered.contains("latency")
        || lowered.contains("queue")
        || lowered.contains("failure")
        || lowered.contains("error")
        || lowered.contains("risk")
        || lowered.contains("severity")
        || lowered.contains("requested")
}

fn trend_direction(metric_name: &str, slope: f64) -> String {
    if slope.abs() < 0.03 {
        return "stable".to_string();
    }
    let worsening = if metric_is_risk(metric_name) {
        slope > 0.0
    } else {
        slope < 0.0
    };
    if worsening {
        "worsening".to_string()
    } else {
        "improving".to_string()
    }
}

fn overall_direction(metrics: &[MetricTrend]) -> String {
    let score = metrics.iter().fold(0.0, |acc, metric| {
        let weighted = if metric_is_risk(metric.metric_name.as_str()) {
            metric.slope
        } else {
            -metric.slope
        };
        acc + weighted
    });
    if score > 0.08 {
        "worsening".to_string()
    } else if score < -0.08 {
        "improving".to_string()
    } else {
        "stable".to_string()
    }
}

fn build_headline(service_key: &str, metrics: &[MetricTrend], direction: &str) -> String {
    if let Some(metric) = metrics.first() {
        return format!(
            "{} trend is {} via {} ({:.2} latest, {:.2} slope).",
            service_key, direction, metric.metric_name, metric.latest, metric.slope
        );
    }
    format!("{} trend is {}.", service_key, direction)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_detects_worsening_tracey_pressure() {
        let base = now_utc();
        let samples = vec![
            ServiceMetricSample {
                id: uuid::Uuid::new_v4(),
                discovery_run_id: uuid::Uuid::new_v4(),
                service_key: "tracey".to_string(),
                metric_source: "probe".to_string(),
                metrics: json!({"pressure_score": 0.2, "continuum_overall_score": 0.8}),
                sampled_at: base,
            },
            ServiceMetricSample {
                id: uuid::Uuid::new_v4(),
                discovery_run_id: uuid::Uuid::new_v4(),
                service_key: "tracey".to_string(),
                metric_source: "probe".to_string(),
                metrics: json!({"pressure_score": 0.5, "continuum_overall_score": 0.7}),
                sampled_at: base + chrono::TimeDelta::minutes(5),
            },
            ServiceMetricSample {
                id: uuid::Uuid::new_v4(),
                discovery_run_id: uuid::Uuid::new_v4(),
                service_key: "tracey".to_string(),
                metric_source: "probe".to_string(),
                metrics: json!({"pressure_score": 0.8, "continuum_overall_score": 0.6}),
                sampled_at: base + chrono::TimeDelta::minutes(10),
            },
        ];

        let summaries = summarize_trends(&samples);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].direction, "worsening");
        assert!(summaries[0].headline.contains("pressure_score"));
    }

    #[test]
    fn pressure_score_picks_nested_pressure_signals() {
        let value = json!({
            "status": {
                "resource_forecast": {"queue_pressure": 72},
                "continuum_loop": {"overall_score": 0.81}
            }
        });
        assert!(pressure_score(&value) >= 0.72);
    }

    #[test]
    fn collects_shared_storage_pressure_metrics() {
        let service = ServiceSnapshot {
            service_key: "shared-storage".to_string(),
            display_name: "Shared Storage".to_string(),
            kind: "storage".to_string(),
            role_name: "qc01_shared_storage".to_string(),
            playbooks: vec![],
            host_targets: vec![],
            hosts: vec![],
            namespace: None,
            service_name: None,
            deployment_environment: None,
            internal_url: None,
            public_url: None,
            repo_path: None,
            repo_url: None,
            repo_branch: None,
            health: crate::models::ServiceHealth::Healthy,
            capabilities: vec!["persistent_storage".to_string()],
            dependencies: vec![],
            storage_paths: vec!["/home/continuum-shared-storage".to_string()],
            raw_defaults: json!({}),
            probe: json!({
                "metrics": {
                    "filesystem": {
                        "usage_ratio": 0.87,
                        "inode_usage_ratio": 0.12,
                        "read_only": false
                    },
                    "expected_subdirectories": ["postgres", "prometheus"],
                    "missing_subdirectories": ["postgres"]
                }
            }),
            discovered_at: now_utc(),
            updated_at: now_utc(),
        };

        let metrics = collect_metric_samples(uuid::Uuid::new_v4(), &[service]);
        let payload = metrics
            .first()
            .and_then(|sample| sample.metrics.get("storage_usage_pressure"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);

        assert!(payload >= 0.87);
    }
}
