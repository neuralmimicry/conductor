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
}
