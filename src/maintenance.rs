use std::collections::BTreeSet;

use chrono::{DateTime, Datelike, Duration, Utc, Weekday};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn parse_weekly_window(value: &str) -> Result<(Weekday, u32, u32), String> {
    let mut parts = value.split_whitespace();
    let day = parts
        .next()
        .ok_or_else(|| "expected `Weekday HH:MM`".to_string())?;
    let time = parts
        .next()
        .ok_or_else(|| "expected `Weekday HH:MM`".to_string())?;
    if parts.next().is_some() {
        return Err("expected exactly `Weekday HH:MM`".to_string());
    }
    let weekday = match day.to_ascii_lowercase().as_str() {
        "mon" | "monday" => Weekday::Mon,
        "tue" | "tues" | "tuesday" => Weekday::Tue,
        "wed" | "wednesday" => Weekday::Wed,
        "thu" | "thur" | "thurs" | "thursday" => Weekday::Thu,
        "fri" | "friday" => Weekday::Fri,
        "sat" | "saturday" => Weekday::Sat,
        "sun" | "sunday" => Weekday::Sun,
        _ => return Err("weekday must be Monday to Sunday".to_string()),
    };
    let (hour, minute) = time
        .split_once(':')
        .ok_or_else(|| "time must use 24-hour HH:MM format".to_string())?;
    if hour.len() != 2 || minute.len() != 2 {
        return Err("time must use 24-hour HH:MM format".to_string());
    }
    let hour = hour
        .parse::<u32>()
        .map_err(|_| "time must use 24-hour HH:MM format".to_string())?;
    let minute = minute
        .parse::<u32>()
        .map_err(|_| "time must use 24-hour HH:MM format".to_string())?;
    if hour > 23 || minute > 59 {
        return Err("time must use 24-hour HH:MM format".to_string());
    }
    Ok((weekday, hour, minute))
}

pub fn next_window_after(
    now: DateTime<Utc>,
    weekday: Weekday,
    hour: u32,
    minute: u32,
    minimum_notice_hours: u64,
) -> DateTime<Utc> {
    let earliest = now + Duration::hours(minimum_notice_hours.min(i64::MAX as u64) as i64);
    (0..=14)
        .filter_map(|days| {
            let date = (earliest + Duration::days(days)).date_naive();
            (date.weekday() == weekday)
                .then(|| {
                    date.and_hms_opt(hour, minute, 0)
                        .map(|value| value.and_utc())
                })
                .flatten()
        })
        .find(|candidate| *candidate >= earliest)
        .unwrap_or(earliest)
}

pub fn proposal_id(
    chassis_id: &str,
    affected_devices: &[String],
    issue_generation: &str,
    window_start: DateTime<Utc>,
) -> String {
    let mut devices = affected_devices.to_vec();
    devices.sort();
    devices.dedup();
    let material = format!(
        "{}:{}:{}:{}",
        chassis_id.trim().to_ascii_lowercase(),
        devices.join(","),
        issue_generation,
        window_start.to_rfc3339(),
    );
    let digest = Sha256::digest(material.as_bytes());
    let suffix = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{}-{suffix}", chassis_id.trim().to_ascii_lowercase())
}

pub fn has_critical_battery(assessment: &Value) -> bool {
    assessment
        .pointer("/evidence/critical_sensors")
        .and_then(Value::as_array)
        .is_some_and(|sensors| {
            sensors.iter().any(|sensor| {
                sensor
                    .as_str()
                    .is_some_and(|name| name.eq_ignore_ascii_case("SYS_V3_Battery"))
            })
        })
}

pub fn assessment_is_fresh(assessment: &Value, now_epoch_ms: i64, max_age_ms: i64) -> bool {
    assessment
        .get("assessed_at_unix_ms")
        .and_then(Value::as_i64)
        .is_some_and(|observed| {
            let age = now_epoch_ms.saturating_sub(observed);
            age >= -30_000 && age <= max_age_ms
        })
}

/// Reduce Continuum's inventory representation to operator-safe service
/// labels. Endpoint URLs and arbitrary inventory fields are never copied into
/// the maintenance dashboard or the persistent proposal event.
pub fn affected_service_labels(device: Option<&Value>) -> Vec<String> {
    let mut labels = BTreeSet::new();
    for key in ["affected_services", "services"] {
        let Some(items) = device
            .and_then(|item| item.get(key))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for item in items.iter().take(128) {
            let label = item.as_str().or_else(|| {
                ["id", "service_key", "name", "label"]
                    .iter()
                    .find_map(|key| item.get(*key).and_then(Value::as_str))
            });
            if let Some(label) = label.map(str::trim).filter(|label| !label.is_empty()) {
                labels.insert(label.to_string());
            }
        }
    }
    labels.into_iter().take(128).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn weekly_schedule_parses_british_weekday_and_utc_time() {
        assert_eq!(
            parse_weekly_window("Sunday 02:00").expect("valid slot"),
            (Weekday::Sun, 2, 0)
        );
        assert!(parse_weekly_window("Sunday 2:00").is_err());
        assert!(parse_weekly_window("Sunday 02:00 extra").is_err());
    }

    #[test]
    fn next_slot_respects_notice_and_does_not_return_a_past_weekly_slot() {
        let now = DateTime::parse_from_rfc3339("2026-10-12T08:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let next = next_window_after(now, Weekday::Sun, 2, 0, 168);
        assert_eq!(next.to_rfc3339(), "2026-10-25T02:00:00+00:00");
    }

    #[test]
    fn maintenance_issue_requires_fresh_critical_battery_evidence() {
        let assessment = json!({
            "assessed_at_unix_ms": 1_000_000,
            "evidence": {"critical_sensors": ["SYS_V3_Battery"]}
        });
        assert!(has_critical_battery(&assessment));
        assert!(assessment_is_fresh(&assessment, 1_060_000, 120_000));
        assert!(!assessment_is_fresh(&assessment, 1_200_001, 120_000));
        assert!(!has_critical_battery(
            &json!({"evidence":{"critical_sensors":[]}})
        ));
    }

    #[test]
    fn service_labels_omit_urls_and_are_deduplicated() {
        let device = json!({
            "affected_services": [
                {"id":"slurm","url":"https://private.invalid/health"},
                "aarnn-node"
            ],
            "services": [{"service_key":"slurm"}]
        });
        assert_eq!(
            affected_service_labels(Some(&device)),
            ["aarnn-node", "slurm"]
        );
    }

    #[test]
    fn proposal_id_is_stable_and_device_order_independent() {
        let start = DateTime::parse_from_rfc3339("2026-10-25T02:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let first = proposal_id(
            "centriq-qc04-qc05",
            &["qc04".into(), "qc05".into()],
            "episode-1",
            start,
        );
        let second = proposal_id(
            "centriq-qc04-qc05",
            &["qc05".into(), "qc04".into()],
            "episode-1",
            start,
        );
        assert_eq!(first, second);
        assert!(first.starts_with("centriq-qc04-qc05-"));
    }
}
