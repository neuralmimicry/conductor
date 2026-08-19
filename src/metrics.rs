use std::sync::atomic::{AtomicU64, Ordering};

/// Small dependency-free Prometheus registry for the workflow outcomes that
/// operators need even when the database-backed dashboard is unavailable.
#[derive(Default)]
pub struct ConductorMetrics {
    pub execution_cycles_success: AtomicU64,
    pub execution_cycles_failure: AtomicU64,
    pub events_persistence_success: AtomicU64,
    pub events_persistence_failure: AtomicU64,
}

pub static METRICS: std::sync::LazyLock<ConductorMetrics> =
    std::sync::LazyLock::new(ConductorMetrics::default);

pub fn record_execution_cycle(success: bool) {
    let counter = if success {
        &METRICS.execution_cycles_success
    } else {
        &METRICS.execution_cycles_failure
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn record_event_persistence(success: bool) {
    let counter = if success {
        &METRICS.events_persistence_success
    } else {
        &METRICS.events_persistence_failure
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn render_prometheus() -> String {
    let success = METRICS.execution_cycles_success.load(Ordering::Relaxed);
    let failure = METRICS.execution_cycles_failure.load(Ordering::Relaxed);
    let persisted = METRICS.events_persistence_success.load(Ordering::Relaxed);
    let persistence_failure = METRICS.events_persistence_failure.load(Ordering::Relaxed);

    format!(
        "# HELP conductor_execution_cycles_total Execution cycles by outcome.\n\
# TYPE conductor_execution_cycles_total counter\n\
conductor_execution_cycles_total{{outcome=\"success\"}} {success}\n\
conductor_execution_cycles_total{{outcome=\"failure\"}} {failure}\n\
# HELP conductor_event_persistence_total Conductor event persistence attempts by outcome.\n\
# TYPE conductor_event_persistence_total counter\n\
conductor_event_persistence_total{{outcome=\"success\"}} {persisted}\n\
conductor_event_persistence_total{{outcome=\"failure\"}} {persistence_failure}\n"
    )
}

#[cfg(test)]
mod tests {
    use super::render_prometheus;

    #[test]
    fn render_exposes_workflow_outcome_contract() {
        let output = render_prometheus();
        assert!(output.contains("conductor_execution_cycles_total{outcome=\"success\"}"));
        assert!(output.contains("conductor_event_persistence_total{outcome=\"failure\"}"));
    }
}
