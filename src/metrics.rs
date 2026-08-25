use std::sync::atomic::{AtomicU64, Ordering};

/// Small dependency-free Prometheus registry for the workflow outcomes that
/// operators need even when the database-backed dashboard is unavailable.
#[derive(Default)]
pub struct ConductorMetrics {
    pub execution_cycles_success: AtomicU64,
    pub execution_cycles_failure: AtomicU64,
    pub discovery_cycles_success: AtomicU64,
    pub discovery_cycles_failure: AtomicU64,
    pub planning_cycles_success: AtomicU64,
    pub planning_cycles_failure: AtomicU64,
    pub approval_cycles_success: AtomicU64,
    pub approval_cycles_failure: AtomicU64,
    pub execution_cycle_duration_ms_total: AtomicU64,
    pub execution_cycle_duration_ms_max: AtomicU64,
    pub discovery_cycle_duration_ms_total: AtomicU64,
    pub discovery_cycle_duration_ms_max: AtomicU64,
    pub planning_cycle_duration_ms_total: AtomicU64,
    pub planning_cycle_duration_ms_max: AtomicU64,
    pub work_queue_depth: AtomicU64,
    pub work_items_claimed_total: AtomicU64,
    pub work_item_claim_conflicts_total: AtomicU64,
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

fn record_cycle(
    success: bool,
    elapsed_ms: u64,
    success_counter: &AtomicU64,
    failure_counter: &AtomicU64,
    duration_total: &AtomicU64,
    duration_max: &AtomicU64,
) {
    let counter = if success {
        success_counter
    } else {
        failure_counter
    };
    counter.fetch_add(1, Ordering::Relaxed);
    duration_total.fetch_add(elapsed_ms, Ordering::Relaxed);
    let mut observed = duration_max.load(Ordering::Relaxed);
    while elapsed_ms > observed {
        match duration_max.compare_exchange_weak(
            observed,
            elapsed_ms,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(current) => observed = current,
        }
    }
}

pub fn record_discovery_cycle(success: bool, elapsed_ms: u64) {
    record_cycle(
        success,
        elapsed_ms,
        &METRICS.discovery_cycles_success,
        &METRICS.discovery_cycles_failure,
        &METRICS.discovery_cycle_duration_ms_total,
        &METRICS.discovery_cycle_duration_ms_max,
    );
}

pub fn record_planning_cycle(success: bool, elapsed_ms: u64) {
    record_cycle(
        success,
        elapsed_ms,
        &METRICS.planning_cycles_success,
        &METRICS.planning_cycles_failure,
        &METRICS.planning_cycle_duration_ms_total,
        &METRICS.planning_cycle_duration_ms_max,
    );
}

pub fn record_approval_cycle(success: bool) {
    let counter = if success {
        &METRICS.approval_cycles_success
    } else {
        &METRICS.approval_cycles_failure
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn record_execution_duration(elapsed_ms: u64) {
    METRICS
        .execution_cycle_duration_ms_total
        .fetch_add(elapsed_ms, Ordering::Relaxed);
    let mut observed = METRICS
        .execution_cycle_duration_ms_max
        .load(Ordering::Relaxed);
    while elapsed_ms > observed {
        match METRICS
            .execution_cycle_duration_ms_max
            .compare_exchange_weak(observed, elapsed_ms, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(current) => observed = current,
        }
    }
}

pub fn set_work_queue_depth(depth: usize) {
    METRICS
        .work_queue_depth
        .store(depth.min(u64::MAX as usize) as u64, Ordering::Relaxed);
}

pub fn record_claimed_work_items(count: usize) {
    METRICS
        .work_items_claimed_total
        .fetch_add(count.min(u64::MAX as usize) as u64, Ordering::Relaxed);
}

pub fn record_work_item_claim_conflict() {
    METRICS
        .work_item_claim_conflicts_total
        .fetch_add(1, Ordering::Relaxed);
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
    let discovery_success = METRICS.discovery_cycles_success.load(Ordering::Relaxed);
    let discovery_failure = METRICS.discovery_cycles_failure.load(Ordering::Relaxed);
    let planning_success = METRICS.planning_cycles_success.load(Ordering::Relaxed);
    let planning_failure = METRICS.planning_cycles_failure.load(Ordering::Relaxed);
    let approval_success = METRICS.approval_cycles_success.load(Ordering::Relaxed);
    let approval_failure = METRICS.approval_cycles_failure.load(Ordering::Relaxed);
    let execution_duration_total = METRICS
        .execution_cycle_duration_ms_total
        .load(Ordering::Relaxed);
    let execution_duration_max = METRICS
        .execution_cycle_duration_ms_max
        .load(Ordering::Relaxed);
    let discovery_duration_total = METRICS
        .discovery_cycle_duration_ms_total
        .load(Ordering::Relaxed);
    let discovery_duration_max = METRICS
        .discovery_cycle_duration_ms_max
        .load(Ordering::Relaxed);
    let planning_duration_total = METRICS
        .planning_cycle_duration_ms_total
        .load(Ordering::Relaxed);
    let planning_duration_max = METRICS
        .planning_cycle_duration_ms_max
        .load(Ordering::Relaxed);
    let queue_depth = METRICS.work_queue_depth.load(Ordering::Relaxed);
    let claimed = METRICS.work_items_claimed_total.load(Ordering::Relaxed);
    let claim_conflicts = METRICS
        .work_item_claim_conflicts_total
        .load(Ordering::Relaxed);

    format!(
        "# HELP conductor_execution_cycles_total Execution cycles by outcome.\n\
# TYPE conductor_execution_cycles_total counter\n\
conductor_execution_cycles_total{{outcome=\"success\"}} {success}\n\
conductor_execution_cycles_total{{outcome=\"failure\"}} {failure}\n\
# HELP conductor_event_persistence_total Conductor event persistence attempts by outcome.\n\
# TYPE conductor_event_persistence_total counter\n\
conductor_event_persistence_total{{outcome=\"success\"}} {persisted}\n\
conductor_event_persistence_total{{outcome=\"failure\"}} {persistence_failure}\n\
# HELP conductor_discovery_cycles_total Discovery cycles by outcome.\n\
# TYPE conductor_discovery_cycles_total counter\n\
conductor_discovery_cycles_total{{outcome=\"success\"}} {discovery_success}\n\
conductor_discovery_cycles_total{{outcome=\"failure\"}} {discovery_failure}\n\
# HELP conductor_planning_cycles_total Planning cycles by outcome.\n\
# TYPE conductor_planning_cycles_total counter\n\
conductor_planning_cycles_total{{outcome=\"success\"}} {planning_success}\n\
conductor_planning_cycles_total{{outcome=\"failure\"}} {planning_failure}\n\
# HELP conductor_approval_cycles_total Approval cycles by outcome.\n\
# TYPE conductor_approval_cycles_total counter\n\
conductor_approval_cycles_total{{outcome=\"success\"}} {approval_success}\n\
conductor_approval_cycles_total{{outcome=\"failure\"}} {approval_failure}\n\
# HELP conductor_cycle_duration_ms_total Sum of cycle durations in milliseconds.\n\
# TYPE conductor_cycle_duration_ms_total counter\n\
conductor_cycle_duration_ms_total{{cycle=\"discovery\"}} {discovery_duration_total}\n\
conductor_cycle_duration_ms_total{{cycle=\"planning\"}} {planning_duration_total}\n\
conductor_cycle_duration_ms_total{{cycle=\"execution\"}} {execution_duration_total}\n\
# HELP conductor_cycle_duration_ms_max Maximum observed cycle duration in milliseconds.\n\
# TYPE conductor_cycle_duration_ms_max gauge\n\
conductor_cycle_duration_ms_max{{cycle=\"discovery\"}} {discovery_duration_max}\n\
conductor_cycle_duration_ms_max{{cycle=\"planning\"}} {planning_duration_max}\n\
conductor_cycle_duration_ms_max{{cycle=\"execution\"}} {execution_duration_max}\n\
# HELP conductor_work_queue_depth Current number of persisted work items not in a terminal state.\n\
# TYPE conductor_work_queue_depth gauge\n\
conductor_work_queue_depth {queue_depth}\n\
# HELP conductor_work_items_claimed_total Work items claimed for execution.\n\
# TYPE conductor_work_items_claimed_total counter\n\
conductor_work_items_claimed_total {claimed}\n\
# HELP conductor_work_item_claim_conflicts_total Work-item claim conflicts observed.\n\
# TYPE conductor_work_item_claim_conflicts_total counter\n\
conductor_work_item_claim_conflicts_total {claim_conflicts}\n"
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
        assert!(output.contains("conductor_work_queue_depth 0"));
        assert!(output.contains("conductor_cycle_duration_ms_max{cycle=\"planning\"}"));
    }
}
