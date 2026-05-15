CREATE INDEX IF NOT EXISTS discovery_runs_finished_at_idx
    ON discovery_runs (finished_at DESC);

CREATE INDEX IF NOT EXISTS improvement_cycles_finished_at_idx
    ON improvement_cycles (finished_at DESC);
