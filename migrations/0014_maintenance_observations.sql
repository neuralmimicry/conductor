CREATE TABLE IF NOT EXISTS maintenance_chassis_observations (
    chassis_id TEXT PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS maintenance_chassis_observations_observed_at_idx
    ON maintenance_chassis_observations (observed_at DESC);
