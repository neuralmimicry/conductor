-- Planner and dashboard history queries request the newest samples across all
-- services.  The existing service_key/sampled_at index cannot satisfy that
-- global ordering, so PostgreSQL sorts the full telemetry table on every
-- refresh and can stall the single Conductor process under the 1 GiB limit.
CREATE INDEX IF NOT EXISTS service_metric_samples_sampled_at_idx
    ON service_metric_samples (sampled_at DESC);
