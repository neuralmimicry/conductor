ALTER TABLE service_snapshots
    ADD COLUMN IF NOT EXISTS repo_url TEXT,
    ADD COLUMN IF NOT EXISTS repo_branch TEXT;

ALTER TABLE work_items
    ADD COLUMN IF NOT EXISTS execution_approved BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS verification_required BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS last_execution_id UUID,
    ADD COLUMN IF NOT EXISTS last_policy JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS work_items_execution_queue_idx
    ON work_items (execution_approved, status, priority DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS service_metric_samples (
    id UUID PRIMARY KEY,
    discovery_run_id UUID NOT NULL REFERENCES discovery_runs(id) ON DELETE CASCADE,
    service_key TEXT NOT NULL,
    metric_source TEXT NOT NULL,
    metrics JSONB NOT NULL,
    sampled_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS service_metric_samples_service_key_sampled_at_idx
    ON service_metric_samples (service_key, sampled_at DESC);

CREATE INDEX IF NOT EXISTS service_metric_samples_discovery_run_idx
    ON service_metric_samples (discovery_run_id);

CREATE TABLE IF NOT EXISTS work_executions (
    id UUID PRIMARY KEY,
    work_item_id UUID NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    target_service TEXT,
    status TEXT NOT NULL,
    refiner_job_id TEXT,
    policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    latest_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    verification JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS work_executions_work_item_id_updated_at_idx
    ON work_executions (work_item_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS work_executions_status_updated_at_idx
    ON work_executions (status, updated_at DESC);
