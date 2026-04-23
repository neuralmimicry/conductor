ALTER TABLE work_items
    ADD COLUMN IF NOT EXISTS claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS claim_token UUID;

CREATE INDEX IF NOT EXISTS work_items_execution_claim_idx
    ON work_items (status, execution_approved, scheduled_for, claim_expires_at, priority DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS conductor_events (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    status TEXT,
    work_item_id UUID REFERENCES work_items(id) ON DELETE SET NULL,
    execution_id UUID REFERENCES work_executions(id) ON DELETE SET NULL,
    refiner_job_id TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS conductor_events_created_at_idx
    ON conductor_events (created_at DESC);

CREATE INDEX IF NOT EXISTS conductor_events_work_item_id_idx
    ON conductor_events (work_item_id, created_at DESC);

CREATE INDEX IF NOT EXISTS conductor_events_execution_id_idx
    ON conductor_events (execution_id, created_at DESC);
