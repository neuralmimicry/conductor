ALTER TABLE service_snapshots
    ADD COLUMN IF NOT EXISTS deployment_environment TEXT;

ALTER TABLE work_items
    ADD COLUMN IF NOT EXISTS delivery_stage TEXT NOT NULL DEFAULT 'development',
    ADD COLUMN IF NOT EXISTS validated_stages JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS rollout_strategy TEXT NOT NULL DEFAULT 'direct';

CREATE INDEX IF NOT EXISTS work_items_delivery_stage_idx
    ON work_items (delivery_stage, status, priority DESC, updated_at DESC);

ALTER TABLE work_executions
    ADD COLUMN IF NOT EXISTS delivery_stage TEXT NOT NULL DEFAULT 'development',
    ADD COLUMN IF NOT EXISTS rollout_strategy TEXT NOT NULL DEFAULT 'direct';

CREATE INDEX IF NOT EXISTS work_executions_delivery_stage_idx
    ON work_executions (delivery_stage, status, updated_at DESC);
