ALTER TABLE work_items
    ADD COLUMN IF NOT EXISTS approval_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
