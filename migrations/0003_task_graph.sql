ALTER TABLE work_items
    ADD COLUMN IF NOT EXISTS depends_on JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS work_items_depends_on_gin_idx
    ON work_items USING GIN (depends_on);
