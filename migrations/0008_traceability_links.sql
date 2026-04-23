CREATE TABLE IF NOT EXISTS traceability_links (
    id UUID PRIMARY KEY,
    link_key TEXT NOT NULL UNIQUE,
    work_item_id UUID REFERENCES work_items(id) ON DELETE CASCADE,
    execution_id UUID REFERENCES work_executions(id) ON DELETE CASCADE,
    finding_key TEXT,
    system TEXT NOT NULL,
    reference_type TEXT NOT NULL,
    reference_key TEXT NOT NULL,
    title TEXT,
    status TEXT,
    url TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS traceability_links_work_item_idx
    ON traceability_links (work_item_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS traceability_links_execution_idx
    ON traceability_links (execution_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS traceability_links_finding_key_idx
    ON traceability_links (finding_key, updated_at DESC);

CREATE INDEX IF NOT EXISTS traceability_links_bucket_idx
    ON traceability_links (system, reference_type, updated_at DESC);
