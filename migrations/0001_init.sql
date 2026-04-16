CREATE TABLE IF NOT EXISTS service_snapshots (
    service_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    role_name TEXT NOT NULL,
    playbooks JSONB NOT NULL,
    hosts JSONB NOT NULL,
    namespace TEXT,
    service_name TEXT,
    internal_url TEXT,
    public_url TEXT,
    repo_path TEXT,
    health TEXT NOT NULL,
    capabilities JSONB NOT NULL,
    dependencies JSONB NOT NULL,
    storage_paths JSONB NOT NULL,
    raw_defaults JSONB NOT NULL,
    probe JSONB NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS discovery_runs (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    services_count INTEGER NOT NULL,
    issues JSONB NOT NULL,
    topology JSONB NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS work_items (
    id UUID PRIMARY KEY,
    dedupe_key TEXT UNIQUE,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    target_service TEXT,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL,
    progress_pct INTEGER NOT NULL DEFAULT 0,
    admin_override BOOLEAN NOT NULL DEFAULT FALSE,
    source TEXT NOT NULL,
    tags JSONB NOT NULL,
    plan JSONB NOT NULL,
    notes JSONB NOT NULL,
    scheduled_for TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS work_items_priority_idx
    ON work_items (priority DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS improvement_cycles (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    summary TEXT NOT NULL,
    source_services JSONB NOT NULL,
    recommendations JSONB NOT NULL,
    gail_response JSONB,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL
);
