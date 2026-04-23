ALTER TABLE service_snapshots
    ADD COLUMN IF NOT EXISTS host_targets JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE discovery_runs
    ADD COLUMN IF NOT EXISTS repositories_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS repository_snapshots (
    repo_key TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    owner TEXT,
    repo_url TEXT,
    local_path TEXT,
    default_branch TEXT,
    current_branch TEXT,
    language TEXT,
    frameworks JSONB NOT NULL,
    build_systems JSONB NOT NULL,
    package_managers JSONB NOT NULL,
    runtime_type TEXT,
    deployment_type TEXT,
    purpose TEXT,
    criticality TEXT NOT NULL,
    visibility TEXT,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    linked_services JSONB NOT NULL,
    dependencies JSONB NOT NULL,
    capabilities JSONB NOT NULL,
    inventory_sources JSONB NOT NULL,
    metadata JSONB NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS repository_snapshots_owner_name_idx
    ON repository_snapshots (owner, name);

CREATE INDEX IF NOT EXISTS repository_snapshots_criticality_idx
    ON repository_snapshots (criticality, updated_at DESC);
