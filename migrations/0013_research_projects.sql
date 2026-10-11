CREATE TABLE IF NOT EXISTS research_projects (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    visibility TEXT NOT NULL,
    domain TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS research_projects_publication_idx
    ON research_projects (status, visibility, updated_at DESC);

CREATE INDEX IF NOT EXISTS research_projects_domain_idx
    ON research_projects (domain, updated_at DESC);
