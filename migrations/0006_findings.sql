CREATE TABLE IF NOT EXISTS findings (
    id UUID PRIMARY KEY,
    finding_key TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    category TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    target_service TEXT,
    target_repository TEXT,
    source_run_id UUID,
    confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    tags JSONB NOT NULL,
    details JSONB NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS findings_severity_idx
    ON findings (severity, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS findings_target_idx
    ON findings (target_service, target_repository);

CREATE TABLE IF NOT EXISTS finding_evidence (
    id UUID PRIMARY KEY,
    finding_id UUID NOT NULL REFERENCES findings(id) ON DELETE CASCADE,
    evidence_type TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    summary TEXT NOT NULL,
    payload JSONB NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS finding_evidence_finding_idx
    ON finding_evidence (finding_id, collected_at DESC);

CREATE TABLE IF NOT EXISTS finding_provenance (
    id UUID PRIMARY KEY,
    finding_id UUID NOT NULL REFERENCES findings(id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    origin TEXT NOT NULL,
    component TEXT NOT NULL,
    detail TEXT NOT NULL,
    confidence_score DOUBLE PRECISION,
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS finding_provenance_finding_idx
    ON finding_provenance (finding_id, recorded_at DESC);
