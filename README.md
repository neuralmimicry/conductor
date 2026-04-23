# Conductor

Rust control-plane service for the NeuralMimicry stack. Conductor discovers the deployed topology from the SwarmHPC Ansible playbooks, scans the mounted NeuralMimicry repositories, optionally enriches that inventory from the GitHub organisation, probes the live service surfaces for Gail, Tracey, Continuum, Refiner, and AARNN, persists state in Postgres, and drives an improvement queue through an admin dashboard and API.

## What It Does

- Parses the SwarmHPC Ansible tree, including inventory groups, `group_vars`, `host_vars`, and tenant playbooks, to infer the deployed topology.
- Builds a first-class repository inventory from the mounted local estate under `/home/pbisaacs/Developer/neuralmimicry` and optional GitHub organisation metadata.
- Resolves repository URLs and branches from Ansible defaults, local git metadata, and explicit repo hints for cross-checking.
- Probes live endpoints to classify health, capture surfaced capabilities, and persist snapshots.
- Infers deployment environments from the existing SwarmHPC tenant environment variables where they are present.
- Derives typed findings, evidence, and provenance records from repository inventory, service topology, runtime probes, and trend summaries.
- Runs an improvement-planning loop that turns evidence-backed findings into graph-aware work items.
- Seeds every new work item into an explicit staged delivery pipeline: `development`, `testing`, `integration`, `integration_testing`, `uat`, then `production`.
- Uses Gail as an optional planning advisor and stores its response alongside each planning cycle.
- Carries rollout strategy metadata on work items and executions so release promotion stays explicit and auditable.
- Runs bounded project-native verification commands after Refiner completes and records missing-toolchain cases as explicit `unavailable` validation outcomes.
- Exposes a work-item traceability view that joins findings, evidence, provenance, executions, and the latest validation state.
- Exposes a dashboard for queue visibility, progress updates, approvals, reprioritisation, scheduling, admin overrides, and manual discovery/planning/execution runs.
- Computes DORA deployment metrics from persisted production-stage execution history and exposes them through the summary API and dashboard.
- Stores service snapshots, repository snapshots, typed findings, evidence, provenance, discovery runs, metric samples, work items, work executions, planning cycles, and persistent conductor events in Postgres.

## Current Boundaries

- Conductor now executes approved, scheduled, dependency-satisfied work items through Refiner's planning and job APIs.
- Execution selection now respects `scheduled_for`, uses DB-backed claims to avoid duplicate dispatch, and supports `dry_run` and `emergency_stop` controls.
- Successful stage verification can auto-promote the work item to the next delivery stage.
- Independent validation now executes discovered repository-native checks when a local repo path and suitable tooling are available.
- Production execution is policy-blocked unless the rollout strategy is `canary` or `red_green`.
- Conductor still does not mutate repositories directly; Refiner remains the only code-change executor.
- Gail is used as an advisory planner, not as the sole orchestration source of truth.
- AARNN is treated as a self-improvement target and telemetry source; Conductor does not yet build or retrain AARNN topologies automatically.
- Probe coverage is implemented for the confirmed endpoints in the local repositories and may need extending as upstream APIs evolve.

## Quick Start

1. Create a Postgres database and export `CONDUCTOR_DATABASE_URL`.
2. Optionally export `CONDUCTOR_ADMIN_TOKEN` and the upstream base URLs / bearer tokens.
3. Start the service:

```bash
cargo run -- --config config/conductor.yaml
```

4. Open `http://127.0.0.1:8091/dashboard`.

Read APIs are protected by default. Set `allow_dashboard_without_token` to `true` only if you explicitly want public read-only access.

## Delivery Model

- Planner-generated work items start at `development`.
- Successful verification can auto-promote the same work item through `testing`, `integration`, `integration_testing`, `uat`, and `production`.
- Production must use `canary` or `red_green`; `direct` production rollout is blocked by policy.
- Services can expose their inferred deployment environment through discovery, and the dashboard shows environment, stage, and rollout together.
- DORA metrics are calculated from production-stage execution history over the configured rolling window.

## Key APIs

- `GET /healthz`
- `GET /api/v1/summary`
- `GET /api/v1/findings`
- `GET /api/v1/findings/{id}`
- `GET /api/v1/findings/{id}/evidence`
- `GET /api/v1/findings/{id}/provenance`
- `GET /api/v1/repositories`
- `GET /api/v1/services`
- `GET /api/v1/topology`
- `GET /api/v1/events`
- `GET /api/v1/work-items`
- `GET /api/v1/work-items/{id}`
- `PATCH /api/v1/work-items/{id}`
- `GET /api/v1/executions`
- `GET /api/v1/work-items/{id}/executions`
- `GET /api/v1/work-items/{id}/traceability`
- `POST /api/v1/execution/run`
- `POST /api/v1/work-items/{id}/execute`
- `POST /api/v1/discovery/run`
- `POST /api/v1/planning/run`

## Storage Model

- Postgres is the system of record.
- `repository_snapshots` now hold the SCM and local-estate inventory that planning and future findings will build on.
- `findings`, `finding_evidence`, and `finding_provenance` now bridge raw inventory/probe state and queued work items with evidence-backed analysis records.
- `work_items` now include short-lived execution claims plus delivery-stage, validated-stage, and rollout metadata.
- `work_executions` now record the attempted delivery stage and rollout strategy for auditability and DORA reporting.
- `data/` is reserved for any local persistent artifacts Conductor may emit in later iterations.
- SQL migrations live in `migrations/` and are executed automatically on startup by default.

## Local Paths Assumed By Default

- Ansible: `/home/pbisaacs/Developer/swarmhpc/swarmhpc/ansible`
- Local repository estate: `/home/pbisaacs/Developer/neuralmimicry`
- Gail: `/home/pbisaacs/Developer/neuralmimicry/gail`
- Tracey: `/home/pbisaacs/Developer/neuralmimicry/tracey`
- Continuum: `/home/pbisaacs/Developer/neuralmimicry/nmc`
- Refiner: `/home/pbisaacs/Developer/neuralmimicry/rag_demo`
- AARNN: `/home/pbisaacs/Developer/neuralmimicry/aarnn_rust`

## Documentation

- `docs/ARCHITECTURE.md`
- `docs/TARGET_ARCHITECTURE.md`
- `docs/OPERATIONS.md`
- `docs/FORMAL_GAP_ANALYSIS.md`
