# Conductor

Rust control-plane service for the NeuralMimicry stack. Conductor discovers the deployed topology from the SwarmHPC Ansible playbooks, scans the mounted NeuralMimicry repositories, optionally enriches that inventory from the GitHub organisation, probes the live service surfaces for Gail, Tracey, Continuum, Refiner, and AARNN, persists state in Postgres, and drives an improvement queue through an admin dashboard and API.

## What It Does

- Parses the SwarmHPC Ansible tree, including inventory groups, `group_vars`, `host_vars`, and tenant playbooks, to infer the deployed topology.
- Builds a first-class repository inventory from the mounted local estate under `/home/pbisaacs/Developer/neuralmimicry`, the mounted SwarmHPC rollout repository, and optional GitHub organisation metadata.
- Resolves repository URLs and branches from Ansible defaults, local git metadata, and explicit repo hints for cross-checking.
- Probes live endpoints to classify health, capture surfaced capabilities, and persist snapshots, including Gail orchestration and trading status when available.
- Infers deployment environments from the existing SwarmHPC tenant environment variables where they are present.
- Derives typed findings, evidence, and provenance records from repository inventory, service topology, runtime probes, and trend summaries.
- Runs an improvement-planning loop that turns evidence-backed findings into graph-aware work items.
- Seeds every new work item into an explicit staged delivery pipeline: `development`, `testing`, `integration`, `integration_testing`, `uat`, then `production`.
- Uses Gail as an optional planning advisor and stores its response alongside each planning cycle.
- Uses Gail's routed AI stack to perform policy-aware autonomous approval reviews for protected work items and records the decision metadata on each work item.
- Carries rollout strategy metadata on work items and executions so release promotion stays explicit and auditable.
- Exposes the mounted SwarmHPC Ansible workspace as a first-class `swarmhpc` deployment-automation target and includes Ansible rollout context in execution payloads for service work.
- Runs bounded project-native verification commands after Refiner completes and records missing-toolchain cases as explicit `unavailable` validation outcomes.
- Periodically self-tests the Conductor repository with its own project-native validation commands and queues a regression work item if that baseline breaks.
- Exposes a work-item traceability view that joins findings, evidence, provenance, executions, and the latest validation state.
- Stores persistent external traceability links for Jira, Confluence, builds, incidents, and rollout records against work items.
- Creates or dedupes Jira issues and Confluence pages natively against Atlassian and persists the resulting references back into traceability links.
- Syncs Jira issue and Confluence page state back into persisted links through API-triggered and background refresh loops.
- Reuses Refiner and Tracey operational APIs to auto-correlate jobs, requirements progress, workspaces, rollout state, runtime posture, rollback risk, and incident-style signals into persistent traceability links.
- Exposes an estate-wide traceability graph that joins services, repositories, findings, work items, executions, and external references into one governed read model.
- Exposes a dashboard for queue visibility, progress updates, approvals, reprioritisation, scheduling, admin overrides, and manual discovery/planning/execution runs.
- Computes DORA deployment metrics from persisted production-stage execution history and exposes correlated bug/incident reference signals through the summary API and dashboard.
- Stores service snapshots, repository snapshots, typed findings, evidence, provenance, discovery runs, metric samples, work items, work executions, planning cycles, and persistent conductor events in Postgres.

## Current Boundaries

- Conductor now executes approved, scheduled, dependency-satisfied work items through Refiner's planning and job APIs.
- Execution selection now respects `scheduled_for`, uses DB-backed claims to avoid duplicate dispatch, and supports `dry_run` and `emergency_stop` controls.
- AI-approved work items are automatically moved into `scheduled` status and the execution loop is triggered immediately when capacity is available.
- Successful stage verification can auto-promote the work item to the next delivery stage.
- Independent validation now executes discovered repository-native checks when a local repo path and suitable tooling are available.
- Deployment automation is now represented as a protected `swarmhpc` service backed by the mounted Ansible workspace, so rollout automation can be reviewed and improved through the same queue.
- Gail capability discovery now absorbs repository-level capabilities such as the trading bridge and backtesting surfaces into the service inventory.
- Conductor now exposes a native API surface for attaching, creating, publishing, and syncing Atlassian-linked traceability references.
- Conductor now exposes an estate-wide graph API for tracing change intent from finding through execution, rollout, and synced external systems.
- Production execution is policy-blocked unless the rollout strategy is `canary` or `red_green`.
- Conductor still does not mutate repositories directly; Refiner remains the only code-change executor.
- Gail is used as an advisory planner, not as the sole orchestration source of truth.
- AARNN is treated as a self-improvement target and telemetry source; Conductor does not yet build or retrain AARNN topologies automatically.
- Probe coverage is implemented for the confirmed endpoints in the local repositories and may need extending as upstream APIs evolve.

## Quick Start

1. Create a Postgres database and export `CONDUCTOR_DATABASE_URL`.
2. Export `CONDUCTOR_LOCAL_REPO_ROOT` and `CONDUCTOR_ANSIBLE_ROOT` if your mounted repository estate or Ansible workspace live somewhere other than the default local paths. When running the container image, mount the NeuralMimicry checkout and the SwarmHPC checkout into those paths.
3. Optionally export `CONDUCTOR_ADMIN_TOKEN`, the upstream base URLs / bearer tokens, and Atlassian credentials if ticket/page lifecycle operations should be enabled. For NeuralMimicry service integrations, prefer Customers-issued service-account bearer tokens scoped to the target service.
4. Start the service:

```bash
cargo run -- --config config/conductor.yaml
```

5. Open `http://127.0.0.1:8091/dashboard`.

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
- `GET /api/v1/traceability/graph`
- `GET /api/v1/events`
- `GET /api/v1/work-items`
- `GET /api/v1/work-items/{id}`
- `PATCH /api/v1/work-items/{id}`
- `GET /api/v1/executions`
- `GET /api/v1/work-items/{id}/executions`
- `GET /api/v1/work-items/{id}/links`
- `POST /api/v1/work-items/{id}/links`
- `POST /api/v1/work-items/{id}/links/jira`
- `POST /api/v1/work-items/{id}/links/confluence`
- `POST /api/v1/work-items/{id}/links/sync`
- `POST /api/v1/links/sync`
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
- `traceability_links` now act as the shared substrate for manual external references, native Atlassian links, and sync metadata.
- `data/` is reserved for any local persistent artifacts Conductor may emit in later iterations.
- SQL migrations live in `migrations/` and are executed automatically on startup by default.

## Local Paths Assumed By Default

- Ansible: `/home/pbisaacs/Developer/swarmhpc/swarmhpc/ansible`
- SwarmHPC repo: `/home/pbisaacs/Developer/swarmhpc/swarmhpc`
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
