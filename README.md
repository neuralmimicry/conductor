# Conductor

Rust control-plane service for the NeuralMimicry stack. Conductor discovers the deployed topology from the SwarmHPC Ansible playbooks, probes the live service surfaces for Gail, Tracey, Continuum, Refiner, and AARNN, persists state in Postgres, and drives an improvement queue through an admin dashboard and API.

## What It Does

- Parses `/home/pbisaacs/Developer/swarmhpc/swarmhpc/ansible/*.yml` and related `group_vars` / `host_vars` to infer the deployed topology.
- Resolves repo hints for Gail, Tracey, Continuum (`nmc`), Refiner (`rag_demo`), and AARNN (`aarnn_rust`).
- Probes live endpoints to classify health, capture surfaced capabilities, and persist snapshots.
- Runs an improvement-planning loop that turns health, dependency, storage, and pressure signals into graph-aware work items.
- Uses Gail as an optional planning advisor and stores its response alongside each planning cycle.
- Exposes a dashboard for queue visibility, progress updates, approvals, reprioritisation, scheduling, admin overrides, and manual discovery/planning/execution runs.
- Stores service snapshots, discovery runs, metric samples, work items, work executions, and planning cycles in Postgres.

## Current Boundaries

- Conductor now executes approved, scheduled, dependency-satisfied work items through Refiner's planning and job APIs.
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

If `allow_dashboard_without_token` is `true`, read-only dashboard API calls remain public while planning, patching, and manual run triggers still require the bearer token.

## Key APIs

- `GET /healthz`
- `GET /api/v1/summary`
- `GET /api/v1/services`
- `GET /api/v1/topology`
- `GET /api/v1/work-items`
- `GET /api/v1/work-items/{id}`
- `PATCH /api/v1/work-items/{id}`
- `GET /api/v1/executions`
- `GET /api/v1/work-items/{id}/executions`
- `POST /api/v1/execution/run`
- `POST /api/v1/work-items/{id}/execute`
- `POST /api/v1/discovery/run`
- `POST /api/v1/planning/run`

## Storage Model

- Postgres is the system of record.
- `data/` is reserved for any local persistent artifacts Conductor may emit in later iterations.
- SQL migrations live in `migrations/` and are executed automatically on startup by default.

## Local Paths Assumed By Default

- Ansible: `/home/pbisaacs/Developer/swarmhpc/swarmhpc/ansible`
- Gail: `/home/pbisaacs/Developer/neuralmimicry/gail`
- Tracey: `/home/pbisaacs/Developer/neuralmimicry/tracey`
- Continuum: `/home/pbisaacs/Developer/neuralmimicry/nmc`
- Refiner: `/home/pbisaacs/Developer/neuralmimicry/rag_demo`
- AARNN: `/home/pbisaacs/Developer/neuralmimicry/aarnn_rust`

## Documentation

- `docs/ARCHITECTURE.md`
- `docs/TARGET_ARCHITECTURE.md`
- `docs/OPERATIONS.md`
