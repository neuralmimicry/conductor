# Architecture

## Overview

Conductor is a Rust service that continuously converts infrastructure intent and runtime observations into a governed improvement backlog.

The main loops are:

1. Discovery loop
   - Reads Ansible playbooks, role defaults, host vars, and group vars.
   - Produces service snapshots with endpoints, dependencies, capabilities, storage hints, and repo hints.
   - Probes the live control surfaces and records the resulting health.
2. Planning loop
   - Consumes the latest service snapshots.
   - Derives improvement recommendations for reliability, storage durability, scaling, integration gaps, and performance pressure.
   - Optionally asks Gail for an advisory summary.
   - Upserts work items unless `planning.auto_queue` is disabled.
3. Admin loop
   - Lets operators change status, priority, progress, schedule, and admin override state.
   - Supports manual discovery, planning, execution-cycle, and per-item execution triggers.
4. Execution loop
   - Selects approved and dependency-satisfied scheduled work items.
   - Evaluates policy gates.
   - Submits approved work into Refiner's planning and job APIs.
   - Polls execution status and stores verification results.

## Core Modules

- `src/discovery.rs`
  - Parses the SwarmHPC Ansible topology.
  - Infers service metadata and dependency edges.
  - Applies live probe results.
- `src/integrations.rs`
  - HTTP client construction.
  - Service-specific probes for Gail, Tracey, Continuum, Refiner, and AARNN.
  - Gail advisory integration for planning cycles.
- `src/planner.rs`
  - Converts topology and probe signals into prioritized work items and dependency edges.
  - Preserves admin overrides during planner refreshes.
- `src/executor.rs`
  - Converts approved work items into Refiner execution attempts.
  - Persists execution policy, payloads, status, and verification outcomes.
- `src/service.rs`
  - Orchestration layer for repository access, auth behavior, summary generation, and background loops.
- `src/app.rs`
  - Axum routes for dashboard, task graph, and execution/admin API.
- `src/storage/postgres.rs`
  - Postgres-backed persistence and migration execution.

## Persistence

Conductor stores six primary entities:

- `service_snapshots`
- `discovery_runs`
- `service_metric_samples`
- `work_items`
- `work_executions`
- `improvement_cycles`

This design gives a stable audit trail without conflating live topology with queued work.

## External Systems

### Gail

Used as the AI gateway and optional planning advisor. Conductor probes health and orchestration status and can request improvement summaries from Gail.

### Tracey

Used for resource and pressure insight. Conductor expects Tracey to surface health and status signals that can influence prioritization.

### Continuum

Represents the broader control plane. Conductor inspects cluster and adaptive-loop surfaces to understand orchestration reach and integration health.

### Refiner

Treated as the code-generation and workflow execution target. Conductor now uses Refiner as the governed execution surface for approved work items while keeping repository mutation inside Refiner's workflow boundary.

### AARNN

Treated as both a runtime surface and a future self-improvement substrate. Current Conductor tracks it as a first-class service and raises scaling / coordination recommendations when the topology suggests bottlenecks.

## Security Model

- Read-only dashboard APIs can be left public if `allow_dashboard_without_token` is enabled.
- Mutating APIs and manual loop triggers require the admin bearer token when configured.
- Upstream bearer tokens are per-integration and remain out of source control.

## Next Iterations

- Extend the task graph beyond `depends_on` into richer dependency/board views.
- Persist more Refiner control-plane state in Postgres while keeping artifacts on NFS.
- Add evented execution updates and richer dashboard visibility.
- Add streaming Gail/Refiner execution surfaces where it materially improves operator feedback.
