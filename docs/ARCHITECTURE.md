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
   - Supports manual discovery and planning triggers.

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
  - Converts topology and probe signals into prioritized work items.
  - Preserves admin overrides during planner refreshes.
- `src/service.rs`
  - Orchestration layer for repository access, auth behavior, summary generation, and background loops.
- `src/app.rs`
  - Axum routes for dashboard and admin API.
- `src/storage/postgres.rs`
  - Postgres-backed persistence and migration execution.

## Persistence

Conductor stores four primary entities:

- `service_snapshots`
- `discovery_runs`
- `work_items`
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

Treated as the code-generation and workflow execution target. Current Conductor builds the queue and identifies integration work; the next iteration should convert approved work items into Refiner jobs.

### AARNN

Treated as both a runtime surface and a future self-improvement substrate. Current Conductor tracks it as a first-class service and raises scaling / coordination recommendations when the topology suggests bottlenecks.

## Security Model

- Read-only dashboard APIs can be left public if `allow_dashboard_without_token` is enabled.
- Mutating APIs and manual loop triggers require the admin bearer token when configured.
- Upstream bearer tokens are per-integration and remain out of source control.

## Planned Next Iteration

- Convert approved work items into Refiner job submissions with verification checkpoints.
- Persist richer probe metrics for trend analysis.
- Add execution workers that can safely stage changes in external repos.
- Introduce explicit policy gates for self-improvement workflows.
