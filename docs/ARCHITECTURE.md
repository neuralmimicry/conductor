# Architecture

## Overview

Conductor is a Rust service that continuously converts infrastructure intent and runtime observations into a governed improvement backlog.

The main loops are:

1. Discovery loop
   - Reads Ansible playbooks, role defaults, host vars, group vars, and inventory groups.
   - Scans the mounted local repository estate and optionally enriches it from the GitHub organisation API.
   - Produces service snapshots with endpoints, dependencies, capabilities, storage hints, resolved host targets, repository associations, and inferred deployment environments.
   - Produces repository snapshots with SCM metadata, runtime/deployment classification, linked services, and inventory provenance.
   - Probes the live control surfaces and records the resulting health.
2. Planning loop
   - Consumes the latest service snapshots, repository snapshots, and metric trends.
   - Derives typed findings with explicit evidence and provenance.
   - Converts those findings into improvement recommendations for reliability, storage durability, scaling, integration gaps, performance pressure, and repository quality gaps.
   - Seeds new work items at the `development` stage with explicit rollout metadata.
   - Optionally asks Gail for an advisory summary.
   - Upserts work items unless `planning.auto_queue` is disabled.
3. Admin loop
   - Lets operators change status, delivery stage, rollout strategy, priority, progress, schedule, and admin override state.
   - Supports manual discovery, planning, execution-cycle, and per-item execution triggers.
4. Execution loop
   - Claims due scheduled work items through Postgres-backed coordination.
   - Respects `scheduled_for` and instance-safe dispatch leases.
   - Evaluates policy gates, including stage prerequisites and production rollout restrictions.
   - Supports `dry_run` preview mode and `emergency_stop` execution halts.
   - Submits approved work into Refiner's planning and job APIs.
   - Polls execution status, stores verification results, and can auto-promote the work item after successful validation.
5. Summary loop
   - Aggregates stage totals, rollout totals, and DORA metrics from persisted work-item and production-execution history.
   - Feeds the dashboard and summary API with current estate and delivery posture data.

## Core Modules

- `src/discovery.rs`
  - Parses the SwarmHPC Ansible topology and inventory groups.
  - Scans the local NeuralMimicry repositories and optional GitHub organisation metadata.
  - Infers service metadata, repository metadata, and dependency edges.
  - Applies live probe results.
- `src/integrations.rs`
  - HTTP client construction.
  - Service-specific probes for Gail, Tracey, Continuum, Refiner, and AARNN.
  - Gail advisory integration for planning cycles.
- `src/findings.rs`
  - Converts repository inventory, service topology, runtime probes, and trend summaries into typed findings.
  - Attaches evidence and provenance records that keep deterministic reasoning visible.
- `src/planner.rs`
  - Persists typed findings and converts them into prioritized work items and dependency edges.
  - Seeds staged delivery metadata for every new recommendation.
  - Preserves admin overrides during planner refreshes.
- `src/executor.rs`
  - Converts approved work items into Refiner execution attempts.
  - Uses DB-backed claiming to avoid duplicate execution dispatch.
  - Persists execution policy, payloads, delivery metadata, status, and verification outcomes.
  - Auto-promotes work items through the staged pipeline when configured to do so.
- `src/service.rs`
  - Orchestration layer for repository access, auth behavior, DORA-aware summary generation, and background loops.
- `src/app.rs`
  - Axum routes for dashboard, task graph, and execution/admin API.
- `src/storage/postgres.rs`
  - Postgres-backed persistence and migration execution.

## Persistence

Conductor stores eleven primary entities:

- `service_snapshots`
- `repository_snapshots`
- `findings`
- `finding_evidence`
- `finding_provenance`
- `discovery_runs`
- `service_metric_samples`
- `work_items`
- `work_executions`
- `improvement_cycles`
- `conductor_events`

This design gives a stable audit trail without conflating live topology with queued work.

## Delivery Model

Conductor promotes one governed work item through these stages:

1. `development`
2. `testing`
3. `integration`
4. `integration_testing`
5. `uat`
6. `production`

Key rules:

- Discovery can infer a service's deployed environment from the tenant environment values already present in SwarmHPC Ansible.
- Planner-generated items begin in `development`.
- A stage can only be promoted when its predecessor has been validated, unless the current stage is already marked as validated.
- `production` cannot run with a `direct` rollout strategy.
- Successful verification can auto-advance the work item and reset approval state for the next stage.
- DORA metrics are calculated from persisted `production` executions, not from planner timestamps.

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

- Read APIs are private by default and only become public if `allow_dashboard_without_token` is explicitly enabled.
- Mutating APIs and manual loop triggers require the admin bearer token when configured.
- Release gates are stage-aware: UAT and production require explicit approval when policy is enabled, and production requires a controlled rollout strategy.
- Upstream bearer tokens are per-integration and remain out of source control.

## Next Iterations

- Extend the task graph beyond `depends_on` into richer dependency/board views.
- Persist more Refiner control-plane state in Postgres while keeping artifacts on NFS.
- Add evented execution updates and richer dashboard visibility.
- Add streaming Gail/Refiner execution surfaces where it materially improves operator feedback.
