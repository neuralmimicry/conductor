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
   - Polls execution status, runs bounded project-native validation commands where possible, stores verification results, and can auto-promote the work item after successful validation.
5. Summary loop
   - Aggregates stage totals, rollout totals, external reference totals, and DORA metrics from persisted work-item, traceability-link, and production-execution history.
   - Feeds the dashboard and summary API with current estate and delivery posture data.
6. External-link sync loop
   - Periodically refreshes Jira issue, Confluence page, Refiner job/workspace, Tracey local runtime state, and Continuum-backed Tracey swarm state when the relevant credentials or base URLs are configured.
   - Keeps persisted traceability links aligned with upstream title, status, version, URL, rollout, rollback, incident, fleet, agent, compromise, and deep-dive metadata changes.
7. Estate traceability graph loop
   - Materialises a graph view from persisted services, repositories, findings, work items, executions, and traceability links.
   - Exposes the change path from finding to governed work to execution to rollout / incident evidence without introducing a second persistence model.

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
- `src/integrations/atlassian.rs`
  - Native Jira and Confluence create/read/update/sync support built on the existing traceability-link model.
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
  - Persists execution policy, payloads, delivery metadata, status, Refiner verification, and independent validation outcomes.
  - Auto-promotes work items through the staged pipeline when configured to do so.
- `src/validation.rs`
  - Discovers bounded project-native verification commands.
  - Executes local validation commands with timeout, output truncation, and missing-tooling tolerance.
- `src/service.rs`
  - Orchestration layer for repository access, auth behavior, DORA-aware summary generation, traceability-link persistence, Refiner/Tracey/Atlassian lifecycle operations, estate traceability graph assembly, and background loops.
- `src/app.rs`
  - Axum routes for dashboard, task graph, per-work-item traceability, estate traceability graph, Atlassian-linked external-reference management, and execution/admin API.
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

The `traceability_links` table extends that audit trail across Jira, Confluence, Refiner build/workspace/requirements records, Tracey runtime and rollback signals, Continuum-backed fleet and per-agent state, incidents, compromise posture, deep-dive state, and rollout records without conflating live topology with queued work.

The estate traceability graph is intentionally derived from the existing persistence model at read time. Conductor does not maintain a second graph-specific table for the same correlations.

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

Used for local runtime and rollout detail. Conductor reuses Tracey `/status` and `/loader/status` for node-local runtime posture, loader threat state, rollback posture, and deep-dive indicators.

### Continuum

Represents the broader control plane and the estate view of the Tracey swarm. Conductor now reuses Continuum health, Tracey fleet, agent, analytics, and assessment surfaces so swarm-level Tracey evidence is sourced from the existing monitoring plane instead of being reimplemented inside Conductor.

### Refiner

Treated as the code-generation and workflow execution target. Conductor now uses Refiner as the governed execution surface for approved work items while keeping repository mutation inside Refiner's workflow boundary, reuses Refiner's job, requirements, and workspace APIs to enrich traceability with execution-native evidence, and prefers the dedicated `refiner.neuralmimicry.ai` public edge when it is available while safely falling back to the shared API edge or discovered internal service URL.

### AARNN

Treated as both a runtime surface and a future self-improvement substrate. Current Conductor tracks it as a first-class service and raises scaling / coordination recommendations when the topology suggests bottlenecks.

### Atlassian

Treated as the programme-management and documentation surface. Conductor can now create or dedupe Jira issues, publish or refresh Confluence pages, persist the resulting references as traceability links, and periodically sync upstream state back into the control plane.

## Security Model

- Read APIs are private by default and only become public if `allow_dashboard_without_token` is explicitly enabled.
- Mutating APIs and manual loop triggers require the admin bearer token when configured.
- Release gates are stage-aware: UAT and production require explicit approval when policy is enabled, and production requires a controlled rollout strategy.
- Upstream bearer tokens are per-integration and remain out of source control.
- Atlassian operations require explicit credentials and stay within the same admin-token boundary as other mutating Conductor APIs.

## Next Iterations

- Extend the task graph beyond `depends_on` into richer dependency/board views.
- Persist more Refiner and Tracey control-plane state in Postgres only where it adds governance value while keeping large artifacts on their native stores.
- Add evented execution updates and richer dashboard visibility.
- Add streaming Gail/Refiner execution surfaces where it materially improves operator feedback.
