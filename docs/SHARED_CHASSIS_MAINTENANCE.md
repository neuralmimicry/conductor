# Shared-chassis maintenance planning

The Centriq 2400 modules are grouped as physical maintenance units:

| Chassis | BMC modules |
| --- | --- |
| Centriq 2400 | qc00, qc01 |
| Centriq 2400 | qc02, qc03 |
| Centriq 2400 | qc04, qc05 |

Each module has its own BMC, while each pair shares chassis power. Work on a
shared battery therefore affects both modules, their services and their
applications. The `maintenance.chassis` configuration records these pairs.
The default planning slot is Sunday at 02:00 UTC with seven days' notice; it is
a proposal, not an approved outage.

Conductor polls Continuum's verified inventory and BMC diagnostic functions,
requires fresh inventory and controller identity, and keeps one bounded,
persistent observation per chassis. It only opens a maintenance proposal for
a fresh critical `SYS_V3_Battery` observation. The proposal lists both module
IDs, safe service labels, the sensor evidence, the suggested UTC window and
the current BMC freshness. Inventory URLs and arbitrary diagnostic fields are
not copied to the dashboard.

The dashboard reads `/api/v1/maintenance-windows`. Admin decisions are posted
to `/api/v1/maintenance-windows/{proposal_id}/decision` with `accept` or
`reject` and require a configured Conductor admin token. Acceptance is only
allowed while the same issue is still present in fresh Continuum evidence and
the proposed time has not passed. Decisions are durable and cannot be replayed.

The current Continuum API does not provide a general service evacuation,
workload scaling, chassis maintenance completion, or restore/rebalance
operation. Therefore an accepted proposal is recorded as
`accepted_blocked`; Conductor does not claim to have moved or scaled any
service. The dashboard lists this blocker. Before automatic preparation can
be enabled, Continuum needs an audited idempotent maintenance workflow that
maps affected Slurm, Kubernetes and native services to verified alternate
capacity, drains or scales them down in dependency order, verifies the chassis
maintenance outcome from both BMC modules and service health, then restores
and rebalances the saved workload state. A worker restart must reconcile that
workflow from durable operation IDs instead of replaying side effects.

## Optional hybrid capacity

On-prem capacity remains the default because it is cheaper. Before proposing
cloud capacity, Conductor must first determine whether unaffected on-prem
hosts can carry the measured workload within their resource and dependency
limits. Hybrid capacity is only a temporary fallback for the remaining,
measured shortfall during an approved maintenance window. It must be sized to
that shortfall, have an explicit expiry, and be stopped after the on-prem
workload has been restored and verified.

Maintenance approval and financial approval are separate gates. An accepted
maintenance proposal does not authorise paid compute. Billing must hold an
explicit, time-bounded cost ceiling before Continuum receives a create/start
request; the provider estimate, reservation, actual cost, operation IDs and
release of unused reservation must be reconciled through Billing. If the
estimate is unavailable, the Billing budget is missing/exhausted, or actual
cost cannot be reconciled, Conductor must leave the proposal blocked and must
not start or extend paid capacity. No automatic increase above the approved
ceiling is allowed. Billing is the budget authority; Conductor and Continuum
must not maintain a competing spend limit.

Continuum's Provider Compute API has asynchronous create and lifecycle jobs
for AWS, GCP and Azure VMs, with health-profile and dependency-policy checks.
It does not currently expose an estate-level temporary capacity-group,
workload evacuation, or scale-to-demand operation. Conductor would need to
request a bounded number of instances through this API and track each exact
job before a supported higher-level workflow can be claimed. The API's
provider credentials and policy are server-side; they must never be sent by
Conductor in request bodies.

At the latest local read-only readiness check (2026-10-12 08:35 UTC), all three
provider adapters reported no provider CLI, no allowed scope, no create health
profiles, no configured dependency policy and `mutation_ready: false`; provider
mutations were disabled. Billing has no infrastructure budget or reservation
routes, and Conductor has no Billing client. The hybrid path is therefore a
design requirement and an explicit blocker, not an available maintenance
option. Do not deploy credentials, enable provider mutations, or incur cloud
cost until Billing's durable budget/hold contract, Continuum's approved
provider profiles, and Conductor's reconciliation workflow are implemented
and validated together.

The configured window and current issue evidence are separate from physical
host recovery. A warm restart may be considered through the existing
evidence-gated Continuum action path; a critical battery continues to block a
cold restart. Battery replacement is a planned shared-chassis outage and must
not be represented as a single-module restart.
