# NeuralMimicry Target Architecture

## Scope

This target architecture covers the three codebases reviewed in this workspace:

- `rag_demo` as Refiner
- `conductor` as Conductor
- `gail` as Gail

The goal is to keep their current strengths, remove duplicated cross-cutting logic, and move the stack closer to the layered-agent model in the supplied image without collapsing service boundaries.

## Design Goals

1. Keep product workflows in Refiner.
2. Keep topology, governance, planning, approval, and task-graph ownership in Conductor.
3. Keep provider routing, AI-runtime orchestration, transcription, neuromorphic routing, and AER ownership in Gail.
4. Use Postgres for control-plane state and NFS-backed persistent storage for large mutable artifacts.
5. Preserve current features and intent:
   - Refiner remains the user-facing execution/product surface.
   - Conductor remains the adaptive improvement/governance surface.
   - Gail remains the shared AI runtime.
6. De-duplicate routing policy and execution interfaces instead of re-implementing them per repo.

## Source Of Truth For Infrastructure

The current deployment source of truth remains the SwarmHPC Ansible inventory and roles.

Relevant files already in use:

- Conductor deployment and Postgres/NFS wiring:
  - `swarmhpc/swarmhpc/ansible/continuum_tenant_conductor_site.yml`
- Gail persistent storage and generated config wiring:
  - `swarmhpc/swarmhpc/ansible/roles/continuum_tenant_gail/defaults/main.yml`
  - `swarmhpc/swarmhpc/ansible/roles/continuum_tenant_gail/tasks/main.yml`
- Refiner shared auth/Postgres and NFS-backed persistent data wiring:
  - `swarmhpc/swarmhpc/ansible/host_vars/spirit.yml`
- Shared Postgres service and storage class:
  - `swarmhpc/swarmhpc/ansible/continuum_tenant_postgres_site.yml`

## Layer Mapping From The Image

| Image layer / component | Target owner | Notes |
| --- | --- | --- |
| Input Layer: UI, CLI, IDE, CI entrypoints | Refiner | Refiner stays the public API and workflow entrypoint. |
| Session Manager | Refiner | Browser/session/workspace/session-resume concerns stay product-side. |
| Permission Gate | Refiner + Conductor + Gail | Refiner owns user/session auth, Gail owns API-scope auth, Conductor owns execution approval policy. |
| Knowledge Layer: Task Graph | Conductor | Conductor owns backlog ordering, dependencies, approvals, and execution readiness. |
| Knowledge Layer: Memory Store | Split | Refiner owns product/session/job memory, Conductor owns planning/audit memory, Gail owns provider telemetry. |
| Knowledge Layer: Skill Registry | Refiner + Gail | Refiner owns workflow/tool inventory, Gail owns provider/specialist routing contract. |
| Knowledge Layer: Context Compressor | Gail | Central prompt/cache/context policies belong in the shared AI runtime. |
| Master Agent Loop | Split | Refiner runs product loops, Conductor runs governance loops, Gail runs AI-selection loops. |
| Observability Layer | Conductor | Conductor should aggregate live topology, health, trends, and execution audit. |
| Multi-Agent Layer | Refiner | Collaboration sessions, subtasks, and solver decomposition stay product-side. |
| Worktree Isolator | Refiner | Repo clones/worktrees belong with Refiner execution, not in Conductor or Gail. |
| Execution Layer: Tool Dispatch | Refiner | MCP/tool execution remains with Refiner. |
| Execution Layer: Streaming Runtime | Gail + Refiner | Gail should stream provider results; Refiner should stream workflow/job/session output. |
| Integration Layer: MCP runtime / external servers | Refiner | Product/tool integrations stay in Refiner. |
| Integration Layer: model providers / specialists | Gail | Provider and neuromorphic integration stays in Gail. |
| Output Layer | Refiner + Conductor | Refiner returns workflow/job output; Conductor returns governance state and audit. |

## Repo Ownership

### Refiner (`rag_demo`)

| Area | Owns | Must not own |
| --- | --- | --- |
| Product workflows | Jira, Confluence, topic research, project solver, delivery pipeline, TODO routing, RAG, MCP, assistant flows, workspaces, collaboration sessions | Shared provider-routing heuristics or direct provider orchestration policy |
| Public API | Browser/API/session auth, SSO/OIDC exchange, workspace endpoints, job endpoints, collaboration/session streaming | Topology governance or cross-service planning backlog |
| Execution | Job creation, solver prompts, workspace mutation, git clone/worktree isolation, task/tool dispatch | Cross-service approval policy or provider metrics authority |
| Data | Product/session/job control state in Postgres over time, artifacts/workspaces/logs/indexes on NFS | Gail provider telemetry or Conductor topology history |

### Conductor (`conductor`)

| Area | Owns | Must not own |
| --- | --- | --- |
| Observability | Ansible topology discovery, live probes, service snapshots, trend sampling, health classification | End-user product UI/session behavior |
| Governance | Improvement queue, task graph, approvals, scheduling, policy gates, audit trail | Direct provider routing or prompt orchestration |
| Execution control | Refiner job submission for approved work items, execution polling, verification status, admin execution APIs | Direct code mutation outside Refiner’s workflow boundary |
| Data | Postgres system of record for snapshots, work items, executions, cycles, trend samples | Large mutable workspaces or AI provider metrics |

### Gail (`gail`)

| Area | Owns | Must not own |
| --- | --- | --- |
| AI runtime | Multi-provider orchestration, scoring, direct provider adapters, transcription, neuromorphic routing, AER encode/decode | Product-specific workflow prompts or job/session state |
| Shared routing contract | Workflow/role tags, keyword tags, default provider specialties, specialist attachment behavior | Product work queue or service-governance logic |
| Runtime persistence | Provider metrics, Ollama inventory, later prompt cache/context compaction on NFS | Product artifacts or governance ledger |
| API | Shared bearer-token protected AI endpoints | Browser session auth or product routing |

## Target Persistence Split

| Domain | System of record | Why |
| --- | --- | --- |
| Conductor service snapshots, discovery runs, metric samples, work items, work executions, improvement cycles | Postgres | Relational control-plane state with audit/history queries |
| Refiner auth/shared identity state | Postgres | Already aligned to shared auth DB wiring |
| Refiner job/session/todo/schedule metadata | Postgres | Needs durable, queryable control-plane state and cross-instance safety |
| Refiner workspaces, repo clones, job artifacts, logs, exported bundles, RAG indexes | NFS | Large mutable artifact storage shared across pods/nodes |
| Gail provider metrics, model inventory, later prompt cache | NFS | Durable file-backed runtime artifacts; does not justify its own relational store today |
| Conductor local scratch/debug files | NFS or ephemeral local storage | Not a system of record; keep optional |

## Concrete Data Placement By Repo

| Repo | Postgres | NFS |
| --- | --- | --- |
| Refiner | Auth/shared identity today; target: session index, job index, TODO/schedule state, collaboration-room metadata | `job_data`, workspaces, cloned repos, solver artifacts, session history/event logs, RAG files |
| Conductor | `conductor` DB for topology, backlog, executions, trends | `conductor-data` only for optional exported reports/debug artifacts |
| Gail | None required for current runtime | `/app/data` for provider metrics, model inventory, future prompt/cache artifacts |

## Target Runtime Workflows

### 1. User workflow execution

1. User/API/CLI enters through Refiner.
2. Refiner resolves session/auth and product context.
3. Refiner calls Gail for shared AI-runtime decisions.
4. Refiner performs tool use, RAG, MCP, workspace mutation, and job orchestration.
5. Refiner writes durable control-plane metadata to Postgres and large artifacts to NFS.

### 2. Self-improvement governance loop

1. Conductor reads Ansible and probes live services.
2. Conductor writes topology snapshots and trends to Postgres.
3. Conductor derives recommendations and stores them as graph-aware work items.
4. Approved, dependency-satisfied work items are executed through Refiner’s job APIs.
5. Refiner performs the code-change workflow.
6. Conductor stores execution policy, submitted payload, job status, and verification outcome in Postgres.

### 3. Shared AI runtime loop

1. Refiner or future clients call Gail.
2. Gail loads the shared routing contract.
3. Gail chooses providers/specialists, runs concurrent candidates, and persists telemetry to NFS.
4. Gail returns a normalized runtime response without taking over product logic.

## Improvements Identified From The Image

### Implement now

- Shared routing contract for Refiner and Gail instead of duplicated hardcoded workflow/provider tags.
- Conductor task graph with explicit `depends_on` edges.
- Conductor execution loop running in the background instead of existing only as a latent module.
- Conductor execution APIs for queue inspection and manual triggering.
- Clear separation of Postgres control-plane state vs NFS artifact state.

### Next migrations

- Persist Refiner collaboration/session state in Postgres instead of in-memory TTL-only stores.
- Move more Refiner job/session indexes out of file-only state into Postgres while keeping artifacts on NFS.
- Add Gail streaming output so Refiner can expose end-to-end streamed workflow responses.
- Add a Gail-owned prompt/context cache and compaction layer on NFS.
- Evolve Refiner clone-per-job behavior into explicit git-worktree isolation where it materially reduces merge conflicts.
- Add a Conductor-visible event stream for execution lifecycle updates rather than polling-only dashboards.

## Migration Phases

### Phase 0: Foundation And De-duplication

Status: implemented in this change.

- Introduce a shared AI routing contract file consumed by Refiner and Gail.
- Keep Gail as the owner of shared AI-runtime behavior; Refiner keeps a repo-local copy for offline/development fallback.
- Add Conductor task-graph dependencies.
- Wire Conductor background execution loop.
- Add Conductor execution APIs and execution audit persistence.
- Update architecture documentation and repo docs.

### Phase 1: Durable Refiner Control Plane

- Move Refiner collaboration/session indexes, TODO scheduling metadata, and job ledger state into Postgres.
- Keep workspaces, artifacts, and logs on NFS.
- Add migration/backfill scripts rather than rewriting history in place.
- Maintain API compatibility while moving storage under the hood.

### Phase 2: Streaming And Evented Coordination

- Add Gail response streaming.
- Add Refiner workflow/job/session event streams backed by durable logs.
- Surface Conductor execution progress through a first-class event feed instead of dashboard refresh polling.

### Phase 3: Advanced Multi-Agent Runtime

- Introduce explicit mailbox/board abstractions only if they are required by real workflows.
- Keep those abstractions in Refiner for product work and in Conductor for governance work; do not push them into Gail.
- Add skill/registry injection only where it reduces prompt duplication instead of creating a parallel framework.

## Implemented In This Workspace

This repo set now moves toward the target architecture by doing the following:

- Refiner and Gail share one JSON routing-contract schema instead of maintaining separate hardcoded routing maps.
- Gail ships the routing contract with the service runtime.
- Refiner exposes the routing-contract path/version in orchestration status for operator visibility.
- Conductor work items now carry dependency edges.
- Conductor only auto-executes approved, scheduled, dependency-satisfied work items.
- Conductor exposes recent executions and per-work-item execution history.
- Conductor can trigger an execution cycle or execute a single work item explicitly through the API.

## Non-Goals

- Conductor does not become a direct code-mutating runtime.
- Gail does not absorb product workflows or session management.
- Refiner does not take back shared provider orchestration from Gail.
- This change does not perform a risky all-at-once Refiner state migration from file-backed storage into Postgres.
