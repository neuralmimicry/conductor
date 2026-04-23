# Conductor Formal Gap Analysis

Date: 2026-04-23
Authoring context: local analysis of `conductor`, adjacent NeuralMimicry repositories, SwarmHPC Ansible inventory, GitHub organisation metadata, and Atlassian availability.
Language: UK English

## 1. Purpose

This document analyses the current `conductor` codebase against the formal "Conductor System for Repository and Runtime Self-Improvement" specification supplied for this project.

The goal is not to restate the specification. The goal is to answer four practical questions:

1. What `conductor` currently does, and what the code appears to intend.
2. Which parts of the wider NeuralMimicry estate should be reused rather than rebuilt.
3. Which requirements are already implemented, partially implemented, or absent.
4. Which enhancement phases would move the system towards the specified target with the least waste and the least architectural churn.

## 2. Executive Assessment

`conductor` is already a real control-plane service. It is not a placeholder. It discovers deployed topology from SwarmHPC Ansible, probes a subset of live services, persists state in Postgres, creates heuristic work items, applies lightweight policy gates, and executes approved work through Refiner's planning and job APIs.

That said, the current system is still only a narrow slice of the formal target. It is best characterised as:

- a topology-aware improvement queue and governance service
- with limited live probing and trend sampling
- plus controlled hand-off into Refiner for code-change execution

It is not yet the broader repository-and-runtime self-improvement conductor described by the specification.

The most important conclusion from this review is that the missing capability should not be built inside `conductor` from scratch. The wider estate already contains many of the missing parts:

- SwarmHPC Ansible already holds the main infrastructure source of truth.
- `rag_demo` already contains guarded Atlassian write actions, Git/VCS workflow helpers, job execution, capability inventory, audit views, and the project solver that `conductor` already depends on indirectly.
- `jirastats` already contains mature Jira and Confluence discovery, reporting, caching, and research helpers.
- `gail` already owns shared LLM orchestration and neuromorphic specialist routing.
- `tracey` already owns richer runtime telemetry, event normalisation, governance, and distributed observability behaviour.
- `aarnn_rust` already contains infrastructure autodetection and adaptive runtime analysis patterns.
- `customers`, `billing`, and `nmchain` already provide identity, team, financial, and immutable audit context that can inform governance.

The correct enhancement strategy is therefore:

- harden `conductor`'s current control-plane behaviour first
- expand inventory and evidence models second
- reuse existing estate capabilities for analysis, research, execution, and documentation
- only then add autonomous improvement loops on top of a stronger evidence and validation substrate

## 3. Current Intent and Workflows

## 3.1 Discovery workflow

The discovery loop is implemented and operational.

Observed behaviour:

- reads playbooks, role defaults, `group_vars`, and `host_vars` from the SwarmHPC Ansible tree
- infers service names, hosts, namespaces, service URLs, storage hints, dependencies, and repo hints
- resolves local git metadata for known repositories
- probes selected services over HTTP
- persists a current service snapshot set and a discovery-run record
- derives metric samples from probe output for later trend summarisation

This is implemented primarily in:

- `src/discovery.rs`
- `src/integrations.rs`
- `src/service.rs`
- `src/storage/postgres.rs`

The design intent is clear: infrastructure intent in Ansible is treated as the baseline source of truth, with live probes used to refine current health and capability state.

## 3.2 Planning workflow

The planning loop is also implemented.

Observed behaviour:

- loads current service snapshots and recent metric samples
- summarises trends
- generates heuristic improvement recommendations
- upserts those recommendations as work items when auto-queueing is enabled
- optionally asks Gail for an advisory planning summary
- persists each planning cycle in Postgres

This is implemented primarily in:

- `src/planner.rs`
- `src/trends.rs`
- `src/integrations.rs`

Current planning is intentionally simple. It focuses on:

- service health degradation
- Tracey pressure signals
- Continuum adaptive loop visibility
- AARNN singleton scaling signals
- persistent storage hints
- Refiner integration as executor

This is useful, but it is still heuristic service governance, not deep repository or runtime analysis.

## 3.3 Governance workflow

The governance layer is present but narrow.

Observed behaviour:

- work items carry status, priority, scheduling state, dependency edges, approval state, verification requirement, and notes
- policy evaluation checks protected services, protected repository roots, blocked keywords, and basic verification requirements
- admin APIs allow reprioritisation, approval, schedule changes, manual execution, and manual loop triggering

This is implemented primarily in:

- `src/models.rs`
- `src/policy.rs`
- `src/app.rs`

The intent is good: separate discovery, planning, policy, execution, and verification stages. This matches the staged control model described in `docs/OPERATIONS.md`.

## 3.4 Execution workflow

The execution loop is real and important.

Observed behaviour:

- selects approved work items in `scheduled` state
- blocks work when dependency edges are not satisfied
- evaluates policy before execution
- calls Refiner planning and job endpoints
- polls Refiner job state
- performs shallow verification against returned stage and finding data
- records execution payloads, policy decisions, errors, and verification output

This is implemented primarily in:

- `src/executor.rs`
- `src/service.rs`
- `src/storage/postgres.rs`

Architecturally this is the strongest current part of `conductor`: it does not mutate repositories directly, and instead treats Refiner as the code-change worker under a governance envelope.

## 3.5 Dashboard and operator workflow

The dashboard and API give operators a usable control surface.

Observed behaviour:

- summary, topology, work item, execution, planning-cycle, and discovery-run endpoints
- admin mutation routes for work items and loop triggers
- SSE stream for execution events
- HTML dashboard backed by the same API

This is implemented primarily in:

- `src/app.rs`
- `src/dashboard.rs`
- `assets/dashboard.html`

This is adequate for a control-room style MVP, but not yet for large multi-team programme management.

## 4. Current Modularisation

The codebase is already separated into coherent modules.

| Module | Current responsibility | Assessment |
| --- | --- | --- |
| `src/config.rs` | config schema, defaults, normalisation | clean and serviceable |
| `src/discovery.rs` | Ansible parsing, service inference, probe application | useful foundation, presently too estate-specific |
| `src/integrations.rs` | HTTP client and service-specific probes | practical, but narrow and hardcoded |
| `src/trends.rs` | metric sample normalisation and trend summaries | good support module |
| `src/planner.rs` | heuristic recommendation generation | works, but far too limited for the specification |
| `src/policy.rs` | approval and keyword/path policy checks | only a first-stage gate |
| `src/executor.rs` | Refiner submission, polling, verification | strategically important, needs hardening |
| `src/service.rs` | background orchestration and aggregate queries | solid orchestration layer |
| `src/app.rs` | HTTP routes and auth checks | adequate for current scope |
| `src/storage/postgres.rs` | persistence and migrations | good base, but current schema is too small |
| `src/repository.rs` | repository abstraction | good seam for extension and testing |

The modular structure is not the main problem. The problem is that the current modules do not yet model enough of the required system.

## 5. Current Strengths

The repository already contains a number of good decisions that should be preserved.

- Clear separation between discovery, planning, policy, execution, and persistence.
- Postgres-backed state rather than file-only state.
- Explicit work-item model with dependency edges and approval gates.
- Refiner-mediated execution rather than direct uncontrolled mutation.
- Use of Ansible as the infrastructure source of truth.
- Reuse of Gail as an advisory planner rather than as an ungoverned decision-maker.
- Background loops for discovery, planning, and execution.
- Event publication for execution lifecycle updates.
- Basic test coverage across routing, policy, planning, and execution.
- `cargo check` and `cargo test` both pass in the current repository state.

These are strong foundations for a governed self-improvement control plane.

## 6. Reusable Estate Capabilities

The formal target explicitly requires reuse of existing approved internal capabilities wherever possible. The wider estate already satisfies a large part of that requirement.

## 6.1 SwarmHPC Ansible

Primary value to `conductor`:

- source of truth for deployed service inventory
- deployment topology
- namespace and ingress data
- storage and PVC hints
- service dependency hints
- host ownership and environment structure

Important observation:

The actual Ansible estate is materially broader than the services `conductor` currently models. In addition to `conductor`, `gail`, `tracey`, `refiner`, `aarnn`, and `postgres`, the playbooks clearly cover services such as:

- `billing`
- `customers`
- `nmchain`
- `nmstt`
- `ollama`
- `prometheus`
- `grafana`
- `observability`
- `webots`
- `nemoclaw`
- `octobot`

Current `conductor` discovery only partially exploits that estate.

## 6.2 Refiner (`rag_demo`)

Refiner already contains several capabilities `conductor` should reuse rather than rebuild:

- guarded Jira and Confluence write actions
- Git and VCS workflow helpers
- project solver planning and execution loops
- capability inventory
- AI orchestration visibility
- audit and secrets endpoints
- job/workspace lifecycle already used as the controlled executor

This makes Refiner the natural reuse point for:

- patch generation
- branch generation
- pull request preparation
- Atlassian write operations under confirmation
- solver-based change execution

`conductor` should continue to own governance and evidence, but not duplicate Refiner's execution substrate.

## 6.3 JiraStats (`jirastats`)

`jirastats` already contains reusable Atlassian and research logic:

- Jira issue discovery and pagination
- Confluence space/page discovery
- local caching of Atlassian content
- topic research workflows
- analysis and reporting patterns
- a large regression test suite around Atlassian behaviour

This makes `jirastats` the best starting point for:

- Atlassian read adapters
- Confluence content inventory
- Jira search and dedupe logic
- external research provenance gathering

## 6.4 Gail

Gail already owns:

- shared LLM orchestration
- provider routing
- metrics persistence for LLM providers
- neuromorphic specialist access
- AER translation
- orchestration status surfaces

This makes Gail the natural reuse point for:

- LLM-backed recommendation synthesis
- classification and summarisation steps
- model role and trust classification
- staged LLM and AARNN participation under governance

`conductor` should call Gail, not reimplement a second AI orchestration layer.

## 6.5 Tracey

Tracey already contains:

- richer runtime telemetry ingestion
- event normalisation
- distributed status surfaces
- governance-style posture control
- Prometheus and observability integration patterns
- adaptive, consensus-oriented runtime analysis

This makes Tracey the best reuse point for:

- runtime metrics ingestion
- anomaly and hotspot detection
- temporal signal normalisation
- distributed system observability inputs

## 6.6 AARNN Rust

`aarnn_rust` already contains:

- infrastructure autodetection against the SwarmHPC Ansible tree
- runtime status and activity surfaces
- deployment mode and multi-network reasoning
- adaptive and temporal pattern logic

This makes it the best reuse point for:

- AARNN-backed anomaly or temporal pattern analysis
- infrastructure autodetection logic that can be lifted into shared estate discovery
- experimentation with adaptive recommendation ranking

## 6.7 Customers, Billing, nmchain, nmstt

These repositories are not primary analysis engines, but they matter for completeness:

- `customers` provides user, team, role, and membership context
- `billing` provides operator dashboards and anomaly patterns around financial flows
- `nmchain` provides immutable audit ledger patterns
- `nmstt` shows how split services preserve stable public contracts while moving execution elsewhere

Together they provide useful patterns for:

- ownership inference
- multi-user governance
- audit immutability
- safe service-boundary design

## 7. Requirement Coverage Summary

Status meanings used below:

- Implemented: materially present in the current codebase
- Partial: present in limited, heuristic, or incomplete form
- Missing: not presently implemented in a meaningful way

| Requirement range | Area | Status | Notes |
| --- | --- | --- | --- |
| REQ-001 to REQ-016 | Core purpose and reuse of existing capabilities | Partial | central orchestration exists; estate reuse is manual and narrow |
| REQ-017 to REQ-025 | Repository discovery and understanding | Partial | local repo and GitHub-backed repository snapshots now exist, but deeper SCM state and ownership modelling are still absent |
| REQ-026 to REQ-038 | Code analysis | Partial | typed findings now exist for repository/service inventory, runtime health, and some repository-quality heuristics, but deep static code smell, security, and concurrency scanning are still absent |
| REQ-039 to REQ-049 | Runtime and operational analysis | Partial | limited service probes and trend summaries only |
| REQ-050 to REQ-056 | Architecture and modularisation analysis | Partial | service topology exists; deeper module/repo boundary analysis is absent |
| REQ-057 to REQ-065 | Security | Missing | no dependency scan, secret scan, supply-chain posture, or tenant review |
| REQ-066 to REQ-073 | Robustness and resilience | Partial | some health and dependency handling exists; no systematic resilience analysis |
| REQ-074 to REQ-081 | Performance, efficiency, latency | Partial | Tracey pressure heuristics exist; no general benchmarking or profiling pipeline |
| REQ-082 to REQ-088 | Parallelism and multi-core | Missing | no concurrency analysis of target repos or workloads |
| REQ-089 to REQ-095 | Multi-user and distributed systems | Partial | limited dependency graph only; no tenant/session/distributed contract analysis |
| REQ-096 to REQ-101 | Research | Missing | no dedicated provenance-aware research pipeline in `conductor` |
| REQ-102 to REQ-116 | LLM and AARNN utilisation | Partial | Gail advisory planning exists; staged role/trust/provenance model is absent |
| REQ-117 to REQ-125 | Atlassian integration | Missing | no Jira or Confluence adapter in `conductor` yet |
| REQ-126 to REQ-132 | Recommendation and planning | Partial | prioritised heuristic work items exist, but not dependency-aware implementation plans at programme level |
| REQ-133 to REQ-139 | Change generation | Partial | Refiner execution exists, but branch/PR/documentation generation is indirect and weakly modelled |
| REQ-140 to REQ-146 | Validation | Partial | verification exists and findings now carry explicit evidence/provenance, but independent multi-mode validation is still incomplete |
| REQ-147 to REQ-157 | Governance, safety, trust | Partial | approval and policy gates exist, but permission separation and risk tiers are under-specified |
| REQ-158 to REQ-163 | Observability | Partial | broadcast events and stored runs exist; no self-metrics or persistent event ledger |
| REQ-164 to REQ-168 | Knowledge model | Partial | findings, evidence, provenance, repositories, services, and runs are now modelled explicitly, but there is still no richer cross-repository graph or long-horizon temporal knowledge model |
| REQ-169 to REQ-175 | Non-functional requirements | Partial | some modularity and resilience exist; scaling, incremental analysis, and secret protection need more work |
| REQ-176 to REQ-185 | Acceptance and implementation principles | Partial | evidence-first staging is now materially stronger because work can be traced back to explicit findings, but maturity targets are still not met |

## 8. Detailed Gap Analysis

## 8.1 Discovery and estate inventory gaps

What exists:

- Ansible parsing
- Ansible inventory host-group expansion
- basic dependency inference
- local repository estate scanning
- repository snapshots with SCM and deployment classification
- optional GitHub organisation inventory enrichment
- live probes
- local repo path, repo URL, and branch extraction aligned to discovered services

Main gaps:

- no branch, tag, commit, or pull-request inventory beyond current/default branch state
- no verified ownership, stale-repo, duplicate-repo, or orphaned-component inventory
- repository classification is still heuristic and not yet backed by language-specific analyzers
- no explicit environment model for development, test, staging, and production boundaries
- no durable dependency map across repositories, services, and infrastructure

Implication:

`conductor` cannot yet answer "what exists?" with enough fidelity to support systematic self-improvement.

## 8.2 Repository and code analysis gaps

What exists:

- local repo path hints
- repo URL and current branch capture

Main gaps:

- no static analysis pipeline
- no AST or symbol graph extraction
- no duplication, dead-code, coupling, or cohesion analysis
- no large-function or weak-abstraction detection
- no blocking-I/O, shared-state, or race-risk analysis
- no language-specific adapter framework
- no repo-native tool discovery beyond a few coarse checks in policy verification

Implication:

The current planner cannot produce evidence-backed code improvement findings. It can only infer operationally-themed backlog items from topology and probes.

## 8.3 Runtime and operational analysis gaps

What exists:

- health probes for a handful of services
- limited metric sample extraction from probe payloads
- coarse trend summaries

Main gaps:

- no process, container, pod, or workload inventory
- no ingestion of logs, traces, queue depth, lock contention, or thread pool state
- no autoscaling effectiveness analysis
- no crash-loop or restart-pattern analysis
- no tail-latency or hot-path analysis
- no confidence model for incomplete telemetry

Implication:

`conductor` currently reasons about service health, not about runtime behaviour at the level required by the specification.

## 8.4 Architecture and modularisation gaps

What exists:

- service dependency graph inferred from Ansible
- explicit `depends_on` edges between work items

Main gaps:

- no repository-to-repository dependency graph
- no module-to-module dependency analysis within repositories
- no cyclic dependency detection
- no interface or contract analysis
- no automated decomposition recommendations based on code evidence

Implication:

The system knows some service relationships, but not the actual software architecture relationships that drive maintainability and modularity.

## 8.5 Security and supply-chain gaps

What exists:

- admin token gating
- protected-service and protected-repo policy hints
- blocked command keywords

Main gaps:

- no secret scanning
- no dependency vulnerability scanning
- no container/image hygiene checks
- no authz boundary review
- no API exposure review
- no tenant-isolation review
- no software supply-chain verification
- no mapping of findings to recognised security controls

Implication:

The system currently has governance controls for its own actions, but it does not yet analyse target-estate security posture.

## 8.6 Robustness, resilience, performance, and concurrency gaps

What exists:

- health-aware planning
- dependency blocking for execution
- Refiner polling and basic verification
- trend-based Tracey and Continuum heuristics

Main gaps:

- no systematic retry, backoff, timeout, circuit-breaker, or fallback analysis across target repos
- no idempotency analysis
- no distributed failure-mode analysis
- no benchmark harness integration
- no workload partitioning or multi-core suitability analysis
- no concurrency model review for target code

Implication:

The formal target asks for a conductor that improves non-blocking behaviour, scalability, multi-core use, and resilience. The current implementation does not yet inspect those dimensions directly.

## 8.7 LLM and AARNN pipeline gaps

What exists:

- Gail advisory planning summary
- AARNN treated as a first-class service target

Main gaps:

- no explicit model registry inside `conductor`
- no trust-level, role, or permitted-action classification for models
- no distinction between deterministic findings, LLM reasoning, AARNN inference, and human input
- no confidence scoring on recommendations
- no evidence envelope attached to intelligent outputs
- no staged decision pipeline combining deterministic evidence, Gail reasoning, and AARNN temporal inference

Implication:

LLM and AARNN usage is present only as a hint, not as a governed pipeline.

## 8.8 Atlassian integration gaps

What exists:

- none inside `conductor` itself

Main gaps:

- no Jira adapter
- no Confluence adapter
- no work-item creation or dedupe against Jira
- no Confluence publication of findings, requirements, architecture, or progress
- no link model between work items, PRs, builds, incidents, and tickets

Implication:

This is one of the clearest missing capability areas, and it can be addressed mostly by reusing existing `rag_demo` and `jirastats` logic.

## 8.9 Validation gaps

What exists:

- Refiner-stage polling
- simple verification result parsing
- repo-type-aware suggested verification commands in policy

Main gaps:

- no actual execution of repository-native verification commands by `conductor`
- no independent validation separate from the component that proposed the change
- no contract, resilience, or performance validation mode
- no benchmark comparison storage
- no residual-risk recording beyond shallow failure reasons

Implication:

`conductor` currently records execution outcomes, but does not yet provide the independent validation model required by the specification.

## 8.10 Governance, safety, and trust gaps

What exists:

- approval flag
- admin token
- protected target checks
- keyword blocking
- staged execution flow

Main gaps:

- no explicit permission separation between read, recommend, generate, validate, and apply
- no risk-tier model
- no dry-run mode for the full pipeline
- no environment-scoped restrictions
- no kill switch for autonomous actions
- no prevention of duplicate autonomous actions across multiple `conductor` instances

Implication:

Current governance is directionally correct, but too coarse to support safe autonomous improvement at scale.

## 8.11 Observability and knowledge-model gaps

What exists:

- stored discovery runs
- stored improvement cycles
- stored work items
- stored executions
- SSE event stream

Main gaps:

- no persistent event journal
- no metrics about `conductor`'s own loop latency, queue depth, throughput, or failures
- no traceability graph from finding to change to validation to ticket
- no cross-repository knowledge model
- no temporal relationship history beyond a few row timestamps

Implication:

The current database is an audit log for a narrow workflow, not a knowledge model for estate-wide self-improvement.

## 9. Load-Bearing Risks in the Current Implementation

These are the most important near-term engineering issues observed in the current codebase.

## 9.1 `scheduled_for` is stored but not enforced

Work items carry `scheduled_for`, but the execution selector currently runs any item that is both approved and in `scheduled` state.

Practical effect:

- a work item scheduled for the future may execute immediately
- operator intent can be violated
- dependency-aware scheduling semantics are weaker than the data model suggests

This is a correctness issue, not just a feature gap.

## 9.2 No execution lease or multi-instance coordination

Execution selection is performed without a database lease, row lock, or coordination token.

Practical effect:

- two `conductor` instances can pick the same work item
- duplicate Refiner jobs can be submitted
- the current design is unsafe for active-active deployment

This is the most important resilience gap in the current executor.

## 9.3 Public read access is allowed by default

`allow_dashboard_without_token` defaults to `true`.

Practical effect:

- service topology, queue state, and execution summaries can be exposed unintentionally
- this is an unsafe default for a governance service

## 9.4 Probe and planning coverage is hardcoded to a small service subset

Discovery, probing, and planning all know about a small list of named services.

Practical effect:

- the wider estate is invisible or only weakly classified
- new services require code edits rather than adapter registration
- reuse of the broader repository estate is constrained by configuration shape

## 9.5 Verification is not independent

Execution verification is derived mainly from Refiner job result payloads.

Practical effect:

- the same execution chain proposes, performs, and effectively self-reports success
- this violates the spirit of the independent validation requirements

## 10. Enhancement Roadmap

The roadmap below prioritises correctness, reuse, and governability before breadth.

## Phase 0: Immediate safety and correctness hardening

Objective:

make the current executor safe enough to extend

Required changes:

- enforce `scheduled_for <= now()` when selecting runnable work
- add a database-backed work-item lease or claim step
- prevent duplicate execution start across multiple instances
- add a global kill switch and execution dry-run mode
- make dashboard reads private by default unless explicitly opened
- persist execution events rather than relying only on broadcast SSE

Why first:

There is no value in adding deep analysis if the execution plane can still duplicate or violate schedule intent.

## Phase 1: Estate inventory and knowledge model

Objective:

make `conductor` understand the whole estate, not just a handful of services

Required changes:

- add GitHub organisation inventory ingestion
- expand Ansible-to-service modelling across all tenant playbooks
- classify repositories by language, framework, deployment type, criticality, and ownership
- build a normalised graph for repositories, services, environments, dependencies, and owners
- capture repository freshness, branch/tag metadata, and duplicate/overlap signals

Primary reuse:

- SwarmHPC Ansible
- GitHub API
- `jirastats` discovery/caching patterns
- Refiner Git and GitHub helpers where appropriate

## Phase 2: Evidence and findings model

Objective:

replace heuristic backlog items with typed, provenance-aware findings

Status as of 2026-04-23:

Phase 2 has now started and a first usable slice is implemented in `conductor`.

Required changes:

- add first-class finding, evidence, confidence, source, and provenance models
- distinguish deterministic evidence, LLM reasoning, AARNN inference, and human input
- record confidence and data completeness
- introduce finding dedupe keys and correlation across static/runtime/security sources

Delivered in this pass:

- added persisted `findings`, `finding_evidence`, and `finding_provenance` entities
- added typed finding severities and statuses
- planner now persists evidence-backed findings before generating work items
- planning recommendations now retain `finding_id` and `finding_key` references
- added repository-aware findings such as missing test baselines and archived live repositories
- added API routes for findings, evidence, and provenance retrieval

Residual Phase 2 work:

- add richer source typing for LLM, AARNN, research, and human-originated findings
- correlate multiple source types into a single finding lifecycle instead of replacing the current set each cycle
- add patch/status workflows for findings so operators can suppress, accept, or resolve them directly
- expand evidence from topology heuristics into real static-analysis, security, and contract-analysis outputs

Suggested module additions:

- `src/findings.rs`
- future `src/evidence/`
- future `src/knowledge/`

## Phase 3: Repository and code analysis

Objective:

support the repository-analysis half of the specification

Required changes:

- discover build systems, package managers, and test frameworks automatically
- add language-specific adapters for Rust, Python, JavaScript/TypeScript, and container/IaC artefacts
- integrate repo-native tools where present
- add static analysis for maintainability, security, concurrency, and performance smells
- extract repository dependency and module graphs

Primary reuse:

- repo-native commands already present in target repositories
- Refiner project solver for change workflows
- Gail for summarisation, clustering, and rationale synthesis

## Phase 4: Runtime, telemetry, and resilience analysis

Objective:

support the runtime-analysis half of the specification

Required changes:

- ingest logs, metrics, traces, events, restarts, and queue/backlog signals
- add adapters for Tracey, Prometheus, Grafana, and service-native status surfaces
- model runtime confidence when telemetry is partial
- detect hot paths, tail latency, autoscaling issues, and failure patterns
- add resilience-pattern analysis for timeouts, retries, circuit breaking, and graceful shutdown

Primary reuse:

- Tracey
- Prometheus and Grafana already present in the estate
- AARNN runtime and activity surfaces where useful

## Phase 5: Atlassian integration and programme management

Objective:

turn validated findings into documented work and progress

Required changes:

- add Atlassian read and write adapters inside `conductor`
- publish architecture, requirements, findings, and progress into Confluence
- create and update Jira epics, stories, tasks, and bugs from validated findings
- dedupe against existing issues
- link findings to repositories, executions, PRs, and tickets

Primary reuse:

- `rag_demo/refiner/integrations/atlassian/actions.py`
- `jirastats/atlassian_utils.py`
- `jirastats/confluence_analysis.py`

## Phase 6: Governed autonomous improvement

Objective:

reach the advanced target state without losing independent validation

Required changes:

- introduce staged decision pipelines
- require deterministic evidence before LLM or AARNN escalation
- keep LLM and AARNN advisory unless policy explicitly allows stronger roles
- split permissions for analyse, recommend, generate, validate, and apply
- add human approval gates by risk tier and environment

## 11. Recommended Target Extensions to the Codebase

The existing module layout can be retained if new capability is added as separate packages rather than by overloading the current files.

Suggested additions:

- `src/inventory/`
  - GitHub org and repo inventory
  - repository classification
  - ownership mapping
- `src/findings/`
  - finding types
  - severity, confidence, provenance
  - dedupe and correlation
- `src/analyzers/static/`
  - code analysis adapters
- `src/analyzers/runtime/`
  - Tracey, Prometheus, service-native telemetry
- `src/analyzers/security/`
  - secrets, dependency, and supply-chain analysis
- `src/knowledge/`
  - graph model and impact analysis
- `src/integrations/github.rs`
  - repo, branch, tag, PR, commit, and ownership ingestion
- `src/integrations/atlassian.rs`
  - Jira and Confluence read/write integration
- `src/validation/`
  - independent validation runners
- `src/execution/lease.rs`
  - DB-backed claiming and coordination
- `src/telemetry/`
  - self-observability metrics and persistent event journalling

## 12. Recommended Atlassian Documentation Structure

To satisfy the requirement that systems, requirements, and progress are documented in Atlassian, the following structure is recommended.

Confluence:

- Conductor overview and scope
- formal requirements baseline
- current-state architecture
- reusable-estate capability map
- gap analysis
- roadmap and phase plan
- progress log with dated updates

Jira:

- one epic per major phase
- stories or tasks for each adapter, analyser, and governance capability
- bugs for current load-bearing correctness issues
- link each ticket back to findings and Confluence pages

## 13. Recommended First Jira Backlog Themes

These are the best initial workstreams if tickets are created next.

1. Execution safety hardening
2. Estate inventory and knowledge graph
3. Repository analysis adapters
4. Runtime and telemetry ingestion
5. Atlassian integration
6. Independent validation pipeline
7. Governed LLM and AARNN decision pipeline

## 14. Minimal Acceptance Path

The formal specification defines minimal, operational, mature, and advanced acceptance states.

From the current codebase, the shortest credible path is:

1. Fix execution correctness and multi-instance safety.
2. Expand estate inventory across GitHub and Ansible.
3. Add typed findings with evidence and provenance.
4. Add repository analysis and runtime telemetry ingestion using internal estate capabilities.
5. Add Atlassian integration for documentation and work-item management.
6. Add independent validation and risk-tier governance.
7. Then add broader autonomous improvement loops.

## 15. Progress Update: Phase 0 Hardening

Status as of 2026-04-23:

Phase 0 has now been implemented in the `conductor` repository.

Delivered in this pass:

- `scheduled_for` is now enforced for automatic execution selection.
- execution dispatch now uses DB-backed short-lived work-item claims so two `conductor` instances do not submit the same scheduled item simultaneously
- execution claiming is serialised in Postgres before dispatch
- `execution.dry_run` now suppresses automatic dispatch and allows manual preview payload generation
- `execution.emergency_stop` now halts execution dispatch without disabling discovery or planning
- persistent `conductor_events` storage has been added alongside the existing SSE stream
- a new `GET /api/v1/events` route exposes persisted event history
- read APIs are private by default unless `allow_dashboard_without_token` is explicitly enabled

Residual Phase 0 work still worth doing:

- add first-class UI surfacing for persisted events and claim state in the dashboard
- add recovery handling for stale `in_operation` items when a process dies after dispatch has started but before completion is recorded
- add explicit operator-facing metrics for queue depth, claim contention, and execution-cycle latency

This means the most urgent correctness gaps identified in section 9 are now materially reduced, but the broader requirements coverage described above remains unchanged outside Phase 0.

## 16. Progress Update: Phase 1 Inventory Foundation

Status as of 2026-04-23:

Phase 1 has now started in the `conductor` repository.

Delivered in this pass:

- `conductor` now stores first-class `repository_snapshots` alongside `service_snapshots`
- discovery now scans the mounted local NeuralMimicry repository estate instead of relying only on a small fixed set of repo hints
- discovery now optionally enriches repository inventory from the GitHub organisation API
- service discovery now expands Ansible host targets through `inventory/hosts.ini` so tenant workloads resolve to concrete hosts rather than only group labels
- generic `continuum_tenant_k8s_app` playbooks and role defaults now contribute repository URL and branch metadata more consistently
- repository snapshots now record language, framework, build-system, package-manager, runtime-type, deployment-type, criticality, linked-service, and provenance fields
- a new `GET /api/v1/repositories` route exposes the repository inventory
- the Conductor tenant rollout template now wires the mounted repo root and optional GitHub token into the runtime config

Residual Phase 1 work still worth doing:

- add commit, tag, PR, archive-age, stale-repo, and orphaned-component analysis
- model repository-to-repository dependencies beyond service-derived links
- surface repository inventory in the dashboard UI, not only the API
- add ownership and environment modelling that is stronger than repo-owner heuristics

This means REQ-017 to REQ-025 are now better covered in structure and storage, but still remain partial overall because deeper SCM understanding and knowledge-graph behaviour are not yet implemented.

## 17. Progress Update: Phase 2 Findings and Evidence Model

Status as of 2026-04-23:

Phase 2 has now started in the `conductor` repository and is partially delivered.

Delivered in this pass:

- `conductor` now stores first-class `findings`, `finding_evidence`, and `finding_provenance` records
- planning now persists typed findings before any work items are queued
- recommendations now keep `finding_id` and `finding_key` references for traceability
- findings currently cover repository inventory, service health, runtime trend deterioration, repository lifecycle risk, and repository test-baseline gaps
- new API routes expose findings, evidence, and provenance directly
- the shared Refiner Atlassian client was hardened to work correctly with current Jira Cloud search and write behaviour so Confluence/Jira documentation updates continue to flow through reused estate tooling

Residual Phase 2 work still worth doing:

- add static-code, dependency, security, and concurrency analyzers as new evidence sources
- add richer confidence and completeness scoring for incomplete telemetry
- model LLM, AARNN, research, and human-originated provenance explicitly rather than only deterministic-rule provenance
- add direct operator workflows for finding suppression, acceptance, and resolution

This means REQ-026 to REQ-038 and REQ-164 to REQ-168 are no longer structurally missing, but they remain partial because deeper multi-source analysis and knowledge correlation are still to be built.

## 18. Final Conclusion

`conductor` is already a useful governance and execution coordinator for the NeuralMimicry stack, but it is presently much closer to a topology-aware improvement queue than to the full autonomous improvement conductor defined in the requirements.

The good news is that the gap is not mainly a lack of infrastructure. The missing capability is already distributed across the wider estate.

The correct next move is therefore not a large rewrite. It is a controlled expansion of `conductor` around five principles:

- harden the existing execution plane first
- broaden estate inventory second
- add evidence and validation before autonomy
- reuse existing internal systems wherever safe
- document every stage in Confluence and Jira so governance remains visible

If those principles are followed, the current codebase can evolve into the specified conductor without discarding its present design strengths.
