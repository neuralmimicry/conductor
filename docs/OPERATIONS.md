# Operations

## Prerequisites

- Rust toolchain
- Postgres 15+ reachable from Conductor
- Access to the SwarmHPC Ansible tree
- Access to the mounted NeuralMimicry repository estate if local repo inventory should be enabled
- Network reachability to whichever upstream services you want probed

## Configuration

The default config file is `config/conductor.yaml`.

Important environment variables:

- `CONDUCTOR_DATABASE_URL`
- `CONDUCTOR_ADMIN_TOKEN`
- `CONDUCTOR_INSTANCE_ID`
- `GITHUB_TOKEN` if GitHub organisation inventory enrichment should include authenticated access
- `GAIL_BASE_URL`
- `TRACEY_BASE_URL`
- `CONTINUUM_BASE_URL`
- `REFINER_BASE_URL`
- `AARNN_BASE_URL`
- `GRAFANA_BASE_URL`
- `PROMETHEUS_BASE_URL`
- `POSTGRES_CONNECTION_STRING` when the shared estate Postgres instance should be probed independently of `CONDUCTOR_DATABASE_URL`
- `SHARED_STORAGE_MOUNT_PATH` when the shared NFS export is mounted somewhere other than the discovered default path
- Optional bearer token variables for each upstream
- Optional username/password variables for Grafana and Prometheus if the public edge is protected and in-cluster routing is unavailable
- `ATLASSIAN_BASE_URL`
- `ATLASSIAN_USERNAME`
- `ATLASSIAN_API_TOKEN`
- `ATLASSIAN_JIRA_PROJECT_KEY`
- `ATLASSIAN_CONFLUENCE_SPACE_KEY`
- `ATLASSIAN_CONFLUENCE_PARENT_PAGE_ID`

Empty environment variables are normalized away, so leaving a variable unset does not create a broken empty-string URL.

Important discovery controls in `config/conductor.yaml`:

- `discovery.local_repo_root`: local checkout root scanned for repository inventory and service-to-repo matching.
- `discovery.github.enabled`: turns GitHub organisation inventory enrichment on or off.
- `discovery.github.owner`: GitHub organisation or user to inventory.
- `discovery.github.token`: optional PAT used for authenticated inventory and higher rate limits.

Important execution controls in `config/conductor.yaml`:

- `execution.dry_run`: prevents automatic Refiner submission and allows manual preview payload generation.
- `execution.emergency_stop`: halts execution dispatch without disabling discovery or planning.
- `execution.claim_ttl_seconds`: expiry for short-lived dispatch claims used to avoid duplicate multi-instance execution starts.
- `execution.instance_id`: stable identifier written into dispatch claims and events.

Important delivery controls in `config/conductor.yaml`:

- `delivery.auto_advance`: automatically promotes a successfully verified work item to the next stage.
- `delivery.require_uat_before_production`: keeps production gated behind validated UAT unless explicitly relaxed.
- `delivery.production_canary_percentage`: percentage written into canary rollout payloads sent to Refiner.
- `delivery.dora_window_days`: rolling window used for DORA metrics in the summary API and dashboard.

Important validation controls in `config/conductor.yaml`:

- `validation.enabled`: turns post-Refiner independent validation on or off.
- `validation.require_success`: fails the execution when independent validation reports a failed or timed-out command.
- `validation.allow_missing_tooling`: records missing local toolchains as `unavailable` instead of failing immediately.
- `validation.timeout_seconds`: per-command timeout for local validation commands.
- `validation.max_commands`: upper bound on discovered project-native validation commands per execution.

Important Atlassian controls in `config/conductor.yaml`:

- `integrations.atlassian.enabled`: enables native Jira/Confluence lifecycle operations and periodic sync.
- `integrations.atlassian.timeout_seconds`: request timeout for Jira and Confluence API calls.
- `integrations.atlassian.sync_interval_seconds`: background refresh cadence for Atlassian-backed traceability links. Set to `0` to disable background sync.
- `integrations.atlassian.jira_project_key`: default Jira project used when the Jira link API is called without an explicit project.
- `integrations.atlassian.jira_issue_type`: default Jira issue type for native issue creation.
- `integrations.atlassian.confluence_space_key`: default Confluence space used for native page publication.
- `integrations.atlassian.confluence_parent_page_id`: optional parent page used when Confluence pages are created.

Important Refiner and Tracey sync controls in `config/conductor.yaml`:

- `integrations.refiner.sync_interval_seconds`: background cadence for Refiner-backed traceability enrichment. Set to `0` to disable job/workspace/requirements sync.
- `integrations.tracey.sync_interval_seconds`: background cadence for Tracey-backed runtime, rollout, rollback, and incident-signal sync. Set to `0` to disable runtime correlation refresh.
- `CONTINUUM_BASE_URL`: may point either at the native Continuum root or the public monitoring path. Conductor now normalises the NeuralMimicry public edge to `/services/health/monitoring` automatically when needed and reuses Continuum for Tracey fleet, analytics, and assessment correlation.
- `REFINER_BASE_URL`: may stay unset for in-cluster execution, but when public routing is required Conductor now prefers `https://refiner.neuralmimicry.ai`, then the shared `https://api.neuralmimicry.ai` edge, then the discovered service URL.
- `integrations.grafana`: probes Grafana health plus dashboard/datasource inventory and will fall back from the public edge to the discovered cluster URL when needed.
- `integrations.prometheus`: probes Prometheus runtime and target coverage so service-specific scrape failures can become improvement findings.
- `integrations.postgres.connection_string`: optional dedicated shared-instance DSN; when unset Conductor falls back to `CONDUCTOR_DATABASE_URL`.
- `integrations.shared_storage.mount_path`: optional mounted shared-storage root. Set this explicitly when Conductor runs outside the storage host or when the NFS export is mounted at a non-default path.

## Running Locally

```bash
export CONDUCTOR_DATABASE_URL=postgres://conductor:conductor@127.0.0.1:5432/conductor
cargo run -- --config config/conductor.yaml
```

## Running In A Container

```bash
docker build -t conductor .
docker run --rm -p 8091:8091 \
  -e CONDUCTOR_DATABASE_URL=postgres://conductor:conductor@host.docker.internal:5432/conductor \
  -e CONDUCTOR_ADMIN_TOKEN=change-me \
  conductor
```

## Admin Workflow

1. Open `/dashboard`.
2. Load the admin token if one is configured.
3. Inspect the topology and queue.
4. Trigger discovery or planning on demand when upstream state changes.
5. Review typed findings through the API before approving or reprioritising downstream work.
6. Reprioritize, schedule, or override planner-generated work items.
7. Use `execution.dry_run` for preview-only validation and `execution.emergency_stop` for immediate execution halt.
8. Promote work through `development`, `testing`, `integration`, `integration_testing`, `uat`, and `production` instead of treating execution as a single undifferentiated step.
9. Keep production work on `canary` or `red_green`; `direct` production rollout is blocked by policy.
10. Use `GET /api/v1/work-items/{id}/traceability` when you need the finding, evidence, execution, and validation chain in one response.
11. Use `POST /api/v1/work-items/{id}/links` to persist manual Jira, Confluence, build, incident, or rollout references back into Conductor once external systems have been updated.
12. Use `POST /api/v1/work-items/{id}/links/jira` to create or dedupe a Jira issue natively from the selected work item.
13. Use `POST /api/v1/work-items/{id}/links/confluence` to publish or refresh a Confluence page natively from the selected work item.
14. Use `POST /api/v1/work-items/{id}/links/sync` or `POST /api/v1/links/sync` to refresh Refiner, Tracey, Jira, and Confluence state back into persisted traceability links.
15. Use `GET /api/v1/traceability/graph` when you need the estate-level evidence path from service/repository finding through work item, execution, rollout, swarm runtime evidence, and external references.

## Failure Modes

- Discovery failures usually indicate missing Ansible paths or malformed playbook data.
- Repository inventory failures usually indicate a missing mounted repo root or GitHub API/auth issues; discovery still records a partial-success run in those cases.
- Finding generation failures usually indicate schema drift between persisted snapshots and planner expectations; planning will fail loudly rather than queue evidence-free work.
- Probe failures are classified into degraded, unreachable, or missing states based on the upstream error signature.
- Migration failures usually indicate a bad database URL, insufficient permissions, or startup from a directory that does not contain `migrations/`.
- If a Conductor instance exits after claiming a work item but before starting execution, the claim expires after `execution.claim_ttl_seconds` and the item becomes runnable again.

## Verification

Run these checks before shipping changes:

```bash
cargo fmt
cargo check
cargo test
```

For a live local verification after startup:

```bash
curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  http://127.0.0.1:8091/api/v1/summary | jq '.delivery_stage_totals, .rollout_strategy_totals, .external_reference_totals, .dora_metrics'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  http://127.0.0.1:8091/api/v1/services | jq '.services[] | {service_key, deployment_environment, health}'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  http://127.0.0.1:8091/api/v1/work-items | jq '.work_items[] | {title, delivery_stage, rollout_strategy, validated_stages, status}'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  http://127.0.0.1:8091/api/v1/work-items/${WORK_ITEM_ID}/traceability | jq '.traceability | {work_item: .work_item.title, finding: .finding.finding_key, independent_validation: .independent_validation}'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  -H "content-type: application/json" \
  -d '{"system":"jira","reference_type":"bug","reference_key":"KAN-5","status":"To Do"}' \
  http://127.0.0.1:8091/api/v1/work-items/${WORK_ITEM_ID}/links | jq '.link'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  -H "content-type: application/json" \
  -d '{"issue_type":"Bug"}' \
  http://127.0.0.1:8091/api/v1/work-items/${WORK_ITEM_ID}/links/jira | jq '.result | {upstream_action, reference_key: .link.reference_key, status: .link.status}'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  -H "content-type: application/json" \
  -d '{}' \
  http://127.0.0.1:8091/api/v1/work-items/${WORK_ITEM_ID}/links/confluence | jq '.result | {upstream_action, page_id: .link.reference_key, title: .link.title}'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  -H "content-type: application/json" \
  -d '{"systems":["refiner","tracey","jira","confluence"]}' \
  http://127.0.0.1:8091/api/v1/work-items/${WORK_ITEM_ID}/links/sync | jq '.sync | {synced_systems, errors, links}'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  http://127.0.0.1:8091/api/v1/traceability/graph | jq '.graph | {node_totals, relationship_totals, edges: (.edges | length)}'
```

For the current NeuralMimicry public edge, successful Tracey swarm sync depends on either:

- a reachable `CONTINUUM_BASE_URL`, or
- discovery exposing the existing Continuum monitoring path and token.

Successful Refiner public-edge sync depends on either:

- `https://refiner.neuralmimicry.ai` being live on vega, or
- the shared `https://api.neuralmimicry.ai` edge still fronting the same Refiner instance.

## Safe Extension Path

When extending Conductor toward autonomous self-improvement, keep the stages explicit:

1. Discover and observe.
2. Plan and queue.
3. Require approval or policy gates.
4. Execute through Refiner or another controlled worker.
5. Verify results.
6. Record the outcome.

Skipping those boundaries would make the system harder to audit and harder to secure.
