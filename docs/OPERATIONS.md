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
- Optional bearer token variables for each upstream

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
  http://127.0.0.1:8091/api/v1/summary | jq '.delivery_stage_totals, .rollout_strategy_totals, .dora_metrics'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  http://127.0.0.1:8091/api/v1/services | jq '.services[] | {service_key, deployment_environment, health}'

curl -s -H "authorization: Bearer ${CONDUCTOR_ADMIN_TOKEN}" \
  http://127.0.0.1:8091/api/v1/work-items | jq '.work_items[] | {title, delivery_stage, rollout_strategy, validated_stages, status}'
```

## Safe Extension Path

When extending Conductor toward autonomous self-improvement, keep the stages explicit:

1. Discover and observe.
2. Plan and queue.
3. Require approval or policy gates.
4. Execute through Refiner or another controlled worker.
5. Verify results.
6. Record the outcome.

Skipping those boundaries would make the system harder to audit and harder to secure.
