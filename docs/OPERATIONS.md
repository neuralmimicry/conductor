# Operations

## Prerequisites

- Rust toolchain
- Postgres 15+ reachable from Conductor
- Access to the SwarmHPC Ansible tree
- Network reachability to whichever upstream services you want probed

## Configuration

The default config file is `config/conductor.yaml`.

Important environment variables:

- `CONDUCTOR_DATABASE_URL`
- `CONDUCTOR_ADMIN_TOKEN`
- `GAIL_BASE_URL`
- `TRACEY_BASE_URL`
- `CONTINUUM_BASE_URL`
- `REFINER_BASE_URL`
- `AARNN_BASE_URL`
- Optional bearer token variables for each upstream

Empty environment variables are normalized away, so leaving a variable unset does not create a broken empty-string URL.

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
5. Reprioritize, schedule, or override planner-generated work items.

## Failure Modes

- Discovery failures usually indicate missing Ansible paths or malformed playbook data.
- Probe failures are classified into degraded, unreachable, or missing states based on the upstream error signature.
- Migration failures usually indicate a bad database URL, insufficient permissions, or startup from a directory that does not contain `migrations/`.

## Verification

Run these checks before shipping changes:

```bash
cargo fmt
cargo check
cargo test
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
