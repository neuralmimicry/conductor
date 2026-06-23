# Conductor — Wiki Home

**Conductor** is the estate control-plane for the NeuralMimicry platform. It scans the deployed topology from SwarmHPC Ansible playbooks, enriches it from live service probes, derives typed findings and evidence, and drives a governed work-item queue through a staged delivery pipeline backed by Postgres.

Conductor owns the **plan** and **govern** lifecycle segments.

> ☕ [Support NeuralMimicry on Crowdfunder](https://www.crowdfunder.co.uk/p/qr/aWggxwPW?utm_campaign=sharemodal&utm_medium=referral&utm_source=shortlink)

---

## Quick navigation

| Page | Description |
|---|---|
| [Getting Started](Getting-Started) | Prerequisites, database setup, first run |
| [Architecture](Architecture) | Data flow, storage model, delivery pipeline |
| [Configuration](Configuration) | `conductor.yaml` reference and environment variables |
| [API Reference](API-Reference) | REST endpoints for work items, findings, executions |
| [Delivery Model](Delivery-Model) | Stage progression, rollout policies, DORA metrics |
| [Contributing](Contributing) | Running Conductor locally, PR guidelines |

---

## Quick start

```bash
# Set the Postgres connection string
export CONDUCTOR_DATABASE_URL="postgres://user:pass@localhost:5432/conductor"

# Run (migrations execute automatically on startup)
cargo run -- --config config/conductor.yaml
```

Dashboard: **http://127.0.0.1:8091/dashboard**

## Lifecycle ownership

| Segment | Service |
|---|---|
| `plan`, `govern` | **Conductor** (this repo) |
| `code`, `build`, `test`, `iterate` | [Refiner](https://github.com/neuralmimicry/rag_demo) |
| `release`, `operate` | [Continuum / NMC](https://github.com/neuralmimicry/nmc) |

## Delivery stage progression

Work items move through: `development` → `testing` → `integration` → `integration_testing` → `uat` → `production`

Production execution is policy-blocked unless the rollout strategy is `canary` or `red_green`.

## Key APIs

- `GET /api/v1/summary` — estate overview
- `GET /api/v1/work-items` — current queue
- `PATCH /api/v1/work-items/{id}` — update or approve a work item
- `POST /api/v1/execution/run` — trigger execution
- `POST /api/v1/discovery/run` — re-scan estate
- `GET /api/v1/traceability/graph` — full estate traceability graph

## Get involved

- 🐛 [Report a bug or request a feature](https://github.com/neuralmimicry/conductor/issues)
- 💬 [Join the discussion](https://github.com/neuralmimicry/conductor/discussions)
- 📧 Direct support: [info@neuralmimicry.ai](mailto:info@neuralmimicry.ai) · **£1,000/day + VAT**
- 🌐 [neuralmimicry.ai](https://neuralmimicry.ai)
