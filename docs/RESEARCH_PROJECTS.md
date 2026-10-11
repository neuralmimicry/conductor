# Conductor Research & Projects

Conductor stores research project intent, ownership, domain, status, progress, budget and resource bounds, evidence, milestones, review decisions, citation evidence, artefact provenance and sandbox state. PostgreSQL deployments persist projects through migration `0013_research_projects.sql`; the in-memory repository is for tests and local development.

## Workflow

Create a private project with `POST /api/v1/research-projects`, then queue it with `POST /api/v1/research-projects/{id}/queue-research`. Queueing creates a Refiner `project_solver` work item with verification required, execution approval disabled, and explicit restrictions against repository changes and production access. The standard Conductor approval and execution gates remain in force.

Every factual claim in a research result must be recorded in `fact_evidence` with at least two distinct online HTTPS source URLs and two distinct publisher identities. Each citation records its title, publisher and access time. If Refiner cannot find two independent sources, the claim must be marked unverified and the project must remain blocked or in review; unsupported claims must not be presented as fact. A human evidence reviewer must check that the claim inventory is complete and that the sources are online, relevant and independent. Conductor validates the recorded structure but does not fetch arbitrary URLs to verify availability.

The project list reconciles progress with the newest linked Refiner work item. Successful Refiner work moves a project to `review` at no more than 90 per cent; it does not mark the project complete. A failed, aborted or held job moves the project to `blocked` with a next step to review its logs and cause. A human reviewer must assess the deliverables before setting a project to `completed`.

## Publication and access

The public catalogue at `GET /api/v1/public/research-projects` is unauthenticated, read-only and contains only projects that pass all publication checks. Publication requires all review decisions, separate subject, quality and evidence reviewers, a complete claim-by-claim evidence ledger satisfying the two-source rule, and at least one reviewed public HTTPS artefact with a recorded licence. Private projects, customer-only artefacts, budgets, resource limits, internal evidence URLs and sandbox metadata are excluded from the public catalogue. Its CORS response is limited to the NeuralMimicry production website origins.

Private project APIs require the Conductor admin token even when general dashboard reads are public. The dashboard supports private project creation, progress updates, Refiner queueing and metadata review. The website's `/research-projects` page can load the public catalogue when `VITE_CONDUCTOR_PUBLIC_API_BASE` is configured to the public Conductor API origin. Until the public API route and cross-origin policy are deployed and configured, the page presents an honest empty state.

Customer-only file downloads are not available yet: they require a customer-authenticated server-side authorisation and artifact-delivery path. Sandbox status is recorded for planning and audit, but this implementation does not provision, charge for or tear down lab infrastructure. Do not represent either capability as operational until its end-to-end deployment and validation are complete.

## Project budget priority

Research and education activity remains subordinate to the top-priority USDT ROI objective. Store budgets as decimal strings, set an explicit USDT ceiling and non-empty resource limits before queueing work (`0` explicitly prohibits spend), reject recorded spend above that ceiling, and do not launch a paid sandbox through this workflow.
