# Qualified immutable delivery

Conductor owns UAT/production stages; Refiner implements and tests improvements
using project_solver, research, NMIDE and Gail-assisted agents as appropriate.
Successful Refiner jobs can return `release_candidate` at the root or under
`result`; Conductor persists that proposal in the work-item plan with its origin.
It is untrusted until independent qualification completes.

`delivery.qualified_releases` is an operator-owned map keyed by `owner/repository`.
Each entry fixes the workflow, all required validation/build/package job names,
UAT/production service keys and the acceptance contract. An empty map blocks
release execution. Refiner cannot supply a cheaper or weaker qualification policy.
Candidate manifests contain full source SHA, Actions run ID/attempt, release tag,
asset/checksum, immutable image, exact UAT/production Deployment/container/previous
image coordinates, and the policy's acceptance contract.

Qualification re-fetches the configured default branch head, exact Actions run
and attempt-specific job list, resolved release tag and uploaded nonempty release
asset digest. All required jobs must succeed; stale, skipped, ambiguous or
truncated observations fail. `gh attestation verify` additionally checks the OCI
artifact's signature, repository, signing workflow and source digest. Conductor
checks its verified SLSA subject and exact run/attempt invocation identity. The
runtime needs a trusted gh CLI supporting `attestation verify --source-digest`
(e.g. 2.83.0), approved registry/GitHub credentials and published image attestations;
missing tooling/evidence blocks delivery, without a weaker fallback.

UAT requires independently discovered UAT service coordinates and Continuum's
`validation_isolation_verified=true` proof for a supported state/credential/network
isolation adapter. That adapter is not yet implemented: creating an arbitrary
second Deployment or applying a label does not qualify a sandbox. Production
requires the latest successful, unexpired UAT execution for the same complete
candidate identity, renewed normal authorization and live
`zero_unavailable_rollout_ready=true`. Continuum's typed production rollout
rechecks the stateless surge-first strategy under resource-version/image CAS.
Stateful single-writer products require a separately qualified cutover adapter.

The independently fetched health probe must expose
`metrics.delivery_acceptance.observed_at_unix_ms` (age 0–60 seconds) and
`metrics.delivery_acceptance.checks`, an object whose operator-required named
checks are all true. Required checks must include the applicable authenticated
user paths, state preservation, dependency/schema compatibility and rollback/restore
acceptance. `acceptance.metric_pointer` selects an independently measured positive
metric, with direction, positive minimum benefit and explicit regression budget.
Three fresh baseline samples and at least three post-rollout samples across the
configured health window reject degradation and require measured benefit.

Intent is persisted before typed Continuum mutation. Ambiguous replies are checked
against actual request/image state and never blindly resubmitted. The execution
records a reconciliation marker before any later status observation. Failed
post-rollout acceptance returns through Continuum to the exact previous digest
only while the recorded operation still owns the target; readiness and recovered
health are checked. Concurrent ownership, missing state or failed recovery leaves
an actionable failed execution with its durable request/evidence. Software tests
cannot make arbitrary production regressions impossible; unsupported transition
adapters and missing prerequisites deliberately block promotion.

Remaining rollout qualification: deploy/configure isolated UAT and its typed
isolation adapter; publish image provenance; qualify functional measurement
endpoints; add native-package and single-writer traffic-cutover adapters; and
exercise complete restart/rollback/reconciliation against isolated live targets.
The source implementation is not evidence that these live transitions passed.
