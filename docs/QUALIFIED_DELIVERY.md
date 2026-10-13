# Qualified immutable delivery

Conductor owns UAT/production stages; Refiner implements and tests improvements
using project_solver, research, NMIDE and Gail-assisted agents as appropriate.
Successful Refiner jobs can return `release_candidate` at the root or under
`result`; Conductor persists that proposal in the work-item plan with its origin.
It is untrusted until independent qualification completes.

`delivery.qualified_releases` is an operator-owned map keyed by `owner/repository`.
Each entry fixes the workflow, all required validation/build/package job names,
complete UAT/production coordinates and known-good rollback images, and the acceptance contract. An empty map blocks
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

The release producer now calls the reusable dual-architecture CI workflow before
Debian builds. It uses the commit-pinned GitHub provenance action to attest each
Debian package, each architecture image and the published index using their actual
registry digests. Signing authority is scoped to producer jobs; ordinary validation
remains read-only. Release publication is serialized and refuses an existing tag
for another source or an existing asset with different/missing digest evidence.
Tag resolution explicitly selects `refs/tags`, avoiding branch/tag ambiguity.
These producer changes require a successful canonical release run before any new
package/provenance can be accepted.

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

Conductor also reviews Continuum's authenticated `/devices/recovery-history`
after startup and every five minutes, even when host mutation policy is disabled.
Each warm restart produces one durable, deduplicated investigation with the
recovery identity, affected services, correlated Tracey logs, and bounded
previous/current workload-log references. Missing/corrupt/truncated audit history
creates an evidence-gap work item. Findings are investigation signals, never an
automatic root-cause claim, second reset or production approval.

Prometheus range queries cover the restart hour before/after it: host probes,
node readiness/pressure, available memory/filesystem space, boot time and estate
ingress 5xx. Missing series, non-finite samples and gaps remain visible. Grafana
annotations must match `host:DEVICE` and the time window. The existing operator
integration URLs and credentials are used; HTTP responses have size/deadline
limits and redirects are rejected. Monitoring availability does not close the
recurrence investigation.

Optional `host_recovery.review_ssh` entries separately allowlist read-only journal
collection, without enabling any restart action:

```yaml
host_recovery:
  review_ssh:
    - device_id: qc01
      address: 192.168.1.61
      username: pbisaacs
      identity_file: /run/secrets/restart-review-ssh
      known_hosts_file: /etc/conductor/restart-review-known-hosts
```

The IP must match the unique device in fresh Continuum inventory. Fixed SSH uses
only the approved key/known-host file, strict verification, BatchMode and no agent
forwarding. Fixed read-only `sudo -n journalctl` commands query previous-boot
warnings/kernel and current-boot warnings with numeric time bounds. Each source
has a ten-second, 256 KiB, 200-record limit; sensitive excerpts are redacted.
Boot IDs and truncation/gaps are recorded. Empty journals do not prove complete
previous-boot coverage; persistent journals/retention still need qualification.

Production re-fetches fresh complete recovery history and requires a successful
work item with `plan.review_complete=true` for every recorded warm restart.
Any open restart-review work also holds promotion. Completing the review requires
independent evidence for recurrence prevention, remaining faults, owners and
scoped follow-ups through the governed workflow.
