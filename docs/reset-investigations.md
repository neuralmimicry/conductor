# Durable reset investigations

Enable `host_recovery.automatic_restart_investigations: true` only with the owner's
authorization for automatic incident investigation and governed resolution work.
`restart_review_interval_seconds` defaults to 300 (30–3600 supported). This is
independent of the physical host-reset loop's `enabled` flag.

Conductor reads paginated Continuum reset history at startup and periodically.
The authoritative NMC instance must run outside the affected host's failure domain.
It retains warm/cold action audits and independently observed boot-ID changes.
`post-restart-review:<request_id>` persists before collection, resumes interrupted
investigations, and survives Conductor's own host reboot. Pending activities also
survive rotation of the source audit. Completed analysis and resolution are separate
checkpoints; restored availability alone does not close an incident.

The internal RCA executor gathers bounded host/workload/monitoring/BMC evidence
and asks Gail for a structured report. It also gathers the window around the last
healthy observation when the outage substantially precedes the reboot. A bounded
output allowance preserves input space for diagnostics. Raw journal messages are
redacted before inclusion. Optional `review_ssh` coordinates must match fresh
inventory and pinned known-host keys; use a dedicated forced-command identity.
SwarmHPC's `conductor_rca_evidence_site.yml` provisions this access without personal
SSH credentials or unrestricted commands.

Code/configuration proposals for known services enter the existing development
verification and Refiner delivery pipeline. Physical maintenance remains outside
the generic executor even if an approval cycle subsequently marks it approved.
Deterministic child identities prevent repeated polls from duplicating proposals.
Gail cannot mark root cause as confirmed or claim a physical repair occurred.
An incident remains open until its separate resolution activities pass acceptance.
Unavailable evidence/model dependencies retain the activity for a later retry.
