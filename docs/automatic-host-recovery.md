# Automatic physical-host recovery

Conductor provides a persistent automatic host-recovery loop when
`host_recovery.enabled` is true and one or more device IDs are explicitly
allowlisted. Continuum remains the infrastructure control plane: Conductor
collects its typed inventory, DHCP, Tracey, controller and Gail trainer-status
observations, then submits eligible restarts only through Continuum's audited
controller-action API.

The loop evaluates each configured host on a bounded interval. It requires a
fresh verified device/controller relationship, fresh Tracey offline evidence,
no current DHCP lease, fresh BMC chassis and sensor data, a readable system
event log, a known mapping for every hosted service, failed live probes for
those services, a complete configured dependency map with healthy live
probes, healthy or explicitly unavailable Prometheus/Grafana observations,
and a successful Gail trainer-status read showing no active job requesting the
host. Missing, stale, ambiguous or contradictory inputs suppress the action
and are exposed as blockers. A critical `SYS_V3_Battery` reading is a warning
for a warm restart but blocks a cold restart; other critical sensors and an
unavailable system event log block either restart.

Each allowlisted target maps Continuum's exact service labels to Conductor
`service_key` values. Dependencies map exact Continuum device IDs to one or
more Conductor service keys. An empty dependency list is accepted only when
`allow_empty_dependencies` is explicitly true. The selected action must be
`warm_restart` or `cold_restart`; the loop never silently escalates from one
to the other. Continuum performs its own fresh identity, inventory, power,
dependency, monitoring and audit checks before touching the BMC.

Before an action, Conductor durably records the request ID, payload digest,
inventory revision, typed preflight, scheduler evidence and offline-episode
identity. Request IDs are stable for the same host, inventory revision,
selected action and last-seen timestamp. An action intent or uncertain result
is never replayed for that offline episode. The loop waits for the host to
return and verifies mapped services, dependencies and monitoring before
reporting recovery. The authenticated read-only status endpoint is
`GET /api/v1/host-recovery`.

The code defaults to disabled and dry-run. The production pilot allowlists
qc04 with `cold_restart`, because the owner explicitly requested a BMC power
cycle for that host. The current pilot intentionally has empty service and
dependency maps and does not permit empty dependencies, so it reports blocked
state without submitting a BMC action until those mappings are verified and
filled in. Gail's current trainer-status proxy returns HTTP 404, qc04's BMC
event log is unavailable, and its dependency inventory is empty; these remain
independent fail-closed blockers. No live controller action is claimed by
this document.
