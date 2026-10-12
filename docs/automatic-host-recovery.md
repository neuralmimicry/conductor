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

The code defaults to disabled and dry-run. qc04's critical `SYS_V3_Battery`
sensor blocks a cold restart. The owner has requested a warm restart as the
interim recovery candidate; it remains blocked until current Gail/Slurm,
service, dependency, monitoring, Tracey and Continuum controller evidence is
complete. The current pilot has no verified service/dependency maps, Gail's
trainer-status route returned HTTP 404, and qc04's system event log was
unavailable. No live controller action is claimed by this document.

Battery replacement is a different operation from host recovery. qc04 and
qc05 share chassis power, so maintenance must be planned for both modules.
See [shared-chassis maintenance planning](SHARED_CHASSIS_MAINTENANCE.md) for
the dashboard, approval state and the still-missing Continuum evacuation and
restore operation.
