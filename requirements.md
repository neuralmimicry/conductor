Overview: Improve Conductor through the Conductor execution loop.

Delivery Context:
- Current stage: development
- Validated stages: none
- Rollout strategy: canary

Requirements Register:
- REQ-001: Inspect and record current repository, runtime, or job evidence before selecting an operation.
- REQ-002: Implement only the scoped change, job update, or progress-monitoring action supported by that evidence.
- REQ-003: Preserve secure, resilient behaviour and avoid destructive commands.
- REQ-004: Update or add tests covering the changed path, or provide the relevant live operational check.
- REQ-005: Run verification commands and report the outcome.
- REQ-006: Leave unrelated files untouched.
- REQ-007: Record rollback/recovery steps and the acceptance signal proving the gap is closed.
- REQ-008: Preserve staged progression and rollout governance metadata.
- REQ-009: Capture a fresh protected-target readiness baseline before any change.
- REQ-010: Use the selected canary or red-green rollout strategy and verify the post-rollout health window.
- REQ-011: Automatically revert the exact produced commit without rewriting history if health or verification degrades.
- REQ-012: Verify rollback readiness and recovery before finalising the delivery.
- REQ-013: When runtime rollout or restart work is needed, use the available Ansible automation context: {"ansible_root":"/srv/swarmhpc/ansible","config_path":"/srv/swarmhpc/ansible/ansible.cfg","host_targets":["rk1"],"hosts":["spirit"],"inventory_path":"/srv/swarmhpc/ansible/inventory/hosts.ini","playbooks":["continuum_tenant_conductor_site.yml"],"repo_root":"/srv/swarmhpc","roles_path":"/srv/swarmhpc/ansible/roles","secrets_root":"/srv/swarmhpc/ansible/.secrets"}.

Work Item Summary:
Prometheus reports 1/1 scrape target(s) down for Conductor. Restore exporter coverage or scrape reachability so Conductor can weigh live runtime evidence when prioritising improvement work.

Authoritative delivery constraints (mandatory; implement and verify these, do not merely describe them):
- No structured delivery constraints were supplied; follow the work-item summary exactly.

Plan JSON:
{"action":"restore_observability_coverage","down_targets":1,"finding_id":"2fd5559b-e8fa-4048-8658-901ea5c5027b","finding_key":"prometheus_target_health:conductor","service":"conductor","total_targets":1}

Planner guidance (advisory; it must not weaken or contradict the authoritative work-item requirements):
Overview: This work item addresses the loss of observability coverage for the Conductor service, which is currently reporting 1/1 scrape targets down. The objective is to restore runtime visibility by verifying the exporter, checking network reachability, and updating monitoring configuration to ensure Conductor can weigh live evidence when prioritising improvement work.

Requirements Register:
- REQ-001: Restore Prometheus scrape coverage for Conductor by identifying and repairing the failed target path.
- REQ-002: Ensure the Conductor service is reachable and responding correctly after the monitoring fix.
- REQ-003: Validate that the Ansible playbook syntax is correct and targets are properly configured before rollout.
- REQ-004: Implement a canary rollout strategy to deploy the monitoring fix to `rk1` first, then `spirit`.
- REQ-005: Verify that the fix does not introduce new degradation or service instability.
- REQ-006: Maintain a 24-hour post-rollout monitoring window to confirm sustained health before full promotion.

Rollout Notes: The change scope is limited to monitoring configuration and verification steps. No service code changes are required. Ensure protected-target readiness is maintained throughout the process. Automatic rollback should be triggered if degradation is observed during the canary or full rollout phases.


Protected rollout contract (mandatory): capture a fresh readiness baseline before any change; use the selected canary or red_green strategy; verify health throughout the post-rollout window; if health or verification degrades, automatically revert the exact produced commit without rewriting history, rerun tests and GitHub Actions, and verify recovery.