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
Make Conductor continuously reconcile Ansible intent with live services, hosts, GPUs, RAM, and node reappearance so resource changes become actionable findings.

Authoritative delivery constraints (mandatory; implement and verify these, do not merely describe them):
- No structured delivery constraints were supplied; follow the work-item summary exactly.

Plan JSON:
{"gap_id":"estate-discovery-resource-drift","kind":"formal_gap","outcomes":["resource fingerprint per host","drift finding with provenance","automatic re-integration after recovery"],"repositories":["conductor","swarmhpc"],"validation":["cargo test --lib","ansible-playbook --syntax-check"]}

Planner guidance (advisory; it must not weaken or contradict the authoritative work-item requirements):
Overview: This work item addresses the gap in estate discovery and resource-drift coverage for the Conductor service. The objective is to ensure Conductor continuously reconciles Ansible intent with live services, hosts, GPUs, RAM, and node reappearance, transforming resource changes into actionable findings. The implementation must be incremental, test-backed, and non-destructive to the existing codebase.

Requirements Register:
- REQ-001: Conductor must ingest live telemetry for hosts, GPUs, and RAM to establish a current resource fingerprint per host.
- REQ-002: The system must generate drift findings with provenance, linking observed state changes to specific Ansible intent deviations.
- REQ-003: Automatic re-integration logic must be implemented to handle resource recovery without manual intervention.
- REQ-004: Code changes must pass `cargo fmt --check` and `cargo check` to ensure formatting and compilation integrity.
- REQ-005: Unit tests must be updated or created to cover the new drift detection logic via `cargo test --lib`.
- REQ-006: Ansible playbooks must pass syntax checks using `ansible-playbook ... --syntax-check` before deployment.
- REQ-007: The rollout strategy must utilise a canary approach on host 'spirit' to mitigate risk during the transition.
- REQ-008: Operational notes must include a post-rollout readiness health window to verify system stability.
- REQ-009: Any degradation detected during the canary phase must trigger an automatic rollback procedure.
- REQ-010: The implementation must leave unrelated files untouched and avoid destructive commands.
- REQ-011: Verification must include a fresh protected-target readiness baseline check before finalising the gap closure.
- REQ-012: The acceptance signal is the successful generation of actionable drift findings and stable operation post-rollout.


Protected rollout contract (mandatory): capture a fresh readiness baseline before any change; use the selected canary or red_green strategy; verify health throughout the post-rollout window; if health or verification degrades, automatically revert the exact produced commit without rewriting history, rerun tests and GitHub Actions, and verify recovery.