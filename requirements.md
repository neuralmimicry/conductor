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
conductor is linked to live services but no obvious test capability was discovered in the repository inventory. Establish at least a minimal regression or smoke-test baseline before deeper autonomous changes.

Authoritative delivery constraints (mandatory; implement and verify these, do not merely describe them):
- No structured delivery constraints were supplied; follow the work-item summary exactly.

Plan JSON:
{"action":"establish_repository_test_baseline","finding_id":"71cf72ad-ffa1-4c0d-a545-7e8d9f6ba55b","finding_key":"repository_test_baseline:neuralmimicry/conductor","linked_services":["conductor"],"repository":"neuralmimicry/conductor"}

Planner guidance (advisory; it must not weaken or contradict the authoritative work-item requirements):
Overview: This work item establishes a minimal regression and smoke-test baseline for the Conductor service, which is linked to live services but currently lacks discoverable test coverage. The goal is to verify repository and runtime integrity, validate infrastructure configurations, and confirm operational health before deeper autonomous changes.

Requirements Register:
- REQ-001: Inspect the Conductor repository at /srv/neuralmimicry/conductor to confirm path accessibility, branch state, and file structure.
- REQ-002: Query the local K3s cluster status using kubectl get nodes to verify control-plane connectivity and worker node probes.
- REQ-003: Review service health logs and metrics on host 'spirit' to identify specific degradation causes before proceeding with remediation.
- REQ-004: Execute ansible-playbook /srv/swarmhpc/ansible/playbooks/continuum_tenant_conductor_site.yml --check to validate Ansible syntax and inventory without applying changes.
- REQ-005: Run cargo fmt --check and cargo check to verify Rust codebase integrity and build readiness.
- REQ-006: Execute cargo test to establish a baseline passing test count and identify any pre-existing failures.
- REQ-007: Assess evidence sufficiency: if logs indicate acute failure, proceed with targeted remediation; otherwise, recommend further investigation before implementation.
- REQ-008: Document findings, uncertainty, and recommended next steps in the repository or runbook.

Rollout and Operational Notes:
- This plan focuses on verification and baseline establishment; no destructive code or infrastructure changes are included.
- Ansible syntax validation is performed in check mode to avoid unintended runtime impacts.
- Container and service health are reviewed before deciding on further intervention.
- If acute degradation is observed, prioritize targeted remediation over baseline establishment.
- If evidence is insufficient, recommend additional investigation before implementation.
- Rollout strategy (canary) is noted for future deployment phases once test coverage is confirmed.


Protected rollout contract (mandatory): capture a fresh readiness baseline before any change; use the selected canary or red_green strategy; verify health throughout the post-rollout window; if health or verification degrades, automatically revert the exact produced commit without rewriting history, rerun tests and GitHub Actions, and verify recovery.