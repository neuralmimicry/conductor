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
Conductor self-tests are failing or incomplete. Latest result: no executable project-native validation commands were discovered.

Authoritative delivery constraints (mandatory; implement and verify these, do not merely describe them):
- No structured delivery constraints were supplied; follow the work-item summary exactly.

Plan JSON:
{"action":"restore_conductor_self_tests","repository":"conductor","self_test":{"commands":[],"completeness":"skipped","failure_reasons":[],"planned_commands":[],"repo_path":"/srv/neuralmimicry/conductor","required_checks":[],"summary":"no executable project-native validation commands were discovered"}}

Planner guidance (advisory; it must not weaken or contradict the authoritative work-item requirements):
Overview: This work item restores the Conductor self-test baseline by generating a minimal validation infrastructure where none previously existed. The objective is to ensure Conductor continuous improvement is supported by automated, test-backed verification.

Requirements Register:
- REQ-001: Inspect and document the current repository, runtime, and job state evidence.
- REQ-002: Verify cluster and service health before implementing any remediation or test changes.
- REQ-003: Generate a validation plan using the Refiner project-solver without destructive code changes.
- REQ-004: Implement the proposed validation commands and update test coverage accordingly.
- REQ-005: Execute and record test results to confirm the baseline is restored and stable.
- REQ-006: Prepare for canary rollout by ensuring rollback readiness and monitoring coverage.

Notes:
- Priority is given to non-destructive, incremental changes that preserve existing functionality.
- Runtime health and cluster integrity must be confirmed before proceeding with test execution.
- The Refiner project-solver should be used to derive the validation plan, reducing manual inference errors.
- Test coverage should focus on the Conductor-to-Refiner contract and any observed runtime anomalies.


Protected rollout contract (mandatory): capture a fresh readiness baseline before any change; use the selected canary or red_green strategy; verify health throughout the post-rollout window; if health or verification degrades, automatically revert the exact produced commit without rewriting history, rerun tests and GitHub Actions, and verify recovery.