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
Conductor self-tests are failing or incomplete. Latest result: independent validation failed: 2 passed, 1 failed, 0 unavailable.

Authoritative delivery constraints (mandatory; implement and verify these, do not merely describe them):
- No structured delivery constraints were supplied; follow the work-item summary exactly.

Plan JSON:
{"action":"restore_conductor_self_tests","repository":"conductor","self_test":{"commands":[{"command":"cargo test","duration_ms":1200002,"exit_code":null,"reason":"cargo test exceeded the validation timeout of 1200 seconds","status":"timeout","stderr_excerpt":null,"stdout_excerpt":null}],"completeness":"full","failure_reasons":["cargo test exceeded the validation timeout of 1200 seconds"],"planned_commands":["cargo fmt --check","cargo check","cargo test"],"repo_path":"/srv/neuralmimicry/conductor","required_checks":["cargo fmt --check","cargo check","cargo test"],"summary":"independent validation failed: 2 passed, 1 failed, 0 unavailable"}}

Planner guidance (advisory; it must not weaken or contradict the authoritative work-item requirements):
Overview: This work item addresses the failure of Conductor self-tests caused by a timeout during the `cargo test` execution. The approach prioritises inspecting the current state, optimising the test execution to prevent timeouts, and validating the fix via a canary rollout to ensure system stability.

Requirements Register:
- REQ-001: Inspect repository state and logs to confirm the `cargo test` timeout and gather evidence of failure.
- REQ-002: Execute `cargo fmt --check` to ensure code style compliance without triggering the failing test suite.
- REQ-003: Execute `cargo check` to verify compilation status and identify any potential issues before running tests.
- REQ-004: Modify the test execution strategy to resolve the timeout, such as increasing the timeout or parallelising tests.
- REQ-005: Run `cargo test` in a controlled environment to validate that the timeout issue is resolved.
- REQ-006: Execute a canary staged rollout using the Ansible playbook to deploy the fix to the spirit host.
- REQ-007: Perform post-rollout readiness health checks to confirm Conductor self-tests are passing.
- REQ-008: Verify the acceptance signal by confirming the test suite completes within the expected duration.
- REQ-009: Ensure no destructive changes are made to unrelated files or components during the implementation.
- REQ-010: Document the resolution and prepare for full rollout if the canary phase is successful.
- REQ-011: Implement automatic rollback on degradation if the canary rollout fails or causes instability.
- REQ-012: Maintain a fresh protected-target readiness baseline throughout the implementation process.


Protected rollout contract (mandatory): capture a fresh readiness baseline before any change; use the selected canary or red_green strategy; verify health throughout the post-rollout window; if health or verification degrades, automatically revert the exact produced commit without rewriting history, rerun tests and GitHub Actions, and verify recovery.