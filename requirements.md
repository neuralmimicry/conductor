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
- REQ-013: When runtime rollout or restart work is needed, use the available Ansible automation context: {"ansible_root":"/srv/swarmhpc/ansible","config_path":"/srv/swarmhpc/ansible/ansible.cfg","host_targets":["rk1"],"hosts":["spirit"],"inventory_path":"/srv/swarmhpc/ansible/inventory/hosts.ini","playbooks":["continuum_tenant_conductor_site.yml"],"repo_root":"/srv/swarmhpc","roles_path":"/srv/swarmhpc/ansible/roles","secrets_root":"/srv/swarmhpc/ansible/.secrets"}.

Work Item Summary:
Make Conductor continuously reconcile Ansible intent with live services, hosts, GPUs, RAM, and node reappearance so resource changes become actionable findings.

Authoritative delivery constraints (mandatory; implement and verify these, do not merely describe them):
- Required work-item outcome REQ-014: resource fingerprint per host (must be implemented and verified).
- Required work-item outcome REQ-015: drift finding with provenance (must be implemented and verified).
- Required work-item outcome REQ-016: automatic re-integration after recovery (must be implemented and verified).
- Required verification command: cargo test --lib (must be run and pass).
- Required verification command: ansible-playbook --syntax-check (must be run and pass).

Plan JSON:
{"gap_id":"estate-discovery-resource-drift","kind":"formal_gap","outcomes":["resource fingerprint per host","drift finding with provenance","automatic re-integration after recovery"],"repositories":["conductor","swarmhpc"],"validation":["cargo test --lib","ansible-playbook --syntax-check"]}