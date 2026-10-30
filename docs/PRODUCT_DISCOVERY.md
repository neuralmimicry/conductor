# Business product discovery

Conductor recognises Cora, Foresight, Verity and Relay as separate services and
repositories. `continuum_tenant_business_suite` is deployment automation shared
by them, not a fifth runtime service. Discovery expands the role's selected
`continuum_business_products`, deduplicates suite/individual playbooks, and
retains the product's repository, namespace, service port, capabilities and
operational dependencies. It does not invent a public ingress.

The product's own `/readyz` must report database readiness. Generic fallback to
Continuum's URL or credential is forbidden for these products: controller
health is not product health, and a controller token is not a product token.
Readiness does not qualify a forecast, approve an action or bypass release
policy. Existing actor/source permissions and Conductor approvals remain in force.

Run `cargo fmt --all -- --check` and `cargo test --locked --lib`. Regression
coverage exercises shared/per-product playbooks, deduplication, exact coordinates
and dependencies, and a deliberately incorrect Continuum URL/token that must
never receive the product probe. The source also retains the previously deployed
reset-investigation/recovery changes; replacing that runtime with older clean
main would remove existing operational behavior.

After rollout, POST the existing administrator discovery endpoint, then inspect
`GET /api/v1/services`, `/api/v1/repositories` and `/api/v1/topology`. Each product
must appear once with its own probe endpoint and source repository. A missing
source checkout is not a reason to fabricate a branch or runtime observation.
