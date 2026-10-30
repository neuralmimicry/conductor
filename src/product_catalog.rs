//! Product identities shared by service discovery, repository classification and
//! dependency planning. Recognition never grants execution or model approval.
#[derive(Clone, Copy)]
pub(crate) struct BusinessProduct {
    pub key: &'static str,
    pub name: &'static str,
    pub purpose: &'static str,
    pub capabilities: &'static [&'static str],
    pub dependencies: &'static [&'static str],
}

pub(crate) const BUSINESS_PRODUCTS: &[BusinessProduct] = &[
    BusinessProduct {
        key: "cora",
        name: "Cora",
        purpose: "permissioned_organisational_context",
        capabilities: &["temporal_context", "source_revocation", "sourced_briefings"],
        dependencies: &["customers", "postgres", "relay", "gail"],
    },
    BusinessProduct {
        key: "foresight",
        name: "Foresight",
        purpose: "qualified_numerical_planning",
        capabilities: &[
            "typed_capacity_tasks",
            "forecast_requests",
            "qualification_abstention",
        ],
        dependencies: &["customers", "postgres", "relay", "verity", "gail"],
    },
    BusinessProduct {
        key: "verity",
        name: "Verity",
        purpose: "task_specific_evidence_and_outcomes",
        capabilities: &[
            "isolated_evaluation",
            "evidence_records",
            "observed_outcomes",
        ],
        dependencies: &["customers", "postgres", "relay"],
    },
    BusinessProduct {
        key: "relay",
        name: "Relay",
        purpose: "permissioned_ingestion_and_approved_delivery",
        capabilities: &["durable_checkpoints", "refiner_mcp", "actor_bound_receipts"],
        dependencies: &["customers", "postgres", "refiner", "conductor"],
    },
];

pub(crate) fn business_product(key: &str) -> Option<&'static BusinessProduct> {
    BUSINESS_PRODUCTS.iter().find(|product| product.key == key)
}
