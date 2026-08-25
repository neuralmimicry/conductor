//! Declarative starting backlog for the platform's known self-improvement gaps.
//!
//! Discovery findings remain the source of current runtime facts. These
//! entries ensure known capability gaps cannot disappear merely because one
//! probe is healthy on one particular day. The planner reconciles each entry
//! using its stable key, so repeated cycles never create duplicate work.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImprovementGap {
    pub id: &'static str,
    pub title: &'static str,
    pub summary: &'static str,
    pub target_service: &'static str,
    pub priority: i32,
    pub tags: &'static [&'static str],
    pub depends_on: &'static [&'static str],
    pub repositories: &'static [&'static str],
    pub outcomes: &'static [&'static str],
    pub validation: &'static [&'static str],
}

/// The ten cross-product gaps identified during the platform review.
///
/// They are ordered by control-plane prerequisites: Conductor must first be
/// able to observe and safely validate change, after which runtime, serving,
/// and knowledge improvements can be delivered through the same queue.
pub const KNOWN_GAPS: &[ImprovementGap] = &[
    ImprovementGap {
        id: "estate-discovery-resource-drift",
        title: "Close estate discovery and resource-drift coverage",
        summary: "Make Conductor continuously reconcile Ansible intent with live services, hosts, GPUs, RAM, and node reappearance so resource changes become actionable findings.",
        target_service: "conductor",
        priority: 95,
        tags: &["formal_gap", "discovery", "resource_drift", "ansible"],
        depends_on: &[],
        repositories: &["conductor", "swarmhpc"],
        outcomes: &[
            "resource fingerprint per host",
            "drift finding with provenance",
            "automatic re-integration after recovery",
        ],
        validation: &["cargo test --lib", "ansible-playbook --syntax-check"],
    },
    ImprovementGap {
        id: "runtime-observability-missing-data",
        title: "Close runtime observability and missing-data paths",
        summary: "Ensure Tracey, Prometheus, Grafana, and each application emit meaningful health, throughput, latency, restart, and missing-data signals that Conductor can plan from.",
        target_service: "tracey",
        priority: 92,
        tags: &[
            "formal_gap",
            "observability",
            "tracey",
            "grafana",
            "prometheus",
        ],
        depends_on: &["estate-discovery-resource-drift"],
        repositories: &["tracey", "swarmhpc", "conductor"],
        outcomes: &[
            "consistent application dashboards",
            "explicit no-data reasons",
            "scrape-to-application backtrace",
        ],
        validation: &["cargo test --lib", "promtool check rules"],
    },
    ImprovementGap {
        id: "resilience-node-reintegration",
        title: "Close node failure and automatic reintegration coverage",
        summary: "Make node loss, reboot, GPU replacement, and changed resources safe for serving and training, with health caches invalidated and capacity recalculated on return.",
        target_service: "swarmhpc",
        priority: 91,
        tags: &["formal_gap", "resilience", "node_recovery", "gpu"],
        depends_on: &["estate-discovery-resource-drift"],
        repositories: &["swarmhpc", "gail", "tracey"],
        outcomes: &[
            "short-lived failure state",
            "resource-aware reintegration",
            "no stale capacity routing",
        ],
        validation: &[
            "ansible-playbook --syntax-check",
            "curl readiness and provider probes",
        ],
    },
    ImprovementGap {
        id: "adaptive-provider-capacity",
        title: "Close adaptive provider capacity and queue scheduling coverage",
        summary: "Route work by measured useful throughput, queue wait, request type, and response quality so every usable provider can work in parallel without rewarding fast but useless responses.",
        target_service: "gail",
        priority: 90,
        tags: &[
            "formal_gap",
            "gail",
            "adaptive_routing",
            "capacity",
            "latency",
        ],
        depends_on: &["runtime-observability-missing-data"],
        repositories: &["gail", "tracey"],
        outcomes: &[
            "per-model capacity estimate",
            "quality-aware admission",
            "latency-aware spillover",
        ],
        validation: &["cargo test --lib", "provider parallel-request smoke test"],
    },
    ImprovementGap {
        id: "aarnn-biological-readiness",
        title: "Close AARNN biological readiness and trust integration",
        summary: "Respect hidden-to-output synaptic growth requirements while exposing readiness and quality evidence so AARNN only enters the response pool after biological connectivity exists.",
        target_service: "aarnn",
        priority: 87,
        tags: &["formal_gap", "aarnn", "biological_readiness", "quality"],
        depends_on: &["adaptive-provider-capacity"],
        repositories: &["aarnn_rust", "gail"],
        outcomes: &[
            "connectivity-aware readiness",
            "quality-gated pool admission",
            "stimulus-driven growth telemetry",
        ],
        validation: &[
            "cargo test --lib",
            "readiness probe with disconnected outputs",
        ],
    },
    ImprovementGap {
        id: "training-model-placement",
        title: "Close training completion and model placement coverage",
        summary: "Ensure Slurm training completes, the newest local model is validated, and exactly one promoted version is placed on the largest available VRAM target with throughput tie-breaking.",
        target_service: "gail",
        priority: 89,
        tags: &["formal_gap", "training", "slurm", "model_promotion", "vram"],
        depends_on: &[
            "resilience-node-reintegration",
            "runtime-observability-missing-data",
        ],
        repositories: &["gail", "swarmhpc"],
        outcomes: &[
            "successful Slurm lifecycle",
            "single active local model",
            "validated largest-VRAM placement",
        ],
        validation: &[
            "cargo test --lib",
            "squeue and training heartbeat validation",
        ],
    },
    ImprovementGap {
        id: "validation-release-governance",
        title: "Close independent validation and safe release coverage",
        summary: "Make every Conductor change pass product-native QA, independent validation, CI evidence, and staged rollout gates before it can affect the platform.",
        target_service: "conductor",
        priority: 94,
        tags: &["formal_gap", "validation", "governance", "ci", "refiner"],
        depends_on: &["estate-discovery-resource-drift"],
        repositories: &["conductor", "rag_demo", "swarmhpc"],
        outcomes: &[
            "repeatable QA contract",
            "CI-backed evidence",
            "safe canary and rollback",
        ],
        validation: &["cargo test --all-targets", "gh run watch"],
    },
    ImprovementGap {
        id: "traceability-atlassian-knowledge",
        title: "Close cross-system traceability and knowledge coverage",
        summary: "Correlate findings, changes, deployments, incidents, Jira, Confluence, and runtime outcomes into an auditable estate graph usable by operators and planners.",
        target_service: "conductor",
        priority: 82,
        tags: &["formal_gap", "traceability", "atlassian", "knowledge_graph"],
        depends_on: &["validation-release-governance"],
        repositories: &["conductor", "jirastats", "rag_demo", "tracey"],
        outcomes: &[
            "automatic evidence links",
            "ownership-aware graph",
            "incident-to-change correlation",
        ],
        validation: &["cargo test --lib", "traceability graph API smoke test"],
    },
    ImprovementGap {
        id: "research-programme-planning",
        title: "Close provenance-aware research and programme planning coverage",
        summary: "Give Conductor a reusable research path through Gail and Refiner that turns evidence into dependency-aware, explainable multi-product improvement plans.",
        target_service: "conductor",
        priority: 80,
        tags: &["formal_gap", "research", "planning", "gail", "refiner"],
        depends_on: &["traceability-atlassian-knowledge"],
        repositories: &["conductor", "gail", "rag_demo", "jirastats"],
        outcomes: &[
            "provenance-aware recommendations",
            "programme-level dependencies",
            "measured outcome learning",
        ],
        validation: &["cargo test --lib", "planning cycle with Gail advisory"],
    },
    ImprovementGap {
        id: "platform-self-improvement-loop",
        title: "Close the governed platform self-improvement loop",
        summary: "Prove Conductor can discover an opportunity, plan it, obtain policy approval, execute through Refiner, validate the result, and learn from the measured outcome.",
        target_service: "conductor",
        priority: 98,
        tags: &["formal_gap", "conductor", "self_improvement", "end_to_end"],
        depends_on: &[
            "validation-release-governance",
            "research-programme-planning",
        ],
        repositories: &["conductor", "rag_demo", "gail", "tracey", "swarmhpc"],
        outcomes: &[
            "end-to-end governed improvement",
            "outcome feedback into planning",
            "safe recovery on failed rollout",
        ],
        validation: &[
            "cargo test --all-targets",
            "discovery-to-execution integration test",
        ],
    },
];

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::KNOWN_GAPS;

    #[test]
    fn catalogue_contains_ten_unique_stable_work_items() {
        assert_eq!(KNOWN_GAPS.len(), 10);
        let keys = KNOWN_GAPS.iter().map(|gap| gap.id).collect::<HashSet<_>>();
        assert_eq!(keys.len(), KNOWN_GAPS.len());
        assert!(KNOWN_GAPS.iter().all(|gap| !gap.repositories.is_empty()));
        assert!(KNOWN_GAPS.iter().all(|gap| !gap.validation.is_empty()));
    }
}
