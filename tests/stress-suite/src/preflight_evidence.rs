use serde::{Deserialize, Serialize};

use crate::topology_decision::{
    BlockedStage, CleanupReceipt, ImpairedEdge, Mechanic, NetworkProof, TopologyDecision,
};
use crate::verdict::Verdict;

pub const PREFLIGHT_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityChecks {
    pub docker: bool,
    pub cap_net_admin: bool,
    pub tc: bool,
    pub netem_seed: bool,
}

impl CapabilityChecks {
    pub const fn passed() -> Self {
        Self {
            docker: true,
            cap_net_admin: true,
            tc: true,
            netem_seed: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityFailure {
    pub checks: CapabilityChecks,
    pub reason: String,
}

impl CapabilityFailure {
    pub fn docker(reason: impl Into<String>) -> Self {
        Self::new(false, false, false, false, reason)
    }

    pub fn cap_net_admin(reason: impl Into<String>) -> Self {
        Self::new(true, false, false, false, reason)
    }

    pub fn tc(reason: impl Into<String>) -> Self {
        Self::new(true, true, false, false, reason)
    }

    pub fn netem_seed(reason: impl Into<String>) -> Self {
        Self::new(true, true, true, false, reason)
    }

    fn new(
        docker: bool,
        cap_net_admin: bool,
        tc: bool,
        netem_seed: bool,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            checks: CapabilityChecks {
                docker,
                cap_net_admin,
                tc,
                netem_seed,
            },
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreflightEvidence {
    pub schema_version: u16,
    pub verdict: Verdict,
    pub tier: String,
    pub run_id: String,
    pub checks: CapabilityChecks,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chosen_fabric: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mechanic: Option<Mechanic>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkProof>,
    #[serde(default)]
    pub rendered_and_installed_edges: Vec<ImpairedEdge>,
    #[serde(default)]
    pub host_published_data_ports: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_stage: Option<BlockedStage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub cleanup: CleanupReceipt,
}

impl PreflightEvidence {
    pub fn capability_blocked(
        run_id: &str,
        failure: CapabilityFailure,
        cleanup: CleanupReceipt,
    ) -> Self {
        Self {
            schema_version: PREFLIGHT_SCHEMA_VERSION,
            verdict: Verdict::Blocked,
            tier: "docker-netem".to_owned(),
            run_id: run_id.to_owned(),
            checks: failure.checks,
            chosen_fabric: None,
            mechanic: None,
            network: None,
            rendered_and_installed_edges: Vec::new(),
            host_published_data_ports: Vec::new(),
            blocked_stage: Some(BlockedStage::Preflight),
            blocked_reason: Some(failure.reason),
            cleanup,
        }
    }

    pub fn from_decision(decision: TopologyDecision) -> Self {
        match decision.verdict {
            crate::topology_decision::Verdict::Pass => pass_evidence(decision),
            crate::topology_decision::Verdict::Blocked => blocked_evidence(decision),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != PREFLIGHT_SCHEMA_VERSION {
            return Err(format!(
                "unsupported preflight schema {}",
                self.schema_version
            ));
        }
        match self.verdict {
            Verdict::Pass => validate_pass(self),
            Verdict::Blocked => validate_blocked(self),
            Verdict::Fail | Verdict::NotComparable => {
                Err("docker-netem preflight emits only PASS or BLOCKED".to_owned())
            }
        }?;
        if !self.cleanup.attempted {
            return Err("preflight cleanup must be attempted".to_owned());
        }
        Ok(())
    }
}

pub fn classify_blocked_checks(reason: Option<&str>) -> CapabilityChecks {
    let Some(reason) = reason else {
        return CapabilityChecks {
            docker: false,
            cap_net_admin: false,
            tc: false,
            netem_seed: false,
        };
    };
    if reason.contains("NET_ADMIN") {
        return CapabilityFailure::cap_net_admin(reason).checks;
    }
    if reason.contains("tc netem seed") || reason.contains("seed") {
        return CapabilityFailure::netem_seed(reason).checks;
    }
    if reason.contains("tc") {
        return CapabilityFailure::tc(reason).checks;
    }
    CapabilityChecks {
        docker: true,
        cap_net_admin: false,
        tc: false,
        netem_seed: false,
    }
}

fn pass_evidence(decision: TopologyDecision) -> PreflightEvidence {
    PreflightEvidence {
        schema_version: PREFLIGHT_SCHEMA_VERSION,
        verdict: Verdict::Pass,
        tier: "docker-netem".to_owned(),
        run_id: decision.run_id,
        checks: CapabilityChecks::passed(),
        chosen_fabric: decision.chosen_fabric,
        mechanic: decision.mechanic,
        network: decision.network,
        rendered_and_installed_edges: decision.impaired_edges,
        host_published_data_ports: decision.host_published_data_ports,
        blocked_stage: None,
        blocked_reason: None,
        cleanup: decision.cleanup,
    }
}

fn blocked_evidence(decision: TopologyDecision) -> PreflightEvidence {
    PreflightEvidence {
        schema_version: PREFLIGHT_SCHEMA_VERSION,
        verdict: Verdict::Blocked,
        tier: "docker-netem".to_owned(),
        run_id: decision.run_id,
        checks: classify_blocked_checks(decision.blocked_reason.as_deref()),
        chosen_fabric: None,
        mechanic: None,
        network: None,
        rendered_and_installed_edges: Vec::new(),
        host_published_data_ports: Vec::new(),
        blocked_stage: decision.blocked_stage,
        blocked_reason: decision.blocked_reason,
        cleanup: decision.cleanup,
    }
}

fn validate_pass(evidence: &PreflightEvidence) -> Result<(), String> {
    if !(evidence.checks.docker
        && evidence.checks.cap_net_admin
        && evidence.checks.tc
        && evidence.checks.netem_seed)
    {
        return Err("pass requires all capability checks".to_owned());
    }
    if evidence.chosen_fabric.is_none()
        || evidence.mechanic.is_none()
        || evidence.network.is_none()
        || evidence.rendered_and_installed_edges.is_empty()
    {
        return Err("pass requires fabric proof".to_owned());
    }
    Ok(())
}

fn validate_blocked(evidence: &PreflightEvidence) -> Result<(), String> {
    if evidence.blocked_stage != Some(BlockedStage::Preflight) {
        return Err("blocked preflight requires blocked_stage".to_owned());
    }
    if evidence
        .blocked_reason
        .as_deref()
        .is_none_or(|reason| reason.trim().is_empty())
    {
        return Err("blocked preflight requires reason".to_owned());
    }
    if evidence.chosen_fabric.is_some()
        || evidence.mechanic.is_some()
        || evidence.network.is_some()
        || !evidence.rendered_and_installed_edges.is_empty()
    {
        return Err("blocked preflight cannot claim partial fabric pass".to_owned());
    }
    Ok(())
}
