use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Verdict {
    Pass,
    Blocked,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mechanic {
    NetemSidecar,
    Router,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Egress,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockedStage {
    Preflight,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Impairment {
    Delay { delay_ms: u64, seed: u32 },
    PacketLoss { percent: u8, seed: u32 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EdgeProbe {
    pub target_class_delta_packets: u64,
    pub nontarget_class_delta_packets: u64,
    pub opposite_class_delta_packets: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImpairedEdge {
    pub edge_id: String,
    pub sender: String,
    pub receiver: String,
    pub destination_ipv4: String,
    pub direction: Direction,
    pub classid: String,
    pub impairment: Impairment,
    pub tc_commands: Vec<String>,
    pub probe: EdgeProbe,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeAddress {
    pub name: String,
    pub ipv4: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NetworkProof {
    pub name: String,
    pub subnet: String,
    pub nodes: Vec<NodeAddress>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CleanupReceipt {
    pub attempted: bool,
    pub containers_removed: usize,
    pub network_removed: bool,
    pub image_removed: bool,
}

impl CleanupReceipt {
    pub const fn empty() -> Self {
        Self {
            attempted: true,
            containers_removed: 0,
            network_removed: false,
            image_removed: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TopologyDecision {
    pub schema_version: u16,
    pub verdict: Verdict,
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chosen_fabric: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mechanic: Option<Mechanic>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkProof>,
    #[serde(default)]
    pub impaired_edges: Vec<ImpairedEdge>,
    #[serde(default)]
    pub host_published_data_ports: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_stage: Option<BlockedStage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sidecar_unworkable_reason: Option<String>,
    pub cleanup: CleanupReceipt,
}

#[derive(Debug, thiserror::Error)]
pub enum TopologyDecisionError {
    #[error("unsupported topology decision schema version {found}")]
    UnsupportedSchema { found: u16 },
    #[error("pass topology decision violated {invariant}")]
    PassInvariant { invariant: &'static str },
    #[error("blocked topology decision violated {invariant}")]
    BlockedInvariant { invariant: &'static str },
}

impl TopologyDecision {
    pub fn validate(&self) -> Result<(), TopologyDecisionError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(TopologyDecisionError::UnsupportedSchema {
                found: self.schema_version,
            });
        }

        match self.verdict {
            Verdict::Pass => self.validate_pass(),
            Verdict::Blocked => self.validate_blocked(),
        }
    }

    fn validate_pass(&self) -> Result<(), TopologyDecisionError> {
        if self.chosen_fabric.as_deref().is_none_or(str::is_empty) {
            return Err(pass_invariant("chosen_fabric"));
        }
        if self.mechanic.is_none() {
            return Err(pass_invariant("mechanic"));
        }
        if self.network.is_none() {
            return Err(pass_invariant("network"));
        }
        if self.impaired_edges.len() != 2 {
            return Err(pass_invariant("exactly_two_impaired_edges"));
        }
        if !self.host_published_data_ports.is_empty() {
            return Err(pass_invariant("no_host_published_data_ports"));
        }
        if self.blocked_stage.is_some() || self.blocked_reason.is_some() {
            return Err(pass_invariant("no_blocked_fields"));
        }
        if !self.cleanup.attempted {
            return Err(pass_invariant("cleanup_attempted"));
        }

        let classids = self
            .impaired_edges
            .iter()
            .map(|edge| edge.classid.as_str())
            .collect::<HashSet<_>>();
        if classids.len() != self.impaired_edges.len()
            || self
                .impaired_edges
                .iter()
                .any(|edge| edge.classid.is_empty())
        {
            return Err(pass_invariant("unique_classids"));
        }

        for edge in &self.impaired_edges {
            if edge.direction != Direction::Egress {
                return Err(pass_invariant("edge_direction"));
            }
            if edge.destination_ipv4.is_empty() || edge.tc_commands.is_empty() {
                return Err(pass_invariant("edge_destination_and_tc_commands"));
            }
            if edge.probe.target_class_delta_packets == 0
                || edge.probe.nontarget_class_delta_packets != 0
                || edge.probe.opposite_class_delta_packets != 0
            {
                return Err(pass_invariant("isolated_counter_deltas"));
            }
            for command in &edge.tc_commands {
                let contains_netem = command.split_whitespace().any(|part| part == "netem");
                let contains_seed = command.split_whitespace().any(|part| part == "seed");
                if contains_netem != contains_seed {
                    return Err(pass_invariant("netem_seed_placement"));
                }
            }
        }

        let has_delay = self.impaired_edges.iter().any(|edge| {
            edge.sender == "probe"
                && edge.receiver == "cc-lb"
                && matches!(edge.impairment, Impairment::Delay { .. })
        });
        let has_packet_loss = self.impaired_edges.iter().any(|edge| {
            edge.sender == "cc-lb"
                && edge.receiver == "postgres"
                && matches!(edge.impairment, Impairment::PacketLoss { .. })
        });
        if !has_delay || !has_packet_loss {
            return Err(pass_invariant("required_directed_edges"));
        }

        Ok(())
    }

    fn validate_blocked(&self) -> Result<(), TopologyDecisionError> {
        if self.blocked_stage != Some(BlockedStage::Preflight) {
            return Err(blocked_invariant("blocked_stage_preflight"));
        }
        if self
            .blocked_reason
            .as_deref()
            .is_none_or(|reason| reason.trim().is_empty())
        {
            return Err(blocked_invariant("nonempty_blocked_reason"));
        }
        if self.chosen_fabric.is_some()
            || self.mechanic.is_some()
            || self.network.is_some()
            || !self.impaired_edges.is_empty()
        {
            return Err(blocked_invariant("no_partial_fabric_claim"));
        }
        if !self.cleanup.attempted {
            return Err(blocked_invariant("cleanup_attempted"));
        }

        Ok(())
    }
}

fn pass_invariant(invariant: &'static str) -> TopologyDecisionError {
    TopologyDecisionError::PassInvariant { invariant }
}

fn blocked_invariant(invariant: &'static str) -> TopologyDecisionError {
    TopologyDecisionError::BlockedInvariant { invariant }
}
