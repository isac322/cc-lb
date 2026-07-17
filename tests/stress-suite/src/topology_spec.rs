use std::collections::HashSet;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

pub const TOPOLOGY_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    Data,
    HealthProbe,
    AdminProbe,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Egress,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Bridge,
    Localhost,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Impairment {
    Delay { delay_ms: u64 },
    PacketLoss { percent: u8 },
}

impl Impairment {
    pub fn netem_arguments(&self) -> String {
        match self {
            Self::Delay { delay_ms } => format!("delay {delay_ms}ms"),
            Self::PacketLoss { percent } => format!("loss {percent}%"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DirectedEdge {
    pub edge_id: String,
    pub kind: EdgeKind,
    pub sender: String,
    pub receiver: String,
    pub direction: Direction,
    pub transport: Transport,
    pub impairment_id: String,
    pub impairment: Impairment,
    pub sender_netns: String,
    pub destination_ipv4: Ipv4Addr,
    pub classid: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TopologySpec {
    pub schema_version: u16,
    pub profile: String,
    pub seed: u32,
    pub edges: Vec<DirectedEdge>,
}

#[derive(Debug, thiserror::Error)]
pub enum TopologyValidationError {
    #[error("unsupported_topology_schema:{found}")]
    UnsupportedSchema { found: u16 },
    #[error("direct_localhost_data_edge:{edge_id}")]
    DirectLocalhostDataEdge { edge_id: String },
    #[error("duplicate_data_classid:{classid}")]
    DuplicateDataClassid { classid: String },
    #[error("missing_topology_field:{field} on {edge_id}")]
    MissingField {
        edge_id: String,
        field: &'static str,
    },
}

impl TopologySpec {
    pub fn smoke(seed: u32) -> Self {
        Self {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            profile: "smoke".to_owned(),
            seed,
            edges: vec![
                DirectedEdge {
                    edge_id: "client-to-proxy".to_owned(),
                    kind: EdgeKind::Data,
                    sender: "client".to_owned(),
                    receiver: "proxy".to_owned(),
                    direction: Direction::Egress,
                    transport: Transport::Bridge,
                    impairment_id: "client-proxy-delay".to_owned(),
                    impairment: Impairment::Delay { delay_ms: 40 },
                    sender_netns: "ccstress_smoke_client".to_owned(),
                    destination_ipv4: Ipv4Addr::new(172, 30, 0, 3),
                    classid: "1:10".to_owned(),
                },
                DirectedEdge {
                    edge_id: "proxy-to-client".to_owned(),
                    kind: EdgeKind::Data,
                    sender: "proxy".to_owned(),
                    receiver: "client".to_owned(),
                    direction: Direction::Egress,
                    transport: Transport::Bridge,
                    impairment_id: "proxy-client-loss".to_owned(),
                    impairment: Impairment::PacketLoss { percent: 5 },
                    sender_netns: "ccstress_smoke_proxy".to_owned(),
                    destination_ipv4: Ipv4Addr::new(172, 30, 0, 2),
                    classid: "1:20".to_owned(),
                },
            ],
        }
    }

    #[cfg(test)]
    pub fn localhost_data_edge() -> Self {
        Self {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            profile: "fixture".to_owned(),
            seed: 1,
            edges: vec![DirectedEdge {
                edge_id: "client-to-replica-localhost".to_owned(),
                kind: EdgeKind::Data,
                sender: "client".to_owned(),
                receiver: "replica".to_owned(),
                direction: Direction::Egress,
                transport: Transport::Localhost,
                impairment_id: "client-replica-delay".to_owned(),
                impairment: Impairment::Delay { delay_ms: 40 },
                sender_netns: "ccstress_fixture_client".to_owned(),
                destination_ipv4: Ipv4Addr::LOCALHOST,
                classid: "1:10".to_owned(),
            }],
        }
    }

    pub fn validate(&self) -> Result<(), TopologyValidationError> {
        if self.schema_version != TOPOLOGY_SCHEMA_VERSION {
            return Err(TopologyValidationError::UnsupportedSchema {
                found: self.schema_version,
            });
        }

        let mut classids = HashSet::new();
        for edge in &self.edges {
            for (field, value) in [
                ("edge_id", edge.edge_id.as_str()),
                ("sender", edge.sender.as_str()),
                ("receiver", edge.receiver.as_str()),
                ("impairment_id", edge.impairment_id.as_str()),
                ("sender_netns", edge.sender_netns.as_str()),
                ("classid", edge.classid.as_str()),
            ] {
                if value.is_empty() {
                    return Err(TopologyValidationError::MissingField {
                        edge_id: edge.edge_id.clone(),
                        field,
                    });
                }
            }
            match (edge.kind, edge.transport) {
                (EdgeKind::Data, Transport::Localhost) => {
                    return Err(TopologyValidationError::DirectLocalhostDataEdge {
                        edge_id: edge.edge_id.clone(),
                    });
                }
                (EdgeKind::Data, Transport::Bridge) => {
                    if !classids.insert(edge.classid.as_str()) {
                        return Err(TopologyValidationError::DuplicateDataClassid {
                            classid: edge.classid.clone(),
                        });
                    }
                }
                (EdgeKind::HealthProbe, Transport::Bridge | Transport::Localhost)
                | (EdgeKind::AdminProbe, Transport::Bridge | Transport::Localhost) => {}
            }
        }

        Ok(())
    }
}
