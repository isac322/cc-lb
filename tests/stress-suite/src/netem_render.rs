use std::net::Ipv4Addr;

use crate::topology_spec::{DirectedEdge, EdgeKind, TopologySpec, TopologyValidationError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedEdge {
    pub edge_id: String,
    pub sender_netns: String,
    pub destination_ipv4: Ipv4Addr,
    pub classid: String,
    pub commands: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum NetemRenderError {
    #[error(transparent)]
    InvalidTopology(#[from] TopologyValidationError),
}

pub fn render_topology(spec: &TopologySpec) -> Result<Vec<RenderedEdge>, NetemRenderError> {
    spec.validate()?;
    Ok(spec
        .edges
        .iter()
        .filter(|edge| matches!(edge.kind, EdgeKind::Data))
        .map(|edge| render_data_edge(edge, spec.seed))
        .collect())
}

fn render_data_edge(edge: &DirectedEdge, seed: u32) -> RenderedEdge {
    let class_handle = edge.classid.replace(':', "");
    let netem = edge.impairment.netem_arguments();
    RenderedEdge {
        edge_id: edge.edge_id.clone(),
        sender_netns: edge.sender_netns.clone(),
        destination_ipv4: edge.destination_ipv4,
        classid: edge.classid.clone(),
        commands: vec![
            "tc qdisc replace dev eth0 root handle 1: htb default 1".to_owned(),
            "tc class replace dev eth0 parent 1: classid 1:1 htb rate 1000mbit ceil 1000mbit"
                .to_owned(),
            format!(
                "tc class replace dev eth0 parent 1: classid {} htb rate 1000mbit ceil 1000mbit",
                edge.classid
            ),
            format!(
                "tc filter replace dev eth0 protocol ip parent 1: prio 10 u32 match ip dst {}/32 flowid {}",
                edge.destination_ipv4, edge.classid
            ),
            format!(
                "tc qdisc replace dev eth0 parent {} handle {class_handle}: netem {netem} seed {seed}",
                edge.classid
            ),
        ],
    }
}
