use crate::topology_decision::{
    BlockedStage, CleanupReceipt, Direction, EdgeProbe, ImpairedEdge, Impairment, Mechanic,
    NetworkProof, NodeAddress, SCHEMA_VERSION, TopologyDecision, Verdict,
};

fn valid_pass_decision() -> TopologyDecision {
    TopologyDecision {
        schema_version: SCHEMA_VERSION,
        verdict: Verdict::Pass,
        run_id: "proofA".to_owned(),
        chosen_fabric: Some("docker-bridge".to_owned()),
        mechanic: Some(Mechanic::NetemSidecar),
        network: Some(NetworkProof {
            name: "ccstress_proofA".to_owned(),
            subnet: "172.30.0.0/24".to_owned(),
            nodes: vec![
                NodeAddress {
                    name: "probe".to_owned(),
                    ipv4: "172.30.0.2".to_owned(),
                },
                NodeAddress {
                    name: "cc-lb".to_owned(),
                    ipv4: "172.30.0.3".to_owned(),
                },
                NodeAddress {
                    name: "postgres".to_owned(),
                    ipv4: "172.30.0.4".to_owned(),
                },
            ],
        }),
        impaired_edges: vec![
            ImpairedEdge {
                edge_id: "probe-to-cc-lb".to_owned(),
                sender: "probe".to_owned(),
                receiver: "cc-lb".to_owned(),
                destination_ipv4: "172.30.0.3".to_owned(),
                direction: Direction::Egress,
                classid: "1:10".to_owned(),
                impairment: Impairment::Delay {
                    delay_ms: 40,
                    seed: 101,
                },
                tc_commands: vec![
                    "tc qdisc replace dev eth0 parent 1:10 netem delay 40ms seed 101".to_owned(),
                ],
                probe: EdgeProbe {
                    target_class_delta_packets: 16,
                    nontarget_class_delta_packets: 0,
                    opposite_class_delta_packets: 0,
                },
            },
            ImpairedEdge {
                edge_id: "cc-lb-to-postgres".to_owned(),
                sender: "cc-lb".to_owned(),
                receiver: "postgres".to_owned(),
                destination_ipv4: "172.30.0.4".to_owned(),
                direction: Direction::Egress,
                classid: "1:20".to_owned(),
                impairment: Impairment::PacketLoss {
                    percent: 25,
                    seed: 202,
                },
                tc_commands: vec![
                    "tc qdisc replace dev eth0 parent 1:20 netem loss 25% seed 202".to_owned(),
                ],
                probe: EdgeProbe {
                    target_class_delta_packets: 16,
                    nontarget_class_delta_packets: 0,
                    opposite_class_delta_packets: 0,
                },
            },
        ],
        host_published_data_ports: Vec::new(),
        blocked_stage: None,
        blocked_reason: None,
        sidecar_unworkable_reason: None,
        cleanup: CleanupReceipt {
            attempted: true,
            containers_removed: 5,
            network_removed: true,
            image_removed: true,
        },
    }
}

#[test]
fn fabric_decision_schema() {
    // Given: a concrete sidecar proof for the two required directed edges.
    let decision = valid_pass_decision();

    // When: the decision crosses the JSON evidence boundary.
    let json = serde_json::to_value(&decision).expect("decision serializes");
    let decoded: TopologyDecision = serde_json::from_value(json.clone()).expect("decision parses");

    // Then: the machine-readable proof schema is complete and valid.
    assert_eq!(json["mechanic"], "netem-sidecar");
    assert_eq!(json["impaired_edges"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        json["host_published_data_ports"].as_array().map(Vec::len),
        Some(0)
    );
    assert!(decoded.validate().is_ok());
}

#[test]
fn fabric_decision_rejects_incomplete_or_unshaped_proofs() {
    // Given: a valid decision, plus malformed pass and blocked variants.
    let mut missing_edge = valid_pass_decision();
    missing_edge.impaired_edges.pop();
    let mut duplicate_classid = valid_pass_decision();
    duplicate_classid.impaired_edges[1].classid = "1:10".to_owned();
    let mut leaky_port = valid_pass_decision();
    leaky_port
        .host_published_data_ports
        .push("5432/tcp".to_owned());
    let mut nonisolated_counter = valid_pass_decision();
    nonisolated_counter.impaired_edges[0]
        .probe
        .nontarget_class_delta_packets = 1;
    let mut missing_blocker = valid_pass_decision();
    missing_blocker.verdict = Verdict::Blocked;
    missing_blocker.chosen_fabric = None;
    missing_blocker.mechanic = None;
    missing_blocker.network = None;
    missing_blocker.impaired_edges.clear();
    missing_blocker.blocked_stage = Some(BlockedStage::Preflight);

    // When: validation checks the evidence variants.
    let missing_edge_result = missing_edge.validate();
    let duplicate_classid_result = duplicate_classid.validate();
    let leaky_port_result = leaky_port.validate();
    let nonisolated_counter_result = nonisolated_counter.validate();
    let missing_blocker_result = missing_blocker.validate();

    // Then: neither incomplete evidence shape can be accepted.
    assert!(missing_edge_result.is_err());
    assert!(duplicate_classid_result.is_err());
    assert!(leaky_port_result.is_err());
    assert!(nonisolated_counter_result.is_err());
    assert!(missing_blocker_result.is_err());
}
