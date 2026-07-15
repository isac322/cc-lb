use crate::docker::Docker;
use crate::fabric_commands::observer_commands;
use crate::fabric_inspect::host_published_data_ports;
use crate::fabric_probe::{EdgeRun, measure_edge};
use crate::fabric_runtime::{
    build_helper_image, check_sidecar_capabilities, create_network, install_commands, start_nodes,
    start_sidecars, start_udp_sinks, warm_sender_path,
};
use crate::fabric_state::{
    CCLB_IP, FabricCleanup, POSTGRES_IP, PROBE_IP, ResourceNames, SUBNET, blocked, node,
};
use crate::netem_render::render_topology;
use crate::topology_decision::{
    CleanupReceipt, Mechanic, NetworkProof, SCHEMA_VERSION, TopologyDecision, Verdict,
};
use crate::topology_spec::{
    DirectedEdge, Direction as SpecDirection, EdgeKind, Impairment as SpecImpairment,
    TOPOLOGY_SCHEMA_VERSION, TopologySpec, Transport,
};
use std::net::Ipv4Addr;

const PROBE_DELAY_SEED: u32 = 101;
const CCLB_LOSS_SEED: u32 = 202;

pub fn prove(run_id: &str) -> TopologyDecision {
    let names = match ResourceNames::new(run_id) {
        Ok(names) => names,
        Err(reason) => return blocked(run_id, reason, CleanupReceipt::empty()),
    };
    let docker = Docker;
    prove_with_names(&docker, &names, run_id)
}

pub fn prove_with_names(docker: &Docker, names: &ResourceNames, run_id: &str) -> TopologyDecision {
    let mut cleanup = FabricCleanup::new(docker, names, run_id);
    let result = prove_sidecar(docker, names, run_id);
    let receipt = cleanup.cleanup();

    match result {
        Ok(mut decision) => {
            decision.cleanup = receipt;
            if let Err(error) = decision.validate() {
                return blocked(
                    run_id,
                    format!("proof validation failed: {error}"),
                    decision.cleanup,
                );
            }
            decision
        }
        Err(reason) => blocked(run_id, reason, receipt),
    }
}

fn prove_sidecar(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
) -> Result<TopologyDecision, String> {
    build_helper_image(docker, names, run_id)?;
    check_sidecar_capabilities(docker, names, run_id)?;
    create_network(docker, names, run_id)?;
    start_nodes(docker, names, run_id)?;
    warm_sender_path(docker, names, run_id, &names.probe, CCLB_IP)?;
    warm_sender_path(docker, names, run_id, &names.cc_lb, POSTGRES_IP)?;
    start_sidecars(docker, names, run_id)?;
    start_udp_sinks(docker, names)?;

    let rendered = render_preflight_commands()?;
    let probe_commands = rendered.probe_commands;
    let cc_lb_commands = rendered.cc_lb_commands;
    install_commands(docker, &names.probe_sidecar, &probe_commands)?;
    install_commands(docker, &names.cc_lb_sidecar, &cc_lb_commands)?;
    install_commands(
        docker,
        &names.postgres_counter,
        &observer_commands(CCLB_IP, "1:30"),
    )?;

    let host_published_data_ports = host_published_data_ports(docker, names)?;
    if !host_published_data_ports.is_empty() {
        return Err("proof containers unexpectedly published data ports".to_owned());
    }

    let delay_edge = measure_edge(
        docker,
        run_id,
        &EdgeRun::delay(
            &names.probe,
            &names.probe_sidecar,
            &names.cc_lb_sidecar,
            PROBE_DELAY_SEED,
            probe_commands,
        ),
    )?;
    let loss_edge = measure_edge(
        docker,
        run_id,
        &EdgeRun::packet_loss(
            &names.cc_lb,
            &names.cc_lb_sidecar,
            &names.postgres_counter,
            CCLB_LOSS_SEED,
            cc_lb_commands,
        ),
    )?;

    Ok(TopologyDecision {
        schema_version: SCHEMA_VERSION,
        verdict: Verdict::Pass,
        run_id: run_id.to_owned(),
        chosen_fabric: Some("docker-bridge".to_owned()),
        mechanic: Some(Mechanic::NetemSidecar),
        network: Some(NetworkProof {
            name: names.network.clone(),
            subnet: SUBNET.to_owned(),
            nodes: vec![
                node("probe", PROBE_IP),
                node("cc-lb", CCLB_IP),
                node("postgres", POSTGRES_IP),
            ],
        }),
        impaired_edges: vec![delay_edge, loss_edge],
        host_published_data_ports,
        blocked_stage: None,
        blocked_reason: None,
        sidecar_unworkable_reason: None,
        cleanup: CleanupReceipt::empty(),
    })
}

struct PreflightCommands {
    probe_commands: Vec<String>,
    cc_lb_commands: Vec<String>,
}

fn render_preflight_commands() -> Result<PreflightCommands, String> {
    let mut probe_commands = render_edge_commands(
        DirectedEdge {
            edge_id: "probe-to-cc-lb".to_owned(),
            kind: EdgeKind::Data,
            sender: "probe".to_owned(),
            receiver: "cc-lb".to_owned(),
            direction: SpecDirection::Egress,
            transport: Transport::Bridge,
            impairment_id: "probe-to-cc-lb".to_owned(),
            impairment: SpecImpairment::Delay { delay_ms: 40 },
            sender_netns: "probe".to_owned(),
            destination_ipv4: Ipv4Addr::new(172, 30, 0, 3),
            classid: "1:10".to_owned(),
        },
        PROBE_DELAY_SEED,
    )?;
    append_class(&mut probe_commands, "1:11");
    let mut cc_lb_commands = render_edge_commands(
        DirectedEdge {
            edge_id: "cc-lb-to-postgres".to_owned(),
            kind: EdgeKind::Data,
            sender: "cc-lb".to_owned(),
            receiver: "postgres".to_owned(),
            direction: SpecDirection::Egress,
            transport: Transport::Bridge,
            impairment_id: "cc-lb-to-postgres".to_owned(),
            impairment: SpecImpairment::PacketLoss { percent: 10 },
            sender_netns: "cc-lb".to_owned(),
            destination_ipv4: Ipv4Addr::new(172, 30, 0, 4),
            classid: "1:20".to_owned(),
        },
        CCLB_LOSS_SEED,
    )?;
    append_class(&mut cc_lb_commands, "1:21");
    Ok(PreflightCommands {
        probe_commands,
        cc_lb_commands,
    })
}

fn render_edge_commands(edge: DirectedEdge, seed: u32) -> Result<Vec<String>, String> {
    let topology = TopologySpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        profile: "docker-netem-preflight".to_owned(),
        seed,
        edges: vec![edge],
    };
    let mut rendered = render_topology(&topology).map_err(|error| error.to_string())?;
    let Some(edge) = rendered.pop() else {
        return Err("preflight topology rendered no data edge".to_owned());
    };
    Ok(edge.commands)
}

fn append_class(commands: &mut Vec<String>, classid: &str) {
    commands.push(format!(
        "tc class replace dev eth0 parent 1: classid {classid} htb rate 1000mbit ceil 1000mbit"
    ));
}
