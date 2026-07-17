use serde::Deserialize;

use crate::docker::Docker;
use crate::fabric_state::{CCLB_IP, POSTGRES_IP, ResourceNames, docker_command, label};
use crate::topology_decision::{Direction, EdgeProbe, ImpairedEdge, Impairment};

pub struct EdgeRun<'a> {
    edge_id: &'static str,
    sender: &'a str,
    sender_name: &'static str,
    receiver: &'static str,
    destination_ipv4: &'static str,
    target_sidecar: &'a str,
    target_classid: &'static str,
    nontarget_classid: &'static str,
    opposite_sidecar: &'a str,
    opposite_classid: &'static str,
    impairment: Impairment,
    tc_commands: Vec<String>,
}

impl<'a> EdgeRun<'a> {
    pub fn delay(
        sender: &'a str,
        target_sidecar: &'a str,
        opposite_sidecar: &'a str,
        seed: u32,
        tc_commands: Vec<String>,
    ) -> Self {
        Self {
            edge_id: "probe-to-cc-lb",
            sender,
            sender_name: "probe",
            receiver: "cc-lb",
            destination_ipv4: CCLB_IP,
            target_sidecar,
            target_classid: "1:10",
            nontarget_classid: "1:11",
            opposite_sidecar,
            opposite_classid: "1:21",
            impairment: Impairment::Delay { delay_ms: 40, seed },
            tc_commands,
        }
    }

    pub fn packet_loss(
        sender: &'a str,
        target_sidecar: &'a str,
        opposite_sidecar: &'a str,
        seed: u32,
        tc_commands: Vec<String>,
    ) -> Self {
        Self {
            edge_id: "cc-lb-to-postgres",
            sender,
            sender_name: "cc-lb",
            receiver: "postgres",
            destination_ipv4: POSTGRES_IP,
            target_sidecar,
            target_classid: "1:20",
            nontarget_classid: "1:21",
            opposite_sidecar,
            opposite_classid: "1:30",
            impairment: Impairment::PacketLoss { percent: 10, seed },
            tc_commands,
        }
    }
}

pub fn measure_edge(
    docker: &Docker,
    run_id: &str,
    edge: &EdgeRun<'_>,
) -> Result<ImpairedEdge, String> {
    let before = counters(docker, edge)?;
    probe_traffic(
        docker,
        &ResourceNames::new(run_id)?,
        run_id,
        edge.sender,
        edge.destination_ipv4,
        "isolated sender-namespace probe",
    )?;
    let after = counters(docker, edge)?;
    let probe = EdgeProbe {
        target_class_delta_packets: delta(after.target, before.target, "target")?,
        nontarget_class_delta_packets: delta(after.nontarget, before.nontarget, "nontarget")?,
        opposite_class_delta_packets: delta(after.opposite, before.opposite, "opposite")?,
    };
    if probe.target_class_delta_packets == 0
        || probe.nontarget_class_delta_packets != 0
        || probe.opposite_class_delta_packets != 0
    {
        return Err(format!(
            "isolated counter proof failed for {}: target={}, nontarget={}, opposite={}",
            edge.edge_id,
            probe.target_class_delta_packets,
            probe.nontarget_class_delta_packets,
            probe.opposite_class_delta_packets,
        ));
    }
    Ok(ImpairedEdge {
        edge_id: edge.edge_id.to_owned(),
        sender: edge.sender_name.to_owned(),
        receiver: edge.receiver.to_owned(),
        destination_ipv4: edge.destination_ipv4.to_owned(),
        direction: Direction::Egress,
        classid: edge.target_classid.to_owned(),
        impairment: edge.impairment.clone(),
        tc_commands: edge.tc_commands.clone(),
        probe,
    })
}

pub fn probe_traffic(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
    sender: &str,
    destination_ipv4: &str,
    operation: &str,
) -> Result<(), String> {
    docker_command(
        docker,
        vec![
            "run".to_owned(),
            "--rm".to_owned(),
            "--label".to_owned(),
            label(run_id),
            "--network".to_owned(),
            format!("container:{sender}"),
            names.helper_image.clone(),
            "sh".to_owned(),
            "-ec".to_owned(),
            format!("printf fabric-proof | nc -u -w 1 {destination_ipv4} 18080"),
        ],
        operation,
    )?;
    Ok(())
}

struct CounterSet {
    target: u64,
    nontarget: u64,
    opposite: u64,
}

fn counters(docker: &Docker, edge: &EdgeRun<'_>) -> Result<CounterSet, String> {
    Ok(CounterSet {
        target: class_packets(docker, edge.target_sidecar, edge.target_classid)?,
        nontarget: class_packets(docker, edge.target_sidecar, edge.nontarget_classid)?,
        opposite: class_packets(docker, edge.opposite_sidecar, edge.opposite_classid)?,
    })
}

fn class_packets(docker: &Docker, sidecar: &str, classid: &str) -> Result<u64, String> {
    let output = docker_command(
        docker,
        vec![
            "exec".to_owned(),
            sidecar.to_owned(),
            "tc".to_owned(),
            "-s".to_owned(),
            "-j".to_owned(),
            "class".to_owned(),
            "show".to_owned(),
            "dev".to_owned(),
            "eth0".to_owned(),
        ],
        "tc class counter capture",
    )?;
    let classes: Vec<TcClass> = serde_json::from_str(&output)
        .map_err(|error| format!("tc class counter JSON parse failed: {error}"))?;
    classes
        .into_iter()
        .find(|class| class.handle == classid)
        .map(|class| class.stats.packets)
        .ok_or_else(|| format!("tc class {classid} missing from sidecar {sidecar}"))
}

fn delta(after: u64, before: u64, label: &str) -> Result<u64, String> {
    after
        .checked_sub(before)
        .ok_or_else(|| format!("{label} class counter decreased from {before} to {after}"))
}

#[derive(Deserialize)]
struct TcClass {
    handle: String,
    #[serde(default)]
    stats: TcStats,
}

#[derive(Default, Deserialize)]
struct TcStats {
    #[serde(default)]
    packets: u64,
}
