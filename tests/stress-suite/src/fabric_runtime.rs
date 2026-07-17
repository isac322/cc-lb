use std::path::PathBuf;

use crate::docker::Docker;
use crate::fabric_state::{
    ALPINE_IMAGE, CCLB_IP, POSTGRES_IMAGE, POSTGRES_IP, PROBE_IP, ResourceNames, SUBNET,
    docker_command, label,
};

pub fn build_helper_image(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
) -> Result<(), String> {
    let dockerfile = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("netem/Dockerfile");
    docker_command(
        docker,
        vec![
            "build".to_owned(),
            "-q".to_owned(),
            "--label".to_owned(),
            label(run_id),
            "--tag".to_owned(),
            names.helper_image.clone(),
            "--file".to_owned(),
            dockerfile.display().to_string(),
            env!("CARGO_MANIFEST_DIR").to_owned(),
        ],
        "netem helper image build",
    )?;
    Ok(())
}

pub fn check_sidecar_capabilities(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
) -> Result<(), String> {
    docker_command(
        docker,
        vec![
            "run".to_owned(),
            "--rm".to_owned(),
            "--label".to_owned(),
            label(run_id),
            "--cap-add=NET_ADMIN".to_owned(),
            names.helper_image.clone(),
            "sh".to_owned(),
            "-ec".to_owned(),
            "tc qdisc replace dev lo root netem delay 1ms seed 1 && tc qdisc del dev lo root"
                .to_owned(),
        ],
        "NET_ADMIN, tc, and tc netem seed capability",
    )?;
    Ok(())
}

pub fn create_network(docker: &Docker, names: &ResourceNames, run_id: &str) -> Result<(), String> {
    docker_command(
        docker,
        vec![
            "network".to_owned(),
            "create".to_owned(),
            "--driver".to_owned(),
            "bridge".to_owned(),
            "--subnet".to_owned(),
            SUBNET.to_owned(),
            "--label".to_owned(),
            label(run_id),
            names.network.clone(),
        ],
        "private static-IP bridge creation",
    )?;
    Ok(())
}

pub fn start_nodes(docker: &Docker, names: &ResourceNames, run_id: &str) -> Result<(), String> {
    start_node(
        docker,
        names,
        run_id,
        &names.probe,
        PROBE_IP,
        ALPINE_IMAGE,
        Vec::new(),
    )?;
    start_node(
        docker,
        names,
        run_id,
        &names.cc_lb,
        CCLB_IP,
        ALPINE_IMAGE,
        Vec::new(),
    )?;
    start_node(
        docker,
        names,
        run_id,
        &names.postgres,
        POSTGRES_IP,
        POSTGRES_IMAGE,
        vec![
            "-e".to_owned(),
            "POSTGRES_PASSWORD=proof".to_owned(),
            "-e".to_owned(),
            "POSTGRES_DB=proof".to_owned(),
        ],
    )
}

fn start_node(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
    name: &str,
    ipv4: &str,
    image: &str,
    mut environment: Vec<String>,
) -> Result<(), String> {
    let mut args = vec![
        "run".to_owned(),
        "-d".to_owned(),
        "--name".to_owned(),
        name.to_owned(),
        "--label".to_owned(),
        label(run_id),
        "--network".to_owned(),
        names.network.clone(),
        "--ip".to_owned(),
        ipv4.to_owned(),
    ];
    args.append(&mut environment);
    args.push(image.to_owned());
    if image == ALPINE_IMAGE {
        args.push("sleep".to_owned());
        args.push("infinity".to_owned());
    }
    docker_command(docker, args, "private data-path container start")?;
    Ok(())
}

pub fn warm_sender_path(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
    sender: &str,
    destination_ipv4: &str,
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
            "ping".to_owned(),
            "-c".to_owned(),
            "1".to_owned(),
            "-W".to_owned(),
            "1".to_owned(),
            destination_ipv4.to_owned(),
        ],
        "path warmup",
    )?;
    Ok(())
}

pub fn start_sidecars(docker: &Docker, names: &ResourceNames, run_id: &str) -> Result<(), String> {
    start_sidecar(docker, names, run_id, &names.probe_sidecar, &names.probe)?;
    start_sidecar(docker, names, run_id, &names.cc_lb_sidecar, &names.cc_lb)?;
    start_sidecar(
        docker,
        names,
        run_id,
        &names.postgres_counter,
        &names.postgres,
    )
}

pub fn start_udp_sinks(docker: &Docker, names: &ResourceNames) -> Result<(), String> {
    for sidecar in [&names.cc_lb_sidecar, &names.postgres_counter] {
        docker_command(
            docker,
            vec![
                "exec".to_owned(),
                "-d".to_owned(),
                sidecar.clone(),
                "nc".to_owned(),
                "-l".to_owned(),
                "-u".to_owned(),
                "-p".to_owned(),
                "18080".to_owned(),
            ],
            "sender-namespace UDP sink start",
        )?;
    }
    Ok(())
}

fn start_sidecar(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
    sidecar: &str,
    sender: &str,
) -> Result<(), String> {
    docker_command(
        docker,
        vec![
            "run".to_owned(),
            "-d".to_owned(),
            "--name".to_owned(),
            sidecar.to_owned(),
            "--label".to_owned(),
            label(run_id),
            "--network".to_owned(),
            format!("container:{sender}"),
            "--cap-add=NET_ADMIN".to_owned(),
            names.helper_image.clone(),
            "sleep".to_owned(),
            "infinity".to_owned(),
        ],
        "netem sidecar namespace join",
    )?;
    Ok(())
}

pub fn install_commands(docker: &Docker, sidecar: &str, commands: &[String]) -> Result<(), String> {
    docker_command(
        docker,
        vec![
            "exec".to_owned(),
            sidecar.to_owned(),
            "sh".to_owned(),
            "-ec".to_owned(),
            commands.join(" && "),
        ],
        "directional tc setup",
    )?;
    Ok(())
}
