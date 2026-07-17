use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::netem_render::{NetemRenderError, render_topology};
use crate::topology_spec::TopologySpec;

const TIER: &str = "elevated-netns";
const BRIDGE: &str = "ccstress_elevated_br0";
const SMOKE_SEED: u32 = 1;

#[derive(Clone, Copy)]
struct NamespaceSpec {
    name: &'static str,
    host_veth: &'static str,
    address: &'static str,
}

const NAMESPACES: [NamespaceSpec; 2] = [
    NamespaceSpec {
        name: "ccstress_smoke_client",
        host_veth: "ccstress_elevated_client_host",
        address: "172.30.0.2",
    },
    NamespaceSpec {
        name: "ccstress_smoke_proxy",
        host_veth: "ccstress_elevated_proxy_host",
        address: "172.30.0.3",
    },
];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreviewCommand {
    pub command: String,
    pub requires_sudo: bool,
}

impl PreviewCommand {
    fn sudo(command: impl Into<String>) -> Self {
        Self {
            command: format!("sudo {}", command.into()),
            requires_sudo: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ElevatedPreviewEvidence {
    pub tier: String,
    pub requires_sudo: bool,
    pub commands: Vec<PreviewCommand>,
    pub cleanup: Vec<PreviewCommand>,
    pub executed: bool,
}

#[derive(Debug, Error)]
pub enum ElevatedPreviewError {
    #[error(transparent)]
    Render(#[from] NetemRenderError),
    #[error("create elevated preview directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("serialize elevated preview: {source}")]
    Serialize {
        #[source]
        source: serde_json::Error,
    },
    #[error("write elevated preview {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub fn run_preview(output: &Path) -> ExitCode {
    match render_preview().and_then(|evidence| write_preview(output, &evidence)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "failed to write elevated-netns preview {}: {error}",
                output.display()
            );
            ExitCode::FAILURE
        }
    }
}

pub fn render_preview() -> Result<ElevatedPreviewEvidence, ElevatedPreviewError> {
    let mut commands = bridge_setup_commands();
    for namespace in NAMESPACES {
        commands.extend(namespace_setup_commands(namespace));
    }
    for edge in render_topology(&TopologySpec::smoke(SMOKE_SEED))? {
        commands.extend(edge.commands.into_iter().map(|command| {
            PreviewCommand::sudo(format!("ip netns exec {} {command}", edge.sender_netns))
        }));
    }
    Ok(ElevatedPreviewEvidence {
        tier: TIER.to_owned(),
        requires_sudo: true,
        commands,
        cleanup: cleanup_commands(),
        executed: false,
    })
}

fn bridge_setup_commands() -> Vec<PreviewCommand> {
    vec![
        PreviewCommand::sudo(format!("ip link add name {BRIDGE} type bridge")),
        PreviewCommand::sudo(format!("ip link set dev {BRIDGE} up")),
    ]
}

fn namespace_setup_commands(namespace: NamespaceSpec) -> Vec<PreviewCommand> {
    vec![
        PreviewCommand::sudo(format!("ip netns add {}", namespace.name)),
        PreviewCommand::sudo(format!(
            "ip link add name {} type veth peer name eth0 netns {}",
            namespace.host_veth, namespace.name
        )),
        PreviewCommand::sudo(format!(
            "ip link set dev {} master {BRIDGE}",
            namespace.host_veth
        )),
        PreviewCommand::sudo(format!("ip link set dev {} up", namespace.host_veth)),
        PreviewCommand::sudo(format!(
            "ip netns exec {} ip link set dev lo up",
            namespace.name
        )),
        PreviewCommand::sudo(format!(
            "ip netns exec {} ip addr add {}/24 dev eth0",
            namespace.name, namespace.address
        )),
        PreviewCommand::sudo(format!(
            "ip netns exec {} ip link set dev eth0 up",
            namespace.name
        )),
    ]
}

fn cleanup_commands() -> Vec<PreviewCommand> {
    let mut cleanup = NAMESPACES
        .iter()
        .map(|namespace| {
            PreviewCommand::sudo(format!(
                "ip netns exec {} tc qdisc del dev eth0 root",
                namespace.name
            ))
        })
        .collect::<Vec<_>>();
    cleanup.extend(
        NAMESPACES
            .iter()
            .map(|namespace| PreviewCommand::sudo(format!("ip netns del {}", namespace.name))),
    );
    cleanup.push(PreviewCommand::sudo(format!("ip link del dev {BRIDGE}")));
    cleanup
}

fn write_preview(
    path: &Path,
    evidence: &ElevatedPreviewEvidence,
) -> Result<(), ElevatedPreviewError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            ElevatedPreviewError::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            }
        })?;
    }
    let bytes = serde_json::to_vec_pretty(evidence)
        .map_err(|source| ElevatedPreviewError::Serialize { source })?;
    std::fs::write(path, bytes).map_err(|source| ElevatedPreviewError::Write {
        path: path.to_path_buf(),
        source,
    })
}
