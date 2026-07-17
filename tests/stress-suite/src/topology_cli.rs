use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::netem_render::NetemRenderError;
use crate::netem_render::render_topology;
use crate::topology_spec::{TopologySpec, TopologyValidationError};

#[derive(Debug, thiserror::Error)]
enum TopologyCliError {
    #[error("unsupported topology profile {profile}")]
    UnsupportedProfile { profile: String },
    #[error("create topology directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("serialize topology: {source}")]
    Serialize {
        #[source]
        source: serde_json::Error,
    },
    #[error("write topology {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("read topology {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse topology {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    InvalidTopology(#[from] TopologyValidationError),
    #[error(transparent)]
    NetemRender(#[from] NetemRenderError),
}

pub fn emit_topology(seed: u32, profile: &str, path: &Path) -> ExitCode {
    match topology_for_profile(seed, profile).and_then(|topology| {
        render_topology(&topology)?;
        write_topology(path, &topology)
    }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

pub fn check_topology(path: &Path) -> ExitCode {
    match read_topology(path).and_then(|topology| {
        topology.validate()?;
        Ok(())
    }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn topology_for_profile(seed: u32, profile: &str) -> Result<TopologySpec, TopologyCliError> {
    match profile {
        "smoke" => Ok(TopologySpec::smoke(seed)),
        _ => Err(TopologyCliError::UnsupportedProfile {
            profile: profile.to_owned(),
        }),
    }
}

fn write_topology(path: &Path, topology: &TopologySpec) -> Result<(), TopologyCliError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| TopologyCliError::CreateDirectory {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let bytes = serde_json::to_vec_pretty(topology)
        .map_err(|source| TopologyCliError::Serialize { source })?;
    std::fs::write(path, bytes).map_err(|source| TopologyCliError::Write {
        path: path.to_path_buf(),
        source,
    })
}

fn read_topology(path: &Path) -> Result<TopologySpec, TopologyCliError> {
    let bytes = std::fs::read(path).map_err(|source| TopologyCliError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| TopologyCliError::Parse {
        path: path.to_path_buf(),
        source,
    })
}
