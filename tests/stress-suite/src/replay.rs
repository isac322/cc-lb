use std::fs;
use std::path::{Path, PathBuf};

use crate::manifest::{Manifest, ManifestError};

pub fn read_and_verify(path: &Path) -> Result<Manifest, ReplayError> {
    let bytes = fs::read(path).map_err(|source| ReplayError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let manifest = serde_json::from_slice(&bytes).map_err(ReplayError::Parse)?;
    verify(&manifest)?;
    Ok(manifest)
}

pub fn verify(manifest: &Manifest) -> Result<(), ReplayError> {
    let expected_hash = manifest
        .integrity_hash
        .as_deref()
        .ok_or(ReplayError::MissingIntegrity)?;
    let actual_hash = manifest.computed_integrity_hash()?;
    if expected_hash != actual_hash {
        return Err(ReplayError::Integrity {
            expected: expected_hash.to_owned(),
            actual: actual_hash,
        });
    }
    let actual_schedule = manifest.computed_schedule_hash()?;
    if manifest.schedule_hash != actual_schedule {
        return Err(ReplayError::Integrity {
            expected: manifest.schedule_hash.clone(),
            actual: actual_schedule,
        });
    }
    if let Some(decision) = &manifest.topology_decision {
        decision.validate().map_err(ReplayError::TopologyDecision)?;
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("manifest_read: {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("manifest_parse: {0}")]
    Parse(serde_json::Error),
    #[error("manifest_integrity: manifest is missing integrity_hash")]
    MissingIntegrity,
    #[error("manifest_integrity: expected {expected}, computed {actual}")]
    Integrity { expected: String, actual: String },
    #[error("manifest_topology_decision: {0}")]
    TopologyDecision(crate::topology_decision::TopologyDecisionError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
}
