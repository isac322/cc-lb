use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::traffic::{ExpectedLabel, ExpectedStatusClass};

#[derive(Debug, Deserialize)]
pub struct ClassifyFixture {
    pub cases: Vec<ClassifyCase>,
}

#[derive(Debug, Deserialize)]
pub struct ClassifyCase {
    pub case_id: String,
    pub expected_label: ExpectedLabel,
    pub report_label: Option<EvidenceLabel>,
    pub observed: ObservedOutcome,
    pub expected_classification: Classification,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceLabel {
    Healthy,
    ProviderError,
    ClientCancel,
    Malformed,
    Unsupported,
    #[serde(rename = "unexpected_5xx")]
    Unexpected5xx,
}

impl From<ExpectedLabel> for EvidenceLabel {
    fn from(value: ExpectedLabel) -> Self {
        match value {
            ExpectedLabel::Healthy => Self::Healthy,
            ExpectedLabel::ProviderError => Self::ProviderError,
            ExpectedLabel::ClientCancel => Self::ClientCancel,
            ExpectedLabel::Malformed => Self::Malformed,
            ExpectedLabel::Unsupported => Self::Unsupported,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObservedOutcome {
    Http { status: u16 },
    ClientCancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Classification {
    Expected,
    Unexpected,
}

#[derive(Debug, Serialize)]
pub struct ClassificationReport {
    pub all_correct: bool,
    pub items: Vec<ClassificationItem>,
}

#[derive(Debug, Serialize)]
pub struct ClassificationItem {
    pub case_id: String,
    pub expected_label: EvidenceLabel,
    pub expected_status_class: ExpectedStatusClass,
    pub observed: ObservedOutcome,
    pub classified_as: Classification,
    pub correct: bool,
}

pub fn classify_fixture(fixture: &ClassifyFixture) -> ClassificationReport {
    let items = fixture
        .cases
        .iter()
        .map(|case| {
            let expected_status_class = case.expected_label.status_class();
            let classification = classify(expected_status_class, &case.observed);
            ClassificationItem {
                case_id: case.case_id.clone(),
                expected_label: case
                    .report_label
                    .unwrap_or_else(|| case.expected_label.into()),
                expected_status_class,
                observed: case.observed.clone(),
                classified_as: classification,
                correct: classification == case.expected_classification,
            }
        })
        .collect::<Vec<_>>();
    let all_correct = items.iter().all(|item| item.correct);
    ClassificationReport { all_correct, items }
}

pub fn run(input: &Path, output: &Path) -> Result<ClassificationReport, ClassifierError> {
    let bytes = fs::read(input).map_err(ClassifierError::Read)?;
    let fixture = serde_json::from_slice(&bytes).map_err(ClassifierError::Parse)?;
    let report = classify_fixture(&fixture);
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(ClassifierError::Write)?;
    }
    let bytes = serde_json::to_vec_pretty(&report).map_err(ClassifierError::Serialize)?;
    fs::write(output, bytes).map_err(ClassifierError::Write)?;
    Ok(report)
}

fn classify(expected: ExpectedStatusClass, observed: &ObservedOutcome) -> Classification {
    let matches = match (expected, observed) {
        (ExpectedStatusClass::Success2xx, ObservedOutcome::Http { status }) => {
            (200..300).contains(status)
        }
        (ExpectedStatusClass::Provider529, ObservedOutcome::Http { status }) => *status == 529,
        (ExpectedStatusClass::ClientCancelled, ObservedOutcome::ClientCancelled) => true,
        (ExpectedStatusClass::Malformed400, ObservedOutcome::Http { status }) => *status == 400,
        (ExpectedStatusClass::Unsupported4xx, ObservedOutcome::Http { status }) => {
            (400..500).contains(status)
        }
        (ExpectedStatusClass::Success2xx, ObservedOutcome::ClientCancelled)
        | (ExpectedStatusClass::Provider529, ObservedOutcome::ClientCancelled)
        | (ExpectedStatusClass::ClientCancelled, ObservedOutcome::Http { .. })
        | (ExpectedStatusClass::Malformed400, ObservedOutcome::ClientCancelled)
        | (ExpectedStatusClass::Unsupported4xx, ObservedOutcome::ClientCancelled) => false,
    };
    if matches {
        Classification::Expected
    } else {
        Classification::Unexpected
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClassifierError {
    #[error("read fixture: {0}")]
    Read(std::io::Error),
    #[error("parse fixture: {0}")]
    Parse(serde_json::Error),
    #[error("serialize report: {0}")]
    Serialize(serde_json::Error),
    #[error("write report: {0}")]
    Write(std::io::Error),
}
