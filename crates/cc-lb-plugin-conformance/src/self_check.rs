use cc_lb_plugin_wire::self_check::{
    SelfCheckStage as WireSelfCheckStage, SelfCheckStatus as WireSelfCheckStatus,
};
use cc_lb_runtime_protocol::self_check::{SelfCheckExecutionError, execute_self_check};
use thiserror::Error;

pub fn run(wasm: &[u8]) -> Result<SelfCheckReport, SelfCheckError> {
    let response = execute_self_check(wasm)?;

    Ok(SelfCheckReport {
        status: response.status.into(),
        failures: response
            .failures
            .into_iter()
            .map(|failure| SelfCheckFailure {
                function: stage_name(failure.stage).to_owned(),
                version: 0,
                reason: failure.message,
            })
            .collect(),
    })
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfCheckReport {
    pub status: SelfCheckStatus,
    pub failures: Vec<SelfCheckFailure>,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfCheckStatus {
    Success,
    Failure,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfCheckFailure {
    pub function: String,
    pub version: u32,
    pub reason: String,
}

#[non_exhaustive]
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum SelfCheckError {
    #[error("plugin instantiation failed: {reason}")]
    Instantiate { reason: String },
    #[error("plugin does not export cc_lb_self_check")]
    MissingExport,
    #[error("self-check reported failure status")]
    FailureStatus,
    #[error("self-check output invalid: {reason}")]
    Output { reason: String },
    #[error("wasm execution failed: {reason}")]
    WasmTrap { reason: String },
}

impl From<SelfCheckExecutionError> for SelfCheckError {
    fn from(error: SelfCheckExecutionError) -> Self {
        let reason = error.to_string();
        match error {
            SelfCheckExecutionError::Instantiate { reason } => {
                SelfCheckError::Instantiate { reason }
            }
            SelfCheckExecutionError::MissingSelfCheckExport => SelfCheckError::MissingExport,
            SelfCheckExecutionError::Call { reason } => SelfCheckError::WasmTrap { reason },
            SelfCheckExecutionError::FailureStatus { .. } => SelfCheckError::FailureStatus,
            SelfCheckExecutionError::Validation(_)
            | SelfCheckExecutionError::SerializeRequest { .. }
            | SelfCheckExecutionError::OutputTooLarge { .. }
            | SelfCheckExecutionError::DecodeResponse { .. }
            | SelfCheckExecutionError::SuccessWithFailures { .. }
            | SelfCheckExecutionError::FailureWithoutFailures
            | SelfCheckExecutionError::Clock { .. } => SelfCheckError::Output { reason },
        }
    }
}

impl From<WireSelfCheckStatus> for SelfCheckStatus {
    fn from(status: WireSelfCheckStatus) -> Self {
        match status {
            WireSelfCheckStatus::Success => SelfCheckStatus::Success,
            WireSelfCheckStatus::Failure => SelfCheckStatus::Failure,
        }
    }
}

fn stage_name(stage: WireSelfCheckStage) -> &'static str {
    match stage {
        WireSelfCheckStage::RequestPreparation => "request_preparation",
        WireSelfCheckStage::WireFunctionTest => "wire_function_test",
        WireSelfCheckStage::CapabilityVerification => "capability_verification",
        WireSelfCheckStage::ResponseValidation => "response_validation",
    }
}
