extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stages during self-check execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelfCheckStage {
    /// Preparing self-check request.
    RequestPreparation,
    /// Testing wire function.
    WireFunctionTest,
    /// Verifying capabilities.
    CapabilityVerification,
    /// Validating response.
    ResponseValidation,
}

/// Status of a self-check execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelfCheckStatus {
    /// Self-check passed successfully.
    Success,
    /// Self-check failed (see failures for details).
    Failure,
}

/// Describes a single failure during self-check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelfCheckFailure {
    /// Stage at which the failure occurred.
    pub stage: SelfCheckStage,
    /// Human-readable error message.
    pub message: String,
}

/// Request to perform a self-check on a plugin.
///
/// Self-check is a dry-run validation that tests wire functions without invoking handlers.
/// Uses the skip-handler model: serialize/deserialize round-trip only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelfCheckRequest {
    /// Wire function names to test.
    pub functions_to_test: Vec<String>,
    /// Unix timestamp (seconds) when the self-check was initiated.
    pub initiated_at: i64,
}

/// Response from a self-check execution.
///
/// Skip-handler model: contains only validation results, not handler execution results.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelfCheckResponse {
    /// Overall status of the self-check.
    pub status: SelfCheckStatus,
    /// Details of any failures encountered.
    pub failures: Vec<SelfCheckFailure>,
    /// Unix timestamp (seconds) when the self-check completed.
    pub completed_at: i64,
}

/// Error type for self-check operations.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum SelfCheckError {
    #[error("self-check request is empty (no functions to test)")]
    EmptyRequest,

    #[error(
        "self-check function list exceeds maximum of {}",
        crate::limits::IMPLEMENTED_FUNCTIONS_MAX
    )]
    TooManyFunctions,

    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("self-check response serialization failed: {0}")]
    SerializationFailed(String),
}

impl SelfCheckRequest {
    /// Validate this request.
    pub fn validate(&self) -> Result<(), SelfCheckError> {
        if self.functions_to_test.is_empty() {
            return Err(SelfCheckError::EmptyRequest);
        }

        if self.functions_to_test.len() > crate::limits::IMPLEMENTED_FUNCTIONS_MAX {
            return Err(SelfCheckError::TooManyFunctions);
        }

        if self.initiated_at <= 0 {
            return Err(SelfCheckError::InvalidTimestamp(
                "initiated_at must be positive".to_string(),
            ));
        }

        Ok(())
    }
}

impl SelfCheckResponse {
    /// Validate this response.
    pub fn validate(&self) -> Result<(), SelfCheckError> {
        if self.completed_at <= 0 {
            return Err(SelfCheckError::InvalidTimestamp(
                "completed_at must be positive".to_string(),
            ));
        }

        if self.failures.len() > crate::limits::IMPLEMENTED_FUNCTIONS_MAX {
            return Err(SelfCheckError::TooManyFunctions);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;
    use alloc::vec;

    #[test]
    fn test_self_check_stage_serde() {
        let stages = [
            SelfCheckStage::RequestPreparation,
            SelfCheckStage::WireFunctionTest,
            SelfCheckStage::CapabilityVerification,
            SelfCheckStage::ResponseValidation,
        ];

        for stage in &stages {
            let json = serde_json::to_string(stage).unwrap();
            let deserialized: SelfCheckStage = serde_json::from_str(&json).unwrap();
            assert_eq!(*stage, deserialized);
        }
    }

    #[test]
    fn test_self_check_status_serde() {
        let statuses = [SelfCheckStatus::Success, SelfCheckStatus::Failure];

        for status in &statuses {
            let json = serde_json::to_string(status).unwrap();
            let deserialized: SelfCheckStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(*status, deserialized);
        }
    }

    #[test]
    fn test_self_check_failure_serde() {
        let failure = SelfCheckFailure {
            stage: SelfCheckStage::WireFunctionTest,
            message: "test failed".to_string(),
        };

        let json = serde_json::to_string(&failure).unwrap();
        let deserialized: SelfCheckFailure = serde_json::from_str(&json).unwrap();
        assert_eq!(failure, deserialized);
    }

    #[test]
    fn test_self_check_failure_deny_unknown_fields() {
        let json = r#"{"stage":"wire_function_test","message":"test","unknown":"field"}"#;
        let result: Result<SelfCheckFailure, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_self_check_request_valid() {
        let request = SelfCheckRequest {
            functions_to_test: vec!["route".to_string(), "observe".to_string()],
            initiated_at: 1000,
        };
        assert!(request.validate().is_ok());
    }

    #[test]
    fn test_self_check_request_empty() {
        let request = SelfCheckRequest {
            functions_to_test: vec![],
            initiated_at: 1000,
        };
        assert_eq!(request.validate(), Err(SelfCheckError::EmptyRequest));
    }

    #[test]
    fn test_self_check_request_too_many_functions() {
        let mut functions = Vec::new();
        for i in 0..=crate::limits::IMPLEMENTED_FUNCTIONS_MAX {
            functions.push(format!("func_{}", i));
        }

        let request = SelfCheckRequest {
            functions_to_test: functions,
            initiated_at: 1000,
        };
        assert_eq!(request.validate(), Err(SelfCheckError::TooManyFunctions));
    }

    #[test]
    fn test_self_check_request_invalid_timestamp() {
        let request = SelfCheckRequest {
            functions_to_test: vec!["route".to_string()],
            initiated_at: 0,
        };
        assert!(request.validate().is_err());

        let request2 = SelfCheckRequest {
            functions_to_test: vec!["route".to_string()],
            initiated_at: -100,
        };
        assert!(request2.validate().is_err());
    }

    #[test]
    fn test_self_check_request_serde() {
        let request = SelfCheckRequest {
            functions_to_test: vec!["route".to_string(), "observe".to_string()],
            initiated_at: 1234567890,
        };

        let json = serde_json::to_string(&request).unwrap();
        let deserialized: SelfCheckRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_self_check_request_deny_unknown_fields() {
        let json = r#"{"functions_to_test":["route"],"initiated_at":1000,"unknown":"field"}"#;
        let result: Result<SelfCheckRequest, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_self_check_response_success() {
        let response = SelfCheckResponse {
            status: SelfCheckStatus::Success,
            failures: vec![],
            completed_at: 2000,
        };
        assert!(response.validate().is_ok());
    }

    #[test]
    fn test_self_check_response_with_failures() {
        let response = SelfCheckResponse {
            status: SelfCheckStatus::Failure,
            failures: vec![SelfCheckFailure {
                stage: SelfCheckStage::WireFunctionTest,
                message: "function not found".to_string(),
            }],
            completed_at: 2000,
        };
        assert!(response.validate().is_ok());
    }

    #[test]
    fn test_self_check_response_invalid_timestamp() {
        let response = SelfCheckResponse {
            status: SelfCheckStatus::Success,
            failures: vec![],
            completed_at: 0,
        };
        assert!(response.validate().is_err());
    }

    #[test]
    fn test_self_check_response_serde() {
        let response = SelfCheckResponse {
            status: SelfCheckStatus::Success,
            failures: vec![],
            completed_at: 1234567890,
        };

        let json = serde_json::to_string(&response).unwrap();
        let deserialized: SelfCheckResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(response, deserialized);
    }

    #[test]
    fn test_self_check_response_deny_unknown_fields() {
        let json = r#"{"status":"success","failures":[],"completed_at":2000,"unknown":"field"}"#;
        let result: Result<SelfCheckResponse, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_status_snake_case_names() {
        assert_eq!(
            serde_json::to_string(&SelfCheckStatus::Success).unwrap(),
            "\"success\""
        );
        assert_eq!(
            serde_json::to_string(&SelfCheckStatus::Failure).unwrap(),
            "\"failure\""
        );
    }

    #[test]
    fn test_stage_snake_case_names() {
        assert_eq!(
            serde_json::to_string(&SelfCheckStage::RequestPreparation).unwrap(),
            "\"request_preparation\""
        );
        assert_eq!(
            serde_json::to_string(&SelfCheckStage::WireFunctionTest).unwrap(),
            "\"wire_function_test\""
        );
    }
}
