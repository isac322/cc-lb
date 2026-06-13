use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_plugin_wire::limits::{
    IMPLEMENTED_FUNCTIONS_MAX, SELF_CHECK_FUEL, SELF_CHECK_OUTPUT_MAX_BYTES, SELF_CHECK_WALL_MS,
};
use cc_lb_plugin_wire::self_check::{
    SelfCheckError, SelfCheckRequest, SelfCheckResponse, SelfCheckStatus,
};
use cc_lb_plugin_wire::wire_function::all_wire_functions;
use thiserror::Error;

use crate::handshake::{BuildPluginError, build_plugin};

const SELF_CHECK_EXPORT: &str = "cc_lb_self_check";

pub fn execute_self_check(
    plugin_bytes: &[u8],
) -> Result<SelfCheckResponse, SelfCheckExecutionError> {
    let mut plugin =
        build_plugin(plugin_bytes, SELF_CHECK_WALL_MS, SELF_CHECK_FUEL).map_err(|source| {
            match source {
                BuildPluginError::Instantiate { reason } => {
                    SelfCheckExecutionError::Instantiate { reason }
                }
            }
        })?;

    if !plugin.function_exists(SELF_CHECK_EXPORT) {
        return Err(SelfCheckExecutionError::MissingSelfCheckExport);
    }

    let request = build_request()?;
    let request = serde_json::to_string(&request).map_err(|source| {
        SelfCheckExecutionError::SerializeRequest {
            reason: source.to_string(),
        }
    })?;
    let response = plugin
        .call::<&str, String>(SELF_CHECK_EXPORT, request.as_str())
        .map_err(|source| SelfCheckExecutionError::Call {
            reason: source.to_string(),
        })?;

    if response.len() > SELF_CHECK_OUTPUT_MAX_BYTES {
        return Err(SelfCheckExecutionError::OutputTooLarge {
            bytes: response.len(),
            max: SELF_CHECK_OUTPUT_MAX_BYTES,
        });
    }

    let response: SelfCheckResponse = serde_json::from_str(&response).map_err(|source| {
        SelfCheckExecutionError::DecodeResponse {
            reason: source.to_string(),
        }
    })?;
    response.validate()?;
    validate_status_failures(&response)?;

    Ok(response)
}

fn build_request() -> Result<SelfCheckRequest, SelfCheckExecutionError> {
    let functions = all_wire_functions();
    if functions.len() > IMPLEMENTED_FUNCTIONS_MAX {
        return Err(SelfCheckExecutionError::Validation(
            SelfCheckError::TooManyFunctions,
        ));
    }

    let initiated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| SelfCheckExecutionError::Clock {
            reason: source.to_string(),
        })?
        .as_secs();
    let initiated_at =
        i64::try_from(initiated_at).map_err(|source| SelfCheckExecutionError::Clock {
            reason: source.to_string(),
        })?;
    let request = SelfCheckRequest {
        functions_to_test: functions
            .iter()
            .map(|function| (*function).to_owned())
            .collect(),
        initiated_at,
    };
    request.validate()?;
    Ok(request)
}

fn validate_status_failures(response: &SelfCheckResponse) -> Result<(), SelfCheckExecutionError> {
    match response.status {
        SelfCheckStatus::Success if !response.failures.is_empty() => {
            Err(SelfCheckExecutionError::SuccessWithFailures {
                count: response.failures.len(),
            })
        }
        SelfCheckStatus::Failure if response.failures.is_empty() => {
            Err(SelfCheckExecutionError::FailureWithoutFailures)
        }
        SelfCheckStatus::Failure => Err(SelfCheckExecutionError::FailureStatus {
            failures: response.failures.len(),
        }),
        SelfCheckStatus::Success => Ok(()),
    }
}

#[derive(Debug, Error)]
pub enum SelfCheckExecutionError {
    #[error("self-check validation failed: {0}")]
    Validation(#[from] SelfCheckError),
    #[error("self-check plugin instantiation failed: {reason}")]
    Instantiate { reason: String },
    #[error("plugin does not export cc_lb_self_check")]
    MissingSelfCheckExport,
    #[error("self-check request serialization failed: {reason}")]
    SerializeRequest { reason: String },
    #[error("self-check call failed: {reason}")]
    Call { reason: String },
    #[error("self-check output size {bytes} exceeds maximum {max}")]
    OutputTooLarge { bytes: usize, max: usize },
    #[error("self-check response decode failed: {reason}")]
    DecodeResponse { reason: String },
    #[error("self-check success response included {count} failure(s)")]
    SuccessWithFailures { count: usize },
    #[error("self-check failure response did not include failures")]
    FailureWithoutFailures,
    #[error("self-check reported failure status with {failures} failure(s)")]
    FailureStatus { failures: usize },
    #[error("self-check timestamp generation failed: {reason}")]
    Clock { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_self_check_accepts_success_response() {
        let wasm = self_check_module(
            r#"{"status":"success","failures":[],"completed_at":1}"#,
            false,
        );

        let response = execute_self_check(&wasm).expect("self-check succeeds");

        assert_eq!(response.status, SelfCheckStatus::Success);
        assert!(response.failures.is_empty());
    }

    #[test]
    fn execute_self_check_rejects_failure_status() {
        let wasm = self_check_module(
            r#"{"status":"failure","failures":[{"stage":"wire_function_test","message":"bad wire shape"}],"completed_at":1}"#,
            false,
        );

        let err = execute_self_check(&wasm).expect_err("failure status rejected at executor level");

        match err {
            SelfCheckExecutionError::FailureStatus { failures } => assert_eq!(failures, 1),
            other => panic!("expected failure status rejection, got {other:?}"),
        }
    }

    #[test]
    fn execute_self_check_rejects_missing_export() {
        let wasm = wat::parse_str(r#"(module (func (export "shape") (result i32) (i32.const 0)))"#)
            .expect("wat parses");

        let err = execute_self_check(&wasm).expect_err("missing export rejected");

        match err {
            SelfCheckExecutionError::MissingSelfCheckExport => {}
            other => panic!("expected missing export, got {other:?}"),
        }
    }

    #[test]
    fn execute_self_check_rejects_user_host_imports() {
        let wasm = self_check_module(
            r#"{"status":"success","failures":[],"completed_at":1}"#,
            true,
        );

        let err = execute_self_check(&wasm).expect_err("host import rejected");

        match err {
            SelfCheckExecutionError::Instantiate { .. } | SelfCheckExecutionError::Call { .. } => {}
            other => panic!("expected purity failure, got {other:?}"),
        }
    }

    #[test]
    fn execute_self_check_rejects_oversized_output() {
        let output = "x".repeat(SELF_CHECK_OUTPUT_MAX_BYTES + 1);
        let wasm = self_check_module(&output, false);

        let err = execute_self_check(&wasm).expect_err("oversized response rejected");

        match err {
            SelfCheckExecutionError::OutputTooLarge { bytes, max } => {
                assert_eq!(bytes, SELF_CHECK_OUTPUT_MAX_BYTES + 1);
                assert_eq!(max, SELF_CHECK_OUTPUT_MAX_BYTES);
            }
            other => panic!("expected oversized output, got {other:?}"),
        }
    }

    #[test]
    fn execute_self_check_rejects_success_with_failures() {
        let wasm = self_check_module(
            r#"{"status":"success","failures":[{"stage":"wire_function_test","message":"bad"}],"completed_at":1}"#,
            false,
        );

        let err = execute_self_check(&wasm).expect_err("status/failures mismatch rejected");

        match err {
            SelfCheckExecutionError::SuccessWithFailures { count } => assert_eq!(count, 1),
            other => panic!("expected status/failures mismatch, got {other:?}"),
        }
    }

    #[test]
    fn execute_self_check_rejects_failure_without_failures() {
        let wasm = self_check_module(
            r#"{"status":"failure","failures":[],"completed_at":1}"#,
            false,
        );

        let err = execute_self_check(&wasm).expect_err("status/failures mismatch rejected");

        match err {
            SelfCheckExecutionError::FailureWithoutFailures => {}
            other => panic!("expected status/failures mismatch, got {other:?}"),
        }
    }

    #[test]
    fn execute_self_check_rejects_response_validation_errors() {
        let wasm = self_check_module(
            r#"{"status":"success","failures":[],"completed_at":0}"#,
            false,
        );

        let err = execute_self_check(&wasm).expect_err("invalid response rejected");

        match err {
            SelfCheckExecutionError::Validation(SelfCheckError::InvalidTimestamp(_)) => {}
            other => panic!("expected response validation error, got {other:?}"),
        }
    }

    fn self_check_module(output: &str, import_user_host: bool) -> Vec<u8> {
        let output_helper = bytes_helper("self_check_out", output.as_bytes());
        let user_import = if import_user_host {
            r#"(import "extism:host/user" "cc_lb_log" (func $cc_lb_log (param i64 i64)))"#
        } else {
            ""
        };
        let user_call = if import_user_host {
            "  (call $cc_lb_log (call $self_check_out) (call $self_check_out))"
        } else {
            ""
        };

        let wat = format!(
            r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {user_import}
  {output_helper}
  (func (export "cc_lb_self_check") (result i32)
{user_call}
    (call $output_set (call $self_check_out) (i64.const {len}))
    (i32.const 0))
)
"#,
            len = output.len()
        );
        wat::parse_str(&wat).expect("self-check wat parses")
    }

    fn bytes_helper(name: &str, bytes: &[u8]) -> String {
        let mut stores = String::new();
        for (index, byte) in bytes.iter().enumerate() {
            stores.push_str(&format!(
                "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
            ));
        }
        format!(
            r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
            len = bytes.len()
        )
    }
}
