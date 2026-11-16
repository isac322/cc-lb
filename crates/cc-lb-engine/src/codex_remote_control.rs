//! Private-profile Remote Control enrollment wire adaptation.
//!
//! These helpers do not authorize ownership, issue relay credentials, or persist
//! enrollments. The caller must perform those steps before supplying a local token.

use chrono::{DateTime, SecondsFormat, Utc};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;

use crate::authn_rail::Authenticated;

#[path = "codex_remote_control_observation.rs"]
mod observation;
pub use observation::{RemoteControlObservation, RemoteControlWebSocketTermination};

#[derive(Clone, PartialEq, Eq)]
pub struct EnrollmentIdentity {
    pub server_id: String,
    pub environment_id: String,
    pub installation_id: String,
}

/// Authenticated request selectors, not evidence of ownership.
pub struct EnrollmentRequest<'a> {
    pub body: &'a Value,
    pub installation_id: &'a str,
    /// Required for refresh; absent only for a new enrollment.
    pub previous: Option<&'a EnrollmentIdentity>,
}

/// Intentionally has no Debug or Display implementation.
pub struct PreparedEnrollment {
    pub identity: EnrollmentIdentity,
    pub provider_token: SecretString,
    pub provider_expires_at: String,
    pub effective_expires_at: DateTime<Utc>,
    pub response_body: SecretString,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EnrollmentWireError {
    #[error("invalid remote control enrollment payload")]
    InvalidPayload,
    #[error("remote control enrollment identity mismatch")]
    IdentityMismatch,
    #[error("invalid or expired remote control credential lifetime")]
    InvalidExpiry,
    #[error("local remote control credential must differ from provider credential")]
    CredentialNotReplaced,
}

/// Decode only after authentication. `local_expires_at` is the actual issued
/// relay credential's Unix-seconds expiry, not the principal access lifetime.
/// A successful result still requires the caller's durable ownership fences.
pub fn prepare_enrollment_response(
    _auth: &Authenticated,
    request: EnrollmentRequest<'_>,
    provider_body: &[u8],
    local_token: &SecretString,
    local_expires_at: u64,
    now: DateTime<Utc>,
) -> Result<PreparedEnrollment, EnrollmentWireError> {
    let response = sonic_rs::from_slice::<Value>(provider_body)
        .map_err(|_| EnrollmentWireError::InvalidPayload)?;
    let local_expires_at =
        i64::try_from(local_expires_at).map_err(|_| EnrollmentWireError::InvalidExpiry)?;
    prepare_response(request, response, local_token, local_expires_at, now)
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, EnrollmentWireError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(EnrollmentWireError::InvalidPayload)
}

fn prepare_response(
    request: EnrollmentRequest<'_>,
    mut response: Value,
    local_token: &SecretString,
    local_expires_at: i64,
    now: DateTime<Utc>,
) -> Result<PreparedEnrollment, EnrollmentWireError> {
    // Remove the provider secret before any validation or output construction.
    let token = response
        .as_object_mut()
        .and_then(|object| object.remove("remote_control_token"))
        .ok_or(EnrollmentWireError::InvalidPayload)?;
    let provider_token = match token {
        Value::String(token) => SecretString::from(token),
        _ => return Err(EnrollmentWireError::InvalidPayload),
    };
    if provider_token.expose_secret().is_empty() || local_token.expose_secret().is_empty() {
        return Err(EnrollmentWireError::InvalidPayload);
    }
    if provider_token.expose_secret() == local_token.expose_secret() {
        return Err(EnrollmentWireError::CredentialNotReplaced);
    }
    let installation_id = required_string(request.body, "installation_id")?;
    if request.installation_id.is_empty() || installation_id != request.installation_id {
        return Err(EnrollmentWireError::IdentityMismatch);
    }
    let server_id = required_string(&response, "server_id")?;
    let environment_id = required_string(&response, "environment_id")?;
    if let Some(previous) = request.previous {
        if previous.installation_id != installation_id
            || previous.server_id != server_id
            || previous.environment_id != environment_id
            || required_string(request.body, "server_id")? != previous.server_id
        {
            return Err(EnrollmentWireError::IdentityMismatch);
        }
    }
    let identity = EnrollmentIdentity {
        server_id: server_id.to_owned(),
        environment_id: environment_id.to_owned(),
        installation_id: installation_id.to_owned(),
    };
    let provider_expires_at = required_string(&response, "expires_at")?.to_owned();
    let provider_expiry = DateTime::parse_from_rfc3339(&provider_expires_at)
        .map_err(|_| EnrollmentWireError::InvalidExpiry)?
        .with_timezone(&Utc);
    let local_expiry =
        DateTime::from_timestamp(local_expires_at, 0).ok_or(EnrollmentWireError::InvalidExpiry)?;
    let effective_expires_at = provider_expiry.min(local_expiry);
    if effective_expires_at <= now {
        return Err(EnrollmentWireError::InvalidExpiry);
    }
    if local_expiry < provider_expiry {
        response["expires_at"] =
            Value::String(local_expiry.to_rfc3339_opts(SecondsFormat::Secs, true));
    }
    response["remote_control_token"] = Value::String(local_token.expose_secret().to_owned());
    let serialized = serde_json::to_string(&response);
    // The inserted local secret is only needed in the serialized response.
    if let Some(Value::String(token)) = response
        .as_object_mut()
        .and_then(|object| object.remove("remote_control_token"))
    {
        drop(SecretString::from(token));
    }
    let response_body = serialized
        .map(SecretString::from)
        .map_err(|_| EnrollmentWireError::InvalidPayload)?;
    Ok(PreparedEnrollment {
        identity,
        provider_token,
        provider_expires_at,
        effective_expires_at,
        response_body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_893_456_000, 0).unwrap() // 2030-01-01
    }

    fn response() -> Value {
        json!({"server_id":"opaque server / 1", "environment_id":"environment?2",
            "remote_control_token":"synthetic-provider-secret", "expires_at":"2030-01-01T01:00:00Z",
            "unknown":{"array":[null,{"future":true}]}})
    }

    fn prepare(value: Value, expiry: i64) -> Result<PreparedEnrollment, EnrollmentWireError> {
        prepare_response(
            EnrollmentRequest {
                body: &json!({"installation_id":"install opaque"}),
                installation_id: "install opaque",
                previous: None,
            },
            value,
            &SecretString::from("synthetic-local-secret"),
            expiry,
            now(),
        )
    }

    #[test]
    fn preserves_rfc3339_text_and_unknown_fields() {
        for expiry in [
            "2030-01-01T01:00:00Z",
            "2030-01-01T01:00:00+00:00",
            "2030-01-01T02:00:00.123456+01:00",
        ] {
            let mut original = response();
            original["expires_at"] = json!(expiry);
            let prepared = prepare(original.clone(), now().timestamp() + 7_200).unwrap();
            assert_eq!(
                prepared.provider_token.expose_secret(),
                "synthetic-provider-secret"
            );
            assert_eq!(prepared.provider_expires_at, expiry);
            let expected_expiry = DateTime::parse_from_rfc3339(expiry)
                .unwrap()
                .with_timezone(&Utc);
            assert_eq!(prepared.effective_expires_at, expected_expiry);
            let output: Value =
                serde_json::from_str(prepared.response_body.expose_secret()).unwrap();
            original["remote_control_token"] = json!("synthetic-local-secret");
            assert_eq!(output, original);
        }
    }

    #[test]
    fn caps_advertised_expiry_to_actual_local_relay_lifetime() {
        let prepared = prepare(response(), now().timestamp() + 60).unwrap();
        let output: Value = serde_json::from_str(prepared.response_body.expose_secret()).unwrap();
        let mut expected = response();
        expected["remote_control_token"] = json!("synthetic-local-secret");
        expected["expires_at"] = json!("2030-01-01T00:01:00Z");
        assert_eq!(output, expected);
        assert_eq!(prepared.provider_expires_at, "2030-01-01T01:00:00Z");
        assert_eq!(
            prepared.effective_expires_at.timestamp(),
            now().timestamp() + 60
        );
        let equal = prepare(response(), now().timestamp() + 3_600).unwrap();
        assert_eq!(
            equal.effective_expires_at.timestamp(),
            now().timestamp() + 3_600
        );
        let mut offset = response();
        offset["expires_at"] = json!("2030-01-01T02:00:00+01:00");
        let equal = prepare(offset, now().timestamp() + 3_600).unwrap();
        let output: Value = serde_json::from_str(equal.response_body.expose_secret()).unwrap();
        assert_eq!(output["expires_at"], "2030-01-01T02:00:00+01:00");
    }

    #[test]
    fn rejects_missing_wrong_type_and_empty_response_fields() {
        for field in [
            "server_id",
            "environment_id",
            "remote_control_token",
            "expires_at",
        ] {
            for invalid in [None, Some(Value::Null), Some(json!(1)), Some(json!(""))] {
                let mut body = response();
                match invalid {
                    Some(value) => body[field] = value,
                    None => {
                        body.as_object_mut().unwrap().remove(field);
                    }
                }
                assert!(prepare(body, now().timestamp() + 7_200).is_err());
            }
        }
    }

    #[test]
    fn rejects_invalid_expired_and_boundary_expiries() {
        for expiry in [
            "not-a-date",
            "2030-01-01",
            "2030-01-01T00:00:00Z",
            "2029-12-31T23:59:59Z",
        ] {
            let mut body = response();
            body["expires_at"] = json!(expiry);
            assert!(matches!(
                prepare(body, now().timestamp() + 7_200),
                Err(EnrollmentWireError::InvalidExpiry)
            ));
        }
        for local in [now().timestamp(), now().timestamp() - 1, i64::MAX] {
            assert!(matches!(
                prepare(response(), local),
                Err(EnrollmentWireError::InvalidExpiry)
            ));
        }
    }

    #[test]
    fn refresh_checks_request_installation_server_and_response_identity() {
        let identity = EnrollmentIdentity {
            server_id: "opaque server / 1".into(),
            environment_id: "environment?2".into(),
            installation_id: "install opaque".into(),
        };
        let request =
            json!({"server_id":identity.server_id,"installation_id":identity.installation_id});
        let local = SecretString::from("synthetic-local-secret");
        let run = |request: &Value, body: Value, header: &str| {
            prepare_response(
                EnrollmentRequest {
                    body: request,
                    installation_id: header,
                    previous: Some(&identity),
                },
                body,
                &local,
                now().timestamp() + 7_200,
                now(),
            )
        };
        assert!(run(&request, response(), "install opaque").is_ok());
        for field in ["server_id", "environment_id"] {
            let mut body = response();
            body[field] = json!("different");
            assert!(matches!(
                run(&request, body, "install opaque"),
                Err(EnrollmentWireError::IdentityMismatch)
            ));
        }
        for field in ["server_id", "installation_id"] {
            for value in [json!("different"), json!(""), Value::Null, json!(1)] {
                let mut changed = request.clone();
                changed[field] = value;
                assert!(run(&changed, response(), "install opaque").is_err());
            }
            let mut changed = request.clone();
            changed.as_object_mut().unwrap().remove(field);
            assert!(run(&changed, response(), "install opaque").is_err());
        }
        assert!(run(&request, response(), "different").is_err());
        assert!(run(&request, response(), "").is_err());
    }

    #[test]
    fn errors_do_not_echo_secrets_and_vendor_token_cannot_be_reused() {
        let mut body = response();
        body["expires_at"] = json!("synthetic-provider-secret");
        let error = prepare(body, now().timestamp() + 7_200).err().unwrap();
        assert!(!format!("{error:?} {error}").contains("synthetic-provider-secret"));
        let mut body = response();
        body["remote_control_token"] = json!({"secret":"synthetic-provider-secret"});
        let error = prepare(body, now().timestamp() + 7_200).err().unwrap();
        assert!(!format!("{error:?} {error}").contains("synthetic-provider-secret"));
        let request = json!({"installation_id":"install opaque"});
        for token in ["", "synthetic-provider-secret"] {
            assert!(
                prepare_response(
                    EnrollmentRequest {
                        body: &request,
                        installation_id: "install opaque",
                        previous: None
                    },
                    response(),
                    &SecretString::from(token.to_owned()),
                    now().timestamp() + 7_200,
                    now()
                )
                .is_err()
            );
        }
    }

    #[test]
    fn rejects_non_object_payload_without_inventing_token_alphabet_policy() {
        for value in [Value::Null, json!([]), json!("synthetic-provider-secret")] {
            assert!(prepare(value, now().timestamp() + 7_200).is_err());
        }
        let mut opaque = response();
        opaque["remote_control_token"] = json!("opaque token without a specified alphabet");
        assert!(prepare(opaque, now().timestamp() + 7_200).is_ok());
    }
}
