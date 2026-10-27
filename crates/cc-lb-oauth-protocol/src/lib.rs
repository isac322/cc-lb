#![forbid(unsafe_code)]

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TokenEndpointParseError {
    #[error("oauth token response json failed: {source}")]
    Json { source: serde_json::Error },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TokenEndpointResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_in: u64,
    pub refresh_token_expires_in: Option<u64>,
    pub scope: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingTokenParts {
    pub refresh_token: String,
    pub refresh_token_expires_at_unix_secs: Option<u64>,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshedTokenParts {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at_unix_secs: u64,
    pub refresh_token_expires_at_unix_secs: Option<u64>,
    pub scopes: Vec<String>,
}

pub fn refresh_token_form_body(client_id: &str, refresh_token: &str) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("grant_type", "refresh_token");
    serializer.append_pair("client_id", client_id);
    serializer.append_pair("refresh_token", refresh_token);
    serializer.finish()
}

pub fn refresh_token_json_body(client_id: &str, refresh_token: &str) -> String {
    serde_json::json!({
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    })
    .to_string()
}

pub fn parse_token_endpoint_response(
    body: &[u8],
) -> Result<TokenEndpointResponse, TokenEndpointParseError> {
    let value = serde_json::from_slice::<serde_json::Value>(body)
        .map_err(|source| TokenEndpointParseError::Json { source })?;
    let mut response = serde_json::from_value::<TokenEndpointResponse>(value.clone())
        .map_err(|source| TokenEndpointParseError::Json { source })?;
    if value.get("account_id").is_none() {
        response.account_id = account_id_from_access_token(&response.access_token);
    }
    Ok(response)
}

pub fn account_id_from_access_token(access_token: &str) -> Option<String> {
    let payload = access_token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims = serde_json::from_slice::<serde_json::Value>(&decoded).ok()?;
    claims
        .get("https://api.openai.com/auth")
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(serde_json::Value::as_str)
        .filter(|account_id| !account_id.is_empty())
        .map(str::to_owned)
}

#[derive(Deserialize)]
struct RefreshErrorResponse<'a> {
    #[serde(borrow)]
    error: Option<&'a str>,
}

pub fn is_terminal_refresh_failure(status: u16, body: &[u8]) -> bool {
    match status {
        401 => true,
        400 => {
            serde_json::from_slice::<RefreshErrorResponse<'_>>(body)
                .ok()
                .and_then(|response| response.error)
                == Some("invalid_grant")
        }
        _ => false,
    }
}

pub fn refresh_requires_reconnect(reason: Option<&str>) -> bool {
    matches!(
        reason,
        Some("status_400" | "status_401" | "refresh_token_expired")
    )
}

pub fn refreshed_token_parts(
    existing: ExistingTokenParts,
    response: TokenEndpointResponse,
    now_epoch_secs: u64,
) -> RefreshedTokenParts {
    let scopes = response
        .scope
        .map(|scope| scope.split_whitespace().map(str::to_owned).collect())
        .unwrap_or(existing.scopes);
    let refresh_token_expires_at_unix_secs = response
        .refresh_token_expires_in
        .map(|expires_in| now_epoch_secs.saturating_add(expires_in))
        .or(existing.refresh_token_expires_at_unix_secs);

    RefreshedTokenParts {
        access_token: response.access_token,
        refresh_token: response.refresh_token.unwrap_or(existing.refresh_token),
        expires_at_unix_secs: now_epoch_secs.saturating_add(response.expires_in),
        refresh_token_expires_at_unix_secs,
        scopes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_form_body_encodes_refresh_token_grant() {
        // Given: an OAuth refresh token request with characters requiring encoding.
        let client_id = "client id";
        let refresh_token = "rt+/=";

        // When: the protocol helper builds a form-urlencoded request body.
        let body = refresh_token_form_body(client_id, refresh_token);

        // Then: the body carries the refresh grant contract exactly once.
        let parsed = url::form_urlencoded::parse(body.as_bytes()).collect::<Vec<_>>();
        assert_eq!(
            parsed,
            vec![
                ("grant_type".into(), "refresh_token".into()),
                ("client_id".into(), "client id".into()),
                ("refresh_token".into(), "rt+/=".into()),
            ]
        );
    }

    #[test]
    fn refresh_json_body_encodes_openai_refresh_grant() {
        let body = refresh_token_json_body("codex-client", "refresh-token");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid json");
        assert_eq!(
            parsed,
            serde_json::json!({
                "grant_type": "refresh_token",
                "client_id": "codex-client",
                "refresh_token": "refresh-token",
            })
        );
    }

    #[test]
    fn terminal_refresh_failure_classifies_oauth_error_responses() {
        assert!(is_terminal_refresh_failure(401, b""));
        assert!(is_terminal_refresh_failure(
            400,
            br#"{"error":"invalid_grant"}"#
        ));
        assert!(is_terminal_refresh_failure(
            400,
            br#"{"error":"invalid_grant","error_description":"expired"}"#
        ));

        assert!(!is_terminal_refresh_failure(
            400,
            br#"{"error":"invalid_client"}"#
        ));
        assert!(!is_terminal_refresh_failure(
            400,
            br#"{"error":"invalid_grant"#
        ));
        assert!(!is_terminal_refresh_failure(
            400,
            b"<html>bad request</html>"
        ));
        assert!(!is_terminal_refresh_failure(
            429,
            br#"{"error":"invalid_grant"}"#
        ));
        assert!(!is_terminal_refresh_failure(
            500,
            br#"{"error":"invalid_grant"}"#
        ));
    }

    #[test]
    fn refresh_reconnect_reasons_are_terminal_only() {
        for reason in ["status_400", "status_401", "refresh_token_expired"] {
            assert!(refresh_requires_reconnect(Some(reason)));
        }
        for reason in [None, Some("network"), Some("status_429"), Some("parse")] {
            assert!(!refresh_requires_reconnect(reason));
        }
    }

    #[test]
    fn refreshed_token_parts_preserve_missing_refresh_token_scope_and_expiry() {
        // Given: an endpoint response that omits optional refresh-token fields and scope.
        let response =
            parse_token_endpoint_response(br#"{"access_token":"new-access","expires_in":30}"#)
                .unwrap();
        let existing = ExistingTokenParts {
            refresh_token: "old-refresh".to_owned(),
            refresh_token_expires_at_unix_secs: Some(1_000),
            scopes: vec!["old:scope".to_owned()],
        };

        // When: the response is materialized into refreshed token parts.
        let refreshed = refreshed_token_parts(existing, response, 100);

        // Then: fallback fields remain unchanged and access expiry is relative to now.
        assert_eq!(refreshed.access_token, "new-access");
        assert_eq!(refreshed.refresh_token, "old-refresh");
        assert_eq!(refreshed.expires_at_unix_secs, 130);
        assert_eq!(refreshed.refresh_token_expires_at_unix_secs, Some(1_000));
        assert_eq!(refreshed.scopes, vec!["old:scope"]);
    }

    #[test]
    fn refreshed_token_parts_apply_returned_scope_and_expiries() {
        // Given: an endpoint response that rotates the token and both expiry clocks.
        let response = parse_token_endpoint_response(
            br#"{"access_token":"new-access","refresh_token":"new-refresh","expires_in":5,"refresh_token_expires_in":7,"scope":"a b c"}"#,
        )
        .unwrap();
        let existing = ExistingTokenParts {
            refresh_token: "old-refresh".to_owned(),
            refresh_token_expires_at_unix_secs: Some(500),
            scopes: vec!["old".to_owned()],
        };

        // When: the response is materialized into refreshed token parts.
        let refreshed = refreshed_token_parts(existing, response, u64::MAX - 1);

        // Then: returned token/scope win and both relative expiries saturate.
        assert_eq!(refreshed.refresh_token, "new-refresh");
        assert_eq!(refreshed.expires_at_unix_secs, u64::MAX);
        assert_eq!(refreshed.refresh_token_expires_at_unix_secs, Some(u64::MAX));
        assert_eq!(refreshed.scopes, vec!["a", "b", "c"]);
    }
    #[test]
    fn parse_account_id_from_jwt_access_token_with_top_level_precedence_and_opaque_fallback() {
        let jwt_access_token = "eyJhbGciOiJub25lIn0.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiand0LWFjY291bnQifX0.signature";
        let jwt_response = parse_token_endpoint_response(
            format!(r#"{{"access_token":"{jwt_access_token}","expires_in":60}}"#).as_bytes(),
        )
        .expect("JWT token response parses");
        assert_eq!(jwt_response.account_id.as_deref(), Some("jwt-account"));

        let top_level_response = parse_token_endpoint_response(
            format!(
                r#"{{"access_token":"{jwt_access_token}","expires_in":60,"account_id":"response-account"}}"#
            )
            .as_bytes(),
        )
        .expect("top-level account id response parses");
        assert_eq!(
            top_level_response.account_id.as_deref(),
            Some("response-account")
        );

        let opaque_response =
            parse_token_endpoint_response(br#"{"access_token":"opaque-access","expires_in":60}"#)
                .expect("opaque token response parses");
        assert_eq!(opaque_response.account_id, None);
    }

    #[test]
    fn account_id_from_access_token_derives_reusable_openai_jwt_claim() {
        let access_token = jwt_access_token(serde_json::json!({
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "jwt-account",
            },
        }));

        assert_eq!(
            account_id_from_access_token(&access_token).as_deref(),
            Some("jwt-account")
        );
    }

    #[test]
    fn account_id_from_access_token_rejects_opaque_malformed_and_empty_claims() {
        use base64::Engine as _;

        assert_eq!(account_id_from_access_token("opaque-access"), None);
        let malformed_claims = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(b"{malformed-json")
        );
        assert_eq!(account_id_from_access_token(&malformed_claims), None);
        assert_eq!(
            account_id_from_access_token("header.not-base64.signature"),
            None
        );
        assert_eq!(
            account_id_from_access_token(&jwt_access_token(serde_json::json!({}))),
            None
        );
        assert_eq!(
            account_id_from_access_token(&jwt_access_token(serde_json::json!({
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": "",
                },
            }))),
            None
        );
    }

    #[test]
    fn parse_token_endpoint_response_preserves_explicit_top_level_account_id() {
        let access_token = jwt_access_token(serde_json::json!({
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "jwt-account",
            },
        }));
        let response = parse_token_endpoint_response(
            serde_json::json!({
                "access_token": access_token,
                "expires_in": 60,
                "account_id": "response-account",
            })
            .to_string()
            .as_bytes(),
        )
        .expect("token response parses");

        assert_eq!(response.account_id.as_deref(), Some("response-account"));
    }

    fn jwt_access_token(claims: serde_json::Value) -> String {
        use base64::Engine as _;

        format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("claims serialize"))
        )
    }
}
