#![forbid(unsafe_code)]

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
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct TokenEndpointErrorResponse {
    error: Option<String>,
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

pub fn parse_token_endpoint_response(
    body: &[u8],
) -> Result<TokenEndpointResponse, TokenEndpointParseError> {
    serde_json::from_slice(body).map_err(|source| TokenEndpointParseError::Json { source })
}

fn parse_token_endpoint_error_response(body: &[u8]) -> TokenEndpointErrorResponse {
    serde_json::from_slice(body).unwrap_or(TokenEndpointErrorResponse { error: None })
}

fn canonical_terminal_token_endpoint_error_code(code: &str) -> Option<&'static str> {
    match code {
        "invalid_grant" => Some("invalid_grant"),
        "invalid_client" => Some("invalid_client"),
        "unauthorized_client" => Some("unauthorized_client"),
        _ => None,
    }
}

pub fn terminal_token_endpoint_error_code(status: u16, body: &[u8]) -> Option<&'static str> {
    if !(400..500).contains(&status) || matches!(status, 408 | 429) {
        return None;
    }
    parse_token_endpoint_error_response(body)
        .error
        .as_deref()
        .and_then(canonical_terminal_token_endpoint_error_code)
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
    fn terminal_token_endpoint_errors_are_classified_by_status_and_oauth_code() {
        for status in [400, 401, 403] {
            for code in ["invalid_grant", "invalid_client", "unauthorized_client"] {
                let body = format!(r#"{{"error":"{code}","error_description":"rejected"}}"#);
                assert_eq!(
                    terminal_token_endpoint_error_code(status, body.as_bytes()),
                    Some(code),
                );
            }
        }
    }

    #[test]
    fn transient_status_or_unknown_token_endpoint_error_is_retryable() {
        for (status, body) in [
            (400, br#"{"error":"temporarily_unavailable"}"#.as_slice()),
            (400, br#"{"error_description":"missing code"}"#.as_slice()),
            (400, br#"not json"#.as_slice()),
            (408, br#"{"error":"invalid_grant"}"#.as_slice()),
            (429, br#"{"error":"invalid_grant"}"#.as_slice()),
            (500, br#"{"error":"invalid_grant"}"#.as_slice()),
        ] {
            assert_eq!(terminal_token_endpoint_error_code(status, body), None);
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
}
