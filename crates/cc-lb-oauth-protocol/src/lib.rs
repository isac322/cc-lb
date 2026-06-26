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
    pub scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingTokenParts {
    pub refresh_token: String,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshedTokenParts {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at_unix_secs: u64,
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

pub fn refreshed_token_parts(
    existing: ExistingTokenParts,
    response: TokenEndpointResponse,
    now_epoch_secs: u64,
) -> RefreshedTokenParts {
    let scopes = response
        .scope
        .map(|scope| scope.split_whitespace().map(str::to_owned).collect())
        .unwrap_or(existing.scopes);

    RefreshedTokenParts {
        access_token: response.access_token,
        refresh_token: response.refresh_token.unwrap_or(existing.refresh_token),
        expires_at_unix_secs: now_epoch_secs.saturating_add(response.expires_in),
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
    fn refreshed_token_parts_preserve_missing_refresh_token_and_scope() {
        // Given: an endpoint response that omits optional refresh_token and scope.
        let response =
            parse_token_endpoint_response(br#"{"access_token":"new-access","expires_in":30}"#)
                .unwrap();
        let existing = ExistingTokenParts {
            refresh_token: "old-refresh".to_owned(),
            scopes: vec!["old:scope".to_owned()],
        };

        // When: the response is materialized into refreshed token parts.
        let refreshed = refreshed_token_parts(existing, response, 100);

        // Then: fallback fields remain unchanged and expiry is relative to now.
        assert_eq!(refreshed.access_token, "new-access");
        assert_eq!(refreshed.refresh_token, "old-refresh");
        assert_eq!(refreshed.expires_at_unix_secs, 130);
        assert_eq!(refreshed.scopes, vec!["old:scope"]);
    }

    #[test]
    fn refreshed_token_parts_split_returned_scope() {
        // Given: an endpoint response that returns scopes as OAuth whitespace text.
        let response = parse_token_endpoint_response(
            br#"{"access_token":"new-access","refresh_token":"new-refresh","expires_in":5,"scope":"a b c"}"#,
        )
        .unwrap();
        let existing = ExistingTokenParts {
            refresh_token: "old-refresh".to_owned(),
            scopes: vec!["old".to_owned()],
        };

        // When: the response is materialized into refreshed token parts.
        let refreshed = refreshed_token_parts(existing, response, u64::MAX - 1);

        // Then: returned refresh_token/scope win and expiry saturates.
        assert_eq!(refreshed.refresh_token, "new-refresh");
        assert_eq!(refreshed.expires_at_unix_secs, u64::MAX);
        assert_eq!(refreshed.scopes, vec!["a", "b", "c"]);
    }
}
