use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Endpoint-level classification of a persisted request event.
///
/// New rows are classified at the earliest request boundary from the ingress
/// path (see [`RequestEventKind::from_path`]); scheduler-driven cache
/// keepalive writes classify as [`Self::Renewal`] directly. Rows persisted
/// before this field existed carry no classification and read back as
/// `None`, which filter predicates resolve to [`Self::Unclassified`] via
/// [`RequestEventKind::effective`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestEventKind {
    /// `POST /v1/messages` exactly.
    Messages,
    /// `POST /v1/messages/count_tokens` exactly.
    CountTokens,
    /// `POST /v1/responses` exactly.
    OpenaiResponses,
    /// Gemini `generateContent` and `streamGenerateContent` model actions.
    GeminiGenerateContent,
    /// Gemini `countTokens` model action.
    GeminiCountTokens,
    /// `/v1/models` and `/v1/models/{id}`.
    Models,
    /// `/v1/files`, `/v1/files/{id}`, and `/v1/files/{id}/content`.
    Files,
    /// Any other classified ingress path (wildcard passthrough routes).
    Other,
    /// Scheduler cache-keepalive renewal; authoritative via `source_kind`.
    Renewal,
    /// No endpoint metadata was recorded (historical rows).
    Unclassified,
}

impl RequestEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::CountTokens => "count_tokens",
            Self::OpenaiResponses => "openai_responses",
            Self::GeminiGenerateContent => "gemini_generate_content",
            Self::GeminiCountTokens => "gemini_count_tokens",
            Self::Models => "models",
            Self::Files => "files",
            Self::Other => "other",
            Self::Renewal => "renewal",
            Self::Unclassified => "unclassified",
        }
    }

    /// Classify an ingress request path. Exact-segment matching only:
    /// Message and Responses endpoints match exactly. Gemini model actions
    /// require a non-empty, single-segment model name. `/v1/models` and
    /// `/v1/files` match themselves and slash-separated descendants; every
    /// other path, including prefix lookalikes, is [`Self::Other`].
    pub fn from_path(path: &str) -> Self {
        match path {
            "/v1/messages" => Self::Messages,
            "/v1/messages/count_tokens" => Self::CountTokens,
            "/v1/responses" => Self::OpenaiResponses,
            p if p.starts_with("/v1beta/models/") => {
                let Some((model, action)) = p
                    .strip_prefix("/v1beta/models/")
                    .and_then(|suffix| suffix.rsplit_once(':'))
                else {
                    return Self::Other;
                };
                if model.is_empty() || model.contains('/') {
                    return Self::Other;
                }
                match action {
                    "generateContent" | "streamGenerateContent" => Self::GeminiGenerateContent,
                    "countTokens" => Self::GeminiCountTokens,
                    _ => Self::Other,
                }
            }
            p if p == "/v1/models" || p.starts_with("/v1/models/") => Self::Models,
            p if p == "/v1/files" || p.starts_with("/v1/files/") => Self::Files,
            _ => Self::Other,
        }
    }

    /// Resolve the effective kind for a row or event. `source_kind =
    /// "renewal"` is authoritative and takes precedence over any recorded
    /// `event_kind` (renewals replay `/v1/messages` but are not proxy
    /// traffic); otherwise the recorded `event_kind` wins, and a missing one
    /// resolves to [`Self::Unclassified`].
    pub fn effective(source_kind: Option<&str>, event_kind: Option<Self>) -> Self {
        if source_kind == Some("renewal") {
            return Self::Renewal;
        }
        event_kind.unwrap_or(Self::Unclassified)
    }
}

impl fmt::Display for RequestEventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseRequestEventKindError(pub String);

impl fmt::Display for ParseRequestEventKindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown request event kind: {:?}", self.0)
    }
}

impl std::error::Error for ParseRequestEventKindError {}

impl FromStr for RequestEventKind {
    type Err = ParseRequestEventKindError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "messages" => Ok(Self::Messages),
            "count_tokens" => Ok(Self::CountTokens),
            "openai_responses" => Ok(Self::OpenaiResponses),
            "gemini_generate_content" => Ok(Self::GeminiGenerateContent),
            "gemini_count_tokens" => Ok(Self::GeminiCountTokens),
            "models" => Ok(Self::Models),
            "files" => Ok(Self::Files),
            "other" => Ok(Self::Other),
            "renewal" => Ok(Self::Renewal),
            "unclassified" => Ok(Self::Unclassified),
            other => Err(ParseRequestEventKindError(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RequestEventKind;

    #[test]
    fn from_path_matches_exact_messages_endpoints() {
        assert_eq!(
            RequestEventKind::from_path("/v1/messages"),
            RequestEventKind::Messages
        );
        assert_eq!(
            RequestEventKind::from_path("/v1/messages/count_tokens"),
            RequestEventKind::CountTokens
        );
        // Descendants of /v1/messages other than count_tokens are not
        // messages traffic.
        assert_eq!(
            RequestEventKind::from_path("/v1/messages/extra"),
            RequestEventKind::Other
        );
        assert_eq!(
            RequestEventKind::from_path("/v1/messages/"),
            RequestEventKind::Other
        );
    }

    #[test]
    fn from_path_matches_multi_provider_endpoints() {
        for (path, expected) in [
            ("/v1/responses", RequestEventKind::OpenaiResponses),
            (
                "/v1beta/models/gemini-2.5-pro:generateContent",
                RequestEventKind::GeminiGenerateContent,
            ),
            (
                "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
                RequestEventKind::GeminiGenerateContent,
            ),
            (
                "/v1beta/models/gemini-2.5-pro:countTokens",
                RequestEventKind::GeminiCountTokens,
            ),
        ] {
            assert_eq!(RequestEventKind::from_path(path), expected, "path {path:?}");
        }
    }

    #[test]
    fn from_path_matches_models_and_files_descendants() {
        assert_eq!(
            RequestEventKind::from_path("/v1/models"),
            RequestEventKind::Models
        );
        assert_eq!(
            RequestEventKind::from_path("/v1/models/claude-sonnet-4-5"),
            RequestEventKind::Models
        );
        assert_eq!(
            RequestEventKind::from_path("/v1/files"),
            RequestEventKind::Files
        );
        assert_eq!(
            RequestEventKind::from_path("/v1/files/file-abc/content"),
            RequestEventKind::Files
        );
    }

    #[test]
    fn from_path_rejects_prefix_lookalikes() {
        for path in [
            "/v1/modelsx",
            "/v1/models-extra",
            "/v1/filesx",
            "/v1/messagesx",
            "/v1/messages/count_tokens/extra",
            "/v1/responses/",
            "/v1/responses/extra",
            "/v1/responsesx",
            "/v1beta/models/:generateContent",
            "/v1beta/models/:streamGenerateContent",
            "/v1beta/models/:countTokens",
            "/v1beta/models/gemini/pro:generateContent",
            "/v1beta/models/gemini/pro:streamGenerateContent",
            "/v1beta/models/gemini/pro:countTokens",
            "/v1beta/models/gemini-2.5-pro:generateContent/extra",
            "/v1beta/models/gemini-2.5-pro:streamGenerateContent/",
            "/v1beta/models/gemini-2.5-pro:countTokens/extra",
            "/v1beta/models/gemini-2.5-pro:generateContents",
            "/v1beta/models/gemini-2.5-pro",
            "/v1beta/modelsx/gemini-2.5-pro:generateContent",
            "/v1beta/model/gemini-2.5-pro:countTokens",
            "/v1",
            "/api/v1/messages",
            "",
        ] {
            assert_eq!(
                RequestEventKind::from_path(path),
                RequestEventKind::Other,
                "path {path:?} must classify as other"
            );
        }
    }

    #[test]
    fn effective_gives_source_kind_renewal_precedence() {
        for kind in [
            RequestEventKind::OpenaiResponses,
            RequestEventKind::GeminiGenerateContent,
            RequestEventKind::GeminiCountTokens,
        ] {
            assert_eq!(
                RequestEventKind::effective(Some("renewal"), Some(kind)),
                RequestEventKind::Renewal
            );
            assert_eq!(RequestEventKind::effective(None, Some(kind)), kind);
        }
        assert_eq!(
            RequestEventKind::effective(Some("renewal"), Some(RequestEventKind::Messages)),
            RequestEventKind::Renewal
        );
        assert_eq!(
            RequestEventKind::effective(Some("renewal"), None),
            RequestEventKind::Renewal
        );
        assert_eq!(
            RequestEventKind::effective(Some("proxy"), Some(RequestEventKind::Files)),
            RequestEventKind::Files
        );
        assert_eq!(
            RequestEventKind::effective(None, Some(RequestEventKind::Models)),
            RequestEventKind::Models
        );
        assert_eq!(
            RequestEventKind::effective(Some("proxy"), None),
            RequestEventKind::Unclassified
        );
        assert_eq!(
            RequestEventKind::effective(None, None),
            RequestEventKind::Unclassified
        );
    }

    #[test]
    fn as_str_roundtrips_through_from_str_and_serde() {
        for kind in [
            RequestEventKind::Messages,
            RequestEventKind::CountTokens,
            RequestEventKind::OpenaiResponses,
            RequestEventKind::GeminiGenerateContent,
            RequestEventKind::GeminiCountTokens,
            RequestEventKind::Models,
            RequestEventKind::Files,
            RequestEventKind::Other,
            RequestEventKind::Renewal,
            RequestEventKind::Unclassified,
        ] {
            let parsed: RequestEventKind = kind.as_str().parse().expect("from_str roundtrip");
            assert_eq!(parsed, kind);
            let json = serde_json::to_string(&kind).expect("serialize kind");
            assert_eq!(json, format!("\"{}\"", kind.as_str()));
        }
        assert!("bogus".parse::<RequestEventKind>().is_err());
    }
}
