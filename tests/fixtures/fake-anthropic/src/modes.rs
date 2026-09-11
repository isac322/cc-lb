use http::HeaderMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FakeMode {
    Ok,
    Unauthorized,
    RateLimited,
    /// A rate-limit error whose canonical message exceeds the proxy's 1024-byte
    /// `upstream_error_message` cap, so request-log QA can exercise truncation
    /// (storage/API) and long/multiline wrapping (admin-web drawer).
    RateLimitedLong,
    Overloaded,
    ServerError,
    Timeout,
    Slow,
    TamperUnknownEvent,
    TruncateMidStream,
    SenpiTools,
    OpenCodeTools,
}

impl FakeMode {
    pub fn from_headers(headers: &HeaderMap, default: Self) -> Self {
        headers
            .get("x-fake-mode")
            .and_then(|value| value.to_str().ok())
            .map(Self::parse)
            .unwrap_or(default)
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "401" => Self::Unauthorized,
            "429" => Self::RateLimited,
            "429-long" => Self::RateLimitedLong,
            "529" => Self::Overloaded,
            "500" => Self::ServerError,
            "timeout" => Self::Timeout,
            "slow" => Self::Slow,
            "tamper-unknown-event" => Self::TamperUnknownEvent,
            "truncate-mid-stream" => Self::TruncateMidStream,
            "opencode-tools" => Self::OpenCodeTools,
            "senpi-tools" => Self::SenpiTools,
            _ => Self::Ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};

    use super::FakeMode;

    #[test]
    fn explicit_default_mode_is_used_and_request_header_takes_precedence() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            FakeMode::from_headers(&headers, FakeMode::Overloaded),
            FakeMode::Overloaded
        );

        headers.insert("x-fake-mode", HeaderValue::from_static("401"));
        assert_eq!(
            FakeMode::from_headers(&headers, FakeMode::Overloaded),
            FakeMode::Unauthorized
        );
    }
}
