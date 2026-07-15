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
}

impl FakeMode {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        headers
            .get("x-fake-mode")
            .and_then(|value| value.to_str().ok())
            .map(Self::from_str)
            .unwrap_or(Self::Ok)
    }

    fn from_str(value: &str) -> Self {
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
            _ => Self::Ok,
        }
    }
}
