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
    /// An Anthropic content-safety refusal: HTTP 200 with
    /// `stop_reason: "refusal"` and populated `stop_details`. Request-log QA
    /// uses it to prove an abnormal stop is distinguishable from a success,
    /// since both return HTTP 200.
    Refusal,
    /// A context-window exhaustion stop: HTTP 200 with
    /// `stop_reason: "model_context_window_exceeded"` and null `stop_details`.
    /// Request-log QA uses it to prove an abnormal stop is distinguishable
    /// from a success, since both return HTTP 200.
    ContextWindowExceeded,
}

impl FakeMode {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        headers
            .get("x-fake-mode")
            .and_then(|value| value.to_str().ok())
            .map(Self::from_str)
            .or_else(|| {
                std::env::var("FAKE_DEFAULT_MODE")
                    .ok()
                    .map(|value| Self::from_str(&value))
            })
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
            "opencode-tools" => Self::OpenCodeTools,
            "senpi-tools" => Self::SenpiTools,
            "refusal" => Self::Refusal,
            "context-window-exceeded" => Self::ContextWindowExceeded,
            _ => Self::Ok,
        }
    }
}
