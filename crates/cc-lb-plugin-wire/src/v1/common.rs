extern crate alloc;

use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeaderWire {
    pub name: String,
    pub value_base64: String,
}

impl HeaderWire {
    pub fn dry_run_sample() -> Self {
        Self {
            name: String::from("x-cc-lb-dry-run"),
            value_base64: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Principal {
    pub id: String,
    pub kind: String,
    pub claims: serde_json::Map<String, serde_json::Value>,
}

impl Principal {
    pub fn dry_run_sample() -> Self {
        Self {
            id: String::from("dry-run-principal"),
            kind: String::from("api_key"),
            claims: serde_json::Map::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RequestWire {
    pub request_id: String,
    pub headers: Vec<HeaderWire>,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub body_base64: String,
}

impl RequestWire {
    pub fn dry_run_sample() -> Self {
        Self {
            request_id: String::from("dry-run-request"),
            headers: Vec::new(),
            method: String::from("POST"),
            path: String::from("/v1/messages"),
            query: None,
            body_base64: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RateLimitObservationWire {
    pub kind: String,
    pub window: String,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub reset: Option<String>,
}

impl RateLimitObservationWire {
    pub fn dry_run_sample() -> Self {
        Self {
            kind: String::from("requests"),
            window: String::from("dry-run"),
            limit: None,
            remaining: None,
            reset: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CandidateWire {
    pub upstream_id: String,
    pub name: String,
    pub kind: String,
    pub observed_rate_limits: Vec<RateLimitObservationWire>,
    pub observed_at_unix_secs: u64,
}

impl CandidateWire {
    pub fn dry_run_sample() -> Self {
        Self {
            upstream_id: String::from("00000000-0000-0000-0000-000000000000"),
            name: String::from("dry-run-upstream"),
            kind: String::from("anthropic_api_key"),
            observed_rate_limits: Vec::new(),
            observed_at_unix_secs: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[serde(deny_unknown_fields)]
pub enum UpstreamWire {
    AnthropicDirect,
    CustomAnthropicSpec { base_url: String },
}

impl UpstreamWire {
    pub fn dry_run_sample() -> Self {
        Self::AnthropicDirect
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[serde(deny_unknown_fields)]
pub enum DialectBinding {
    #[serde(rename = "self")]
    SelfReferenced,
}

impl DialectBinding {
    pub fn dry_run_sample() -> Self {
        Self::SelfReferenced
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShapedRequestWire {
    pub url: String,
    pub method: String,
    pub headers: Vec<HeaderWire>,
    pub body_base64: String,
}

impl ShapedRequestWire {
    pub fn dry_run_sample() -> Self {
        Self {
            url: String::from("https://api.anthropic.com/v1/messages"),
            method: String::from("POST"),
            headers: Vec::new(),
            body_base64: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum UpstreamErrorCategory {
    Unauthorized,
    Retryable,
    Failed,
}

impl UpstreamErrorCategory {
    pub fn dry_run_sample() -> Self {
        Self::Unauthorized
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpstreamErrorWire {
    pub status: u16,
    pub body_base64: Option<String>,
    pub category: UpstreamErrorCategory,
}

impl UpstreamErrorWire {
    pub fn dry_run_sample() -> Self {
        Self {
            status: 401,
            body_base64: None,
            category: UpstreamErrorCategory::dry_run_sample(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[serde(deny_unknown_fields)]
pub enum ObserveEventWire {
    RequestStarted {
        request_id: String,
        downstream_user_agent: Option<String>,
    },
    AuthnComplete {
        principal_id: String,
        principal_kind: String,
    },
    UpstreamChosen {
        upstream: UpstreamWire,
    },
    Chunk {
        batch_index: u64,
        event_count: usize,
        total_bytes: usize,
    },
    RequestFinished {
        status: u16,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        duration_ms: u64,
    },
    Error {
        code: String,
        message: String,
        source: String,
    },
}

impl ObserveEventWire {
    pub fn dry_run_sample() -> Self {
        Self::RequestStarted {
            request_id: String::from("dry-run-request"),
            downstream_user_agent: None,
        }
    }
}
