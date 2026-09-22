use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderValue};
use axum::response::Response;

use crate::oauth::OAuthState;

use super::config::AppConfig;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct AppState {
    pub(crate) config: AppConfig,
    pub(crate) oauth: OAuthState,
    request_counts: Mutex<BTreeMap<&'static str, u64>>,
    pub(crate) last_x_api_key: Mutex<Option<String>>,
    pub(crate) last_authorization: Mutex<Option<String>>,
    pub(crate) last_selected_headers: Mutex<BTreeMap<&'static str, Option<String>>>,
    #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
    pub(crate) injected_usage:
        Mutex<std::collections::HashMap<String, super::debug::InjectedUsage>>,
    #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
    pub(crate) default_injected_usage: Mutex<Option<super::debug::InjectedUsage>>,
}

impl AppState {
    pub(crate) fn new(config: AppConfig) -> Self {
        Self {
            config,
            oauth: OAuthState::default(),
            request_counts: Mutex::new(BTreeMap::new()),
            last_x_api_key: Mutex::new(None),
            last_authorization: Mutex::new(None),
            last_selected_headers: Mutex::new(BTreeMap::from([
                ("x-organization-uuid", None),
                ("x-trusted-device-token", None),
            ])),
            #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
            injected_usage: Mutex::new(std::collections::HashMap::new()),
            #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
            default_injected_usage: Mutex::new(None),
        }
    }

    pub(crate) fn record_auth(&self, label: &'static str) {
        if let Ok(mut counts) = self.request_counts.lock() {
            let count = counts
                .entry(label)
                .and_modify(|value| *value += 1)
                .or_insert(1);
            eprintln!("fake-anthropic request_count key_label={label} count={count}");
        }
    }

    pub(crate) fn record_last_request(&self, headers: &HeaderMap) {
        if let Ok(mut last_x_api_key) = self.last_x_api_key.lock() {
            *last_x_api_key = headers
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned);
        }
        if let Ok(mut last_authorization) = self.last_authorization.lock() {
            *last_authorization = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned);
        }
        if let Ok(mut selected_headers) = self.last_selected_headers.lock() {
            for name in ["x-organization-uuid", "x-trusted-device-token"] {
                selected_headers.insert(
                    name,
                    headers
                        .get(name)
                        .and_then(|value| value.to_str().ok())
                        .map(ToOwned::to_owned),
                );
            }
        }
    }
}

pub fn with_fixture_headers(mut response: Response) -> Response {
    let request_id = next_request_id();
    let request_id = match HeaderValue::from_str(&request_id) {
        Ok(value) => value,
        Err(_) => HeaderValue::from_static("req_fallback"),
    };
    response.headers_mut().insert("request-id", request_id);
    response.headers_mut().insert(
        "anthropic-organization-id",
        HeaderValue::from_static("org_test"),
    );
    response.headers_mut().insert(
        "anthropic-ratelimit-requests-remaining",
        HeaderValue::from_static("999"),
    );
    response.headers_mut().insert(
        "anthropic-ratelimit-tokens-remaining",
        HeaderValue::from_static("999000"),
    );
    response
}

fn next_request_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };
    format!("req_{nanos:x}{counter:x}")
}
