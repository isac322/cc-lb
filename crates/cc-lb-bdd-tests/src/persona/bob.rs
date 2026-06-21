use anyhow::Result;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use fake_anthropic::{ScriptedMessageResponse, SseEvent};
use serde_json::json;

use crate::harness::BddHarness;
use crate::persona::http;
use crate::results::HttpResponse;

pub struct Bob<'a> {
    harness: Option<&'a BddHarness>,
}

impl<'a> Bob<'a> {
    pub(crate) fn new(_storage: crate::backend::StorageHandle) -> Self {
        Self { harness: None }
    }

    pub(crate) fn from_harness(harness: &'a BddHarness) -> Self {
        Self {
            harness: Some(harness),
        }
    }

    pub async fn admin_request(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> HttpResponse {
        match self.harness {
            Some(harness) => http::admin_request(harness.admin_router(), method, path, body).await,
            None => http::json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "bdd_harness_unavailable" }),
            ),
        }
    }

    pub async fn proxy_request(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> HttpResponse {
        match self.harness {
            Some(harness) => http::proxy_request(harness.proxy_router(), method, path, body).await,
            None => http::json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "bdd_harness_unavailable" }),
            ),
        }
    }

    pub async fn proxy_request_with_headers(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
        headers: HeaderMap,
    ) -> HttpResponse {
        match self.harness {
            Some(harness) => {
                http::proxy_request_with_headers(
                    harness.proxy_router(),
                    method,
                    path,
                    body,
                    headers,
                )
                .await
            }
            None => http::json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "bdd_harness_unavailable" }),
            ),
        }
    }

    pub async fn bob_send_message(&self, body: serde_json::Value) -> HttpResponse {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("sk-ant-bdd"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        self.proxy_request_with_headers(Method::POST, "/v1/messages", Some(body), headers)
            .await
    }

    pub fn recorded_message_count(&self) -> usize {
        self.harness
            .map(|harness| harness.script.request_count())
            .unwrap_or_default()
    }

    pub fn push_sse_response(&self, events: Vec<SseEvent>) -> Result<()> {
        let Some(harness) = self.harness else {
            anyhow::bail!("bdd harness unavailable for scripted SSE response");
        };
        harness
            .script
            .push_response(ScriptedMessageResponse::sse(events));
        Ok(())
    }

    pub fn push_drop_response(&self, bytes: Vec<u8>, after_bytes: usize) -> Result<()> {
        let Some(harness) = self.harness else {
            anyhow::bail!("bdd harness unavailable for scripted drop response");
        };
        harness.script.push_response(
            ScriptedMessageResponse::drop_after_bytes(bytes, after_bytes)
                .with_header("content-type", "text/event-stream; charset=utf-8"),
        );
        Ok(())
    }

    pub fn push_json_response(&self, body: serde_json::Value) -> Result<()> {
        let Some(harness) = self.harness else {
            anyhow::bail!("bdd harness unavailable for scripted JSON response");
        };
        harness
            .script
            .push_response(ScriptedMessageResponse::Json(body));
        Ok(())
    }
}
