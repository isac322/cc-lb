use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_signer_anthropic_oauth::{
    OAuthHttpClient, OAuthHttpError, OAuthTokenRequest, OAuthTokenResponse,
};
use http::StatusCode;
use secrecy::ExposeSecret;

const TEST_NOW_SECS: u64 = 1_700_000_000;

#[derive(Debug)]
pub struct FakeOAuthClient {
    calls: AtomicU32,
    responses: Mutex<VecDeque<OAuthTokenResponse>>,
    bodies: Mutex<Vec<String>>,
}

impl FakeOAuthClient {
    pub fn new(responses: Vec<OAuthTokenResponse>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicU32::new(0),
            responses: Mutex::new(VecDeque::from(responses)),
            bodies: Mutex::new(Vec::new()),
        })
    }

    pub fn call_count(&self) -> u32 {
        self.calls.load(Ordering::Relaxed)
    }

    pub fn bodies(&self) -> Vec<String> {
        self.bodies.lock().expect("bodies lock").clone()
    }
}

#[async_trait]
impl OAuthHttpClient for FakeOAuthClient {
    async fn post_token(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthHttpError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.bodies
            .lock()
            .expect("bodies lock")
            .push(request.form_body.expose_secret().to_owned());
        self.responses
            .lock()
            .expect("responses lock")
            .pop_front()
            .ok_or_else(|| OAuthHttpError::Request {
                reason: "fake response queue empty".to_owned(),
            })
    }
}

pub fn success_response(
    access_token: &str,
    refresh_token: Option<&str>,
    expires_in: u64,
) -> OAuthTokenResponse {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "access_token".to_owned(),
        serde_json::Value::String(access_token.to_owned()),
    );
    if let Some(refresh_token) = refresh_token {
        fields.insert(
            "refresh_token".to_owned(),
            serde_json::Value::String(refresh_token.to_owned()),
        );
    }
    fields.insert(
        "expires_in".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(expires_in)),
    );
    fields.insert(
        "scope".to_owned(),
        serde_json::Value::String("messages files".to_owned()),
    );
    OAuthTokenResponse {
        status: StatusCode::OK,
        body: Bytes::from(serde_json::Value::Object(fields).to_string()),
    }
}

pub fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(TEST_NOW_SECS))
}
