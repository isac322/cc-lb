use std::collections::BTreeMap;

use rand::RngExt as _;
use rand::rngs::StdRng;

use crate::manifest::{Manifest, Persona, PlannedRequest, Principal, Profile, Session, Wave};
use crate::traffic::{ExpectedLabel, RequestBodyShape, SlowReaderPolicy};

pub struct TrafficCatalog {
    pub personas: Vec<Persona>,
    pub principals: Vec<Principal>,
    pub sessions: Vec<Session>,
}

impl TrafficCatalog {
    pub fn new() -> Self {
        Self {
            personas: crate::traffic_catalog_data::personas(),
            principals: crate::traffic_catalog_data::principals(),
            sessions: crate::traffic_catalog_data::sessions(),
        }
    }

    pub fn plan_requests(
        &self,
        waves: &[Wave],
        profile: Profile,
        rng: &mut StdRng,
    ) -> Vec<PlannedRequest> {
        let mut number = 1;
        let mut requests = Vec::with_capacity(waves.len() * profile.requests_per_wave());
        for wave in waves {
            for _ in 0..profile.requests_per_wave() {
                let template = recipe((number - 1) % 8);
                let body = template.body.to_owned();
                requests.push(PlannedRequest {
                    request_id: format!("request-{number}"),
                    scheduled_send_at_ms: wave.starts_at_ms
                        + rng.random_range(25_u64..profile.send_window_ms()),
                    wave_id: wave.wave_id.clone(),
                    persona_id: template.persona_id.to_owned(),
                    principal_id: template.principal_id.to_owned(),
                    session_id: template.session_id.to_owned(),
                    body_hash: Manifest::sha256_hex(body.as_bytes()),
                    body,
                    body_shape: template.body_shape,
                    stream: template.stream,
                    max_tokens: template.max_tokens,
                    model: template.model.map(str::to_owned),
                    hot_prefix_group: template.hot_prefix_group.to_owned(),
                    provider_headers: headers(template, rng),
                    slow_reader_policy: template.slow_reader_policy,
                    fake_script_id: template.fake_script_id.to_owned(),
                    expected_label: template.expected_label,
                    expected_status_class: template.expected_label.status_class(),
                });
                number += 1;
            }
        }
        requests
    }
}

#[derive(Clone, Copy)]
struct RequestTemplate {
    persona_id: &'static str,
    principal_id: &'static str,
    session_id: &'static str,
    body: &'static str,
    body_shape: RequestBodyShape,
    stream: bool,
    max_tokens: Option<u32>,
    model: Option<&'static str>,
    hot_prefix_group: &'static str,
    slow_reader_policy: SlowReaderPolicy,
    fake_script_id: &'static str,
    expected_label: ExpectedLabel,
}

macro_rules! template {
    ($persona_id:expr, $principal_id:expr, $session_id:expr, $body:expr, $body_shape:expr, $stream:expr, $max_tokens:expr, $model:expr, $hot_prefix_group:expr, $slow_reader_policy:expr, $fake_script_id:expr, $expected_label:expr $(,)?) => {
        RequestTemplate {
            persona_id: $persona_id,
            principal_id: $principal_id,
            session_id: $session_id,
            body: $body,
            body_shape: $body_shape,
            stream: $stream,
            max_tokens: $max_tokens,
            model: $model,
            hot_prefix_group: $hot_prefix_group,
            slow_reader_policy: $slow_reader_policy,
            fake_script_id: $fake_script_id,
            expected_label: $expected_label,
        }
    };
}

fn recipe(index: usize) -> RequestTemplate {
    const BRIEF: &str = r#"{"model":"fake-alpha","max_tokens":64,"messages":[{"role":"user","content":"Summarize the shared project brief."}],"stream":false}"#;
    match index {
        0 | 6 => template!(
            "steady-user",
            "stress-primary",
            "session-primary-1",
            BRIEF,
            RequestBodyShape::MessageText,
            false,
            Some(64),
            Some("fake-alpha"),
            "project-brief",
            SlowReaderPolicy::Eager,
            "fake-alpha",
            ExpectedLabel::Healthy,
        ),
        1 => template!(
            "steady-user",
            "stress-primary",
            "session-primary-2",
            r#"{"model":"fake-alpha","max_tokens":256,"messages":[{"role":"user","content":[{"type":"text","text":"Shared project brief."},{"type":"text","text":"Provide implementation risks."}]}],"stream":true}"#,
            RequestBodyShape::ContentBlocks,
            true,
            Some(256),
            Some("fake-alpha"),
            "project-brief",
            SlowReaderPolicy::Paced { delay_ms: 25 },
            "fake-alpha",
            ExpectedLabel::Healthy,
        ),
        2 => template!(
            "burst-user",
            "stress-secondary",
            "session-secondary-1",
            r#"{"model":"fake-beta","max_tokens":96,"messages":[{"role":"user","content":"Simulate an overloaded provider."}],"stream":false}"#,
            RequestBodyShape::MessageText,
            false,
            Some(96),
            Some("fake-beta"),
            "provider-errors",
            SlowReaderPolicy::Eager,
            "fake-beta",
            ExpectedLabel::ProviderError,
        ),
        3 => template!(
            "steady-user",
            "stress-primary",
            "session-primary-1",
            r#"{"model":"fake-beta","max_tokens":512,"messages":[{"role":"user","content":"Stream a long answer for cancellation."}],"stream":true}"#,
            RequestBodyShape::MessageText,
            true,
            Some(512),
            Some("fake-beta"),
            "cancellation",
            SlowReaderPolicy::CancelAfterChunks { chunks: 3 },
            "fake-beta",
            ExpectedLabel::ClientCancel,
        ),
        4 => template!(
            "edge-user",
            "stress-tertiary",
            "session-tertiary-1",
            r#"{"model":"fake-alpha","max_tokens":64,"messages":["#,
            RequestBodyShape::MalformedJson,
            false,
            None,
            None,
            "invalid-input",
            SlowReaderPolicy::Eager,
            "fake-alpha",
            ExpectedLabel::Malformed,
        ),
        5 => template!(
            "steady-user",
            "stress-primary",
            "session-primary-2",
            r#"{"model":"unsupported-fake-model","max_tokens":128,"messages":[{"role":"user","content":"Reject this unsupported model."}],"stream":false}"#,
            RequestBodyShape::UnsupportedModel,
            false,
            Some(128),
            Some("unsupported-fake-model"),
            "invalid-input",
            SlowReaderPolicy::Eager,
            "fake-alpha",
            ExpectedLabel::Unsupported,
        ),
        7 => template!(
            "steady-user",
            "stress-primary",
            "session-primary-1",
            r#"{"model":"fake-beta","max_tokens":384,"messages":[{"role":"user","content":"Use the shared tool contract."}],"tools":[{"name":"lookup","description":"Find a fact","input_schema":{"type":"object"}}],"stream":true}"#,
            RequestBodyShape::ToolUse,
            true,
            Some(384),
            Some("fake-beta"),
            "tool-contract",
            SlowReaderPolicy::Paced { delay_ms: 10 },
            "fake-beta",
            ExpectedLabel::Healthy,
        ),
        _ => template!(
            "steady-user",
            "stress-primary",
            "session-primary-1",
            BRIEF,
            RequestBodyShape::MessageText,
            false,
            Some(64),
            Some("fake-alpha"),
            "project-brief",
            SlowReaderPolicy::Eager,
            "fake-alpha",
            ExpectedLabel::Healthy,
        ),
    }
}

fn headers(template: RequestTemplate, rng: &mut StdRng) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    if template.stream {
        headers.insert(
            "x-fake-weather-seed".to_owned(),
            rng.random_range(1_u64..u64::MAX).to_string(),
        );
        headers.insert("x-fake-inter-token-ms".to_owned(), "5".to_owned());
    }
    match template.expected_label {
        ExpectedLabel::ProviderError => {
            headers.insert("x-fake-mode".to_owned(), "529".to_owned());
            headers.insert("x-fake-retry-after".to_owned(), "7".to_owned());
        }
        ExpectedLabel::ClientCancel => {
            headers.insert("x-fake-mode".to_owned(), "slow".to_owned());
            headers.insert("x-fake-delta-count".to_owned(), "100".to_owned());
        }
        ExpectedLabel::Healthy | ExpectedLabel::Malformed | ExpectedLabel::Unsupported => {}
    }
    headers
}
