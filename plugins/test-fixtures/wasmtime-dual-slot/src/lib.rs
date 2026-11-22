#![cfg_attr(target_arch = "wasm32", no_std)]
extern crate alloc;
use alloc::{boxed::Box, format, vec, vec::Vec};
use cc_lb_pdk_wasmtime::types::v2::*;
use cc_lb_pdk_wasmtime::types::v3;

#[cc_lb_pdk_wasmtime::plugin(
    name = "dual-slot-regression",
    version = "0.1.0",
    description = "Body-preserving dual-slot fixture",
    usage = "Local HTTP regression only"
)]
mod fixture {
    use super::*;

    fn shape(req: ShapeRequest) -> ShapeResponse {
        let base = req
            .upstream_identity
            .base_url
            .as_deref()
            .expect("local base URL")
            .trim_end_matches('/');
        let mut headers = req.headers.into_vec();
        headers.push(Header {
            name: Box::from("x-qa-shape"),
            value: Box::from(b"qualified-v2".as_slice()),
        });
        let url = match req.query {
            Some(query) => format!("{base}{}?{query}", req.path),
            None => format!("{base}{}", req.path),
        };
        ShapeResponse {
            url: url.into_boxed_str(),
            method: req.method,
            headers: headers.into_boxed_slice(),
            body: req.body,
        }
    }

    fn filter(req: v3::FilterRequest) -> FilterResponse {
        // The negative probe makes filter execution independently observable without editing a payload.
        let reject = req
            .headers
            .iter()
            .any(|h| h.name.eq_ignore_ascii_case("x-qa-reject") && h.value.as_ref() == b"1");
        FilterResponse {
            results: req
                .candidates
                .into_vec()
                .into_iter()
                .map(|candidate| PerCandidateReason {
                    upstream_id: candidate.upstream_id,
                    decision: Box::from(if reject { "reject" } else { "accept" }),
                    reason: Box::from("qa-qualified-filter-v3"),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn response(req: TransformResponseRequest) -> TransformResponseResult {
        let mut headers = req.response_headers.into_vec();
        headers.push(Header {
            name: Box::from("x-qa-transform"),
            value: Box::from(req.upstream_identity.provider.as_bytes()),
        });
        TransformResponseResult::Replace {
            status: None,
            headers: Some(headers.into_boxed_slice()),
            body: None,
        }
    }

    fn sse(req: TransformSseEventRequest) -> TransformSseEventResult {
        TransformSseEventResult::Replace {
            events: vec![req.event].into_boxed_slice(),
        }
    }

    #[cc_lb_pdk_wasmtime::handler(
        shape,
        provider = "openai",
        wire = 2,
        description = "Shape OpenAI",
        usage = "QA"
    )]
    pub fn shape_openai(req: ShapeRequest) -> ShapeResponse {
        shape(req)
    }
    #[cc_lb_pdk_wasmtime::handler(
        shape,
        provider = "gemini",
        wire = 2,
        description = "Shape Gemini",
        usage = "QA"
    )]
    pub fn shape_gemini(req: ShapeRequest) -> ShapeResponse {
        shape(req)
    }
    #[cc_lb_pdk_wasmtime::handler(
        filter,
        provider = "openai",
        wire = 3,
        description = "Filter OpenAI",
        usage = "QA"
    )]
    pub fn filter_openai(req: v3::FilterRequest) -> FilterResponse {
        filter(req)
    }
    #[cc_lb_pdk_wasmtime::handler(
        filter,
        provider = "gemini",
        wire = 3,
        description = "Filter Gemini",
        usage = "QA"
    )]
    pub fn filter_gemini(req: v3::FilterRequest) -> FilterResponse {
        filter(req)
    }
    #[cc_lb_pdk_wasmtime::handler(
        transform_response,
        provider = "openai",
        wire = 2,
        description = "OpenAI response marker",
        usage = "QA"
    )]
    pub fn response_openai(req: TransformResponseRequest) -> TransformResponseResult {
        response(req)
    }
    #[cc_lb_pdk_wasmtime::handler(
        transform_response,
        provider = "gemini",
        wire = 2,
        description = "Gemini response marker",
        usage = "QA"
    )]
    pub fn response_gemini(req: TransformResponseRequest) -> TransformResponseResult {
        response(req)
    }
    #[cc_lb_pdk_wasmtime::handler(
        transform_sse_event,
        provider = "openai",
        wire = 2,
        description = "Preserve OpenAI SSE",
        usage = "QA"
    )]
    pub fn sse_openai(req: TransformSseEventRequest) -> TransformSseEventResult {
        sse(req)
    }
    #[cc_lb_pdk_wasmtime::handler(
        transform_sse_event,
        provider = "gemini",
        wire = 2,
        description = "Preserve Gemini SSE",
        usage = "QA"
    )]
    pub fn sse_gemini(req: TransformSseEventRequest) -> TransformSseEventResult {
        sse(req)
    }
}
