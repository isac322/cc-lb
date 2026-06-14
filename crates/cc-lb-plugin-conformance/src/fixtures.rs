use std::collections::{BTreeMap, BTreeSet};

use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, PluginIdentity};
use cc_lb_plugin_wire::v2::common::{
    CandidateWire, HeaderWire, ObserveEventWire, Principal, UpstreamWire,
};
use cc_lb_plugin_wire::v2::observe::ObserveRequest;
use cc_lb_plugin_wire::v2::shape::ShapeRequest;
use cc_lb_plugin_wire::v3::filter::FilterRequest;
use cc_lb_plugin_wire::wire_function::WireFunction;

pub fn synth_principal() -> Principal {
    Principal::dry_run_sample()
}

pub fn synth_metadata() -> AugmentedMetadata {
    let mut negotiated_functions = BTreeMap::new();
    negotiated_functions.insert(String::from("shape"), 1);
    negotiated_functions.insert(String::from("filter"), 1);
    negotiated_functions.insert(String::from("observe"), 1);

    AugmentedMetadata {
        identity: PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: String::from("dry-run-plugin"),
            plugin_version: String::from("0.0.0"),
        },
        negotiated_functions,
        negotiated_capabilities: BTreeSet::new(),
        handshake_completed_at: 0,
        self_check_passed: true,
        self_check_completed_at: 0,
        expires_at: 0,
    }
}

pub fn shape_request_builder() -> ShapeRequestBuilder {
    ShapeRequestBuilder {
        request: ShapeRequest::dry_run_sample(),
    }
}

#[non_exhaustive]
pub struct ShapeRequestBuilder {
    request: ShapeRequest,
}

impl ShapeRequestBuilder {
    pub fn method(mut self, method: impl Into<String>) -> Self {
        self.request.request.method = method.into();
        self
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.request.request.path = path.into();
        self
    }

    pub fn query(mut self, query: impl Into<String>) -> Self {
        self.request.request.query = Some(query.into());
        self
    }

    pub fn body_base64(mut self, body_base64: impl Into<String>) -> Self {
        self.request.request.body_base64 = body_base64.into();
        self
    }

    pub fn headers(mut self, headers: Vec<HeaderWire>) -> Self {
        self.request.request.headers = headers;
        self
    }

    pub fn principal(mut self, principal: Principal) -> Self {
        self.request.principal = principal;
        self
    }

    pub fn upstream(mut self, upstream: UpstreamWire) -> Self {
        self.request.upstream = upstream;
        self
    }

    pub fn build(self) -> ShapeRequest {
        self.request
    }
}

pub fn filter_request_builder() -> FilterRequestBuilder {
    FilterRequestBuilder {
        request: FilterRequest::dry_run_sample(),
    }
}

#[non_exhaustive]
pub struct FilterRequestBuilder {
    request: FilterRequest,
}

impl FilterRequestBuilder {
    pub fn request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request.request_id = request_id.into();
        self
    }

    pub fn headers(mut self, headers: Vec<HeaderWire>) -> Self {
        self.request.headers = headers;
        self
    }

    pub fn method(mut self, method: impl Into<String>) -> Self {
        self.request.method = method.into();
        self
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.request.path = path.into();
        self
    }

    pub fn query(mut self, query: impl Into<String>) -> Self {
        self.request.query = Some(query.into());
        self
    }

    pub fn body_base64(mut self, body_base64: impl Into<String>) -> Self {
        self.request.body_base64 = body_base64.into();
        self
    }

    pub fn principal(mut self, principal: Principal) -> Self {
        self.request.principal = principal;
        self
    }

    pub fn candidates(mut self, candidates: Vec<CandidateWire>) -> Self {
        self.request.candidates = candidates;
        self
    }

    pub fn build(self) -> FilterRequest {
        self.request
    }
}

pub fn observe_request_builder() -> ObserveRequestBuilder {
    ObserveRequestBuilder {
        request: ObserveRequest::dry_run_sample(),
    }
}

#[non_exhaustive]
pub struct ObserveRequestBuilder {
    request: ObserveRequest,
}

impl ObserveRequestBuilder {
    pub fn add_event(mut self, event: ObserveEventWire) -> Self {
        self.request.events.push(event);
        self
    }

    pub fn build(self) -> ObserveRequest {
        self.request
    }
}

pub fn dry_run_sample<F: WireFunction>() -> F::Request {
    F::dry_run_request()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_plugin_wire::v2::common::ObserveEventWire;
    use cc_lb_plugin_wire::v2::shape::ShapeFn;

    #[test]
    fn shape_builder_overrides_request_fields() {
        let request = shape_request_builder()
            .method("GET")
            .path("/v1/models")
            .query("limit=1")
            .body_base64("e30=")
            .headers(vec![HeaderWire {
                name: String::from("content-type"),
                value_base64: String::from("YXBwbGljYXRpb24vanNvbg=="),
            }])
            .principal(synth_principal())
            .upstream(UpstreamWire::dry_run_sample())
            .build();

        assert_eq!(request.request.method, "GET");
        assert_eq!(request.request.path, "/v1/models");
        assert_eq!(request.request.query.as_deref(), Some("limit=1"));
        assert_eq!(request.request.body_base64, "e30=");
        assert_eq!(request.request.headers.len(), 1);
    }

    #[test]
    fn filter_builder_overrides_filter_fields() {
        let request = filter_request_builder()
            .request_id("req-1")
            .headers(Vec::new())
            .method("POST")
            .path("/v1/messages")
            .query("beta=true")
            .body_base64("")
            .principal(synth_principal())
            .candidates(vec![CandidateWire::dry_run_sample()])
            .build();

        assert_eq!(request.request_id, "req-1");
        assert_eq!(request.query.as_deref(), Some("beta=true"));
        assert_eq!(request.candidates.len(), 1);
    }

    #[test]
    fn observe_builder_appends_events() {
        let request = observe_request_builder()
            .add_event(ObserveEventWire::dry_run_sample())
            .build();

        assert_eq!(request.events.len(), 1);
    }

    #[test]
    fn dry_run_sample_forwards_to_wire_function() {
        let request = dry_run_sample::<ShapeFn>();

        assert_eq!(request, ShapeRequest::dry_run_sample());
    }

    #[test]
    fn synth_metadata_has_protocol_defaults_and_zero_timestamps() {
        let metadata = synth_metadata();

        assert_eq!(metadata.identity.plugin_name, "dry-run-plugin");
        assert_eq!(metadata.negotiated_functions.get("shape"), Some(&1));
        assert_eq!(metadata.negotiated_functions.get("filter"), Some(&1));
        assert_eq!(metadata.negotiated_functions.get("observe"), Some(&1));
        assert_eq!(metadata.handshake_completed_at, 0);
        assert_eq!(metadata.self_check_completed_at, 0);
        assert_eq!(metadata.expires_at, 0);
    }
}
