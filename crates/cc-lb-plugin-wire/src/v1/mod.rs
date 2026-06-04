pub mod build_signer;
pub mod common;
pub mod normalize_error;
pub mod observe;
pub mod on_unauthorized;
pub mod route;
pub mod shape;
pub mod sign;

pub use common::{
    CandidateWire, DialectBinding, HeaderWire, ObserveEventWire, Principal,
    RateLimitObservationWire, RequestWire, ShapedRequestWire,
    SubscriptionQuotaCandidateSnapshotWire, UpstreamErrorCategory, UpstreamErrorWire, UpstreamWire,
};

#[cfg(test)]
mod tests {
    extern crate alloc;

    use core::fmt::Debug;

    use serde::{Serialize, de::DeserializeOwned};

    use super::*;
    use crate::wire_function::{FallbackPolicy, WireFunction};

    fn round_trip<T>(value: T)
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug,
    {
        let bytes = serde_json::to_vec(&value).unwrap();
        let decoded: T = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value, decoded);
    }

    fn assert_v1<F: WireFunction>() {
        assert_eq!(F::SUPPORTED_VERSIONS, &[1]);
    }

    #[test]
    fn seven_impls() {
        assert_eq!(<route::RouteFn as WireFunction>::NAME, "route");
        assert_eq!(<shape::ShapeFn as WireFunction>::NAME, "shape");
        assert_eq!(
            <normalize_error::NormalizeErrorFn as WireFunction>::NAME,
            "normalize_error"
        );
        assert_eq!(
            <build_signer::BuildSignerFn as WireFunction>::NAME,
            "build_signer"
        );
        assert_eq!(<sign::SignFn as WireFunction>::NAME, "sign");
        assert_eq!(
            <on_unauthorized::OnUnauthorizedFn as WireFunction>::NAME,
            "on_unauthorized"
        );
        assert_eq!(<observe::ObserveFn as WireFunction>::NAME, "observe");

        assert_v1::<route::RouteFn>();
        assert_v1::<shape::ShapeFn>();
        assert_v1::<normalize_error::NormalizeErrorFn>();
        assert_v1::<build_signer::BuildSignerFn>();
        assert_v1::<sign::SignFn>();
        assert_v1::<on_unauthorized::OnUnauthorizedFn>();
        assert_v1::<observe::ObserveFn>();
    }

    #[test]
    fn dry_run_round_trip() {
        round_trip(<route::RouteFn as WireFunction>::dry_run_request());
        round_trip(<route::RouteFn as WireFunction>::dry_run_response());
        round_trip(<shape::ShapeFn as WireFunction>::dry_run_request());
        round_trip(<shape::ShapeFn as WireFunction>::dry_run_response());
        round_trip(<normalize_error::NormalizeErrorFn as WireFunction>::dry_run_request());
        round_trip(<normalize_error::NormalizeErrorFn as WireFunction>::dry_run_response());
        round_trip(<build_signer::BuildSignerFn as WireFunction>::dry_run_request());
        round_trip(<build_signer::BuildSignerFn as WireFunction>::dry_run_response());
        round_trip(<sign::SignFn as WireFunction>::dry_run_request());
        round_trip(<sign::SignFn as WireFunction>::dry_run_response());
        round_trip(<on_unauthorized::OnUnauthorizedFn as WireFunction>::dry_run_request());
        round_trip(<on_unauthorized::OnUnauthorizedFn as WireFunction>::dry_run_response());
        round_trip(<observe::ObserveFn as WireFunction>::dry_run_request());
        round_trip(<observe::ObserveFn as WireFunction>::dry_run_response());
    }

    #[test]
    fn all_wire_types_have_dry_run_sample() {
        round_trip(HeaderWire::dry_run_sample());
        round_trip(Principal::dry_run_sample());
        round_trip(RequestWire::dry_run_sample());
        round_trip(RateLimitObservationWire::dry_run_sample());
        round_trip(SubscriptionQuotaCandidateSnapshotWire::dry_run_sample());
        round_trip(CandidateWire::dry_run_sample());
        round_trip(UpstreamWire::dry_run_sample());
        round_trip(DialectBinding::dry_run_sample());
        round_trip(ShapedRequestWire::dry_run_sample());
        round_trip(UpstreamErrorCategory::dry_run_sample());
        round_trip(UpstreamErrorWire::dry_run_sample());
        round_trip(ObserveEventWire::dry_run_sample());
        round_trip(route::RouteRequest::dry_run_sample());
        round_trip(route::RouteResponse::dry_run_sample());
        round_trip(shape::ShapeRequest::dry_run_sample());
        round_trip(shape::ShapeResponse::dry_run_sample());
        round_trip(normalize_error::NormalizeErrorRequest::dry_run_sample());
        round_trip(normalize_error::NormalizeErrorResponse::dry_run_sample());
        round_trip(build_signer::BuildSignerRequest::dry_run_sample());
        round_trip(build_signer::BuildSignerResponse::dry_run_sample());
        round_trip(sign::SignRequest::dry_run_sample());
        round_trip(sign::SignResponse::dry_run_sample());
        round_trip(on_unauthorized::OnUnauthorizedRequest::dry_run_sample());
        round_trip(on_unauthorized::OnUnauthorizedResponse::dry_run_sample());
        round_trip(observe::ObserveRequest::dry_run_sample());
        round_trip(observe::ObserveResponse::dry_run_sample());
    }

    #[test]
    fn sign_build_signer_are_fail_request() {
        assert_eq!(
            <sign::SignFn as WireFunction>::FALLBACK,
            FallbackPolicy::FailRequest
        );
        assert_eq!(
            <build_signer::BuildSignerFn as WireFunction>::FALLBACK,
            FallbackPolicy::FailRequest
        );
    }

    #[test]
    fn shape_is_fail_request() {
        assert_eq!(
            <shape::ShapeFn as WireFunction>::FALLBACK,
            FallbackPolicy::FailRequest
        );
    }

    #[test]
    fn observe_is_silent_skip() {
        assert_eq!(
            <observe::ObserveFn as WireFunction>::FALLBACK,
            FallbackPolicy::SilentSkip
        );
    }

    #[test]
    fn route_is_use_default() {
        assert_eq!(
            <route::RouteFn as WireFunction>::FALLBACK,
            FallbackPolicy::UseDefault
        );
    }

    #[test]
    fn normalize_on_unauthorized_pass_through() {
        assert_eq!(
            <normalize_error::NormalizeErrorFn as WireFunction>::FALLBACK,
            FallbackPolicy::PassThrough
        );
        assert_eq!(
            <on_unauthorized::OnUnauthorizedFn as WireFunction>::FALLBACK,
            FallbackPolicy::PassThrough
        );
    }

    #[test]
    fn deny_unknown_fields_on_wire_structs() {
        let route = r#"{"request_id":"dry","headers":[],"method":"POST","path":"/v1/messages","query":null,"body_base64":"","principal":{"id":"p","kind":"api_key","claims":{}},"candidates":[],"extra":true}"#;
        assert!(serde_json::from_str::<route::RouteRequest>(route).is_err());

        let header = r#"{"name":"x-test","value_base64":"","extra":true}"#;
        assert!(serde_json::from_str::<HeaderWire>(header).is_err());

        let observe = r#"{"events":[],"extra":true}"#;
        assert!(serde_json::from_str::<observe::ObserveRequest>(observe).is_err());
    }
}
