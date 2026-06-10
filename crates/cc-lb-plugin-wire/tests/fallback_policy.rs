use cc_lb_plugin_wire::v1::build_signer::BuildSignerFn;
use cc_lb_plugin_wire::v1::normalize_error::NormalizeErrorFn;
use cc_lb_plugin_wire::v1::observe::ObserveFn;
use cc_lb_plugin_wire::v1::on_unauthorized::OnUnauthorizedFn;
use cc_lb_plugin_wire::v1::shape::ShapeFn;
use cc_lb_plugin_wire::v1::sign::SignFn;
use cc_lb_plugin_wire::wire_function::{FallbackPolicy, WireFunction};

const fn policy_tag(policy: FallbackPolicy) -> u8 {
    match policy {
        FallbackPolicy::FailRequest => 0,
        FallbackPolicy::SilentSkip => 1,
        FallbackPolicy::UseDefault => 2,
        FallbackPolicy::PassThrough => 3,
    }
}

macro_rules! const_assert {
    ($actual:expr, $expected:expr $(,)?) => {
        const _: () = assert!(policy_tag($actual) == policy_tag($expected));
    };
}

const_assert!(
    <SignFn as WireFunction>::FALLBACK,
    FallbackPolicy::FailRequest,
);
const_assert!(
    <BuildSignerFn as WireFunction>::FALLBACK,
    FallbackPolicy::FailRequest,
);
const_assert!(
    <ShapeFn as WireFunction>::FALLBACK,
    FallbackPolicy::FailRequest,
);
const_assert!(
    <ObserveFn as WireFunction>::FALLBACK,
    FallbackPolicy::SilentSkip,
);
const_assert!(
    <NormalizeErrorFn as WireFunction>::FALLBACK,
    FallbackPolicy::PassThrough,
);
const_assert!(
    <OnUnauthorizedFn as WireFunction>::FALLBACK,
    FallbackPolicy::PassThrough,
);

#[test]
fn fallback_policy_matrix_matches_protocol_contract() {
    let actual = [
        (
            <SignFn as WireFunction>::NAME,
            <SignFn as WireFunction>::FALLBACK,
        ),
        (
            <BuildSignerFn as WireFunction>::NAME,
            <BuildSignerFn as WireFunction>::FALLBACK,
        ),
        (
            <ShapeFn as WireFunction>::NAME,
            <ShapeFn as WireFunction>::FALLBACK,
        ),
        (
            <ObserveFn as WireFunction>::NAME,
            <ObserveFn as WireFunction>::FALLBACK,
        ),
        (
            <NormalizeErrorFn as WireFunction>::NAME,
            <NormalizeErrorFn as WireFunction>::FALLBACK,
        ),
        (
            <OnUnauthorizedFn as WireFunction>::NAME,
            <OnUnauthorizedFn as WireFunction>::FALLBACK,
        ),
    ];

    assert_eq!(
        actual,
        [
            ("sign", FallbackPolicy::FailRequest),
            ("build_signer", FallbackPolicy::FailRequest),
            ("shape", FallbackPolicy::FailRequest),
            ("observe", FallbackPolicy::SilentSkip),
            ("normalize_error", FallbackPolicy::PassThrough),
            ("on_unauthorized", FallbackPolicy::PassThrough),
        ]
    );
}
