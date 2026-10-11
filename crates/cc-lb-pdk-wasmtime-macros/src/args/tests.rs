use super::HandlerArgs;

#[test]
fn filter_handler_rejects_wire_three() {
    let result = syn::parse2::<HandlerArgs>(quote::quote!(
        filter,
        wire = 3,
        description = "Unsupported filter wire",
        usage = "Test only"
    ));
    let Err(error) = result else {
        panic!("filter wire 3 must remain unsupported")
    };

    assert!(error.to_string().contains("require 1"));
}

#[test]
fn filter_v2_supports_owned_and_view_dispatch_only() {
    use super::HandlerKind;
    assert!(HandlerKind::Filter.supports_wire_version(2));
    assert_eq!(
        HandlerKind::Filter.dispatch_helper(2, false),
        Some("run_filter_v2")
    );
    assert_eq!(
        HandlerKind::Filter.dispatch_helper(2, true),
        Some("run_filter_v2_view")
    );
    for kind in [
        HandlerKind::Shape,
        HandlerKind::TransformResponse,
        HandlerKind::TransformSseEvent,
    ] {
        assert!(!kind.supports_wire_version(2));
        assert!(kind.dispatch_helper(2, false).is_none());
    }
}
