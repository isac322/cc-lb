use super::HandlerArgs;

#[test]
fn realtime_frame_handler_parses() {
    let args = syn::parse2::<HandlerArgs>(quote::quote!(
        transform_realtime_frame,
        provider = "openai",
        wire = 2,
        description = "OpenAI realtime frame",
        usage = "Test only"
    ))
    .expect("realtime frame handler parses");

    assert_eq!(args.kind, super::HandlerKind::TransformRealtimeFrame);
    assert_eq!(args.provider, super::ProviderFamily::Openai);
}

#[test]
fn filter_handler_accepts_wire_three() {
    let args = syn::parse2::<HandlerArgs>(quote::quote!(
        filter,
        provider = "openai",
        wire = 3,
        description = "OpenAI V3 filter",
        usage = "Test only"
    ))
    .expect("provider-qualified wire 3 filter parses");

    assert_eq!(args.provider, super::ProviderFamily::Openai);
    assert_eq!(args.wire_version, 3);
    assert_eq!(
        args.kind.dispatch_helper(args.wire_version, false),
        Some("run_filter_v3")
    );
    assert_eq!(
        args.kind.dispatch_helper(args.wire_version, true),
        Some("run_filter_v3_view")
    );
}

#[test]
fn filter_handler_rejects_wire_one_and_two() {
    for wire in [1u8, 2] {
        let result = syn::parse2::<HandlerArgs>(quote::quote!(
            filter,
            provider = "openai",
            wire = #wire,
            description = "Legacy filter wire",
            usage = "Test only"
        ));
        assert!(result.is_err(), "filter wire {wire} must be rejected");
    }
}

#[test]
fn handler_requires_provider() {
    let result = syn::parse2::<HandlerArgs>(quote::quote!(
        filter,
        wire = 3,
        description = "Bare handler",
        usage = "Test only"
    ));
    let Err(error) = result else {
        panic!("bare handler without provider must be rejected")
    };
    assert!(error.to_string().contains("provider"));
}

#[test]
fn shape_envelope_gemini_requires_gemini_provider_and_wire_two() {
    let args = syn::parse2::<HandlerArgs>(quote::quote!(
        shape_envelope_gemini,
        provider = "gemini",
        wire = 2,
        description = "Gemini envelope",
        usage = "Test only"
    ))
    .expect("Gemini envelope handler parses");

    assert_eq!(args.kind, super::HandlerKind::ShapeEnvelopeGemini);
    assert_eq!(
        args.kind.export_name(args.provider),
        "cc_lb_shape_envelope_gemini"
    );
    assert_eq!(
        args.kind.metadata_key(args.provider),
        "shape_envelope_gemini"
    );
}

#[test]
fn shape_envelope_gemini_rejects_non_gemini_provider() {
    let result = syn::parse2::<HandlerArgs>(quote::quote!(
        shape_envelope_gemini,
        provider = "openai",
        wire = 2,
        description = "Wrong provider",
        usage = "Test only"
    ));
    assert!(result.is_err());
}

#[test]
fn provider_qualified_shape_handler_rejects_wire_one_and_accepts_wire_two() {
    let err = syn::parse2::<HandlerArgs>(quote::quote!(
        shape,
        provider = "openai",
        wire = 1,
        description = "OpenAI shape V1",
        usage = "Test only"
    ));
    assert!(err.is_err());

    let args = syn::parse2::<HandlerArgs>(quote::quote!(
        shape,
        provider = "openai",
        wire = 2,
        description = "OpenAI shape V2",
        usage = "Test only"
    ))
    .expect("provider-qualified wire 2 shape parses");
    assert_eq!(args.provider, super::ProviderFamily::Openai);
    assert_eq!(args.wire_version, 2);
}

#[test]
fn all_handler_exports_match_the_provider_contract() {
    use super::{HandlerKind, ProviderFamily};
    for kind in [
        HandlerKind::Filter,
        HandlerKind::Shape,
        HandlerKind::TransformResponse,
        HandlerKind::TransformSseEvent,
        HandlerKind::TransformRealtimeFrame,
    ] {
        for provider in [
            ProviderFamily::Anthropic,
            ProviderFamily::Openai,
            ProviderFamily::Gemini,
        ] {
            assert_eq!(
                kind.export_name(provider),
                format!("cc_lb_{}_{}", kind.wire_name(), provider.as_str())
            );
        }
    }
}

#[test]
fn unknown_provider_is_rejected() {
    let result = syn::parse2::<HandlerArgs>(quote::quote!(
        shape,
        provider = "other",
        wire = 2,
        description = "Unsupported provider",
        usage = "Test only"
    ));
    let Err(error) = result else {
        panic!("unknown provider must be rejected")
    };
    assert!(error.to_string().contains("anthropic"));
}
