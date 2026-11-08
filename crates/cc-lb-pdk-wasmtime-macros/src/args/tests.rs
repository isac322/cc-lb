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
    assert_eq!(args.provider, Some(super::ProviderFamily::Openai));
}

#[test]
fn provider_qualified_handler_accepts_wire_two() {
    let args = syn::parse2::<HandlerArgs>(quote::quote!(
        filter,
        provider = "openai",
        wire = 2,
        description = "OpenAI V2 filter",
        usage = "Test only"
    ))
    .expect("provider-qualified wire 2 filter parses");

    assert_eq!(args.provider, Some(super::ProviderFamily::Openai));
    assert_eq!(args.wire_version, 2);
}

#[test]
fn legacy_handler_rejects_wire_two() {
    let result = syn::parse2::<HandlerArgs>(quote::quote!(
        filter,
        wire = 2,
        description = "Unsupported legacy filter wire",
        usage = "Test only"
    ));
    let Err(error) = result else {
        panic!("legacy wire 2 must remain unsupported")
    };

    assert!(error.to_string().contains("provider-qualified"));
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
fn provider_qualified_handler_parses() {
    let args = syn::parse2::<HandlerArgs>(quote::quote!(
        shape,
        provider = "openai",
        wire = 1,
        description = "OpenAI shape",
        usage = "Test only"
    ))
    .expect("provider-qualified handler parses");

    assert_eq!(args.provider, Some(super::ProviderFamily::Openai));
}

#[test]
fn legacy_handler_has_no_provider_suffix() {
    let args = syn::parse2::<HandlerArgs>(quote::quote!(
        filter,
        wire = 1,
        description = "Legacy filter",
        usage = "Test only"
    ))
    .expect("legacy handler parses");
    assert_eq!(args.provider, None);
    assert_eq!(args.kind.export_name(args.provider), "cc_lb_filter");
}

#[test]
fn all_handler_exports_match_the_provider_contract() {
    use super::{HandlerKind, ProviderFamily};
    for kind in [
        HandlerKind::Filter,
        HandlerKind::Shape,
        HandlerKind::TransformResponse,
        HandlerKind::TransformSseEvent,
    ] {
        for provider in [
            ProviderFamily::Anthropic,
            ProviderFamily::Openai,
            ProviderFamily::Gemini,
        ] {
            assert_eq!(
                kind.export_name(Some(provider)),
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
        wire = 1,
        description = "Unsupported provider",
        usage = "Test only"
    ));
    let Err(error) = result else {
        panic!("unknown provider must be rejected")
    };
    assert!(error.to_string().contains("anthropic"));
}
