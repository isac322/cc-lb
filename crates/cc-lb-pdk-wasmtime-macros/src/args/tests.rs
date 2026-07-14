use syn::parse_quote;

use super::{HandlerArgs, HandlerKind};

#[test]
fn filter_handler_accepts_wire_v2() {
    let args: HandlerArgs = parse_quote!(
        filter,
        wire = 2,
        description = "V2 filter",
        usage = "Test only"
    );

    assert_eq!(args.wire_version, 2);
    assert_eq!(
        HandlerKind::Filter.dispatch_helper(2, false),
        Some("run_filter_v2")
    );
    assert!(
        HandlerKind::Filter
            .fingerprint_type(2)
            .expect("filter V2 fingerprint type")
            .to_string()
            .contains("v2")
    );
}

#[test]
fn non_filter_handler_rejects_wire_v2() {
    let result = syn::parse2::<HandlerArgs>(quote::quote!(
        shape,
        wire = 2,
        description = "Unsupported V2 shape",
        usage = "Test only"
    ));
    let Err(error) = result else {
        panic!("shape V2 must remain unsupported")
    };

    assert!(error.to_string().contains("filter"));
}
