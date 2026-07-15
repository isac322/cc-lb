use super::HandlerArgs;

#[test]
fn filter_handler_rejects_wire_two() {
    let result = syn::parse2::<HandlerArgs>(quote::quote!(
        filter,
        wire = 2,
        description = "Unsupported filter wire",
        usage = "Test only"
    ));
    let Err(error) = result else {
        panic!("filter wire 2 must remain unsupported")
    };

    assert!(error.to_string().contains("require 1"));
}
