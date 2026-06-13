use cc_lb_plugin_wire::guest::run_string_export;
use cc_lb_plugin_wire::handshake::{HandshakeAccept, HandshakeOffer};
use cc_lb_plugin_wire::serde_json;
use cc_lb_plugin_wire::v3::filter::FilterFn;
use cc_lb_plugin_wire::wire_function::WireFunction;
use std::collections::{BTreeMap, BTreeSet};
use extism_pdk::{FnResult, plugin_fn};

#[plugin_fn]
pub fn filter(input: String) -> FnResult<String> {
    Ok(input)
}

#[unsafe(no_mangle)]
pub extern "C" fn cc_lb_handshake() -> i32 {
    run_string_export(handle_handshake_export)
}

fn handle_handshake_export(input: String) -> Result<String, String> {
    let offer: HandshakeOffer = serde_json::from_str(&input).map_err(|error| error.to_string())?;
    offer.validate().map_err(|error| error.to_string())?;

    let mut plugin_supported = BTreeMap::new();
    plugin_supported.insert(
        FilterFn::NAME.to_owned(),
        FilterFn::SUPPORTED_VERSIONS.to_vec(),
    );

    let mut chosen_versions = BTreeMap::new();
    if let Some(chosen) = offer
        .function_versions
        .get(FilterFn::NAME)
        .and_then(|offered| {
            offered
                .iter()
                .filter(|version| FilterFn::SUPPORTED_VERSIONS.contains(version))
                .max()
                .copied()
        })
    {
        chosen_versions.insert(FilterFn::NAME.to_owned(), chosen);
    }

    let accept = HandshakeAccept {
        handshake_schema_version: offer.handshake_schema_version,
        envelope_version: offer.envelope_version,
        chosen_versions,
        plugin_supported,
        implemented_functions: BTreeSet::from([FilterFn::NAME.to_owned()]),
        required_capabilities: BTreeSet::new(),
    };
    accept
        .validate_against_offer(&offer)
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&accept).map_err(|error| error.to_string())
}
