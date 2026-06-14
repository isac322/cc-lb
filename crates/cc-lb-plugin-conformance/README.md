# cc-lb-plugin-conformance

This crate provides a conformance testing suite for cc-lb plugins. It allows external plugin authors to verify their compiled WebAssembly plugins against the cc-lb host protocol in-process.

By using this suite, you can ensure that your plugin behaves correctly under the same constraints and environment as the production cc-lb host.

## Supported Plugin Kinds

The conformance suite supports testing the following plugin types:

* **Router Plugins**: Plugins that route requests to different upstreams.
* **Shape Plugins**: Plugins that transform request and response shapes.
* **Observability Plugins**: Plugins that monitor and log request metrics.

Please note that signer plugins are not supported by this conformance suite.

## Cargo.toml Setup

To use this crate in your plugin project, add it as a development dependency in your `Cargo.toml`:

```toml
[package]
name = "my-cc-lb-plugin"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
cc-lb-pdk = "0.1.0"
cc-lb-plugin-wire = "0.1.0"
serde_json = "1"

[dev-dependencies]
cc-lb-plugin-conformance = "0.1.0"
```

## Quick Start Example

Here is a complete example of how to write conformance tests for your plugin. This example assumes you have compiled your plugin to a WebAssembly binary and made it available to your tests.

```rust
use cc_lb_plugin_conformance::{handshake, self_check, dispatch::PluginSession, fixtures};

const WASM: &[u8] = include_bytes!(env!("CARGO_CDYLIB_FILE_MY_CC_LB_PLUGIN"));

#[test]
fn handshake_negotiates_shape_v1() {
    let report = handshake::run(WASM).unwrap();
    assert_eq!(report.chosen_versions["shape"], 1);
    assert_eq!(report.identity.name, "my-plugin");
}

#[test]
fn self_check_clean() {
    let report = self_check::run(WASM).unwrap();
    assert!(matches!(report.status, self_check::SelfCheckStatus::Success));
    assert!(report.failures.is_empty());
}

#[test]
fn shape_round_trip_basic() {
    use cc_lb_plugin_conformance::prelude::*;
    let mut session = PluginSession::new(WASM).unwrap();
    let req = fixtures::shape_request_builder()
        .method("POST")
        .path("/v1/messages")
        .body_base64("eyJ0ZXN0IjoxfQ==")
        .build();
    let outcome = session.dispatch::<cc_lb_plugin_wire::wire_function::ShapeFn>(req).unwrap();
    let cc_lb_plugin_conformance::dispatch::DispatchOutcome::Ok(resp) = outcome else {
        panic!("shape returned Fallback");
    };
    assert_eq!(resp.url, "https://api.anthropic.com/v1/messages");
}
```

## SemVer Note

The error reason strings returned by this crate are not stable. When writing tests, you should match on the enum variants themselves rather than asserting on the exact text of the error messages.
