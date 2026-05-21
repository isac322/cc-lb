use bytes::Bytes;
use cc_lb_dialect_anthropic::passthrough_sse_bytes;

#[test]
fn unknown_sse_events_are_preserved_byte_for_byte() {
    let input = Bytes::from_static(
        br#": ping

event: vendor_unknown
id: opaque-42
data: {"opaque":true,"n":1}

"#,
    );

    let output = passthrough_sse_bytes(input.clone());

    assert_eq!(output, input);
}
