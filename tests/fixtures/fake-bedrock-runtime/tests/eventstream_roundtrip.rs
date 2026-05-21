use aws_eventstream_codec::{decode_messages, encode_message};

#[test]
fn encode_decode_roundtrip_preserves_headers_and_payload() {
    let payload = br#"{"type":"content_block_delta","index":7}"#;
    let frame = encode_message("chunk", payload);
    let decoded = decode_messages(&frame).expect("frame decodes");

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].header_str(":event-type"), Some("chunk"));
    assert_eq!(
        decoded[0].header_str(":content-type"),
        Some("application/json")
    );
    assert_eq!(decoded[0].header_str(":message-type"), Some("event"));
    assert_eq!(decoded[0].payload, payload);
}
