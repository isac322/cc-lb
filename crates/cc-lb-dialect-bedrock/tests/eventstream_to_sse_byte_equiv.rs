use aws_eventstream_codec::encode_message;
use cc_lb_dialect_bedrock::convert_eventstream_to_sse_bytes;

#[test]
fn event_frames_convert_to_expected_sse_bytes() {
    let first =
        br#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}"#;
    let second = br#"{"type":"vendor_unknown","opaque":true}"#;
    let mut input = encode_message("chunk", first);
    input.extend_from_slice(&encode_message("chunk", second));

    let output = convert_eventstream_to_sse_bytes(&input).expect("event-stream converts");
    let expected = b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\nevent: vendor_unknown\ndata: {\"type\":\"vendor_unknown\",\"opaque\":true}\n\n";

    assert_eq!(&output[..], expected);
}
