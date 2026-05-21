use aws_eventstream_codec::encode_message_with_headers;
use cc_lb_dialect_bedrock::convert_eventstream_to_sse_bytes;

#[test]
fn model_stream_exception_becomes_anthropic_error_sse() {
    let payload = br#"{"message":"forced fake model stream error","originalStatusCode":500}"#;
    let input = encode_message_with_headers(
        &[
            (":event-type", "ModelStreamErrorException"),
            (":content-type", "application/json"),
            (":message-type", "exception"),
            (":exception-type", "ModelStreamErrorException"),
        ],
        payload,
    );

    let output = convert_eventstream_to_sse_bytes(&input).expect("exception converts");
    let output_text = String::from_utf8(output.to_vec()).expect("sse is utf8");
    println!("{output_text}");

    assert!(output_text.starts_with("event: error\ndata: "));
    let json_start = output_text.find('{').expect("json starts");
    let json_end = output_text.rfind('}').expect("json ends") + 1;
    let body: serde_json::Value =
        serde_json::from_str(&output_text[json_start..json_end]).expect("json body");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "api_error");
    assert_eq!(body["error"]["message"], "forced fake model stream error");
}
