use bytes::Bytes;
use cc_lb_dialect_vertex::passthrough_sse_bytes;

#[test]
fn vertex_sse_success_path_is_byte_equivalent_passthrough() {
    let vertex_sse = Bytes::from_static(
        b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: custom\ndata: keep-me\n\n",
    );

    let client_sse = passthrough_sse_bytes(vertex_sse.clone());

    assert_eq!(client_sse, vertex_sse);
}
