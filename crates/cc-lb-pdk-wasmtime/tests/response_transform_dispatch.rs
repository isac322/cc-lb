use cc_lb_pdk_wasmtime::__private::{DEFAULT_ALIGN, alloc_bytes, free_bytes};
use cc_lb_pdk_wasmtime::types::{
    Header, Principal, TransformResponseRequest, TransformResponseResult, TransformSseEventRequest,
    TransformSseEventResult, Upstream,
};
use rkyv::rancor::Error as RkyvError;

fn response_request() -> TransformResponseRequest {
    TransformResponseRequest {
        request_id: Box::from("req-pdk"),
        principal: Principal {
            id: Box::from("tenant"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        upstream: Upstream::AnthropicDirect { base_url: None },
        request_method: Box::from("POST"),
        request_path: Box::from("/v1/messages"),
        canonical_model_id: Box::from("claude-test"),
        response_status: 200,
        response_headers: Box::new([Header {
            name: Box::from("content-type"),
            value: Box::from(&b"application/json"[..]),
        }]),
        body: Box::from(&b"{}"[..]),
    }
}

fn sse_request() -> TransformSseEventRequest {
    TransformSseEventRequest {
        request_id: Box::from("req-pdk-sse"),
        principal: response_request().principal,
        upstream: Upstream::AnthropicDirect { base_url: None },
        request_method: Box::from("POST"),
        request_path: Box::from("/v1/messages"),
        canonical_model_id: Box::from("claude-test"),
        response_status: 200,
        response_headers: Box::new([]),
        event: cc_lb_pdk_wasmtime::types::SseEvent {
            event: Box::from("message_start"),
            data: Box::from(&b"{}"[..]),
        },
    }
}

fn copy_to_guest<T>(value: &T) -> (u32, u32)
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                RkyvError,
            >,
        >,
{
    let bytes = rkyv::to_bytes::<RkyvError>(value).expect("encode input");
    let len = u32::try_from(bytes.len()).expect("fixture fits u32");
    let ptr = alloc_bytes(len, DEFAULT_ALIGN);
    assert_ne!(ptr, 0);
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
    }
    (ptr, len)
}

#[test]
fn run_transform_response_dispatches_owned_request() {
    let (ptr, len) = copy_to_guest(&response_request());
    let packed = cc_lb_pdk_wasmtime::__private::run_transform_response(ptr, len, |request| {
        assert_eq!(&*request.request_id, "req-pdk");
        TransformResponseResult::Unchanged
    });
    let (out_ptr, out_len) = cc_lb_pdk_wasmtime::types::unpack_ret(packed);
    assert_ne!(out_ptr, 0);
    assert_ne!(out_len, 0);
    free_bytes(out_ptr, out_len, DEFAULT_ALIGN);
}

#[test]
fn run_transform_sse_event_dispatches_owned_request() {
    let (ptr, len) = copy_to_guest(&sse_request());
    let packed = cc_lb_pdk_wasmtime::__private::run_transform_sse_event(ptr, len, |request| {
        assert_eq!(&*request.event.event, "message_start");
        TransformSseEventResult::Unchanged
    });
    let (out_ptr, out_len) = cc_lb_pdk_wasmtime::types::unpack_ret(packed);
    assert_ne!(out_ptr, 0);
    assert_ne!(out_len, 0);
    free_bytes(out_ptr, out_len, DEFAULT_ALIGN);
}
