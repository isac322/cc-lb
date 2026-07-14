use cc_lb_pdk_wasmtime::__private::{DEFAULT_ALIGN, alloc_bytes, free_bytes};
use cc_lb_pdk_wasmtime::types::v2::{
    CachePricingSummary, FilterRequest, FilterResponse, Principal,
};
use rkyv::rancor::Error as RkyvError;

fn request() -> FilterRequest {
    FilterRequest {
        request_id: Box::from("req-pdk-v2"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from("claude-test"),
        cache_pricing: CachePricingSummary {
            status: Box::from("known"),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        },
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([]),
        body: Box::from(&b"{}"[..]),
        principal: Principal {
            id: Box::from("tenant"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        candidates: Box::new([]),
    }
}

fn copy_to_guest(request: &FilterRequest) -> (u32, u32) {
    let bytes = rkyv::to_bytes::<RkyvError>(request).expect("encode input");
    let len = u32::try_from(bytes.len()).expect("fixture fits u32");
    let ptr = alloc_bytes(len, DEFAULT_ALIGN);
    assert_ne!(ptr, 0);
    // SAFETY: Categories 3 and 10. alloc_bytes returned a live allocation of exactly len bytes,
    // and bytes.len() equals len, so this non-overlapping copy stays within both allocations.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
    }
    (ptr, len)
}

fn free_response(packed: u64) {
    let (out_ptr, out_len) = cc_lb_pdk_wasmtime::types::unpack_ret(packed);
    assert_ne!(out_ptr, 0);
    assert_ne!(out_len, 0);
    free_bytes(out_ptr, out_len, DEFAULT_ALIGN);
}

#[test]
fn run_filter_v2_dispatches_owned_service_tier() {
    let (ptr, len) = copy_to_guest(&request());
    let packed = cc_lb_pdk_wasmtime::__private::run_filter_v2(ptr, len, |request| {
        assert_eq!(request.service_tier.as_deref(), Some("priority"));
        FilterResponse {
            results: Box::new([]),
        }
    });

    free_response(packed);
}

#[test]
fn run_filter_v2_dispatches_archived_service_tier() {
    let (ptr, len) = copy_to_guest(&request());
    let packed = cc_lb_pdk_wasmtime::__private::run_filter_v2_view(ptr, len, |request| {
        let tier: Option<&str> = request.service_tier.as_ref().map(|value| &**value);
        assert_eq!(tier, Some("priority"));
        FilterResponse {
            results: Box::new([]),
        }
    });

    free_response(packed);
}
