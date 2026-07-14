use core::slice;

use cc_lb_plugin_wire::v2::{ArchivedFilterRequest, FilterRequest, FilterResponse};
use rkyv::rancor::Error;

use crate::__private::{DEFAULT_ALIGN, alloc_bytes, free_bytes};
use cc_lb_plugin_wire::pack_ret;

#[allow(unsafe_code)]
pub fn run_filter_v2<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
where
    F: FnOnce(FilterRequest) -> FilterResponse,
{
    // SAFETY: Categories 3, 6, 10, and 11. The host passes a live, aligned in_len-byte
    // allocation returned by cc_lb_alloc immediately before invoking the guest export.
    let in_bytes = unsafe { slice::from_raw_parts(in_ptr as *const u8, in_len as usize) };
    let archived = rkyv::access::<ArchivedFilterRequest, Error>(in_bytes)
        .expect("rkyv::access(FilterRequest V2) failed - host/guest schema mismatch?");
    let owned = rkyv::deserialize::<FilterRequest, Error>(archived)
        .expect("rkyv::deserialize(FilterRequest V2) failed");
    free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
    encode_response(&handler(owned))
}

#[allow(unsafe_code)]
pub fn run_filter_v2_view<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
where
    F: FnOnce(&ArchivedFilterRequest) -> FilterResponse,
{
    // SAFETY: Categories 3, 6, 10, and 11. The host passes a live, aligned in_len-byte
    // allocation returned by cc_lb_alloc immediately before invoking the guest export.
    let in_bytes = unsafe { slice::from_raw_parts(in_ptr as *const u8, in_len as usize) };
    let archived = rkyv::access::<ArchivedFilterRequest, Error>(in_bytes)
        .expect("rkyv::access(FilterRequest V2) failed - host/guest schema mismatch?");
    let response = handler(archived);
    free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
    encode_response(&response)
}

#[allow(unsafe_code)]
fn encode_response(response: &FilterResponse) -> u64 {
    let bytes = rkyv::to_bytes::<Error>(response).expect("rkyv::to_bytes(FilterResponse) failed");
    let out_len = bytes.len() as u32;
    let out_ptr = alloc_bytes(out_len, DEFAULT_ALIGN);
    assert!(
        out_ptr != 0,
        "cc_lb_alloc returned null for FilterResponse output buffer"
    );
    // SAFETY: Categories 3 and 10. alloc_bytes returned a live out_len-byte allocation,
    // and bytes.len() equals out_len, so the non-overlapping copy is fully in bounds.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr as *mut u8, out_len as usize);
    }
    pack_ret(out_ptr, out_len)
}
