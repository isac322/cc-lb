//! Plugin Development Kit for wasmtime-based cc-lb plugins.
//!
//! Re-exports `#[cc_lb_plugin]` / `#[handler]` from
//! `cc-lb-pdk-wasmtime-macros` plus the host↔guest wire types from
//! `cc-lb-plugin-wire`, and provides the guest-side allocator / dispatch
//! plumbing the macros call into.
//!
//! The public surface for plugin authors is:
//!
//! ```ignore
//! use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler, types::*};
//!
//! #[cc_lb_plugin(
//!     name = "cache-aware",
//!     version = "0.1.0",
//!     description = "Routes requests by cache affinity",
//!     usage = "Attach to a filter chain and configure principal claims.",
//! )]
//! mod cache_aware {
//!     use super::*;
//!
//!     #[handler(
//!         filter,
//!         wire = 1,
//!         description = "Filters upstream candidates",
//!         usage = "Returns accept/reject decisions for each candidate.",
//!     )]
//!     fn filter(req: FilterRequest) -> FilterResponse {
//!         /* ... */
//!     }
//! }
//! ```
//!
//! Everything inside [`__private`] is implementation detail that the
//! generated code refers to by absolute path; nothing under that module is
//! covered by SemVer.
//!
//! See `docs/rfc/0001-plugin-runtime-vnext.md`.
#![no_std]
#![deny(unsafe_code)]

extern crate alloc;
#[cfg(all(
    not(target_arch = "wasm32"),
    target_os = "macos",
    target_pointer_width = "64"
))]
extern crate std;

#[cfg(target_arch = "wasm32")]
mod wasm32_glue {
    //! wasm32-unknown-unknown has no std → no global allocator, no
    //! panic handler. The PDK ships dlmalloc + a trap-on-panic
    //! handler so plugin authors never wire allocator boilerplate
    //! themselves. Both items are gated to the wasm32 target so they
    //! never collide with std on host-side builds.

    #[global_allocator]
    static GLOBAL: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

    #[panic_handler]
    fn panic(_info: &core::panic::PanicInfo) -> ! {
        // `unreachable` compiles down to the wasm `unreachable`
        // instruction — the host catches it as a trap, discards the
        // Store, and the next call rebuilds a fresh worker per
        // RFC §실행 모델.
        core::arch::wasm32::unreachable()
    }
}

pub use cc_lb_pdk_wasmtime_macros::{WireSchema, cc_lb_plugin, handler, plugin};

/// Wire types shared with the host.
pub use cc_lb_plugin_wire as types;

#[doc(hidden)]
pub mod __private {
    //! Implementation detail. Called by macro-generated `cc_lb_alloc`,
    //! `cc_lb_free`, `cc_lb_filter` exports. Not stable API.

    use core::alloc::Layout;
    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "linux",
        target_pointer_width = "64"
    ))]
    use core::ffi::c_void;
    use core::slice;
    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "macos",
        target_pointer_width = "64"
    ))]
    use core::sync::atomic::{AtomicU32, Ordering};
    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "macos",
        target_pointer_width = "64"
    ))]
    use std::{
        collections::BTreeMap,
        sync::{LazyLock, Mutex},
    };

    use cc_lb_plugin_wire::{
        ArchivedFilterRequest, ArchivedObserveEvent, ArchivedShapeRequest,
        ArchivedTransformResponseRequest, ArchivedTransformSseEventRequest, FilterRequest,
        FilterResponse, ObserveEvent, ShapeRequest, ShapeResponse, TransformResponseRequest,
        TransformResponseResult, TransformSseEventRequest, TransformSseEventResult, pack_ret,
    };
    use rkyv::rancor::Error;

    /// Default archive alignment for the rkyv 0.8 root types in
    /// [`cc_lb_plugin_wire`]. The host always passes this value to
    /// `cc_lb_alloc` / `cc_lb_free`.
    pub const DEFAULT_ALIGN: u32 = 16;

    /// Allocate `size` bytes with `align` alignment. Returns `0` on
    /// invalid layout or allocation failure (the host treats `0` as a
    /// trap signal).
    #[allow(unsafe_code)]
    pub fn alloc_bytes(size: u32, align: u32) -> u32 {
        let Ok(layout) = Layout::from_size_align(size as usize, align as usize) else {
            return 0;
        };
        if layout.size() == 0 {
            return 0;
        }
        alloc_with_layout(layout)
    }

    /// Free a previously-allocated buffer. `ptr` MUST have come from
    /// [`alloc_bytes`] and `size`/`align` MUST match the original call —
    /// the host honours this contract.
    #[allow(unsafe_code)]
    pub fn free_bytes(ptr: u32, size: u32, align: u32) {
        if ptr == 0 {
            return;
        }
        let Ok(layout) = Layout::from_size_align(size as usize, align as usize) else {
            return;
        };
        if layout.size() == 0 {
            return;
        }
        free_with_layout(ptr, layout);
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "macos",
        target_pointer_width = "64"
    ))]
    struct NativeAllocation {
        ptr: usize,
        layout: Layout,
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "macos",
        target_pointer_width = "64"
    ))]
    static NATIVE_ALLOCATIONS: LazyLock<Mutex<BTreeMap<u32, NativeAllocation>>> =
        LazyLock::new(|| Mutex::new(BTreeMap::new()));

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "macos",
        target_pointer_width = "64"
    ))]
    static NEXT_NATIVE_HANDLE: AtomicU32 = AtomicU32::new(1);

    #[cfg(not(all(
        not(target_arch = "wasm32"),
        any(target_os = "linux", target_os = "macos"),
        target_pointer_width = "64"
    )))]
    #[allow(unsafe_code)]
    fn alloc_with_layout(layout: Layout) -> u32 {
        // SAFETY: layout has positive size and a valid alignment.
        let ptr = unsafe { alloc::alloc::alloc(layout) };
        if ptr.is_null() {
            return 0;
        }
        if ptr as usize > u32::MAX as usize {
            // Native test builds can have 64-bit pointers, while the guest ABI
            // is intentionally u32. Never truncate a live native pointer.
            unsafe { alloc::alloc::dealloc(ptr, layout) };
            return 0;
        }
        ptr as u32
    }

    #[cfg(not(all(
        not(target_arch = "wasm32"),
        any(target_os = "linux", target_os = "macos"),
        target_pointer_width = "64"
    )))]
    #[allow(unsafe_code)]
    fn free_with_layout(ptr: u32, layout: Layout) {
        // SAFETY: ptr came from `alloc_bytes` with the same layout per the
        // host↔guest contract enforced by `cc-lb-runtime-wasmtime::cache`.
        unsafe { alloc::alloc::dealloc(ptr as *mut u8, layout) };
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "linux",
        target_pointer_width = "64"
    ))]
    fn alloc_with_layout(layout: Layout) -> u32 {
        let ptr = low_mmap(layout.size());
        if ptr.is_null() || ptr as usize > u32::MAX as usize {
            0
        } else {
            ptr as u32
        }
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "macos",
        target_pointer_width = "64"
    ))]
    #[allow(unsafe_code)]
    fn alloc_with_layout(layout: Layout) -> u32 {
        let ptr = unsafe { alloc::alloc::alloc(layout) };
        if ptr.is_null() {
            return 0;
        }
        let handle = NEXT_NATIVE_HANDLE.fetch_add(1, Ordering::Relaxed);
        assert_ne!(handle, 0, "native guest allocation handle overflow");
        let previous = NATIVE_ALLOCATIONS
            .lock()
            .expect("native guest allocation registry poisoned")
            .insert(
                handle,
                NativeAllocation {
                    ptr: ptr as usize,
                    layout,
                },
            );
        assert!(previous.is_none(), "native guest allocation handle reused");
        handle
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "linux",
        target_pointer_width = "64"
    ))]
    fn free_with_layout(ptr: u32, layout: Layout) {
        low_munmap(ptr, layout.size());
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "macos",
        target_pointer_width = "64"
    ))]
    #[allow(unsafe_code)]
    fn free_with_layout(ptr: u32, layout: Layout) {
        let allocation = NATIVE_ALLOCATIONS
            .lock()
            .expect("native guest allocation registry poisoned")
            .remove(&ptr)
            .expect("unknown native guest allocation handle");
        assert_eq!(allocation.layout.size(), layout.size());
        assert_eq!(allocation.layout.align(), layout.align());
        unsafe { alloc::alloc::dealloc(allocation.ptr as *mut u8, allocation.layout) };
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "linux",
        target_pointer_width = "64"
    ))]
    #[allow(unsafe_code)]
    fn low_mmap(size: usize) -> *mut u8 {
        const PROT_READ: i32 = 0x1;
        const PROT_WRITE: i32 = 0x2;
        const MAP_PRIVATE: i32 = 0x02;
        const MAP_ANONYMOUS: i32 = 0x20;
        const MAP_FIXED_NOREPLACE: i32 = 0x100000;
        const MAP_FAILED: *mut c_void = !0usize as *mut c_void;

        unsafe extern "C" {
            fn mmap(
                addr: *mut c_void,
                length: usize,
                prot: i32,
                flags: i32,
                fd: i32,
                offset: isize,
            ) -> *mut c_void;
        }

        let mut addr = 0x1_0000usize;
        while addr < 0x8000_0000usize {
            let ptr = unsafe {
                mmap(
                    addr as *mut c_void,
                    size,
                    PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE,
                    -1,
                    0,
                )
            };
            if ptr != MAP_FAILED {
                return ptr.cast();
            }
            addr += 0x1_0000;
        }
        core::ptr::null_mut()
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "linux",
        target_pointer_width = "64"
    ))]
    #[allow(unsafe_code)]
    fn low_munmap(ptr: u32, size: usize) {
        unsafe extern "C" {
            fn munmap(addr: *mut c_void, length: usize) -> i32;
        }
        let _ = unsafe { munmap(ptr as usize as *mut c_void, size) };
    }

    #[cfg(not(all(
        not(target_arch = "wasm32"),
        target_os = "macos",
        target_pointer_width = "64"
    )))]
    fn guest_ptr(ptr: u32) -> *mut u8 {
        ptr as usize as *mut u8
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        target_os = "macos",
        target_pointer_width = "64"
    ))]
    pub(crate) fn guest_ptr(ptr: u32) -> *mut u8 {
        NATIVE_ALLOCATIONS
            .lock()
            .expect("native guest allocation registry poisoned")
            .get(&ptr)
            .unwrap_or_else(|| panic!("unknown native guest allocation handle {ptr}"))
            .ptr as *mut u8
    }

    /// Owned-mode filter dispatch — invoked from the macro-generated
    /// `cc_lb_filter` export when the handler signature takes
    /// `FilterRequest` (default).
    ///
    /// Flow: read input bytes → `rkyv::access` → `rkyv::deserialize` →
    /// free input → run handler → `rkyv::to_bytes` → allocate output →
    /// copy → return packed `(out_ptr, out_len)`.
    ///
    /// Any rkyv failure or OOM panics, which the host's `TypedFunc::call`
    /// surfaces as a trap; per RFC §실행 모델 the host then discards the
    /// `Store` and the next call rebuilds a fresh worker.
    #[allow(unsafe_code)]
    pub fn run_filter<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(FilterRequest) -> FilterResponse,
    {
        // SAFETY: host promises `(in_ptr, in_len)` covers an initialised
        // buffer obtained from `cc_lb_alloc` immediately before this call.
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };

        let archived: &ArchivedFilterRequest =
            rkyv::access::<ArchivedFilterRequest, Error>(in_bytes)
                .expect("rkyv::access(FilterRequest) failed — host/guest schema mismatch?");

        let owned: FilterRequest = rkyv::deserialize::<FilterRequest, Error>(archived)
            .expect("rkyv::deserialize(FilterRequest) failed");

        // Free input as soon as we own a deserialised copy. Keeps guest
        // peak memory close to one wire-buffer instead of two.
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);

        let response = handler(owned);

        let bytes =
            rkyv::to_bytes::<Error>(&response).expect("rkyv::to_bytes(FilterResponse) failed");

        let out_len = bytes.len() as u32;
        let out_ptr = alloc_bytes(out_len, DEFAULT_ALIGN);
        assert!(
            out_ptr != 0,
            "cc_lb_alloc returned null for FilterResponse output buffer"
        );

        // SAFETY: out_ptr points to `out_len` bytes we just allocated.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), guest_ptr(out_ptr), out_len as usize);
        }

        pack_ret(out_ptr, out_len)
    }

    /// Zero-copy view-mode filter dispatch — invoked from the
    /// macro-generated `cc_lb_filter` export when the handler is
    /// declared with `#[handler(filter, wire = 1, ..., view)]` and takes
    /// `&ArchivedFilterRequest`.
    ///
    /// Skips `rkyv::deserialize` entirely. Field access happens
    /// straight against the archived bytes in guest memory, so the
    /// handler must run BEFORE the input buffer is freed (view types
    /// borrow from `in_bytes`).
    #[allow(unsafe_code)]
    pub fn run_filter_view<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(&ArchivedFilterRequest) -> FilterResponse,
    {
        // SAFETY: same as run_filter.
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };

        let archived: &ArchivedFilterRequest =
            rkyv::access::<ArchivedFilterRequest, Error>(in_bytes)
                .expect("rkyv::access(FilterRequest) failed — host/guest schema mismatch?");

        let response = handler(archived);

        // Now safe to free input — handler is done borrowing from it.
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);

        let bytes =
            rkyv::to_bytes::<Error>(&response).expect("rkyv::to_bytes(FilterResponse) failed");

        let out_len = bytes.len() as u32;
        let out_ptr = alloc_bytes(out_len, DEFAULT_ALIGN);
        assert!(
            out_ptr != 0,
            "cc_lb_alloc returned null for FilterResponse output buffer"
        );

        // SAFETY: out_ptr points to `out_len` bytes we just allocated.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), guest_ptr(out_ptr), out_len as usize);
        }

        pack_ret(out_ptr, out_len)
    }

    /// Owned-mode shape dispatch — invoked from `cc_lb_shape` when the
    /// handler takes `ShapeRequest`.
    #[allow(unsafe_code)]
    pub fn run_shape<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(ShapeRequest) -> ShapeResponse,
    {
        // SAFETY: host promises `(in_ptr, in_len)` covers an initialised buffer.
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };
        let archived: &ArchivedShapeRequest = rkyv::access::<ArchivedShapeRequest, Error>(in_bytes)
            .expect("rkyv::access(ShapeRequest) failed");
        let owned: ShapeRequest = rkyv::deserialize::<ShapeRequest, Error>(archived)
            .expect("rkyv::deserialize(ShapeRequest) failed");
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
        let response = handler(owned);
        encode_and_pack::<ShapeResponse>(&response, "ShapeResponse")
    }

    /// View-mode shape dispatch.
    #[allow(unsafe_code)]
    pub fn run_shape_view<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(&ArchivedShapeRequest) -> ShapeResponse,
    {
        // SAFETY: same as run_shape.
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };
        let archived: &ArchivedShapeRequest = rkyv::access::<ArchivedShapeRequest, Error>(in_bytes)
            .expect("rkyv::access(ShapeRequest) failed");
        let response = handler(archived);
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
        encode_and_pack::<ShapeResponse>(&response, "ShapeResponse")
    }

    /// Observe is side-effect-only — host hands an event, plugin
    /// returns nothing. The packed return is `(0, 0)`.
    #[allow(unsafe_code)]
    pub fn run_observe<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(ObserveEvent),
    {
        // SAFETY: same as run_filter.
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };
        let archived: &ArchivedObserveEvent = rkyv::access::<ArchivedObserveEvent, Error>(in_bytes)
            .expect("rkyv::access(ObserveEvent) failed");
        let owned: ObserveEvent = rkyv::deserialize::<ObserveEvent, Error>(archived)
            .expect("rkyv::deserialize(ObserveEvent) failed");
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
        handler(owned);
        pack_ret(0, 0)
    }

    #[allow(unsafe_code)]
    pub fn run_observe_view<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(&ArchivedObserveEvent),
    {
        // SAFETY: same as run_filter.
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };
        let archived: &ArchivedObserveEvent = rkyv::access::<ArchivedObserveEvent, Error>(in_bytes)
            .expect("rkyv::access(ObserveEvent) failed");
        handler(archived);
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
        pack_ret(0, 0)
    }

    #[allow(unsafe_code)]
    pub fn run_transform_response<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(TransformResponseRequest) -> TransformResponseResult,
    {
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };
        let archived: &ArchivedTransformResponseRequest =
            match rkyv::access::<ArchivedTransformResponseRequest, Error>(in_bytes) {
                Ok(value) => value,
                Err(_) => panic!("rkyv::access(TransformResponseRequest) failed"),
            };
        let owned: TransformResponseRequest =
            match rkyv::deserialize::<TransformResponseRequest, Error>(archived) {
                Ok(value) => value,
                Err(_) => panic!("rkyv::deserialize(TransformResponseRequest) failed"),
            };
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
        let response = handler(owned);
        encode_and_pack::<TransformResponseResult>(&response, "TransformResponseResult")
    }

    #[allow(unsafe_code)]
    pub fn run_transform_response_view<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(&ArchivedTransformResponseRequest) -> TransformResponseResult,
    {
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };
        let archived: &ArchivedTransformResponseRequest =
            match rkyv::access::<ArchivedTransformResponseRequest, Error>(in_bytes) {
                Ok(value) => value,
                Err(_) => panic!("rkyv::access(TransformResponseRequest) failed"),
            };
        let response = handler(archived);
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
        encode_and_pack::<TransformResponseResult>(&response, "TransformResponseResult")
    }

    #[allow(unsafe_code)]
    pub fn run_transform_sse_event<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(TransformSseEventRequest) -> TransformSseEventResult,
    {
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };
        let archived: &ArchivedTransformSseEventRequest =
            match rkyv::access::<ArchivedTransformSseEventRequest, Error>(in_bytes) {
                Ok(value) => value,
                Err(_) => panic!("rkyv::access(TransformSseEventRequest) failed"),
            };
        let owned: TransformSseEventRequest =
            match rkyv::deserialize::<TransformSseEventRequest, Error>(archived) {
                Ok(value) => value,
                Err(_) => panic!("rkyv::deserialize(TransformSseEventRequest) failed"),
            };
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
        let response = handler(owned);
        encode_and_pack::<TransformSseEventResult>(&response, "TransformSseEventResult")
    }

    #[allow(unsafe_code)]
    pub fn run_transform_sse_event_view<F>(in_ptr: u32, in_len: u32, handler: F) -> u64
    where
        F: FnOnce(&ArchivedTransformSseEventRequest) -> TransformSseEventResult,
    {
        let in_bytes =
            unsafe { slice::from_raw_parts(guest_ptr(in_ptr) as *const u8, in_len as usize) };
        let archived: &ArchivedTransformSseEventRequest =
            match rkyv::access::<ArchivedTransformSseEventRequest, Error>(in_bytes) {
                Ok(value) => value,
                Err(_) => panic!("rkyv::access(TransformSseEventRequest) failed"),
            };
        let response = handler(archived);
        free_bytes(in_ptr, in_len, DEFAULT_ALIGN);
        encode_and_pack::<TransformSseEventResult>(&response, "TransformSseEventResult")
    }

    /// Serialize an rkyv-Serializable response, allocate a guest
    /// buffer, copy, and return a packed `(out_ptr, out_len)`. Shared
    /// tail for every owned/view dispatch helper.
    #[allow(unsafe_code)]
    fn encode_and_pack<T>(value: &T, type_name: &'static str) -> u64
    where
        T: for<'a> rkyv::Serialize<
                rkyv::api::high::HighSerializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    Error,
                >,
            >,
    {
        let bytes = rkyv::to_bytes::<Error>(value)
            .unwrap_or_else(|_| panic!("rkyv::to_bytes({type_name}) failed"));
        let out_len = bytes.len() as u32;
        let out_ptr = alloc_bytes(out_len, DEFAULT_ALIGN);
        assert!(out_ptr != 0, "cc_lb_alloc returned null for {type_name}");
        // SAFETY: out_ptr points to `out_len` bytes we just allocated.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), guest_ptr(out_ptr), out_len as usize);
        }
        pack_ret(out_ptr, out_len)
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;

    use super::__private::{self, DEFAULT_ALIGN, alloc_bytes, free_bytes};
    use cc_lb_plugin_wire::{
        CachePricingSummary, FilterRequest, FilterResponse, Header, Principal,
        TransformResponseRequest, TransformResponseResult, TransformSseEventRequest,
        TransformSseEventResult, Upstream,
    };
    use rkyv::rancor::Error as RkyvError;

    fn filter_request() -> FilterRequest {
        FilterRequest {
            request_id: Box::from("req-pdk-service-tier"),
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

    #[allow(unsafe_code)]
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
        // SAFETY: alloc_bytes returned a live allocation of exactly len bytes,
        // and bytes.len() equals len, so this non-overlapping copy stays in bounds.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), __private::guest_ptr(ptr), bytes.len());
        }
        (ptr, len)
    }

    fn free_response(packed: u64) {
        let (out_ptr, out_len) = cc_lb_plugin_wire::unpack_ret(packed);
        assert_ne!(out_ptr, 0);
        assert_ne!(out_len, 0);
        free_bytes(out_ptr, out_len, DEFAULT_ALIGN);
    }

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
            event: cc_lb_plugin_wire::SseEvent {
                event: Box::from("message_start"),
                data: Box::from(&b"{}"[..]),
            },
        }
    }

    #[test]
    fn run_filter_dispatches_owned_service_tier() {
        let (ptr, len) = copy_to_guest(&filter_request());
        let packed = __private::run_filter(ptr, len, |request| {
            assert_eq!(request.service_tier.as_deref(), Some("priority"));
            FilterResponse {
                results: Box::new([]),
            }
        });

        free_response(packed);
    }

    #[test]
    fn run_filter_dispatches_archived_service_tier() {
        let (ptr, len) = copy_to_guest(&filter_request());
        let packed = __private::run_filter_view(ptr, len, |request| {
            let tier: Option<&str> = request.service_tier.as_ref().map(|value| &**value);
            assert_eq!(tier, Some("priority"));
            FilterResponse {
                results: Box::new([]),
            }
        });

        free_response(packed);
    }

    #[test]
    fn run_transform_response_dispatches_owned_request() {
        let (ptr, len) = copy_to_guest(&response_request());
        let packed = __private::run_transform_response(ptr, len, |request| {
            assert_eq!(&*request.request_id, "req-pdk");
            TransformResponseResult::Unchanged
        });
        free_response(packed);
    }

    #[test]
    fn run_transform_sse_event_dispatches_owned_request() {
        let (ptr, len) = copy_to_guest(&sse_request());
        let packed = __private::run_transform_sse_event(ptr, len, |request| {
            assert_eq!(&*request.event.event, "message_start");
            TransformSseEventResult::Unchanged
        });
        free_response(packed);
    }
}
