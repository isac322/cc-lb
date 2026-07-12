use std::{cell::RefCell, mem::align_of};

use rkyv::{
    Portable, Serialize,
    api::high::{HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    rancor::Error as RkyvError,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

pub mod filter;
pub mod observe;
pub mod shape;

pub use filter::WasmtimeFilterPlugin;
pub use observe::WasmtimeObservabilityHookPlugin;
pub use shape::WasmtimeUpstreamDialect;

const MAX_RETAINED_SCRATCH_CAPACITY: usize = 1024 * 1024;

thread_local! {
    static INPUT_SERIALIZATION_SCRATCH: RefCell<AlignedVec<16>> =
        RefCell::new(AlignedVec::new());
    static ARCHIVED_COPY_SCRATCH: RefCell<AlignedVec<16>> =
        RefCell::new(AlignedVec::new());
}

pub(super) fn serialize_with_input_scratch<T, R>(
    value: &T,
    with_bytes: impl for<'a> FnOnce(&'a [u8]) -> R,
) -> Result<R, RkyvError>
where
    T: for<'a> Serialize<HighSerializer<AlignedVec<16>, ArenaHandle<'a>, RkyvError>>,
{
    INPUT_SERIALIZATION_SCRATCH.with(|slot| {
        let mut slot = slot.borrow_mut();
        let writer = std::mem::replace(&mut *slot, AlignedVec::new());
        let mut bytes = match rkyv::api::high::to_bytes_in::<_, RkyvError>(value, writer) {
            Ok(bytes) => bytes,
            Err(error) => return Err(error),
        };
        let result = with_bytes(&bytes);
        recycle_scratch(&mut bytes);
        *slot = bytes;
        Ok(result)
    })
}

pub(super) fn access_archived_scoped_or_copy<T, R>(
    guest_bytes: &[u8],
    with_archived: impl for<'a> FnOnce(&'a T) -> R,
) -> Result<R, RkyvError>
where
    T: Portable + for<'a> CheckBytes<HighValidator<'a, RkyvError>>,
{
    const {
        assert!(
            align_of::<T>() <= AlignedVec::<16>::ALIGNMENT,
            "archived wire root alignment exceeds the aligned fallback",
        );
    }

    if guest_bytes.as_ptr() as usize % align_of::<T>() == 0 {
        if let Ok(archived) = rkyv::access::<T, RkyvError>(guest_bytes) {
            return Ok(with_archived(archived));
        }
    }

    ARCHIVED_COPY_SCRATCH.with(|slot| {
        let mut bytes = slot.borrow_mut();
        bytes.clear();
        bytes.extend_from_slice(guest_bytes);
        let result = rkyv::access::<T, RkyvError>(&bytes).map(with_archived);
        recycle_scratch(&mut bytes);
        result
    })
}

fn recycle_scratch(bytes: &mut AlignedVec<16>) {
    bytes.clear();
    if bytes.capacity() > MAX_RETAINED_SCRATCH_CAPACITY {
        *bytes = AlignedVec::new();
    }
}

#[cfg(test)]
mod tests {
    use std::mem::align_of;

    use cc_lb_plugin_wire::{ArchivedFilterResponse, FilterResponse, PerCandidateReason};
    use rkyv::{rancor::Error as RkyvError, util::AlignedVec};

    use super::access_archived_scoped_or_copy;

    #[test]
    fn access_archived_uses_aligned_guest_bytes_and_copies_misaligned_bytes() {
        // Given
        let response = FilterResponse {
            results: Box::new([PerCandidateReason {
                upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
                decision: Box::from("accept"),
                reason: Box::from("aligned-or-copy"),
            }]),
        };
        let aligned = rkyv::to_bytes::<RkyvError>(&response).expect("encode");
        let aligned_start = aligned.as_ptr() as usize;
        let aligned_end = aligned_start + aligned.len();

        // When
        let aligned_result =
            access_archived_scoped_or_copy::<ArchivedFilterResponse, _>(&aligned, |archived| {
                let archived_address = std::ptr::from_ref(archived).cast::<u8>() as usize;
                let reason: &str = &archived.results[0].reason;
                (
                    reason.to_owned(),
                    (aligned_start..aligned_end).contains(&archived_address),
                )
            })
            .expect("aligned access");

        let mut storage = AlignedVec::<16>::with_capacity(aligned.len() + 1);
        storage.push(0);
        storage.extend_from_slice(&aligned);
        let misaligned = &storage[1..];
        assert_ne!(
            misaligned.as_ptr() as usize % align_of::<ArchivedFilterResponse>(),
            0,
            "fixture must use an actually misaligned host pointer",
        );
        let misaligned_start = misaligned.as_ptr() as usize;
        let misaligned_end = misaligned_start + misaligned.len();
        let misaligned_result =
            access_archived_scoped_or_copy::<ArchivedFilterResponse, _>(misaligned, |archived| {
                let archived_address = std::ptr::from_ref(archived).cast::<u8>() as usize;
                let reason: &str = &archived.results[0].reason;
                (
                    reason.to_owned(),
                    !(misaligned_start..misaligned_end).contains(&archived_address),
                )
            })
            .expect("misaligned fallback access");

        // Then
        assert_eq!(aligned_result, ("aligned-or-copy".to_owned(), true));
        assert_eq!(misaligned_result, ("aligned-or-copy".to_owned(), true));
    }
}
