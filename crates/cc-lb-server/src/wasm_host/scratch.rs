use std::{cell::RefCell, mem::align_of};

use rkyv::{
    Portable, Serialize,
    api::high::{HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    rancor::Error as RkyvError,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

const MAX_RETAINED_SCRATCH_CAPACITY: usize = 1024 * 1024;

thread_local! {
    static INPUT_SERIALIZATION_SCRATCH: RefCell<AlignedVec<16>> =
        RefCell::new(AlignedVec::new());
    static ARCHIVED_COPY_SCRATCH: RefCell<AlignedVec<16>> =
        RefCell::new(AlignedVec::new());
}

pub(crate) fn serialize_with_input_scratch<T, R>(
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

pub(crate) fn access_archived_scoped_or_copy<T, R>(
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

    if (guest_bytes.as_ptr() as usize).is_multiple_of(align_of::<T>())
        && let Ok(archived) = rkyv::access::<T, RkyvError>(guest_bytes)
    {
        return Ok(with_archived(archived));
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
