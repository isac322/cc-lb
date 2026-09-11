pub mod filter;
mod filter_request;
pub mod observe;
mod scratch;
pub mod shape;

pub use filter::WasmtimeFilterPlugin;
pub use observe::WasmtimeObservabilityHookPlugin;
pub(super) use scratch::{access_archived_scoped_or_copy, serialize_with_input_scratch};
pub use shape::WasmtimeUpstreamDialect;

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

    #[test]
    fn access_archived_rejects_invalid_bytes() {
        // Given: invalid bytes (all zeros)
        let invalid_bytes = vec![0xAA; 32];

        // When: we try to access them as ArchivedFilterResponse
        let result =
            access_archived_scoped_or_copy::<ArchivedFilterResponse, _>(&invalid_bytes, |_| {});

        // Then: it must return an error
        assert!(result.is_err());
    }
}
