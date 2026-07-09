use crate::{PluginSlot, StorageError, StorageResult};

pub fn validate_sse_batching_knobs(
    slot: PluginSlot,
    sse_per_event: bool,
    batched_events_per_flush: u32,
    batched_flush_ms: u64,
) -> StorageResult<()> {
    if matches!(
        slot,
        PluginSlot::TransformResponse | PluginSlot::TransformSseEvent
    ) {
        if sse_per_event {
            return Err(StorageError::InvalidInput {
                field: "sse_per_event".to_string(),
                reason: "sse_per_event is not supported for response-transform slots".to_string(),
            });
        }
        if batched_events_per_flush != 1 {
            return Err(StorageError::InvalidInput {
                field: "batched_events_per_flush".to_string(),
                reason: "batched_events_per_flush is not supported for response-transform slots"
                    .to_string(),
            });
        }
        if batched_flush_ms != 100 {
            return Err(StorageError::InvalidInput {
                field: "batched_flush_ms".to_string(),
                reason: "batched_flush_ms is not supported for response-transform slots"
                    .to_string(),
            });
        }
    }
    Ok(())
}

pub fn validate_identifier(name: &str, value: &str) -> StorageResult<()> {
    if value.is_empty() {
        return Err(StorageError::InvalidInput {
            field: name.to_string(),
            reason: "identifier cannot be empty".to_string(),
        });
    }

    if value.contains('\0') {
        return Err(StorageError::InvalidInput {
            field: name.to_string(),
            reason: "identifier cannot contain NUL bytes".to_string(),
        });
    }

    if value.starts_with("system.") {
        return Err(StorageError::InvalidInput {
            field: name.to_string(),
            reason: "identifier cannot use reserved system. prefix".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_accepts_valid() {
        assert!(validate_identifier("field", "valid_id").is_ok());
        assert!(validate_identifier("field", "id-123").is_ok());
        assert!(validate_identifier("field", "id.name").is_ok());
    }

    #[test]
    fn test_validate_identifier_rejects_empty() {
        let err = validate_identifier("field", "").unwrap_err();
        match err {
            StorageError::InvalidInput { field, reason } => {
                assert_eq!(field, "field");
                assert!(reason.contains("empty"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_identifier_rejects_nul_byte() {
        let err = validate_identifier("field", "id\0with_nul").unwrap_err();
        match err {
            StorageError::InvalidInput { field, reason } => {
                assert_eq!(field, "field");
                assert!(reason.contains("NUL"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_identifier_rejects_nul_byte_at_end() {
        let err = validate_identifier("field", "id\0").unwrap_err();
        match err {
            StorageError::InvalidInput { .. } => {}
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_identifier_rejects_reserved_system_prefix() {
        let err = validate_identifier("field", "system.reserved").unwrap_err();
        match err {
            StorageError::InvalidInput { field, reason } => {
                assert_eq!(field, "field");
                assert!(reason.contains("system"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }
}
