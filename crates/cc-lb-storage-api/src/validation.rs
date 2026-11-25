use crate::{Limit, LimitKind, StorageError, StorageResult};

/// Maximum limit window supported by the durable rolling-bucket engine (7 days).
pub const MAX_LIMIT_WINDOW_SECS: u64 = 604_800;

pub fn validate_limits(field: &str, limits: &[Limit]) -> StorageResult<()> {
    for (idx, limit) in limits.iter().enumerate() {
        if limit.cap_micros < 0 {
            return Err(StorageError::InvalidInput {
                field: format!("{field}[{idx}].cap_micros"),
                reason: "cap must be non-negative".to_owned(),
            });
        }
        if limit.kind == LimitKind::Concurrent {
            if limit.cap_micros > i64::from(u32::MAX) {
                return Err(StorageError::InvalidInput {
                    field: format!("{field}[{idx}].cap_micros"),
                    reason: format!("concurrent cap must not exceed {}", u32::MAX),
                });
            }
            if limit.window_secs > MAX_LIMIT_WINDOW_SECS {
                return Err(StorageError::InvalidInput {
                    field: format!("{field}[{idx}].window_secs"),
                    reason: format!("limit window must not exceed {MAX_LIMIT_WINDOW_SECS} seconds"),
                });
            }
        } else if !(1..=MAX_LIMIT_WINDOW_SECS).contains(&limit.window_secs) {
            return Err(StorageError::InvalidInput {
                field: format!("{field}[{idx}].window_secs"),
                reason: format!(
                    "limit window must be between 1 and {MAX_LIMIT_WINDOW_SECS} seconds"
                ),
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

    #[test]
    fn test_validate_limits_bounds() {
        assert!(
            validate_limits(
                "default_limits",
                &[
                    Limit {
                        kind: LimitKind::Requests,
                        window_secs: MAX_LIMIT_WINDOW_SECS,
                        cap_micros: 100,
                    },
                    Limit {
                        kind: LimitKind::Concurrent,
                        window_secs: 0,
                        cap_micros: 4,
                    },
                    Limit {
                        kind: LimitKind::Concurrent,
                        window_secs: 60,
                        cap_micros: 4,
                    },
                ],
            )
            .is_ok()
        );

        assert!(matches!(
            validate_limits(
                "default_limits",
                &[Limit {
                    kind: LimitKind::Requests,
                    window_secs: 0,
                    cap_micros: 10,
                }],
            ),
            Err(StorageError::InvalidInput { ref field, .. }) if field == "default_limits[0].window_secs"
        ));
        assert!(matches!(
            validate_limits(
                "default_limits",
                &[Limit {
                    kind: LimitKind::CostUsd,
                    window_secs: MAX_LIMIT_WINDOW_SECS + 1,
                    cap_micros: 100,
                }],
            ),
            Err(StorageError::InvalidInput { ref field, .. }) if field == "default_limits[0].window_secs"
        ));
        assert!(matches!(
            validate_limits(
                "limit_overrides",
                &[Limit {
                    kind: LimitKind::TotalTokens,
                    window_secs: 60,
                    cap_micros: -1,
                }],
            ),
            Err(StorageError::InvalidInput { ref field, .. }) if field == "limit_overrides[0].cap_micros"
        ));
        assert!(matches!(
            validate_limits(
                "default_limits",
                &[Limit {
                    kind: LimitKind::Concurrent,
                    window_secs: 60,
                    cap_micros: i64::from(u32::MAX) + 1,
                }],
            ),
            Err(StorageError::InvalidInput { ref field, .. }) if field == "default_limits[0].cap_micros"
        ));
    }
}
