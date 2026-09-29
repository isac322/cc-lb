pub use cc_lb_storage_api::{KeyStatus, Limit, LimitKind};

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, de::DeserializeOwned};

    fn bincode_roundtrip<T>(value: &T) -> T
    where
        T: Serialize + DeserializeOwned,
    {
        let encoded = bincode::serde::encode_to_vec(value, bincode::config::standard())
            .expect("encode value");
        let (decoded, consumed) =
            bincode::serde::decode_from_slice::<T, _>(&encoded, bincode::config::standard())
                .expect("decode value");
        assert_eq!(consumed, encoded.len());
        decoded
    }

    #[test]
    fn key_status_bincode_roundtrip() {
        let value = KeyStatus::Revoked;

        let decoded: KeyStatus = bincode_roundtrip(&value);

        assert_eq!(decoded, value);
    }

    #[test]
    fn is_subset_of_accepts_same_kind_window_and_lower_cap() {
        let parent = Limit {
            kind: LimitKind::OutputTokens,
            window_secs: 300,
            cap_micros: 1_000,
        };
        let child = Limit {
            kind: LimitKind::OutputTokens,
            window_secs: 300,
            cap_micros: 750,
        };

        assert!(child.is_subset_of(&parent));
    }

    #[test]
    fn is_subset_of_rejects_larger_cap() {
        let parent = Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        };
        let child = Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 200,
        };

        assert!(!child.is_subset_of(&parent));
    }

    #[test]
    fn is_subset_of_rejects_different_kind() {
        let parent = Limit {
            kind: LimitKind::Concurrent,
            window_secs: 60,
            cap_micros: 1,
        };
        let child = Limit {
            kind: LimitKind::CostUsd,
            window_secs: 60,
            cap_micros: 1,
        };

        assert!(!child.is_subset_of(&parent));
    }
}
