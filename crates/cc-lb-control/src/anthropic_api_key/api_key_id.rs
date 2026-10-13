//! Anthropic API-key id format (the `id` field of the org Admin API's
//! `v1/organizations/api_keys` items).

const PREFIX: &str = "apikey_";
const MAX_BODY_LEN: usize = 120;

/// True when `id` matches `^apikey_[A-Za-z0-9]{1,120}$`.
pub fn validate_api_key_id(id: &str) -> bool {
    id.strip_prefix(PREFIX).is_some_and(|body| {
        (1..=MAX_BODY_LEN).contains(&body.len())
            && body.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_the_documented_shape() {
        assert!(validate_api_key_id("apikey_01AbC"));
        assert!(validate_api_key_id(&format!("apikey_{}", "a".repeat(120))));
        for bad in [
            "",
            "apikey_",
            "APIKEY_abc",
            "apikey_ab-c",
            "apikey_ab c",
            " apikey_abc",
            "key_abc",
            "apikey_é",
        ] {
            assert!(!validate_api_key_id(bad), "{bad:?}");
        }
        assert!(!validate_api_key_id(&format!("apikey_{}", "a".repeat(121))));
    }
}
