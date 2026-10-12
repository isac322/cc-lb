//! Anthropic workspace id format.

const PREFIX: &str = "wrkspc_";
const MAX_BODY_LEN: usize = 120;

/// True when `id` matches `^wrkspc_[A-Za-z0-9]{1,120}$`.
pub fn validate_workspace_id(id: &str) -> bool {
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
        assert!(validate_workspace_id("wrkspc_01AbC"));
        assert!(validate_workspace_id(&format!(
            "wrkspc_{}",
            "a".repeat(120)
        )));
        for bad in [
            "",
            "wrkspc_",
            "WRKSPC_abc",
            "wrkspc_ab-c",
            "wrkspc_ab c",
            " wrkspc_abc",
            "ws_abc",
            "wrkspc_é",
        ] {
            assert!(!validate_workspace_id(bad), "{bad:?}");
        }
        assert!(!validate_workspace_id(&format!(
            "wrkspc_{}",
            "a".repeat(121)
        )));
    }
}
