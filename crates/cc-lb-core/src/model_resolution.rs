pub fn canonical_model_id(requested: &str) -> &str {
    match requested {
        "claude-sonnet-4-5" => "claude-sonnet-4-5-20250929",
        _ => requested,
    }
}

pub fn cache_threshold_tokens(_canonical_model: &str) -> usize {
    1024
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_model_id_collapses_sonnet_45_alias() {
        assert_eq!(
            canonical_model_id("claude-sonnet-4-5"),
            "claude-sonnet-4-5-20250929"
        );
        assert_eq!(
            canonical_model_id("claude-sonnet-4-5-20250929"),
            "claude-sonnet-4-5-20250929"
        );
    }
}
