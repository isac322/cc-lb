mod fetchers;
mod keys;

pub use fetchers::{CompatFetchError, CompatFetchOutcome, run_compat_fetcher};
pub use keys::{
    CLAUDE_CODE_LATEST_VERSION_FALLBACK, CLAUDE_CODE_LATEST_VERSION_KEY,
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, COMPATIBILITY_KEYS,
    CompatFetcher, CompatibilityKey, claude_code_user_agent,
};
