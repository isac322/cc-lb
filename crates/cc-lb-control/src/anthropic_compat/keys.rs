use std::time::Duration;

pub const CLAUDE_CODE_STABLE_VERSION_KEY: &str = "claude_code_stable_version";
pub const CLAUDE_CODE_STABLE_VERSION_FALLBACK: &str = "2.1.150";

pub const CLAUDE_CODE_LATEST_VERSION_KEY: &str = "claude_code_latest_version";
// Last verified published latest release; the daily refresh replaces it once
// the compat cron lands the fetched value.
pub const CLAUDE_CODE_LATEST_VERSION_FALLBACK: &str = "2.1.282";

pub static COMPATIBILITY_KEYS: &[CompatibilityKey] = &[
    CompatibilityKey {
        name: CLAUDE_CODE_STABLE_VERSION_KEY,
        refresh_interval: Duration::from_secs(86_400),
        fallback: CLAUDE_CODE_STABLE_VERSION_FALLBACK,
        fetcher: CompatFetcher::ClaudeCodeStableVersion,
    },
    CompatibilityKey {
        name: CLAUDE_CODE_LATEST_VERSION_KEY,
        refresh_interval: Duration::from_secs(86_400),
        fallback: CLAUDE_CODE_LATEST_VERSION_FALLBACK,
        fetcher: CompatFetcher::ClaudeCodeLatestVersion,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompatibilityKey {
    pub name: &'static str,
    pub refresh_interval: Duration,
    pub fallback: &'static str,
    pub fetcher: CompatFetcher,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompatFetcher {
    ClaudeCodeStableVersion,
    ClaudeCodeLatestVersion,
}

pub fn claude_code_user_agent(version: &str) -> String {
    if version.is_empty() {
        String::new()
    } else {
        format!("claude-cli/{version} (external, cli)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_code_user_agent_formats_non_empty_version() {
        assert_eq!(
            claude_code_user_agent("2.1.150"),
            "claude-cli/2.1.150 (external, cli)"
        );
    }

    #[test]
    fn claude_code_user_agent_empty_version_returns_empty() {
        assert_eq!(claude_code_user_agent(""), "");
    }
}
