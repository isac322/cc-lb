//! Classification of a terminal provider response as a billing stop.

use serde::{Deserialize, Serialize};

/// Why Anthropic stopped serving an upstream for billing reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingStopKind {
    /// 402: payment problem.
    BillingError,
    /// 400 `invalid_request_error` mentioning the credit balance.
    CreditsExhausted,
    /// 400 for a self-set spend limit.
    SpendLimit,
    /// 429 `rate_limit_error` for the tier's monthly spend cap.
    TierSpendCap,
}

impl BillingStopKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BillingError => "billing_error",
            Self::CreditsExhausted => "credits_exhausted",
            Self::SpendLimit => "spend_limit",
            Self::TierSpendCap => "tier_spend_cap",
        }
    }
}

/// Maps a proxied request's final status, provider error type, and provider
/// message to a billing stop. Message matching is ASCII case-insensitive.
pub fn classify_billing_stop(
    status: u16,
    error_type: Option<&str>,
    message: Option<&str>,
) -> Option<BillingStopKind> {
    let message = message.unwrap_or_default();
    match status {
        402 => Some(BillingStopKind::BillingError),
        400 if error_type == Some("invalid_request_error")
            && contains_ignore_ascii_case(message, "credit balance") =>
        {
            Some(BillingStopKind::CreditsExhausted)
        }
        400 if contains_ignore_ascii_case(message, "reached your specified")
            && contains_ignore_ascii_case(message, "API usage limits") =>
        {
            Some(BillingStopKind::SpendLimit)
        }
        429 if error_type == Some("rate_limit_error")
            && contains_ignore_ascii_case(message, "You have reached your API usage limits") =>
        {
            Some(BillingStopKind::TierSpendCap)
        }
        _ => None,
    }
}

fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let needle = needle.as_bytes();
    needle.is_empty()
        || haystack
            .as_bytes()
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_each_billing_stop() {
        assert_eq!(
            classify_billing_stop(402, None, None),
            Some(BillingStopKind::BillingError)
        );
        assert_eq!(
            classify_billing_stop(
                400,
                Some("invalid_request_error"),
                Some("Your credit balance is too low to access the Anthropic API.")
            ),
            Some(BillingStopKind::CreditsExhausted)
        );
        assert_eq!(
            classify_billing_stop(
                400,
                Some("invalid_request_error"),
                Some("You have reached your specified API usage limits.")
            ),
            Some(BillingStopKind::SpendLimit)
        );
        assert_eq!(
            classify_billing_stop(
                429,
                Some("rate_limit_error"),
                Some("You have reached your API usage limits for this month.")
            ),
            Some(BillingStopKind::TierSpendCap)
        );
    }

    #[test]
    fn ignores_unrelated_failures() {
        assert_eq!(
            classify_billing_stop(400, Some("invalid_request_error"), Some("bad model")),
            None
        );
        assert_eq!(
            classify_billing_stop(400, Some("api_error"), Some("credit balance")),
            None
        );
        assert_eq!(
            classify_billing_stop(429, Some("rate_limit_error"), Some("slow down")),
            None
        );
        assert_eq!(
            classify_billing_stop(500, None, Some("credit balance")),
            None
        );
        assert_eq!(classify_billing_stop(200, None, None), None);
    }
}
