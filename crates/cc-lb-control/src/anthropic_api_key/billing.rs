//! Classification of a terminal provider response as a billing stop.

use serde::{Deserialize, Serialize};

/// Why a provider stopped serving an upstream for billing reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingStopKind {
    /// Anthropic 402: payment problem.
    BillingError,
    /// Anthropic 400 `invalid_request_error` mentioning the credit balance.
    CreditsExhausted,
    /// Anthropic 400 for a self-set spend limit.
    SpendLimit,
    /// Anthropic 429 `rate_limit_error` for the tier's monthly spend cap.
    TierSpendCap,
    OpenaiCreditBalanceExhausted,
    OpenaiOrganizationSpendLimitExceeded,
    OpenaiProjectSpendLimitExceeded,
    OpenaiOrganizationUsageLimitExceeded,
    GeminiPaymentRequired,
    GeminiFailedPrecondition,
}

impl BillingStopKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BillingError => "billing_error",
            Self::CreditsExhausted => "credits_exhausted",
            Self::SpendLimit => "spend_limit",
            Self::TierSpendCap => "tier_spend_cap",
            Self::OpenaiCreditBalanceExhausted => "openai_credit_balance_exhausted",
            Self::OpenaiOrganizationSpendLimitExceeded => {
                "openai_organization_spend_limit_exceeded"
            }
            Self::OpenaiProjectSpendLimitExceeded => "openai_project_spend_limit_exceeded",
            Self::OpenaiOrganizationUsageLimitExceeded => {
                "openai_organization_usage_limit_exceeded"
            }
            Self::GeminiPaymentRequired => "gemini_payment_required",
            Self::GeminiFailedPrecondition => "gemini_failed_precondition",
        }
    }
}

/// Maps a proxied request's final status and provider error fields to a
/// billing stop. Anthropic matching remains message-based for compatibility;
/// OpenAI and Gemini use provider-defined error tokens.
pub fn classify_billing_stop(
    provider: cc_lb_storage_api::Provider,
    status: u16,
    error_type: Option<&str>,
    error_code: Option<&str>,
    message: Option<&str>,
) -> Option<BillingStopKind> {
    match provider {
        cc_lb_storage_api::Provider::Anthropic => classify_anthropic(status, error_type, message),
        cc_lb_storage_api::Provider::Openai => {
            if status != 429 || error_type != Some("insufficient_quota") {
                return None;
            }
            match error_code {
                Some("credit_balance_exhausted") => {
                    Some(BillingStopKind::OpenaiCreditBalanceExhausted)
                }
                Some("organization_spend_limit_exceeded") => {
                    Some(BillingStopKind::OpenaiOrganizationSpendLimitExceeded)
                }
                Some("project_spend_limit_exceeded") => {
                    Some(BillingStopKind::OpenaiProjectSpendLimitExceeded)
                }
                Some("organization_usage_limit_exceeded") => {
                    Some(BillingStopKind::OpenaiOrganizationUsageLimitExceeded)
                }
                _ => None,
            }
        }
        cc_lb_storage_api::Provider::Gemini => {
            let is_payment_required = error_type
                .is_some_and(|token| token.eq_ignore_ascii_case("payment_required"))
                || error_code.is_some_and(|token| token.eq_ignore_ascii_case("payment_required"));
            let is_failed_precondition = error_type
                .is_some_and(|token| token.eq_ignore_ascii_case("failed_precondition"))
                || error_code
                    .is_some_and(|token| token.eq_ignore_ascii_case("failed_precondition"));
            match status {
                402 if is_payment_required => Some(BillingStopKind::GeminiPaymentRequired),
                400 if is_failed_precondition => Some(BillingStopKind::GeminiFailedPrecondition),
                _ => None,
            }
        }
    }
}

fn classify_anthropic(
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
            classify_billing_stop(
                cc_lb_storage_api::Provider::Anthropic,
                402,
                None,
                None,
                None,
            ),
            Some(BillingStopKind::BillingError)
        );
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Anthropic,
                400,
                Some("invalid_request_error"),
                None,
                Some("Your credit balance is too low to access the Anthropic API."),
            ),
            Some(BillingStopKind::CreditsExhausted)
        );
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Anthropic,
                400,
                Some("invalid_request_error"),
                None,
                Some("You have reached your specified API usage limits."),
            ),
            Some(BillingStopKind::SpendLimit)
        );
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Anthropic,
                429,
                Some("rate_limit_error"),
                None,
                Some("You have reached your API usage limits for this month."),
            ),
            Some(BillingStopKind::TierSpendCap)
        );
    }

    #[test]
    fn classifies_openai_documented_codes() {
        let cases = [
            (
                "credit_balance_exhausted",
                BillingStopKind::OpenaiCreditBalanceExhausted,
            ),
            (
                "organization_spend_limit_exceeded",
                BillingStopKind::OpenaiOrganizationSpendLimitExceeded,
            ),
            (
                "project_spend_limit_exceeded",
                BillingStopKind::OpenaiProjectSpendLimitExceeded,
            ),
            (
                "organization_usage_limit_exceeded",
                BillingStopKind::OpenaiOrganizationUsageLimitExceeded,
            ),
        ];
        for (code, expected) in cases {
            assert_eq!(
                classify_billing_stop(
                    cc_lb_storage_api::Provider::Openai,
                    429,
                    Some("insufficient_quota"),
                    Some(code),
                    Some("provider message"),
                ),
                Some(expected),
                "{code}"
            );
        }
    }

    #[test]
    fn classifies_gemini_billing_tokens_case_insensitively() {
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Gemini,
                402,
                Some("PAYMENT_REQUIRED"),
                None,
                None,
            ),
            Some(BillingStopKind::GeminiPaymentRequired)
        );
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Gemini,
                400,
                Some("FAILED_PRECONDITION"),
                None,
                None,
            ),
            Some(BillingStopKind::GeminiFailedPrecondition)
        );
    }

    #[test]
    fn ignores_unrelated_failures() {
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Anthropic,
                400,
                Some("invalid_request_error"),
                None,
                Some("bad model"),
            ),
            None
        );
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Openai,
                429,
                Some("rate_limit_error"),
                Some("rate_limit_exceeded"),
                Some("slow down"),
            ),
            None
        );
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Openai,
                429,
                Some("insufficient_quota"),
                None,
                Some("quota"),
            ),
            None
        );
        for token in ["rate_limit_exceeded", "quota_exceeded", "too_many_requests"] {
            assert_eq!(
                classify_billing_stop(
                    cc_lb_storage_api::Provider::Gemini,
                    429,
                    Some(token),
                    None,
                    Some("quota"),
                ),
                None,
                "{token}"
            );
        }
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Anthropic,
                500,
                None,
                None,
                Some("credit balance"),
            ),
            None
        );
        assert_eq!(
            classify_billing_stop(
                cc_lb_storage_api::Provider::Anthropic,
                200,
                None,
                None,
                None,
            ),
            None
        );
    }
}
