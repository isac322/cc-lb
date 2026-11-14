use serde::{Deserialize, Serialize};

use crate::SubscriptionQuotaCredits;

/// Physical source of a Codex credit observation. WebSocket meters use the
/// response-header stream, independently of scheduled usage API observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreditSource {
    /// A response header or provider WebSocket meter.
    Header,
    /// An account usage API observation.
    Api,
}

/// Comparable order of usage polls issued by one running publisher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreditRequestOrder {
    /// Per-process identity; sequences from different runs never compare.
    pub publisher: uuid::Uuid,
    /// Existing publication-fence sequence captured before the provider call.
    pub sequence: u64,
}

/// Provenance of one explicitly reported credit field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreditObservation {
    /// Local capture time of this field, not of a later partial update.
    pub observed_at_unix_millis: u64,
    /// Physical provider observation source.
    pub source: CreditSource,
    /// Optional order from the existing usage-poll publication fence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_order: Option<CreditRequestOrder>,
}

impl CreditObservation {
    /// Whether this field's own evidence is inside the freshness boundary.
    pub fn is_fresh(self, now_unix_millis: u64, max_staleness_secs: u64) -> bool {
        now_unix_millis.saturating_sub(self.observed_at_unix_millis)
            <= max_staleness_secs.saturating_mul(1_000)
    }

    /// Compare request order where provable, otherwise observation time.
    pub fn recency(self, other: Self) -> std::cmp::Ordering {
        if self.source == other.source
            && let (Some(left), Some(right)) = (self.request_order, other.request_order)
            && left.publisher == right.publisher
        {
            return left.sequence.cmp(&right.sequence);
        }
        self.observed_at_unix_millis
            .cmp(&other.observed_at_unix_millis)
    }
}

/// Independent observation clocks for the four credit facts used by operators
/// and routing. These clocks are excluded from checkpoint semantic identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreditObservations {
    /// Availability flag evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_credits: Option<CreditObservation>,
    /// Unlimited-credit flag evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unlimited: Option<CreditObservation>,
    /// Credit-limit restriction evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_reached: Option<CreditObservation>,
    /// Balance evidence, independent of flag-only updates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<CreditObservation>,
}

impl CreditObservations {
    /// Whether a legacy wire representation can omit the provenance object.
    pub fn is_empty(&self) -> bool {
        self.has_credits.is_none()
            && self.unlimited.is_none()
            && self.limit_reached.is_none()
            && self.balance.is_none()
    }
}

/// Accepts plain decimal strings without trimming or losing their precision.
/// Scientific notation and incomplete decimal fractions are not observations.
pub fn is_credit_balance(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    let start = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let mut digits = 0;
    let mut fraction = false;
    for &byte in &bytes[start..] {
        if byte.is_ascii_digit() {
            digits += 1;
        } else if byte == b'.' && !fraction && digits > 0 {
            fraction = true;
            digits = 0;
        } else {
            return false;
        }
    }
    digits > 0
}

impl SubscriptionQuotaCredits {
    /// Attach legacy row provenance to explicit fields only. Missing and invalid
    /// fields never refresh retained evidence.
    pub fn stamp_observations(&mut self, observed_at_unix_millis: u64, source: CreditSource) {
        let observation = CreditObservation {
            observed_at_unix_millis,
            source,
            request_order: None,
        };
        if self.has_credits.is_some() {
            self.observations.has_credits.get_or_insert(observation);
        }
        if self.unlimited.is_some() {
            self.observations.unlimited.get_or_insert(observation);
        }
        if self.limit_reached.is_some() {
            self.observations.limit_reached.get_or_insert(observation);
        }
        if self.balance.as_deref().is_some_and(is_credit_balance) {
            self.observations.balance.get_or_insert(observation);
        } else {
            self.balance = None;
            self.observations.balance = None;
        }
    }

    /// Attach the caller's already-captured usage-poll publication fence.
    pub fn stamp_request_order(&mut self, order: CreditRequestOrder) {
        for observation in [
            &mut self.observations.has_credits,
            &mut self.observations.unlimited,
            &mut self.observations.limit_reached,
            &mut self.observations.balance,
        ]
        .into_iter()
        .flatten()
        {
            observation.request_order = Some(order);
        }
    }

    /// Merge explicit fields independently. Callers stamp legacy values first.
    /// Equal-time contradictory flags keep the restrictive value unless a
    /// same-source publication fence proves the incoming request is newer.
    pub fn merge_observations(&mut self, incoming: &Self) {
        merge_flag(
            &mut self.has_credits,
            &mut self.observations.has_credits,
            incoming.has_credits,
            incoming.observations.has_credits,
            false,
        );
        merge_flag(
            &mut self.unlimited,
            &mut self.observations.unlimited,
            incoming.unlimited,
            incoming.observations.unlimited,
            false,
        );
        merge_flag(
            &mut self.limit_reached,
            &mut self.observations.limit_reached,
            incoming.limit_reached,
            incoming.observations.limit_reached,
            true,
        );
        if let (Some(value), Some(observation)) =
            (incoming.balance.as_deref(), incoming.observations.balance)
            && is_credit_balance(value)
            && self.observations.balance.is_none_or(|current| {
                observation.recency(current).is_gt()
                    || (observation.recency(current).is_eq()
                        && observation.source >= current.source)
            })
        {
            self.balance = Some(value.to_owned());
            self.observations.balance = Some(observation);
        }
        if incoming.approx_local_messages.is_some() {
            self.approx_local_messages = incoming.approx_local_messages;
        }
        if incoming.approx_cloud_messages.is_some() {
            self.approx_cloud_messages = incoming.approx_cloud_messages;
        }
    }
}

fn merge_flag(
    value: &mut Option<bool>,
    observation: &mut Option<CreditObservation>,
    incoming_value: Option<bool>,
    incoming_observation: Option<CreditObservation>,
    restrictive: bool,
) {
    let (Some(next), Some(next_observation)) = (incoming_value, incoming_observation) else {
        return;
    };
    let replace = observation.is_none_or(|current| {
        next_observation.recency(current).is_gt()
            || (next_observation.recency(current).is_eq()
                && ((*value != Some(next) && next == restrictive)
                    || (*value == Some(next) && next_observation.source >= current.source)))
    });
    if replace {
        *value = Some(next);
        *observation = Some(next_observation);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_decimal_contract_preserves_sign_precision_and_rejects_non_observations() {
        for value in [
            "0",
            "-0.42",
            "+5",
            "12.9900000000",
            "99999999999999999999999.00001",
        ] {
            assert!(is_credit_balance(value), "{value}");
        }
        for value in ["", "+", "-", "NaN", "1e3", ".5", "5.", " 5", "5 ", "1.2.3"] {
            assert!(!is_credit_balance(value), "{value}");
        }
    }

    #[test]
    fn partial_observations_preserve_flags_and_balance_without_refreshing_them() {
        let mut first = SubscriptionQuotaCredits {
            has_credits: Some(false),
            balance: Some("12.9900000000".to_owned()),
            ..Default::default()
        };
        first.stamp_observations(1_000, CreditSource::Header);
        let mut balance = SubscriptionQuotaCredits {
            balance: Some("0.4200000000".to_owned()),
            ..Default::default()
        };
        balance.stamp_observations(10_000, CreditSource::Header);
        first.merge_observations(&balance);
        assert_eq!(first.has_credits, Some(false));
        assert_eq!(
            first
                .observations
                .has_credits
                .unwrap()
                .observed_at_unix_millis,
            1_000
        );
        assert_eq!(first.balance.as_deref(), Some("0.4200000000"));
        assert_eq!(
            first.observations.balance.unwrap().observed_at_unix_millis,
            10_000
        );
        let mut flags = SubscriptionQuotaCredits {
            has_credits: Some(true),
            ..Default::default()
        };
        flags.stamp_observations(20_000, CreditSource::Api);
        first.merge_observations(&flags);
        assert_eq!(first.has_credits, Some(true));
        assert_eq!(first.balance.as_deref(), Some("0.4200000000"));
        assert_eq!(
            first.observations.balance.unwrap().observed_at_unix_millis,
            10_000
        );
    }

    #[test]
    fn equal_time_flags_are_restrictive_until_same_field_reports_a_newer_value() {
        for reverse in [false, true] {
            let mut positive = SubscriptionQuotaCredits {
                has_credits: Some(true),
                unlimited: Some(true),
                limit_reached: Some(false),
                balance: Some("0.6".to_owned()),
                ..Default::default()
            };
            positive.stamp_observations(1_000, CreditSource::Api);
            let mut negative = SubscriptionQuotaCredits {
                has_credits: Some(false),
                unlimited: Some(false),
                limit_reached: Some(true),
                balance: Some("0.9".to_owned()),
                ..Default::default()
            };
            negative.stamp_observations(1_000, CreditSource::Header);
            let mut state = if reverse {
                positive.clone()
            } else {
                negative.clone()
            };
            state.merge_observations(if reverse { &negative } else { &positive });
            assert_eq!(state.has_credits, Some(false));
            assert_eq!(state.unlimited, Some(false));
            assert_eq!(state.limit_reached, Some(true));
            assert_eq!(state.balance.as_deref(), Some("0.6"));
            let mut newer = SubscriptionQuotaCredits {
                has_credits: Some(true),
                ..Default::default()
            };
            newer.stamp_observations(2_000, CreditSource::Api);
            state.merge_observations(&newer);
            assert_eq!(state.has_credits, Some(true));
            assert_eq!(state.limit_reached, Some(true));
        }
    }
}
