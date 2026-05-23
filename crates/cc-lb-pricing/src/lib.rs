#![forbid(unsafe_code)]

const MICROS_PER_USD: u64 = 1_000_000;
const MICROS_PER_MILLION: u128 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PricingStatus {
    Known,
    Unknown,
}

impl PricingStatus {
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Known => "known",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CostEstimate {
    pub micros_usd: Option<u64>,
    pub pricing_status: PricingStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UsdPerMillion {
    micros_usd: u64,
}

impl UsdPerMillion {
    pub const fn from_whole_usd(usd: u64) -> Self {
        Self {
            micros_usd: usd * MICROS_PER_USD,
        }
    }

    pub const fn as_micros_usd(self) -> u64 {
        self.micros_usd
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pricing {
    pub model: &'static str,
    pub input_per_million_usd: UsdPerMillion,
    pub output_per_million_usd: UsdPerMillion,
}

const OPUS_4_1_INPUT: UsdPerMillion = UsdPerMillion::from_whole_usd(15);
const OPUS_4_1_OUTPUT: UsdPerMillion = UsdPerMillion::from_whole_usd(75);
const OPUS_4_5_INPUT: UsdPerMillion = UsdPerMillion::from_whole_usd(5);
const OPUS_4_5_OUTPUT: UsdPerMillion = UsdPerMillion::from_whole_usd(25);
const SONNET_INPUT: UsdPerMillion = UsdPerMillion::from_whole_usd(3);
const SONNET_OUTPUT: UsdPerMillion = UsdPerMillion::from_whole_usd(15);
const HAIKU_4_5_INPUT: UsdPerMillion = UsdPerMillion::from_whole_usd(1);
const HAIKU_4_5_OUTPUT: UsdPerMillion = UsdPerMillion::from_whole_usd(5);

const PRICING_TABLE: [Pricing; 10] = [
    Pricing {
        model: "claude-opus-4-1",
        input_per_million_usd: OPUS_4_1_INPUT,
        output_per_million_usd: OPUS_4_1_OUTPUT,
    },
    Pricing {
        model: "claude-opus-4-1-20250805",
        input_per_million_usd: OPUS_4_1_INPUT,
        output_per_million_usd: OPUS_4_1_OUTPUT,
    },
    Pricing {
        model: "claude-opus-4-5",
        input_per_million_usd: OPUS_4_5_INPUT,
        output_per_million_usd: OPUS_4_5_OUTPUT,
    },
    Pricing {
        model: "claude-sonnet-4-5",
        input_per_million_usd: SONNET_INPUT,
        output_per_million_usd: SONNET_OUTPUT,
    },
    Pricing {
        model: "claude-sonnet-4-5-20250929",
        input_per_million_usd: SONNET_INPUT,
        output_per_million_usd: SONNET_OUTPUT,
    },
    Pricing {
        model: "claude-haiku-4-5",
        input_per_million_usd: HAIKU_4_5_INPUT,
        output_per_million_usd: HAIKU_4_5_OUTPUT,
    },
    Pricing {
        model: "claude-3-5-sonnet-20241022",
        input_per_million_usd: SONNET_INPUT,
        output_per_million_usd: SONNET_OUTPUT,
    },
    Pricing {
        model: "claude-3-5-sonnet-20240620",
        input_per_million_usd: SONNET_INPUT,
        output_per_million_usd: SONNET_OUTPUT,
    },
    Pricing {
        model: "claude-3-5-sonnet-latest",
        input_per_million_usd: SONNET_INPUT,
        output_per_million_usd: SONNET_OUTPUT,
    },
    Pricing {
        model: "claude-3-5-sonnet",
        input_per_million_usd: SONNET_INPUT,
        output_per_million_usd: SONNET_OUTPUT,
    },
];

pub fn pricing_table() -> &'static [Pricing] {
    &PRICING_TABLE
}

pub fn pricing_for_model(model: &str) -> Option<Pricing> {
    match model {
        "claude-opus-4-1" => Some(PRICING_TABLE[0]),
        "claude-opus-4-1-20250805" => Some(PRICING_TABLE[1]),
        "claude-opus-4-5" => Some(PRICING_TABLE[2]),
        "claude-sonnet-4-5" => Some(PRICING_TABLE[3]),
        "claude-sonnet-4-5-20250929" => Some(PRICING_TABLE[4]),
        "claude-haiku-4-5" => Some(PRICING_TABLE[5]),
        "claude-3-5-sonnet-20241022" => Some(PRICING_TABLE[6]),
        "claude-3-5-sonnet-20240620" => Some(PRICING_TABLE[7]),
        "claude-3-5-sonnet-latest" => Some(PRICING_TABLE[8]),
        "claude-3-5-sonnet" => Some(PRICING_TABLE[9]),
        _ => None,
    }
}

pub fn virtual_cost_micros(model: &str, input_tokens: u64, output_tokens: u64) -> CostEstimate {
    let Some(pricing) = pricing_for_model(model) else {
        return CostEstimate {
            micros_usd: None,
            pricing_status: PricingStatus::Unknown,
        };
    };

    let numerator = (input_tokens as u128)
        .saturating_mul(pricing.input_per_million_usd.as_micros_usd() as u128)
        .saturating_add(
            (output_tokens as u128)
                .saturating_mul(pricing.output_per_million_usd.as_micros_usd() as u128),
        );
    let rounded_micros = numerator.saturating_add(MICROS_PER_MILLION / 2) / MICROS_PER_MILLION;

    CostEstimate {
        micros_usd: Some(rounded_micros.try_into().unwrap_or(u64::MAX)),
        pricing_status: PricingStatus::Known,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_model_returns_expected_micros() {
        let estimate = virtual_cost_micros("claude-sonnet-4-5", 10, 20);

        assert_eq!(estimate.pricing_status, PricingStatus::Known);
        assert_eq!(estimate.micros_usd, Some(330));
    }

    #[test]
    fn unknown_model_returns_unknown_without_cost() {
        let estimate = virtual_cost_micros("not-a-claude-model", 10, 20);

        assert_eq!(estimate.pricing_status, PricingStatus::Unknown);
        assert_eq!(estimate.micros_usd, None);
    }

    #[test]
    fn zero_tokens_returns_zero_micros_for_known_model() {
        let estimate = virtual_cost_micros("claude-opus-4-1", 0, 0);

        assert_eq!(estimate.pricing_status, PricingStatus::Known);
        assert_eq!(estimate.micros_usd, Some(0));
    }

    #[test]
    fn large_token_counts_saturate_without_overflowing() {
        let estimate = virtual_cost_micros("claude-opus-4-1", u64::MAX, u64::MAX);

        assert_eq!(estimate.pricing_status, PricingStatus::Known);
        assert_eq!(estimate.micros_usd, Some(u64::MAX));
    }
}
