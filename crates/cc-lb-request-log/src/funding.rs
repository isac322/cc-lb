use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

/// Estimate of this response's main-limit meter state, never a billing receipt.
/// Missing evidence is represented by `None`, including on legacy request rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestFunding {
    /// Complete supported main-limit meters were below exhaustion.
    IncludedEstimated,
    /// An exhausted main meter accompanied explicit available credit flags.
    CreditsEstimated,
}

impl RequestFunding {
    /// Stable database and API vocabulary.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IncludedEstimated => "included_estimated",
            Self::CreditsEstimated => "credits_estimated",
        }
    }
}

/// A stored funding value outside the supported vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseRequestFundingError;

impl fmt::Display for ParseRequestFundingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid request funding estimate")
    }
}
impl std::error::Error for ParseRequestFundingError {}

impl FromStr for RequestFunding {
    type Err = ParseRequestFundingError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "included_estimated" => Ok(Self::IncludedEstimated),
            "credits_estimated" => Ok(Self::CreditsEstimated),
            _ => Err(ParseRequestFundingError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_stored_values_fail_closed_and_legacy_rows_remain_unknown() {
        for invalid in ["credits", "included", "unknown", "", "CREDITS_ESTIMATED"] {
            assert!(invalid.parse::<RequestFunding>().is_err());
        }
        let legacy: crate::RequestEvent =
            serde_json::from_str(r#"{"status":200,"duration_ms":0}"#).unwrap();
        assert_eq!(legacy.funding, None);
        for funding in [
            RequestFunding::IncludedEstimated,
            RequestFunding::CreditsEstimated,
        ] {
            let event = crate::RequestEvent {
                funding: Some(funding),
                ..Default::default()
            };
            let restored: crate::RequestEvent =
                serde_json::from_value(serde_json::to_value(event).unwrap()).unwrap();
            assert_eq!(restored.funding, Some(funding));
            assert_eq!(funding.as_str().parse(), Ok(funding));
        }
    }
}
