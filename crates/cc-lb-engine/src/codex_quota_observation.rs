//! Pure helpers used by the off-path quota subscriber. No account balance or
//! routing decision is evidence of a particular request's funding.

use cc_lb_request_log::RequestFunding;
use http::{HeaderMap, HeaderValue};
use serde_json::Value;

pub fn funding_from_headers(headers: &HeaderMap) -> Option<RequestFunding> {
    let mut five_hour = None;
    let mut seven_day = None;
    for window in ["primary", "secondary"] {
        let minutes = headers
            .get(format!("x-codex-{window}-window-minutes"))?
            .to_str()
            .ok()?
            .parse::<u64>()
            .ok()?;
        let used = headers
            .get(format!("x-codex-{window}-used-percent"))?
            .to_str()
            .ok()?
            .parse::<f64>()
            .ok()?;
        if !used.is_finite() || used < 0.0 {
            return None;
        }
        match minutes {
            300 => five_hour = Some(used),
            10_080 => seven_day = Some(used),
            _ => return None,
        }
    }
    let (Some(five_hour), Some(seven_day)) = (five_hour, seven_day) else {
        return None;
    };
    if five_hour < 100.0 && seven_day < 100.0 {
        return Some(RequestFunding::IncludedEstimated);
    }
    let flag = |name| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("true") || v == "1")
    };
    (!flag("x-codex-credits-limit-reached")
        && (flag("x-codex-credits-has-credits") || flag("x-codex-credits-unlimited")))
    .then_some(RequestFunding::CreditsEstimated)
}

/// Returns whether a `codex.rate_limits` frame belongs to the main `codex`
/// meter family rather than an auxiliary limit family (`codex_bengalfox`, `premium`, etc.).
pub fn is_main_codex_frame(value: &Value) -> bool {
    ["metered_limit_name", "limit_name", "limit_id"]
        .into_iter()
        .all(|key| {
            value
                .get(key)
                .and_then(Value::as_str)
                .is_none_or(|family| family.eq_ignore_ascii_case("codex"))
        })
}

/// Adapt the actual Codex event to the existing header parser. The caller has
/// already parsed this frame once, on the quota worker. Extra limit families
/// remain separate and never overwrite the main Codex quota window meters;
/// workspace-level credit depletion frames are still translated into credit
/// exhaustion headers.
pub fn websocket_headers(value: &Value) -> Option<HeaderMap> {
    if value.get("type").and_then(Value::as_str) != Some("codex.rate_limits") {
        return None;
    }
    let is_main_codex = is_main_codex_frame(value);
    let credits_depleted = value.get("rate_limit_reached_type").and_then(Value::as_str)
        == Some("workspace_member_credits_depleted");
    if !is_main_codex && !credits_depleted {
        return None;
    }
    let mut headers = HeaderMap::new();
    if is_main_codex {
        let limits = value
            .get("rate_limits")
            .filter(|limits| limits.is_object())
            .unwrap_or(value);
        for window in ["primary", "secondary"] {
            let Some(meter) = limits.get(window).and_then(Value::as_object) else {
                continue;
            };
            if let Some(used) = meter.get("used_percent").and_then(Value::as_f64)
                && used.is_finite()
                && used >= 0.0
            {
                headers.insert(
                    http::header::HeaderName::from_bytes(
                        format!("x-codex-{window}-used-percent").as_bytes(),
                    )
                    .ok()?,
                    HeaderValue::from_str(&used.to_string()).ok()?,
                );
            }
            for (field, suffix) in [
                ("window_minutes", "window-minutes"),
                ("reset_at", "reset-at"),
            ] {
                if let Some(number) = meter.get(field).and_then(Value::as_u64) {
                    headers.insert(
                        http::header::HeaderName::from_bytes(
                            format!("x-codex-{window}-{suffix}").as_bytes(),
                        )
                        .ok()?,
                        HeaderValue::from_str(&number.to_string()).ok()?,
                    );
                }
            }
        }
    }
    if let Some(credits) = value.get("credits").and_then(Value::as_object) {
        for (field, header) in [
            ("has_credits", "x-codex-credits-has-credits"),
            ("unlimited", "x-codex-credits-unlimited"),
            ("overage_limit_reached", "x-codex-credits-limit-reached"),
        ] {
            if let Some(flag) = credits.get(field).and_then(Value::as_bool) {
                headers.insert(
                    header,
                    HeaderValue::from_static(if flag { "true" } else { "false" }),
                );
            }
        }
        if let Some(balance) = credits.get("balance").and_then(Value::as_str)
            && cc_lb_domain::is_credit_balance(balance)
        {
            headers.insert(
                "x-codex-credits-balance",
                HeaderValue::from_str(balance).ok()?,
            );
        }
    }
    if credits_depleted {
        headers.insert(
            "x-codex-credits-limit-reached",
            HeaderValue::from_static("true"),
        );
    }
    Some(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meter() -> Value {
        json!({
            "type": "codex.rate_limits",
            "rate_limits": {
                "primary": {"used_percent": 12.5, "window_minutes": 300, "reset_at": 1_900_000_000u64},
                "secondary": {"used_percent": 50, "window_minutes": 10080, "reset_at": 1_900_500_000u64}
            },
            "credits": {"has_credits": true, "unlimited": false, "balance": "12.9900000000"}
        })
    }

    #[test]
    fn own_complete_meters_estimate_without_using_balance_or_account_state() {
        let mut value = meter();
        assert_eq!(
            funding_from_headers(&websocket_headers(&value).unwrap()),
            Some(RequestFunding::IncludedEstimated)
        );
        value["rate_limits"]["primary"]["used_percent"] = json!(100);
        assert_eq!(
            funding_from_headers(&websocket_headers(&value).unwrap()),
            Some(RequestFunding::CreditsEstimated)
        );
        value["credits"]["has_credits"] = json!(false);
        assert_eq!(
            funding_from_headers(&websocket_headers(&value).unwrap()),
            None
        );
        value["credits"]["balance"] = json!("9999999999999999999999");
        assert_eq!(
            funding_from_headers(&websocket_headers(&value).unwrap()),
            None
        );
    }

    #[test]
    fn incomplete_unknown_additional_and_credit_only_meters_do_not_estimate() {
        for path in ["primary", "secondary"] {
            let mut value = meter();
            value["rate_limits"][path] = Value::Null;
            assert_eq!(
                funding_from_headers(&websocket_headers(&value).unwrap()),
                None
            );
        }
        let mut value = meter();
        value["rate_limits"]["primary"]["window_minutes"] = json!(60);
        assert_eq!(
            funding_from_headers(&websocket_headers(&value).unwrap()),
            None
        );
        value["metered_limit_name"] = json!("codex_other");
        assert!(websocket_headers(&value).is_none());
        let value =
            json!({"type":"codex.rate_limits", "credits":{"has_credits":true,"balance":"42.0"}});
        assert_eq!(
            funding_from_headers(&websocket_headers(&value).unwrap()),
            None
        );
    }

    #[test]
    fn null_and_missing_balance_are_not_zero_observations() {
        for credits in [
            Value::Null,
            json!({"has_credits":true}),
            json!({"balance":" 12"}),
        ] {
            let value = json!({"type":"codex.rate_limits", "credits":credits});
            let headers = websocket_headers(&value).unwrap();
            assert!(!headers.contains_key("x-codex-credits-balance"));
        }
        let headers = websocket_headers(&meter()).unwrap();
        assert_eq!(headers["x-codex-credits-balance"], "12.9900000000");
    }

    #[test]
    fn non_codex_limit_id_is_rejected_and_workspace_credits_depletion_is_propagated() {
        let mut supplemental = meter();
        supplemental["limit_id"] = json!("codex_bengalfox");
        assert!(websocket_headers(&supplemental).is_none());

        let mut codex_main = meter();
        codex_main["limit_id"] = json!("codex");
        assert!(websocket_headers(&codex_main).is_some());

        let depleted = json!({
            "type": "codex.rate_limits",
            "limit_id": "premium",
            "limit_name": null,
            "primary": null,
            "secondary": null,
            "credits": {"has_credits": false, "unlimited": false, "balance": null},
            "plan_type": "team",
            "rate_limit_reached_type": "workspace_member_credits_depleted"
        });
        let headers = websocket_headers(&depleted).expect("depletion frame produces headers");
        assert_eq!(headers["x-codex-credits-limit-reached"], "true");
        assert_eq!(headers["x-codex-credits-has-credits"], "false");
        assert_eq!(headers["x-codex-credits-unlimited"], "false");
        assert!(!headers.contains_key("x-codex-primary-used-percent"));
        assert!(!headers.contains_key("x-codex-secondary-used-percent"));

        let parsed = cc_lb_quota::parse_openai_codex_quota_headers(&headers);
        let unified = parsed
            .iter()
            .find(|obs| obs.window == cc_lb_storage_api::SubscriptionQuotaWindow::Unified)
            .expect("unified credit observation");
        assert_eq!(unified.fallback_available, Some(false));
        assert_eq!(
            unified.credits.as_ref().and_then(|c| c.limit_reached),
            Some(true)
        );

        let top_level_main = json!({
            "type": "codex.rate_limits",
            "limit_id": "codex",
            "primary": {"used_percent": 15.0, "window_minutes": 300, "reset_at": 1_900_000_000u64},
            "secondary": {"used_percent": 40.0, "window_minutes": 10080, "reset_at": 1_900_500_000u64},
            "credits": {"has_credits": false, "unlimited": false}
        });
        let top_headers = websocket_headers(&top_level_main).expect("top-level codex headers");
        assert_eq!(top_headers["x-codex-primary-used-percent"], "15");
        assert_eq!(
            funding_from_headers(&top_headers),
            Some(RequestFunding::IncludedEstimated)
        );

        let overage_blocked = json!({
            "type": "codex.rate_limits",
            "limit_id": "codex",
            "rate_limits": {
                "primary": {"used_percent": 100, "window_minutes": 300, "reset_at": 1_900_000_000u64},
                "secondary": {"used_percent": 50, "window_minutes": 10080, "reset_at": 1_900_500_000u64}
            },
            "credits": {"has_credits": true, "unlimited": false, "overage_limit_reached": true}
        });
        let overage_headers = websocket_headers(&overage_blocked).expect("overage headers");
        assert_eq!(overage_headers["x-codex-credits-limit-reached"], "true");
        assert_eq!(funding_from_headers(&overage_headers), None);
    }
}
