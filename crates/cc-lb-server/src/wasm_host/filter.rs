use std::sync::Arc;

use cc_lb_domain::{Principal, UpstreamCandidate};
use cc_lb_plugin_wire::ArchivedFilterResponse;
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, PerCandidateReason, RoutingContext};
use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntimeError};
use uuid::Uuid;

use super::access_archived_scoped_or_copy;
use super::filter_request::FilterWireRequest;

pub struct WasmtimeFilterPlugin {
    dispatch: Arc<WasmPluginWireDispatch>,
    plugin_id: Uuid,
    plugin_name: String,
}

impl WasmtimeFilterPlugin {
    pub fn new(
        dispatch: Arc<WasmPluginWireDispatch>,
        plugin_id: Uuid,
        plugin_name: impl Into<String>,
    ) -> Self {
        Self {
            dispatch,
            plugin_id,
            plugin_name: plugin_name.into(),
        }
    }
}

impl FilterPlugin for WasmtimeFilterPlugin {
    fn filter(
        &self,
        ctx: &RoutingContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let wire_version =
            self.dispatch
                .filter_wire_version()
                .ok_or_else(|| FilterError::Runtime {
                    reason: "plugin metadata missing filter hook".to_owned(),
                })?;

        let guest_result = FilterWireRequest {
            wire_version,
            ctx,
            principal,
            candidates,
            cookie_redaction: self.dispatch.cookie_redaction(),
        }
        .encode(|in_bytes| {
                self.dispatch.call_filter_scoped(in_bytes, |guest_bytes| {
                    let bound = self.dispatch.wire_bounds().output_body_bytes;
                    if guest_bytes.len() as u64 > bound {
                        return Err(FilterError::Runtime {
                            reason: format!(
                                "filter output {} bytes exceeds wire_bounds.output_body_bytes ({bound})",
                                guest_bytes.len(),
                            ),
                        });
                    }
                    access_archived_scoped_or_copy::<ArchivedFilterResponse, _>(
                        guest_bytes,
                        |archived| {
                            wire_to_host_output(
                                archived,
                                self.dispatch.wire_bounds().reason_bytes as usize,
                            )
                        },
                    )
                    .map_err(|e| FilterError::Runtime {
                        reason: format!("rkyv access response: {e}"),
                    })
                    .and_then(std::convert::identity)
                })
            })
        .map_err(|e| FilterError::Runtime {
            reason: format!("rkyv encode request: {e}"),
        })?;
        guest_result.map_err(runtime_error_to_filter)?
    }

    fn plugin_id(&self) -> Uuid {
        self.plugin_id
    }

    fn plugin_name(&self) -> &str {
        &self.plugin_name
    }
}

fn runtime_error_to_filter(err: WasmtimeRuntimeError) -> FilterError {
    match err {
        WasmtimeRuntimeError::GuestTrap { phase, source } => FilterError::Trap {
            reason: format!("{phase}: {source}"),
        },
        other => FilterError::Runtime {
            reason: other.to_string(),
        },
    }
}

fn wire_to_host_output(
    archived: &ArchivedFilterResponse,
    reason_cap: usize,
) -> Result<FilterOutput, FilterError> {
    let mut kept_upstream_ids = Vec::new();
    let mut per_candidate_reasons = Vec::new();
    let mut reasons = Vec::new();

    for result in archived.results.iter() {
        let upstream_id_str: &str = &result.upstream_id;
        let decision_str: &str = &result.decision;
        let reason_str: &str = &result.reason;
        let upstream_id =
            Uuid::parse_str(upstream_id_str).map_err(|source| FilterError::Runtime {
                reason: format!(
                    "plugin returned invalid upstream_id `{upstream_id_str}`: {source}"
                ),
            })?;
        if decision_str == "accept" {
            kept_upstream_ids.push(upstream_id);
        } else {
            per_candidate_reasons.push(per_candidate_reason_from_label(decision_str, reason_str));
        }
        if !reason_str.is_empty() {
            let truncated = truncate_reason(reason_str, reason_cap);
            reasons.push(format!("{upstream_id_str}: {truncated}"));
        }
    }

    Ok(FilterOutput {
        kept_upstream_ids,
        reason: reasons.join("; "),
        per_candidate_reasons,
        subscription_preference: None,
    })
}

fn truncate_reason(reason: &str, cap: usize) -> std::borrow::Cow<'_, str> {
    if reason.len() <= cap {
        return std::borrow::Cow::Borrowed(reason);
    }
    let mut end = cap;
    while end > 0 && !reason.is_char_boundary(end) {
        end -= 1;
    }
    std::borrow::Cow::Owned(reason[..end].to_owned())
}

fn per_candidate_reason_from_label(decision: &str, reason: &str) -> PerCandidateReason {
    let label = if decision == "accept" {
        reason
    } else {
        decision
    };
    let label = label.replace('-', "_").to_ascii_lowercase();
    if label.contains("rate_limit") {
        PerCandidateReason::RateLimited
    } else if label.contains("quota") {
        PerCandidateReason::InsufficientQuota
    } else if label.contains("unhealthy") {
        PerCandidateReason::Unhealthy
    } else {
        PerCandidateReason::RejectedByPlugin
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use cc_lb_plugin_wire::FilterResponse as WireFilterResponse;
    use cc_lb_plugin_wire::PerCandidateReason as WirePerCandidateReason;
    use rkyv::rancor::Error as RkyvError;

    #[test]
    fn t1__wire_to_host_splits_kept_and_rejected() {
        let response = WireFilterResponse {
            results: Box::new([
                WirePerCandidateReason {
                    upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
                    decision: Box::from("accept"),
                    reason: Box::from("top-K"),
                },
                WirePerCandidateReason {
                    upstream_id: Box::from("22222222-2222-2222-2222-222222222222"),
                    decision: Box::from("rate-limit"),
                    reason: Box::from("burst exceeded"),
                },
            ]),
        };
        let bytes = rkyv::to_bytes::<RkyvError>(&response).expect("encode");
        let archived =
            rkyv::access::<ArchivedFilterResponse, RkyvError>(&bytes).expect("archived view");
        let out = wire_to_host_output(archived, 256).expect("conversion must succeed");
        assert_eq!(out.kept_upstream_ids.len(), 1);
        assert_eq!(
            out.kept_upstream_ids[0],
            Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
        );
        assert_eq!(out.per_candidate_reasons.len(), 1);
        assert_eq!(
            out.per_candidate_reasons[0],
            PerCandidateReason::RateLimited
        );
    }

    #[test]
    fn t2__guest_trap_normalizes_to_filter_trap_with_exact_reason() {
        let error = runtime_error_to_filter(WasmtimeRuntimeError::GuestTrap {
            phase: "cc_lb_filter",
            source: anyhow::anyhow!("guest exploded"),
        });

        match error {
            FilterError::Trap { reason } => {
                assert_eq!(reason, "cc_lb_filter: guest exploded");
            }
            FilterError::Runtime { reason } => {
                panic!("expected trap normalization, got runtime error: {reason}");
            }
        }
    }
}
