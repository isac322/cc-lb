//! Tests for FilterPlugin trait, FilterOutput, and FilterError.

use cc_lb_plugin_api::{
    FilterError, FilterOutput, FilterPlugin, Principal, PrincipalKind, RequestContext,
    TerminalStrategy, UpstreamCandidate, UpstreamKind,
};
use bytes::Bytes;
use http::Method;
use uuid::Uuid;

#[test]
fn filter_output_equality() {
    let output1 = FilterOutput {
        kept_upstream_ids: vec![Uuid::new_v4()],
        reason: "allowed".to_string(),
        per_candidate_reasons: Vec::new(),
    };
    let output2 = FilterOutput {
        kept_upstream_ids: output1.kept_upstream_ids.clone(),
        reason: "allowed".to_string(),
        per_candidate_reasons: Vec::new(),
    };
    assert_eq!(output1, output2);
}

#[test]
fn filter_output_inequality_different_ids() {
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let output1 = FilterOutput {
        kept_upstream_ids: vec![id1],
        reason: "allowed".to_string(),
        per_candidate_reasons: Vec::new(),
    };
    let output2 = FilterOutput {
        kept_upstream_ids: vec![id2],
        reason: "allowed".to_string(),
        per_candidate_reasons: Vec::new(),
    };
    assert_ne!(output1, output2);
}

#[test]
fn filter_output_inequality_different_reason() {
    let id = Uuid::new_v4();
    let output1 = FilterOutput {
        kept_upstream_ids: vec![id],
        reason: "allowed".to_string(),
        per_candidate_reasons: Vec::new(),
    };
    let output2 = FilterOutput {
        kept_upstream_ids: vec![id],
        reason: "rejected".to_string(),
        per_candidate_reasons: Vec::new(),
    };
    assert_ne!(output1, output2);
}

#[test]
fn filter_output_debug_formatting() {
    let id = Uuid::new_v4();
    let output = FilterOutput {
        kept_upstream_ids: vec![id],
        reason: "test reason".to_string(),
        per_candidate_reasons: Vec::new(),
    };
    let debug_str = format!("{:?}", output);
    assert!(debug_str.contains("FilterOutput"));
    assert!(debug_str.contains("test reason"));
}

#[test]
fn filter_output_clone() {
    let id = Uuid::new_v4();
    let output = FilterOutput {
        kept_upstream_ids: vec![id],
        reason: "original".to_string(),
        per_candidate_reasons: Vec::new(),
    };
    let cloned = output.clone();
    assert_eq!(output, cloned);
}

#[test]
fn filter_error_runtime_display() {
    let err = FilterError::Runtime {
        reason: "custom filter failed".to_string(),
    };
    let display_str = format!("{}", err);
    assert!(display_str.contains("filter runtime error"));
    assert!(display_str.contains("custom filter failed"));
}

#[test]
fn filter_error_trap_display() {
    let err = FilterError::Trap {
        reason: "plugin crashed".to_string(),
    };
    let display_str = format!("{}", err);
    assert!(display_str.contains("filter trap error"));
    assert!(display_str.contains("plugin crashed"));
}

#[test]
fn filter_error_implements_error_trait() {
    let err = FilterError::Runtime {
        reason: "test".to_string(),
    };
    let _: &dyn std::error::Error = &err;
}

#[test]
fn filter_error_debug_formatting() {
    let err = FilterError::Trap {
        reason: "test trap".to_string(),
    };
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("Trap"));
    assert!(debug_str.contains("test trap"));
}

struct AllowAllFilter;

impl FilterPlugin for AllowAllFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let ids: Vec<Uuid> = candidates.iter().map(|c| c.upstream_id).collect();
        Ok(FilterOutput {
            kept_upstream_ids: ids,
            reason: "all candidates allowed".to_string(),
            per_candidate_reasons: Vec::new(),
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "allow-all"
    }
}

struct SelectiveFilter;

impl FilterPlugin for SelectiveFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let filtered: Vec<Uuid> = candidates
            .iter()
            .filter(|c| c.name.contains("prod"))
            .map(|c| c.upstream_id)
            .collect();
        Ok(FilterOutput {
            kept_upstream_ids: filtered,
            reason: "filtered to production upstreams".to_string(),
            per_candidate_reasons: Vec::new(),
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::new_v4()
    }

    fn plugin_name(&self) -> &str {
        "selective"
    }
}

struct ErrorProducingFilter;

impl FilterPlugin for ErrorProducingFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Err(FilterError::Runtime {
            reason: "filter infrastructure failed".to_string(),
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "error"
    }
}

#[test]
fn filter_plugin_object_safety() {
    let allow: Box<dyn FilterPlugin> = Box::new(AllowAllFilter);
    let selective: Box<dyn FilterPlugin> = Box::new(SelectiveFilter);
    let error: Box<dyn FilterPlugin> = Box::new(ErrorProducingFilter);

    let _plugins: Vec<Box<dyn FilterPlugin>> = vec![allow, selective, error];
}

#[test]
fn allow_all_filter_execution() {
    let filter = AllowAllFilter;
    let ctx = RequestContext {
        request_id: "req-1".to_string(),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_string(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: "claude-3-sonnet".to_string(),
    };
    let principal = Principal {
        id: "principal-1".to_string(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::json!({}).as_object().unwrap().clone(),
    };
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let candidates = vec![
        UpstreamCandidate {
            upstream_id: id1,
            name: "upstream-1".to_string(),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 0,
            cache_score: None,
            base_url: None,
        },
        UpstreamCandidate {
            upstream_id: id2,
            name: "upstream-2".to_string(),
            kind: UpstreamKind::AnthropicOauth,
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 0,
            cache_score: None,
            base_url: None,
        },
    ];

    let result = filter.filter(&ctx, &principal, &candidates);
    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.kept_upstream_ids.len(), 2);
    assert!(output.kept_upstream_ids.contains(&id1));
    assert!(output.kept_upstream_ids.contains(&id2));
}

#[test]
fn selective_filter_execution() {
    let filter = SelectiveFilter;
    let ctx = RequestContext {
        request_id: "req-2".to_string(),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_string(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: "claude-3-sonnet".to_string(),
    };
    let principal = Principal {
        id: "principal-2".to_string(),
        kind: PrincipalKind::OAuthSubject,
        claims: serde_json::json!({}).as_object().unwrap().clone(),
    };
    let prod_id = Uuid::new_v4();
    let staging_id = Uuid::new_v4();
    let candidates = vec![
        UpstreamCandidate {
            upstream_id: prod_id,
            name: "upstream-prod".to_string(),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 0,
            cache_score: None,
            base_url: None,
        },
        UpstreamCandidate {
            upstream_id: staging_id,
            name: "upstream-staging".to_string(),
            kind: UpstreamKind::AnthropicOauth,
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 0,
            cache_score: None,
            base_url: None,
        },
    ];

    let result = filter.filter(&ctx, &principal, &candidates);
    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.kept_upstream_ids.len(), 1);
    assert_eq!(output.kept_upstream_ids[0], prod_id);
}

#[test]
fn error_filter_execution() {
    let filter = ErrorProducingFilter;
    let ctx = RequestContext {
        request_id: "req-3".to_string(),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_string(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: "claude-3-sonnet".to_string(),
    };
    let principal = Principal {
        id: "principal-3".to_string(),
        kind: PrincipalKind::InternalKey,
        claims: serde_json::json!({}).as_object().unwrap().clone(),
    };
    let candidates = vec![];

    let result = filter.filter(&ctx, &principal, &candidates);
    assert!(result.is_err());
}

#[test]
fn filter_plugin_methods() {
    let filter = SelectiveFilter;
    let id = filter.plugin_id();
    let name = filter.plugin_name();
    assert_eq!(name, "selective");
    assert!(!id.is_nil());
}

#[test]
fn allow_all_filter_plugin_methods() {
    let filter = AllowAllFilter;
    let id = filter.plugin_id();
    let name = filter.plugin_name();
    assert_eq!(name, "allow-all");
    assert_eq!(id, Uuid::nil());
}

#[test]
fn terminal_strategy_default() {
    let default = TerminalStrategy::default();
    assert_eq!(default, TerminalStrategy::FirstPick);
}

#[test]
fn terminal_strategy_equality() {
    let first1 = TerminalStrategy::FirstPick;
    let first2 = TerminalStrategy::FirstPick;
    assert_eq!(first1, first2);

    let round_robin = TerminalStrategy::RoundRobin;
    assert_ne!(first1, round_robin);
}

#[test]
fn terminal_strategy_serialization() {
    let strategies = vec![
        TerminalStrategy::FirstPick,
        TerminalStrategy::RoundRobin,
        TerminalStrategy::LeastConnections,
    ];

    for strategy in strategies {
        let json = serde_json::to_string(&strategy).expect("serialization failed");
        let deserialized: TerminalStrategy =
            serde_json::from_str(&json).expect("deserialization failed");
        assert_eq!(strategy, deserialized);
    }
}

#[test]
fn terminal_strategy_serialization_format() {
    let first_pick = TerminalStrategy::FirstPick;
    let json = serde_json::to_string(&first_pick).unwrap();
    assert_eq!(json, "\"first-pick\"");

    let round_robin = TerminalStrategy::RoundRobin;
    let json = serde_json::to_string(&round_robin).unwrap();
    assert_eq!(json, "\"round-robin\"");

    let least_conn = TerminalStrategy::LeastConnections;
    let json = serde_json::to_string(&least_conn).unwrap();
    assert_eq!(json, "\"least-connections\"");
}

#[test]
fn terminal_strategy_deserialization_from_kebab_case() {
    let json = "\"first-pick\"";
    let strategy: TerminalStrategy = serde_json::from_str(json).unwrap();
    assert_eq!(strategy, TerminalStrategy::FirstPick);

    let json = "\"round-robin\"";
    let strategy: TerminalStrategy = serde_json::from_str(json).unwrap();
    assert_eq!(strategy, TerminalStrategy::RoundRobin);

    let json = "\"least-connections\"";
    let strategy: TerminalStrategy = serde_json::from_str(json).unwrap();
    assert_eq!(strategy, TerminalStrategy::LeastConnections);
}

#[test]
fn terminal_strategy_debug_formatting() {
    let first = TerminalStrategy::FirstPick;
    let debug_str = format!("{:?}", first);
    assert_eq!(debug_str, "FirstPick");

    let round_robin = TerminalStrategy::RoundRobin;
    let debug_str = format!("{:?}", round_robin);
    assert_eq!(debug_str, "RoundRobin");

    let least_conn = TerminalStrategy::LeastConnections;
    let debug_str = format!("{:?}", least_conn);
    assert_eq!(debug_str, "LeastConnections");
}

#[test]
fn terminal_strategy_copy_clone() {
    let first = TerminalStrategy::FirstPick;
    let cloned = first.clone();
    let copied = first;
    assert_eq!(cloned, copied);
    assert_eq!(copied, TerminalStrategy::FirstPick);
}
