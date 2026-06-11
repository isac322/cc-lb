use cc_lb_plugin_wire::guest::run_string_export;
use cc_lb_plugin_wire::handshake::{HandshakeAccept, HandshakeOffer};
use cc_lb_plugin_wire::self_check::{
    SelfCheckFailure, SelfCheckRequest, SelfCheckResponse, SelfCheckStage, SelfCheckStatus,
};
use cc_lb_plugin_wire::serde_json::{self, Value};
use cc_lb_plugin_wire::v2::common::{CacheScoreWire, CandidateWire};
use cc_lb_plugin_wire::v3::filter::{FilterFn, FilterRequest, FilterResponse, PerCandidateReason};
use cc_lb_plugin_wire::wire_function::WireFunction;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use uuid::Uuid;

const DEFAULT_KEEP_K: usize = 1;
const ACCEPT_REASON: &str = "top-K by cache_score";
const REJECT_REASON: &str = "below K by cache_score";
const PLUGIN_METADATA_JSON: &[u8; 103] = b"{\"magic\":[204,27,112,16,0,1,0,0],\"abi_envelope\":1,\"plugin_name\":\"cache-aware\",\"plugin_version\":\"0.1.0\"}";

#[used]
#[unsafe(link_section = "cc_lb.plugin.v1")]
static CC_LB_PLUGIN_METADATA: [u8; PLUGIN_METADATA_JSON.len()] = *PLUGIN_METADATA_JSON;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheAwareConfig {
    keep_k: usize,
}

impl CacheAwareConfig {
    pub fn new(keep_k: usize) -> Self {
        Self {
            keep_k: keep_k.max(DEFAULT_KEEP_K),
        }
    }

    pub fn keep_k(self) -> usize {
        self.keep_k
    }

    pub fn from_keep_k_value(value: Option<&str>) -> Self {
        let keep_k = value
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_KEEP_K);
        Self::new(keep_k)
    }
}

impl Default for CacheAwareConfig {
    fn default() -> Self {
        Self::new(DEFAULT_KEEP_K)
    }
}

pub fn cache_score(cache_score: Option<&CacheScoreWire>) -> u32 {
    match cache_score {
        Some(score) if score.predicted_cache_read_tokens > 0 => 1,
        _ => 0,
    }
}

pub fn predicted_cache_read_tokens(cache_score: Option<&CacheScoreWire>) -> u32 {
    cache_score
        .map(|score| score.predicted_cache_read_tokens)
        .unwrap_or(0)
}

pub fn kept_candidate_indices(candidates: &[CandidateWire], keep_k: usize) -> Vec<usize> {
    let mut ranked: Vec<usize> = (0..candidates.len()).collect();
    ranked.sort_by(|left, right| {
        let left_candidate = &candidates[*left];
        let right_candidate = &candidates[*right];
        cache_score(right_candidate.cache_score.as_ref())
            .cmp(&cache_score(left_candidate.cache_score.as_ref()))
            .then_with(|| {
                predicted_cache_read_tokens(right_candidate.cache_score.as_ref()).cmp(
                    &predicted_cache_read_tokens(left_candidate.cache_score.as_ref()),
                )
            })
    });
    ranked
        .into_iter()
        .take(keep_k.max(DEFAULT_KEEP_K))
        .collect()
}

pub fn filter_candidates(request: FilterRequest, config: CacheAwareConfig) -> FilterResponse {
    let kept_indices = kept_candidate_indices(&request.candidates, config.keep_k());
    let mut keep_mask = vec![false; request.candidates.len()];
    for index in kept_indices {
        keep_mask[index] = true;
    }
    let kept_upstream_ids = request
        .candidates
        .iter()
        .zip(keep_mask.iter())
        .filter(|(_candidate, kept)| **kept)
        .map(|(candidate, _kept)| candidate_uuid(candidate))
        .collect::<Vec<_>>();
    let kept_count = kept_upstream_ids.len();
    let candidate_count = request.candidates.len();

    FilterResponse {
        kept_upstream_ids,
        reason: format!("kept {kept_count} of {candidate_count} by cache_score"),
        per_candidate_reasons: request
            .candidates
            .iter()
            .enumerate()
            .map(|(index, candidate)| candidate_result(candidate, keep_mask[index]))
            .collect(),
    }
}

pub fn filter_handler(request: FilterRequest) -> Result<FilterResponse, Infallible> {
    Ok(filter_candidates(request, host_config()))
}

fn candidate_result(candidate: &CandidateWire, keep: bool) -> PerCandidateReason {
    PerCandidateReason {
        upstream_id: candidate_uuid(candidate),
        kept: keep,
        reason: if keep { ACCEPT_REASON } else { REJECT_REASON }.to_owned(),
    }
}

fn candidate_uuid(candidate: &CandidateWire) -> Uuid {
    Uuid::parse_str(&candidate.upstream_id).expect("host candidate upstream_id must be a UUID")
}

fn host_config() -> CacheAwareConfig {
    CacheAwareConfig::from_keep_k_value(host_keep_k().as_deref())
}

#[cfg(target_arch = "wasm32")]
fn host_keep_k() -> Option<String> {
    extism_pdk::config::get("keep_k").ok().flatten()
}

#[cfg(not(target_arch = "wasm32"))]
fn host_keep_k() -> Option<String> {
    None
}

#[unsafe(no_mangle)]
pub extern "C" fn filter() -> i32 {
    run_string_export(handle_filter_export)
}

#[unsafe(no_mangle)]
pub extern "C" fn cc_lb_handshake() -> i32 {
    run_string_export(handle_handshake_export)
}

#[unsafe(no_mangle)]
pub extern "C" fn cc_lb_self_check() -> i32 {
    run_string_export(handle_self_check_export)
}

fn handle_filter_export(input: String) -> Result<String, String> {
    let envelope: Value = serde_json::from_str(&input).map_err(|error| error.to_string())?;
    let envelope_version = envelope
        .get("_v")
        .and_then(Value::as_u64)
        .ok_or_else(|| "plugin envelope missing numeric _v".to_owned())?;

    match envelope_version {
        1 => {
            let mut payload_envelope = envelope;
            if let Value::Object(object) = &mut payload_envelope {
                object.remove("_v");
            }
            let payload: FilterRequest =
                serde_json::from_value(payload_envelope).map_err(|error| error.to_string())?;
            let result = filter_handler(payload).map_err(|error| format!("{error:?}"))?;
            let mut out = serde_json::to_value(&result).map_err(|error| error.to_string())?;
            out["_v"] = Value::from(1_u64);
            serde_json::to_string(&out).map_err(|error| error.to_string())
        }
        unsupported_version => Err(format!(
            "unsupported plugin envelope _v: {unsupported_version}"
        )),
    }
}

fn handle_handshake_export(input: String) -> Result<String, String> {
    let offer: HandshakeOffer = serde_json::from_str(&input).map_err(|error| error.to_string())?;
    offer.validate().map_err(|error| error.to_string())?;

    let mut plugin_supported = BTreeMap::new();
    plugin_supported.insert(
        FilterFn::NAME.to_owned(),
        FilterFn::SUPPORTED_VERSIONS.to_vec(),
    );

    let implemented_functions = BTreeSet::from([FilterFn::NAME.to_owned()]);
    let required_capabilities = BTreeSet::new();
    let mut chosen_versions = BTreeMap::new();
    if let Some(chosen) = offer
        .function_versions
        .get(FilterFn::NAME)
        .and_then(|offered_versions| {
            offered_versions
                .iter()
                .filter(|version| FilterFn::SUPPORTED_VERSIONS.contains(version))
                .max()
                .copied()
        })
    {
        chosen_versions.insert(FilterFn::NAME.to_owned(), chosen);
    }

    let accept = HandshakeAccept {
        handshake_schema_version: offer.handshake_schema_version,
        envelope_version: offer.envelope_version,
        chosen_versions,
        plugin_supported,
        implemented_functions,
        required_capabilities,
    };
    accept
        .validate_against_offer(&offer)
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&accept).map_err(|error| error.to_string())
}

fn handle_self_check_export(input: String) -> Result<String, String> {
    let request: SelfCheckRequest =
        serde_json::from_str(&input).map_err(|error| error.to_string())?;
    request.validate().map_err(|error| error.to_string())?;

    let mut failures = Vec::new();
    if let Err(message) = filter_wire_roundtrip_check() {
        failures.push(SelfCheckFailure {
            stage: SelfCheckStage::WireFunctionTest,
            message,
        });
    }

    let response = SelfCheckResponse {
        status: if failures.is_empty() {
            SelfCheckStatus::Success
        } else {
            SelfCheckStatus::Failure
        },
        failures,
        completed_at: request.initiated_at,
    };
    response.validate().map_err(|error| error.to_string())?;
    serde_json::to_string(&response).map_err(|error| error.to_string())
}

fn filter_wire_roundtrip_check() -> Result<(), String> {
    let sample = FilterFn::dry_run_request();
    let bytes = serde_json::to_vec(&sample)
        .map_err(|error| format!("filter@1 request serialize failed: {error}"))?;
    let decoded: FilterRequest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("filter@1 request deserialize failed: {error}"))?;
    let bytes = serde_json::to_vec(&decoded)
        .map_err(|error| format!("filter@1 request reserialize failed: {error}"))?;
    let _: <FilterFn as WireFunction>::Request = serde_json::from_slice(&bytes)
        .map_err(|error| format!("filter@1 request wire decode failed: {error}"))?;

    let sample = FilterFn::dry_run_response();
    let bytes = serde_json::to_vec(&sample)
        .map_err(|error| format!("filter@1 response serialize failed: {error}"))?;
    let decoded: FilterResponse = serde_json::from_slice(&bytes)
        .map_err(|error| format!("filter@1 response deserialize failed: {error}"))?;
    let bytes = serde_json::to_vec(&decoded)
        .map_err(|error| format!("filter@1 response reserialize failed: {error}"))?;
    let _: <FilterFn as WireFunction>::Response = serde_json::from_slice(&bytes)
        .map_err(|error| format!("filter@1 response wire decode failed: {error}"))?;

    Ok(())
}
