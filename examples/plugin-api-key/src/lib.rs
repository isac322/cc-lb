use std::collections::HashMap;
use std::fmt::Write as _;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::sync::OnceLock;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extism_pdk::{Error, FnResult, Json, WithReturnCode, config, plugin_fn};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

const SIGNER_FACTORY_REF: &str = "anthropic-key";
const DEFAULT_REQUESTS_PER_WINDOW: u64 = 1_000;
const DEFAULT_INPUT_TOKENS_PER_WINDOW: u64 = 1_000_000;
const DEFAULT_OUTPUT_TOKENS_PER_WINDOW: u64 = 200_000;
const DEFAULT_WINDOW_MS: u64 = 60_000;

static CONFIG: OnceLock<Result<PluginConfig, String>> = OnceLock::new();

type KeyMap = HashMap<String, String, BuildHasherDefault<DefaultHasher>>;

#[derive(Clone, Debug)]
struct PluginConfig {
    keys: KeyMap,
    allow_sha256_fallback: bool,
}

#[derive(Debug, Deserialize)]
struct AuthnRequest {
    #[serde(rename = "_version")]
    version: u64,
    request: RequestContext,
}

#[derive(Debug, Deserialize)]
struct RequestContext {
    #[serde(default, rename = "headers")]
    downstream_headers: Vec<HeaderWire>,
    method: String,
    path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HeaderWire {
    name: String,
    value_base64: String,
}

#[derive(Debug, Serialize)]
struct AuthnOutcome {
    #[serde(rename = "_version")]
    version: u64,
    principal: Principal,
    quotas: PrincipalQuotas,
    signer_factory_ref: String,
    signer_state: SignerState,
}

#[derive(Debug, Serialize)]
struct Principal {
    id: String,
    kind: PrincipalKind,
    claims: Map<String, Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum PrincipalKind {
    ApiKey,
}

#[derive(Debug, Serialize)]
struct PrincipalQuotas {
    requests_per_window: u64,
    input_tokens_per_window: u64,
    output_tokens_per_window: u64,
    window_ms: u64,
    allowed_models: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SignerState {
    api_key: String,
    signer_factory_ref: String,
}

#[derive(Debug, Deserialize)]
struct BuildSignerRequest {
    #[serde(rename = "_version")]
    version: u64,
    factory_state: SignerState,
}

#[derive(Debug, Serialize)]
struct BuildSignerResponse {
    #[serde(rename = "_version")]
    version: u64,
    signer_state: SignerState,
}

#[derive(Debug, Deserialize)]
struct SignRequest {
    #[serde(rename = "_version")]
    version: u64,
    shaped: ShapedRequestWire,
    signer_state: SignerState,
}

#[derive(Debug, Deserialize)]
struct ShapedRequestWire {
    #[serde(default)]
    headers: Vec<HeaderWire>,
}

#[derive(Debug, Serialize)]
struct SignResponse {
    #[serde(rename = "_version")]
    version: u64,
    headers: Vec<HeaderWire>,
}

#[plugin_fn]
pub fn authenticate(Json(input): Json<AuthnRequest>) -> FnResult<Json<AuthnOutcome>> {
    ensure_version(input.version)?;
    let _request_shape = (&input.request.method, &input.request.path);
    let api_key = extract_api_key(&input.request.downstream_headers)
        .ok_or_else(|| plugin_error("no api key"))?;
    let config = plugin_config()?;

    if let Some(principal_id) = principal_for_key(&config.keys, &api_key) {
        return Ok(Json(authn_outcome(principal_id, api_key)));
    }

    if config.allow_sha256_fallback {
        return Ok(Json(authn_outcome(hash_principal_id(&api_key), api_key)));
    }

    Err(plugin_error("unknown api key"))
}

#[plugin_fn]
pub fn build_signer(Json(input): Json<BuildSignerRequest>) -> FnResult<Json<BuildSignerResponse>> {
    ensure_version(input.version)?;
    Ok(Json(BuildSignerResponse {
        version: 1,
        signer_state: input.factory_state,
    }))
}

#[plugin_fn]
pub fn sign(Json(input): Json<SignRequest>) -> FnResult<Json<SignResponse>> {
    ensure_version(input.version)?;
    Ok(Json(SignResponse {
        version: 1,
        headers: signed_headers(input.shaped.headers, &input.signer_state),
    }))
}

fn plugin_config() -> FnResult<&'static PluginConfig> {
    let result = CONFIG.get_or_init(load_plugin_config);
    match result {
        Ok(config) => Ok(config),
        Err(reason) => Err(plugin_error(reason)),
    }
}

fn load_plugin_config() -> Result<PluginConfig, String> {
    let keys_json = config::get("keys").unwrap_or_else(|| "{}".to_owned());
    let keys = serde_json::from_str::<KeyMap>(&keys_json)
        .map_err(|source| format!("invalid keys config: {source}"))?;
    let allow_sha256_fallback = config::get("allow_sha256_fallback")
        .map(|value| matches!(value.as_str(), "true" | "1" | "yes"))
        .unwrap_or(false);
    Ok(PluginConfig {
        keys,
        allow_sha256_fallback,
    })
}

fn extract_api_key(headers: &[HeaderWire]) -> Option<String> {
    header_value(headers, "x-api-key")
        .filter(|value| !value.trim().is_empty())
        .or_else(|| bearer_token(headers))
}

fn bearer_token(headers: &[HeaderWire]) -> Option<String> {
    header_value(headers, "authorization")
        .and_then(|value| value.strip_prefix("Bearer ").map(ToOwned::to_owned))
        .filter(|value| !value.trim().is_empty())
}

fn header_value(headers: &[HeaderWire], name: &str) -> Option<String> {
    let needle = name.to_ascii_lowercase();
    headers
        .iter()
        .find(|header| header.name.to_ascii_lowercase() == needle)
        .and_then(|header| BASE64.decode(&header.value_base64).ok())
        .and_then(|value| String::from_utf8(value).ok())
}

fn principal_for_key(keys: &KeyMap, api_key: &str) -> Option<String> {
    keys.iter().find_map(|(principal_id, configured_key)| {
        (configured_key == api_key).then(|| principal_id.clone())
    })
}

fn authn_outcome(principal_id: String, api_key: String) -> AuthnOutcome {
    AuthnOutcome {
        version: 1,
        principal: Principal {
            id: principal_id,
            kind: PrincipalKind::ApiKey,
            claims: Map::new(),
        },
        quotas: default_quotas(),
        signer_factory_ref: SIGNER_FACTORY_REF.to_owned(),
        signer_state: SignerState {
            api_key,
            signer_factory_ref: SIGNER_FACTORY_REF.to_owned(),
        },
    }
}

fn default_quotas() -> PrincipalQuotas {
    PrincipalQuotas {
        requests_per_window: DEFAULT_REQUESTS_PER_WINDOW,
        input_tokens_per_window: DEFAULT_INPUT_TOKENS_PER_WINDOW,
        output_tokens_per_window: DEFAULT_OUTPUT_TOKENS_PER_WINDOW,
        window_ms: DEFAULT_WINDOW_MS,
        allowed_models: Vec::new(),
    }
}

fn hash_principal_id(api_key: &str) -> String {
    let digest = Sha256::digest(api_key.as_bytes());
    let mut out = String::with_capacity(8);
    for byte in digest.iter().take(4) {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn signed_headers(headers: Vec<HeaderWire>, signer_state: &SignerState) -> Vec<HeaderWire> {
    let _factory_ref = &signer_state.signer_factory_ref;
    let signed_header = HeaderWire {
        name: "x-api-key".to_owned(),
        value_base64: BASE64.encode(signer_state.api_key.as_bytes()),
    };
    let mut replaced = false;
    let mut out = Vec::with_capacity(headers.len().saturating_add(1));
    for header in headers {
        if header.name.eq_ignore_ascii_case("x-api-key") {
            if !replaced {
                out.push(signed_header.clone());
                replaced = true;
            }
        } else {
            out.push(header);
        }
    }
    if !replaced {
        out.push(signed_header);
    }
    out
}

fn ensure_version(version: u64) -> FnResult<()> {
    if version == 1 {
        Ok(())
    } else {
        Err(plugin_error("unsupported envelope version"))
    }
}

fn plugin_error(message: impl Into<String>) -> WithReturnCode {
    WithReturnCode::new(Error::msg(message.into()), 1)
}
