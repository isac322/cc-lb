use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::sync::OnceLock;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extism_pdk::{Error, FnResult, Json, WithReturnCode, config, plugin_fn};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

const INTERNAL_TOKEN_PREFIX: &str = "ck-internal-";
const DEFAULT_REQUESTS_PER_WINDOW: u64 = 1_000;
const DEFAULT_INPUT_TOKENS_PER_WINDOW: u64 = 1_000_000;
const DEFAULT_OUTPUT_TOKENS_PER_WINDOW: u64 = 200_000;
const DEFAULT_WINDOW_MS: u64 = 60_000;

static TOKENS: OnceLock<Result<TokenMap, String>> = OnceLock::new();

type TokenMap = HashMap<String, TokenMapping, BuildHasherDefault<DefaultHasher>>;

#[derive(Clone, Debug, Deserialize)]
struct TokenMapping {
    principal_id: String,
    real_credential_kind: String,
    real_credential_storage_key: String,
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
    InternalKey,
}

#[derive(Debug, Serialize)]
struct PrincipalQuotas {
    requests_per_window: u64,
    input_tokens_per_window: u64,
    output_tokens_per_window: u64,
    window_ms: u64,
    allowed_models: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SignerState {
    signer_factory_ref: String,
}

#[plugin_fn]
pub fn authenticate(Json(input): Json<AuthnRequest>) -> FnResult<Json<AuthnOutcome>> {
    ensure_version(input.version)?;
    let _request_shape = (&input.request.method, &input.request.path);
    let bearer = bearer_token(&input.request.downstream_headers)
        .ok_or_else(|| plugin_error("Unauthenticated: missing bearer token"))?;

    if !bearer.starts_with(INTERNAL_TOKEN_PREFIX) {
        return Err(plugin_error("Unauthenticated: not an internal token"));
    }

    let tokens = token_map()?;
    let mapping = tokens
        .get(&bearer)
        .ok_or_else(|| plugin_error("Unauthenticated: unknown internal token"))?;
    let signer_ref = signer_factory_ref(&mapping.real_credential_kind)?;
    Ok(Json(authn_outcome(mapping, signer_ref)))
}

fn token_map() -> FnResult<&'static TokenMap> {
    match TOKENS.get_or_init(load_token_map) {
        Ok(tokens) => Ok(tokens),
        Err(reason) => Err(plugin_error(reason.clone())),
    }
}

fn load_token_map() -> Result<TokenMap, String> {
    let tokens_json = config::get("tokens")
        .ok_or_else(|| "invalid internal-key plugin config: tokens is required".to_owned())?;
    let tokens = serde_json::from_str::<TokenMap>(&tokens_json)
        .map_err(|source| format!("invalid internal-key plugin config: {source}"))?;
    if tokens.is_empty() {
        return Err("invalid internal-key plugin config: tokens is required".to_owned());
    }
    for mapping in tokens.values() {
        if mapping.principal_id.trim().is_empty()
            || mapping.real_credential_kind.trim().is_empty()
            || mapping.real_credential_storage_key.trim().is_empty()
        {
            return Err(
                "invalid internal-key plugin config: token mappings require principal_id, real_credential_kind, and real_credential_storage_key".to_owned(),
            );
        }
    }
    Ok(tokens)
}

fn bearer_token(headers: &[HeaderWire]) -> Option<String> {
    header_value(headers, "authorization")
        .and_then(|value| strip_bearer_prefix(&value))
        .filter(|value| !value.trim().is_empty())
}

fn strip_bearer_prefix(value: &str) -> Option<String> {
    let prefix = "Bearer ";
    let lower = value.to_ascii_lowercase();
    if lower.starts_with("bearer ") {
        Some(value[prefix.len()..].to_owned())
    } else {
        None
    }
}

fn header_value(headers: &[HeaderWire], name: &str) -> Option<String> {
    let needle = name.to_ascii_lowercase();
    headers
        .iter()
        .find(|header| header.name.to_ascii_lowercase() == needle)
        .and_then(|header| BASE64.decode(&header.value_base64).ok())
        .and_then(|value| String::from_utf8(value).ok())
}

fn signer_factory_ref(kind: &str) -> FnResult<&'static str> {
    match kind {
        "anthropic_api_key" => Ok("anthropic-key"),
        "anthropic_oauth" => Ok("anthropic-oauth"),
        "aws_sigv4" => Ok("aws-sigv4"),
        "gcp_oauth" => Ok("gcp-oauth"),
        _ => Err(plugin_error("unsupported real_credential_kind")),
    }
}

fn authn_outcome(mapping: &TokenMapping, signer_ref: &str) -> AuthnOutcome {
    let mut claims = Map::new();
    claims.insert(
        "real_credential_storage_key".to_owned(),
        Value::String(mapping.real_credential_storage_key.clone()),
    );
    claims.insert(
        "real_credential_kind".to_owned(),
        Value::String(mapping.real_credential_kind.clone()),
    );

    AuthnOutcome {
        version: 1,
        principal: Principal {
            id: mapping.principal_id.clone(),
            kind: PrincipalKind::InternalKey,
            claims,
        },
        quotas: default_quotas(),
        signer_factory_ref: signer_ref.to_owned(),
        signer_state: SignerState {
            signer_factory_ref: signer_ref.to_owned(),
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
