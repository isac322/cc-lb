use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::sync::OnceLock;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extism_pdk::{Error, FnResult, Json, WithReturnCode, config, plugin_fn};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

const TOKEN_PREFIX: &str = "sk-ant-oat01-";
const SIGNER_FACTORY_REF: &str = "anthropic-oauth";
const DEFAULT_REQUESTS_PER_WINDOW: u64 = 5_000;
const DEFAULT_INPUT_TOKENS_PER_WINDOW: u64 = 5_000_000;
const DEFAULT_OUTPUT_TOKENS_PER_WINDOW: u64 = 1_000_000;
const DEFAULT_WINDOW_MS: u64 = 60_000;

static CONFIG: OnceLock<Result<PluginConfig, String>> = OnceLock::new();

type PrincipalMap = HashMap<String, PrincipalConfig, BuildHasherDefault<DefaultHasher>>;

#[derive(Clone, Debug, Deserialize)]
struct PluginConfig {
    client_id: String,
    principals: PrincipalMap,
}

#[derive(Clone, Debug, Deserialize)]
struct PrincipalConfig {
    refresh_token_storage_key: String,
    token_prefix: Option<String>,
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
    SubscriptionBearer,
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
    if !bearer.starts_with(TOKEN_PREFIX) {
        return Err(plugin_error(
            "Unauthenticated: invalid Anthropic OAuth token prefix",
        ));
    }

    let config = plugin_config()?;
    let _client_id = &config.client_id;
    let (principal_id, principal_config) =
        principal_for_token(config, &bearer, &input.request.downstream_headers)?;

    Ok(Json(authn_outcome(principal_id, principal_config)))
}

fn plugin_config() -> FnResult<&'static PluginConfig> {
    match CONFIG.get_or_init(load_plugin_config) {
        Ok(config) => Ok(config),
        Err(reason) => Err(plugin_error(reason)),
    }
}

fn load_plugin_config() -> Result<PluginConfig, String> {
    let config_json = config::get("config").unwrap_or_else(|| {
        let client_id = config::get("client_id").unwrap_or_default();
        let principals = config::get("principals").unwrap_or_else(|| "{}".to_owned());
        format!(
            r#"{{"client_id":{},"principals":{}}}"#,
            json_string(&client_id),
            principals
        )
    });
    let parsed = serde_json::from_str::<PluginConfig>(&config_json)
        .map_err(|source| format!("invalid oauth plugin config: {source}"))?;
    if parsed.client_id.trim().is_empty() {
        return Err("invalid oauth plugin config: client_id is required".to_owned());
    }
    if parsed.principals.is_empty() {
        return Err("invalid oauth plugin config: principals is required".to_owned());
    }
    Ok(parsed)
}

fn principal_for_token<'a>(
    config: &'a PluginConfig,
    bearer: &str,
    headers: &[HeaderWire],
) -> FnResult<(String, &'a PrincipalConfig)> {
    let mut matches = config
        .principals
        .iter()
        .filter_map(|(principal_id, principal)| {
            principal
                .token_prefix
                .as_ref()
                .filter(|prefix| bearer.starts_with(prefix.as_str()))
                .map(|_| (principal_id.clone(), principal))
        });

    if let Some(first) = matches.next() {
        if matches.next().is_some() {
            return Err(plugin_error("ambiguous principal mapping"));
        }
        return Ok(first);
    }

    if config
        .principals
        .values()
        .any(|principal| principal.token_prefix.is_some())
    {
        return Err(plugin_error(
            "Unauthenticated: unknown Anthropic OAuth bearer",
        ));
    }

    let principal_id = header_value(headers, "cc-lb-principal-id")
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| plugin_error("ambiguous principal mapping"))?;
    let principal = config
        .principals
        .get(&principal_id)
        .ok_or_else(|| plugin_error("Unauthenticated: unknown principal mapping"))?;
    Ok((principal_id, principal))
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

fn authn_outcome(principal_id: String, principal_config: &PrincipalConfig) -> AuthnOutcome {
    let mut claims = Map::new();
    claims.insert(
        "refresh_token_storage_key".to_owned(),
        Value::String(principal_config.refresh_token_storage_key.clone()),
    );
    AuthnOutcome {
        version: 1,
        principal: Principal {
            id: principal_id,
            kind: PrincipalKind::SubscriptionBearer,
            claims,
        },
        quotas: default_quotas(),
        signer_factory_ref: SIGNER_FACTORY_REF.to_owned(),
        signer_state: SignerState {
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

fn ensure_version(version: u64) -> FnResult<()> {
    if version == 1 {
        Ok(())
    } else {
        Err(plugin_error("unsupported envelope version"))
    }
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}

fn plugin_error(message: impl Into<String>) -> WithReturnCode {
    WithReturnCode::new(Error::msg(message.into()), 1)
}
