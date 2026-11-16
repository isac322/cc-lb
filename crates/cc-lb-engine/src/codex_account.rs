//! Projection of an authenticated provider account into the Codex accounts/check wire shape.
//!
//! This module deliberately does not resolve an owner binding or authenticate a request. The
//! caller must perform those operations first and pass the resulting provider workspace id,
//! permitted local alias, and configured proxy origin as [`AccountProjectionMetadata`].

use std::fmt;

use serde::Deserialize;
use serde_json::{Map, Value};
use url::Url;

use crate::Authenticated;

/// Authoritative, post-authentication metadata supplied by the resolved owner binding.
///
/// These values are not read from the upstream response or from client input. This projection
/// only renders them; it does not verify owner permissions or select another workspace.
#[derive(Debug, Clone, Copy)]
pub struct AccountProjectionMetadata<'a> {
    pub provider_workspace_id: &'a str,
    pub local_workspace_alias: &'a str,
    pub proxy_origin: &'a str,
}

/// Safe, non-secret projection failures. Input values are intentionally not included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountProjectionError {
    InvalidMetadata,
    InvalidOrigin,
    InvalidPayload,
    SelectedAccountMissing,
    SelectedAccountDuplicate,
}

impl fmt::Display for AccountProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidMetadata => "invalid account projection metadata",
            Self::InvalidOrigin => "invalid proxy origin",
            Self::InvalidPayload => "invalid accounts response",
            Self::SelectedAccountMissing => "selected account is missing",
            Self::SelectedAccountDuplicate => "selected account is duplicated",
        };
        f.write_str(message)
    }
}

impl std::error::Error for AccountProjectionError {}

#[derive(Debug, Deserialize)]
struct WorkspaceSwitchRequest {
    workspace_id: String,
}

/// Safe, non-secret failures while parsing a workspace switch request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceSwitchRequestError {
    InvalidRequest,
}

impl fmt::Display for WorkspaceSwitchRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid workspace switch request")
    }
}

impl std::error::Error for WorkspaceSwitchRequestError {}

/// Parse the authenticated client's local workspace target.
///
/// Credential and route-purpose admission precede body parsing; target owner authorization is
/// performed by the issuer after parsing and is not repeated here.
/// The returned string is the decoded target without trimming or normalization.
pub fn workspace_switch_target(
    _auth: &Authenticated,
    body: &[u8],
) -> Result<String, WorkspaceSwitchRequestError> {
    let request = sonic_rs::from_slice::<WorkspaceSwitchRequest>(body)
        .map_err(|_| WorkspaceSwitchRequestError::InvalidRequest)?;
    if request.workspace_id.trim().is_empty() {
        return Err(WorkspaceSwitchRequestError::InvalidRequest);
    }
    Ok(request.workspace_id)
}

/// Project one authenticated provider workspace into the canonical Codex list response.
///
/// `upstream` is consumed so unknown provider accounts and fields cannot be accidentally
/// forwarded. The caller owns the authority represented by `metadata`; this helper does not
/// perform owner resolution, entitlement checks, or routing decisions.
pub fn project_codex_accounts(
    _auth: &Authenticated,
    upstream: Value,
    metadata: &AccountProjectionMetadata<'_>,
) -> Result<Value, AccountProjectionError> {
    validate_metadata(metadata)?;
    let origin = metadata.proxy_origin;

    let Value::Object(mut root) = upstream else {
        return Err(AccountProjectionError::InvalidPayload);
    };
    let accounts = root
        .remove("accounts")
        .ok_or(AccountProjectionError::InvalidPayload)?;

    match accounts {
        Value::Array(accounts) => {
            if let Some(ordering) = root.remove("account_ordering") {
                validate_ordering(ordering)?;
            }
            project_list(accounts, metadata, origin)
        }
        Value::Object(accounts) => {
            let ordering = root.remove("account_ordering");
            project_map(accounts, ordering, metadata, origin)
        }
        _ => Err(AccountProjectionError::InvalidPayload),
    }
}

fn validate_ordering(value: Value) -> Result<(), AccountProjectionError> {
    let Value::Array(values) = value else {
        return Err(AccountProjectionError::InvalidPayload);
    };
    for value in values {
        if !value.is_string() {
            return Err(AccountProjectionError::InvalidPayload);
        }
    }
    Ok(())
}

fn validate_metadata(
    metadata: &AccountProjectionMetadata<'_>,
) -> Result<(), AccountProjectionError> {
    if metadata.provider_workspace_id.trim().is_empty()
        || metadata.local_workspace_alias.trim().is_empty()
    {
        return Err(AccountProjectionError::InvalidMetadata);
    }
    validate_origin(metadata.proxy_origin)
}

fn contains_control_or_whitespace(value: &str) -> bool {
    value
        .chars()
        .any(|character| character.is_control() || character.is_whitespace())
}

fn validate_origin(origin: &str) -> Result<(), AccountProjectionError> {
    if origin.is_empty() || contains_control_or_whitespace(origin) || origin.contains('\\') {
        return Err(AccountProjectionError::InvalidOrigin);
    }
    let (scheme, rest) = origin
        .split_once("://")
        .ok_or(AccountProjectionError::InvalidOrigin)?;
    if !scheme.eq_ignore_ascii_case("https") {
        return Err(AccountProjectionError::InvalidOrigin);
    }
    let suffix_start = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let suffix = &rest[suffix_start..];
    if !suffix.is_empty() && suffix != "/" {
        return Err(AccountProjectionError::InvalidOrigin);
    }
    let parsed = Url::parse(origin).map_err(|_| AccountProjectionError::InvalidOrigin)?;
    if parsed.scheme() != "https"
        || parsed.host().is_none()
        || parsed.path() != "" && parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.port_or_known_default().is_none()
    {
        return Err(AccountProjectionError::InvalidOrigin);
    }

    // `Url::username` is empty for `https://@host`; inspect the raw authority so empty userinfo
    // is rejected as well. URL parsing already rejects malformed and out-of-range ports.
    let authority = &rest[..suffix_start];
    if authority.contains('@') {
        return Err(AccountProjectionError::InvalidOrigin);
    }
    Ok(())
}

fn project_list(
    accounts: Vec<Value>,
    metadata: &AccountProjectionMetadata<'_>,
    origin: &str,
) -> Result<Value, AccountProjectionError> {
    let selected_id = metadata.provider_workspace_id;

    let mut selected = None;
    for account in accounts {
        let Value::Object(mut account) = account else {
            return Err(AccountProjectionError::InvalidPayload);
        };
        let id = match account.remove("id") {
            Some(Value::String(id)) => id,
            Some(_) => return Err(AccountProjectionError::InvalidPayload),
            None => return Err(AccountProjectionError::InvalidPayload),
        };
        if id == selected_id {
            if selected.is_some() {
                return Err(AccountProjectionError::SelectedAccountDuplicate);
            }
            selected = Some(account);
        }
    }
    let selected = selected.ok_or(AccountProjectionError::SelectedAccountMissing)?;
    let account = render_account(selected, metadata.local_workspace_alias, origin)?;
    Ok(render_response(account, metadata.local_workspace_alias))
}
fn project_map(
    mut accounts: Map<String, Value>,
    ordering: Option<Value>,
    metadata: &AccountProjectionMetadata<'_>,
    origin: &str,
) -> Result<Value, AccountProjectionError> {
    let ordering = match ordering {
        None => Vec::new(),
        Some(Value::Array(values)) => values,
        Some(_) => return Err(AccountProjectionError::InvalidPayload),
    };
    let selected_id = metadata.provider_workspace_id;

    let mut selected = None;

    for key_value in ordering {
        let key = match key_value {
            Value::String(key) => key,
            _ => return Err(AccountProjectionError::InvalidPayload),
        };
        let mut wrapper = match accounts.remove(&key) {
            None => continue,
            Some(Value::Object(wrapper)) => wrapper,
            Some(_) => return Err(AccountProjectionError::InvalidPayload),
        };
        let nested = match wrapper.remove("account") {
            Some(Value::Object(nested)) => nested,
            None | Some(_) => return Err(AccountProjectionError::InvalidPayload),
        };
        let nested_id = match nested.get("account_id") {
            Some(Value::String(id)) => id,
            Some(Value::Null) | None => continue,
            Some(_) => return Err(AccountProjectionError::InvalidPayload),
        };
        if nested_id != selected_id {
            continue;
        }
        if selected.is_some() {
            return Err(AccountProjectionError::SelectedAccountDuplicate);
        }
        selected = Some((wrapper, nested));
    }

    let (wrapper, nested) = selected.ok_or(AccountProjectionError::SelectedAccountMissing)?;
    let account = render_map_account(wrapper, nested, metadata.local_workspace_alias, origin)?;
    Ok(render_response(account, metadata.local_workspace_alias))
}

fn render_response(account: Value, alias: &str) -> Value {
    let mut root = Map::new();
    root.insert("accounts".to_owned(), Value::Array(vec![account]));
    root.insert(
        "account_ordering".to_owned(),
        Value::Array(vec![Value::String(alias.to_owned())]),
    );
    root.insert(
        "default_account_id".to_owned(),
        Value::String(alias.to_owned()),
    );
    Value::Object(root)
}

fn render_account(
    mut source: Map<String, Value>,
    alias: &str,
    origin: &str,
) -> Result<Value, AccountProjectionError> {
    let mut output = Map::new();
    output.insert("id".to_owned(), Value::String(alias.to_owned()));
    copy_optional_bool(&mut source, &mut output, "can_access_with_session", true)?;
    copy_optional_string(&mut source, &mut output, "sso_connection_name", true)?;
    copy_required_bool(&mut source, &mut output, "is_deactivated")?;
    copy_required_bool(&mut source, &mut output, "is_fedramp_compliant_workspace")?;
    copy_optional_string(&mut source, &mut output, "plan_type", true)?;
    output.insert(
        "workspace_backend_origin".to_owned(),
        Value::String(origin.to_owned()),
    );
    output.insert(
        "account_routing_override".to_owned(),
        Value::String("NO_CONSTRAINT".to_owned()),
    );
    copy_optional_string(&mut source, &mut output, "name", true)?;
    copy_optional_string(&mut source, &mut output, "profile_picture_url", true)?;
    copy_optional_string(&mut source, &mut output, "structure", false)?;
    Ok(Value::Object(output))
}

fn render_map_account(
    mut wrapper: Map<String, Value>,
    mut nested: Map<String, Value>,
    alias: &str,
    origin: &str,
) -> Result<Value, AccountProjectionError> {
    nested.remove("account_id");
    let mut output = Map::new();
    output.insert("id".to_owned(), Value::String(alias.to_owned()));
    copy_optional_bool(&mut wrapper, &mut output, "can_access_with_session", true)?;
    copy_optional_string(&mut wrapper, &mut output, "sso_connection_name", true)?;
    copy_required_bool(&mut nested, &mut output, "is_deactivated")?;
    copy_required_bool(&mut nested, &mut output, "is_fedramp_compliant_workspace")?;
    copy_optional_string(&mut nested, &mut output, "plan_type", true)?;
    output.insert(
        "workspace_backend_origin".to_owned(),
        Value::String(origin.to_owned()),
    );
    output.insert(
        "account_routing_override".to_owned(),
        Value::String("NO_CONSTRAINT".to_owned()),
    );
    copy_optional_string(&mut nested, &mut output, "name", true)?;
    copy_optional_string(&mut nested, &mut output, "profile_picture_url", true)?;
    copy_optional_string(&mut nested, &mut output, "structure", false)?;
    Ok(Value::Object(output))
}

fn copy_required_bool(
    source: &mut Map<String, Value>,
    output: &mut Map<String, Value>,
    key: &str,
) -> Result<(), AccountProjectionError> {
    if let Some((key, value)) = source.remove_entry(key) {
        if !value.is_boolean() {
            return Err(AccountProjectionError::InvalidPayload);
        }
        output.insert(key, value);
    }
    Ok(())
}

fn copy_optional_bool(
    source: &mut Map<String, Value>,
    output: &mut Map<String, Value>,
    key: &str,
    allow_null: bool,
) -> Result<(), AccountProjectionError> {
    if let Some((key, value)) = source.remove_entry(key) {
        if !(value.is_boolean() || (allow_null && value.is_null())) {
            return Err(AccountProjectionError::InvalidPayload);
        }
        output.insert(key, value);
    }
    Ok(())
}

fn copy_optional_string(
    source: &mut Map<String, Value>,
    output: &mut Map<String, Value>,
    key: &str,
    allow_null: bool,
) -> Result<(), AccountProjectionError> {
    if let Some((key, value)) = source.remove_entry(key) {
        if !(value.is_string() || (allow_null && value.is_null())) {
            return Err(AccountProjectionError::InvalidPayload);
        }
        output.insert(key, value);
    }
    Ok(())
}
