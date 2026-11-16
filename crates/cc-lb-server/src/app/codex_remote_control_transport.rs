use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{body::Body, extract::{FromRequestParts, Path, ws::WebSocketUpgrade}};
use cc_lb_clock::unix_secs;
use cc_lb_control::api_keys::session_token;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_engine::{Authenticated, LifecycleContext};
use cc_lb_engine::codex_remote_control::{RemoteControlObservation, RemoteControlWebSocketTermination};
use cc_lb_storage_api::{
    AccessRelaySelector, IssueRelayEnrollment, OwnerBindingExpectation,
    RefreshRelayEnrollment, RelayCredential, RelayDispatch, RelayEnrollment, RelayEnrollmentIdentity,
    RelayEnrollmentMetadata, UpstreamKind, UpstreamRecord,
};
use cc_lb_upstream::{DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, UpstreamDialect, shape_request, sign_request};
use http::{HeaderMap, HeaderValue, Response, StatusCode, request::Parts};
use http_body_util::BodyExt;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use zeroize::{Zeroize, Zeroizing};

use super::{Operation, ValidatedBody, local_error, nonempty_string, single_header};
use super::super::ProxyState;

const IO_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) async fn dispatch(
    state: &ProxyState,
    auth: &Authenticated,
    operation: Operation,
    parts: &mut Parts,
    body: ValidatedBody,
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    let mut observation = None;
    match dispatch_checked(state, auth, operation, parts, body, observer, &mut observation).await {
        Ok(response) => {
            if let Some(observation) = observation.take() {
                let (mut parts, body) = response.into_parts();
                parts.extensions.remove::<cc_lb_engine::TerminalClassification>();
                return Response::from_parts(parts, observation.wrap_body(body));
            }
            response
        }
        Err(error) => {
            let transport_failure = matches!(&error, Error::Transport);
            let local_observer = if observation.is_none() { observer } else { None };
            let mut response = match error {
                Error::Session(error) => {
                    let mut response = cc_lb_engine::codex_sessions::session_error_response(error, local_observer);
                    if matches!(operation, Operation::Pair | Operation::PairStatus | Operation::Server)
                        && matches!(response.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
                    {
                        response.headers_mut().insert("cc-lb-error-origin", HeaderValue::from_static("relay"));
                        response.headers_mut().remove("cc-lb-auth-reason");
                    }
                    response
                }
                Error::Invalid => local_error(StatusCode::BAD_REQUEST, "invalid_remote_control_request", "remote control identity does not match enrollment", local_observer),
                Error::Affinity => local_error(StatusCode::SERVICE_UNAVAILABLE, "upstream_affinity_unavailable", "creating remote control owner is unavailable", local_observer),
                Error::Transport => local_error(StatusCode::BAD_GATEWAY, "remote_control_transport_failed", "remote control provider transport failed", local_observer),
                Error::ProviderExpired => local_error(StatusCode::SERVICE_UNAVAILABLE, "remote_control_provider_expired", "remote control provider enrollment expired", local_observer),
            };
            if let Some(observation) = observation.take() {
                response.extensions_mut().remove::<cc_lb_engine::TerminalClassification>();
                if transport_failure { observation.complete_transport_failure(); }
                else { observation.complete_local(response.status()); }
            }
            response
        }
    }
}

#[derive(Debug)]
pub(super) enum Error {
    Session(cc_lb_engine::codex_sessions::SessionError),
    Invalid,
    Affinity,
    Transport,
    ProviderExpired,
}

impl From<cc_lb_storage_api::codex_sessions::SessionStoreError> for Error {
    fn from(error: cc_lb_storage_api::codex_sessions::SessionStoreError) -> Self {
        match error {
            cc_lb_storage_api::codex_sessions::SessionStoreError::OwnerConflict
            | cc_lb_storage_api::codex_sessions::SessionStoreError::Storage(_) => Self::Affinity,
            other => Self::Session(other.into()),
        }
    }
}

fn owner_error(error: cc_lb_engine::codex_sessions::SessionError) -> Error {
    match error {
        cc_lb_engine::codex_sessions::SessionError::OwnerConflict
        | cc_lb_engine::codex_sessions::SessionError::Unavailable => Error::Affinity,
        other => Error::Session(other),
    }
}

pub(super) fn same_proof(expected: &Authenticated, current: &Authenticated) -> bool {
    expected.principal_id() == current.principal_id()
        && expected.key_id() == current.key_id()
        && expected.session() == current.session()
        && expected.relay().map(|target| (&target.enrollment_id, &target.server_id, &target.environment_id, &target.installation_id))
            == current.relay().map(|target| (&target.enrollment_id, &target.server_id, &target.environment_id, &target.installation_id))
}

async fn dispatch_checked(
    state: &ProxyState,
    auth: &Authenticated,
    operation: Operation,
    parts: &mut Parts,
    body: ValidatedBody,
    observer: Option<&LifecycleContext>,
    observation: &mut Option<RemoteControlObservation>,
) -> Result<Response<Body>, Error> {
    let ValidatedBody { bytes: body, value } = body;
    let snapshot = auth.session().ok_or(Error::Invalid)?;
    let now = unix_secs(state.clock.now());
    let mut previous = None;
    let mut relay = None;
    let binding = if matches!(operation, Operation::Pair | Operation::PairStatus | Operation::Server) {
        let enrollment = fenced_relay(state, auth, now).await?;
        if let Some(request) = value.as_ref() {
            for (field, expected) in [
                ("family_id", snapshot.family_id.as_str()),
                ("enrollment_id", enrollment.enrollment_id.as_str()),
                ("server_id", enrollment.identity.server_id.as_str()),
                ("environment_id", enrollment.identity.environment_id.as_str()),
                ("installation_id", enrollment.identity.installation_id.as_str()),
            ] {
                if request.get(field).is_some_and(|value| value.as_str() != Some(expected)) {
                    return Err(Error::Invalid);
                }
            }
        }
        if operation == Operation::Server {
            if single_header(&parts.headers, "x-codex-server-id").map_err(|_| Error::Invalid)? != enrollment.identity.server_id
                || single_header(&parts.headers, "x-codex-installation-id").map_err(|_| Error::Invalid)? != enrollment.identity.installation_id {
                return Err(Error::Invalid);
            }
        }
        let binding = state.storage.resolve_owner_binding(&OwnerBindingExpectation {
            binding_id: snapshot.owner_binding_id.clone(), principal_id: auth.principal_id().to_owned(),
            provider: snapshot.provider.clone(), owner_generation: snapshot.owner_generation,
            binding_revision: snapshot.binding_revision, workspace_id: snapshot.workspace_id.clone(),
        }).await.map_err(|_| Error::Affinity)?;
        relay = Some(enrollment);
        binding
    } else {
        let binding = state.codex_sessions.fence_session_dispatch(auth).await.map_err(owner_error)?;
        let selector = match operation {
            Operation::Refresh => {
                let request = value.as_ref().ok_or(Error::Invalid)?;
                Some(AccessRelaySelector::Server {
                    server_id: nonempty_string(&request, "server_id").map_err(|_| Error::Invalid)?.to_owned(),
                    installation_id: nonempty_string(&request, "installation_id").map_err(|_| Error::Invalid)?.to_owned(),
                })
            }
            Operation::Clients | Operation::RevokeClient => {
                let Path(parameters) = Path::<HashMap<String, String>>::from_request_parts(parts, state).await.map_err(|_| Error::Invalid)?;
                Some(AccessRelaySelector::Environment {
                    environment_id: parameters.get("environment_id").filter(|value| !value.is_empty()).ok_or(Error::Invalid)?.clone(),
                })
            }
            _ => None,
        };
        if let Some(selector) = selector {
            previous = Some(match state.storage.lookup_access_relay_enrollment(auth.principal_id(), snapshot, &selector, now).await {
                Ok(enrollment) => enrollment,
                Err(cc_lb_storage_api::codex_sessions::SessionStoreError::OwnerConflict) => {
                    state.codex_sessions.fence_session_dispatch(auth).await.map_err(owner_error)?;
                    return Err(Error::Invalid);
                }
                Err(error) => return Err(error.into()),
            });
        }
        binding
    };
    let view = state.dynamic_view.load();
    let record = view.upstreams_snapshot().iter().find(|record| record.id == binding.upstream_id
        && record.enabled && record.kind == UpstreamKind::OpenaiOauth).ok_or(Error::Affinity)?;
    let url = provider_url(record, parts.uri.path(), parts.uri.query())?;
    let context = DialectShapeContext {
        request_id: uuid::Uuid::new_v4().to_string(), downstream_headers: outbound_headers(&parts.headers),
        method: parts.method.clone(), path: parts.uri.path().to_owned(), query: parts.uri.query().map(str::to_owned), body_bytes: body,
    };
    let api = Upstream::OpenaiDirect { base_url: record.base_url.clone() };
    let principal = Principal { id: auth.principal_id().to_owned(), kind: PrincipalKind::OAuthSubject };
    let shaped = shape_request(&RemoteDialect { url }, &context, &api, &principal).map_err(|_| Error::Transport)?;
    let signer: Arc<dyn cc_lb_upstream::Signer> = if let Some(enrollment) = relay.as_ref() {
        let plaintext = Zeroizing::new(state.aead.decrypt(&enrollment.vendor_ciphertext,
            &enrollment_aad(auth.principal_id(), &enrollment.enrollment_id, &enrollment.identity, enrollment.generation)).map_err(|_| Error::Affinity)?);
        let token = std::str::from_utf8(&plaintext).map_err(|_| Error::Affinity)?;
        Arc::new(cc_lb_signer_anthropic_key::OpenaiSubscriptionSigner::new(token, None,
            Some(enrollment.provider_expires_at_unix_secs), state.clock.clone()).map_err(|_| Error::Affinity)?)
    } else {
        // Reuse the server's account signer, but pin its ChatGPT workspace to
        // the binding, not an incoming header or a mutable client selector.
        let encrypted = record.oauth_credentials.as_ref().ok_or(Error::Affinity)?;
        let mut tokens = encrypted.decrypt(&state.aead, record.id.as_bytes()).map_err(|_| Error::Affinity)?;
        tokens.account_id = Some(binding.provider_workspace_id.clone());
        let signer = crate::dynamic_view_builder::build_subscription_signer(record, &tokens, state.clock.clone(), None, None);
        tokens.access_token.zeroize();
        tokens.refresh_token.zeroize();
        signer.map_err(|_| Error::Affinity)?
    };
    let signed = sign_request(signer.as_ref(), shaped).await.map_err(|_| Error::Affinity)?;
    // Recheck live authority after signer construction and immediately before
    // the one owner-affine send. Never retry/fail over this request.
    let current_auth = state.lifecycle.authenticate(&parts.headers).await
        .map_err(|error| Error::Session(cc_lb_engine::codex_sessions::SessionError::Unauthenticated(error)))?;
    if !same_proof(auth, &current_auth) { return Err(Error::Invalid); }
    if relay.is_some() {
        let current = fenced_relay(state, auth, unix_secs(state.clock.now())).await?;
        if current.enrollment_id != relay.as_ref().unwrap().enrollment_id
            || current.generation != relay.as_ref().unwrap().generation { return Err(Error::Affinity); }
    } else {
        let current = state.codex_sessions.fence_session_dispatch(auth).await.map_err(owner_error)?;
        if current != binding { return Err(Error::Affinity); }
    }
    if operation == Operation::Server {
        return websocket(state, auth, parts, signed, record,
            relay.as_ref().ok_or(Error::Invalid)?.generation, observer, observation).await;
    }
    *observation = Some(RemoteControlObservation::begin(auth, observer, record, &state.clock));
    let response = tokio::time::timeout(IO_TIMEOUT, state.remote_control_dispatch.dispatch(signed)).await
        .map_err(|_| Error::Transport)?.map_err(|_| Error::Transport)?;
    if let Some(observation) = observation.as_mut() {
        observation.response_started(response.status(), response.headers());
    }
    if operation == Operation::Pair && response.status().is_success() {
        #[derive(Deserialize)]
        struct PairIdentity { server_id: String, environment_id: String }
        let (mut response_parts, response_body) = response.into_parts();
        let response_body = read_secret_body(response_body, state.body_caps.messages).await?;
        let identity: PairIdentity = sonic_rs::from_slice(&response_body).map_err(|_| Error::Transport)?;
        let enrolled = relay.as_ref().ok_or(Error::Invalid)?;
        if identity.server_id != enrolled.identity.server_id || identity.environment_id != enrolled.identity.environment_id {
            return Err(Error::Transport);
        }
        cc_lb_engine::codex_sessions::mark_upstream_response(&mut response_parts.headers);
        response_parts.headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return Ok(Response::from_parts(response_parts, Body::from(response_body.to_vec())));
    }
    if !response.status().is_success() || !matches!(operation, Operation::Enroll | Operation::Refresh) {
        let (mut response_parts, response_body) = response.into_parts();
        cc_lb_engine::codex_sessions::mark_upstream_response(&mut response_parts.headers);
        response_parts.headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return Ok(Response::from_parts(response_parts, response_body));
    }
    let status = response.status();
    let provider_body = read_secret_body(response.into_body(), state.body_caps.messages).await?;
    enroll_response(state, auth, operation, value.as_ref().ok_or(Error::Invalid)?, &parts.headers, &provider_body, previous.as_ref(), status).await
}

pub(super) async fn fenced_relay(state: &ProxyState, auth: &Authenticated, now: u64) -> Result<RelayEnrollment, Error> {
    let snapshot = auth.session().ok_or(Error::Invalid)?;
    let target = auth.relay().ok_or(Error::Invalid)?;
    match state.storage.fence_relay_dispatch(auth.principal_id(), snapshot, now).await? {
        RelayDispatch::ProviderExpired => Err(Error::ProviderExpired),
        RelayDispatch::Authorized(enrollment) => {
            if target.enrollment_id != enrollment.enrollment_id || target.server_id != enrollment.identity.server_id
                || target.environment_id != enrollment.identity.environment_id || target.installation_id != enrollment.identity.installation_id {
                return Err(Error::Invalid);
            }
            Ok(enrollment)
        }
    }
}

fn outbound_headers(incoming: &HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for field in ["accept", "content-type", "x-codex-server-id", "x-codex-installation-id", "x-codex-name",
        "x-codex-protocol-version", "x-codex-subscribe-cursor", "x-codex-host-device-kind"] {
        if let Some(value) = incoming.get(field) { headers.insert(field, value.clone()); }
    }
    headers
}

fn provider_url(record: &UpstreamRecord, path: &str, query: Option<&str>) -> Result<url::Url, Error> {
    let mut url = record.base_url.clone().unwrap_or_else(|| url::Url::parse("https://chatgpt.com/backend-api/codex").expect("existing ChatGPT origin"));
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some()
        || !(url.scheme() == "https" || (url.scheme() == "http" && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")))) {
        return Err(Error::Affinity);
    }
    url.set_path(path);
    if url.path() != path { return Err(Error::Invalid); }
    url.set_query(query);
    url.set_fragment(None);
    Ok(url)
}

struct RemoteDialect { url: url::Url }
impl UpstreamDialect for RemoteDialect {
    fn shape(&self, context: &DialectShapeContext, _: &Upstream, _: &Principal, builder: &mut ShapedRequestBuilder) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(self.url.clone(), context.method.clone(), context.downstream_headers.clone(), context.body_bytes.clone()))
    }
}

fn enrollment_aad(principal: &str, enrollment: &str, identity: &RelayEnrollmentIdentity, generation: u64) -> Vec<u8> {
    let fields = [principal, identity.family_id.as_str(), enrollment, identity.server_id.as_str(), identity.environment_id.as_str(), identity.installation_id.as_str()];
    let domain = b"cc-lb/remote-control/vendor/v1\0";
    let mut aad = Vec::with_capacity(domain.len() + fields.iter().map(|field| 8 + field.len()).sum::<usize>() + 8);
    aad.extend_from_slice(domain);
    for field in fields {
        aad.extend_from_slice(&(field.len() as u64).to_be_bytes());
        aad.extend_from_slice(field.as_bytes());
    }
    aad.extend_from_slice(&generation.to_be_bytes());
    aad
}

async fn read_secret_body(mut body: Body, cap: usize) -> Result<Zeroizing<Vec<u8>>, Error> {
    let mut bytes = Zeroizing::new(Vec::new());
    while let Some(frame) = tokio::time::timeout(IO_TIMEOUT, body.frame()).await.map_err(|_| Error::Transport)? {
        let frame = frame.map_err(|_| Error::Transport)?;
        if let Ok(chunk) = frame.into_data() {
            let fits = bytes.len().saturating_add(chunk.len()) <= cap;
            if fits { bytes.extend_from_slice(&chunk); }
            if let Ok(mut chunk) = chunk.try_into_mut() { chunk.as_mut().zeroize(); }
            if !fits { return Err(Error::Transport); }
        }
    }
    Ok(bytes)
}

#[allow(clippy::too_many_arguments)]
async fn enroll_response(
    state: &ProxyState, auth: &Authenticated, operation: Operation, request: &serde_json::Value, headers: &HeaderMap,
    provider_body: &[u8], previous: Option<&RelayEnrollmentMetadata>, status: StatusCode,
) -> Result<Response<Body>, Error> {
    #[derive(Deserialize)]
    struct ProviderExpiry { expires_at: String }
    let snapshot = auth.session().ok_or(Error::Invalid)?;
    let now = unix_secs(state.clock.now());
    let family = state.storage.get_session_family(&snapshot.family_id).await?.ok_or(Error::Invalid)?;
    let expiry: ProviderExpiry = sonic_rs::from_slice(provider_body).map_err(|_| Error::Transport)?;
    let provider_expiry = chrono::DateTime::parse_from_rfc3339(&expiry.expires_at).map_err(|_| Error::Transport)?.timestamp();
    let provider_expiry = u64::try_from(provider_expiry).map_err(|_| Error::Transport)?;
    let local_expiry = family.expires_at.min(provider_expiry);
    if local_expiry <= now { return Err(Error::ProviderExpired); }
    let minted = session_token::mint_relay();
    let local_token = SecretString::from(minted.plaintext.expose().to_owned());
    let previous_identity = previous.map(|previous| cc_lb_engine::codex_remote_control::EnrollmentIdentity {
        server_id: previous.identity.server_id.clone(), environment_id: previous.identity.environment_id.clone(), installation_id: previous.identity.installation_id.clone(),
    });
    let prepared = cc_lb_engine::codex_remote_control::prepare_enrollment_response(auth,
        cc_lb_engine::codex_remote_control::EnrollmentRequest { body: request,
            installation_id: single_header(headers, "x-codex-installation-id").map_err(|_| Error::Invalid)?, previous: previous_identity.as_ref() },
        provider_body, &local_token, local_expiry, chrono::DateTime::from_timestamp(i64::try_from(now).map_err(|_| Error::Transport)?, 0).ok_or(Error::Transport)?)
        .map_err(|_| Error::Transport)?;
    let enrollment_id = previous.map_or_else(|| uuid::Uuid::new_v4().to_string(), |previous| previous.enrollment_id.clone());
    let generation = previous.map_or(Some(1), |previous| previous.generation.checked_add(1)).ok_or(Error::Affinity)?;
    let identity = RelayEnrollmentIdentity { family_id: snapshot.family_id.clone(), server_id: prepared.identity.server_id,
        environment_id: prepared.identity.environment_id, installation_id: prepared.identity.installation_id };
    let ciphertext = state.aead.encrypt(prepared.provider_token.expose_secret().as_bytes(),
        &enrollment_aad(auth.principal_id(), &enrollment_id, &identity, generation)).map_err(|_| Error::Affinity)?;
    let credential = RelayCredential { credential_id: uuid::Uuid::new_v4().to_string(), enrollment_id: enrollment_id.clone(), identity: identity.clone(),
        enrollment_generation: generation, index_hash: minted.index_hash, verify_hash: minted.verify_hash, salt: minted.secret_salt,
        issued_at_unix_secs: now, expires_at_unix_secs: local_expiry };
    let grant = IssueRelayEnrollment { principal_id: auth.principal_id().to_owned(), access: snapshot.clone(), enrollment_id, identity,
        vendor_ciphertext: ciphertext, provider_expires_at_unix_secs: provider_expiry, credential };
    if operation == Operation::Refresh {
        state.storage.refresh_relay_enrollment(RefreshRelayEnrollment { grant, expected_generation: previous.ok_or(Error::Invalid)?.generation }, now).await?;
    } else {
        state.storage.issue_relay_enrollment(grant, now).await?;
    }
    // Release the one-time local secret only after the atomic persistence fence.
    let mut response = Response::new(Body::from(prepared.response_body.expose_secret().to_owned()));
    *response.status_mut() = status;
    response.headers_mut().insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response.headers_mut().insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

#[allow(clippy::too_many_arguments)]
async fn websocket(state: &ProxyState, auth: &Authenticated, parts: &mut Parts,
    signed: cc_lb_upstream::SignedRequest, upstream: &UpstreamRecord, generation: u64,
    observer: Option<&LifecycleContext>, observation: &mut Option<RemoteControlObservation>,
) -> Result<Response<Body>, Error> {
    let upgrade = WebSocketUpgrade::from_request_parts(parts, state).await.map_err(|_| Error::Invalid)?;
    let mut config = state.websocket_bulkhead_config;
    config.semaphore_permits = config.max_conns_per_upstream;
    let bulkhead = state.websocket_bulkheads.bulkhead(format!("remote-control:{}", upstream.id), config);
    let permit = bulkhead.acquire_owned().await.map_err(|_| Error::Affinity)?;
    let Some(drain_guard) = state.drain_controller.try_track() else { return Err(Error::Affinity); };
    // Admission may wait while another connection holds the permit. Revalidate
    // authority after that wait and before opening the provider socket.
    let admitted_auth = state
        .lifecycle
        .authenticate(&parts.headers)
        .await
        .map_err(|error| {
            Error::Session(cc_lb_engine::codex_sessions::SessionError::Unauthenticated(error))
        })?;
    let admitted_generation = fenced_relay(
        state,
        &admitted_auth,
        unix_secs(state.clock.now()),
    )
    .await?
    .generation;
    if !same_proof(auth, &admitted_auth) || admitted_generation != generation {
        return Err(Error::Affinity);
    }
    *observation = Some(RemoteControlObservation::begin(auth, observer, upstream, &state.clock));
    let connected = tokio::select! {
        _ = state.drain_controller.wait_for_draining() => return Err(Error::Affinity),
        result = state.remote_control_websocket.connect(
            signed, state.body_caps.messages, tokio::time::Instant::now() + IO_TIMEOUT,
            |status, headers| {
                if let Some(observation) = observation.as_mut() { observation.response_started(status, headers); }
            },
        ) => result.map_err(|_| Error::Transport)?,
    };
    let provider = match connected {
        super::handshake::Outcome::Accepted(provider, _) => provider,
        super::handshake::Outcome::Rejected(mut response) => {
            cc_lb_engine::codex_sessions::mark_upstream_response(response.headers_mut());
            response.headers_mut().insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            return Ok(response);
        }
    };
    let current_auth = state.lifecycle.authenticate(&parts.headers).await
        .map_err(|error| Error::Session(cc_lb_engine::codex_sessions::SessionError::Unauthenticated(error)))?;
    if !same_proof(auth, &current_auth)
        || fenced_relay(state, &current_auth, unix_secs(state.clock.now())).await?.generation != generation {
        return Err(Error::Affinity);
    }
    let state = state.clone();
    let auth = auth.clone();
    let auth_headers = parts.headers.clone();
    let observation = observation.take().map(RemoteControlObservation::own_websocket);
    let response = upgrade.max_message_size(state.body_caps.messages).max_frame_size(state.body_caps.messages)
        .on_upgrade(move |downstream| async move {
            let _permit = permit;
            let _drain_guard = drain_guard;
            let reason = super::socket::relay(downstream, provider, &state, &auth, &auth_headers).await;
            if let Some(observation) = observation {
                use super::socket::StopReason;
                let termination = match reason {
                    None => RemoteControlWebSocketTermination::Graceful,
                    Some(StopReason::Authentication) => RemoteControlWebSocketTermination::Authentication,
                    Some(StopReason::OwnerChanged) => RemoteControlWebSocketTermination::OwnerChanged,
                    Some(StopReason::Overloaded) => RemoteControlWebSocketTermination::Overloaded,
                    Some(StopReason::Transport) => RemoteControlWebSocketTermination::Transport,
                    Some(StopReason::WriteTimeout) => RemoteControlWebSocketTermination::WriteTimeout,
                    Some(StopReason::Draining) => RemoteControlWebSocketTermination::Draining,
                    Some(StopReason::DrainTimeout) => RemoteControlWebSocketTermination::DrainTimeout,
                    Some(StopReason::PeerErrorClose(code)) => RemoteControlWebSocketTermination::PeerErrorClose(code),
                };
                observation.complete_websocket(termination);
            }
        });
    Ok(response)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> RelayEnrollmentIdentity {
        RelayEnrollmentIdentity { family_id:"family".into(), server_id:"server".into(),
            environment_id:"environment".into(), installation_id:"installation".into() }
    }

    #[test]
    fn provider_ciphertext_aad_binds_every_identity_and_generation() {
        let aead = cc_lb_aead::AeadService::from_master_key([7;32]);
        let identity = identity();
        let aad = enrollment_aad("principal","enrollment",&identity,1);
        let ciphertext = aead.encrypt(b"synthetic-vendor-token",&aad).unwrap();
        assert!(aead.decrypt(&ciphertext,&aad).is_ok());
        for aad in [
            enrollment_aad("other","enrollment",&identity,1),
            enrollment_aad("principal","other",&identity,1),
            enrollment_aad("principal","enrollment",&identity,2),
            enrollment_aad("principal","enrollment",&RelayEnrollmentIdentity { family_id:"other".into(),..identity.clone() },1),
            enrollment_aad("principal","enrollment",&RelayEnrollmentIdentity { server_id:"other".into(),..identity.clone() },1),
            enrollment_aad("principal","enrollment",&RelayEnrollmentIdentity { environment_id:"other".into(),..identity.clone() },1),
            enrollment_aad("principal","enrollment",&RelayEnrollmentIdentity { installation_id:"other".into(),..identity.clone() },1),
        ] {
            assert!(aead.decrypt(&ciphertext,&aad).is_err());
        }
        assert_ne!(enrollment_aad("ab","c",&identity,1),enrollment_aad("a","bc",&identity,1));
        assert!(aead.decrypt(&ciphertext[..ciphertext.len()-1],&aad).is_err());
    }

    #[test]
    fn remote_target_uses_only_the_selected_owner_origin() {
        let mut record = UpstreamRecord { kind:UpstreamKind::OpenaiOauth,
            base_url:Some(url::Url::parse("https://owner.example:8443/backend-api/codex").unwrap()),
            ..UpstreamRecord::default() };
        let url = provider_url(&record,super::super::ENROLL,Some("cursor=a%2Fb&base=https://attacker.example")).unwrap();
        assert_eq!(url.scheme(),"https");
        assert_eq!(url.host_str(),Some("owner.example"));
        assert_eq!(url.port(),Some(8443));
        assert_eq!(url.path(),super::super::ENROLL);
        assert!(provider_url(&record,"/backend-api/wham/remote/control/environments/environment/clients/%2e%2e",None).is_err());
        let opaque = "/backend-api/wham/remote/control/environments/env%20%2F%3F/clients/client%20%2F%3F";
        assert_eq!(provider_url(&record,opaque,None).unwrap().path(),opaque);
        for base in ["https://user:password@owner.example/backend-api/codex",
            "https://owner.example/backend-api/codex#fragment", "http://remote.example/backend-api/codex"] {
            record.base_url = Some(url::Url::parse(base).unwrap());
            assert!(provider_url(&record,super::super::ENROLL,None).is_err());
        }
    }

    #[test]
    fn captured_stock_headers_survive_without_forwarding_local_credentials() {
        let mut headers = HeaderMap::new();
        let captured = ["x-codex-server-id","x-codex-installation-id","x-codex-name",
            "x-codex-protocol-version","x-codex-subscribe-cursor","x-codex-host-device-kind"];
        for field in captured { headers.insert(field,HeaderValue::from_static("synthetic")); }
        for field in ["authorization","x-api-key","x-goog-api-key","cookie","host",
            "cc-lb-error-origin","cc-lb-auth-reason","chatgpt-account-id","x-openai-fedramp"] {
            headers.insert(field,HeaderValue::from_static("must-not-forward"));
        }
        let outbound = outbound_headers(&headers);
        assert_eq!(outbound.len(),captured.len());
        for field in captured { assert_eq!(outbound[field],"synthetic"); }
    }

    #[tokio::test]
    async fn provider_enrollment_body_is_bounded_before_decode() {
        assert!(read_secret_body(Body::from("12345"),4).await.is_err());
        let bytes = read_secret_body(Body::from("1234"),4).await.unwrap();
        assert_eq!(bytes.as_slice(),b"1234");
    }
}
