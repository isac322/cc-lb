# ADR 0012: Provider-neutral admin identity authentication and audit

## Status

Accepted.

## Context

The admin API currently authenticates every operator with one shared bearer secret. `crates/cc-lb-admin/src/auth.rs::require_admin_auth` compares the request's bearer token with the configured token and forwards the request without attaching an authenticated identity to request extensions.

The audit paths therefore cannot identify the operator. `crates/cc-lb-admin/src/routes.rs::emit_admin_action`, `settings.rs::apply_config`, `v1/keys.rs::emit_key_audit`, `v1/oauth.rs::enqueue_upstream_audit`, `v1/upstreams.rs::enqueue_upstream_audit`, `v1/principals.rs::emit_audit`, `v1/plugins.rs::emit_audit`, `v1/plugins.rs::emit_chain_audit`, `v1/plugins_wasm.rs::enqueue_upload_audit`, and `v1/plugins_wasm.rs::enqueue_upload_attempt_audit` all record `actor: Some("admin")`.

Admin audit delivery also uses machinery designed for the proxy data plane. `crates/cc-lb-control/src/audit_writer.rs::AuditWriterSink::try_enqueue` drops events when its queue is full, and `AuditWriterSink::flush_batch` logs storage failures without returning them to the administrator whose request caused the event. That behavior is appropriate for high-volume data-plane telemetry but not for low-volume administrative accountability.

Sensitive reads lack audit coverage. `routes.rs::query_audit`, `v1/keys.rs::issue_key`, the configuration reads in `settings.rs` and `routes.rs`, request-event detail, key listing, and configuration export can expose security-relevant data without recording who read it. The web client assumes the shared-token model through `web/src/lib/auth.ts` local storage and `AuthRequiredGate`.

We need to accept identities verified by a front-end authentication layer, beginning with Cloudflare Access, without pre-registering users in cc-lb. We must preserve the existing shared token as a break-glass and backward-compatible method. The design must also admit future RFC 9068 bearer-token and cc-lb-owned OIDC login/session providers without changing admin handlers, authorization call sites, audit recording, or web identity display.

## Decision

### 1. Authentication kernel, not a JWT feature

Admin authentication is an `AdminAuthenticator` that maps request credentials to either `AdminIdentity` or `AdminAuthError`. Each authentication method is an in-process built-in provider. A provider validates its protocol and returns its local subject and actor kind. The kernel, not the provider, decides whether the request may enter the admin API.

We will not expose this boundary through the plugin ABI or WASM. Authentication code handles credentials and trust configuration and remains part of the trusted server binary.

### 2. Identity model

Every verified request carries this identity:

```rust
AdminIdentity {
    authority: String,
    subject: String,
    kind: AdminActorKind, // Human, Service, or BreakGlass
    provider_id: String,
    email: Option<String>,
    display_name: Option<String>,
    groups: Vec<String>,
    expires_at_unix_secs: Option<u64>,
}
```

The stable audit key is `(authority, subject)`. `authority` is the verified issuer, such as `https://<team>.cloudflareaccess.com`, or the fixed `static-token` namespace. It is not the configurable provider alias. `email` and `display_name` are display snapshots, not identifiers. We do not automatically link subjects from different authorities by email, consistent with OpenID Connect Core section 5.7.

Cloudflare Access user subjects are scoped to an account and email and may change after account removal, re-addition, or an email change. Audit records preserve the authority, subject, and display fields observed at the time; they do not claim continuity across a subject change.

### 3. Provider registry and entry policy

`[admin.auth]` contains a provider list. The same list may be supplied without a TOML change through `CC_LB_ADMIN_AUTH_PROVIDERS_JSON`, whose value is a JSON array of provider objects. The environment value replaces the TOML provider list when present; malformed JSON, an empty array, or an invalid provider fails startup without falling back to another authentication method. Static-token objects contain only a `token_env` reference; the token itself remains in that separate environment variable. This implementation supplies `static_token` and `cloudflare_access`. The registry reserves `jwt_bearer` and `oidc_session` as future provider categories but does not implement them.

For example:

```sh
export CC_LB_ADMIN_AUTH_PROVIDERS_JSON='[
  {
    "kind": "cloudflare_access",
    "id": "cloudflare",
    "team_domain": "https://team.cloudflareaccess.com",
    "audiences": ["ACCESS_APPLICATION_AUD"]
  },
  {
    "kind": "static_token",
    "id": "break-glass",
    "token_env": "CC_LB_ADMIN_TOKEN"
  }
]'
export CC_LB_ADMIN_TOKEN='...'
```

Configuration precedence is `CC_LB_ADMIN_AUTH_PROVIDERS_JSON`, then TOML `[[admin.auth.providers]]`.

Each provider examines only its configured credential location and returns `NotPresent`, `Verified(AdminIdentity)`, or `Rejected(reason)`. The kernel applies these rules:

- No credential found by any provider: 401.
- Any `Rejected` result: 401 without falling back to another provider.
- More than one `Verified` result: 401 with `ambiguous_actor`.
- Exactly one `Verified` result: continue with that identity.

A future deployment may need one credential as an edge gate and another as the actor identity. Such a "gate + actor" composition must be declared as an explicit compound profile; the kernel will not infer it from multiple credentials. Authentication failures retain the existing 100 ms delay before the 401 response.

### 4. Admission

All verified identities initially receive the same full administrator permission set. The front-end Access application policy and provider configuration define the admitted population.

`crates/cc-lb-admin/src/auth/authorize.rs` owns `AdminAction::{Read, Write, SensitiveRead}` and `authorize(&AdminIdentity, AdminAction)`. It initially returns `Ok(())` for every verified identity. The authentication middleware calls it with `Read` for `GET` and `HEAD`, and `Write` for other methods. Sensitive handlers call it with `SensitiveRead`. A denied action returns 403 with `{"error":"forbidden"}`. Future RBAC changes this function and its policy inputs, not the handlers' identity or audit contracts.

### 4b. Provider independence contract

Admin handlers, `authorize`, synchronous admin audit recording, and the web client consume only `AdminIdentity`. They never inspect provider implementation types, credential headers, JWT claims, or session cookies.

Provider traits and implementations live under `crates/cc-lb-admin/src/auth/`. The crate exports the provider-neutral identity, authenticator, provider contract required for composition and testing, and authentication errors. Concrete providers and their protocol helpers remain crate-private. The server constructs providers through one registry builder rather than importing provider types.

A new authentication method requires one provider implementation, one `AdminAuthProviderConfig` variant, and one provider-registry construction arm. Existing handlers, authorization, audit storage, and UI remain unchanged. A fake-provider integration test enforces this property by injecting a new provider and observing its identity in an existing mutation's audit record without modifying any handler or concrete provider.

### 5. Request boundary versus login boundary

`AdminAuthenticator` performs request authentication only. A future cc-lb-owned SSO implementation acts as an OpenID Connect relying party in a separate login module. That module owns `/admin/auth/login`, `/admin/auth/callback`, and session issuance. An `oidc_session` provider then validates the resulting session at the request boundary.

Login and callback routes do not require a blanket exception from a front-end gate. Each protocol route validates its own state and callback requirements, and deployments must report incompatible edge policies rather than weakening the full admin surface. A session provider owns session expiration and revocation checks.

### 6. Audit actor

`AuditEntry` gains `actor_authority`, `actor_subject`, `actor_kind`, and `actor_email`. The existing `actor` field remains the human-readable display value: email when present, otherwise `authority/subject`. `principal_id` continues to identify the principal being operated on and never substitutes for the actor.

System and scheduler events use `actor_kind = "system"`, `actor_authority = "cc-lb"`, and a component name in `actor_subject`. No audit entry stores raw credentials, tokens, or unfiltered identity claims.

### 7. Audit durability and coverage

Administrative changes, authentication and authorization security events, and the designated sensitive reads are appended synchronously through `AuditStore::append_audit` before the response returns. `AuditWriterSink` remains the proxy data-plane path.

Handlers keep their existing mutation order and append the audit record after the operation. If that local write fails, the handler logs the failure and returns 500 with `{"error":"audit_write_failed"}`. The mutation may already have completed. This ADR does not decide whether a future policy should block a mutation unless its audit intent can first be persisted.

Authentication failures record `admin_action = "auth_rejected"`, the request route, and the provider decision reason. They never record credential values. Authorization denials record `authz_denied` with the verified actor. Sensitive-read audit covers audit-log queries, configuration and draft/history/diff reads, key issuance and listing, request-event detail, and configuration/data export. Dashboard, event-list, and usage reads remain unaudited. Audit payloads contain only filter and target metadata, never response bodies, plaintext keys, secrets, or tokens.

External SIEM export is independent of the local synchronous write and remains future work.

### 8. Web UI

The web client discovers the current identity and authentication mode through `GET /admin/v1/auth/session`. It renders the shared-token entry gate only when the deployment uses static-token authentication without an external provider. Otherwise it enters through credentials supplied by the front-end authentication layer and displays the returned identity.

The audit UI continues to use `actor` as its concise display value and exposes the four structured actor fields in event details.


## Consequences

- A Cloudflare service token with an empty `sub` and a `common_name` is recorded as a `Service` actor rather than being rejected as an invalid human identity.
- A shared static token is recorded as a `BreakGlass` actor and cannot identify which person possessed the shared secret.
- Administrative responses incur one local database write for each audited operation or sensitive read.
- Provider-specific validation stays isolated from handlers and audit code, so new request authentication methods do not create parallel authorization or actor-recording paths.
- Identity continuity is bounded by the issuer's subject semantics. cc-lb records observed identities but does not infer account linking.

## Verification

- `cargo check --workspace` and `cargo clippy --workspace --all-targets -- -D warnings`.
- `cargo test -p cc-lb-config`.
- `cargo test -p cc-lb-admin --test integration`.
- `cargo test -p cc-lb-storage-sqlite audit`.
- `cargo test -p cc-lb-storage-postgres audit --features postgres` when `CI_POSTGRES_URL` is available.
- `grep -rn "admin_token" crates/` and `grep -rn 'actor: Some("admin"' crates/` both return no matches.
- A local static-token server returns a `BreakGlass` identity from `/admin/v1/auth/session`, records the actor for a kill-switch mutation and audit-log read, and records a credential-free request as `auth_rejected`.
- In `crates/cc-lb-admin/web`, `bun run test`, `bun run typecheck`, and `bun run build` pass; browser verification covers static-token entry, external identity display, and structured audit actor details.
- Provider-independence scans find no `cloudflare_access`, `static_token`, `jsonwebtoken`, or `Cf-Access` references in admin handlers, audit recording, authorization, or identity modules. The server references provider construction only through the registry builder and `AdminAuthenticator`.
