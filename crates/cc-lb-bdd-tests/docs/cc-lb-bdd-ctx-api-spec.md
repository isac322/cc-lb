# BddCtx (Test Context) API Specification

This document defines the concrete Rust API specification for `BddCtx`, the per-scenario test context that wires the storage backend, fake-anthropic, OAuth mock, persona clients, no-real-API guard, and capture sink together.

---

## §1 Overview

The `BddCtx` struct manages the lifecycle and resources of a single BDD scenario. It provides a clean, isolated environment for each test run, ensuring that concurrent tests do not interfere with each other.

### Lifecycle (Per Scenario)

1. **Initialization**: A fresh `BddCtx` is constructed at the start of each scenario. This sets up an isolated database instance (either a temporary SQLite file or a dedicated PostgreSQL schema).
2. **Fixture Setup**: The context seeds initial database state and spawns required mock servers on dynamically allocated loopback ports.
3. **Execution**: Persona clients (Alice, Bob, Charlie, Dana) interact with the system under test, using the context to perform actions and assert outcomes.
4. **Teardown**: When the context is dropped, it captures diagnostic state if the test failed, stops all spawned mock servers, and cleans up database resources.

### Responsibilities

- **Database Isolation**: Guarantee complete database isolation between concurrent tests.
- **Mock Management**: Manage the lifecycle of in-process mock servers, ensuring they start on free ports and stop cleanly.
- **Persona Provisioning**: Provide authenticated clients for each persona with stable identities.
- **Safety Enforcement**: Prevent any real external API calls.
- **Diagnostic Capture**: Dump detailed execution logs and state on failure.

### What it MUST NOT do

- **No Global State**: Do not use global mutable state or shared counters, as tests run concurrently.
- **No Real Network I/O**: Do not allow any network traffic to leave the loopback interface.
- **No Persistent DB Pollution**: Do not leave temporary SQLite files or PostgreSQL schemas behind after a successful run.

---

## §2 Construction

Each BDD scenario is defined using the `bdd_scenario!` macro. By default, this macro generates two independent `#[tokio::test]` functions: one for SQLite and one for PostgreSQL.

### Signatures

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Sqlite,
    Postgres,
}

pub struct BddCtx {
    scenario_id: &'static str,
    backend_kind: BackendKind,
}

impl BddCtx {
    pub async fn new(scenario_id: &'static str, backend: BackendKind) -> Result<Self, BddError>;
}
```

### Macro Expansion and Backend Matrix

The `bdd_scenario!` macro automates the generation of the backend matrix. Every scenario produces both `_sqlite` and `_postgres` tests by default. If a scenario is backend-specific, the `backend` attribute can restrict it.

```rust
// Example macro invocation
bdd_scenario!(
    id = "F1.1a",
    fn_name = f1_1a,
    persona = Alice,
    title = "Alice registers a new principal",
    description = "Alice creates a principal and verifies active state.",
    given = |ctx| async move { ctx.alice().await },
    when = |ctx, alice| async move { alice.create_principal("team-x").await },
    then = |ctx, _alice, result| async move {
        ctx.assert(result.is_active, "Principal must be active");
    },
);

// Generated code structure (simplified)
#[tokio::test]
async fn f1_1a_sqlite() {
    let ctx = BddCtx::new("F1.1a", BackendKind::Sqlite).await.unwrap();
    // Execute given, when, then blocks...
}

#[tokio::test]
async fn f1_1a_postgres() {
    if let Some(ctx) = BddCtx::new_postgres_if_configured("F1.1a").await {
        // Execute given, when, then blocks...
    }
}
```

For backend-specific scenarios, the macro supports attributes like `backend = sqlite_only` or `backend = postgres_only` to skip the unwanted backend.

---

## §3 Storage Backend

The storage backend is isolated per test to prevent cross-test contamination. This follows the database isolation principles outlined in §6 of the test conversion plan.

### SQLite Isolation

For SQLite, `BddCtx` creates a unique temporary directory using the `tempfile` crate. A fresh SQLite database file is initialized inside this directory.

```rust
let temp_dir = tempfile::tempdir()?;
let db_path = temp_dir.path().join(format!("bdd-{}.sqlite", scenario_id));
let connection_string = format!("sqlite://{}", db_path.display());
```

### PostgreSQL Isolation

For PostgreSQL, `BddCtx` uses a single database instance but isolates each test run within a dedicated schema. The schema name is derived from the scenario ID and a unique UUID to prevent collisions.

```rust
let unique_id = uuid::Uuid::new_v4().simple();
let schema_name = format!("bdd_{}_{}", scenario_id.replace('.', "_"), unique_id);
let create_schema_sql = format!("CREATE SCHEMA {};", schema_name);
let set_search_path_sql = format!("SET search_path TO {}, public;", schema_name);
```

During teardown, the schema is dropped using `DROP SCHEMA <schema_name> CASCADE`.

### Seeding and Accessor

The storage is initialized and migrated automatically using the workspace's migration assets. The context exposes the storage instance via an `Arc<dyn Storage>` accessor.

```rust
impl BddCtx {
    pub fn store(&self) -> Arc<dyn cc_lb_storage_api::Storage> {
        // Returns the active SQLite or PostgreSQL storage adapter
    }
}
```

---

## §4 Fake-Anthropic Spawn

Scenarios that simulate interactions with the Anthropic API use an in-process mock server. This mock server is based on the existing `fake-anthropic` fixture.

### Signatures and Handles

```rust
pub struct FakeAnthropicHandle {
    base_url: Url,
    script: MessageScript,
    _join_handle: tokio::task::JoinHandle<()>,
}

impl FakeAnthropicHandle {
    pub fn base_url(&self) -> &Url { &self.base_url }
    pub fn script(&self) -> &MessageScript { &self.script }
}

impl BddCtx {
    pub async fn spawn_fake_anthropic(&self) -> Result<FakeAnthropicHandle, BddError>;
}
```

### Script Methods

The `MessageScript` handle allows scenarios to control mock behavior and inspect recorded requests. The existing implementation is located in `tests/fixtures/fake-anthropic/src/routes.rs:48-156`.

```rust
impl MessageScript {
    pub fn push_response(&self, response: ScriptedMessageResponse);
    pub fn pop_response(&self) -> Option<ScriptedMessageResponse>;
    pub fn requests(&self) -> Vec<RecordedMessageRequest>;
    pub async fn wait_for_requests(&self, n: usize, timeout: Duration) -> Vec<RecordedMessageRequest>;
}

impl ScriptedMessageResponse {
    pub fn with_delay(mut self, delay: Duration) -> Self;
    pub fn with_header(mut self, name: &str, value: &str) -> Self;
}
```

### Planned Extensions (TODO PR)

To support advanced scenarios, the following extensions must be implemented in the `fake-anthropic` crate as part of the initial milestone:

1. **SSE Streaming Response**: Support for Server-Sent Events streaming.
2. **Mid-Response Drop**: Simulate network failures by dropping the connection after sending a partial response.
3. **Conditional Response**: Support predicate-based response selection instead of strict FIFO queueing.

```rust
pub enum ResponseKind {
    Json(serde_json::Value),
    Sse(Vec<SseEvent>),
    DropAfterBytes(Vec<u8>, usize),
}

impl ScriptedMessageResponse {
    pub fn with_kind(mut self, kind: ResponseKind) -> Self;
}

impl MessageScript {
    pub fn push_conditional<F>(&self, predicate: F)
    where
        F: Fn(&RecordedMessageRequest) -> Option<ScriptedMessageResponse> + Send + Sync + 'static;
}
```

---

## §5 Mock-Anthropic-OAuth-Server Spawn

Scenarios involving OAuth token refresh or key issuance use an in-process OAuth mock server. This is based on the existing `mock-anthropic-oauth-server` fixture.

### Signatures and Behavior

```rust
pub struct MockOAuthHandle {
    base_url: Url,
    _join_handle: tokio::task::JoinHandle<()>,
}

impl MockOAuthHandle {
    pub fn base_url(&self) -> &Url { &self.base_url }
}

impl BddCtx {
    pub async fn spawn_oauth_server(&self) -> Result<MockOAuthHandle, BddError>;
}
```

The mock OAuth server handles token exchange, token refresh, and public key retrieval requests. It records all incoming requests for verification in the capture sink.

---

## §6 Persona Clients

To make scenarios readable and expressive, `BddCtx` provides pre-configured clients for the four standard personas: Alice, Bob, Charlie, and Dana.

### Signatures

```rust
impl BddCtx {
    pub async fn alice(&self) -> Result<OperatorClient, BddError>;
    pub async fn bob(&self) -> Result<DeveloperClient, BddError>;
    pub async fn charlie(&self) -> Result<SreClient, BddError>;
    pub async fn dana(&self) -> Result<AuditorClient, BddError>;
}
```

### Stable Identity and Principal Registry

To ensure consistency within a single scenario, the context maintains an internal `PrincipalRegistry`. This registry maps each persona to a stable database identity (such as a specific principal ID and API key) across multiple client calls.

```rust
pub struct PrincipalRegistry {
    // Maps persona names to their registered principal records and credentials
}
```

---

## §7 No-Real-API Guard (3-Layer Gate)

To guarantee that no test run ever contacts the real Anthropic API, a strict three-layer gate is enforced.

1. **Layer 1: Build-Time Grep Lint**: A build-time test in `tests/no_real_anthropic.rs` scans the test suite source code. If the string `api.anthropic.com` is found, the build fails immediately.
2. **Layer 2: Runtime Loopback Guard**: All HTTP clients constructed within the test suite must use a loopback-only wrapper. The helper function `assert_loopback_upstream` validates every destination URL before any request is dispatched.
3. **Layer 3: CI Environment Isolation**: The GitHub Actions workflows (`.github/workflows/postgres.yml` and `ci.yml`) are configured with no external secrets. No `ANTHROPIC_API_KEY` is present in the environment.

```rust
/// Validates that the target URL resolves to a loopback address.
///
/// # Panics
/// Panics immediately if the host is not `localhost`, `127.0.0.1`, or `::1`.
pub fn assert_loopback_upstream(url: &str) {
    let parsed = Url::parse(url).expect("Invalid URL in loopback guard");
    let host = parsed.host_str().expect("Missing host in loopback guard");
    
    if host != "localhost" && host != "127.0.0.1" && host != "::1" {
        panic!("[SECURITY VIOLATION] Attempted to contact non-loopback upstream: {}", url);
    }
}
```

This guard is integrated into `BddCtx::spawn_fake_anthropic` and all persona client builders.

---

## §8 Captures Sink

When a scenario runs, all interactions (HTTP requests, responses, database changes, and audit entries) are recorded in an in-memory capture sink.

### Path and Gitignore

On failure, the capture sink dumps its contents to a JSON file:
`crates/cc-lb-bdd-tests/tests/__captures__/<scenario_id>.json`

This directory is explicitly added to `.gitignore` to prevent test artifacts from being committed.

### Behavior on Failure

If a scenario panics or fails an assertion, the macro's panic handler writes the captured data to disk.

```rust
#[derive(Serialize)]
pub struct CaptureDump {
    pub scenario_id: &'static str,
    pub requests: Vec<RecordedMessageRequest>,
    pub responses: Vec<ScriptedMessageResponse>,
    pub persona_calls: Vec<PersonaCallRecord>,
    pub audit_entries: Vec<AuditLogRecord>,
}
```

### CI Retention

On CI failure, the `actions/upload-artifact` step uploads the `__captures__` directory.
- **PR Runs**: Retained for 14 days.
- **Nightly Runs**: Retained for 30 days.

### Accessor

```rust
impl BddCtx {
    pub fn capture(&self) -> &CaptureSink { &self.capture_sink }
}
```

---

## §9 Assert Helpers

To provide clear diagnostic messages, `BddCtx` exposes specialized assertion helpers. These helpers automatically prefix panic messages with the scenario ID and active persona.

### Signatures

```rust
impl BddCtx {
    pub fn assert(&self, condition: bool, message: &str);
    pub fn assert_eq<T>(&self, left: T, right: T, message: &str)
    where
        T: PartialEq + std::fmt::Debug;
    pub async fn assert_audit_row_exists(&self, action: &str, actor: &str) -> Result<(), BddError>;
    pub fn assert_redacted(&self, value: &str);
}
```

---

## §10 Teardown Order

To prevent resource leaks and port collisions, `BddCtx` implements the `Drop` trait with a strict teardown sequence.

1. **Snapshot Captures**: Only if the test thread is panicking.
2. **Drop Fake-Anthropic Mock Server**: Stop the fake-anthropic mock server.
3. **Drop OAuth Mock Server**: Stop the OAuth mock server.
4. **Drop WireMock LiteLLM Server**: If active, stop the WireMock LiteLLM server.
5. **Clean Database**: DROP SCHEMA CASCADE or unlink SQLite file.

### Drop Implementation

```rust
impl Drop for BddCtx {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.write_capture_dump();
        }
        // Stop mock servers and perform database cleanup
    }
}
```

---

## §11 Lifetime and Send-Sync Constraints

To support concurrent execution under `cargo nextest`, `BddCtx` must be safely usable across thread boundaries.

- **No `!Send` Types**: The context must not contain any types that prevent it from being `Send` or `Sync`.
- **No Static Mutable State**: All state must be instance-local. Global counters or static variables are forbidden.
- **Internal Thread Safety**: The context uses `Arc` and thread-safe synchronization primitives internally to allow safe sharing across asynchronous tasks.

```rust
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    let _ = assert_send_sync::<BddCtx>;
};
```

---

## §12 Reference: Existing Patterns Reused

The design of `BddCtx` directly reuses and adapts proven patterns from the existing codebase:

### Storage Conformance Harness

The `BddBackend` trait and fixture lifecycle are modeled after the conformance harness in `crates/cc-lb-storage-conformance/src/harness.rs:1-68`.

- **Reused Concept**: The abstraction of database creation, opening, and teardown per backend kind.

### Pipeline E2E Tests

The in-process spawning of mock upstreams and server lifecycles is based on `crates/cc-lb-server/tests/pipeline_e2e.rs:461-573`.

- **Reused Concept**: Spawning `axum` servers on dynamically allocated loopback ports and configuring the `Lifecycle` engine with mock upstreams.

### WireMock Integration

The use of `wiremock::MockServer` for auxiliary upstreams (such as the LiteLLM price catalog) is adapted from `tests/integration/managed_api_key_full_flow.rs:54-80`.

- **Reused Concept**: Mounting mock endpoints to simulate pricing catalog updates.

---

## §13 Out of Scope for this Spec

The following areas are explicitly excluded from this specification and will be addressed in subsequent implementation phases:

- **Implementation Bodies**: The actual internal code of the `BddCtx` methods and persona clients.
- **Retry Policies**: Client-side HTTP retry logic and backoff strategies.
- **Plugin-Specific Helpers**: Custom assertion helpers or fixtures dedicated to specific WASM plugins. These will be defined within their respective scenario files.
