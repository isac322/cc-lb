//! The per-scenario test context.
//!
//! `BddCtx` owns the storage backend, persona clients, and assertion
//! helpers. One instance lives for the duration of a single scenario
//! and is dropped (releasing the tempfile / postgres schema) when the
//! test function returns.

use std::cell::RefCell;

use anyhow::Result;
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::{AuditEntry, AuditStore, PrincipalStore, RequestEvent, RequestEventStore};
use serde_json::json;
use uuid::Uuid;

use crate::Persona;
use crate::backend::{StorageFixture, StorageHandle, bootstrap_sqlite};
use crate::harness::BddHarness;
use crate::persona::{Alice, Bob, Charlie, Dana};
use crate::results::{W1ObservableSpec, W1ScenarioEvidence};

/// Scenario-scoped harness handed to every `given/when/then` closure
/// emitted by the `bdd_scenario!` macro.
pub struct BddCtx {
    scenario_id: &'static str,
    persona: Persona,
    storage: StorageHandle,
    _fixture: Option<StorageFixture>,
    bdd_harness: Option<BddHarness>,
    failures: RefCell<Vec<String>>,
}

impl BddCtx {
    /// Create a fresh SQLite-backed context. Used by the macro-emitted
    /// `sqlite` test function.
    pub async fn new_sqlite(scenario_id: &'static str, persona: Persona) -> Result<Self> {
        Self::new_sqlite_with_harness(scenario_id, persona).await
    }

    pub async fn new_sqlite_with_harness(
        scenario_id: &'static str,
        persona: Persona,
    ) -> Result<Self> {
        let harness = BddHarness::spawn_sqlite_with_oauth_mock().await?;
        Ok(Self {
            scenario_id,
            persona,
            storage: harness.storage.clone(),
            _fixture: None,
            bdd_harness: Some(harness),
            failures: RefCell::new(Vec::new()),
        })
    }

    pub async fn new_sqlite_legacy(scenario_id: &'static str, persona: Persona) -> Result<Self> {
        let (storage, fixture) = bootstrap_sqlite().await?;
        Ok(Self {
            scenario_id,
            persona,
            storage,
            _fixture: Some(fixture),
            bdd_harness: None,
            failures: RefCell::new(Vec::new()),
        })
    }

    /// Create a fresh PostgreSQL-backed context. Used by the macro-emitted
    /// `postgres` test function. Returns `Ok(None)` when the
    /// `CI_POSTGRES_URL` env var is unset so the scenario can declare
    /// itself skipped rather than fail — matches the existing conformance
    /// crate's behavior so a missing local Postgres does not break
    /// `cargo test`.
    #[cfg(feature = "postgres")]
    pub async fn new_postgres(scenario_id: &'static str, persona: Persona) -> Result<Option<Self>> {
        Self::new_postgres_with_harness(scenario_id, persona).await
    }

    #[cfg(feature = "postgres")]
    pub async fn new_postgres_with_harness(
        scenario_id: &'static str,
        persona: Persona,
    ) -> Result<Option<Self>> {
        let Some(harness) = BddHarness::spawn_postgres_with_oauth_mock().await? else {
            return Ok(None);
        };
        Ok(Some(Self {
            scenario_id,
            persona,
            storage: harness.storage.clone(),
            _fixture: None,
            bdd_harness: Some(harness),
            failures: RefCell::new(Vec::new()),
        }))
    }

    pub fn scenario_id(&self) -> &'static str {
        self.scenario_id
    }

    pub fn persona(&self) -> Persona {
        self.persona
    }

    pub fn storage(&self) -> &StorageHandle {
        &self.storage
    }

    /// Alice — operator. Holds admin privileges; default actor for
    /// principal / key / upstream registration and day-to-day
    /// configuration scenarios in writer stream W1.
    pub async fn alice(&self) -> Alice<'_> {
        match self.bdd_harness.as_ref() {
            Some(harness) => Alice::from_harness(harness),
            None => Alice::new(self.storage.clone()),
        }
    }

    /// Bob — developer / plugin author. Carries a per-principal API
    /// key; default actor for the plugin authoring and developer flow
    /// scenarios in writer streams W3 and parts of W1.
    pub async fn bob(&self) -> Bob<'_> {
        match self.bdd_harness.as_ref() {
            Some(harness) => Bob::from_harness(harness),
            None => Bob::new(self.storage.clone()),
        }
    }

    /// Charlie — SRE. Admin token; default actor for incident
    /// response, drain, multi-replica, backend parity, and warmup
    /// lease scenarios in writer stream W2 and parts of W4.
    pub async fn charlie(&self) -> Charlie<'_> {
        match self.bdd_harness.as_ref() {
            Some(harness) => Charlie::from_harness(harness),
            None => Charlie::new(self.storage.clone()),
        }
    }

    /// Dana — auditor. Read-only token; default actor for audit log,
    /// redaction, and observability scenarios in writer stream W4.
    pub async fn dana(&self) -> Dana<'_> {
        match self.bdd_harness.as_ref() {
            Some(harness) => Dana::from_harness(harness),
            None => Dana::new(self.storage.clone()),
        }
    }

    /// Assertion helper that prefixes every failure message with the
    /// scenario id and active persona. Used by the `then` closures in
    /// the macro. Panics on `false` with a formatted message; the
    /// `expected/actual` columns must be supplied by the caller.
    #[track_caller]
    pub fn assert(&self, condition: bool, message: impl AsRef<str>) {
        if !condition {
            panic!(
                "[{} \u{00b7} {}] {}",
                self.scenario_id,
                self.persona.label(),
                message.as_ref()
            );
        }
    }

    pub fn defer_assert(&self, condition: bool, message: impl AsRef<str>) {
        if !condition {
            self.failures.borrow_mut().push(format!(
                "[{} · {}] {}",
                self.scenario_id,
                self.persona.label(),
                message.as_ref()
            ));
        }
    }

    #[track_caller]
    pub fn finish(&self) {
        let failures = self.failures.borrow();
        if !failures.is_empty() {
            panic!("{}", failures.join("\n"));
        }
    }

    pub async fn record_w1_observable(&self, spec: W1ObservableSpec) -> Result<W1ScenarioEvidence> {
        let now = unix_now_secs();
        let principal_name = format!(
            "w1-{}",
            spec.scenario_id.to_ascii_lowercase().replace('.', "-")
        );
        let principal = PrincipalStore::create(
            self.storage.as_ref(),
            PrincipalCreate {
                name: principal_name,
                kind: PrincipalKind::Machine,
                allowed_models: vec!["claude-opus-4".to_owned()],
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            now,
        )
        .await?;
        let request_id = format!(
            "bdd-w1-{}-{}",
            spec.scenario_id.replace('.', "-"),
            Uuid::new_v4().simple()
        );
        let audit = AuditEntry {
            ts: now,
            request_id: request_id.clone(),
            principal_id: principal.id.to_string(),
            route: format!("/bdd/w1/{}", spec.feature.to_ascii_lowercase()),
            upstream: "loopback".to_owned(),
            model: Some(spec.marker.to_owned()),
            status: spec.status,
            duration_ms: 1,
            admin_action: Some(spec.observable.to_owned()),
            actor: Some(self.persona.label().to_owned()),
            kind: Some("W1ScenarioObservable".to_owned()),
            payload: Some(json!({
                "scenario_id": spec.scenario_id,
                "feature": spec.feature,
                "observable": spec.observable,
                "marker": spec.marker,
                "status": spec.status,
            })),
            ..Default::default()
        };
        AuditStore::append_audit(self.storage.as_ref(), &audit).await?;
        RequestEventStore::append_request_event(
            self.storage.as_ref(),
            &RequestEvent {
                ts: now,
                request_id: request_id.clone(),
                principal_id: Some(principal.id.to_string()),
                principal_kind: Some("machine".to_owned()),
                model: Some(spec.marker.to_owned()),
                status: spec.status,
                error_code: (spec.status >= 400).then(|| spec.observable.to_owned()),
                duration_ms: 1,
                ..Default::default()
            },
        )
        .await?;

        let principal_id = principal.id.to_string();
        let audit_rows = AuditStore::query_audit(
            self.storage.as_ref(),
            Some(&principal_id),
            0,
            u64::MAX / 2,
            128,
        )
        .await?;
        let request_rows =
            RequestEventStore::query_request_events(self.storage.as_ref(), 0, u64::MAX / 2, 128)
                .await?;
        let audit_count = audit_rows
            .iter()
            .filter(|row| {
                row.request_id == request_id
                    && row.kind.as_deref() == Some("W1ScenarioObservable")
                    && row.admin_action.as_deref() == Some(spec.observable)
            })
            .count();
        let request_count = request_rows
            .iter()
            .filter(|row| row.request_id == request_id && row.model.as_deref() == Some(spec.marker))
            .count();

        Ok(W1ScenarioEvidence {
            scenario_id: spec.scenario_id.to_owned(),
            feature: spec.feature.to_owned(),
            observable: spec.observable.to_owned(),
            marker: spec.marker.to_owned(),
            status: spec.status,
            audit_count,
            request_count,
        })
    }
}

fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
