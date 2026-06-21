//! The per-scenario test context.
//!
//! `BddCtx` owns the storage backend, persona clients, and assertion
//! helpers. One instance lives for the duration of a single scenario
//! and is dropped (releasing the tempfile / postgres schema) when the
//! test function returns.

use anyhow::Result;

use crate::Persona;
use crate::backend::{StorageFixture, StorageHandle, bootstrap_sqlite};
use crate::persona::{Alice, Bob, Charlie, Dana};

/// Scenario-scoped harness handed to every `given/when/then` closure
/// emitted by the `bdd_scenario!` macro.
pub struct BddCtx {
    scenario_id: &'static str,
    persona: Persona,
    storage: StorageHandle,
    _fixture: StorageFixture,
}

impl BddCtx {
    /// Create a fresh SQLite-backed context. Used by the macro-emitted
    /// `_sqlite` test function.
    pub async fn new_sqlite(scenario_id: &'static str, persona: Persona) -> Result<Self> {
        let (storage, fixture) = bootstrap_sqlite().await?;
        Ok(Self {
            scenario_id,
            persona,
            storage,
            _fixture: fixture,
        })
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
    pub async fn alice(&self) -> Alice {
        Alice::new(self.storage.clone())
    }

    /// Bob — developer / plugin author. Carries a per-principal API
    /// key; default actor for the plugin authoring and developer flow
    /// scenarios in writer streams W3 and parts of W1.
    pub async fn bob(&self) -> Bob {
        Bob::new(self.storage.clone())
    }

    /// Charlie — SRE. Admin token; default actor for incident
    /// response, drain, multi-replica, backend parity, and warmup
    /// lease scenarios in writer stream W2 and parts of W4.
    pub async fn charlie(&self) -> Charlie {
        Charlie::new(self.storage.clone())
    }

    /// Dana — auditor. Read-only token; default actor for audit log,
    /// redaction, and observability scenarios in writer stream W4.
    pub async fn dana(&self) -> Dana {
        Dana::new(self.storage.clone())
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
}
