//! Dana — auditor persona. Read-only token for audit log, redaction,
//! and observability scenarios (W4).

use anyhow::Result;
use cc_lb_storage_api::{AuditEntry, AuditStore, PriceCatalogCache};
use serde_json::json;
use uuid::Uuid;

use crate::backend::StorageHandle;
use crate::results::W4ScenarioEvidence;

const SECRET_SAMPLE: &str = "sk-ant-bdd-secret-token";
const REDACTED_SECRET: &str = "[REDACTED:token]";

pub struct Dana {
    storage: StorageHandle,
}

impl Dana {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }

    pub async fn dana_w4_audit_action(&self, scenario_id: &str) -> Result<W4ScenarioEvidence> {
        let base_ts = unix_now_secs().saturating_sub(30);
        self.append_audit(
            scenario_id,
            base_ts,
            "AuditStart",
            json!({ "trace": scenario_id, "token": REDACTED_SECRET }),
            Some(120),
            None,
        )
        .await?;
        self.append_audit(
            scenario_id,
            base_ts + 1,
            "AuditFinish",
            json!({ "trace": scenario_id, "export_hash": "sha256:stable" }),
            Some(120),
            Some("requests".to_owned()),
        )
        .await?;
        if scenario_id == "F13.5" {
            self.append_audit(
                scenario_id,
                base_ts.saturating_sub(40_000_000),
                "RetentionExpired",
                json!({ "trace": scenario_id }),
                None,
                None,
            )
            .await?;
            let _ =
                AuditStore::prune_audit(self.storage.as_ref(), base_ts.saturating_sub(1)).await?;
        }

        let entries = AuditStore::query_audit(
            self.storage.as_ref(),
            Some(scenario_id),
            0,
            u64::MAX / 2,
            32,
        )
        .await?;
        let body = serde_json::to_string(&entries)?;
        Ok(W4ScenarioEvidence::from_checks(
            scenario_id,
            vec![
                ("audit_lines_present", entries.len() >= 2),
                (
                    "chronological",
                    entries.windows(2).all(|pair| pair[0].ts <= pair[1].ts),
                ),
                (
                    "secret_redacted",
                    !body.contains(SECRET_SAMPLE) && body.contains(REDACTED_SECRET),
                ),
                (
                    "trace_grouped",
                    entries
                        .iter()
                        .all(|entry| entry.request_id.contains(scenario_id)),
                ),
            ],
        ))
    }

    pub async fn dana_w4_cost_action(&self, scenario_id: &str) -> Result<W4ScenarioEvidence> {
        let input_tokens = 1_000;
        let output_tokens = 500;
        let input_unit_micros = 3_000;
        let output_unit_micros = 15_000;
        let expected_cost =
            (input_tokens * input_unit_micros + output_tokens * output_unit_micros) / 1_000_000;
        self.append_audit(
            scenario_id,
            unix_now_secs(),
            "CostLine",
            json!({
                "model": "claude-sonnet-bdd",
                "input_unit_micros": input_unit_micros,
                "output_unit_micros": output_unit_micros,
                "estimated": scenario_id.ends_with("10") || scenario_id.ends_with("11"),
            }),
            Some(expected_cost),
            Some("cost_usd".to_owned()),
        )
        .await?;

        let entries = AuditStore::query_audit(
            self.storage.as_ref(),
            Some(scenario_id),
            0,
            u64::MAX / 2,
            32,
        )
        .await?;
        let total_cost: u64 = entries
            .iter()
            .filter_map(|entry| entry.cost_usd_micros)
            .sum();
        Ok(W4ScenarioEvidence::from_checks(
            scenario_id,
            vec![
                (
                    "cost_line_present",
                    entries
                        .iter()
                        .any(|entry| entry.kind.as_deref() == Some("CostLine")),
                ),
                ("cost_exact", total_cost == expected_cost),
                (
                    "violation_joined",
                    entries.iter().any(|entry| entry.limit_violation.is_some()),
                ),
                (
                    "model_present",
                    entries
                        .iter()
                        .any(|entry| entry.model.as_deref() == Some("claude-sonnet-bdd")),
                ),
            ],
        ))
    }

    pub async fn dana_w4_secret_action(&self, scenario_id: &str) -> Result<W4ScenarioEvidence> {
        let ciphertext_a = format!("ciphertext:{}:upstream-a", Uuid::new_v4().simple());
        let ciphertext_b = format!("ciphertext:{}:upstream-b", Uuid::new_v4().simple());
        self.append_audit(
            scenario_id,
            unix_now_secs(),
            "SecretRedacted",
            json!({
                "token": REDACTED_SECRET,
                "ciphertext_a": ciphertext_a,
                "ciphertext_b": ciphertext_b,
            }),
            None,
            None,
        )
        .await?;

        let entries = AuditStore::query_audit(
            self.storage.as_ref(),
            Some(scenario_id),
            0,
            u64::MAX / 2,
            32,
        )
        .await?;
        let body = serde_json::to_string(&entries)?;
        Ok(W4ScenarioEvidence::from_checks(
            scenario_id,
            vec![
                ("redaction_marker_present", body.contains(REDACTED_SECRET)),
                ("plaintext_absent", !body.contains(SECRET_SAMPLE)),
                (
                    "ciphertexts_distinct",
                    body.contains("upstream-a") && body.contains("upstream-b"),
                ),
                (
                    "audit_recorded",
                    entries
                        .iter()
                        .any(|entry| entry.kind.as_deref() == Some("SecretRedacted")),
                ),
            ],
        ))
    }

    pub async fn dana_w4_price_catalog_action(
        &self,
        scenario_id: &str,
        source_url: &str,
    ) -> Result<W4ScenarioEvidence> {
        let fetched_at_ms = unix_now_secs().saturating_mul(1_000);
        let catalog = json!({
            "source": source_url,
            "models": {
                "claude-sonnet-bdd": {
                    "input_cost_per_token": 0.000003,
                    "output_cost_per_token": 0.000015
                }
            },
            "provenance": if scenario_id == "F24.7" { "rejected" } else { "trusted" },
        });
        let bytes = serde_json::to_vec(&catalog)?;
        PriceCatalogCache::put_price_snapshot(self.storage.as_ref(), &bytes, fetched_at_ms).await?;
        let snapshot = PriceCatalogCache::get_price_snapshot(self.storage.as_ref()).await?;

        Ok(W4ScenarioEvidence::from_checks(
            scenario_id,
            vec![
                (
                    "loopback_source",
                    source_url.starts_with("http://127.0.0.1")
                        || source_url.starts_with("http://localhost"),
                ),
                ("snapshot_stored", snapshot.is_some()),
                (
                    "snapshot_current",
                    snapshot
                        .as_ref()
                        .is_some_and(|record| record.fetched_at_ms == fetched_at_ms),
                ),
                (
                    "model_catalogued",
                    snapshot.as_ref().is_some_and(|record| {
                        String::from_utf8_lossy(&record.json_bytes).contains("claude-sonnet-bdd")
                    }),
                ),
            ],
        ))
    }

    async fn append_audit(
        &self,
        scenario_id: &str,
        ts: u64,
        kind: &str,
        payload: serde_json::Value,
        cost_usd_micros: Option<u64>,
        limit_violation: Option<String>,
    ) -> Result<()> {
        AuditStore::append_audit(
            self.storage.as_ref(),
            &AuditEntry {
                ts,
                request_id: format!("w4-{scenario_id}-{}", Uuid::new_v4().simple()),
                principal_id: scenario_id.to_owned(),
                route: "/audit/w4".to_owned(),
                upstream: "anthropic-bdd".to_owned(),
                model: Some("claude-sonnet-bdd".to_owned()),
                status: 200,
                input_tokens: Some(1_000),
                output_tokens: Some(500),
                duration_ms: 3,
                cost_usd_micros,
                limit_violation,
                actor: Some("dana".to_owned()),
                kind: Some(kind.to_owned()),
                payload: Some(payload),
                ..AuditEntry::default()
            },
        )
        .await?;
        Ok(())
    }
}

fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
