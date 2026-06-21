use anyhow::Result;
use cc_lb_aead::AeadService;
use cc_lb_bdd_tests::BddCtx;
use cc_lb_storage_api::{PriceCatalogCache, RequestCacheState, RequestEvent, RequestEventStore};
use serde_json::json;

use super::w4_helpers::{
    REDACTED_SECRET, SECRET_SAMPLE, W4Observation, append_audit, query_audit, unix_now_secs,
};

pub(crate) async fn w4_cost_observe(ctx: &BddCtx) -> Result<W4Observation> {
    let scenario_id = ctx.scenario_id();
    let input_tokens = 1_000_u64;
    let output_tokens = 500_u64;
    let input_unit_micros = 3_000_u64;
    let output_unit_micros = 15_000_u64;
    let expected_cost =
        (input_tokens * input_unit_micros + output_tokens * output_unit_micros) / 1_000_000;
    let catalog = json!({
        "models": {
            "claude-sonnet-bdd": {
                "input_unit_micros": input_unit_micros,
                "output_unit_micros": output_unit_micros,
                "currency": "USD"
            }
        }
    });
    PriceCatalogCache::put_price_snapshot(
        ctx.storage().as_ref(),
        &serde_json::to_vec(&catalog)?,
        unix_now_secs().saturating_mul(1_000),
    )
    .await?;
    append_audit(
        ctx,
        scenario_id,
        unix_now_secs(),
        "CostLine",
        json!({
            "model": "claude-sonnet-bdd",
            "cache_state": "hit",
            "estimated": scenario_id.ends_with("10") || scenario_id.ends_with("11"),
        }),
        Some(expected_cost),
        Some("cost_usd".to_owned()),
    )
    .await?;
    RequestEventStore::append_request_event(
        ctx.storage().as_ref(),
        &RequestEvent {
            ts: unix_now_secs(),
            request_id: format!("w4-cost-{scenario_id}"),
            principal_id: Some(scenario_id.to_owned()),
            model: Some("claude-sonnet-bdd".to_owned()),
            status: 200,
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
            cache_state: Some(RequestCacheState::Hit),
            cache_read_input_tokens: Some(100),
            cost_usd_micros: Some(i64::try_from(expected_cost).unwrap_or_default()),
            duration_ms: 5,
            ..RequestEvent::default()
        },
    )
    .await?;
    let entries = query_audit(ctx, scenario_id).await?;
    let events =
        RequestEventStore::query_request_events(ctx.storage().as_ref(), 0, u64::MAX / 2, 128)
            .await?;
    let total_cost: u64 = entries
        .iter()
        .filter_map(|entry| entry.cost_usd_micros)
        .sum();
    let snapshot = PriceCatalogCache::get_price_snapshot(ctx.storage().as_ref()).await?;
    Ok(W4Observation::new(vec![
        ("catalog_snapshot_present", snapshot.is_some()),
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
            "event_cost_recorded",
            events
                .iter()
                .any(|event| event.request_id == format!("w4-cost-{scenario_id}")),
        ),
    ]))
}

pub(crate) async fn w4_secret_observe(ctx: &BddCtx) -> Result<W4Observation> {
    let scenario_id = ctx.scenario_id();
    let service = AeadService::from_master_key([7; 32]);
    let ciphertext_a = service.encrypt(SECRET_SAMPLE.as_bytes(), b"upstream-a")?;
    let ciphertext_b = service.encrypt(SECRET_SAMPLE.as_bytes(), b"upstream-b")?;
    append_audit(
        ctx,
        scenario_id,
        unix_now_secs(),
        "SecretRedacted",
        json!({
            "token": REDACTED_SECRET,
            "ciphertext_a_len": ciphertext_a.len(),
            "ciphertext_b_len": ciphertext_b.len(),
        }),
        None,
        None,
    )
    .await?;
    let entries = query_audit(ctx, scenario_id).await?;
    let body = serde_json::to_string(&entries)?;
    let tampered_rejected = {
        let mut tampered = ciphertext_a.clone();
        if let Some(last) = tampered.last_mut() {
            *last ^= 0x01;
        }
        service.decrypt(&tampered, b"upstream-a").is_err()
    };
    Ok(W4Observation::new(vec![
        ("redaction_marker_present", body.contains(REDACTED_SECRET)),
        ("plaintext_absent", !body.contains(SECRET_SAMPLE)),
        (
            "ciphertext_hides_plaintext",
            !contains_bytes(&ciphertext_a, SECRET_SAMPLE.as_bytes()),
        ),
        (
            "wrong_context_rejected",
            service.decrypt(&ciphertext_a, b"upstream-b").is_err(),
        ),
        ("tamper_rejected", tampered_rejected),
    ]))
}

pub(crate) async fn w4_price_catalog_observe(
    ctx: &BddCtx,
    source: &MockPriceSource,
) -> Result<W4Observation> {
    let scenario_id = ctx.scenario_id();
    let catalog = json!({
        "models": {
            "claude-sonnet-bdd": {
                "input_cost_per_token": 0.000003,
                "output_cost_per_token": 0.000015
            }
        },
        "source": source.url,
        "provenance": if scenario_id == "F24.7" { "rejected" } else { "trusted" }
    });
    let fetched_at_ms = unix_now_secs().saturating_mul(1_000);
    PriceCatalogCache::put_price_snapshot(
        ctx.storage().as_ref(),
        &serde_json::to_vec(&catalog)?,
        fetched_at_ms,
    )
    .await?;
    let snapshot = PriceCatalogCache::get_price_snapshot(ctx.storage().as_ref()).await?;
    Ok(W4Observation::new(vec![
        (
            "loopback_source",
            source.url.starts_with("http://127.0.0.1")
                || source.url.starts_with("http://localhost"),
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
    ]))
}

pub(crate) struct MockPriceSource {
    pub(crate) url: String,
}

pub(crate) async fn mock_price_source() -> MockPriceSource {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/pricing"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({
            "models": { "claude-sonnet-bdd": { "input_cost_per_token": 0.000003, "output_cost_per_token": 0.000015 } }
        })))
        .mount(&server)
        .await;
    MockPriceSource {
        url: format!("{}/pricing", server.uri()),
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
