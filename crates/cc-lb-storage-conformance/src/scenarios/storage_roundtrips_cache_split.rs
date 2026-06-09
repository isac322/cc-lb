use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{RequestEventStore as _, types::RequestEvent};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn request_event_cache_split_round_trip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    with_conformance_fixture(backend, |storage| async move {
        let event = RequestEvent {
            ts: 1_800_000_000,
            ts_ms: Some(1_800_000_000_000),
            request_id: "req_cache_split_breakdown".to_owned(),
            principal_id: Some("p_split".to_owned()),
            key_id: Some("k_split".to_owned()),
            model: Some("claude-sonnet-4-5-20250929".to_owned()),
            status: 200,
            input_tokens: Some(123),
            output_tokens: Some(45),
            cache_creation_input_tokens: Some(700),
            cache_creation_input_tokens_5m: Some(400),
            cache_creation_input_tokens_1h: Some(300),
            cache_read_input_tokens: Some(200),
            cost_usd_micros: Some(987_654),
            cost_input_micros: Some(369_000),
            cost_output_micros: Some(675_000),
            cost_cache_creation_5m_micros: Some(150_000),
            cost_cache_creation_1h_micros: Some(180_000),
            cost_cache_read_micros: Some(60_000),
            duration_ms: 1234,
            ..Default::default()
        };

        storage.append_request_event(&event).await?;

        let recent = storage.query_recent_request_events(0, u64::MAX, 10).await?;
        let got = recent
            .into_iter()
            .find(|e| e.request_id == "req_cache_split_breakdown")
            .ok_or_else(|| anyhow::anyhow!("event must round-trip"))?;

        ensure!(
            got.cache_creation_input_tokens_5m == Some(400),
            "cache_creation_input_tokens_5m mismatch: {:?}",
            got.cache_creation_input_tokens_5m
        );
        ensure!(
            got.cache_creation_input_tokens_1h == Some(300),
            "cache_creation_input_tokens_1h mismatch: {:?}",
            got.cache_creation_input_tokens_1h
        );
        ensure!(
            got.cache_creation_input_tokens == Some(700),
            "cache_creation_input_tokens (legacy sum) mismatch: {:?}",
            got.cache_creation_input_tokens
        );
        ensure!(
            got.cost_input_micros == Some(369_000),
            "cost_input_micros mismatch: {:?}",
            got.cost_input_micros
        );
        ensure!(
            got.cost_output_micros == Some(675_000),
            "cost_output_micros mismatch: {:?}",
            got.cost_output_micros
        );
        ensure!(
            got.cost_cache_creation_5m_micros == Some(150_000),
            "cost_cache_creation_5m_micros mismatch: {:?}",
            got.cost_cache_creation_5m_micros
        );
        ensure!(
            got.cost_cache_creation_1h_micros == Some(180_000),
            "cost_cache_creation_1h_micros mismatch: {:?}",
            got.cost_cache_creation_1h_micros
        );
        ensure!(
            got.cost_cache_read_micros == Some(60_000),
            "cost_cache_read_micros mismatch: {:?}",
            got.cost_cache_read_micros
        );
        ensure!(
            got.cost_usd_micros == Some(987_654),
            "cost_usd_micros mismatch: {:?}",
            got.cost_usd_micros
        );

        Ok(())
    })
    .await
}
