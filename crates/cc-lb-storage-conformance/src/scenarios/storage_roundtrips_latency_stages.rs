use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{RequestEvent, RequestEventStore as _};

use crate::harness::{ConformanceBackend, stored_request_events, with_conformance_fixture};

pub async fn request_event_latency_stage_round_trip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    with_conformance_fixture(backend, |storage| async move {
        // Event 1: all 7 new latency fields populated.
        let event_full = RequestEvent {
            ts: 1_800_000_001,
            ts_ms: Some(1_800_000_001_000),
            request_id: "req_latency_full".to_owned(),
            event_id: Some("req_latency_full".to_owned()),
            principal_id: Some("p_latency".to_owned()),
            key_id: Some("k_latency".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 200,
            duration_ms: 500,
            auth_ms: Some(50),
            route_ms: Some(30),
            limit_reserve_ms: Some(10),
            bulkhead_wait_ms: Some(3),
            dns_ms: Some(12),
            connect_ms: Some(85),
            connection_reused: Some(false),
            ..Default::default()
        };

        // Event 2: warm-pool invariant — connection_reused=true, dns_ms/connect_ms absent.
        let event_warm = RequestEvent {
            ts: 1_800_000_002,
            ts_ms: Some(1_800_000_002_000),
            request_id: "req_latency_warm".to_owned(),
            event_id: Some("req_latency_warm".to_owned()),
            principal_id: Some("p_latency".to_owned()),
            key_id: Some("k_latency".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 200,
            duration_ms: 400,
            auth_ms: Some(45),
            route_ms: Some(28),
            limit_reserve_ms: Some(8),
            bulkhead_wait_ms: Some(2),
            dns_ms: None,
            connect_ms: None,
            connection_reused: Some(true),
            ..Default::default()
        };

        // Event 3: all 7 new fields None — simulates an event written by an old version.
        let event_none = RequestEvent {
            ts: 1_800_000_003,
            ts_ms: Some(1_800_000_003_000),
            request_id: "req_latency_none".to_owned(),
            event_id: Some("req_latency_none".to_owned()),
            principal_id: Some("p_latency".to_owned()),
            key_id: Some("k_latency".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 200,
            duration_ms: 300,
            auth_ms: None,
            route_ms: None,
            limit_reserve_ms: None,
            bulkhead_wait_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            ..Default::default()
        };

        storage.append_request_event(&event_full).await?;
        storage.append_request_event(&event_warm).await?;
        storage.append_request_event(&event_none).await?;

        let recent = stored_request_events(storage.as_ref()).await?;

        // --- Event 1: assert all 7 fields survived the round-trip ---
        let got_full = recent
            .iter()
            .find(|e| e.request_id == "req_latency_full")
            .ok_or_else(|| anyhow::anyhow!("req_latency_full must round-trip"))?;

        ensure!(
            got_full.auth_ms == Some(50),
            "auth_ms mismatch: {:?}",
            got_full.auth_ms
        );
        ensure!(
            got_full.route_ms == Some(30),
            "route_ms mismatch: {:?}",
            got_full.route_ms
        );
        ensure!(
            got_full.limit_reserve_ms == Some(10),
            "limit_reserve_ms mismatch: {:?}",
            got_full.limit_reserve_ms
        );
        ensure!(
            got_full.bulkhead_wait_ms == Some(3),
            "bulkhead_wait_ms mismatch: {:?}",
            got_full.bulkhead_wait_ms
        );
        ensure!(
            got_full.dns_ms == Some(12),
            "dns_ms mismatch: {:?}",
            got_full.dns_ms
        );
        ensure!(
            got_full.connect_ms == Some(85),
            "connect_ms mismatch: {:?}",
            got_full.connect_ms
        );
        ensure!(
            got_full.connection_reused == Some(false),
            "connection_reused mismatch: {:?}",
            got_full.connection_reused
        );

        // --- Event 2: warm-pool — reused=true, dns/connect stay None (not Some(0)) ---
        let got_warm = recent
            .iter()
            .find(|e| e.request_id == "req_latency_warm")
            .ok_or_else(|| anyhow::anyhow!("req_latency_warm must round-trip"))?;

        ensure!(
            got_warm.connection_reused == Some(true),
            "connection_reused mismatch: {:?}",
            got_warm.connection_reused
        );
        ensure!(
            got_warm.dns_ms.is_none(),
            "dns_ms must be None for warm pool, got: {:?}",
            got_warm.dns_ms
        );
        ensure!(
            got_warm.connect_ms.is_none(),
            "connect_ms must be None for warm pool, got: {:?}",
            got_warm.connect_ms
        );
        ensure!(
            got_warm.auth_ms == Some(45),
            "auth_ms mismatch: {:?}",
            got_warm.auth_ms
        );
        ensure!(
            got_warm.route_ms == Some(28),
            "route_ms mismatch: {:?}",
            got_warm.route_ms
        );
        ensure!(
            got_warm.bulkhead_wait_ms == Some(2),
            "bulkhead_wait_ms mismatch: {:?}",
            got_warm.bulkhead_wait_ms
        );

        // --- Event 3: all 7 new fields must stay None after round-trip ---
        let got_none = recent
            .iter()
            .find(|e| e.request_id == "req_latency_none")
            .ok_or_else(|| anyhow::anyhow!("req_latency_none must round-trip"))?;

        ensure!(
            got_none.auth_ms.is_none(),
            "auth_ms must be None: {:?}",
            got_none.auth_ms
        );
        ensure!(
            got_none.route_ms.is_none(),
            "route_ms must be None: {:?}",
            got_none.route_ms
        );
        ensure!(
            got_none.limit_reserve_ms.is_none(),
            "limit_reserve_ms must be None: {:?}",
            got_none.limit_reserve_ms
        );
        ensure!(
            got_none.bulkhead_wait_ms.is_none(),
            "bulkhead_wait_ms must be None: {:?}",
            got_none.bulkhead_wait_ms
        );
        ensure!(
            got_none.dns_ms.is_none(),
            "dns_ms must be None: {:?}",
            got_none.dns_ms
        );
        ensure!(
            got_none.connect_ms.is_none(),
            "connect_ms must be None: {:?}",
            got_none.connect_ms
        );
        ensure!(
            got_none.connection_reused.is_none(),
            "connection_reused must be None: {:?}",
            got_none.connection_reused
        );

        Ok(())
    })
    .await
}
