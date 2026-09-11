use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{AuditEntry, AuditSink, AuditStore};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;
const BASE_TS: u64 = 1_800_700_000;

pub async fn batch_roundtrip_and_prune_boundary<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let entries = [entry(0), entry(1), entry(2), entry(3)];

        storage.append_audit_entries(&[]).await?;
        ensure!(
            storage.query_audit(None, 0, u64::MAX, 10).await?.is_empty(),
            "an empty audit batch must not write rows"
        );

        storage.append_audit_entries(&entries).await?;
        ensure!(
            storage.query_audit(None, 0, u64::MAX, 10).await? == entries,
            "audit batch must round-trip every entry in append order"
        );

        ensure!(
            storage
                .prune_audit_before((BASE_TS + 2) * KEY_SEQUENCE_SCALE, 0)
                .await?
                == 0,
            "a zero-sized audit prune batch must be a no-op"
        );
        ensure!(
            storage
                .prune_audit_before((BASE_TS + 2) * KEY_SEQUENCE_SCALE, 1)
                .await?
                == 1,
            "the first audit prune batch must remove one oldest row"
        );
        ensure!(
            storage
                .prune_audit_before((BASE_TS + 2) * KEY_SEQUENCE_SCALE, 10)
                .await?
                == 1,
            "the second audit prune batch must remove the other row below the cutoff"
        );

        let remaining = storage.query_audit(None, 0, u64::MAX, 10).await?;
        ensure!(
            remaining == vec![entries[2].clone(), entries[3].clone()],
            "audit pruning must retain the row exactly at the exclusive cutoff and newer rows"
        );
        Ok(())
    })
    .await
}

pub async fn sink_records_in_order<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditSink,
{
    with_conformance_fixture(backend, |storage| async move {
        let entries = [entry(10), entry(11)];
        storage.sink_audit(entries[0].clone());
        storage.sink_audit(entries[1].clone());

        ensure!(
            storage.query_audit(None, 0, u64::MAX, 10).await? == entries,
            "AuditSink must record every submitted entry in submission order"
        );
        Ok(())
    })
    .await
}

fn entry(offset: u64) -> AuditEntry {
    AuditEntry {
        ts: BASE_TS + offset,
        request_id: format!("audit-batch-{offset}"),
        principal_id: "audit-batch-principal".to_owned(),
        route: "messages".to_owned(),
        upstream: "anthropic_direct".to_owned(),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(offset),
        output_tokens: Some(offset + 1),
        duration_ms: offset + 2,
        agent_label: Some("audit-batch-agent".to_owned()),
        api_key_id: Some("audit-batch-key".to_owned()),
        cost_usd_micros: Some(offset + 3),
        kind: Some("request".to_owned()),
        ..Default::default()
    }
}
