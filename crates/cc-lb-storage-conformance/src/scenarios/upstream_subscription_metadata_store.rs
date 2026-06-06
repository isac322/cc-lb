use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamSubscriptionMetadataStore,
{
    put_then_get(Arc::clone(&backend)).await?;
    update_overwrites(Arc::clone(&backend)).await?;
    list_returns_all_sorted(Arc::clone(&backend)).await?;
    missing_returns_none(backend).await
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: UpstreamSubscriptionMetadataStore,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

scenario!(put_then_get, |storage| async move {
    let record = record(upstream_id(2), Some("org-a"));
    storage.put_upstream_subscription_metadata(&record).await?;
    let loaded = storage
        .get_upstream_subscription_metadata(record.upstream_id)
        .await?;
    ensure!(loaded == Some(record), "record should round-trip");
    Ok(())
});

scenario!(update_overwrites, |storage| async move {
    let mut record = record(upstream_id(3), Some("org-a"));
    storage.put_upstream_subscription_metadata(&record).await?;
    record.organization_role = Some("admin".to_owned());
    record.workspace_role = Some("developer".to_owned());
    record.observed_at_unix_millis = 200;
    record.raw_roles = Some(r#"{"updated":true}"#.to_owned());
    storage.put_upstream_subscription_metadata(&record).await?;
    let loaded = storage
        .get_upstream_subscription_metadata(record.upstream_id)
        .await?;
    ensure!(loaded == Some(record), "new observation overwrites row");
    Ok(())
});

scenario!(list_returns_all_sorted, |storage| async move {
    let first = record(upstream_id(1), Some("org-a"));
    let second = record(upstream_id(2), Some("org-b"));
    storage.put_upstream_subscription_metadata(&second).await?;
    storage.put_upstream_subscription_metadata(&first).await?;
    let listed = storage.list_upstream_subscription_metadata().await?;
    ensure!(listed == [first, second], "list should sort by upstream id");
    Ok(())
});

scenario!(missing_returns_none, |storage| async move {
    ensure!(
        storage
            .get_upstream_subscription_metadata(upstream_id(99))
            .await?
            .is_none(),
        "unknown upstream should return none"
    );
    Ok(())
});

fn record(
    upstream_id: Uuid,
    organization_uuid: Option<&str>,
) -> UpstreamSubscriptionMetadataRecord {
    UpstreamSubscriptionMetadataRecord {
        upstream_id,
        organization_uuid: organization_uuid.map(str::to_owned),
        organization_role: Some("member".to_owned()),
        workspace_role: Some("user".to_owned()),
        observed_at_unix_millis: 100,
        last_error: None,
        raw_roles: Some(r#"{"organization_role":"member"}"#.to_owned()),
        raw_bootstrap: Some(r#"{"workspace":"default"}"#.to_owned()),
    }
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
