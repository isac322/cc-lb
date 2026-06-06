use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{OrganizationMetadataRecord, OrganizationMetadataStore};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: OrganizationMetadataStore,
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
            B::Storage: OrganizationMetadataStore,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

scenario!(put_then_get, |storage| async move {
    let record = record("org-a");
    storage.put_organization_metadata(&record).await?;
    let loaded = storage.get_organization_metadata("org-a").await?;
    ensure!(loaded == Some(record), "record should round-trip");
    Ok(())
});

scenario!(update_overwrites, |storage| async move {
    let mut record = record("org-a");
    storage.put_organization_metadata(&record).await?;
    record.organization_name = Some("Updated Org".to_owned());
    record.rate_limit_tier = Some("tier-2".to_owned());
    record.overage_credit_granted = Some(false);
    record.observed_at_unix_millis = 300;
    storage.put_organization_metadata(&record).await?;
    let loaded = storage.get_organization_metadata("org-a").await?;
    ensure!(loaded == Some(record), "new observation overwrites row");
    Ok(())
});

scenario!(list_returns_all_sorted, |storage| async move {
    let first = record("org-a");
    let second = record("org-b");
    storage.put_organization_metadata(&second).await?;
    storage.put_organization_metadata(&first).await?;
    let listed = storage.list_organization_metadata().await?;
    ensure!(listed == [first, second], "list should sort by org uuid");
    Ok(())
});

scenario!(missing_returns_none, |storage| async move {
    ensure!(
        storage
            .get_organization_metadata("missing")
            .await?
            .is_none(),
        "unknown org should return none"
    );
    Ok(())
});

fn record(organization_uuid: &str) -> OrganizationMetadataRecord {
    OrganizationMetadataRecord {
        organization_uuid: organization_uuid.to_owned(),
        organization_name: Some(format!("Org {organization_uuid}")),
        organization_type: Some("claude_max".to_owned()),
        rate_limit_tier: Some("tier-1".to_owned()),
        has_extra_usage_enabled: Some(true),
        billing_type: Some("subscription".to_owned()),
        subscription_created_at_unix_secs: Some(1_700_000_000),
        account_email: Some("local@example.com".to_owned()),
        account_display_name: Some("Local User".to_owned()),
        account_uuid: Some("acct-a".to_owned()),
        overage_credit_amount_minor_units: Some(1000),
        overage_credit_currency: Some("USD".to_owned()),
        overage_credit_granted: Some(true),
        overage_credit_eligible: Some(true),
        observed_at_unix_millis: 200,
        last_error: None,
        raw_profile: Some(r#"{"organization":{"uuid":"org-a"}}"#.to_owned()),
        raw_overage_grant: Some(r#"{"eligible":true}"#.to_owned()),
    }
}
