use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::StorageResult;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationMetadataRecord {
    pub organization_uuid: String,
    pub organization_name: Option<String>,
    pub organization_type: Option<String>,
    pub rate_limit_tier: Option<String>,
    pub seat_tier: Option<String>,
    pub has_extra_usage_enabled: Option<bool>,
    pub billing_type: Option<String>,
    pub subscription_created_at_unix_secs: Option<i64>,
    pub account_email: Option<String>,
    pub account_display_name: Option<String>,
    pub account_uuid: Option<String>,
    pub overage_credit_amount_minor_units: Option<i64>,
    pub overage_credit_currency: Option<String>,
    pub overage_credit_granted: Option<bool>,
    pub overage_credit_eligible: Option<bool>,
    pub observed_at_unix_millis: i64,
    pub last_error: Option<String>,
    pub raw_profile: Option<String>,
    pub raw_overage_grant: Option<String>,
}

#[async_trait]
pub trait OrganizationMetadataStore: Send + Sync {
    async fn put_organization_metadata(
        &self,
        record: &OrganizationMetadataRecord,
    ) -> StorageResult<()>;

    async fn get_organization_metadata(
        &self,
        organization_uuid: &str,
    ) -> StorageResult<Option<OrganizationMetadataRecord>>;

    async fn list_organization_metadata(&self) -> StorageResult<Vec<OrganizationMetadataRecord>>;
}
