use async_trait::async_trait;
use cc_lb_storage_api::{OrganizationMetadataRecord, OrganizationMetadataStore, StorageResult};
use sqlx::{Row, postgres::PgRow};

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

#[async_trait]
impl OrganizationMetadataStore for PostgresStorage {
    async fn put_organization_metadata(
        &self,
        record: &OrganizationMetadataRecord,
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO organization_metadata_v1 \
             (organization_uuid, organization_name, organization_type, rate_limit_tier, has_extra_usage_enabled, \
              billing_type, subscription_created_at_unix_secs, account_email, account_display_name, account_uuid, \
              overage_credit_amount_minor_units, overage_credit_currency, overage_credit_granted, overage_credit_eligible, \
              observed_at_unix_millis, last_error, raw_profile, raw_overage_grant) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) \
             ON CONFLICT (organization_uuid) DO UPDATE SET \
              organization_name = EXCLUDED.organization_name, \
              organization_type = EXCLUDED.organization_type, \
              rate_limit_tier = EXCLUDED.rate_limit_tier, \
              has_extra_usage_enabled = EXCLUDED.has_extra_usage_enabled, \
              billing_type = EXCLUDED.billing_type, \
              subscription_created_at_unix_secs = EXCLUDED.subscription_created_at_unix_secs, \
              account_email = EXCLUDED.account_email, \
              account_display_name = EXCLUDED.account_display_name, \
              account_uuid = EXCLUDED.account_uuid, \
              overage_credit_amount_minor_units = EXCLUDED.overage_credit_amount_minor_units, \
              overage_credit_currency = EXCLUDED.overage_credit_currency, \
              overage_credit_granted = EXCLUDED.overage_credit_granted, \
              overage_credit_eligible = EXCLUDED.overage_credit_eligible, \
              observed_at_unix_millis = EXCLUDED.observed_at_unix_millis, \
              last_error = EXCLUDED.last_error, \
              raw_profile = EXCLUDED.raw_profile, \
              raw_overage_grant = EXCLUDED.raw_overage_grant",
        )
        .bind(&record.organization_uuid)
        .bind(&record.organization_name)
        .bind(&record.organization_type)
        .bind(&record.rate_limit_tier)
        .bind(record.has_extra_usage_enabled)
        .bind(&record.billing_type)
        .bind(record.subscription_created_at_unix_secs)
        .bind(&record.account_email)
        .bind(&record.account_display_name)
        .bind(&record.account_uuid)
        .bind(record.overage_credit_amount_minor_units)
        .bind(&record.overage_credit_currency)
        .bind(record.overage_credit_granted)
        .bind(record.overage_credit_eligible)
        .bind(record.observed_at_unix_millis)
        .bind(&record.last_error)
        .bind(&record.raw_profile)
        .bind(&record.raw_overage_grant)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn get_organization_metadata(
        &self,
        organization_uuid: &str,
    ) -> StorageResult<Option<OrganizationMetadataRecord>> {
        let row = sqlx::query(
            "SELECT organization_uuid, organization_name, organization_type, rate_limit_tier, has_extra_usage_enabled, \
             billing_type, subscription_created_at_unix_secs, account_email, account_display_name, account_uuid, \
             overage_credit_amount_minor_units, overage_credit_currency, overage_credit_granted, overage_credit_eligible, \
             observed_at_unix_millis, last_error, raw_profile, raw_overage_grant \
             FROM organization_metadata_v1 WHERE organization_uuid = $1",
        )
            .bind(organization_uuid)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }

    async fn list_organization_metadata(&self) -> StorageResult<Vec<OrganizationMetadataRecord>> {
        let rows = sqlx::query(
            "SELECT organization_uuid, organization_name, organization_type, rate_limit_tier, has_extra_usage_enabled, \
             billing_type, subscription_created_at_unix_secs, account_email, account_display_name, account_uuid, \
             overage_credit_amount_minor_units, overage_credit_currency, overage_credit_granted, overage_credit_eligible, \
             observed_at_unix_millis, last_error, raw_profile, raw_overage_grant \
             FROM organization_metadata_v1 ORDER BY organization_uuid ASC",
        )
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        rows.into_iter().map(row_to_record).collect()
    }
}

fn row_to_record(row: PgRow) -> StorageResult<OrganizationMetadataRecord> {
    Ok(OrganizationMetadataRecord {
        organization_uuid: row.try_get("organization_uuid").map_err(map_sqlx_error)?,
        organization_name: row.try_get("organization_name").map_err(map_sqlx_error)?,
        organization_type: row.try_get("organization_type").map_err(map_sqlx_error)?,
        rate_limit_tier: row.try_get("rate_limit_tier").map_err(map_sqlx_error)?,
        has_extra_usage_enabled: row
            .try_get("has_extra_usage_enabled")
            .map_err(map_sqlx_error)?,
        billing_type: row.try_get("billing_type").map_err(map_sqlx_error)?,
        subscription_created_at_unix_secs: row
            .try_get("subscription_created_at_unix_secs")
            .map_err(map_sqlx_error)?,
        account_email: row.try_get("account_email").map_err(map_sqlx_error)?,
        account_display_name: row
            .try_get("account_display_name")
            .map_err(map_sqlx_error)?,
        account_uuid: row.try_get("account_uuid").map_err(map_sqlx_error)?,
        overage_credit_amount_minor_units: row
            .try_get("overage_credit_amount_minor_units")
            .map_err(map_sqlx_error)?,
        overage_credit_currency: row
            .try_get("overage_credit_currency")
            .map_err(map_sqlx_error)?,
        overage_credit_granted: row
            .try_get("overage_credit_granted")
            .map_err(map_sqlx_error)?,
        overage_credit_eligible: row
            .try_get("overage_credit_eligible")
            .map_err(map_sqlx_error)?,
        observed_at_unix_millis: row
            .try_get("observed_at_unix_millis")
            .map_err(map_sqlx_error)?,
        last_error: row.try_get("last_error").map_err(map_sqlx_error)?,
        raw_profile: row.try_get("raw_profile").map_err(map_sqlx_error)?,
        raw_overage_grant: row.try_get("raw_overage_grant").map_err(map_sqlx_error)?,
    })
}
