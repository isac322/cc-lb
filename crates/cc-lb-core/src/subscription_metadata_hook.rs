use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_storage_api::{OrganizationMetadataRecord, Storage, UpstreamSubscriptionMetadataRecord};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::anthropic_metadata::{
    MetadataHttpClient, OverageGrantResponse, ProfileResponse, RolesResponse,
    fetch_claude_cli_roles, fetch_oauth_profile, fetch_overage_credit_grant,
};

const METADATA_HOOK_CHANNEL_CAPACITY: usize = 128;

#[derive(Clone)]
pub struct MetadataHookHandle {
    sender: mpsc::Sender<MetadataHookRequest>,
}

#[derive(Debug, Clone)]
pub struct MetadataHookRequest {
    pub upstream_id: Uuid,
    pub access_token: String,
    pub user_agent: String,
}

pub fn start_subscription_metadata_hook(
    storage: Arc<dyn Storage>,
    http_client: MetadataHttpClient,
    cancel: CancellationToken,
) -> MetadataHookHandle {
    let (sender, mut receiver) = mpsc::channel(METADATA_HOOK_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        loop {
            let request = tokio::select! {
                _ = cancel.cancelled() => return,
                request = receiver.recv() => request,
            };
            let Some(request) = request else {
                return;
            };
            if let Err(error) =
                process_request(storage.clone(), &http_client, &cancel, request).await
            {
                tracing::warn!(error = %error, "subscription metadata hook failed");
            }
        }
    });
    MetadataHookHandle { sender }
}

impl MetadataHookHandle {
    pub fn enqueue(&self, request: MetadataHookRequest) {
        match self.sender.try_send(request) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!("cclb_subscription_metadata_hook_dropped_total").increment(1);
                tracing::warn!("metadata hook channel full, dropping request");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("metadata hook channel closed, dropping request");
            }
        }
    }
}

async fn process_request(
    storage: Arc<dyn Storage>,
    client: &MetadataHttpClient,
    cancel: &CancellationToken,
    request: MetadataHookRequest,
) -> Result<(), String> {
    run_metadata_refresh(
        storage,
        client,
        request.upstream_id,
        &request.access_token,
        &request.user_agent,
        cancel,
    )
    .await
    .map_err(|error| error.to_string())
}

#[derive(Debug, Error)]
pub enum MetadataRefreshError {
    #[error("subscription metadata storage write failed: {0}")]
    Storage(String),
}

#[derive(Debug, Clone)]
pub struct MetadataRefreshRecords {
    pub subscription_metadata_record: UpstreamSubscriptionMetadataRecord,
    pub organization_metadata_record: Option<OrganizationMetadataRecord>,
}

pub async fn run_metadata_refresh(
    storage: Arc<dyn Storage>,
    client: &MetadataHttpClient,
    upstream_id: Uuid,
    access_token: &str,
    user_agent: &str,
    cancel: &CancellationToken,
) -> Result<(), MetadataRefreshError> {
    let records = fetch_metadata_only(client, upstream_id, access_token, user_agent, cancel).await;
    storage
        .put_upstream_subscription_metadata(&records.subscription_metadata_record)
        .await
        .map_err(|error| MetadataRefreshError::Storage(error.to_string()))?;

    if let Some(org_record) = records.organization_metadata_record.as_ref() {
        storage
            .put_organization_metadata(org_record)
            .await
            .map_err(|error| MetadataRefreshError::Storage(error.to_string()))?;
    }

    Ok(())
}

pub async fn fetch_metadata_only(
    client: &MetadataHttpClient,
    upstream_id: Uuid,
    access_token: &str,
    user_agent: &str,
    cancel: &CancellationToken,
) -> MetadataRefreshRecords {
    let observed_at_unix_millis = now_unix_millis();
    let mut errors = Vec::new();

    let profile = match fetch_oauth_profile(client, access_token, user_agent, cancel).await {
        Ok(profile) => Some(profile),
        Err(error) => {
            errors.push(format!("profile: {error}"));
            None
        }
    };
    let profile_org_uuid = profile
        .as_ref()
        .and_then(|profile| profile.organization.as_ref())
        .and_then(|organization| organization.uuid.clone());

    let roles = match fetch_claude_cli_roles(client, access_token, user_agent, cancel).await {
        Ok(roles) => Some(roles),
        Err(error) => {
            errors.push(format!("roles: {error}"));
            None
        }
    };
    let org_uuid = profile_org_uuid.or_else(|| {
        roles
            .as_ref()
            .and_then(|roles| roles.organization_uuid.clone())
    });

    let overage = if let Some(org_uuid) = org_uuid.as_deref() {
        match fetch_overage_credit_grant(client, access_token, user_agent, org_uuid, cancel).await {
            Ok(overage) => Some(overage),
            Err(error) => {
                errors.push(format!("overage_credit_grant: {error}"));
                None
            }
        }
    } else {
        None
    };

    let last_error = (!errors.is_empty()).then(|| errors.join("; "));
    let subscription_record = subscription_record(
        upstream_id,
        org_uuid.clone(),
        roles.as_ref(),
        observed_at_unix_millis,
        last_error.clone(),
    );
    let organization_record = org_uuid.and_then(|uuid| {
        organization_record(
            uuid,
            profile.as_ref(),
            overage.as_ref(),
            observed_at_unix_millis,
            last_error,
        )
    });

    MetadataRefreshRecords {
        subscription_metadata_record: subscription_record,
        organization_metadata_record: organization_record,
    }
}

fn subscription_record(
    upstream_id: Uuid,
    org_uuid: Option<String>,
    roles: Option<&RolesResponse>,
    observed_at_unix_millis: i64,
    last_error: Option<String>,
) -> UpstreamSubscriptionMetadataRecord {
    UpstreamSubscriptionMetadataRecord {
        upstream_id,
        organization_uuid: org_uuid,
        organization_role: roles.and_then(|roles| roles.organization_role.clone()),
        workspace_role: roles.and_then(|roles| roles.workspace_role.clone()),
        observed_at_unix_millis,
        last_error,
        raw_roles: roles.map(|roles| String::from_utf8_lossy(&roles.raw_body).into_owned()),
        raw_bootstrap: roles.and_then(raw_bootstrap_json),
    }
}

fn organization_record(
    organization_uuid: String,
    profile: Option<&ProfileResponse>,
    overage: Option<&OverageGrantResponse>,
    observed_at_unix_millis: i64,
    last_error: Option<String>,
) -> Option<OrganizationMetadataRecord> {
    let organization = profile.and_then(|profile| profile.organization.as_ref());
    let account = profile.and_then(|profile| profile.account.as_ref());
    Some(OrganizationMetadataRecord {
        organization_uuid,
        organization_name: organization.and_then(|organization| {
            organization
                .organization_name
                .clone()
                .or_else(|| organization.name.clone())
        }),
        organization_type: organization
            .and_then(|organization| organization.organization_type.clone()),
        rate_limit_tier: organization.and_then(|organization| organization.rate_limit_tier.clone()),
        has_extra_usage_enabled: organization
            .and_then(|organization| organization.has_extra_usage_enabled),
        billing_type: organization.and_then(|organization| organization.billing_type.clone()),
        subscription_created_at_unix_secs: organization.and_then(|organization| {
            parse_unix_secs_from_iso(&organization.subscription_created_at)
        }),
        account_email: account.and_then(|account| account.email.clone()),
        account_display_name: account.and_then(|account| account.display_name.clone()),
        account_uuid: account.and_then(|account| account.uuid.clone()),
        overage_credit_amount_minor_units: overage.and_then(overage_amount_minor_units),
        overage_credit_currency: overage.and_then(overage_currency),
        overage_credit_granted: overage.and_then(overage_granted),
        overage_credit_eligible: overage.and_then(overage_eligible),
        observed_at_unix_millis,
        last_error,
        raw_profile: profile.map(|profile| String::from_utf8_lossy(&profile.raw_body).into_owned()),
        raw_overage_grant: overage
            .map(|overage| String::from_utf8_lossy(&overage.raw_body).into_owned()),
    })
}

fn raw_bootstrap_json(roles: &RolesResponse) -> Option<String> {
    roles
        .raw_bootstrap
        .as_ref()
        .or(roles.bootstrap.as_ref())
        .and_then(|value| serde_json::to_string(value).ok())
}

fn overage_amount_minor_units(overage: &OverageGrantResponse) -> Option<i64> {
    overage.amount_minor_units.or_else(|| {
        overage
            .overage_credit
            .as_ref()
            .and_then(|credit| credit.amount_minor_units)
    })
}

fn overage_currency(overage: &OverageGrantResponse) -> Option<String> {
    overage.currency.clone().or_else(|| {
        overage
            .overage_credit
            .as_ref()
            .and_then(|credit| credit.currency.clone())
    })
}

fn overage_granted(overage: &OverageGrantResponse) -> Option<bool> {
    overage.granted.or_else(|| {
        overage
            .overage_credit
            .as_ref()
            .and_then(|credit| credit.granted)
    })
}

fn overage_eligible(overage: &OverageGrantResponse) -> Option<bool> {
    overage.eligible.or_else(|| {
        overage
            .overage_credit
            .as_ref()
            .and_then(|credit| credit.eligible)
    })
}

fn parse_unix_secs_from_iso(value: &Option<String>) -> Option<i64> {
    value.as_deref().and_then(|value| {
        value.parse::<i64>().ok().or_else(|| {
            chrono::DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|dt| dt.timestamp())
        })
    })
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::parse_unix_secs_from_iso;

    #[test]
    fn parses_numeric_subscription_created_at() {
        assert_eq!(
            parse_unix_secs_from_iso(&Some("1700000000".to_owned())),
            Some(1700000000)
        );
    }

    #[test]
    fn parses_rfc3339_subscription_created_at() {
        assert_eq!(
            parse_unix_secs_from_iso(&Some("2026-02-13T21:10:28.760744Z".to_owned())),
            Some(1771017028)
        );
    }
}
