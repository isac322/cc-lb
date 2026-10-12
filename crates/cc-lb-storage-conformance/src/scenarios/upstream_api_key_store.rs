//! `UpstreamApiKeyStore` conformance (STO-04) plus the storage-level QA items
//! UPD-08, STO-05, STO-06, ACC-06, ACC-08, POL-05, POL-07, CONC-07, and the
//! usage-poll claim (one owner per upstream, guarded completion, lease
//! expiry). Every scenario runs unchanged against SQLite and Postgres.

use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyCapabilities, ApiKeyClass, ApiKeyInference, ApiKeyOrgCapability,
    ApiKeyResolution, ApiKeyType, ApiKeyUsagePollOutcome, ApiKeyWorkspace, ApiKeyWorkspaceMode,
    ApiUsageAttribution, RemediationCode, StorageError, UpstreamApiKeyDetailsRecord,
    UpstreamApiKeyStore, UpstreamApiUsageBucketQuery, UpstreamApiUsageBucketRecord,
    UpstreamApiUsageBucketWrite, UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStore,
    UpstreamUpdate,
};
use tokio::sync::Barrier;
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

const HOUR: u32 = 3_600;
const DAY: u32 = 86_400;
/// 2026-10-01T00:00:00Z: UTC midnight, so hour and day buckets both align.
const MONTH_START: u64 = 1_790_812_800;

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    sto05_upd08_workspace_id_tri_state_bumps_revision(Arc::clone(&backend)).await?;
    sto04_details_create_get_list_update_conflict(Arc::clone(&backend)).await?;
    acc08_capabilities_json_roundtrip_all_values(Arc::clone(&backend)).await?;
    conc07_access_and_poll_writes_are_column_scoped(Arc::clone(&backend)).await?;
    conc07_concurrent_access_and_poll_writes_both_survive(Arc::clone(&backend)).await?;
    conc07_same_second_recheck_rejects_stale_demotion(Arc::clone(&backend)).await?;
    pol07_failure_counter_and_schedule_persist(Arc::clone(&backend)).await?;
    pol05_bucket_upsert_is_idempotent_and_latest_wins(Arc::clone(&backend)).await?;
    sto04_poll_candidate_query_filters(Arc::clone(&backend)).await?;
    sto04_poll_candidates_rotate_past_the_limit(Arc::clone(&backend)).await?;
    acc06_soft_and_hard_delete_semantics(Arc::clone(&backend)).await?;
    sto06_unix_seconds_roundtrip_exactly(Arc::clone(&backend)).await?;
    prune_upstream_api_usage_is_bounded(Arc::clone(&backend)).await?;
    conc_poll_claim_two_claimers_one_wins(Arc::clone(&backend)).await?;
    conc_poll_claim_stale_completion_rejected(Arc::clone(&backend)).await?;
    conc_poll_claim_and_completion_reject_disabled_upstream(Arc::clone(&backend)).await?;
    conc_poll_claim_lease_expiry_is_reclaimable(Arc::clone(&backend)).await?;
    pol_completion_never_shortens_retry_after(Arc::clone(&backend)).await?;
    conc_access_write_releases_poll_claim(Arc::clone(&backend)).await?;
    reset_poll_state_keeps_usage_rows(backend).await
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

scenario!(
    sto05_upd08_workspace_id_tri_state_bumps_revision,
    |storage| async move {
        let store = storage.as_ref();
        let created = UpstreamStore::create(
            store,
            UpstreamCreate {
                workspace_id: Some("wrkspc_initial".to_owned()),
                ..api_key_create("sto05-workspace")
            },
        )
        .await?;
        ensure!(
            created.workspace_id.as_deref() == Some("wrkspc_initial"),
            "create persists workspace_id"
        );
        ensure!(
            UpstreamStore::get_by_id(store, created.id).await?.as_ref() == Some(&created),
            "get_by_id reads workspace_id back"
        );

        // None: unchanged (another spec field still bumps the revision).
        let unchanged = UpstreamStore::update(
            store,
            created.id,
            created.revision,
            UpstreamUpdate {
                enabled: Some(true),
                workspace_id: None,
                ..UpstreamUpdate::default()
            },
        )
        .await?;
        ensure!(
            unchanged.workspace_id.as_deref() == Some("wrkspc_initial"),
            "None leaves workspace_id unchanged"
        );

        // Some(Some): set; the workspace alone bumps spec_revision by one.
        let set = UpstreamStore::update(
            store,
            created.id,
            unchanged.revision,
            UpstreamUpdate {
                workspace_id: Some(Some("wrkspc_next".to_owned())),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
        ensure!(
            set.workspace_id.as_deref() == Some("wrkspc_next"),
            "Some(Some) sets workspace_id"
        );
        ensure!(
            set.revision == unchanged.revision + 1,
            "workspace update bumps spec_revision by one"
        );

        // Some(None): cleared, through `update_spec` this time.
        let cleared = UpstreamStore::update_spec(
            store,
            created.id,
            set.revision,
            UpstreamUpdate {
                workspace_id: Some(None),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
        ensure!(
            cleared.workspace_id.is_none(),
            "Some(None) clears workspace_id"
        );
        ensure!(
            cleared.revision == set.revision + 1,
            "workspace clear bumps spec_revision by one"
        );

        // The reconcile hash input (id, revision) observed through `list`
        // changes with the workspace write.
        let listed = UpstreamStore::list(store, None, 10).await?;
        ensure!(
            listed
                .iter()
                .any(|record| record.id == cleared.id && record.revision == cleared.revision),
            "list exposes the bumped revision"
        );

        let stale = UpstreamStore::update(
            store,
            created.id,
            set.revision,
            UpstreamUpdate {
                workspace_id: Some(Some("wrkspc_stale".to_owned())),
                ..UpstreamUpdate::default()
            },
        )
        .await;
        ensure!(
            matches!(stale, Err(StorageError::Conflict { .. })),
            "stale workspace update conflicts"
        );
        Ok(())
    }
);

scenario!(
    sto04_details_create_get_list_update_conflict,
    |storage| async move {
        let store = storage.as_ref();
        let plain = create_api_key_upstream(store, "sto04-plain").await?;
        ensure!(
            UpstreamApiKeyStore::get_api_key_details(store, plain.id)
                .await?
                .is_none(),
            "create without access writes no details row"
        );

        let created_access = access(Some(1_800_000_000));
        let with_access = UpstreamStore::create(
            store,
            UpstreamCreate {
                api_key_access: Some(created_access.clone()),
                ..api_key_create("sto04-access")
            },
        )
        .await?;
        let details = require_details(store, with_access.id).await?;
        ensure_access_matches(&details, &created_access)?;
        ensure!(
            details.usage_poll_failures == 0
                && details.usage_next_poll_after_unix_secs.is_none()
                && details.usage_last_success_at_unix_secs.is_none()
                && details.usage_last_attempt_at_unix_secs.is_none(),
            "fresh details carry no poll state"
        );

        let mut rechecked = access(Some(1_800_000_600));
        rechecked.verified = false;
        rechecked.key_class = ApiKeyClass::WorkspaceBound;
        rechecked.api_key_id = None;
        rechecked.remediation = vec![RemediationCode::KeyWorkspaceScopedUsageUnavailable];
        rechecked.usage_report_state = Some(ApiKeyOrgCapability::NotApplicable);
        rechecked.last_error = Some("scrubbed failure".to_owned());
        UpstreamApiKeyStore::write_api_key_access(
            store,
            with_access.id,
            with_access.revision,
            &rechecked,
        )
        .await?;
        ensure_access_matches(&require_details(store, with_access.id).await?, &rechecked)?;
        ensure!(
            UpstreamStore::get_by_id(store, with_access.id)
                .await?
                .map(|record| record.revision)
                == Some(with_access.revision),
            "details writes never bump spec_revision"
        );

        // Insert-if-missing on an upstream with no details row.
        UpstreamApiKeyStore::write_api_key_access(store, plain.id, plain.revision, &rechecked)
            .await?;
        ensure_access_matches(&require_details(store, plain.id).await?, &rechecked)?;

        let stale = UpstreamApiKeyStore::write_api_key_access(
            store,
            with_access.id,
            with_access.revision + 1,
            &created_access,
        )
        .await;
        ensure!(
            matches!(stale, Err(StorageError::Conflict { .. })),
            "stale revision conflicts"
        );
        ensure_access_matches(&require_details(store, with_access.id).await?, &rechecked)?;

        UpstreamApiKeyStore::record_api_key_probe_failure(
            store,
            with_access.id,
            with_access.revision,
            "connect timeout",
            &[RemediationCode::ProbeUnreachable],
            1_800_000_900,
        )
        .await?;
        let failed = require_details(store, with_access.id).await?;
        ensure!(
            failed.last_error.as_deref() == Some("connect timeout")
                && failed.remediation == [RemediationCode::ProbeUnreachable]
                && failed.last_attempt_at_unix_secs == Some(1_800_000_900),
            "probe failure writes error, remediation, and attempt time"
        );
        ensure!(
            failed.verified == rechecked.verified
                && failed.key_class == rechecked.key_class
                && failed.capabilities == rechecked.capabilities
                && failed.checked_at_unix_secs == rechecked.checked_at_unix_secs
                && failed.usage_report_state == rechecked.usage_report_state,
            "probe failure leaves the capability snapshot"
        );

        let fresh = create_api_key_upstream(store, "sto04-probe-only").await?;
        UpstreamApiKeyStore::record_api_key_probe_failure(
            store,
            fresh.id,
            fresh.revision,
            "unreachable",
            &[RemediationCode::ProbeUnreachable],
            1_800_001_000,
        )
        .await?;
        let probe_only = require_details(store, fresh.id).await?;
        ensure!(
            !probe_only.verified
                && probe_only.key_class == ApiKeyClass::Unknown
                && probe_only.capabilities == ApiKeyCapabilities::not_checked()
                && probe_only.checked_at_unix_secs.is_none(),
            "probe failure on a missing row inserts not-checked defaults"
        );

        let listed = UpstreamApiKeyStore::list_api_key_details(
            store,
            &[with_access.id, Uuid::new_v4(), plain.id],
        )
        .await?;
        let mut expected_ids = vec![with_access.id, plain.id];
        expected_ids.sort();
        ensure!(
            listed
                .iter()
                .map(|details| details.upstream_id)
                .collect::<Vec<_>>()
                == expected_ids,
            "list returns existing rows ordered by upstream id"
        );
        ensure!(
            UpstreamApiKeyStore::list_api_key_details(store, &[])
                .await?
                .is_empty(),
            "empty id list returns nothing"
        );
        Ok(())
    }
);

scenario!(
    acc08_capabilities_json_roundtrip_all_values,
    |storage| async move {
        let store = storage.as_ref();
        let inference = [
            ApiKeyInference::Ok,
            ApiKeyInference::Denied,
            ApiKeyInference::Unverified,
        ];
        let modes = [
            ApiKeyWorkspaceMode::Bound,
            ApiKeyWorkspaceMode::Required,
            ApiKeyWorkspaceMode::Selected,
        ];
        let org = [
            ApiKeyOrgCapability::Ok,
            ApiKeyOrgCapability::Denied,
            ApiKeyOrgCapability::Error,
            ApiKeyOrgCapability::NotApplicable,
        ];
        let resolution = [
            ApiKeyResolution::Resolved,
            ApiKeyResolution::Unresolved,
            ApiKeyResolution::NotApplicable,
        ];
        let key_types = [
            None,
            Some(ApiKeyType::Personal),
            Some(ApiKeyType::ServiceAccount),
            Some(ApiKeyType::Workspace),
        ];
        let classes = [
            ApiKeyClass::Admin,
            ApiKeyClass::Enterprise,
            ApiKeyClass::OauthToken,
            ApiKeyClass::WorkspaceBound,
            ApiKeyClass::OrgScoped,
            ApiKeyClass::Unknown,
        ];
        let report_states = [
            None,
            Some(ApiKeyOrgCapability::Ok),
            Some(ApiKeyOrgCapability::Denied),
            Some(ApiKeyOrgCapability::Error),
            Some(ApiKeyOrgCapability::NotApplicable),
        ];
        for index in 0..classes.len() {
            let upstream = create_api_key_upstream(store, &format!("acc08-{index}")).await?;
            let write = ApiKeyAccessWrite {
                verified: index % 2 == 0,
                key_class: classes[index],
                key_hint: format!("sk-ant-api03-...{index:04}"),
                api_key_id: (index % 3 != 0).then(|| format!("apikey_{index}")),
                needs_workspace: index % 2 == 1,
                capabilities: ApiKeyCapabilities {
                    inference: inference[index % inference.len()],
                    workspace: ApiKeyWorkspace {
                        mode: modes[index % modes.len()],
                        id: (index % 2 == 0).then(|| format!("wrkspc_{index}")),
                        name: (index % 3 == 0).then(|| "작업 공간 \u{1F511} \"q\"".to_owned()),
                    },
                    workspace_list: org[index % org.len()],
                    api_key_resolution: resolution[index % resolution.len()],
                    key_type: key_types[index % key_types.len()],
                },
                remediation: if index == 0 {
                    RemediationCode::ALL.to_vec()
                } else {
                    Vec::new()
                },
                usage_report_state: report_states[index % report_states.len()],
                last_error: Some(format!(
                    "키 오류 {index} — ✓ \u{1F511} \"quoted\"\n\ttab \\ backslash"
                )),
                checked_at_unix_secs: Some(1_800_000_000 + index as u64),
                last_attempt_at_unix_secs: 1_800_000_000 + index as u64,
            };
            UpstreamApiKeyStore::write_api_key_access(
                store,
                upstream.id,
                upstream.revision,
                &write,
            )
            .await?;
            ensure_access_matches(&require_details(store, upstream.id).await?, &write)?;
        }
        Ok(())
    }
);

scenario!(
    conc07_access_and_poll_writes_are_column_scoped,
    |storage| async move {
        let store = storage.as_ref();
        let upstream = create_api_key_upstream(store, "conc07-sequential").await?;
        let first = access(Some(1_800_000_000));
        UpstreamApiKeyStore::write_api_key_access(store, upstream.id, upstream.revision, &first)
            .await?;
        let first_revision = require_details(store, upstream.id).await?.access_revision;

        // A Re-check lands between the poller's read and its write.
        let mut recheck = access(Some(1_800_000_500));
        recheck.usage_report_state = Some(ApiKeyOrgCapability::Ok);
        recheck.key_hint = "sk-ant-api03-...NEW1".to_owned();
        UpstreamApiKeyStore::write_api_key_access(store, upstream.id, upstream.revision, &recheck)
            .await?;
        let recheck_revision = require_details(store, upstream.id).await?.access_revision;

        let demote = ApiKeyUsagePollOutcome {
            usage_report_state: Some(ApiKeyOrgCapability::Denied),
            access_revision_seen: first_revision,
            consecutive_failures: 0,
            next_poll_after_unix_secs: Some(1_800_000_800),
            ..poll_outcome(1_800_000_400)
        };
        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            upstream.id,
            upstream.revision,
            &demote,
        )
        .await?;
        let details = require_details(store, upstream.id).await?;
        ensure_access_matches(&details, &recheck)?;
        ensure!(
            details.usage_last_attempt_at_unix_secs == Some(1_800_000_400)
                && details.usage_next_poll_after_unix_secs == Some(1_800_000_800),
            "poll-state columns written even when the demotion is skipped"
        );

        // With an unchanged `access_revision` the demotion applies.
        let demote = ApiKeyUsagePollOutcome {
            access_revision_seen: recheck_revision,
            ..demote
        };
        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            upstream.id,
            upstream.revision,
            &demote,
        )
        .await?;
        let demoted = require_details(store, upstream.id).await?;
        ensure!(
            demoted.usage_report_state == Some(ApiKeyOrgCapability::Denied),
            "demotion applies when access_revision is unchanged"
        );
        ensure!(
            demoted.key_hint == recheck.key_hint
                && demoted.capabilities == recheck.capabilities
                && demoted.checked_at_unix_secs == recheck.checked_at_unix_secs,
            "poll write leaves capability columns"
        );

        // A later Re-check leaves the poll-state columns.
        UpstreamApiKeyStore::write_api_key_access(store, upstream.id, upstream.revision, &first)
            .await?;
        let after = require_details(store, upstream.id).await?;
        ensure!(
            after.usage_last_attempt_at_unix_secs == Some(1_800_000_400)
                && after.usage_next_poll_after_unix_secs == Some(1_800_000_800),
            "access write leaves poll-state columns"
        );
        Ok(())
    }
);

scenario!(
    conc07_concurrent_access_and_poll_writes_both_survive,
    |storage| async move {
        let upstream = create_api_key_upstream(storage.as_ref(), "conc07-concurrent").await?;
        let seed = access(Some(1_800_000_000));
        UpstreamApiKeyStore::write_api_key_access(
            storage.as_ref(),
            upstream.id,
            upstream.revision,
            &seed,
        )
        .await?;
        let seed_revision = require_details(storage.as_ref(), upstream.id)
            .await?
            .access_revision;

        let mut recheck = access(Some(1_800_000_500));
        recheck.key_hint = "sk-ant-api03-...CONC".to_owned();
        recheck.usage_report_state = Some(ApiKeyOrgCapability::Ok);
        let poll = ApiKeyUsagePollOutcome {
            usage_report_state: Some(ApiKeyOrgCapability::Denied),
            access_revision_seen: seed_revision,
            consecutive_failures: 1,
            next_poll_after_unix_secs: Some(1_800_000_900),
            buckets: vec![bucket(
                ApiUsageAttribution::ApiKey,
                "apikey_conc",
                HOUR,
                MONTH_START,
                "claude-sonnet-4-5",
                10,
            )],
            ..poll_outcome(1_800_000_450)
        };

        let barrier = Arc::new(Barrier::new(2));
        let recheck_task = {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            let recheck = recheck.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                UpstreamApiKeyStore::write_api_key_access(
                    storage.as_ref(),
                    upstream.id,
                    upstream.revision,
                    &recheck,
                )
                .await
            })
        };
        let poll_task = {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            let poll = poll.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
                    storage.as_ref(),
                    upstream.id,
                    upstream.revision,
                    &poll,
                )
                .await
            })
        };
        recheck_task.await??;
        poll_task.await??;

        let details = require_details(storage.as_ref(), upstream.id).await?;
        // Either order: the Re-check's columns survive and, because it bumped
        // `access_revision`, its usage_report_state wins over the demotion.
        ensure_access_matches(&details, &recheck)?;
        ensure!(
            details.usage_poll_failures == 1
                && details.usage_next_poll_after_unix_secs == Some(1_800_000_900)
                && details.usage_last_attempt_at_unix_secs == Some(1_800_000_450),
            "poll-state columns survive the concurrent Re-check"
        );
        let buckets = query_all(
            storage.as_ref(),
            upstream.id,
            ApiUsageAttribution::ApiKey,
            "apikey_conc",
            HOUR,
        )
        .await?;
        ensure!(buckets.len() == 1, "poll buckets survive");
        Ok(())
    }
);

scenario!(
    conc07_same_second_recheck_rejects_stale_demotion,
    |storage| async move {
        let store = storage.as_ref();
        let checked_at = Some(1_800_000_000);
        let upstream = create_api_key_upstream(store, "conc07-same-second").await?;
        UpstreamApiKeyStore::write_api_key_access(
            store,
            upstream.id,
            upstream.revision,
            &access(checked_at),
        )
        .await?;
        // The poller reads its candidate...
        let seen = UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
            store,
            1_800_000_000,
            Some(upstream.id),
            10,
        )
        .await?
        .first()
        .and_then(|candidate| candidate.details.as_ref())
        .map(|details| details.access_revision);
        let Some(seen) = seen else {
            anyhow::bail!("org-scoped upstream with details is a poll candidate");
        };

        // ...then a Re-check in the same second restores `ok` with an
        // identical `checked_at`.
        let recheck = access(checked_at);
        UpstreamApiKeyStore::write_api_key_access(store, upstream.id, upstream.revision, &recheck)
            .await?;
        let rechecked = require_details(store, upstream.id).await?;
        ensure!(
            rechecked.checked_at_unix_secs == checked_at && rechecked.access_revision > seen,
            "an access write with an identical timestamp still bumps access_revision"
        );

        // The stale 401/403 demotion is rejected; its poll state still lands.
        let stale = ApiKeyUsagePollOutcome {
            usage_report_state: Some(ApiKeyOrgCapability::Denied),
            access_revision_seen: seen,
            consecutive_failures: 1,
            next_poll_after_unix_secs: Some(1_800_000_300),
            ..poll_outcome(1_800_000_000)
        };
        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            upstream.id,
            upstream.revision,
            &stale,
        )
        .await?;
        let details = require_details(store, upstream.id).await?;
        ensure_access_matches(&details, &recheck)?;
        ensure!(
            details.usage_poll_failures == 1
                && details.usage_next_poll_after_unix_secs == Some(1_800_000_300)
                && details.access_revision == rechecked.access_revision,
            "stale demotion skips only usage_report_state and leaves access_revision"
        );

        // A legacy upstream polled before any details row existed carries
        // revision 0; a Re-check that inserts the row invalidates it.
        let legacy = create_api_key_upstream(store, "conc07-same-second-legacy").await?;
        let legacy_seen = UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
            store,
            1_800_000_000,
            Some(legacy.id),
            10,
        )
        .await?;
        ensure!(
            legacy_seen.len() == 1 && legacy_seen.iter().all(|c| c.details.is_none()),
            "legacy candidate has no details row"
        );
        UpstreamApiKeyStore::write_api_key_access(
            store,
            legacy.id,
            legacy.revision,
            &access(checked_at),
        )
        .await?;
        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            legacy.id,
            legacy.revision,
            &ApiKeyUsagePollOutcome {
                access_revision_seen: 0,
                ..stale.clone()
            },
        )
        .await?;
        ensure!(
            require_details(store, legacy.id).await?.usage_report_state
                == Some(ApiKeyOrgCapability::Ok),
            "Re-check that inserted the details row wins over a pre-row demotion"
        );

        // Without an intervening access write, the poller's own inserted row
        // starts at revision 0 and the demotion applies.
        let untouched = create_api_key_upstream(store, "conc07-same-second-untouched").await?;
        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            untouched.id,
            untouched.revision,
            &ApiKeyUsagePollOutcome {
                access_revision_seen: 0,
                ..stale
            },
        )
        .await?;
        let untouched = require_details(store, untouched.id).await?;
        ensure!(
            untouched.access_revision == 0
                && untouched.usage_report_state == Some(ApiKeyOrgCapability::Denied),
            "demotion applies to a row no access write has touched"
        );
        Ok(())
    }
);

scenario!(
    pol07_failure_counter_and_schedule_persist,
    |storage| async move {
        let store = storage.as_ref();
        let upstream = create_api_key_upstream(store, "pol07").await?;
        UpstreamApiKeyStore::write_api_key_access(
            store,
            upstream.id,
            upstream.revision,
            &access(Some(1_800_000_000)),
        )
        .await?;

        let success = ApiKeyUsagePollOutcome {
            last_success_at_unix_secs: Some(1_800_000_100),
            next_poll_after_unix_secs: Some(1_800_000_400),
            ..poll_outcome(1_800_000_100)
        };
        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            upstream.id,
            upstream.revision,
            &success,
        )
        .await?;

        for (failures, attempted) in [(1_u32, 1_800_000_400_u64), (2, 1_800_000_700)] {
            let failure = ApiKeyUsagePollOutcome {
                consecutive_failures: failures,
                next_poll_after_unix_secs: Some(attempted + 300),
                ..poll_outcome(attempted)
            };
            UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
                store,
                upstream.id,
                upstream.revision,
                &failure,
            )
            .await?;
        }
        let failed = require_details(store, upstream.id).await?;
        ensure!(
            failed.usage_poll_failures == 2
                && failed.usage_last_success_at_unix_secs == Some(1_800_000_100)
                && failed.usage_last_attempt_at_unix_secs == Some(1_800_000_700)
                && failed.usage_next_poll_after_unix_secs == Some(1_800_001_000)
                && failed.usage_report_state == Some(ApiKeyOrgCapability::Ok),
            "consecutive failures persist; last success is kept; state stays ok"
        );

        // `next_poll_after` gates the candidate query.
        let before = UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
            store,
            1_800_000_999,
            Some(upstream.id),
            10,
        )
        .await?;
        ensure!(before.is_empty(), "not due before next_poll_after");
        let due = UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
            store,
            1_800_001_000,
            Some(upstream.id),
            10,
        )
        .await?;
        ensure!(
            due.len() == 1
                && due[0]
                    .details
                    .as_ref()
                    .map(|details| details.usage_poll_failures)
                    == Some(2),
            "due at next_poll_after with the persisted counter"
        );

        let recovered = ApiKeyUsagePollOutcome {
            last_success_at_unix_secs: Some(1_800_001_000),
            next_poll_after_unix_secs: None,
            ..poll_outcome(1_800_001_000)
        };
        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            upstream.id,
            upstream.revision,
            &recovered,
        )
        .await?;
        let recovered = require_details(store, upstream.id).await?;
        ensure!(
            recovered.usage_poll_failures == 0
                && recovered.usage_last_success_at_unix_secs == Some(1_800_001_000)
                && recovered.usage_next_poll_after_unix_secs.is_none(),
            "the next success clears the counter; None clears the schedule"
        );
        Ok(())
    }
);

scenario!(
    pol05_bucket_upsert_is_idempotent_and_latest_wins,
    |storage| async move {
        let store = storage.as_ref();
        let upstream = create_api_key_upstream(store, "pol05").await?;
        let other = create_api_key_upstream(store, "pol05-other").await?;
        let buckets = |tokens: u64| {
            vec![
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_a",
                    HOUR,
                    MONTH_START,
                    "claude-sonnet-4-5",
                    tokens,
                ),
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_a",
                    HOUR,
                    MONTH_START,
                    "",
                    tokens + 1,
                ),
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_a",
                    HOUR,
                    MONTH_START + 3_600,
                    "claude-sonnet-4-5",
                    tokens + 2,
                ),
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_a",
                    DAY,
                    MONTH_START,
                    "claude-sonnet-4-5",
                    tokens + 3,
                ),
                bucket(
                    ApiUsageAttribution::Workspace,
                    "wrkspc_a",
                    DAY,
                    MONTH_START,
                    "claude-sonnet-4-5",
                    tokens + 4,
                ),
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_b",
                    HOUR,
                    MONTH_START,
                    "claude-sonnet-4-5",
                    tokens + 5,
                ),
            ]
        };
        for (tick, tokens) in [
            (1_800_000_000_u64, 100_u64),
            (1_800_000_300, 100),
            (1_800_000_600, 900),
        ] {
            let outcome = ApiKeyUsagePollOutcome {
                buckets: buckets(tokens),
                last_success_at_unix_secs: Some(tick),
                ..poll_outcome(tick)
            };
            UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
                store,
                upstream.id,
                upstream.revision,
                &outcome,
            )
            .await?;
        }
        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            other.id,
            other.revision,
            &ApiKeyUsagePollOutcome {
                buckets: buckets(7),
                ..poll_outcome(1_800_000_000)
            },
        )
        .await?;

        let hourly = query_all(
            store,
            upstream.id,
            ApiUsageAttribution::ApiKey,
            "apikey_a",
            HOUR,
        )
        .await?;
        let expected_hourly = vec![
            record(
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_a",
                    HOUR,
                    MONTH_START,
                    "",
                    901,
                ),
                1_800_000_600,
            ),
            record(
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_a",
                    HOUR,
                    MONTH_START,
                    "claude-sonnet-4-5",
                    900,
                ),
                1_800_000_600,
            ),
            record(
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_a",
                    HOUR,
                    MONTH_START + 3_600,
                    "claude-sonnet-4-5",
                    902,
                ),
                1_800_000_600,
            ),
        ];
        ensure!(
            hourly == expected_hourly,
            "no duplicate rows and the latest value wins: {hourly:?}"
        );
        let daily = query_all(
            store,
            upstream.id,
            ApiUsageAttribution::ApiKey,
            "apikey_a",
            DAY,
        )
        .await?;
        ensure!(
            daily.len() == 1 && daily[0].uncached_input_tokens == 903,
            "1d bucket upserted separately"
        );
        let workspace = query_all(
            store,
            upstream.id,
            ApiUsageAttribution::Workspace,
            "wrkspc_a",
            DAY,
        )
        .await?;
        ensure!(
            workspace.len() == 1 && workspace[0].uncached_input_tokens == 904,
            "workspace attribution is its own series"
        );
        let other_key = query_all(
            store,
            upstream.id,
            ApiUsageAttribution::ApiKey,
            "apikey_b",
            HOUR,
        )
        .await?;
        ensure!(
            other_key.len() == 1 && other_key[0].uncached_input_tokens == 905,
            "query filters by the subject (current api_key_id)"
        );
        let other_upstream = query_all(
            store,
            other.id,
            ApiUsageAttribution::ApiKey,
            "apikey_a",
            HOUR,
        )
        .await?;
        ensure!(
            other_upstream.len() == 3
                && other_upstream
                    .iter()
                    .all(|row| row.uncached_input_tokens < 100),
            "query filters by upstream"
        );

        let ranged = UpstreamApiKeyStore::query_upstream_api_usage_buckets(
            store,
            &UpstreamApiUsageBucketQuery {
                upstream_id: upstream.id,
                attribution: ApiUsageAttribution::ApiKey,
                subject_id: "apikey_a".to_owned(),
                bucket_width_secs: HOUR,
                start_unix_secs: MONTH_START + 3_600,
                end_unix_secs: MONTH_START + 7_200,
            },
        )
        .await?;
        ensure!(
            ranged.len() == 1 && ranged[0].bucket_start_unix_secs == MONTH_START + 3_600,
            "range is start-inclusive and end-exclusive"
        );
        Ok(())
    }
);

scenario!(sto04_poll_candidate_query_filters, |storage| async move {
    let store = storage.as_ref();
    let now = 1_800_000_000;

    let legacy = create_api_key_upstream(store, "cand-legacy-no-details").await?;

    let null_state = create_api_key_upstream(store, "cand-null-state").await?;
    let mut write = access(Some(now - 10));
    write.usage_report_state = None;
    UpstreamApiKeyStore::write_api_key_access(store, null_state.id, null_state.revision, &write)
        .await?;

    let org_ok = create_api_key_upstream(store, "cand-org-ok").await?;
    UpstreamApiKeyStore::write_api_key_access(
        store,
        org_ok.id,
        org_ok.revision,
        &access(Some(now - 10)),
    )
    .await?;

    let due_now = create_api_key_upstream(store, "cand-due-now").await?;
    UpstreamApiKeyStore::write_api_key_access(
        store,
        due_now.id,
        due_now.revision,
        &access(Some(now - 10)),
    )
    .await?;
    UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
        store,
        due_now.id,
        due_now.revision,
        &ApiKeyUsagePollOutcome {
            next_poll_after_unix_secs: Some(now),
            ..poll_outcome(now - 300)
        },
    )
    .await?;

    let mut excluded = Vec::new();

    let bound = create_api_key_upstream(store, "cand-workspace-bound").await?;
    let mut write = access(Some(now - 10));
    write.key_class = ApiKeyClass::WorkspaceBound;
    UpstreamApiKeyStore::write_api_key_access(store, bound.id, bound.revision, &write).await?;
    excluded.push(bound.id);

    for (name, state) in [
        ("cand-denied", ApiKeyOrgCapability::Denied),
        ("cand-error", ApiKeyOrgCapability::Error),
        ("cand-not-applicable", ApiKeyOrgCapability::NotApplicable),
    ] {
        let upstream = create_api_key_upstream(store, name).await?;
        let mut write = access(Some(now - 10));
        write.usage_report_state = Some(state);
        UpstreamApiKeyStore::write_api_key_access(store, upstream.id, upstream.revision, &write)
            .await?;
        excluded.push(upstream.id);
    }

    let future = create_api_key_upstream(store, "cand-not-due").await?;
    UpstreamApiKeyStore::write_api_key_access(
        store,
        future.id,
        future.revision,
        &access(Some(now - 10)),
    )
    .await?;
    UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
        store,
        future.id,
        future.revision,
        &ApiKeyUsagePollOutcome {
            next_poll_after_unix_secs: Some(now + 1),
            ..poll_outcome(now - 300)
        },
    )
    .await?;
    excluded.push(future.id);

    let disabled = create_api_key_upstream(store, "cand-disabled").await?;
    UpstreamStore::set_enabled(store, disabled.id, disabled.revision, false).await?;
    excluded.push(disabled.id);

    let deleted = create_api_key_upstream(store, "cand-deleted").await?;
    UpstreamStore::soft_delete(store, deleted.id, deleted.revision).await?;
    excluded.push(deleted.id);

    let oauth = UpstreamStore::create(
        store,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "cand-oauth".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await?;
    excluded.push(oauth.id);

    let candidates =
        UpstreamApiKeyStore::list_api_key_usage_poll_candidates(store, now, None, 100).await?;
    let ids = candidates
        .iter()
        .map(|candidate| candidate.upstream.id)
        .collect::<Vec<_>>();
    // Never-polled upstreams first (by id), then the oldest poll attempt.
    let mut expected = vec![legacy.id, null_state.id, org_ok.id];
    expected.sort();
    expected.push(due_now.id);
    ensure!(
        ids == expected,
        "candidates ordered by last poll attempt, then id: {ids:?} vs {expected:?}"
    );
    ensure!(
        excluded.iter().all(|id| !ids.contains(id)),
        "excluded upstreams never appear"
    );
    let legacy_candidate = candidates
        .iter()
        .find(|candidate| candidate.upstream.id == legacy.id);
    ensure!(
        legacy_candidate.is_some_and(|candidate| candidate.details.is_none()
            && candidate.upstream.kind == UpstreamKind::AnthropicApiKey),
        "legacy upstream carries no details"
    );
    let org_candidate = candidates
        .iter()
        .find(|candidate| candidate.upstream.id == org_ok.id);
    ensure!(
        org_candidate.is_some_and(|candidate| candidate
            .details
            .as_ref()
            .map(|details| details.key_class)
            == Some(ApiKeyClass::OrgScoped)),
        "org candidate carries its details row"
    );
    ensure!(
        UpstreamStore::get_by_id(store, org_ok.id).await?.as_ref()
            == org_candidate.map(|candidate| &candidate.upstream),
        "candidate upstream equals get_by_id"
    );

    let only = UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
        store,
        now,
        Some(null_state.id),
        100,
    )
    .await?;
    ensure!(
        only.len() == 1 && only[0].upstream.id == null_state.id,
        "only_upstream narrows to one"
    );
    let excluded_only =
        UpstreamApiKeyStore::list_api_key_usage_poll_candidates(store, now, Some(bound.id), 100)
            .await?;
    ensure!(excluded_only.is_empty(), "only_upstream still filters");
    let limited =
        UpstreamApiKeyStore::list_api_key_usage_poll_candidates(store, now, None, 2).await?;
    ensure!(
        limited
            .iter()
            .map(|candidate| candidate.upstream.id)
            .collect::<Vec<_>>()
            == expected[..2],
        "limit keeps the never-polled lowest ids"
    );
    ensure!(
        UpstreamApiKeyStore::list_api_key_usage_poll_candidates(store, now, None, 0)
            .await?
            .is_empty(),
        "zero limit returns nothing"
    );
    Ok(())
});

// With more due upstreams than the per-tick limit and every polled upstream
// due again on the next tick (jitter 0), successive ticks still reach every
// upstream instead of re-polling the lowest ids.
scenario!(
    sto04_poll_candidates_rotate_past_the_limit,
    |storage| async move {
        let store = storage.as_ref();
        const LIMIT: usize = 2;
        const INTERVAL: u64 = 300;
        let mut all = Vec::new();
        for index in 0..5 {
            all.push(
                create_api_key_upstream(store, &format!("rotate-{index}"))
                    .await?
                    .id,
            );
        }
        all.sort();
        let ticks = all.len().div_ceil(LIMIT);
        let mut polled = Vec::new();
        let mut now = 1_800_000_000;
        for _ in 0..ticks {
            let candidates =
                UpstreamApiKeyStore::list_api_key_usage_poll_candidates(store, now, None, LIMIT)
                    .await?;
            ensure!(
                candidates.len() == LIMIT,
                "every upstream stays due, so each tick fills the limit"
            );
            for candidate in candidates {
                UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
                    store,
                    candidate.upstream.id,
                    candidate.upstream.revision,
                    &ApiKeyUsagePollOutcome {
                        next_poll_after_unix_secs: Some(now + INTERVAL),
                        last_success_at_unix_secs: Some(now),
                        ..poll_outcome(now)
                    },
                )
                .await?;
                polled.push(candidate.upstream.id);
            }
            now += INTERVAL;
        }
        let mut reached = polled.clone();
        reached.sort();
        reached.dedup();
        ensure!(
            reached == all,
            "every upstream is polled within {ticks} ticks: {polled:?} vs {all:?}"
        );
        ensure!(
            polled
                .iter()
                .take(all.len())
                .collect::<std::collections::HashSet<_>>()
                .len()
                == all.len(),
            "no upstream is polled twice before every upstream is polled once"
        );
        Ok(())
    }
);

scenario!(acc06_soft_and_hard_delete_semantics, |storage| async move {
    let store = storage.as_ref();
    let upstream = create_api_key_upstream(store, "acc06").await?;
    UpstreamApiKeyStore::write_api_key_access(
        store,
        upstream.id,
        upstream.revision,
        &access(Some(1_800_000_000)),
    )
    .await?;
    let tick = ApiKeyUsagePollOutcome {
        buckets: vec![
            bucket(
                ApiUsageAttribution::ApiKey,
                "apikey_acc06",
                HOUR,
                MONTH_START,
                "claude-sonnet-4-5",
                5,
            ),
            bucket(
                ApiUsageAttribution::ApiKey,
                "apikey_acc06",
                DAY,
                MONTH_START,
                "claude-sonnet-4-5",
                5,
            ),
        ],
        ..poll_outcome(1_800_000_100)
    };
    UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
        store,
        upstream.id,
        upstream.revision,
        &tick,
    )
    .await?;

    // The poller read this candidate, then the upstream is soft-deleted
    // before its write lands.
    let candidates = UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
        store,
        1_800_000_200,
        Some(upstream.id),
        10,
    )
    .await?;
    ensure!(candidates.len() == 1, "candidate before deletion");
    UpstreamStore::soft_delete(store, upstream.id, upstream.revision).await?;

    let late_tick = ApiKeyUsagePollOutcome {
        buckets: vec![bucket(
            ApiUsageAttribution::ApiKey,
            "apikey_acc06",
            HOUR,
            MONTH_START + 3_600,
            "claude-sonnet-4-5",
            9,
        )],
        consecutive_failures: 4,
        ..poll_outcome(1_800_000_200)
    };
    let late = UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
        store,
        upstream.id,
        upstream.revision,
        &late_tick,
    )
    .await;
    ensure!(
        matches!(late, Err(StorageError::Conflict { .. })),
        "write after soft delete conflicts"
    );
    let late_access = UpstreamApiKeyStore::write_api_key_access(
        store,
        upstream.id,
        upstream.revision + 1,
        &access(Some(1_800_000_300)),
    )
    .await;
    ensure!(
        matches!(late_access, Err(StorageError::Conflict { .. })),
        "access write on a deleted upstream conflicts"
    );

    let kept = require_details(store, upstream.id).await?;
    ensure!(
        kept.usage_poll_failures == 0 && kept.checked_at_unix_secs == Some(1_800_000_000),
        "soft delete keeps details unchanged"
    );
    ensure!(
        query_all(
            store,
            upstream.id,
            ApiUsageAttribution::ApiKey,
            "apikey_acc06",
            HOUR
        )
        .await?
        .len()
            == 1,
        "soft delete keeps usage rows and no bucket lands after it"
    );
    ensure!(
        UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
            store,
            1_800_000_200,
            Some(upstream.id),
            10,
        )
        .await?
        .is_empty(),
        "soft-deleted upstream is not polled"
    );

    UpstreamStore::hard_delete(store, upstream.id).await?;
    ensure!(
        UpstreamApiKeyStore::get_api_key_details(store, upstream.id)
            .await?
            .is_none(),
        "hard delete cascades details"
    );
    for width in [HOUR, DAY] {
        ensure!(
            query_all(
                store,
                upstream.id,
                ApiUsageAttribution::ApiKey,
                "apikey_acc06",
                width
            )
            .await?
            .is_empty(),
            "hard delete cascades usage rows"
        );
    }
    let after_hard = UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
        store,
        upstream.id,
        upstream.revision,
        &tick,
    )
    .await;
    ensure!(
        matches!(after_hard, Err(StorageError::Conflict { .. })),
        "write after hard delete conflicts"
    );
    ensure!(
        UpstreamApiKeyStore::get_api_key_details(store, upstream.id)
            .await?
            .is_none()
            && query_all(
                store,
                upstream.id,
                ApiUsageAttribution::ApiKey,
                "apikey_acc06",
                HOUR
            )
            .await?
            .is_empty(),
        "no orphan rows after hard delete"
    );
    Ok(())
});

scenario!(sto06_unix_seconds_roundtrip_exactly, |storage| async move {
    let store = storage.as_ref();
    let upstream = create_api_key_upstream(store, "sto06").await?;
    // Year 2100 and a non-round value: exact integer seconds, no TZ shift.
    let checked_at = 4_102_444_799;
    let attempted = 1_800_000_001;
    let mut write = access(Some(checked_at));
    write.last_attempt_at_unix_secs = attempted;
    UpstreamApiKeyStore::write_api_key_access(store, upstream.id, upstream.revision, &write)
        .await?;

    let hour_start = MONTH_START + 23 * 3_600;
    let observed_at = 4_102_444_798;
    UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
        store,
        upstream.id,
        upstream.revision,
        &ApiKeyUsagePollOutcome {
            last_success_at_unix_secs: Some(observed_at),
            next_poll_after_unix_secs: Some(observed_at + 1),
            buckets: vec![
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_sto06",
                    HOUR,
                    hour_start,
                    "m",
                    1,
                ),
                bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_sto06",
                    DAY,
                    MONTH_START,
                    "m",
                    1,
                ),
            ],
            ..poll_outcome(observed_at)
        },
    )
    .await?;
    let details = require_details(store, upstream.id).await?;
    ensure!(
        details.checked_at_unix_secs == Some(checked_at)
            && details.last_attempt_at_unix_secs == Some(attempted)
            && details.usage_last_success_at_unix_secs == Some(observed_at)
            && details.usage_last_attempt_at_unix_secs == Some(observed_at)
            && details.usage_next_poll_after_unix_secs == Some(observed_at + 1),
        "details timestamps roundtrip exactly"
    );
    ensure!(
        details.created_at_unix_secs > 0
            && details.updated_at_unix_secs >= details.created_at_unix_secs,
        "bookkeeping timestamps are unix seconds"
    );
    let hourly = query_all(
        store,
        upstream.id,
        ApiUsageAttribution::ApiKey,
        "apikey_sto06",
        HOUR,
    )
    .await?;
    let daily = query_all(
        store,
        upstream.id,
        ApiUsageAttribution::ApiKey,
        "apikey_sto06",
        DAY,
    )
    .await?;
    ensure!(
        hourly.len() == 1
            && hourly[0].bucket_start_unix_secs == hour_start
            && hourly[0].bucket_start_unix_secs % u64::from(HOUR) == 0
            && hourly[0].observed_at_unix_secs == observed_at,
        "hour bucket start and observed_at roundtrip exactly"
    );
    ensure!(
        daily.len() == 1
            && daily[0].bucket_start_unix_secs == MONTH_START
            && daily[0].bucket_start_unix_secs % u64::from(DAY) == 0,
        "UTC day bucket alignment is preserved"
    );
    Ok(())
});

scenario!(prune_upstream_api_usage_is_bounded, |storage| async move {
    let store = storage.as_ref();
    let upstream = create_api_key_upstream(store, "prune").await?;
    let buckets = (0..5)
        .map(|hour| {
            bucket(
                ApiUsageAttribution::ApiKey,
                "apikey_prune",
                HOUR,
                MONTH_START + hour * 3_600,
                "m",
                1,
            )
        })
        .collect();
    UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
        store,
        upstream.id,
        upstream.revision,
        &ApiKeyUsagePollOutcome {
            buckets,
            ..poll_outcome(1_800_000_000)
        },
    )
    .await?;
    let cutoff = MONTH_START + 4 * 3_600;
    ensure!(
        UpstreamApiKeyStore::prune_upstream_api_usage_buckets(store, cutoff, 0).await? == 0,
        "zero batch removes nothing"
    );
    ensure!(
        UpstreamApiKeyStore::prune_upstream_api_usage_buckets(store, cutoff, 3).await? == 3,
        "one batch removes at most batch_size"
    );
    ensure!(
        UpstreamApiKeyStore::prune_upstream_api_usage_buckets(store, cutoff, 3).await? == 1,
        "second batch removes the rest before the cutoff"
    );
    ensure!(
        UpstreamApiKeyStore::prune_upstream_api_usage_buckets(store, cutoff, 3).await? == 0,
        "nothing left before the cutoff"
    );
    let left = query_all(
        store,
        upstream.id,
        ApiUsageAttribution::ApiKey,
        "apikey_prune",
        HOUR,
    )
    .await?;
    ensure!(
        left.len() == 1 && left[0].bucket_start_unix_secs == cutoff,
        "rows at or after the cutoff are kept"
    );
    Ok(())
});

scenario!(reset_poll_state_keeps_usage_rows, |storage| async move {
    let store = storage.as_ref();
    let upstream = create_api_key_upstream(store, "reset-poll").await?;
    UpstreamApiKeyStore::write_api_key_access(
        store,
        upstream.id,
        upstream.revision,
        &access(Some(1_800_000_000)),
    )
    .await?;
    UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
        store,
        upstream.id,
        upstream.revision,
        &ApiKeyUsagePollOutcome {
            consecutive_failures: 3,
            last_success_at_unix_secs: Some(1_800_000_050),
            next_poll_after_unix_secs: Some(1_800_000_900),
            buckets: vec![bucket(
                ApiUsageAttribution::ApiKey,
                "apikey_reset",
                HOUR,
                MONTH_START,
                "m",
                1,
            )],
            ..poll_outcome(1_800_000_100)
        },
    )
    .await?;

    let new_access = access(Some(1_800_001_000));
    let updated = UpstreamStore::update(
        store,
        upstream.id,
        upstream.revision,
        UpstreamUpdate {
            api_key_ciphertext: Some(vec![9, 9, 9]),
            api_key_access: Some(new_access.clone()),
            reset_api_key_poll_state: true,
            ..UpstreamUpdate::default()
        },
    )
    .await?;
    ensure!(
        updated.revision == upstream.revision + 1,
        "key replace bumps spec_revision once"
    );
    let details = require_details(store, upstream.id).await?;
    ensure_access_matches(&details, &new_access)?;
    ensure!(
        details.usage_poll_failures == 0
            && details.usage_next_poll_after_unix_secs.is_none()
            && details.usage_last_success_at_unix_secs.is_none()
            && details.usage_last_attempt_at_unix_secs.is_none(),
        "reset clears poll state"
    );
    ensure!(
        query_all(
            store,
            upstream.id,
            ApiUsageAttribution::ApiKey,
            "apikey_reset",
            HOUR
        )
        .await?
        .len()
            == 1,
        "reset keeps usage rows"
    );

    let stale = UpstreamStore::update(
        store,
        upstream.id,
        upstream.revision + 5,
        UpstreamUpdate {
            api_key_access: Some(access(Some(1_800_002_000))),
            ..UpstreamUpdate::default()
        },
    )
    .await;
    ensure!(
        matches!(stale, Err(StorageError::Conflict { .. })),
        "stale update writes no access"
    );
    ensure_access_matches(&require_details(store, upstream.id).await?, &new_access)?;
    Ok(())
});

// Two pollers (a regular tick and an immediate poll) race to claim one due
// upstream: exactly one wins and owns the row.
scenario!(
    conc_poll_claim_two_claimers_one_wins,
    |storage| async move {
        let upstream = create_api_key_upstream(storage.as_ref(), "claim-race").await?;
        UpstreamApiKeyStore::write_api_key_access(
            storage.as_ref(),
            upstream.id,
            upstream.revision,
            &access(Some(1_800_000_000)),
        )
        .await?;
        let barrier = Arc::new(Barrier::new(2));
        let tokens = [Uuid::new_v4(), Uuid::new_v4()];
        let tasks = tokens.map(|token| {
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            let upstream = upstream.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                claim(
                    storage.as_ref(),
                    &upstream,
                    token,
                    1_800_000_160,
                    1_800_000_100,
                )
                .await
            })
        });
        let mut won = Vec::new();
        for (token, task) in tokens.into_iter().zip(tasks) {
            if task.await?? {
                won.push(token);
            }
        }
        ensure!(won.len() == 1, "exactly one claimer wins, got {won:?}");
        let details = require_details(storage.as_ref(), upstream.id).await?;
        ensure!(
            details.usage_poll_claim_token == won.first().copied()
                && details.usage_poll_claim_lease_until_unix_secs == Some(1_800_000_160),
            "the winner owns the row: {details:?}"
        );
        ensure!(
            UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
                storage.as_ref(),
                1_800_000_100,
                Some(upstream.id),
                10,
            )
            .await?
            .is_empty(),
            "a live claim hides the upstream from the candidate query"
        );
        Ok(())
    }
);

// A poll whose lease expired and was reclaimed cannot complete: its write is
// rejected, lands no bucket, and leaves the new owner's claim in place.
scenario!(
    conc_poll_claim_stale_completion_rejected,
    |storage| async move {
        let store = storage.as_ref();
        let upstream = create_api_key_upstream(store, "claim-stale").await?;
        UpstreamApiKeyStore::write_api_key_access(
            store,
            upstream.id,
            upstream.revision,
            &access(Some(1_800_000_000)),
        )
        .await?;
        let stale = Uuid::new_v4();
        let fresh = Uuid::new_v4();
        ensure!(
            claim(store, &upstream, stale, 1_800_000_160, 1_800_000_100).await?,
            "first claim"
        );
        ensure!(
            !claim(store, &upstream, fresh, 1_800_000_220, 1_800_000_159).await?,
            "a live lease is not reclaimable"
        );
        ensure!(
            claim(store, &upstream, fresh, 1_800_000_220, 1_800_000_160).await?,
            "an expired lease is reclaimable"
        );

        let late = UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            upstream.id,
            upstream.revision,
            &ApiKeyUsagePollOutcome {
                claim_token: Some(stale),
                next_poll_after_unix_secs: Some(1_800_002_200),
                buckets: vec![bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_stale",
                    HOUR,
                    MONTH_START,
                    "m",
                    1,
                )],
                ..poll_outcome(1_800_000_100)
            },
        )
        .await;
        ensure!(
            matches!(late, Err(StorageError::Conflict { .. })),
            "a stale completion is rejected: {late:?}"
        );
        let details = require_details(store, upstream.id).await?;
        ensure!(
            details.usage_poll_claim_token == Some(fresh)
                && details.usage_next_poll_after_unix_secs.is_none()
                && details.usage_last_attempt_at_unix_secs.is_none(),
            "the rejected write changes nothing: {details:?}"
        );
        ensure!(
            query_all(
                store,
                upstream.id,
                ApiUsageAttribution::ApiKey,
                "apikey_stale",
                HOUR
            )
            .await?
            .is_empty(),
            "a rejected completion lands no bucket"
        );

        UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            upstream.id,
            upstream.revision,
            &ApiKeyUsagePollOutcome {
                claim_token: Some(fresh),
                next_poll_after_unix_secs: Some(1_800_000_460),
                last_success_at_unix_secs: Some(1_800_000_160),
                ..poll_outcome(1_800_000_160)
            },
        )
        .await?;
        let details = require_details(store, upstream.id).await?;
        ensure!(
            details.usage_poll_claim_token.is_none()
                && details.usage_poll_claim_lease_until_unix_secs.is_none()
                && details.usage_next_poll_after_unix_secs == Some(1_800_000_460)
                && details.usage_last_success_at_unix_secs == Some(1_800_000_160),
            "the owner's completion lands and releases the claim: {details:?}"
        );
        let again = UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            upstream.id,
            upstream.revision,
            &ApiKeyUsagePollOutcome {
                claim_token: Some(fresh),
                ..poll_outcome(1_800_000_161)
            },
        )
        .await;
        ensure!(
            matches!(again, Err(StorageError::Conflict { .. })),
            "a released claim cannot complete twice: {again:?}"
        );
        Ok(())
    }
);

// Candidate selection lists only enabled upstreams, but a disable can land
// after that read. Neither a claim nor a claimed completion may succeed
// against a disabled spec, even at its current revision.
scenario!(
    conc_poll_claim_and_completion_reject_disabled_upstream,
    |storage| async move {
        let store = storage.as_ref();
        let upstream = create_api_key_upstream(store, "claim-disabled").await?;
        UpstreamApiKeyStore::write_api_key_access(
            store,
            upstream.id,
            upstream.revision,
            &access(Some(1_800_000_000)),
        )
        .await?;

        let disabled =
            UpstreamStore::set_enabled(store, upstream.id, upstream.revision, false).await?;
        let refused = UpstreamApiKeyStore::claim_api_key_usage_poll(
            store,
            disabled.id,
            disabled.revision,
            Uuid::new_v4(),
            1_800_000_160,
            "sk-ant-api03-...Clm",
            1_800_000_100,
        )
        .await;
        ensure!(
            matches!(refused, Err(StorageError::Conflict { .. })),
            "a disabled upstream is not claimable at its current revision: {refused:?}"
        );
        ensure!(
            require_details(store, upstream.id)
                .await?
                .usage_poll_claim_token
                .is_none(),
            "the refused claim leaves no token"
        );

        let enabled =
            UpstreamStore::set_enabled(store, upstream.id, disabled.revision, true).await?;
        let token = Uuid::new_v4();
        ensure!(
            claim(store, &enabled, token, 1_800_000_160, 1_800_000_100).await?,
            "the re-enabled upstream is claimable"
        );
        let disabled_again =
            UpstreamStore::set_enabled(store, upstream.id, enabled.revision, false).await?;
        let completion = UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            disabled_again.id,
            disabled_again.revision,
            &ApiKeyUsagePollOutcome {
                claim_token: Some(token),
                last_success_at_unix_secs: Some(1_800_000_120),
                buckets: vec![bucket(
                    ApiUsageAttribution::ApiKey,
                    "apikey_disabled",
                    HOUR,
                    MONTH_START,
                    "m",
                    1,
                )],
                ..poll_outcome(1_800_000_120)
            },
        )
        .await;
        ensure!(
            matches!(completion, Err(StorageError::Conflict { .. })),
            "a claimed completion against a disabled upstream is rejected: {completion:?}"
        );
        let details = require_details(store, upstream.id).await?;
        ensure!(
            details.usage_last_success_at_unix_secs.is_none()
                && details.usage_last_attempt_at_unix_secs.is_none(),
            "the rejected completion changes nothing: {details:?}"
        );
        ensure!(
            query_all(
                store,
                upstream.id,
                ApiUsageAttribution::ApiKey,
                "apikey_disabled",
                HOUR
            )
            .await?
            .is_empty(),
            "a rejected completion lands no bucket"
        );
        Ok(())
    }
);

// A poller that crashed mid-poll leaves a claim behind; once its lease
// expires the upstream is a candidate again and is reclaimed. A legacy
// upstream with no details row is claimed by inserting the row.
scenario!(
    conc_poll_claim_lease_expiry_is_reclaimable,
    |storage| async move {
        let store = storage.as_ref();
        let upstream = create_api_key_upstream(store, "claim-expiry").await?;
        ensure!(
            UpstreamApiKeyStore::get_api_key_details(store, upstream.id)
                .await?
                .is_none(),
            "legacy upstream starts without details"
        );
        let crashed = Uuid::new_v4();
        ensure!(
            claim(store, &upstream, crashed, 1_800_000_060, 1_800_000_000).await?,
            "a legacy upstream is claimable"
        );
        let details = require_details(store, upstream.id).await?;
        ensure!(
            details.usage_poll_claim_token == Some(crashed)
                && details.usage_poll_claim_lease_until_unix_secs == Some(1_800_000_060)
                && details.key_hint == "sk-ant-api03-...Clm"
                && details.usage_report_state.is_none(),
            "the claim inserts a not-checked row it owns: {details:?}"
        );

        let listed = |now| {
            UpstreamApiKeyStore::list_api_key_usage_poll_candidates(
                store,
                now,
                Some(upstream.id),
                10,
            )
        };
        ensure!(
            listed(1_800_000_059).await?.is_empty(),
            "hidden while the lease is live"
        );
        ensure!(
            listed(1_800_000_060).await?.len() == 1,
            "a candidate again once the lease expires"
        );
        let next = Uuid::new_v4();
        ensure!(
            !claim(store, &upstream, next, 1_800_000_120, 1_800_000_059).await?,
            "not reclaimable before expiry"
        );
        ensure!(
            claim(store, &upstream, next, 1_800_000_120, 1_800_000_060).await?,
            "reclaimable at expiry"
        );
        ensure!(
            require_details(store, upstream.id)
                .await?
                .usage_poll_claim_token
                == Some(next),
            "the new poller owns the row"
        );
        Ok(())
    }
);

// The completion write never shortens a later stored deadline (a 429's
// `retry-after` survives an overlapping success), still extends it, and
// `None` still clears it; the claim re-checks due-ness so a row a 429
// deferred is not claimable early.
scenario!(
    pol_completion_never_shortens_retry_after,
    |storage| async move {
        let store = storage.as_ref();
        let upstream = create_api_key_upstream(store, "retry-after-keep").await?;
        UpstreamApiKeyStore::write_api_key_access(
            store,
            upstream.id,
            upstream.revision,
            &access(Some(1_800_000_000)),
        )
        .await?;
        let write = |next_poll_after_unix_secs, attempted| {
            let outcome = ApiKeyUsagePollOutcome {
                next_poll_after_unix_secs,
                ..poll_outcome(attempted)
            };
            let upstream = &upstream;
            async move {
                UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
                    store,
                    upstream.id,
                    upstream.revision,
                    &outcome,
                )
                .await
            }
        };
        let next_poll_after = || async {
            Ok::<_, anyhow::Error>(
                require_details(store, upstream.id)
                    .await?
                    .usage_next_poll_after_unix_secs,
            )
        };
        // t=1000: a 429 with retry-after 1200; an overlapping success then
        // reports its regular next poll at 1300.
        write(Some(1_800_002_200), 1_800_001_000).await?;
        write(Some(1_800_001_300), 1_800_001_000).await?;
        ensure!(
            next_poll_after().await? == Some(1_800_002_200),
            "an earlier deadline never replaces the stored retry-after"
        );
        ensure!(
            !claim(
                store,
                &upstream,
                Uuid::new_v4(),
                1_800_001_360,
                1_800_001_300
            )
            .await?,
            "the deferred row is not claimable before its retry-after"
        );
        ensure!(
            claim(
                store,
                &upstream,
                Uuid::new_v4(),
                1_800_002_260,
                1_800_002_200
            )
            .await?,
            "claimable once the retry-after passes"
        );
        write(Some(1_800_002_500), 1_800_002_200).await?;
        ensure!(
            next_poll_after().await? == Some(1_800_002_500),
            "a later deadline still extends the schedule"
        );
        write(None, 1_800_002_300).await?;
        ensure!(
            next_poll_after().await?.is_none(),
            "None still clears the schedule"
        );
        Ok(())
    }
);

// An access write (Re-check, key replace) releases a live poll claim: the
// in-flight poll read the old credential, so its completion is rejected and
// a poll of the new access state can claim at once.
scenario!(
    conc_access_write_releases_poll_claim,
    |storage| async move {
        let store = storage.as_ref();
        let upstream = create_api_key_upstream(store, "claim-recheck").await?;
        UpstreamApiKeyStore::write_api_key_access(
            store,
            upstream.id,
            upstream.revision,
            &access(Some(1_800_000_000)),
        )
        .await?;
        let in_flight = Uuid::new_v4();
        ensure!(
            claim(store, &upstream, in_flight, 1_800_000_160, 1_800_000_100).await?,
            "claim"
        );
        UpstreamApiKeyStore::write_api_key_access(
            store,
            upstream.id,
            upstream.revision,
            &access(Some(1_800_000_120)),
        )
        .await?;
        ensure!(
            require_details(store, upstream.id)
                .await?
                .usage_poll_claim_token
                .is_none(),
            "the access write releases the claim"
        );
        let late = UpstreamApiKeyStore::write_api_key_usage_poll_outcome(
            store,
            upstream.id,
            upstream.revision,
            &ApiKeyUsagePollOutcome {
                claim_token: Some(in_flight),
                ..poll_outcome(1_800_000_130)
            },
        )
        .await;
        ensure!(
            matches!(late, Err(StorageError::Conflict { .. })),
            "the old-credential completion is rejected: {late:?}"
        );
        ensure!(
            claim(
                store,
                &upstream,
                Uuid::new_v4(),
                1_800_000_190,
                1_800_000_130
            )
            .await?,
            "the next poll claims immediately"
        );
        Ok(())
    }
);

fn api_key_create(name: &str) -> UpstreamCreate {
    UpstreamCreate {
        id: Uuid::new_v4(),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicApiKey,
        base_url: None,
        api_key_ciphertext: Some(vec![1, 2, 3]),
        oauth_tokens: None,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        workspace_id: None,
        api_key_access: None,
    }
}

async fn create_api_key_upstream<S>(store: &S, name: &str) -> Result<UpstreamRecord>
where
    S: UpstreamStore + ?Sized,
{
    Ok(UpstreamStore::create(store, api_key_create(name)).await?)
}

async fn require_details<S>(store: &S, upstream_id: Uuid) -> Result<UpstreamApiKeyDetailsRecord>
where
    S: UpstreamApiKeyStore + ?Sized,
{
    UpstreamApiKeyStore::get_api_key_details(store, upstream_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("details row missing for {upstream_id}"))
}

async fn query_all<S>(
    store: &S,
    upstream_id: Uuid,
    attribution: ApiUsageAttribution,
    subject_id: &str,
    bucket_width_secs: u32,
) -> Result<Vec<UpstreamApiUsageBucketRecord>>
where
    S: UpstreamApiKeyStore + ?Sized,
{
    Ok(UpstreamApiKeyStore::query_upstream_api_usage_buckets(
        store,
        &UpstreamApiUsageBucketQuery {
            upstream_id,
            attribution,
            subject_id: subject_id.to_owned(),
            bucket_width_secs,
            start_unix_secs: 0,
            end_unix_secs: i64::MAX as u64,
        },
    )
    .await?)
}

fn ensure_access_matches(
    details: &UpstreamApiKeyDetailsRecord,
    access: &ApiKeyAccessWrite,
) -> Result<()> {
    ensure!(
        details.verified == access.verified
            && details.key_class == access.key_class
            && details.key_hint == access.key_hint
            && details.api_key_id == access.api_key_id
            && details.needs_workspace == access.needs_workspace
            && details.capabilities == access.capabilities
            && details.remediation == access.remediation
            && details.usage_report_state == access.usage_report_state
            && details.last_error == access.last_error
            && details.checked_at_unix_secs == access.checked_at_unix_secs
            && details.last_attempt_at_unix_secs == Some(access.last_attempt_at_unix_secs),
        "details {details:?} do not match access {access:?}"
    );
    Ok(())
}

fn access(checked_at_unix_secs: Option<u64>) -> ApiKeyAccessWrite {
    ApiKeyAccessWrite {
        verified: true,
        key_class: ApiKeyClass::OrgScoped,
        key_hint: "sk-ant-api03-...AbCd".to_owned(),
        api_key_id: Some("apikey_01".to_owned()),
        needs_workspace: false,
        capabilities: ApiKeyCapabilities {
            inference: ApiKeyInference::Ok,
            workspace: ApiKeyWorkspace {
                mode: ApiKeyWorkspaceMode::Selected,
                id: Some("wrkspc_01".to_owned()),
                name: Some("Default".to_owned()),
            },
            workspace_list: ApiKeyOrgCapability::Ok,
            api_key_resolution: ApiKeyResolution::Resolved,
            key_type: Some(ApiKeyType::ServiceAccount),
        },
        remediation: Vec::new(),
        usage_report_state: Some(ApiKeyOrgCapability::Ok),
        last_error: None,
        checked_at_unix_secs,
        last_attempt_at_unix_secs: checked_at_unix_secs.unwrap_or(1_800_000_000),
    }
}

fn poll_outcome(attempted_at_unix_secs: u64) -> ApiKeyUsagePollOutcome {
    ApiKeyUsagePollOutcome {
        attempted_at_unix_secs,
        usage_report_state: None,
        access_revision_seen: 0,
        consecutive_failures: 0,
        next_poll_after_unix_secs: None,
        last_success_at_unix_secs: None,
        legacy_key_hint: None,
        claim_token: None,
        buckets: Vec::new(),
    }
}

/// Claims `upstream` for a usage poll at `now` with a lease until `lease_until`.
async fn claim<S>(
    store: &S,
    upstream: &UpstreamRecord,
    token: Uuid,
    lease_until: u64,
    now: u64,
) -> Result<bool>
where
    S: UpstreamApiKeyStore + ?Sized,
{
    Ok(UpstreamApiKeyStore::claim_api_key_usage_poll(
        store,
        upstream.id,
        upstream.revision,
        token,
        lease_until,
        "sk-ant-api03-...Clm",
        now,
    )
    .await?)
}

fn bucket(
    attribution: ApiUsageAttribution,
    subject_id: &str,
    bucket_width_secs: u32,
    bucket_start_unix_secs: u64,
    model: &str,
    tokens: u64,
) -> UpstreamApiUsageBucketWrite {
    UpstreamApiUsageBucketWrite {
        attribution,
        subject_id: subject_id.to_owned(),
        bucket_width_secs,
        bucket_start_unix_secs,
        model: model.to_owned(),
        uncached_input_tokens: tokens,
        cache_creation_5m_input_tokens: tokens * 2,
        cache_creation_1h_input_tokens: tokens * 3,
        cache_read_input_tokens: tokens * 4,
        output_tokens: tokens * 5,
        list_cost_micros: tokens * 1_000,
    }
}

fn record(
    write: UpstreamApiUsageBucketWrite,
    observed_at_unix_secs: u64,
) -> UpstreamApiUsageBucketRecord {
    UpstreamApiUsageBucketRecord {
        attribution: write.attribution,
        subject_id: write.subject_id,
        bucket_width_secs: write.bucket_width_secs,
        bucket_start_unix_secs: write.bucket_start_unix_secs,
        model: write.model,
        uncached_input_tokens: write.uncached_input_tokens,
        cache_creation_5m_input_tokens: write.cache_creation_5m_input_tokens,
        cache_creation_1h_input_tokens: write.cache_creation_1h_input_tokens,
        cache_read_input_tokens: write.cache_read_input_tokens,
        output_tokens: write.output_tokens,
        list_cost_micros: write.list_cost_micros,
        observed_at_unix_secs,
    }
}
