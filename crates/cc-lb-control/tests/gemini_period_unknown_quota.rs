use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use cc_lb_control::gemini_period_unknown_quota::*;
use cc_lb_storage_api::{BackendKind, MetaStore, StorageResult};
use uuid::Uuid;

fn meter(
    group: GeminiQuotaGroupKind,
    model: &str,
    remaining: f64,
    reset: Option<u64>,
) -> GeminiPeriodUnknownMeter {
    GeminiPeriodUnknownMeter {
        group,
        model_id: Some(model.to_owned()),
        bucket_id: None,
        known_period: None,
        remaining_fraction: remaining,
        reset_time_unix_secs: reset,
        remaining_amount: Some("42".to_owned()),
        origin: GeminiRawMeterOrigin::RetrieveUserQuota,
    }
}

fn update(group: GeminiQuotaGroupKind, at: u64, remaining: f64) -> GeminiGroupPollUpdate {
    GeminiGroupPollUpdate::Replace(GeminiPeriodUnknownGroupRecord {
        observed_at_unix_millis: at,
        meters: vec![meter(group, "model", remaining, None)],
    })
}

#[test]
fn meter_normalization_and_allowlist() {
    let group = GeminiQuotaGroupKind::Gemini;
    let normalized = normalize_gemini_period_unknown_meters(
        group,
        vec![
            meter(group, "a", 0.7, Some(10)),
            meter(group, "a", 0.2, None),
            meter(group, "a", 0.2, Some(20)),
            meter(group, "a", 0.2, Some(15)),
            meter(group, "b", 2.0, None),
            meter(group, "c", -1.0, None),
            meter(group, "nan", f64::NAN, None),
            meter(group, "infinity", f64::INFINITY, None),
            meter(GeminiQuotaGroupKind::ThirdParty, "other", 0.5, None),
        ],
    );
    assert_eq!(normalized.len(), 3);
    assert_eq!(normalized[0].model_id.as_deref(), Some("a"));
    assert_eq!(normalized[0].remaining_fraction, 0.2);
    assert_eq!(normalized[0].reset_time_unix_secs, Some(15));
    assert_eq!(normalized[1].remaining_fraction, 1.0);
    assert_eq!(normalized[2].remaining_fraction, 0.0);
    let mut full = normalized[0].clone();
    full.bucket_id = Some("bucket".to_owned());
    full.known_period = Some("5h".to_owned());
    let encoded = serde_json::to_value(&full).unwrap();
    let keys: Vec<_> = encoded
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec![
            "bucket_id",
            "group",
            "known_period",
            "model_id",
            "origin",
            "remaining_amount",
            "remaining_fraction",
            "reset_time_unix_secs"
        ]
    );
    assert_eq!(encoded["group"], "gemini");
    assert_eq!(encoded["origin"], "retrieve_user_quota");
    assert_eq!(encoded["known_period"], "5h");
    let mut summary = full;
    summary.model_id = None;
    summary.remaining_amount = None;
    summary.origin = GeminiRawMeterOrigin::RetrieveUserQuotaSummary;
    let encoded = serde_json::to_value(summary).unwrap();
    assert!(encoded.get("model_id").is_none());
    assert!(encoded.get("remaining_amount").is_none());
    assert_eq!(encoded["origin"], "retrieve_user_quota_summary");
    let many: Vec<_> = (0..40)
        .rev()
        .map(|i| meter(group, &format!("model-{i:02}"), 0.5, None))
        .collect();
    let normalized = normalize_gemini_period_unknown_meters(group, many);
    assert_eq!(normalized.len(), MAX_GEMINI_PERIOD_UNKNOWN_METERS_PER_GROUP);
    assert_eq!(normalized[0].model_id.as_deref(), Some("model-00"));
    assert_eq!(normalized[31].model_id.as_deref(), Some("model-31"));
}

#[test]
fn merge_is_newest_wins_per_group() {
    let gemini = GeminiQuotaGroupKind::Gemini;
    let third = GeminiQuotaGroupKind::ThirdParty;
    let initial = merge_gemini_period_unknown_quota(
        None,
        &GeminiRawQuotaBinding::unscoped(),
        &update(gemini, 20, 0.2),
        &update(third, 10, 0.4),
    );
    let merged = merge_gemini_period_unknown_quota(
        Some(&initial),
        &GeminiRawQuotaBinding::unscoped(),
        &update(gemini, 19, 0.9),
        &update(third, 11, 0.5),
    );
    assert_eq!(merged.gemini, initial.gemini);
    assert_eq!(
        merged.third_party.as_ref().unwrap().observed_at_unix_millis,
        11
    );
    let unchanged = merge_gemini_period_unknown_quota(
        Some(&merged),
        &GeminiRawQuotaBinding::unscoped(),
        &GeminiGroupPollUpdate::Unchanged,
        &GeminiGroupPollUpdate::Unchanged,
    );
    assert_eq!(unchanged, merged);
    let clear = GeminiGroupPollUpdate::Replace(GeminiPeriodUnknownGroupRecord {
        observed_at_unix_millis: 20,
        meters: vec![],
    });
    let cleared = merge_gemini_period_unknown_quota(
        Some(&merged),
        &GeminiRawQuotaBinding::unscoped(),
        &clear,
        &GeminiGroupPollUpdate::Unchanged,
    );
    assert!(cleared.gemini.unwrap().meters.is_empty());
    assert_eq!(cleared.third_party, merged.third_party);
    let same_time = merge_gemini_period_unknown_quota(
        Some(&initial),
        &GeminiRawQuotaBinding::unscoped(),
        &update(gemini, 20, 0.8),
        &GeminiGroupPollUpdate::Unchanged,
    );
    assert_eq!(same_time.gemini.unwrap().meters[0].remaining_fraction, 0.8);
}

#[derive(Default)]
struct ConflictStore {
    value: Mutex<Option<String>>,
    concurrent: Mutex<Option<String>>,
    attempts: AtomicUsize,
    reads: AtomicUsize,
}

#[async_trait]
impl MetaStore for ConflictStore {
    async fn initialize(&self) -> StorageResult<()> {
        Ok(())
    }
    async fn backend_kind(&self) -> StorageResult<BackendKind> {
        Ok(BackendKind::Sqlite)
    }
    async fn get_meta_value(&self, _: &str) -> StorageResult<Option<String>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.value.lock().unwrap().clone())
    }
    async fn compare_and_put_meta_value(
        &self,
        _: &str,
        expected: Option<&str>,
        value: &str,
    ) -> StorageResult<bool> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let mut current = self.value.lock().unwrap();
        if let Some(concurrent) = self.concurrent.lock().unwrap().take() {
            *current = Some(concurrent);
            return Ok(false);
        }
        if current.as_deref() != expected {
            return Ok(false);
        }
        *current = Some(value.to_owned());
        Ok(true)
    }
}

#[tokio::test]
async fn persist_remerges_after_cas_conflict() {
    let store = ConflictStore::default();
    let id = Uuid::nil();
    persist_gemini_period_unknown_quota(
        &store,
        id,
        GeminiRawQuotaBinding::unscoped(),
        GeminiGroupPollUpdate::Unchanged,
        GeminiGroupPollUpdate::Unchanged,
    )
    .await
    .unwrap();
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    let concurrent = merge_gemini_period_unknown_quota(
        None,
        &GeminiRawQuotaBinding::unscoped(),
        &update(GeminiQuotaGroupKind::Gemini, 30, 0.1),
        &update(GeminiQuotaGroupKind::ThirdParty, 40, 0.3),
    );
    *store.concurrent.lock().unwrap() = Some(serde_json::to_string(&concurrent).unwrap());
    persist_gemini_period_unknown_quota(
        &store,
        id,
        GeminiRawQuotaBinding::unscoped(),
        update(GeminiQuotaGroupKind::Gemini, 20, 0.8),
        GeminiGroupPollUpdate::Unchanged,
    )
    .await
    .unwrap();
    assert_eq!(store.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        load_gemini_period_unknown_quota(&store, id).await.unwrap(),
        Some(concurrent.clone())
    );
    *store.concurrent.lock().unwrap() = Some(serde_json::to_string(&concurrent).unwrap());
    persist_gemini_period_unknown_quota(
        &store,
        id,
        GeminiRawQuotaBinding::unscoped(),
        update(GeminiQuotaGroupKind::Gemini, 50, 0.6),
        GeminiGroupPollUpdate::Unchanged,
    )
    .await
    .unwrap();
    assert_eq!(store.attempts.load(Ordering::SeqCst), 3);
    let merged = load_gemini_period_unknown_quota(&store, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(merged.gemini.unwrap().observed_at_unix_millis, 50);
    assert_eq!(merged.third_party, concurrent.third_party);
    for invalid in [
        "broken",
        r#"{"version":99,"gemini":null,"third_party":null}"#,
    ] {
        *store.value.lock().unwrap() = Some(invalid.to_owned());
        assert!(
            load_gemini_period_unknown_quota(&store, id)
                .await
                .unwrap()
                .is_none()
        );
        persist_gemini_period_unknown_quota(
            &store,
            id,
            GeminiRawQuotaBinding::unscoped(),
            update(GeminiQuotaGroupKind::Gemini, 60, 0.5),
            GeminiGroupPollUpdate::Unchanged,
        )
        .await
        .unwrap();
        assert_eq!(
            load_gemini_period_unknown_quota(&store, id)
                .await
                .unwrap()
                .unwrap()
                .version,
            GEMINI_PERIOD_UNKNOWN_QUOTA_VERSION
        );
    }
}

#[test]
fn binding_tracks_subject_and_credential_generation() {
    let bound = GeminiRawQuotaBinding::from_credential(
        Some(" Member@Example.test "),
        Some(" project "),
        10,
    );
    assert!(bound.email_bound);
    assert_eq!(bound.identity_sha256.as_ref().unwrap().len(), 64);
    assert!(bound.matches_credential(Some("member@example.test"), Some("project"), 11));
    assert!(!bound.matches_credential(Some("other@example.test"), Some("project"), 11));
    assert!(!bound.matches_credential(Some("member@example.test"), Some("other-project"), 11));
    let initial = merge_gemini_period_unknown_quota(
        None,
        &bound,
        &update(GeminiQuotaGroupKind::Gemini, 20, 0.2),
        &update(GeminiQuotaGroupKind::ThirdParty, 20, 0.3),
    );
    let refreshed =
        GeminiRawQuotaBinding::from_credential(Some("member@example.test"), Some("project"), 11);
    let continued = merge_gemini_period_unknown_quota(
        Some(&initial),
        &refreshed,
        &GeminiGroupPollUpdate::Unchanged,
        &GeminiGroupPollUpdate::Unchanged,
    );
    assert_eq!(continued.gemini, initial.gemini);
    assert_eq!(continued.third_party, initial.third_party);
    assert_eq!(continued.binding, refreshed);
    for switched in [
        GeminiRawQuotaBinding::from_credential(Some("other@example.test"), Some("project"), 12),
        GeminiRawQuotaBinding::from_credential(
            Some("member@example.test"),
            Some("other-project"),
            12,
        ),
    ] {
        let merged = merge_gemini_period_unknown_quota(
            Some(&continued),
            &switched,
            &update(GeminiQuotaGroupKind::Gemini, 10, 0.5),
            &GeminiGroupPollUpdate::Unchanged,
        );
        assert_eq!(merged.gemini.unwrap().observed_at_unix_millis, 10);
        assert!(merged.third_party.is_none());
        let delayed = merge_gemini_period_unknown_quota(
            Some(&continued),
            &bound,
            &update(GeminiQuotaGroupKind::Gemini, 100, 0.0),
            &GeminiGroupPollUpdate::Unchanged,
        );
        assert_eq!(delayed, continued);
    }
    let project_only = GeminiRawQuotaBinding::from_credential(None, Some("project"), 20);
    assert!(project_only.matches_credential(None, Some("project"), 20));
    assert!(!project_only.matches_credential(None, Some("project"), 21));
    let old_project = merge_gemini_period_unknown_quota(
        None,
        &project_only,
        &update(GeminiQuotaGroupKind::Gemini, 20, 0.2),
        &update(GeminiQuotaGroupKind::ThirdParty, 20, 0.3),
    );
    let next_project = GeminiRawQuotaBinding::from_credential(None, Some("project"), 21);
    let cleared = merge_gemini_period_unknown_quota(
        Some(&old_project),
        &next_project,
        &GeminiGroupPollUpdate::Unchanged,
        &GeminiGroupPollUpdate::Unchanged,
    );
    assert!(cleared.gemini.is_none() && cleared.third_party.is_none());
    assert!(GeminiRawQuotaBinding::unscoped().is_unscoped());
    let missing_project =
        GeminiRawQuotaBinding::from_credential(Some("member@example.test"), Some(" "), 1);
    assert!(!missing_project.is_unscoped());
    assert!(!missing_project.matches_credential(Some("member@example.test"), None, 1));
    assert!(GeminiRawQuotaBinding::unscoped().same_subject(&GeminiRawQuotaBinding::unscoped()));
}

#[tokio::test]
async fn persist_rechecks_binding_after_concurrent_credential_replacement() {
    let store = ConflictStore::default();
    let binding =
        GeminiRawQuotaBinding::from_credential(Some("old@example.test"), Some("project"), 1);
    let replacement =
        GeminiRawQuotaBinding::from_credential(Some("new@example.test"), Some("project"), 2);
    let concurrent = merge_gemini_period_unknown_quota(
        None,
        &replacement,
        &update(GeminiQuotaGroupKind::Gemini, 10, 0.7),
        &update(GeminiQuotaGroupKind::ThirdParty, 10, 0.8),
    );
    *store.concurrent.lock().unwrap() = Some(serde_json::to_string(&concurrent).unwrap());
    persist_gemini_period_unknown_quota(
        &store,
        Uuid::nil(),
        binding,
        update(GeminiQuotaGroupKind::Gemini, 100, 0.0),
        GeminiGroupPollUpdate::Unchanged,
    )
    .await
    .unwrap();
    assert_eq!(store.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        load_gemini_period_unknown_quota(&store, Uuid::nil())
            .await
            .unwrap(),
        Some(concurrent)
    );
}
