use super::*;
use cc_lb_control::gemini_period_unknown_quota::*;
use cc_lb_oauth_protocol::GeminiOAuthProfile;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation;
use cc_lb_storage_api::{SubscriptionQuotaSample, UpstreamSubscriptionQuotaStore};
use serde_json::{Value, json};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU16},
};
use tracing::instrument::WithSubscriber;

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("captured logs lock")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct ProviderState {
    summary: Mutex<Value>,
    summary_status: AtomicU16,
    quota: Mutex<String>,
    quota_status: AtomicU16,
    quota_calls: AtomicUsize,
    delay_quota: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

struct Provider {
    state: Arc<ProviderState>,
    base: Url,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Provider {
    async fn new() -> Self {
        let state = Arc::new(ProviderState {
            summary: Mutex::new(json!({"groups":[]})),
            summary_status: AtomicU16::new(200),
            quota: Mutex::new(raw_quota().to_string()),
            quota_status: AtomicU16::new(200),
            quota_calls: AtomicUsize::new(0),
            delay_quota: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let provider = state.clone();
        let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
            let state = provider.clone();
            async move {
                if request.uri().path().ends_with(":retrieveUserQuotaSummary") {
                    let status = state.summary_status.load(Ordering::SeqCst);
                    let body = state.summary.lock().unwrap().to_string();
                    (http::StatusCode::from_u16(status).unwrap(), body)
                } else if request.uri().path().ends_with(":retrieveUserQuota") {
                    state.quota_calls.fetch_add(1, Ordering::SeqCst);
                    let status = state.quota_status.load(Ordering::SeqCst);
                    let body = state.quota.lock().unwrap().clone();
                    if state.delay_quota.swap(false, Ordering::SeqCst) {
                        state.entered.notify_one();
                        state.release.notified().await;
                    }
                    (http::StatusCode::from_u16(status).unwrap(), body)
                } else {
                    (http::StatusCode::SERVICE_UNAVAILABLE, "{}".to_owned())
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1internal", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            state,
            base,
            server,
        }
    }
}

fn raw_quota() -> Value {
    json!({"buckets":[
        {"modelId":"gemini-2.5-pro","remainingFraction":0.4,"resetTime":"2023-11-14T23:13:20Z","remainingAmount":"40"},
        {"modelId":"claude-sonnet","remainingFraction":0.3,"resetTime":"2023-11-20T22:13:20Z"},
        {"modelId":"gpt-oss","remainingFraction":0.6}
    ]})
}

fn healthy_summary() -> Value {
    json!({"groups":[
        {"displayName":"Gemini models","buckets":[{"bucketId":"gemini-5h","remainingFraction":0.8,"resetTime":"2023-11-14T23:13:20Z"}]},
        {"displayName":"Claude and GPT models","buckets":[{"bucketId":"3p-weekly","remainingFraction":0.7,"resetTime":"2023-11-20T22:13:20Z"}]}
    ]})
}

async fn seed(
    fixture: &DispatchFixture,
    id: Uuid,
    profile: GeminiOAuthProfile,
) -> (Vec<SubscriptionQuotaSample>, GeminiPeriodUnknownQuotaRecord) {
    let (records, _, _) = super::super::super::usage::parse_provider_usage_body(
        super::super::super::usage::ProviderUsageContext {
            upstream_id: id,
            kind: UpstreamKind::GeminiOauth,
            profile,
            project_id: Some("synthetic-project"),
        },
        &serde_json::to_vec(&json!({"summary":healthy_summary()})).unwrap(),
        1_699_999_000_000,
    )
    .unwrap();
    fixture
        .storage
        .record_subscription_quota_samples(&records)
        .await
        .unwrap();
    let (_, _, raw) = super::super::super::usage::parse_provider_usage_body(
        super::super::super::usage::ProviderUsageContext {
            upstream_id: id,
            kind: UpstreamKind::GeminiOauth,
            profile,
            project_id: Some("synthetic-project"),
        },
        &serde_json::to_vec(
            &json!({"summary":null,"fallback":{"gemini":raw_quota(),"third_party":raw_quota()}}),
        )
        .unwrap(),
        1_699_999_000_000,
    )
    .unwrap();
    persist_gemini_period_unknown_quota(fixture.storage.as_ref(), id, raw.gemini, raw.third_party)
        .await
        .unwrap();
    let raw = load_gemini_period_unknown_quota(fixture.storage.as_ref(), id)
        .await
        .unwrap()
        .unwrap();
    (records, raw)
}

async fn latest(fixture: &DispatchFixture, id: Uuid) -> Vec<SubscriptionQuotaSample> {
    let mut records = fixture
        .storage
        .list_latest_subscription_quota_for_upstreams(&[id])
        .await
        .unwrap();
    records.sort_by_key(|record| record.window.as_str());
    records
}

#[tokio::test]
async fn gemini_poll_fallback_matrix() {
    for profile in [
        GeminiOAuthProfile::GeminiCli,
        GeminiOAuthProfile::Antigravity,
    ] {
        let provider = Provider::new().await;
        let mut fixture = DispatchFixture::new().await;
        // Keep metadata enrichment on this isolated provider too.
        let config = Arc::make_mut(&mut fixture.dispatch.config);
        config.oauth.gemini = Some(cc_lb_config::GeminiOAuthConfig {
            code_assist_url: provider.base.clone(),
            userinfo_url: provider.base.clone(),
            ..Default::default()
        });
        config.oauth.antigravity = Some(cc_lb_config::AntigravityOAuthConfig {
            code_assist_url: provider.base.clone(),
            userinfo_url: provider.base.clone(),
            ..Default::default()
        });
        let upstream = provider_metadata::create_provider(
            &fixture,
            UpstreamKind::GeminiOauth,
            provider.base.clone(),
            profile,
        )
        .await;
        let (_, prior_raw) = seed(&fixture, upstream.id, profile).await;
        let legacy = latest(&fixture, upstream.id).await;
        let observation = fixture
            .dispatch
            .poll_usage(upstream.id, None)
            .await
            .unwrap();
        assert!(matches!(
            observation,
            OAuthUsagePollObservation::Success { .. }
        ));
        assert_eq!(provider.state.quota_calls.load(Ordering::SeqCst), 1);
        assert!(
            fixture._quota_rx.try_recv().is_err(),
            "raw-only polls must not enqueue canonical Absent"
        );
        assert_eq!(latest(&fixture, upstream.id).await, legacy);
        let raw = load_gemini_period_unknown_quota(fixture.storage.as_ref(), upstream.id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            raw.gemini.as_ref().unwrap().observed_at_unix_millis
                > prior_raw.gemini.as_ref().unwrap().observed_at_unix_millis
        );
        assert_eq!(raw.gemini.as_ref().unwrap().meters.len(), 1);
        assert_eq!(raw.third_party.as_ref().unwrap().meters.len(), 2);

        // Every failed fallback preserves durable canonical history and raw meters.
        for (status, body) in [
            (500, "{}"),
            (403, "{}"),
            (429, "{}"),
            (
                200,
                r#"{"email":"secret-fallback-leak@example.test", invalid}"#,
            ),
        ] {
            provider.state.quota_status.store(status, Ordering::SeqCst);
            *provider.state.quota.lock().unwrap() = body.to_owned();
            let before_calls = provider.state.quota_calls.load(Ordering::SeqCst);
            let logs = CapturedLogs::default();
            let writer = logs.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .with_max_level(tracing::Level::WARN)
                .finish();
            assert!(matches!(
                fixture
                    .dispatch
                    .poll_usage(upstream.id, None)
                    .with_subscriber(subscriber)
                    .await
                    .unwrap(),
                OAuthUsagePollObservation::Success { .. }
            ));
            let captured = String::from_utf8(logs.0.lock().expect("captured logs lock").clone())
                .expect("UTF-8 tracing output");
            assert!(!captured.contains("secret-fallback-leak@example.test"));
            if status == 200 {
                assert!(captured.contains("Gemini quota fallback JSON parse failed"));
            }
            assert_eq!(
                provider.state.quota_calls.load(Ordering::SeqCst),
                before_calls + 1
            );
            assert!(fixture._quota_rx.try_recv().is_err());
            assert_eq!(latest(&fixture, upstream.id).await, legacy);
            assert_eq!(
                load_gemini_period_unknown_quota(fixture.storage.as_ref(), upstream.id)
                    .await
                    .unwrap(),
                Some(raw.clone())
            );
        }
        provider.state.quota_status.store(200, Ordering::SeqCst);
        *provider.state.quota.lock().unwrap() = raw_quota().to_string();

        // A failed summary can still yield a successful, raw-only observation.
        provider.state.summary_status.store(503, Ordering::SeqCst);
        assert!(matches!(
            fixture
                .dispatch
                .poll_usage(upstream.id, None)
                .await
                .unwrap(),
            OAuthUsagePollObservation::Success { .. }
        ));
        assert!(fixture._quota_rx.try_recv().is_err());
        assert_eq!(latest(&fixture, upstream.id).await, legacy);
        assert_eq!(
            load_gemini_period_unknown_quota(fixture.storage.as_ref(), upstream.id)
                .await
                .unwrap()
                .unwrap()
                .gemini
                .unwrap()
                .meters
                .len(),
            1
        );

        // Summary throttling must never be hidden behind fallback.
        provider.state.summary_status.store(429, Ordering::SeqCst);
        let before_calls = provider.state.quota_calls.load(Ordering::SeqCst);
        assert!(matches!(
            fixture
                .dispatch
                .poll_usage(upstream.id, None)
                .await
                .unwrap(),
            OAuthUsagePollObservation::Throttled { .. }
        ));
        assert_eq!(
            provider.state.quota_calls.load(Ordering::SeqCst),
            before_calls
        );
        assert!(fixture._quota_rx.try_recv().is_err());

        // Both healthy summary groups avoid fallback and clear stale raw groups.
        provider.state.summary_status.store(200, Ordering::SeqCst);
        *provider.state.summary.lock().unwrap() = healthy_summary();
        let before_calls = provider.state.quota_calls.load(Ordering::SeqCst);
        assert!(matches!(
            fixture
                .dispatch
                .poll_usage(upstream.id, None)
                .await
                .unwrap(),
            OAuthUsagePollObservation::Success { .. }
        ));
        assert_eq!(
            provider.state.quota_calls.load(Ordering::SeqCst),
            before_calls
        );
        let cleared = load_gemini_period_unknown_quota(fixture.storage.as_ref(), upstream.id)
            .await
            .unwrap()
            .unwrap();
        assert!(cleared.gemini.unwrap().meters.is_empty());
        assert!(cleared.third_party.unwrap().meters.is_empty());
        let mut published = Vec::new();
        while let Ok(record) = fixture._quota_rx.try_recv() {
            published.push(record);
        }
        assert_eq!(published.len(), 4);

        // Failed metadata persistence is best effort, not a failed usage poll.
        *provider.state.summary.lock().unwrap() = json!({"groups":[]});
        let fresh = provider_metadata::create_provider(
            &fixture,
            UpstreamKind::GeminiOauth,
            provider.base.clone(),
            profile,
        )
        .await;
        for operation in ["INSERT", "UPDATE"] {
            sqlx::query(match operation {
                "INSERT" => "CREATE TRIGGER reject_gemini_meta_insert BEFORE INSERT ON meta_v1 WHEN NEW.key LIKE 'gemini_period_unknown_quota:%' BEGIN SELECT RAISE(ABORT, 'synthetic metadata failure'); END",
                "UPDATE" => "CREATE TRIGGER reject_gemini_meta_update BEFORE UPDATE ON meta_v1 WHEN NEW.key LIKE 'gemini_period_unknown_quota:%' BEGIN SELECT RAISE(ABORT, 'synthetic metadata failure'); END",
                _ => unreachable!(),
            })
                .execute(fixture.storage.pool()).await.unwrap();
        }
        assert!(matches!(
            fixture.dispatch.poll_usage(fresh.id, None).await.unwrap(),
            OAuthUsagePollObservation::Success { .. }
        ));
        assert!(fixture._quota_rx.try_recv().is_err());
        assert!(
            load_gemini_period_unknown_quota(fixture.storage.as_ref(), fresh.id)
                .await
                .unwrap()
                .is_none()
        );
        for operation in ["INSERT", "UPDATE"] {
            sqlx::query(match operation {
                "INSERT" => "DROP TRIGGER reject_gemini_meta_insert",
                "UPDATE" => "DROP TRIGGER reject_gemini_meta_update",
                _ => unreachable!(),
            })
            .execute(fixture.storage.pool())
            .await
            .unwrap();
        }

        // Rotation while the provider is in flight invalidates all publication.
        let rotated = provider_metadata::create_provider(
            &fixture,
            UpstreamKind::GeminiOauth,
            provider.base.clone(),
            profile,
        )
        .await;
        let (_, before_raw) = seed(&fixture, rotated.id, profile).await;
        let before_legacy = latest(&fixture, rotated.id).await;
        provider.state.delay_quota.store(true, Ordering::SeqCst);
        let poll = fixture.dispatch.poll_usage(rotated.id, None);
        tokio::pin!(poll);
        tokio::select! {
            result = &mut poll => panic!("poll completed before rotation: {result:?}"),
            _ = provider.state.entered.notified() => {}
        }
        let mut bundle =
            crate::scheduler_dispatch::http::decrypt_bundle(&rotated, fixture.aead.as_ref())
                .unwrap();
        bundle.access_token = "rotated-synthetic-token".to_owned();
        let tokens =
            EncryptedOAuthTokens::encrypt(fixture.aead.as_ref(), &bundle, rotated.id.as_bytes())
                .unwrap();
        UpstreamStore::store_oauth_tokens(
            fixture.storage.as_ref(),
            rotated.id,
            rotated.revision,
            tokens,
            true,
        )
        .await
        .unwrap();
        provider.state.release.notify_one();
        assert!(poll.await.is_err());
        assert!(fixture._quota_rx.try_recv().is_err());
        assert_eq!(latest(&fixture, rotated.id).await, before_legacy);
        assert_eq!(
            load_gemini_period_unknown_quota(fixture.storage.as_ref(), rotated.id)
                .await
                .unwrap(),
            Some(before_raw)
        );
    }
}
