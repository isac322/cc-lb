use super::lazy_mode::{LazyFixture, TokenReply, successful_reply};
use super::*;

#[tokio::test]
async fn reconnect_latch_survives_reopened_storage_and_replacement_scheduled_handler() {
    for (status, body, reason) in [
        (
            StatusCode::BAD_REQUEST,
            serde_json::json!({"error": "invalid_grant", "error_description": "private provider details"}),
            "status_400",
        ),
        (
            StatusCode::UNAUTHORIZED,
            serde_json::json!({"error": "unauthorized"}),
            "status_401",
        ),
    ] {
        let fixture =
            LazyFixture::new(vec![TokenReply::new(status, body), successful_reply()]).await;
        let upstream_id = fixture.create_upstream().await;
        fixture
            .refresher(fixture.claims.clone())
            .refresh_one(upstream_id)
            .await
            .expect_err("real provider failure requires reconnect");
        let failed = fixture.record(upstream_id).await;
        assert_eq!(failed.last_apply_error.as_deref(), Some(reason));
        assert_eq!(fixture.endpoint_calls(), 1);

        let database_url = format!(
            "sqlite://{}",
            fixture.inner._dir.path().join("oauth.sqlite").display()
        );
        let reopened = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, fixture.inner.clock.clone())
                .await
                .expect("replacement storage opens"),
        );
        for storage in [fixture.inner.storage.clone(), reopened.clone()] {
            let outcome = dispatch_oauth_refresh_job(
                fixture.inner.scheduler_backend.clone(),
                storage,
                fixture.inner.aead.clone(),
                fixture.inner.oauth_cfg.clone(),
                fixture.inner.clock.clone(),
                AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
            )
            .await
            .expect("scheduled handler reads durable reconnect state");
            assert_eq!(outcome, JobOutcome::Skip);
        }
        assert_eq!(fixture.endpoint_calls(), 1);
        let unchanged = UpstreamStore::get_by_id(reopened.as_ref(), upstream_id)
            .await
            .expect("reopened upstream read")
            .expect("upstream exists");
        assert_eq!(
            unchanged.oauth_token_generation,
            failed.oauth_token_generation
        );
        assert_eq!(unchanged.last_apply_error.as_deref(), Some(reason));

        fixture.replace_tokens(upstream_id, None).await;
        let reconnected = fixture.record(upstream_id).await;
        assert!(reconnected.oauth_token_generation > failed.oauth_token_generation);
        assert_eq!(reconnected.last_apply_error, None);
        assert_eq!(
            dispatch_oauth_refresh_job(
                fixture.inner.scheduler_backend.clone(),
                reopened.clone(),
                fixture.inner.aead.clone(),
                fixture.inner.oauth_cfg.clone(),
                fixture.inner.clock.clone(),
                AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
            )
            .await
            .expect("scheduled refresh resumes after reconnect"),
            JobOutcome::Done
        );
        assert_eq!(fixture.endpoint_calls(), 2);
        let refreshed = UpstreamStore::get_by_id(reopened.as_ref(), upstream_id)
            .await
            .expect("refreshed upstream read")
            .expect("upstream exists");
        assert!(refreshed.oauth_token_generation > reconnected.oauth_token_generation);
        assert_eq!(refreshed.last_apply_error, None);
        let tokens = refreshed
            .oauth_credentials
            .expect("refreshed credentials")
            .decrypt(fixture.inner.aead.as_ref(), upstream_id.as_bytes())
            .expect("decrypt refreshed credential");
        assert_eq!(tokens.access_token, "access-refreshed");
    }
}
