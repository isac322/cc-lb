//! Revisioning and metadata conformance scenarios.

use std::sync::Arc;

use anyhow::Result;
use cc_lb_storage_api::{ConfigDraftState, ConfigStore, HistoryEntry, MetaStore, StorageError};
use serde_json::json;

use crate::harness::{ConformanceBackend, ConformanceFixture};

pub async fn config_draft_optimistic_revision<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();

        let initial = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(initial, ConfigDraftState::default());

        let missing =
            ConfigStore::put_config_draft(storage.as_ref(), ConfigDraftState::default(), 1)
                .await
                .expect_err("a missing draft cannot match a nonzero revision");
        assert!(matches!(missing, StorageError::Conflict { .. }));
        assert_eq!(
            ConfigStore::get_config_draft(storage.as_ref()).await?,
            ConfigDraftState::default()
        );

        let revision = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_000),
                last_validated_revision: Some(99),
                last_validation: Some(json!({
                    "file": {
                        "valid": false,
                        "issues": [{
                            "path": "upstreams.primary",
                            "code": "missing_required",
                            "message": "stale validation",
                            "severity": "error"
                        }]
                    },
                    "effective": { "valid": false, "issues": [] },
                    "filesystem": [],
                    "overrides": []
                })),
                ..ConfigDraftState::default()
            },
            0,
        )
        .await?;
        assert_eq!(revision, 1);

        let stored = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(stored.revision, 1);
        assert_eq!(stored.saved_at_unix_secs, Some(1_800_000_000));
        assert_eq!(stored.last_validated_revision, None);
        assert_eq!(stored.last_validation, None);

        let stale = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_001),
                ..ConfigDraftState::default()
            },
            0,
        )
        .await
        .expect_err("stale draft revision must not be accepted");
        assert!(matches!(stale, StorageError::Conflict { .. }));

        let revision = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_002),
                ..ConfigDraftState::default()
            },
            1,
        )
        .await?;
        assert_eq!(revision, 2);

        let stored = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(stored.revision, 2);
        assert_eq!(stored.saved_at_unix_secs, Some(1_800_000_002));

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn config_history_cap_50<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();

        for revision in 1..=60 {
            ConfigStore::append_config_history(
                storage.as_ref(),
                revision,
                1_800_000_000 + revision,
            )
            .await?;
        }

        let entries = ConfigStore::list_config_history(storage.as_ref(), 100).await?;
        let expected = (11..=60)
            .rev()
            .map(|revision| HistoryEntry {
                revision,
                applied_at_unix_secs: 1_800_000_000 + revision,
            })
            .collect::<Vec<_>>();
        assert_eq!(entries, expected);

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn config_last_validated_revision<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();

        let revision = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_010),
                ..ConfigDraftState::default()
            },
            0,
        )
        .await?;
        assert_eq!(revision, 1);

        let failure = json!({
            "file": {"valid": false, "issues": [{"path": "upstreams.primary", "code": "missing_required", "message": "missing upstream", "severity": "error"}]},
            "effective": {"valid": false, "issues": [{"path": "upstreams.primary", "code": "missing_required", "message": "missing upstream", "severity": "error"}]},
            "filesystem": [{"path": "runtime.data_dir", "code": "not_writable", "message": "runtime data directory is not writable", "severity": "warning"}],
            "overrides": []
        });
        ConfigStore::set_config_validation(storage.as_ref(), revision, false, failure.clone()).await?;
        let draft = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(draft.revision, revision);
        assert_eq!(draft.last_validated_revision, None);
        assert_eq!(draft.last_validation, Some(failure));

        let success_warning = json!({
            "file": {"valid": true, "issues": []},
            "effective": {"valid": true, "issues": []},
            "filesystem": [{"path": "runtime.data_dir", "code": "deprecated", "message": "legacy directory", "severity": "warning"}],
            "overrides": []
        });
        ConfigStore::set_config_validation(storage.as_ref(), revision, true, success_warning.clone()).await?;
        let draft = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(draft.last_validated_revision, Some(revision));
        assert_eq!(draft.last_validation, Some(success_warning));

        let next_revision = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_011),
                ..ConfigDraftState::default()
            },
            revision,
        )
        .await?;
        assert_eq!(next_revision, 2);
        let stale = ConfigStore::set_config_validation(
            storage.as_ref(),
            revision,
            true,
            json!({"stale": true}),
        )
        .await
        .expect_err("validation must reject stale draft revisions");
        assert!(matches!(stale, StorageError::Conflict { .. }));

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn meta_compare_and_put<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    const RACERS: usize = 8;
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let key = "conformance_cas";

        // Absent key: `Some(expected)` cannot match, `None` inserts once.
        assert!(
            !MetaStore::compare_and_put_meta_value(storage.as_ref(), key, Some("a"), "b").await?
        );
        assert_eq!(
            MetaStore::get_meta_value(storage.as_ref(), key).await?,
            None
        );
        assert!(MetaStore::compare_and_put_meta_value(storage.as_ref(), key, None, "a").await?);
        assert!(!MetaStore::compare_and_put_meta_value(storage.as_ref(), key, None, "z").await?);
        assert_eq!(
            MetaStore::get_meta_value(storage.as_ref(), key)
                .await?
                .as_deref(),
            Some("a")
        );

        // Present key: only the exact current value matches.
        assert!(
            !MetaStore::compare_and_put_meta_value(storage.as_ref(), key, Some("x"), "b").await?
        );
        assert!(
            MetaStore::compare_and_put_meta_value(storage.as_ref(), key, Some("a"), "b").await?
        );
        assert_eq!(
            MetaStore::get_meta_value(storage.as_ref(), key)
                .await?
                .as_deref(),
            Some("b")
        );

        // Concurrent racers expecting the same value: exactly one wins and
        // the stored value is the winner's.
        for (race_key, expected) in [
            ("conformance_cas_race_present", Some("b")),
            ("conformance_cas_race_absent", None),
        ] {
            if let Some(seed) = expected {
                MetaStore::put_meta_value(storage.as_ref(), race_key, seed).await?;
            }
            let handles = (0..RACERS)
                .map(|racer| {
                    let storage = Arc::clone(&storage);
                    tokio::spawn(async move {
                        let value = format!("racer-{racer}");
                        let won = MetaStore::compare_and_put_meta_value(
                            storage.as_ref(),
                            race_key,
                            expected,
                            &value,
                        )
                        .await?;
                        Ok::<_, StorageError>(won.then_some(value))
                    })
                })
                .collect::<Vec<_>>();
            let mut winners = Vec::new();
            for handle in handles {
                if let Some(value) = handle.await?? {
                    winners.push(value);
                }
            }
            assert_eq!(
                winners.len(),
                1,
                "exactly one CAS racer must win: {winners:?}"
            );
            assert_eq!(
                MetaStore::get_meta_value(storage.as_ref(), race_key).await?,
                winners.pop()
            );
        }

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn meta_backend_kind_stamp<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let expected = fixture.backend_kind();

        MetaStore::initialize(storage.as_ref()).await?;
        assert_eq!(MetaStore::backend_kind(storage.as_ref()).await?, expected);

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}
