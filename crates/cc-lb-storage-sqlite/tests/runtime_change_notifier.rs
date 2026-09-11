use std::{collections::BTreeMap, sync::Arc, time::Duration};

use cc_lb_clock::{Clock, TestClock};
use cc_lb_storage_api::{
    BackendKind, ChangeChannel, ChangeEvent, Limit, LimitKind, ManagedKeyStore, MetaStore,
    PluginChainEntryInput, PluginRegistryStore, PluginSlotKind, PrincipalCreate, PrincipalKind,
    PrincipalKindLite, PrincipalStore, RuntimeChangeNotifier, UpstreamCreate, UpstreamStore,
    WasmBlob, WasmRegistryEntryInput,
    types::{IssueParams, UpstreamKind as ManagedKeyUpstreamKind},
    upstream::UpstreamKind,
};
use cc_lb_storage_sqlite::SqliteStorage;
use serde_json::json;
use tokio::{sync::broadcast, time};
use uuid::Uuid;

const NOW: u64 = 1_700_000_000;
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(1);
const ABSENCE_WINDOW: Duration = Duration::from_millis(50);

async fn storage() -> (tempfile::TempDir, Arc<TestClock>, SqliteStorage) {
    let directory = tempfile::tempdir().expect("create notifier tempdir");
    let database_url = format!(
        "sqlite://{}",
        directory
            .path()
            .join("runtime-change-notifier.sqlite")
            .display()
    );
    let clock = Arc::new(TestClock::new_at_secs(NOW));
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
        .await
        .expect("open notifier sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("migrate notifier sqlite");
    (directory, clock, storage)
}

fn upstream(name: &str) -> UpstreamCreate {
    UpstreamCreate {
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        ..UpstreamCreate::default()
    }
}

fn principal(name: &str) -> PrincipalCreate {
    PrincipalCreate {
        name: name.to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["claude-sonnet-*".to_owned()],
        allowed_upstreams: Vec::new(),
        default_limits: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        }],
        cache_keepalive: None,
    }
}

fn issue_params() -> IssueParams {
    IssueParams {
        label: "notifier-managed-key".to_owned(),
        description: None,
        upstream_kind: ManagedKeyUpstreamKind::AnthropicKey,
        expires_at_unix_secs: None,
        limit_overrides: Vec::new(),
        secret_salt: [1; 16],
        verify_hash: [2; 32],
        last_4: "0001".to_owned(),
        principal_kind: PrincipalKindLite::Machine,
        index_hash: [3; 32],
    }
}

fn plugin_input(name: &str) -> WasmRegistryEntryInput {
    WasmRegistryEntryInput {
        schema_hash: None,
        name: name.to_owned(),
        version: None,
        original_filename: format!("{name}.wasm"),
        label: None,
        uploaded_at_unix_secs: NOW,
        uploaded_by_admin_id: Uuid::from_u128(40),
        description: "notifier plugin".to_owned(),
        usage: "notifier test".to_owned(),
        hook_metadata: BTreeMap::new(),
        supported_slots: Vec::new(),
    }
}

async fn recv_matching(
    receiver: &mut broadcast::Receiver<ChangeEvent>,
    channel: ChangeChannel,
    payload: &str,
) -> ChangeEvent {
    let deadline = time::Instant::now() + RECEIVE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for {channel:?}/{payload}"
        );
        match time::timeout(remaining, receiver.recv()).await {
            Ok(Ok(event)) if event.channel == channel && event.payload == payload => return event,
            Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
            Ok(Err(broadcast::error::RecvError::Closed)) => panic!("change channel closed"),
            Err(_) => panic!("timed out waiting for {channel:?}/{payload}"),
        }
    }
}

async fn assert_no_matching(
    receiver: &mut broadcast::Receiver<ChangeEvent>,
    channel: ChangeChannel,
    payload: &str,
) {
    let deadline = time::Instant::now() + ABSENCE_WINDOW;
    loop {
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        match time::timeout(remaining, receiver.recv()).await {
            Ok(Ok(event)) if event.channel == channel && event.payload == payload => {
                panic!("unexpected change event {channel:?}/{payload}")
            }
            Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
            Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => return,
        }
    }
}

#[tokio::test]
async fn t3__mutation_succeeds_without_local_subscriber() {
    let (_directory, _clock, storage) = storage().await;

    let record = UpstreamStore::create(&storage, upstream("notifier-no-subscriber"))
        .await
        .expect("create upstream without subscriber");

    assert_eq!(
        UpstreamStore::get_by_id(&storage, record.id)
            .await
            .expect("read created upstream")
            .expect("created upstream exists")
            .id,
        record.id
    );
}

#[tokio::test]
async fn t3__committed_mutations_emit_exact_channels_and_payloads() {
    let (_directory, clock, storage) = storage().await;
    let mut receiver = storage.subscribe().await.expect("subscribe");

    let upstream = UpstreamStore::create(&storage, upstream("notifier-exact-upstream"))
        .await
        .expect("create upstream");
    let event = recv_matching(
        &mut receiver,
        ChangeChannel::Upstream,
        &upstream.id.to_string(),
    )
    .await;
    assert_eq!(event.observed_at, clock.now());

    let principal = PrincipalStore::create(&storage, principal("notifier-exact-principal"), NOW)
        .await
        .expect("create principal");
    recv_matching(
        &mut receiver,
        ChangeChannel::Principal,
        &principal.id.to_string(),
    )
    .await;

    let principal_id = principal.id.to_string();
    storage
        .issue(&principal_id, "notifier-key", issue_params())
        .await
        .expect("issue managed key");
    recv_matching(&mut receiver, ChangeChannel::Principal, &principal_id).await;

    let (plugin, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [9; 32],
                size_bytes: 3,
                bytes: vec![0, 97, 115],
                parse_validated_at_unix_secs: NOW,
            },
            plugin_input("notifier-exact-plugin"),
        )
        .await
        .expect("persist plugin");
    recv_matching(
        &mut receiver,
        ChangeChannel::PluginRegistry,
        &plugin.id.to_string(),
    )
    .await;

    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id: principal.id,
            slot: PluginSlotKind::Router,
            order: 100,
            wasm_registry_id: plugin.id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
        })
        .await
        .expect("insert plugin chain");
    recv_matching(&mut receiver, ChangeChannel::PluginChain, &principal_id).await;
}

#[tokio::test]
async fn t3__rolled_back_mutation_emits_no_change() {
    let (_directory, _clock, storage) = storage().await;
    let upstream = UpstreamStore::create(&storage, upstream("notifier-rolled-back-mutation"))
        .await
        .expect("create upstream fixture");
    sqlx::query(
        "CREATE TRIGGER reject_notifier_upstream_delete \
         AFTER DELETE ON upstream_spec_v1 \
         BEGIN SELECT RAISE(ABORT, 'forced notifier rollback'); END",
    )
    .execute(storage.pool())
    .await
    .expect("install rollback trigger");
    let mut receiver = storage.subscribe().await.expect("subscribe after fixture");
    let payload = upstream.id.to_string();

    UpstreamStore::hard_delete(&storage, upstream.id)
        .await
        .expect_err("trigger must roll back delete");

    assert!(
        UpstreamStore::get_by_id(&storage, upstream.id)
            .await
            .expect("read rolled-back upstream")
            .is_some(),
        "rolled-back upstream must remain stored"
    );
    assert_no_matching(&mut receiver, ChangeChannel::Upstream, &payload).await;
}

#[tokio::test]
async fn t3__change_is_published_only_after_write_transaction_releases() {
    let (_directory, _clock, storage) = storage().await;
    let upstream = UpstreamStore::create(&storage, upstream("notifier-commit-order"))
        .await
        .expect("create upstream fixture");
    let mut receiver = storage.subscribe().await.expect("subscribe after fixture");
    let payload = upstream.id.to_string();
    let write_guard = storage
        .begin_immediate()
        .await
        .expect("hold write transaction");
    let task_storage = storage.clone();

    let delete =
        tokio::spawn(async move { UpstreamStore::hard_delete(&task_storage, upstream.id).await });
    assert_no_matching(&mut receiver, ChangeChannel::Upstream, &payload).await;

    drop(write_guard);
    delete
        .await
        .expect("delete task joins")
        .expect("delete commits after guard release");
    recv_matching(&mut receiver, ChangeChannel::Upstream, &payload).await;
}
