use super::*;

const COMPACTION_ENCRYPTED_CONTENT: &str = "synthetic-compaction-encrypted-content";
const COMPACTION_SUMMARY_ENCRYPTED_CONTENT: &str = "synthetic-compaction-summary-encrypted-content";
const COMPACTION_RESPONSE_ID: &str = "resp_codex_compaction";

fn compaction_item() -> Value {
    json!({
        "type": "compaction",
        "id": "cmp_codex_compaction",
        "encrypted_content": COMPACTION_ENCRYPTED_CONTENT,
    })
}

fn compaction_summary_item() -> Value {
    json!({
        "type": "compaction_summary",
        "id": "cmp_codex_compaction_summary",
        "encrypted_content": COMPACTION_SUMMARY_ENCRYPTED_CONTENT,
    })
}

fn compaction_summary_continuation() -> Value {
    json!({
        "model": "gpt-5-codex",
        "store": false,
        "include": ["reasoning.encrypted_content"],
        "input": [
            compaction_summary_item(),
            {"role": "user", "content": "continue after compaction summary"},
        ],
    })
}

fn compaction_item_done() -> Bytes {
    Bytes::from(
        json!({
            "type": "response.output_item.done",
            "sequence_number": 3,
            "output_index": 0,
            "item": compaction_item(),
        })
        .to_string(),
    )
}

fn compaction_continuation() -> Value {
    json!({
        "model": "gpt-5-codex",
        "store": false,
        "include": ["reasoning.encrypted_content"],
        "input": [
            compaction_item(),
            {"role": "user", "content": "continue after compaction"},
        ],
    })
}

#[tokio::test]
async fn codex_compaction_encrypted_content_binds_owner_across_http_continuation()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = Uuid::from_u128(0x130701);
    let fallback = Uuid::from_u128(0x130702);
    let store = Arc::new(MemoryAffinityStore::default());

    // A stateless, role-aware user message remains healthy and does not create
    // a broad binding before provider state has been observed.
    let stateless_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_stateless",
        "object": "response",
        "status": "completed",
        "output": [],
    })));
    let stateless = lifecycle(
        vec![openai_upstream_record(owner, "owner", true)],
        stateless_dispatch.clone(),
        Some(store.clone()),
    );
    let stateless_response = handle_authenticated(
        &stateless,
        responses_request(json!({
            "model": "gpt-5-codex",
            "store": false,
            "input": [{
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": "first turn",
                    "encrypted_content": "unrelated-input-text-state",
                }],
            }],
        })),
    )
    .await
    .expect("stateless request handled");
    assert_eq!(stateless_response.status(), StatusCode::OK);
    assert_eq!(stateless_dispatch.call_count(), 1);
    assert_eq!(
        store.upstream_for(&ws1296_openai_key("unrelated-input-text-state")),
        None,
        "unrelated input_text encrypted content is not provider state",
    );
    // The provider's actual-shaped output_item.done followed by
    // response.completed is the source of durable compaction state.
    let learning_dispatch = Arc::new(RecordingDispatch::default());
    let mut gateway = ws1296_gateway(lifecycle(
        vec![openai_upstream_record(owner, "owner", true)],
        learning_dispatch,
        Some(store.clone()),
    ))
    .await;
    let turn = ws1296_admit(&mut gateway, ws1296_create(None, None))
        .await
        .expect("learning turn admitted")
        .expect("learning turn dispatches");
    ws1296_send(&mut gateway, &turn, 1).await;
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(1, &ws1296_created(COMPACTION_RESPONSE_ID))
            .await
            .expect("provider response created"),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(1, &compaction_item_done())
            .await
            .expect("provider compaction item completed"),
    );
    ws1296_assert_raw_forward(
        &gateway
            .provider_message(
                1,
                &ws1296_completed(COMPACTION_RESPONSE_ID, vec![compaction_item()]),
            )
            .await
            .expect("provider response completed"),
    );
    gateway.close();

    let healthy_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_healthy_compaction",
        "object": "response",
        "status": "completed",
        "output": [],
    })));
    let healthy = lifecycle(
        vec![
            openai_upstream_record(owner, "owner", true),
            openai_upstream_record(fallback, "fallback", true),
        ],
        healthy_dispatch.clone(),
        Some(store.clone()),
    );
    let healthy_response =
        handle_authenticated(&healthy, responses_request(compaction_continuation()))
            .await
            .expect("healthy continuation handled");
    assert_eq!(healthy_response.status(), StatusCode::OK);
    assert_eq!(
        healthy_dispatch
            .urls
            .lock()
            .expect("dispatch URL lock")
            .as_slice(),
        &["http://owner.local/v1/responses".to_owned()]
    );

    // `compaction_summary` is a source-backed client alias. Seed its
    // provider state directly; it is not learned from a live response event.
    store.insert(
        ws1296_openai_key(COMPACTION_SUMMARY_ENCRYPTED_CONTENT),
        owner,
    );
    let alias_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({
        "id": "resp_healthy_compaction_summary",
        "object": "response",
        "status": "completed",
        "output": [],
    })));
    let alias_lifecycle = lifecycle(
        vec![
            openai_upstream_record(fallback, "fallback", true),
            openai_upstream_record(owner, "owner", true),
        ],
        alias_dispatch.clone(),
        Some(store.clone()),
    );
    let alias_response = handle_authenticated(
        &alias_lifecycle,
        responses_request(compaction_summary_continuation()),
    )
    .await
    .expect("compaction summary continuation handled");
    assert_eq!(alias_response.status(), StatusCode::OK);
    assert_eq!(
        alias_dispatch
            .urls
            .lock()
            .expect("dispatch URL lock")
            .as_slice(),
        &["http://owner.local/v1/responses".to_owned()]
    );

    let observed_store = store.clone();
    let disabled_dispatch = RecordingDispatch::with_response(ResponseSpec::Json(json!({})));
    let disabled_owner = lifecycle(
        vec![
            openai_upstream_record(owner, "owner", false),
            openai_upstream_record(fallback, "fallback", true),
        ],
        disabled_dispatch.clone(),
        Some(store),
    );
    let disabled_response = handle_authenticated(
        &disabled_owner,
        responses_request(compaction_continuation()),
    )
    .await
    .expect("disabled owner continuation handled");
    assert_eq!(disabled_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = disabled_response.into_body().collect().await?.to_bytes();
    let value: Value = serde_json::from_slice(&body)?;
    assert_eq!(value["error"]["type"], "upstream_affinity_unavailable");
    assert_eq!(disabled_dispatch.call_count(), 0, "must not fail over to B");
    assert_eq!(
        observed_store.upstream_for(&ws1296_openai_key(COMPACTION_RESPONSE_ID)),
        None,
        "store:false response ids remain connection-local",
    );
    Ok(())
}
