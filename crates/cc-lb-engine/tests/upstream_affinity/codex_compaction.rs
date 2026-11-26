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
    assert_eq!(disabled_response.status(), StatusCode::CONFLICT);
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

async fn assert_additional_tools_schema_annotations_are_not_affinity_state(
    annotation_type: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for annotation in ["default", "examples"] {
        let ciphertext = format!("synthetic-schema-{annotation_type}-{annotation}");
        let example = json!({
            "type": annotation_type,
            "encrypted_content": ciphertext,
        });
        let mut parameters = json!({
            "type": "object",
            "properties": {
                "type": {"type": "string"},
                "encrypted_content": {"type": "string"},
            },
            "required": ["type", "encrypted_content"],
            "additionalProperties": false,
        });
        parameters[annotation] = if annotation == "examples" {
            json!([example])
        } else {
            example
        };

        let upstream = Uuid::from_u128(0x130703);
        let store = Arc::new(MemoryAffinityStore::default());
        let provider_response = json!({
            "id": "resp_schema_annotation_control",
            "object": "response",
            "status": "completed",
            "output": [],
        });
        let dispatch =
            RecordingDispatch::with_response(ResponseSpec::Json(provider_response.clone()));
        let proxy = lifecycle(
            vec![openai_upstream_record(upstream, "healthy", true)],
            dispatch.clone(),
            Some(store.clone()),
        );
        // Codex a41fe5b, protocol/src/models.rs:1097-1105:
        // AdditionalTools carries a role and arbitrary JSON tool declarations.
        // The default/example is valid function input data, not a Responses item.
        let response = handle_authenticated(
            &proxy,
            responses_request(json!({
                "model": "gpt-5-codex",
                "store": false,
                "input": [
                    {
                        "type": "additional_tools",
                        "role": "system",
                        "tools": [{
                            "type": "function",
                            "name": "inspect_example",
                            "description": "Inspect a structured example.",
                            "parameters": parameters,
                        }],
                    },
                    {
                        "type": "message",
                        "role": "user",
                        "content": [{
                            "type": "input_text",
                            "text": "Say hello without calling a tool.",
                        }],
                    },
                ],
            })),
        )
        .await
        .expect("stateless tool-schema request handled");
        let status = response.status();
        let body = response.into_body().collect().await?.to_bytes();
        let value: Value = serde_json::from_slice(&body)?;
        assert_eq!(
            status,
            StatusCode::OK,
            "{annotation_type} schema {annotation} must not require an owner: {value}",
        );
        assert_eq!(value, provider_response);
        assert_eq!(dispatch.call_count(), 1);
        assert_eq!(
            dispatch.urls.lock().expect("dispatch URL lock").as_slice(),
            &["http://healthy.local/v1/responses".to_owned()],
        );
        assert_eq!(
            store.upstream_for(&ws1296_openai_key(&ciphertext)),
            None,
            "{annotation_type} schema {annotation} must not acquire an affinity binding",
        );
    }
    Ok(())
}

#[tokio::test]
async fn codex_additional_tools_schema_annotations_compaction()
-> Result<(), Box<dyn std::error::Error>> {
    assert_additional_tools_schema_annotations_are_not_affinity_state("compaction").await
}

#[tokio::test]
async fn codex_additional_tools_schema_annotations_compaction_summary()
-> Result<(), Box<dyn std::error::Error>> {
    assert_additional_tools_schema_annotations_are_not_affinity_state("compaction_summary").await
}

// The same recursive traversal already inspected reasoning before 7d3d29b;
// this adjacent control must not be attributed to the compaction change.
#[tokio::test]
async fn codex_additional_tools_schema_annotations_reasoning()
-> Result<(), Box<dyn std::error::Error>> {
    assert_additional_tools_schema_annotations_are_not_affinity_state("reasoning").await
}
