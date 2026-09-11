use serde_json::{Value, json};

use cc_lb_engine::model_resolution::canonical_model_id;
use cc_lb_engine::prompt_cache_simulator::{V3PromptCacheBlockSource, analyze_v3_prompt_cache};

const CANONICAL_MODEL: &str = "claude-sonnet-4-5-20250929";

fn salt_scope_request() -> Value {
    json!({
        "model": CANONICAL_MODEL,
        "tools": [{
            "name":"lookup",
            "description":"lookup",
            "input_schema":{"type":"object"},
            "cache_control":{"type":"ephemeral"}
        }],
        "system": [{
            "type":"text",
            "text":"stable system",
            "cache_control":{"type":"ephemeral"}
        }],
        "messages": [{"role":"user","content":[{
            "type":"text",
            "text":"stable message",
            "cache_control":{"type":"ephemeral"}
        }]}]
    })
}

fn with_top_level(mut request: Value, key: &str, value: Value) -> Value {
    let Value::Object(map) = &mut request else {
        panic!("salt-scope fixture must be an object");
    };
    map.insert(key.to_owned(), value);
    request
}

fn invalidator_variants(request: &Value) -> Vec<(&'static str, Value)> {
    vec![
        (
            "thinking",
            with_top_level(
                request.clone(),
                "thinking",
                json!({"type":"enabled","budget_tokens":2048}),
            ),
        ),
        (
            "effort",
            with_top_level(request.clone(), "output_config", json!({"effort":"low"})),
        ),
        (
            "speed",
            with_top_level(request.clone(), "speed", json!("fast")),
        ),
        (
            "tool_choice",
            with_top_level(request.clone(), "tool_choice", json!({"type":"any"})),
        ),
    ]
}

fn all_invalidator_request(request: &Value) -> Value {
    let request = with_top_level(
        request.clone(),
        "thinking",
        json!({"type":"enabled","budget_tokens":2048}),
    );
    let request = with_top_level(request, "output_config", json!({"effort":"low"}));
    let request = with_top_level(request, "speed", json!("fast"));
    with_top_level(request, "tool_choice", json!({"type":"any"}))
}

fn breakpoint_prefix_key(request: &Value, source: V3PromptCacheBlockSource) -> String {
    let analysis = analyze_v3_prompt_cache(request, canonical_model_id(CANONICAL_MODEL));
    let Some(breakpoint) = analysis
        .breakpoints
        .iter()
        .find(|breakpoint| breakpoint.source == source)
    else {
        panic!("salt-scope fixture must contain a {source:?} breakpoint");
    };
    breakpoint.prefix_key.clone()
}

#[test]
fn v5_blake3_prefix_key_golden() {
    let request = json!({
        "model": CANONICAL_MODEL,
        "system": [
            {"type":"text","text":"stable system prefix"},
            {"type":"text","text":"stable cache breakpoint","cache_control":{"type":"ephemeral","ttl":"1h"}}
        ],
        "messages": [{"role":"user","content":[{"type":"text","text":"tail"}]}]
    });

    let analysis = analyze_v3_prompt_cache(&request, canonical_model_id(CANONICAL_MODEL));
    let breakpoint = analysis.breakpoints.first().expect("v5 breakpoint");

    assert_eq!(breakpoint.block_index, 1);
    assert_eq!(
        breakpoint.prefix_key,
        "7d44779303f224534f0737cb9d71fb39f3606798b09b7dec4e52d8b56d20e784"
    );
}

#[test]
fn v5_tools_scope_salt_golden() {
    let request = salt_scope_request();
    let baseline = breakpoint_prefix_key(&request, V3PromptCacheBlockSource::Tools);
    let fully_salted = breakpoint_prefix_key(
        &all_invalidator_request(&request),
        V3PromptCacheBlockSource::Tools,
    );

    assert_eq!(
        fully_salted,
        "ee7065c21fb86d116f207ef7f728a35e3688e32c696aad1ee7abf5a429c34607"
    );
    assert_eq!(fully_salted, baseline);
    for (name, variant) in invalidator_variants(&request) {
        assert_eq!(
            breakpoint_prefix_key(&variant, V3PromptCacheBlockSource::Tools),
            baseline,
            "{name} must not perturb a tools-source prefix"
        );
    }
}

#[test]
fn v5_system_scope_salt_golden() {
    let request = salt_scope_request();
    let baseline = breakpoint_prefix_key(&request, V3PromptCacheBlockSource::System);
    let fully_salted = breakpoint_prefix_key(
        &all_invalidator_request(&request),
        V3PromptCacheBlockSource::System,
    );

    assert_eq!(
        fully_salted,
        "50b0ebb0432ed4899e7183e5ce50244730dcd026bc0908b798dfe53966c2400b"
    );
    assert_ne!(fully_salted, baseline);
    for (name, variant) in invalidator_variants(&request) {
        let variant_key = breakpoint_prefix_key(&variant, V3PromptCacheBlockSource::System);
        if name == "speed" {
            assert_eq!(
                variant_key, fully_salted,
                "speed must perturb a system-source prefix"
            );
        } else {
            assert_eq!(
                variant_key, baseline,
                "{name} must not perturb a system-source prefix"
            );
        }
    }
}

#[test]
fn v5_messages_scope_salt_golden() {
    let request = salt_scope_request();
    let baseline = breakpoint_prefix_key(&request, V3PromptCacheBlockSource::Message);
    let fully_salted = breakpoint_prefix_key(
        &all_invalidator_request(&request),
        V3PromptCacheBlockSource::Message,
    );

    assert_eq!(
        fully_salted,
        "38495b16fa68cceaa0fa0b804cb629e747327111a623d84c2c7394d3ab10526e"
    );
    assert_ne!(fully_salted, baseline);
    for (name, variant) in invalidator_variants(&request) {
        assert_ne!(variant, request, "{name} mutator must change the request");
        assert_ne!(
            breakpoint_prefix_key(&variant, V3PromptCacheBlockSource::Message),
            baseline,
            "{name} must perturb a messages-source prefix"
        );
    }
}

#[test]
fn flatten_order_is_tools_then_system_then_messages() {
    let request = json!({
        "model": CANONICAL_MODEL,
        "tools": [
            {"name":"alpha","description":"alpha","input_schema":{"type":"object"}},
            {"name":"beta","description":"beta","input_schema":{"type":"object"}}
        ],
        "system": [
            {"type":"text","text":"system one"},
            {"type":"text","text":"system two"}
        ],
        "messages": [
            {"role":"user","content":[
                {"type":"text","text":"user one"},
                {"type":"text","text":"user two"}
            ]}
        ]
    });

    let analysis = analyze_v3_prompt_cache(&request, canonical_model_id(CANONICAL_MODEL));

    let sources = analysis
        .blocks
        .iter()
        .map(|block| block.source)
        .collect::<Vec<_>>();
    assert_eq!(
        sources,
        vec![
            V3PromptCacheBlockSource::Tools,
            V3PromptCacheBlockSource::Tools,
            V3PromptCacheBlockSource::System,
            V3PromptCacheBlockSource::System,
            V3PromptCacheBlockSource::Message,
            V3PromptCacheBlockSource::Message,
        ]
    );

    let tools_count = sources
        .iter()
        .filter(|source| **source == V3PromptCacheBlockSource::Tools)
        .count();
    let system_count = sources
        .iter()
        .filter(|source| **source == V3PromptCacheBlockSource::System)
        .count();
    let last_tools_index = sources
        .iter()
        .rposition(|source| *source == V3PromptCacheBlockSource::Tools)
        .expect("tools present");
    let first_system_index = sources
        .iter()
        .position(|source| *source == V3PromptCacheBlockSource::System)
        .expect("system present");
    let last_system_index = sources
        .iter()
        .rposition(|source| *source == V3PromptCacheBlockSource::System)
        .expect("system present");
    let first_message_index = sources
        .iter()
        .position(|source| *source == V3PromptCacheBlockSource::Message)
        .expect("message present");

    assert_eq!(tools_count, 2);
    assert_eq!(system_count, 2);
    assert!(last_tools_index < first_system_index);
    assert!(last_system_index < first_message_index);
}

#[test]
fn lookback_window_spans_at_most_twenty_positions() {
    let deepest_index = 24_u64;
    let content = (0..=deepest_index)
        .map(|index| {
            if index == deepest_index {
                json!({"type":"text","text":format!("block-{index}"),"cache_control":{"type":"ephemeral"}})
            } else {
                json!({"type":"text","text":format!("block-{index}")})
            }
        })
        .collect::<Vec<_>>();
    let request = json!({
        "model": CANONICAL_MODEL,
        "messages": [{"role":"user","content": content}]
    });

    let analysis = analyze_v3_prompt_cache(&request, canonical_model_id(CANONICAL_MODEL));

    let breakpoint = analysis
        .breakpoints
        .first()
        .expect("deepest cache_control produces a breakpoint");
    assert_eq!(breakpoint.block_index, deepest_index);

    let covered_indices = breakpoint
        .lookback_prefixes
        .iter()
        .map(|prefix| prefix.content_block_index)
        .collect::<Vec<_>>();
    let max_distance = breakpoint
        .lookback_prefixes
        .iter()
        .map(|prefix| prefix.lookback_distance)
        .max()
        .expect("lookback prefixes present");

    assert_eq!(breakpoint.lookback_prefixes.len(), 20);
    assert!(max_distance <= 19);
    assert!(covered_indices.contains(&deepest_index));
    assert!(covered_indices.contains(&(deepest_index - 19)));
    assert!(!covered_indices.contains(&(deepest_index - 20)));
}

#[test]
fn cache_control_excluded_from_prefix_key() {
    let request_a = json!({
        "model": CANONICAL_MODEL,
        "messages": [{"role":"user","content": [
            {"type":"text","text":"stable prefix content","cache_control":{"type":"ephemeral"}}
        ]}]
    });
    let request_b = json!({
        "model": CANONICAL_MODEL,
        "messages": [{"role":"user","content": [
            {"type":"text","text":"stable prefix content","cache_control":{"type":"ephemeral","ttl":"1h"}}
        ]}]
    });

    let analysis_a = analyze_v3_prompt_cache(&request_a, canonical_model_id(CANONICAL_MODEL));
    let analysis_b = analyze_v3_prompt_cache(&request_b, canonical_model_id(CANONICAL_MODEL));

    let breakpoint_a = analysis_a
        .breakpoints
        .first()
        .expect("request A breakpoint");
    let breakpoint_b = analysis_b
        .breakpoints
        .first()
        .expect("request B breakpoint");
    assert_eq!(breakpoint_a.block_index, breakpoint_b.block_index);
    assert_eq!(breakpoint_a.prefix_key, breakpoint_b.prefix_key);
    assert_eq!(breakpoint_a.ttl.as_deref(), Some("5m"));
    assert_eq!(breakpoint_b.ttl.as_deref(), Some("1h"));

    let request_without_earlier = json!({
        "model": CANONICAL_MODEL,
        "messages": [{"role":"user","content": [
            {"type":"text","text":"first block"},
            {"type":"text","text":"deep breakpoint block","cache_control":{"type":"ephemeral"}}
        ]}]
    });
    let request_with_earlier = json!({
        "model": CANONICAL_MODEL,
        "messages": [{"role":"user","content": [
            {"type":"text","text":"first block","cache_control":{"type":"ephemeral"}},
            {"type":"text","text":"deep breakpoint block","cache_control":{"type":"ephemeral"}}
        ]}]
    });

    let without_earlier = analyze_v3_prompt_cache(
        &request_without_earlier,
        canonical_model_id(CANONICAL_MODEL),
    );
    let with_earlier =
        analyze_v3_prompt_cache(&request_with_earlier, canonical_model_id(CANONICAL_MODEL));

    let deep_without_earlier = without_earlier
        .breakpoints
        .iter()
        .find(|breakpoint| breakpoint.block_index == 1)
        .expect("deep breakpoint without earlier marker");
    let deep_with_earlier = with_earlier
        .breakpoints
        .iter()
        .find(|breakpoint| breakpoint.block_index == 1)
        .expect("deep breakpoint with earlier marker");

    assert_eq!(with_earlier.breakpoints.len(), 2);
    assert_eq!(
        deep_without_earlier.prefix_key,
        deep_with_earlier.prefix_key
    );
}

#[test]
fn deeper_breakpoint_lookback_reuses_shallower_prefix_key() {
    let shallower_index = 4_u64;
    let deeper_index = 7_u64;
    let content = (0..=deeper_index)
        .map(|index| {
            if index == shallower_index || index == deeper_index {
                json!({"type":"text","text":format!("block-{index}"),"cache_control":{"type":"ephemeral"}})
            } else {
                json!({"type":"text","text":format!("block-{index}")})
            }
        })
        .collect::<Vec<_>>();
    let request = json!({
        "model": CANONICAL_MODEL,
        "messages": [{"role":"user","content": content}]
    });

    let analysis = analyze_v3_prompt_cache(&request, canonical_model_id(CANONICAL_MODEL));

    let shallower = analysis
        .breakpoints
        .iter()
        .find(|breakpoint| breakpoint.block_index == shallower_index)
        .expect("shallower breakpoint");
    let deeper = analysis
        .breakpoints
        .iter()
        .find(|breakpoint| breakpoint.block_index == deeper_index)
        .expect("deeper breakpoint");
    let shared = deeper
        .lookback_prefixes
        .iter()
        .find(|prefix| prefix.content_block_index == shallower_index)
        .expect("deeper lookback covers shallower block index");

    assert_eq!(analysis.breakpoints.len(), 2);
    assert_eq!(shared.prefix_key, shallower.prefix_key);
}

#[test]
fn four_breakpoints_map_to_four_breakpoints() {
    let marked_indices = [2_u64, 5, 8, 11];
    let content = (0..12)
        .map(|index| {
            if marked_indices.contains(&index) {
                json!({"type":"text","text":format!("block-{index}"),"cache_control":{"type":"ephemeral"}})
            } else {
                json!({"type":"text","text":format!("block-{index}")})
            }
        })
        .collect::<Vec<_>>();
    let request = json!({
        "model": CANONICAL_MODEL,
        "messages": [{"role":"user","content": content}]
    });

    let analysis = analyze_v3_prompt_cache(&request, canonical_model_id(CANONICAL_MODEL));

    let breakpoint_indices = analysis
        .breakpoints
        .iter()
        .map(|breakpoint| breakpoint.block_index)
        .collect::<Vec<_>>();

    assert_eq!(analysis.breakpoints.len(), 4);
    assert_eq!(breakpoint_indices, marked_indices.to_vec());
}
