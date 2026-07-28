use std::fs;
use std::path::PathBuf;

use serde_json::{Value, json};

pub struct CorpusCase {
    pub name: String,
    pub request: Value,
}

pub fn wide_corpus() -> Vec<CorpusCase> {
    let mut cases = hash_golden_cases();
    cases.extend([
        case(
            "key-reordered-a",
            parse(r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":[{"type":"text","text":"ordered","metadata":{"z":0,"a":1},"cache_control":{"type":"ephemeral"}}]}]}"#),
        ),
        case(
            "key-reordered-b",
            parse(r#"{"messages":[{"content":[{"cache_control":{"type":"ephemeral"},"metadata":{"a":1,"z":0},"text":"ordered","type":"text"}],"role":"user"}],"model":"claude-sonnet-4-5-20250929"}"#),
        ),
        case(
            "synthetic-system-and-message-strings",
            json!({
                "model": "claude-sonnet-4-5-20250929",
                "system": "system string: escaped \"quote\" / slash\nline\t tab \u{0000} 中文 한글 🚀",
                "messages": [
                    {"role": "user", "content": "message string: \u{0008}\u{000c}\r"},
                    {"role": "user", "content": [
                        {"type": "text", "text": "breakpoint", "cache_control": {"type": "ephemeral"}}
                    ]}
                ]
            }),
        ),
        case(
            "tools-with-nested-reorder-and-scalars",
            json!({
                "model": "claude-sonnet-4-5-20250929",
                "tools": [{
                    "name": "lookup",
                    "description": "tool \"description\" 中文",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "u64": {"const": u64::MAX},
                            "i64": {"const": i64::MIN},
                            "float": {"const": 1.25},
                            "null": {"const": null},
                            "bool": {"const": true}
                        },
                        "required": []
                    },
                    "cache_control": {"type": "ephemeral", "ttl": "1h"}
                }]
            }),
        ),
        case(
            "image-cache-control-first",
            parse(r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":[{"cache_control":{"type":"ephemeral"},"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAEC/w=="}}]}]}"#),
        ),
        case(
            "document-cache-control-middle",
            parse(r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":[{"type":"document","cache_control":{"ttl":"1h","type":"ephemeral"},"source":{"data":"JVBERi0xLjQK","media_type":"application/pdf","type":"base64"},"title":"文書"}]}]}"#),
        ),
        case(
            "tool-use-cache-control-last",
            parse(r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"nested":{"b":2,"a":1},"int":-7,"float":-0.5,"none":null,"ok":false},"cache_control":{"type":"ephemeral"}}]}]}"#),
        ),
        case(
            "tool-result",
            json!({
                "model": "claude-sonnet-4-5-20250929",
                "messages": [{"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "toolu_1",
                    "content": [
                        {"type": "text", "text": "result\n中文"},
                        {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": "/9j/"}}
                    ],
                    "is_error": false,
                    "cache_control": {"type": "ephemeral"}
                }]}]
            }),
        ),
        case(
            "empty-text-and-prefix-only-blocks",
            json!({
                "model": "claude-sonnet-4-5-20250929",
                "tools": [null, "not-an-object", 17],
                "system": "",
                "messages": [
                    {"role": "user", "content": ""},
                    {"role": "user", "content": [
                        null,
                        {"type": "text", "text": ""},
                        {"type": "thinking", "thinking": "not cacheable"},
                        {"type": "unknown", "cache_control": {"type": "ephemeral"}}
                    ]}
                ]
            }),
        ),
        case(
            "four-breakpoint-maximum",
            json!({
                "model": "claude-sonnet-4-5-20250929",
                "messages": [{"role": "user", "content": [
                    {"type": "text", "text": "one", "cache_control": {"type": "ephemeral"}},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AA=="}, "cache_control": {"type": "ephemeral"}},
                    {"type": "document", "source": {"type": "text", "media_type": "text/plain", "data": "doc"}, "cache_control": {"type": "ephemeral", "ttl": "1h"}},
                    {"type": "tool_result", "tool_use_id": "toolu_4", "content": "done", "cache_control": {"type": "ephemeral"}}
                ]}]
            }),
        ),
    ]);
    cases
}

fn hash_golden_cases() -> Vec<CorpusCase> {
    let mut paths = fs::read_dir(hash_golden_dir())
        .expect("hash_golden fixture directory is readable")
        .map(|entry| entry.expect("hash_golden fixture entry is readable").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let name = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("hash_golden fixture has a UTF-8 stem");
            let content = fs::read_to_string(&path).expect("hash_golden fixture is readable");
            case(
                format!("hash-golden-{name}"),
                serde_json::from_str(&content).expect("hash_golden fixture is valid JSON"),
            )
        })
        .collect()
}

fn hash_golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("hash_golden")
}

fn case(name: impl Into<String>, request: Value) -> CorpusCase {
    CorpusCase {
        name: name.into(),
        request,
    }
}

fn parse(input: &str) -> Value {
    serde_json::from_str(input).expect("inline corpus fixture is valid JSON")
}
