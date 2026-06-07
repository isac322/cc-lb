use serde_json::Value;
use std::fs;
use std::path::PathBuf;

use cc_lb_core::lifecycle::cache_prefix_hash;
use cc_lb_storage_api::types::RequestCacheBreakpointSource;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("hash_golden")
}

fn load_fixture_json(name: &str) -> Value {
    let path = fixture_dir().join(format!("{}.json", name));
    let content = fs::read_to_string(&path).expect(&format!("Failed to read fixture {}", name));
    serde_json::from_str(&content).expect(&format!("Invalid JSON in fixture {}", name))
}

fn load_expected_hash(name: &str) -> String {
    let path = fixture_dir().join(format!("{}.expected_hash", name));
    fs::read_to_string(&path)
        .expect(&format!("Failed to read expected hash for {}", name))
        .trim()
        .to_string()
}

fn save_expected_hash(name: &str, hash: &str) {
    let path = fixture_dir().join(format!("{}.expected_hash", name));
    fs::write(&path, format!("{}\n", hash)).expect(&format!("Failed to write hash for {}", name));
}

#[test]
fn golden_base_request_hash_stable() {
    let request = load_fixture_json("base_request");
    let computed_hash = cache_prefix_hash(
        &request,
        RequestCacheBreakpointSource::System,
        "system[0]",
        None,
    );

    // TDD-via-record: if .expected_hash doesn't exist, save it
    let expected_path = fixture_dir().join("base_request.expected_hash");
    if !expected_path.exists() {
        save_expected_hash("base_request", &computed_hash);
        eprintln!(
            "Recorded base_request hash: {} (saved to .expected_hash)",
            computed_hash
        );
    }

    let expected_hash = load_expected_hash("base_request");
    assert_eq!(
        computed_hash, expected_hash,
        "base_request hash changed. Current: {}, Expected: {}",
        computed_hash, expected_hash
    );
    assert_eq!(computed_hash.len(), 64, "Hash should be 64 hex chars (SHA-256)");
}

#[test]
fn golden_json_key_reorder_same_hash() {
    let base = load_fixture_json("base_request");
    let keyswap = load_fixture_json("base_request_keyswap");

    let base_hash = cache_prefix_hash(
        &base,
        RequestCacheBreakpointSource::System,
        "system[0]",
        None,
    );
    let keyswap_hash = cache_prefix_hash(
        &keyswap,
        RequestCacheBreakpointSource::System,
        "system[0]",
        None,
    );

    // TDD-via-record: if .expected_hash doesn't exist, save it
    let keyswap_expected_path = fixture_dir().join("base_request_keyswap.expected_hash");
    if !keyswap_expected_path.exists() {
        save_expected_hash("base_request_keyswap", &keyswap_hash);
        eprintln!(
            "Recorded base_request_keyswap hash: {} (saved to .expected_hash)",
            keyswap_hash
        );
    }

    assert_eq!(
        base_hash, keyswap_hash,
        "Reordered JSON keys should produce same hash (serde canonical ordering)"
    );
}

#[test]
fn golden_whitespace_diff_different_hash() {
    let base = load_fixture_json("base_request");
    let with_space = load_fixture_json("base_request_trailing_space");

    let base_hash = cache_prefix_hash(
        &base,
        RequestCacheBreakpointSource::System,
        "system[0]",
        None,
    );
    let space_hash = cache_prefix_hash(
        &with_space,
        RequestCacheBreakpointSource::System,
        "system[0]",
        None,
    );

    // TDD-via-record: if .expected_hash doesn't exist, save it
    let space_expected_path = fixture_dir().join("base_request_trailing_space.expected_hash");
    if !space_expected_path.exists() {
        save_expected_hash("base_request_trailing_space", &space_hash);
        eprintln!(
            "Recorded base_request_trailing_space hash: {} (saved to .expected_hash)",
            space_hash
        );
    }

    assert_ne!(
        base_hash, space_hash,
        "Trailing space in system text should change hash"
    );
}

#[test]
fn golden_alias_vs_dated_v1() {
    let alias = load_fixture_json("base_request_alias");
    let dated = load_fixture_json("base_request_dated");

    let alias_hash = cache_prefix_hash(
        &alias,
        RequestCacheBreakpointSource::System,
        "system[0]",
        None,
    );
    let dated_hash = cache_prefix_hash(
        &dated,
        RequestCacheBreakpointSource::System,
        "system[0]",
        None,
    );

    eprintln!("v1 alias hash (claude-sonnet-4-5): {}", alias_hash);
    eprintln!(
        "v1 dated hash (claude-sonnet-4-5-20250929): {}",
        dated_hash
    );

    assert_ne!(
        alias_hash, dated_hash,
        "v1 behavior: alias and dated model names produce different hashes. \
         TODO(T20): canonical_model_id will unify these; update base_request_alias.expected_hash \
         to match base_request_dated.expected_hash after canonical resolution lands"
    );

    // Save both for golden-test reference
    let alias_expected_path = fixture_dir().join("base_request_alias.expected_hash");
    if !alias_expected_path.exists() {
        save_expected_hash("base_request_alias", &alias_hash);
        eprintln!("Recorded base_request_alias hash: {}", alias_hash);
    }

    let dated_expected_path = fixture_dir().join("base_request_dated.expected_hash");
    if !dated_expected_path.exists() {
        save_expected_hash("base_request_dated", &dated_hash);
        eprintln!("Recorded base_request_dated hash: {}", dated_hash);
    }
}
