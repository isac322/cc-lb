use cc_lb_server::version::compact_version;
use cc_lb_server::version::format_version;

#[test]
fn version_includes_sha() {
    let output = format_version();
    let lines: Vec<_> = output.lines().collect();

    assert_eq!(lines.len(), 5, "output={output}");
    assert_eq!(lines[0], env!("CARGO_PKG_VERSION"), "output={output}");
    assert!(lines[1].starts_with("Commit: "), "output={output}");
    assert!(lines[2].starts_with("Built: "), "output={output}");
    assert!(lines[3].starts_with("Features: "), "output={output}");
    assert!(lines[4].starts_with("Target: "), "output={output}");

    let commit = lines[1].trim_start_matches("Commit: ");
    let commit_is_hex = commit.len() >= 7 && commit.chars().all(|ch| ch.is_ascii_hexdigit());
    assert!(commit == "unknown" || commit_is_hex, "commit={commit}");
    assert!(lines[2].contains("(rustc "), "output={output}");
}

#[test]
fn compact_version_includes_sha() {
    let output = compact_version();
    let mut parts = output.split_whitespace();
    assert_eq!(parts.next(), Some("cc-lb"), "output={output}");
    assert_eq!(
        parts.next(),
        Some(env!("CARGO_PKG_VERSION")),
        "output={output}"
    );
    let sha = output
        .split_once('(')
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .unwrap_or("");
    assert!(!sha.is_empty(), "output={output}");
    assert!(
        sha == "unknown" || (sha.len() >= 7 && sha.chars().all(|ch| ch.is_ascii_hexdigit())),
        "sha={sha}"
    );
}
