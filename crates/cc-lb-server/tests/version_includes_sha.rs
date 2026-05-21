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
