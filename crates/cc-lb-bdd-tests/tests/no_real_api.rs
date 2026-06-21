// Plan §13 R11 layer (a): scan the BDD harness sources for any
// `api.anthropic.com` string literal at test time. Catches accidental
// hardcoded upstream URLs that slip past code review. Layer (b) — the
// `LoopbackOnlyClient` runtime guard — lands when the fake-anthropic
// helper is wired into `BddCtx`. Layer (c) is the CI workflow grep.

use std::path::Path;

const FORBIDDEN: &str = "api.anthropic.com";

#[test]
fn bdd_harness_does_not_reference_real_anthropic_api() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut violations: Vec<String> = Vec::new();

    for entry in walk_rs(crate_dir) {
        let display = entry
            .strip_prefix(crate_dir)
            .unwrap_or(&entry)
            .display()
            .to_string();
        if display == "tests/no_real_api.rs" {
            continue;
        }
        let body = std::fs::read_to_string(&entry).unwrap_or_default();
        for (idx, line) in body.lines().enumerate() {
            if line.contains(FORBIDDEN) {
                violations.push(format!("{display}:{}", idx + 1));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "BDD harness must never reference {FORBIDDEN}; found at:\n  {}",
        violations.join("\n  "),
    );
}

fn walk_rs(root: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    visit(root, &mut out);
    out.sort();
    out
}

fn visit(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name == "target" || name == "__captures__" || name.starts_with('.') {
                continue;
            }
            visit(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}
