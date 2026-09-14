#!/usr/bin/env bash
# Guard against silently dropped integration-test source files.
#
# Crates with `autotests = false` compile only the explicit `[[test]].path`
# targets from Cargo.toml. This guard follows every target's Rust module/path
# references recursively and fails when a sibling test source is unreachable.
# Trybuild fixture paths referenced by reachable tests are accepted too.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

python3 - <<'PY'
from __future__ import annotations

import glob
import pathlib
import re
import sys
import tomllib

root = pathlib.Path.cwd()
failures: list[str] = []


def resolve_module(current: pathlib.Path, name: str) -> list[pathlib.Path]:
    return [
        current.parent / f"{name}.rs",
        current.parent / name / "mod.rs",
        current.parent / current.stem / f"{name}.rs",
        current.parent / current.stem / name / "mod.rs",
    ]

def referenced_files(source: pathlib.Path, crate_root: pathlib.Path) -> set[pathlib.Path]:
    text = source.read_text(errors="replace")
    found: set[pathlib.Path] = set()

    for value in re.findall(r'#\s*\[\s*path\s*=\s*"([^"]+)"\s*\]', text):
        found.add((source.parent / value).resolve())

    without_path_attrs = re.sub(r'#\s*\[\s*path\s*=\s*"[^"]+"\s*\]\s*', "", text)
    for name in re.findall(r'(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;', without_path_attrs):
        found.update(candidate.resolve() for candidate in resolve_module(source, name))

    for value in re.findall(r'"([^"\n]*?(?:\.rs|\*\.rs))"', text):
        candidates = [source.parent / value, crate_root / value, root / value]
        for candidate in candidates:
            if "*" in str(candidate):
                found.update(pathlib.Path(path).resolve() for path in glob.glob(str(candidate)))
            else:
                found.add(candidate.resolve())
    return found


def consolidated_sources(
    crate_root: pathlib.Path, test_roots: list[pathlib.Path]
) -> set[pathlib.Path]:
    sources: set[pathlib.Path] = set()
    source_trees = {crate_root / "tests"}
    source_trees.update(test_root.parent for test_root in test_roots)
    for source_tree in source_trees:
        if not source_tree.is_dir():
            continue
        for source in source_tree.rglob("*.rs"):
            if source_tree == crate_root:
                relative = source.relative_to(crate_root)
                if relative.parts[0] in {"benches", "examples", "src", "target"}:
                    continue
                if relative == pathlib.Path("build.rs"):
                    continue
            sources.add(source.resolve())
    return sources


manifests = sorted({*root.glob("crates/**/Cargo.toml"), *root.glob("tests/**/Cargo.toml")})
for manifest in manifests:
    try:
        cargo_toml = tomllib.loads(manifest.read_text())
    except tomllib.TOMLDecodeError as error:
        failures.append(f"ERROR: cannot parse {manifest.relative_to(root)}: {error}")
        continue
    if cargo_toml.get("package", {}).get("autotests", True) is not False:
        continue

    crate_root = manifest.parent
    test_roots = [
        (crate_root / target["path"]).resolve()
        for target in cargo_toml.get("test", [])
        if isinstance(target, dict) and isinstance(target.get("path"), str)
    ]
    if not test_roots:
        tests_dir = crate_root / "tests"
        if tests_dir.is_dir() and any(tests_dir.rglob("*.rs")):
            failures.append(
                f"ERROR: {manifest.relative_to(root)} sets autotests=false and has test sources "
                "but no explicit [[test]].path"
            )
        continue

    missing_roots = [test_root for test_root in test_roots if not test_root.is_file()]
    for test_root in missing_roots:
        failures.append(
            f"ERROR: {manifest.relative_to(root)} declares missing test path "
            f"{test_root.relative_to(root)}"
        )
    if missing_roots:
        continue

    all_sources = consolidated_sources(crate_root, test_roots)
    reachable: set[pathlib.Path] = set()
    pending = list(test_roots)
    while pending:
        source = pending.pop()
        if source in reachable or source not in all_sources or not source.is_file():
            continue
        reachable.add(source)
        pending.extend(referenced_files(source, crate_root) - reachable)

    roots = ", ".join(str(path.relative_to(root)) for path in test_roots)
    for source in sorted(all_sources - reachable):
        failures.append(
            f"ERROR: {source.relative_to(root)} is not reachable from declared test root(s) "
            f"{roots} (autotests=false would drop it)"
        )

if failures:
    print("\n".join(failures), file=sys.stderr)
    print("\nIntegration-test consolidation guard FAILED.", file=sys.stderr)
    print("Wire every source into a declared [[test]].path root or a reachable child module.", file=sys.stderr)
    raise SystemExit(1)

print("Integration-test consolidation guard OK: all consolidated test sources are reachable.")
PY
