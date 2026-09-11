#!/usr/bin/env bash
# Guard against silently dropped integration-test source files.
#
# Crates with `autotests = false` compile one `tests/all.rs` binary. This guard
# follows that file's Rust module/path references recursively and fails when a
# source file below `tests/` is unreachable. Trybuild fixture paths referenced
# by reachable tests are accepted as coverage roots too.
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
    tests_dir = crate_root / "tests"
    all_rs = tests_dir / "all.rs"
    if not tests_dir.is_dir():
        continue
    if not all_rs.is_file():
        failures.append(
            f"ERROR: {manifest.relative_to(root)} sets autotests=false but has no {all_rs.relative_to(root)}"
        )
        continue

    all_sources = {path.resolve() for path in tests_dir.rglob("*.rs")}
    reachable: set[pathlib.Path] = set()
    pending = [all_rs.resolve()]
    while pending:
        source = pending.pop()
        if source in reachable or source not in all_sources or not source.is_file():
            continue
        reachable.add(source)
        pending.extend(referenced_files(source, crate_root) - reachable)

    for source in sorted(all_sources - reachable):
        failures.append(
            f"ERROR: {source.relative_to(root)} is not reachable from {all_rs.relative_to(root)} "
            "(autotests=false would drop it)"
        )

if failures:
    print("\n".join(failures), file=sys.stderr)
    print("\nIntegration-test consolidation guard FAILED.", file=sys.stderr)
    print("Wire every source into tests/all.rs or a reachable child module.", file=sys.stderr)
    raise SystemExit(1)

print("Integration-test consolidation guard OK: all consolidated test sources are reachable.")
PY
