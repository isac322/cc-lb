#!/usr/bin/env python3
"""Guard against manual crate version bumps under crates/.

Versions of crates under crates/ are owned by release-plz: the shared workspace
version (inherited via `version.workspace = true`) plus the independently
released crates that pin a literal `package.version`. A manual bump in a feature
PR desyncs release-plz's baseline and makes its `chore: release` PR conflict
forever, so this check compares the effective version of every crate under
crates/ between two source trees and fails if any of them changed.

Only crates/ is inspected on purpose: top-level members such as fuzz/, plugins/
and tests/ are not managed by release-plz and are free to set their own version.
"""

from __future__ import annotations

import argparse
import tomllib
from pathlib import Path


def _load(manifest: Path) -> dict[str, object]:
    with manifest.open("rb") as handle:
        return tomllib.load(handle)


def _workspace_version(root: Path) -> str | None:
    root_manifest = root / "Cargo.toml"
    if not root_manifest.is_file():
        return None
    workspace = _load(root_manifest).get("workspace", {})
    if not isinstance(workspace, dict):
        return None
    package = workspace.get("package", {})
    if not isinstance(package, dict):
        return None
    version = package.get("version")
    return version if isinstance(version, str) else None


def effective_versions(root: Path) -> dict[str, str]:
    """Map each crates/<name> package to its effective version.

    A crate either pins a literal `package.version` or inherits the workspace
    version via `version.workspace = true`; this resolves that single fallback
    the same way Cargo does, without invoking Cargo.
    """
    workspace_version = _workspace_version(root)
    versions: dict[str, str] = {}
    for manifest in sorted((root / "crates").glob("*/Cargo.toml")):
        package = _load(manifest).get("package", {})
        if not isinstance(package, dict):
            continue
        name = package.get("name")
        if not isinstance(name, str):
            continue
        version = package.get("version")
        if isinstance(version, dict) and version.get("workspace") is True:
            version = workspace_version
        if isinstance(version, str):
            versions[name] = version
    return versions


def main() -> int:
    parser = argparse.ArgumentParser(description="Reject manual crates/ version bumps.")
    parser.add_argument("--base", required=True, type=Path, help="base (target) source tree")
    parser.add_argument("--head", required=True, type=Path, help="head (PR) source tree")
    args = parser.parse_args()

    base = effective_versions(args.base)
    head = effective_versions(args.head)
    changed = [
        (name, base[name], head[name])
        for name in base
        if name in head and base[name] != head[name]
    ]
    if not changed:
        return 0

    print("Manual crate version changes under crates/ are not allowed.")
    print("These versions are owned by release-plz. Keep version bumps out of")
    print("feature PRs: use a conventional-commit PR title, and cut a release by")
    print("merging the `chore: release` PR that release-plz maintains.\n")
    for name, old, new in changed:
        print(f"  {name}: {old} -> {new}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
