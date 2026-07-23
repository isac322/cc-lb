#!/usr/bin/env python3
"""Validate crate version changes and dedicated cc-lb server release PRs.

Versions under ``crates/`` are managed by release-plz unless they inherit the
root workspace version. A workspace-version bump is allowed only in a dedicated
server release pull request whose title and changed files match the release
contract.
"""

from __future__ import annotations

import argparse
import re
import tomllib
from dataclasses import dataclass
from functools import total_ordering
from pathlib import Path


RELEASE_TITLE_PREFIX = "chore: release cc-lb v"
RELEASE_TITLE = re.compile(
    r"^chore: release cc-lb v"
    r"(?P<version>"
    r"(?:0|[1-9]\d*)\."
    r"(?:0|[1-9]\d*)\."
    r"(?:0|[1-9]\d*)"
    r"(?:-[0-9A-Za-z.-]+)?"
    r")$"
)
ALLOWED_SERVER_RELEASE_FILES = {"Cargo.toml", "Cargo.lock", "CHANGELOG.md"}


@total_ordering
@dataclass(frozen=True)
class SemVer:
    major: int
    minor: int
    patch: int
    prerelease: tuple[str, ...]

    @classmethod
    def parse(cls, value: str) -> "SemVer":
        match = re.fullmatch(
            r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
            r"(?:-([0-9A-Za-z.-]+))?",
            value,
        )
        if not match:
            raise ValueError(f"invalid server SemVer without build metadata: {value}")

        prerelease = tuple(match.group(4).split(".")) if match.group(4) else ()
        if any(not identifier for identifier in prerelease):
            raise ValueError(f"invalid server SemVer prerelease: {value}")
        if any(
            identifier.isdigit() and len(identifier) > 1 and identifier.startswith("0")
            for identifier in prerelease
        ):
            raise ValueError(f"invalid server SemVer numeric prerelease: {value}")

        return cls(
            int(match.group(1)),
            int(match.group(2)),
            int(match.group(3)),
            prerelease,
        )

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SemVer):
            return NotImplemented
        core = (self.major, self.minor, self.patch)
        other_core = (other.major, other.minor, other.patch)
        if core != other_core:
            return core < other_core
        if not self.prerelease:
            return False
        if not other.prerelease:
            return True
        for left, right in zip(self.prerelease, other.prerelease):
            if left == right:
                continue
            left_numeric = left.isdigit()
            right_numeric = right.isdigit()
            if left_numeric and right_numeric:
                return int(left) < int(right)
            if left_numeric != right_numeric:
                return left_numeric
            return left < right
        return len(self.prerelease) < len(other.prerelease)


@dataclass(frozen=True)
class PackageVersion:
    version: str
    inherited: bool


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


def package_versions(root: Path) -> dict[str, PackageVersion]:
    workspace_version = _workspace_version(root)
    versions: dict[str, PackageVersion] = {}
    for manifest in sorted((root / "crates").glob("*/Cargo.toml")):
        package = _load(manifest).get("package", {})
        if not isinstance(package, dict):
            continue
        name = package.get("name")
        raw_version = package.get("version")
        if not isinstance(name, str):
            continue
        inherited = isinstance(raw_version, dict) and raw_version.get("workspace") is True
        version = workspace_version if inherited else raw_version
        if isinstance(version, str):
            versions[name] = PackageVersion(version=version, inherited=inherited)
    return versions


def effective_versions(root: Path) -> dict[str, str]:
    return {name: item.version for name, item in package_versions(root).items()}


def validate_changes(
    base_root: Path,
    head_root: Path,
    pr_title: str,
    changed_files: set[str],
    existing_tags: set[str],
) -> list[str]:
    base_workspace = _workspace_version(base_root)
    head_workspace = _workspace_version(head_root)
    base_packages = package_versions(base_root)
    head_packages = package_versions(head_root)

    package_changes = {
        name
        for name in base_packages.keys() & head_packages.keys()
        if base_packages[name] != head_packages[name]
    }
    release_title_requested = pr_title.startswith(RELEASE_TITLE_PREFIX)
    if (
        not package_changes
        and base_workspace == head_workspace
        and not release_title_requested
    ):
        return []

    title_match = RELEASE_TITLE.fullmatch(pr_title)
    if not title_match:
        if release_title_requested:
            requested_version = pr_title.removeprefix(RELEASE_TITLE_PREFIX)
            try:
                SemVer.parse(requested_version)
            except ValueError as error:
                return [str(error)]
        return [
            "Managed crate versions may change only in a dedicated server release PR "
            "titled 'chore: release cc-lb vX.Y.Z'."
        ]

    errors: list[str] = []
    title_version = title_match.group("version")
    if head_workspace is None or base_workspace is None:
        errors.append("Both base and head must define [workspace.package].version.")
        return errors
    if title_version != head_workspace:
        errors.append(
            f"PR title version {title_version} does not match workspace version {head_workspace}."
        )
    try:
        if SemVer.parse(head_workspace) <= SemVer.parse(base_workspace):
            errors.append(
                f"Server release version {head_workspace} must be greater than {base_workspace}."
            )
    except ValueError as error:
        errors.append(str(error))
    if changed_files != ALLOWED_SERVER_RELEASE_FILES:
        missing = sorted(ALLOWED_SERVER_RELEASE_FILES - changed_files)
        unrelated = sorted(changed_files - ALLOWED_SERVER_RELEASE_FILES)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unrelated:
            details.append("unrelated files " + ", ".join(unrelated))
        errors.append(
            "Server release PR must change exactly Cargo.toml, Cargo.lock, and "
            "CHANGELOG.md: " + "; ".join(details)
        )
    tag = f"cc-lb-v{head_workspace}"
    if tag in existing_tags:
        errors.append(f"Server release tag {tag} already exists.")

    for name in sorted(package_changes):
        base = base_packages[name]
        head = head_packages[name]
        if not base.inherited or not head.inherited:
            errors.append(f"{name} changes a literal package version; release-plz owns it.")
        elif base.version != base_workspace or head.version != head_workspace:
            errors.append(f"{name} does not follow the workspace version transition.")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate managed crate version changes.")
    parser.add_argument("--base", required=True, type=Path)
    parser.add_argument("--head", required=True, type=Path)
    parser.add_argument("--pr-title", default="")
    parser.add_argument("--changed-file", action="append", default=[])
    parser.add_argument("--existing-tag", action="append", default=[])
    args = parser.parse_args()

    errors = validate_changes(
        args.base,
        args.head,
        args.pr_title,
        set(args.changed_file),
        set(args.existing_tag),
    )
    if not errors:
        return 0
    for error in errors:
        print(error)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
