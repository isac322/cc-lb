#!/usr/bin/env python3
"""Inspect and control immutable cc-lb server releases."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path


SEMVER_PATTERN = re.compile(
    r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-([0-9A-Za-z.-]+))?"
)
SOURCE_PLACEHOLDER = "cc-lb.io/source-revision: __SOURCE_REVISION__"


class ReleaseError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReleaseTarget:
    target_sha: str
    parent_sha: str
    version: str
    parent_version: str
    tag: str
    prerelease: bool
    version_changed: bool


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def parse_server_semver(value: str) -> tuple[int, int, int, tuple[str, ...]]:
    match = SEMVER_PATTERN.fullmatch(value)
    if not match:
        raise ReleaseError(
            f"target version is not server release SemVer without build metadata: {value}"
        )
    prerelease = tuple(match.group(4).split(".")) if match.group(4) else ()
    if any(not identifier for identifier in prerelease):
        raise ReleaseError(f"target version has an invalid prerelease identifier: {value}")
    if any(
        identifier.isdigit() and len(identifier) > 1 and identifier.startswith("0")
        for identifier in prerelease
    ):
        raise ReleaseError(f"target version has a numeric prerelease with leading zero: {value}")
    return int(match.group(1)), int(match.group(2)), int(match.group(3)), prerelease


def workspace_version_at(repo: Path, ref: str) -> str:
    raw = git(repo, "show", f"{ref}:Cargo.toml")
    data = tomllib.loads(raw)
    try:
        value = data["workspace"]["package"]["version"]
    except (KeyError, TypeError) as error:
        raise ReleaseError(f"{ref} has no [workspace.package].version") from error
    if not isinstance(value, str):
        raise ReleaseError(f"{ref} workspace version is not a string")
    return value


def inspect_target(
    repo: Path,
    target_ref: str,
    expected_version: str | None,
) -> ReleaseTarget:
    target_sha = git(repo, "rev-parse", f"{target_ref}^{{commit}}")
    parent_sha = git(repo, "rev-parse", f"{target_sha}^1")
    version = workspace_version_at(repo, target_sha)
    parent_version = workspace_version_at(repo, parent_sha)
    if expected_version is not None and expected_version != version:
        raise ReleaseError(
            f"requested version {expected_version} does not match target version {version}"
        )
    _major, _minor, _patch, prerelease = parse_server_semver(version)
    return ReleaseTarget(
        target_sha=target_sha,
        parent_sha=parent_sha,
        version=version,
        parent_version=parent_version,
        tag=f"cc-lb-v{version}",
        prerelease=bool(prerelease),
        version_changed=version != parent_version,
    )


def decide_release(
    operation: str,
    target_sha: str,
    tag_sha: str | None,
    release_state: str,
    version_changed: bool,
) -> str:
    if operation not in {"auto", "start", "resume"}:
        raise ReleaseError(f"unknown operation: {operation}")
    if release_state not in {"absent", "draft", "published"}:
        raise ReleaseError(f"unknown release state: {release_state}")
    if operation == "auto" and not version_changed:
        return "noop"
    if operation in {"start", "resume"} and not version_changed:
        raise ReleaseError(
            f"manual {operation} target must contain a workspace version change"
        )
    if release_state == "absent":
        if tag_sha is not None:
            raise ReleaseError("tag exists but GitHub Release is absent")
        if operation == "resume":
            raise ReleaseError("cannot resume without a draft Release")
        return "start"
    if tag_sha is None:
        raise ReleaseError(f"{release_state} Release exists without a tag")
    if tag_sha != target_sha:
        raise ReleaseError(
            f"immutable tag points to {tag_sha}, not requested target {target_sha}"
        )
    if release_state == "published":
        return "noop"
    if operation != "resume":
        raise ReleaseError("draft Release exists; explicit resume is required")
    return "resume"


def release_aliases(version: str, published_tags: list[str]) -> tuple[str, ...]:
    major, minor, patch, prerelease = parse_server_semver(version)
    if prerelease:
        return ()
    current = (major, minor, patch)
    published: list[tuple[int, int, int]] = []
    for tag in published_tags:
        if not tag.startswith("cc-lb-v"):
            continue
        value = tag.removeprefix("cc-lb-v")
        try:
            item_major, item_minor, item_patch, item_prerelease = parse_server_semver(value)
        except ReleaseError:
            continue
        if not item_prerelease:
            published.append((item_major, item_minor, item_patch))

    aliases: list[str] = []
    if not any(item[:2] == current[:2] and item > current for item in published):
        aliases.append(f"{major}.{minor}")
    if not any(item[0] == major and item > current for item in published):
        aliases.append(str(major))
    return tuple(aliases)


def stamp_chart(chart_yaml: Path, revision: str) -> None:
    text = chart_yaml.read_text(encoding="utf-8")
    if text.count(SOURCE_PLACEHOLDER) != 1:
        raise ReleaseError("Chart.yaml must contain exactly one source-revision placeholder")
    chart_yaml.write_text(
        text.replace(SOURCE_PLACEHOLDER, f"cc-lb.io/source-revision: {revision}"),
        encoding="utf-8",
    )


def chart_field(text: str, pattern: str, name: str) -> str:
    match = re.search(pattern, text, flags=re.MULTILINE)
    if not match:
        raise ReleaseError(f"Chart.yaml has no {name}")
    return match.group(1)


def verify_chart(chart_yaml: Path, version: str, revision: str) -> None:
    text = chart_yaml.read_text(encoding="utf-8")
    actual_version = chart_field(text, r'^version:\s*["\']?([^"\'\s]+)', "version")
    actual_app = chart_field(text, r'^appVersion:\s*["\']?([^"\'\s]+)', "appVersion")
    actual_revision = chart_field(
        text,
        r'^\s{2}cc-lb\.io/source-revision:\s*["\']?([^"\'\s]+)',
        "source revision",
    )
    expected = (version, version, revision)
    actual = (actual_version, actual_app, actual_revision)
    if actual != expected:
        raise ReleaseError(f"chart metadata mismatch: expected {expected}, got {actual}")


def write_outputs(path: Path, values: dict[str, str | bool]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            rendered = str(value).lower() if isinstance(value, bool) else value
            handle.write(f"{key}={rendered}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect and control cc-lb server releases.")
    commands = parser.add_subparsers(dest="command", required=True)

    inspect = commands.add_parser("inspect")
    inspect.add_argument("--repo", type=Path, default=Path("."))
    inspect.add_argument("--target", required=True)
    inspect.add_argument("--expected-version")
    inspect.add_argument("--github-output", required=True, type=Path)

    decide = commands.add_parser("decide")
    decide.add_argument("--operation", choices=("auto", "start", "resume"), required=True)
    decide.add_argument("--target-sha", required=True)
    decide.add_argument("--tag-sha", default="")
    decide.add_argument(
        "--release-state", choices=("absent", "draft", "published"), required=True
    )
    decide.add_argument("--version-changed", choices=("true", "false"), required=True)
    decide.add_argument("--github-output", required=True, type=Path)

    aliases = commands.add_parser("aliases")
    aliases.add_argument("--version", required=True)
    aliases.add_argument("--published-tag", action="append", default=[])
    aliases.add_argument("--github-output", required=True, type=Path)

    stamp = commands.add_parser("stamp-chart")
    stamp.add_argument("--chart", required=True, type=Path)
    stamp.add_argument("--revision", required=True)

    verify = commands.add_parser("verify-chart")
    verify.add_argument("--chart", required=True, type=Path)
    verify.add_argument("--version", required=True)
    verify.add_argument("--revision", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "inspect":
            target = inspect_target(args.repo, args.target, args.expected_version)
            write_outputs(
                args.github_output,
                {
                    "version": target.version,
                    "tag": target.tag,
                    "target_sha": target.target_sha,
                    "parent_sha": target.parent_sha,
                    "prerelease": target.prerelease,
                    "version_changed": target.version_changed,
                },
            )
        elif args.command == "decide":
            action = decide_release(
                args.operation,
                args.target_sha,
                args.tag_sha or None,
                args.release_state,
                args.version_changed == "true",
            )
            write_outputs(args.github_output, {"action": action})
        elif args.command == "aliases":
            aliases = release_aliases(args.version, args.published_tag)
            write_outputs(args.github_output, {"aliases": " ".join(aliases)})
        elif args.command == "stamp-chart":
            stamp_chart(args.chart, args.revision)
        else:
            verify_chart(args.chart, args.version, args.revision)
    except (ReleaseError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
