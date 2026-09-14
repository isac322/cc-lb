#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
import json
import pathlib
import re
import shlex
import subprocess
import sys
from collections.abc import Iterable

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_WORKFLOW = ROOT / ".github/workflows/ci.yml"
DEFAULT_SHARDED_WORKFLOW = ROOT / ".github/workflows/coverage-sharded.yml"
RUN_PATTERN = re.compile(
    r"^(?P<indent>\s*)run:\s*(?P<style>[>|][+-]?)?\s*(?P<body>.*)$"
)
SHELL_TERMINATORS = {"&&", "||", ";", "|"}
COVERAGE_EXCLUSIONS = (
    "cc-lb-loadgen",
    "cc-lb-stress-suite",
    "tests-multi-replica",
)


def workflow_commands(path: pathlib.Path) -> list[tuple[int, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    commands: list[tuple[int, str]] = []
    index = 0
    while index < len(lines):
        match = RUN_PATTERN.match(lines[index])
        if match is None:
            index += 1
            continue

        line_number = index + 1
        style = match.group("style")
        if style is None:
            commands.append((line_number, match.group("body")))
            index += 1
            continue

        base_indent = len(match.group("indent"))
        block: list[str] = []
        index += 1
        while index < len(lines):
            line = lines[index]
            if not line.strip():
                block.append("")
                index += 1
                continue
            indent = len(line) - len(line.lstrip())
            if indent <= base_indent:
                break
            block.append(line.strip())
            index += 1
        command = "\n".join(block)
        if style.startswith(">"):
            command = command.replace("\\\n", "").replace("\n", " ")
        commands.append((line_number, command))
    return commands


def shell_tokens(path: pathlib.Path, marker: str) -> Iterable[tuple[int, list[str]]]:
    for line_number, command in workflow_commands(path):
        # POSIX shells remove backslash-newline continuations before
        # tokenizing.  Keep real newlines as statement boundaries so a
        # command following the coverage invocation cannot become another
        # package token.
        for statement in command.replace("\\\n", "").splitlines():
            if marker not in statement:
                continue
            try:
                yield line_number, shlex.split(statement, comments=True)
            except ValueError as error:
                raise ValueError(
                    f"{path}:{line_number}: cannot parse run command: {error}"
                ) from error


def coverage_slice_packages(path: pathlib.Path) -> list[str]:
    packages: list[str] = []
    invocation_count = 0
    for line_number, tokens in shell_tokens(path, "scripts/run-coverage-slice.sh"):
        script_indexes = [
            index
            for index, token in enumerate(tokens)
            if token == "scripts/run-coverage-slice.sh"
        ]
        for script_index in script_indexes:
            invocation_count += 1
            invocation_packages: list[str] = []
            for token in tokens[script_index + 1 :]:
                if token in SHELL_TERMINATORS:
                    break
                if token.startswith("-"):
                    raise ValueError(
                        f"{path}:{line_number}: coverage slice accepts package names only, "
                        f"got {token!r}"
                    )
                invocation_packages.append(token)
            if not invocation_packages:
                raise ValueError(f"{path}:{line_number}: coverage slice has no packages")
            packages.extend(invocation_packages)

    if invocation_count == 0:
        raise ValueError(f"{path}: no scripts/run-coverage-slice.sh invocations found")
    return packages


def sharded_exclusions(path: pathlib.Path) -> list[str]:
    exclusions: list[str] = []
    archive_count = 0
    for line_number, tokens in shell_tokens(path, "nextest-archive"):
        is_archive = any(
            tokens[index : index + 3] == ["cargo", "llvm-cov", "nextest-archive"]
            for index in range(max(0, len(tokens) - 2))
        )
        if not is_archive:
            continue

        archive_count += 1
        index = 0
        while index < len(tokens):
            token = tokens[index]
            if token == "--exclude":
                if index + 1 >= len(tokens):
                    raise ValueError(f"{path}:{line_number}: --exclude has no package")
                exclusions.append(tokens[index + 1])
                index += 2
                continue
            if token.startswith("--exclude="):
                package = token.partition("=")[2]
                if not package:
                    raise ValueError(f"{path}:{line_number}: --exclude has no package")
                exclusions.append(package)
            index += 1

    if archive_count != 1:
        raise ValueError(
            f"{path}: expected exactly one cargo llvm-cov nextest-archive command, found {archive_count}"
        )
    return exclusions


def workspace_packages() -> set[str]:
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)

    metadata = json.loads(result.stdout)
    packages_by_id = {package["id"]: package["name"] for package in metadata["packages"]}
    return {packages_by_id[package_id] for package_id in metadata["workspace_members"]}


def print_list(title: str, values: Iterable[str]) -> None:
    values = list(values)
    if not values:
        return
    print(title, file=sys.stderr)
    for value in values:
        print(f"- {value}", file=sys.stderr)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check that CI coverage slices partition every non-excluded workspace package."
    )
    parser.add_argument("--workflow", type=pathlib.Path, default=DEFAULT_WORKFLOW)
    parser.add_argument(
        "--sharded-workflow", type=pathlib.Path, default=DEFAULT_SHARDED_WORKFLOW
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workflow = args.workflow if args.workflow.is_absolute() else ROOT / args.workflow
    sharded_workflow = (
        args.sharded_workflow
        if args.sharded_workflow.is_absolute()
        else ROOT / args.sharded_workflow
    )

    try:
        sliced = coverage_slice_packages(workflow)
        sharded = sharded_exclusions(sharded_workflow)
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    workspace = workspace_packages()
    excluded_counts = collections.Counter(COVERAGE_EXCLUSIONS)
    excluded = set(excluded_counts)
    slice_counts = collections.Counter(sliced)
    sharded_counts = collections.Counter(sharded)

    duplicates = sorted(name for name, count in slice_counts.items() if count > 1)
    duplicate_excluded = sorted(
        name for name, count in excluded_counts.items() if count > 1
    )
    duplicate_sharded = sorted(name for name, count in sharded_counts.items() if count > 1)
    unknown_slices = sorted(set(sliced) - workspace)
    unknown_exclusions = sorted(excluded - workspace)
    included_exclusions = sorted(set(sliced) & excluded)
    missing = sorted(workspace - set(sliced) - excluded)
    sharded_only = sorted(set(sharded) - excluded)
    active_only = sorted(excluded - set(sharded))

    print_list("coverage packages appear in more than one slice:", duplicates)
    print_list("active coverage exclusions are duplicated:", duplicate_excluded)
    print_list("sharded coverage exclusions are duplicated:", duplicate_sharded)
    print_list("coverage slices name non-workspace packages:", unknown_slices)
    print_list("coverage exclusions name non-workspace packages:", unknown_exclusions)
    print_list("coverage packages are both included and excluded:", included_exclusions)
    print_list("workspace packages are missing from coverage slices:", missing)
    print_list("sharded coverage has extra exclusions:", sharded_only)
    print_list("active coverage exclusions are missing from sharded coverage:", active_only)

    if any(
        (
            duplicates,
            duplicate_excluded,
            duplicate_sharded,
            unknown_slices,
            unknown_exclusions,
            included_exclusions,
            missing,
            sharded_only,
            active_only,
        )
    ):
        return 1

    print(
        "Coverage slice guard OK: "
        f"{len(slice_counts)} packages covered, {len(excluded)} explicitly excluded."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
