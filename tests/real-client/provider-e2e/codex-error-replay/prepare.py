#!/usr/bin/env python3
"""Prepare a scratch official Codex workspace; never build, fetch, or run tests."""
import argparse
import datetime
import hashlib
import json
import re
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile
import tomllib

REVISION = "4e4767ffa763ec0d0a19e04cf8ff0163a987297b"
PARSER = "codex-rs/codex-api/src/sse/responses.rs"
ERROR_PARSER = "codex-rs/codex-api/src/sse/responses_error.rs"


def dependency_tables(package):
    for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
        yield package.get(kind, {})
    for target in package.get("target", {}).values():
        yield from dependency_tables(target)


def package_key(package):
    return package["name"], package["version"], package.get("source")


def path_dependency_closure(workspace, document):
    inherited = document["workspace"].get("dependencies", {})
    pending = [workspace / "codex-api"]
    packages = {}
    used_inherited = set()
    while pending:
        directory = pending.pop().resolve()
        if directory in packages:
            continue
        directory.relative_to(workspace)
        package = tomllib.loads((directory / "Cargo.toml").read_text())
        packages[directory] = package
        for dependencies in dependency_tables(package):
            for name, spec in dependencies.items():
                relative_to = directory
                if isinstance(spec, dict) and spec.get("workspace"):
                    used_inherited.add(name)
                    original = inherited[name]
                    original = original if isinstance(original, dict) else {"version": original}
                    spec = {**original, **{key: value for key, value in spec.items() if key != "workspace"}}
                    relative_to = workspace
                if isinstance(spec, dict) and "path" in spec:
                    pending.append(relative_to / spec["path"])
    return packages, used_inherited


def locked_dependency_closure(lock, packages):
    by_name = {}
    for package in lock["package"]:
        by_name.setdefault(package["name"], []).append(package)
    pending = []
    for package in packages.values():
        matches = [candidate for candidate in by_name[package["package"]["name"]] if "source" not in candidate]
        if len(matches) != 1:
            raise ValueError("official path package is ambiguous in Cargo.lock")
        pending.append(matches[0])
    selected = {}
    while pending:
        package = pending.pop()
        key = package_key(package)
        if key in selected:
            continue
        selected[key] = package
        for reference in package.get("dependencies", []):
            parts = reference.split()
            matches = by_name[parts[0]]
            if len(parts) > 1 and not parts[1].startswith("("):
                matches = [candidate for candidate in matches if candidate["version"] == parts[1]]
            if "(" in reference:
                source = reference[reference.index("(") + 1:-1]
                matches = [candidate for candidate in matches if candidate.get("source") == source]
            if len(matches) != 1:
                raise ValueError(f"ambiguous official locked dependency: {reference}")
            pending.append(matches[0])
    return selected


def toml_value(value):
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, list):
        return "[" + ", ".join(map(toml_value, value)) + "]"
    if isinstance(value, dict):
        return "{ " + ", ".join(json.dumps(key) + " = " + toml_value(item) for key, item in value.items()) + " }"
    raise TypeError(f"unsupported upstream TOML value: {type(value).__name__}")


def toml_table(header, values):
    return header + "\n" + "".join(json.dumps(key) + " = " + toml_value(value) + "\n" for key, value in values.items()) + "\n"


def minimize_workspace(exported, root):
    workspace = (exported / "codex-rs").resolve()
    manifest_path = workspace / "Cargo.toml"
    lock_path = workspace / "Cargo.lock"
    original_manifest = manifest_path.read_text()
    original_lock = lock_path.read_text()
    document = tomllib.loads(original_manifest)
    lock = tomllib.loads(original_lock)
    if set(lock) - {"version", "package"}:
        raise ValueError("unsupported auxiliary Cargo.lock tables; preserve them explicitly")
    packages, used_inherited = path_dependency_closure(workspace, document)
    locked = locked_dependency_closure(lock, packages)
    reachable_names = {package["name"] for package in locked.values()}
    members = sorted(str(path.relative_to(workspace)) for path in packages)
    workspace_values = {"members": members, "default-members": ["codex-api"]}
    for name, value in document["workspace"].items():
        if name not in ("members", "default-members") and not isinstance(value, dict):
            workspace_values[name] = value
    patches_kept, patches_removed = {}, {}
    sections = []
    for section in re.split(r"(?m)(?=^\[[^\n]+\]\s*$)", original_manifest):
        header = section.split("\n", 1)[0].strip()
        if header == "[workspace]":
            section = toml_table(header, workspace_values)
        elif header == "[workspace.dependencies]":
            section = toml_table(header, {name: spec for name, spec in document["workspace"]["dependencies"].items() if name in used_inherited})
        elif header.startswith("[patch."):
            selector = next(iter(tomllib.loads(header + "\n__probe = true")["patch"]))
            entries = document["patch"][selector]
            retained = {name: spec for name, spec in entries.items() if (spec.get("package", name) if isinstance(spec, dict) else name) in reachable_names}
            patches_kept[selector] = sorted(retained)
            patches_removed[selector] = sorted(set(entries) - set(retained))
            section = toml_table(header, retained) if retained else ""
        sections.append(section)
    narrowed_manifest = "".join(sections)
    tomllib.loads(narrowed_manifest)
    manifest_path.write_text(narrowed_manifest)
    # Keep official package blocks byte-for-byte, including versions, sources,
    # checksums, and dependency references. Remove unreachable lock entries only.
    lock_sections = re.split(r"(?m)(?=^\[\[package\]\]\s*$)", original_lock)
    retained_blocks = [lock_sections[0]]
    for block in lock_sections[1:]:
        package = tomllib.loads(block)["package"][0]
        if package_key(package) in locked:
            retained_blocks.append(block)
    narrowed_lock = "".join(retained_blocks)
    tomllib.loads(narrowed_lock)
    lock_path.write_text(narrowed_lock)
    (root / "upstream-Cargo.toml").write_text(original_manifest)
    (root / "upstream-Cargo.lock").write_text(original_lock)
    return {
        "root": "codex-rs",
        "dependency_scope": "all normal, dev, build, and target-specific path/workspace dependencies, recursively",
        "members": members,
        "removed_members": sorted(set(document["workspace"]["members"]) - set(members)),
        "retained_workspace_dependencies": sorted(used_inherited),
        "removed_workspace_dependencies": sorted(set(document["workspace"]["dependencies"]) - used_inherited),
        "retained_patches": patches_kept,
        "removed_patches": patches_removed,
        "official_package_manifest_sha256": {
            str(path.relative_to(workspace) / "Cargo.toml"): hashlib.sha256((path / "Cargo.toml").read_bytes()).hexdigest()
            for path in sorted(packages)
        },
        "locked_packages_before": len(lock["package"]),
        "locked_packages_retained": len(locked),
        "retained_git_packages": [
            {"name": package["name"], "source": package["source"]}
            for package in locked.values() if package.get("source", "").startswith("git+")
        ],
        "workspace_only_edits": {
            "Cargo.toml": {"before_sha256": hashlib.sha256(original_manifest.encode()).hexdigest(), "after_sha256": hashlib.sha256(narrowed_manifest.encode()).hexdigest()},
            "Cargo.lock": {"before_sha256": hashlib.sha256(original_lock.encode()).hexdigest(), "after_sha256": hashlib.sha256(narrowed_lock.encode()).hexdigest()},
        },
        "lock_policy": "reachable official lock blocks preserved exactly; Cargo may canonicalize the reduced lock offline, so --locked is intentionally omitted",
        "package_source_policy": "no package manifest or source edits except the recorded test-only parser module declaration and adapter",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex-source", type=Path, required=True, help="local official Codex git repository containing the pinned commit")
    parser.add_argument("--output-parent", type=Path, default=Path(tempfile.gettempdir()), help="existing scratch parent outside this repository")
    parser.add_argument("--sse", type=Path, required=True, help="responses-fault.sse emitted by local-error-contract.py")
    parser.add_argument("--expect", choices=("old", "new"), default="new")
    args = parser.parse_args()
    source = args.codex_source.resolve()
    parent = args.output_parent.resolve()
    sse = args.sse.resolve(strict=True)
    repository = Path(__file__).resolve().parents[4]
    if parent.is_relative_to(repository):
        parser.error("--output-parent must be outside cc-lb")
    revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", REVISION + "^{commit}"], text=True).strip()
    if revision != REVISION:
        parser.error("the exact pinned Codex commit is unavailable")
    # git archive reads committed objects, not a possibly modified worktree.
    root = Path(tempfile.mkdtemp(prefix="cc-lb-codex-1165-", dir=parent))
    ledger = root / "cleanup-ledger.json"
    ledger.write_text(json.dumps([{
        "kind": "scratch-directory", "identifier": str(root),
        "delete_command": "rm -rf -- " + shlex.quote(str(root)),
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "state": "retained",
    }], indent=2) + "\n")
    print("Cleanup ledger:", ledger, flush=True)
    exported = root / "codex"
    exported.mkdir()
    with subprocess.Popen(["git", "-C", str(source), "archive", "--format=tar", REVISION], stdout=subprocess.PIPE) as archive:
        with tarfile.open(fileobj=archive.stdout, mode="r|") as tar:
            tar.extractall(exported, filter="data")
        if archive.wait() != 0:
            raise RuntimeError("exporting official Codex source failed")
    pruning = minimize_workspace(exported, root)
    original = (exported / PARSER).read_bytes()
    fixture = Path(__file__).with_name("local_error_contract_replay.rs").read_bytes()
    (exported / PARSER).with_name("local_error_contract_replay.rs").write_bytes(fixture)
    (exported / PARSER).write_bytes(original + b'\n#[cfg(test)]\n#[path = "local_error_contract_replay.rs"]\nmod local_error_contract_replay;\n')
    manifest = {
        "codex_revision": REVISION,
        "source_export": "git archive of exact commit; no worktree source or rewritten parser",
        "parser_sha256_before_test_module": hashlib.sha256(original).hexdigest(),
        "error_parser_sha256": hashlib.sha256((exported / ERROR_PARSER).read_bytes()).hexdigest(),
        "test_adapter_sha256": hashlib.sha256(fixture).hexdigest(),
        "sse": str(sse), "sse_sha256": hashlib.sha256(sse.read_bytes()).hexdigest(),
        "expect": args.expect,
        "workspace_pruning": pruning,
    }
    (root / "source-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    command = ["env", "CC_LB_REPLAY_SSE=" + str(sse), "CC_LB_REPLAY_EXPECT=" + args.expect, "CC_LB_REPLAY_REPORT=" + str(root / "replay-report.json"), "CARGO_TARGET_DIR=" + str(root / "target"), "cargo", "test", "--offline", "--manifest-path", str(exported / "codex-rs/Cargo.toml"), "-p", "codex-api", "--lib", "local_error_contract_replay", "--", "--nocapture"]
    # Run from the exported codex-rs directory so official .cargo config applies.
    print("Prepared official source:", exported)
    print("Official path packages:", len(pruning["members"]), "Retained lock packages:", pruning["locked_packages_retained"])
    print("Dependency provisioning only (if needed; no provider calls):", shlex.join(["cargo", "fetch", "--manifest-path", str(exported / "codex-rs/Cargo.toml")]))
    print("Run from:", exported / "codex-rs")
    print(shlex.join(command))
    print("Preparation only: no dependency fetch, build, or test was executed.")


if __name__ == "__main__":
    main()
