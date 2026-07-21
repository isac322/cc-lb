# cc-lb Server Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a dedicated Cargo workspace-version release PR publish one immutable cc-lb server tag, a verified GHCR image, a verified OCI Helm chart, and then a GitHub Release.

**Architecture:** Keep release-plz responsible only for crates.io packages. Add a server-release state helper and a serialized `release-server.yml` orchestrator that tags the reviewed version-bump commit, creates a draft Release, invokes ref-aware idempotent Docker and Helm publishers, and publishes the draft only after both artifacts verify. The root Cargo workspace version remains the single server version source.

**Tech Stack:** Python 3 standard library, Git, Cargo metadata, GitHub Actions, GitHub CLI, Docker Buildx, GHCR, Helm 3 OCI, actionlint.

## Global Constraints

- The server version comes only from root `Cargo.toml` `[workspace.package].version`.
- Server source tags use `cc-lb-vX.Y.Z`; Docker and Helm versions use plain `X.Y.Z`.
- Server release versions may use prerelease identifiers but must not use SemVer build metadata (`+...`) because Docker tags cannot preserve the same string.
- Public crates remain owned by release-plz and crates.io trusted publishing.
- Normal feature PRs must not change managed crate versions.
- A server release PR may change only `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md` and must be titled `chore: release cc-lb vX.Y.Z`.
- Stable Docker releases always publish immutable `X.Y.Z`; moving `X.Y` and `X` aliases update only when the candidate is the newest published stable server version for that prefix. Prereleases publish only the exact prerelease tag.
- Never publish a `latest` Docker tag.
- `release-server.yml` is the only normal publication orchestrator. Publisher workflows keep `workflow_call` and explicit `workflow_dispatch`, but remove `release.published` triggers.
- Do not publish the GitHub Release until Docker and Helm artifacts pass post-push verification.
- A public tag, public Release, or published chart version is immutable. Never move or overwrite it.
- Automatic start requires a commit-local workspace-version change against the candidate commit's first parent. Tag existence is the idempotent no-op/resume guard.
- A missed run is recovered by explicit `workflow_dispatch` with the reviewed release commit SHA and version.
- Use job-level least privilege and `GITHUB_TOKEN`; add no long-lived registry secret.
- Keep the existing self-hosted Docker build, BuildKit networking, and sccache secret handling unchanged.

## File Responsibility Map

- `scripts/check-crate-versions.py`: distinguish forbidden feature-PR version changes from a valid dedicated server release PR.
- `scripts/test_check_crate_versions.py`: deterministic unit tests for release-title, changed-file, SemVer, inheritance, and existing-tag rules.
- `scripts/server_release.py`: inspect an immutable Git target, decide start/resume/no-op/error state, and stamp/verify chart source metadata.
- `scripts/test_server_release.py`: temporary-Git-repository tests for commit-local version detection and pure release-state tests.
- `.github/workflows/ci.yml`: pass PR metadata and changed files to the version validator; run its unit tests.
- `.github/workflows/cd.yml`: ref-aware, version-keyed, idempotent Docker publisher with source-revision verification.
- `deploy/helm/cc-lb/Chart.yaml`: declare the source-revision annotation key with a development placeholder.
- `.github/workflows/publish-chart.yml`: ref-aware, version-keyed, idempotent Helm publisher with pull-back verification.
- `.github/workflows/release-server.yml`: serialized server release state machine and publication orchestrator.
- `.github/workflows/release-plz.yml`: crates.io release workflow only; no server outputs or image/chart jobs.
- `release-plz.toml`: crates.io packages only; no `cc-lb-server` anchor.
- `docs/runbook/server-release.md`: operator procedure for release PRs, state inspection, resume, and source-defect recovery.

---

### Task 1: Validate Dedicated Server Release Pull Requests

**Files:**
- Modify: `scripts/check-crate-versions.py:1-91`
- Create: `scripts/test_check_crate_versions.py`
- Modify: `.github/workflows/ci.yml:46-65`

**Interfaces:**
- Consumes: base and head source trees, PR title, changed paths, and fetched Git tags.
- Produces: `validate_changes(base, head, pr_title, changed_files, existing_tags) -> list[str]`; an empty list means valid.

- [ ] **Step 1: Write failing validator tests**

Create `scripts/test_check_crate_versions.py` with a temporary workspace fixture and these exact observable cases:

```python
from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("check-crate-versions.py")
SPEC = importlib.util.spec_from_file_location("check_crate_versions", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def write_workspace(root: Path, workspace_version: str, *, wire_version: str = "0.8.0") -> None:
    (root / "crates/cc-lb-server").mkdir(parents=True)
    (root / "crates/cc-lb-internal").mkdir(parents=True)
    (root / "crates/cc-lb-plugin-wire").mkdir(parents=True)
    (root / "Cargo.toml").write_text(
        "[workspace]\n"
        "members = [\"crates/*\"]\n\n"
        "[workspace.package]\n"
        f"version = \"{workspace_version}\"\n",
        encoding="utf-8",
    )
    for name in ("cc-lb-server", "cc-lb-internal"):
        (root / f"crates/{name}/Cargo.toml").write_text(
            f"[package]\nname = \"{name}\"\nversion.workspace = true\n",
            encoding="utf-8",
        )
    (root / "crates/cc-lb-plugin-wire/Cargo.toml").write_text(
        "[package]\n"
        "name = \"cc-lb-plugin-wire\"\n"
        f"version = \"{wire_version}\"\n",
        encoding="utf-8",
    )
    (root / "Cargo.lock").write_text("# fixture\n", encoding="utf-8")
    (root / "CHANGELOG.md").write_text("# Changelog\n", encoding="utf-8")


class ValidateChangesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.base = root / "base"
        self.head = root / "head"
        write_workspace(self.base, "0.1.0")
        write_workspace(self.head, "0.1.0")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def validate(
        self,
        title: str,
        files: set[str],
        tags: set[str] | None = None,
    ) -> list[str]:
        return MODULE.validate_changes(self.base, self.head, title, files, tags or set())

    def test_ordinary_pr_without_version_change_passes(self) -> None:
        self.assertEqual(self.validate("feat: add routing", {"crates/cc-lb-server/src/main.rs"}), [])

    def test_ordinary_pr_workspace_bump_fails(self) -> None:
        write_workspace(self.head, "0.1.1")
        errors = self.validate("feat: add routing", {"Cargo.toml", "Cargo.lock"})
        self.assertTrue(any("dedicated server release" in error for error in errors))

    def test_valid_server_release_pr_passes(self) -> None:
        write_workspace(self.head, "0.1.1")
        errors = self.validate(
            "chore: release cc-lb v0.1.1",
            {"Cargo.toml", "Cargo.lock", "CHANGELOG.md"},
        )
        self.assertEqual(errors, [])

    def test_release_title_must_match_workspace_version(self) -> None:
        write_workspace(self.head, "0.1.1")
        errors = self.validate(
            "chore: release cc-lb v0.2.0",
            {"Cargo.toml", "Cargo.lock", "CHANGELOG.md"},
        )
        self.assertTrue(any("title version" in error for error in errors))

    def test_release_version_must_increase(self) -> None:
        write_workspace(self.head, "0.1.0-rc.1")
        errors = self.validate(
            "chore: release cc-lb v0.1.0-rc.1",
            {"Cargo.toml", "Cargo.lock", "CHANGELOG.md"},
        )
        self.assertTrue(any("greater than" in error for error in errors))

    def test_release_pr_rejects_unrelated_files(self) -> None:
        write_workspace(self.head, "0.1.1")
        errors = self.validate(
            "chore: release cc-lb v0.1.1",
            {"Cargo.toml", "Cargo.lock", "CHANGELOG.md", "crates/cc-lb-server/src/main.rs"},
        )
        self.assertTrue(any("unrelated files" in error for error in errors))

    def test_release_pr_rejects_literal_public_crate_bump(self) -> None:
        write_workspace(self.head, "0.1.1", wire_version="0.9.0")
        errors = self.validate(
            "chore: release cc-lb v0.1.1",
            {"Cargo.toml", "Cargo.lock", "CHANGELOG.md", "crates/cc-lb-plugin-wire/Cargo.toml"},
        )
        self.assertTrue(any("literal package version" in error for error in errors))

    def test_release_pr_rejects_existing_server_tag(self) -> None:
        write_workspace(self.head, "0.1.1")
        errors = self.validate(
            "chore: release cc-lb v0.1.1",
            {"Cargo.toml", "Cargo.lock", "CHANGELOG.md"},
            {"cc-lb-v0.1.1"},
        )
        self.assertTrue(any("already exists" in error for error in errors))

    def test_release_pr_requires_all_release_files(self) -> None:
        write_workspace(self.head, "0.1.1")
        errors = self.validate(
            "chore: release cc-lb v0.1.1",
            {"Cargo.toml", "Cargo.lock"},
        )
        self.assertTrue(any("must change exactly" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests and confirm the new contract fails**

Run:

```bash
python3 scripts/test_check_crate_versions.py -v
```

Expected: FAIL because `validate_changes` does not exist.

- [ ] **Step 3: Implement SemVer and release-PR validation**

Refactor `scripts/check-crate-versions.py` without removing the existing CLI behavior. Add:

```python
import re
from dataclasses import dataclass
from functools import total_ordering

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
        return cls(
            int(match.group(1)),
            int(match.group(2)),
            int(match.group(3)),
            tuple(match.group(4).split(".")) if match.group(4) else (),
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
    if not package_changes and base_workspace == head_workspace:
        return []

    title_match = RELEASE_TITLE.fullmatch(pr_title)
    if not title_match:
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
            details.append("unrelated " + ", ".join(unrelated))
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
```

Replace `main()` with the complete CLI adapter:

```python
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
```

- [ ] **Step 4: Run validator tests**

Run:

```bash
python3 scripts/test_check_crate_versions.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Wire PR metadata into CI**

Update `.github/workflows/ci.yml` so `guard-crate-versions` still exempts `release-plz-*` branches, but validates dedicated server release PRs:

```yaml
      - name: Validate managed crate version changes
        env:
          BASE_SHA: ${{ github.event.pull_request.base.sha }}
          PR_TITLE: ${{ github.event.pull_request.title }}
        run: |
          set -euo pipefail
          git worktree add --detach "$RUNNER_TEMP/base" "$BASE_SHA"

          args=(
            --base "$RUNNER_TEMP/base"
            --head .
            --pr-title "$PR_TITLE"
          )
          while IFS= read -r path; do
            args+=(--changed-file "$path")
          done < <(git diff --name-only "$BASE_SHA"...HEAD)
          while IFS= read -r tag; do
            args+=(--existing-tag "$tag")
          done < <(git tag --list 'cc-lb-v*')

          python3 scripts/check-crate-versions.py "${args[@]}"
          git worktree remove --force "$RUNNER_TEMP/base"

      - name: Test managed version validator
        run: python3 scripts/test_check_crate_versions.py -v
```

Preserve `fetch-depth: 0` so the validator sees the base commit and tags.

- [ ] **Step 6: Verify CI YAML and script behavior**

Run:

```bash
python3 scripts/test_check_crate_versions.py -v
actionlint .github/workflows/ci.yml
```

Expected: all unit tests PASS; actionlint emits no diagnostics.

- [ ] **Step 7: Commit**

```bash
git add scripts/check-crate-versions.py scripts/test_check_crate_versions.py .github/workflows/ci.yml
git commit -m "ci: validate server release version PRs"
```

---

### Task 2: Implement the Server Release State Helper

**Files:**
- Create: `scripts/server_release.py`
- Create: `scripts/test_server_release.py`

**Interfaces:**
- Produces: `inspect_target(repo, target_ref, expected_version) -> ReleaseTarget`, including `version_changed: bool`.
- Produces: `decide_release(operation, target_sha, tag_sha, release_state, version_changed) -> str` returning `start`, `resume`, or `noop`; invalid states raise `ReleaseError`.
- Produces: `release_aliases(version, published_tags) -> tuple[str, ...]`, updating moving aliases only when the candidate is the newest published stable version for that prefix.
- Produces CLI subcommands: `inspect`, `decide`, `aliases`, `stamp-chart`, `verify-chart`.
- Publisher and orchestrator workflows consume GitHub-output keys `version`, `tag`, `target_sha`, `parent_sha`, `prerelease`, `version_changed`, `action`, and `aliases`.

- [ ] **Step 1: Write failing release-state tests**

Create `scripts/test_server_release.py`. Use a real temporary Git repository for target inspection and direct function calls for state decisions:

```python
from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import server_release


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def write_manifest(repo: Path, version: str, dependency: str = "1") -> None:
    (repo / "Cargo.toml").write_text(
        "[workspace]\n"
        "members = [\"crates/cc-lb-server\"]\n\n"
        "[workspace.package]\n"
        f"version = \"{version}\"\n\n"
        "[workspace.dependencies]\n"
        f"anyhow = \"{dependency}\"\n",
        encoding="utf-8",
    )
    server = repo / "crates/cc-lb-server"
    server.mkdir(parents=True, exist_ok=True)
    (server / "Cargo.toml").write_text(
        "[package]\nname = \"cc-lb-server\"\nversion.workspace = true\n",
        encoding="utf-8",
    )


class InspectTargetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp.name)
        git(self.repo, "init")
        git(self.repo, "config", "user.email", "test@example.com")
        git(self.repo, "config", "user.name", "Test")
        write_manifest(self.repo, "0.1.0")
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-m", "initial")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def commit(self, message: str) -> str:
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-m", message)
        return git(self.repo, "rev-parse", "HEAD")

    def test_detects_commit_local_workspace_version_change(self) -> None:
        write_manifest(self.repo, "0.1.1")
        target = self.commit("chore: release cc-lb v0.1.1")
        result = server_release.inspect_target(self.repo, target, "0.1.1")
        self.assertEqual(result.version, "0.1.1")
        self.assertEqual(result.parent_version, "0.1.0")
        self.assertEqual(result.tag, "cc-lb-v0.1.1")
        self.assertFalse(result.prerelease)

    def test_dependency_only_manifest_change_is_not_a_release(self) -> None:
        write_manifest(self.repo, "0.1.0", dependency="2")
        target = self.commit("chore: bump anyhow")
        result = server_release.inspect_target(self.repo, target, None)
        self.assertFalse(result.version_changed)
        self.assertEqual(
            server_release.decide_release("auto", target, None, "absent", False),
            "noop",
        )
        with self.assertRaisesRegex(server_release.ReleaseError, "workspace version change"):
            server_release.decide_release("start", target, None, "absent", False)

    def test_prerelease_is_reported(self) -> None:
        write_manifest(self.repo, "0.2.0-rc.1")
        target = self.commit("chore: release cc-lb v0.2.0-rc.1")
        result = server_release.inspect_target(self.repo, target, None)
        self.assertTrue(result.prerelease)


class DecideReleaseTests(unittest.TestCase):
    def test_absent_state_starts(self) -> None:
        self.assertEqual(
            server_release.decide_release("start", "abc", None, "absent", True),
            "start",
        )

    def test_published_matching_tag_is_noop(self) -> None:
        self.assertEqual(
            server_release.decide_release("start", "abc", "abc", "published", True),
            "noop",
        )

    def test_draft_requires_explicit_resume(self) -> None:
        with self.assertRaisesRegex(server_release.ReleaseError, "explicit resume"):
            server_release.decide_release("start", "abc", "abc", "draft", True)
        self.assertEqual(
            server_release.decide_release("resume", "abc", "abc", "draft", True),
            "resume",
        )

    def test_inconsistent_or_moved_tag_fails(self) -> None:
        with self.assertRaises(server_release.ReleaseError):
            server_release.decide_release("resume", "abc", "def", "draft", True)
        with self.assertRaises(server_release.ReleaseError):
            server_release.decide_release("start", "abc", "abc", "absent", True)


class ReleaseAliasTests(unittest.TestCase):
    def test_old_patch_resume_does_not_regress_aliases(self) -> None:
        self.assertEqual(
            server_release.release_aliases("1.2.3", ["cc-lb-v1.2.4"]),
            (),
        )

    def test_new_patch_updates_minor_but_not_major_past_newer_minor(self) -> None:
        self.assertEqual(
            server_release.release_aliases("1.2.5", ["cc-lb-v1.3.0"]),
            ("1.2",),
        )

    def test_new_major_updates_both_aliases(self) -> None:
        self.assertEqual(
            server_release.release_aliases("2.0.0", ["cc-lb-v1.9.9"]),
            ("2.0", "2"),
        )

    def test_prerelease_never_updates_moving_aliases(self) -> None:
        self.assertEqual(
            server_release.release_aliases("2.0.0-rc.1", ["cc-lb-v1.9.9"]),
            (),
        )


class ChartMetadataTests(unittest.TestCase):
    def test_stamp_and_verify_chart_revision(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            chart = Path(temp) / "Chart.yaml"
            chart.write_text(
                "apiVersion: v2\nname: cc-lb\nversion: 0.1.1\n"
                "appVersion: \"0.1.1\"\nannotations:\n"
                "  cc-lb.io/source-revision: __SOURCE_REVISION__\n",
                encoding="utf-8",
            )
            server_release.stamp_chart(chart, "deadbeef")
            server_release.verify_chart(chart, "0.1.1", "deadbeef")


if __name__ == "__main__":
    unittest.main()
```

Add `sys.path.insert(0, str(Path(__file__).parent))` before `import server_release` so the direct script invocation imports the sibling module.

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
python3 scripts/test_server_release.py -v
```

Expected: FAIL because `scripts/server_release.py` does not exist.

- [ ] **Step 3: Implement target inspection and release-state decisions**

Create `scripts/server_release.py` with these public definitions and CLI behavior:

```python
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path


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
    if not re.fullmatch(
        r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
        r"(?:-[0-9A-Za-z.-]+)?",
        version,
    ):
        raise ReleaseError(
            f"target version is not server release SemVer without build metadata: {version}"
        )
    return ReleaseTarget(
        target_sha=target_sha,
        parent_sha=parent_sha,
        version=version,
        parent_version=parent_version,
        tag=f"cc-lb-v{version}",
        prerelease="-" in version,
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
    if operation == "start" and not version_changed:
        raise ReleaseError("manual start target must contain a workspace version change")
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
    if "-" in version:
        return ()
    current = tuple(int(part) for part in version.split("."))
    published: list[tuple[int, int, int]] = []
    for tag in published_tags:
        if not tag.startswith("cc-lb-v"):
            continue
        value = tag.removeprefix("cc-lb-v")
        if "-" in value:
            continue
        if re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", value):
            published.append(tuple(int(part) for part in value.split(".")))

    major, minor, _patch = current
    aliases: list[str] = []
    if not any(item[:2] == current[:2] and item > current for item in published):
        aliases.append(f"{major}.{minor}")
    if not any(item[0] == major and item > current for item in published):
        aliases.append(str(major))
    return tuple(aliases)
```

Implement chart helpers using an exact placeholder rather than a general YAML rewriter:

```python
SOURCE_PLACEHOLDER = "cc-lb.io/source-revision: __SOURCE_REVISION__"


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
```

Add the complete CLI adapter:

```python
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
    decide.add_argument("--release-state", choices=("absent", "draft", "published"), required=True)
    decide.add_argument("--version-changed", choices=("true", "false"), required=True)
    decide.add_argument("--github-output", required=True, type=Path)

    stamp = commands.add_parser("stamp-chart")
    stamp.add_argument("--chart", required=True, type=Path)
    stamp.add_argument("--revision", required=True)

    aliases = commands.add_parser("aliases")
    aliases.add_argument("--version", required=True)
    aliases.add_argument("--published-tag", action="append", default=[])
    aliases.add_argument("--github-output", required=True, type=Path)

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
```

- [ ] **Step 4: Run state-helper tests**

Run:

```bash
python3 scripts/test_server_release.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Verify CLI output contracts**

Run against the current checkout:

```bash
tmp_output="$(mktemp)"
python3 scripts/server_release.py inspect \
  --repo . \
  --target HEAD \
  --github-output "$tmp_output"
command cat "$tmp_output"
```

Expected: output contains `version=0.1.0`, `tag=cc-lb-v0.1.0`, a full `target_sha`, a `parent_sha`, `prerelease=false`, and a `version_changed` boolean.

- [ ] **Step 6: Commit**

```bash
git add scripts/server_release.py scripts/test_server_release.py
git commit -m "ci: add server release state helper"
```

---

### Task 3: Make the Docker Publisher Ref-Aware and Idempotent

**Files:**
- Modify: `.github/workflows/cd.yml:1-125`

**Interfaces:**
- Consumes workflow inputs `version: string` and `source_ref: string` for both `workflow_call` and `workflow_dispatch`.
- Publishes `ghcr.io/isac322/cc-lb` from the exact checked-out ref.
- Accepts an existing exact version only when its `org.opencontainers.image.revision` label equals the checked-out commit SHA.

- [ ] **Step 1: Replace event inputs and concurrency key**

Remove `release: types: [published]`. Define identical required inputs for reusable and manual invocation:

```yaml
on:
  workflow_call:
    inputs:
      version:
        description: 'Semver to publish without a leading v'
        required: true
        type: string
      source_ref:
        description: 'Immutable cc-lb source tag to build'
        required: true
        type: string
  workflow_dispatch:
    inputs:
      version:
        description: 'Semver to publish without a leading v'
        required: true
        type: string
      source_ref:
        description: 'Immutable cc-lb source tag to build'
        required: true
        type: string

concurrency:
  group: publish-docker-${{ inputs.version }}
  cancel-in-progress: false
```

Delete all `github.event.release` conditionals and the `if:` guard that accepts any `v*` GitHub Release.

- [ ] **Step 2: Check out and expose the immutable source**

Replace the checkout and timestamp steps with:

```yaml
      - uses: actions/checkout@v7
        with:
          ref: ${{ inputs.source_ref }}
          fetch-depth: 0

      - name: Resolve source revision
        id: source
        run: |
          set -euo pipefail
          echo "sha=$(git rev-parse HEAD)" >> "$GITHUB_OUTPUT"
          echo "SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)" >> "$GITHUB_ENV"

      - name: Validate version
        env:
          VERSION: ${{ inputs.version }}
          SOURCE_REF: ${{ inputs.source_ref }}
        run: |
          set -euo pipefail
          if ! printf '%s' "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'; then
            echo "::error::'$VERSION' must be SemVer without build metadata"
            exit 1
          fi
          if [ "$SOURCE_REF" != "cc-lb-v$VERSION" ]; then
            echo "::error::source_ref '$SOURCE_REF' must equal cc-lb-v$VERSION"
            exit 1
          fi
```

Remove the old `Determine version` step. All later references use `${{ inputs.version }}`.

- [ ] **Step 3: Detect an existing exact image without hiding registry failures**

After GHCR login and before metadata/build, add:

```yaml
      - name: Inspect existing exact image
        id: existing
        env:
          IMAGE: ghcr.io/isac322/cc-lb
          VERSION: ${{ inputs.version }}
          SOURCE_SHA: ${{ steps.source.outputs.sha }}
        run: |
          set -euo pipefail
          error_file="$RUNNER_TEMP/image-inspect-error"
          if docker buildx imagetools inspect "$IMAGE:$VERSION" > /dev/null 2>"$error_file"; then
            docker pull "$IMAGE:$VERSION"
            revision=$(docker image inspect \
              --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
              "$IMAGE:$VERSION")
            if [ "$revision" != "$SOURCE_SHA" ]; then
              echo "::error::existing $IMAGE:$VERSION points to revision '$revision', expected '$SOURCE_SHA'"
              exit 1
            fi
            echo "already_published=true" >> "$GITHUB_OUTPUT"
          elif grep -qiE 'not found|manifest unknown' "$error_file"; then
            echo "already_published=false" >> "$GITHUB_OUTPUT"
          else
            command cat "$error_file" >&2
            echo "::error::could not determine whether $IMAGE:$VERSION exists"
            exit 1
          fi
```

This step fails closed on authentication, network, and registry errors.

- [ ] **Step 4: Build only when the exact version is absent**

Keep `docker/metadata-action@v6`, but use `${{ inputs.version }}` and add the immutable revision label. Gate Buildx setup and build/push with `steps.existing.outputs.already_published != 'true'`:

```yaml
      - name: Derive tags
        id: meta
        uses: docker/metadata-action@v6
        with:
          images: ghcr.io/isac322/cc-lb
          flavor: latest=false
          tags: |
            type=semver,pattern={{version}},value=${{ inputs.version }}
          labels: |
            org.opencontainers.image.revision=${{ steps.source.outputs.sha }}

      - uses: docker/setup-buildx-action@v4
        if: steps.existing.outputs.already_published != 'true'
        with:
          driver-opts: network=host
          buildkitd-flags: --allow-insecure-entitlement network.host

      - name: Build and push
        if: steps.existing.outputs.already_published != 'true'
        uses: docker/build-push-action@v7
        with:
          context: .
          platforms: linux/amd64,linux/arm64,linux/arm/v7,linux/ppc64le
          push: true
          provenance: false
          network: host
          allow: network.host
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          build-args: |
            GIT_SHA=${{ steps.source.outputs.sha }}
            SOURCE_DATE_EPOCH=${{ env.SOURCE_DATE_EPOCH }}
            SCCACHE_BUCKET=sccache
            SCCACHE_ENDPOINT=http://garage.arc-systems.svc.cluster.local:3900
            SCCACHE_REGION=garage
            SCCACHE_S3_USE_SSL=false
          secret-envs: |
            AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID
            AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
```

Keep the existing login and BuildKit secret behavior.

- [ ] **Step 5: Reconcile stable aliases and verify every published tag**

Publish the immutable exact tag first. Compute moving aliases from already published stable `cc-lb-v*` Releases so resuming an older draft can never move `:X` or `:X.Y` backward:

```yaml
      - name: Select moving aliases
        id: aliases
        env:
          GH_TOKEN: ${{ github.token }}
          VERSION: ${{ inputs.version }}
        run: |
          set -euo pipefail
          args=(aliases --version "$VERSION" --github-output "$GITHUB_OUTPUT")
          while IFS= read -r tag; do
            args+=(--published-tag "$tag")
          done < <(
            gh api --paginate "repos/$GITHUB_REPOSITORY/releases?per_page=100" \
              --jq '.[] | select(.draft == false and .prerelease == false) | .tag_name'
          )
          python3 scripts/server_release.py "${args[@]}"

      - name: Reconcile moving aliases
        if: steps.aliases.outputs.aliases != ''
        env:
          ALIASES: ${{ steps.aliases.outputs.aliases }}
          IMAGE: ghcr.io/isac322/cc-lb
          VERSION: ${{ inputs.version }}
        run: |
          set -euo pipefail
          args=()
          for alias in $ALIASES; do
            args+=(--tag "$IMAGE:$alias")
          done
          docker buildx imagetools create "${args[@]}" "$IMAGE:$VERSION"

      - name: Verify published image tags
        env:
          ALIASES: ${{ steps.aliases.outputs.aliases }}
          IMAGE: ghcr.io/isac322/cc-lb
          VERSION: ${{ inputs.version }}
          SOURCE_SHA: ${{ steps.source.outputs.sha }}
        run: |
          set -euo pipefail
          tags=("$VERSION")
          for alias in $ALIASES; do
            tags+=("$alias")
          done
          for tag in "${tags[@]}"; do
            docker pull "$IMAGE:$tag"
            revision=$(docker image inspect \
              --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
              "$IMAGE:$tag")
            if [ "$revision" != "$SOURCE_SHA" ]; then
              echo "::error::$IMAGE:$tag revision '$revision' does not match '$SOURCE_SHA'"
              exit 1
            fi
          done
          output=$(docker run --rm "$IMAGE:$VERSION" --version)
          if ! printf '%s\n' "$output" | grep -Fq "$VERSION"; then
            echo "::error::binary version output '$output' does not contain '$VERSION'"
            exit 1
          fi
```

- [ ] **Step 6: Validate workflow syntax**

Run:

```bash
actionlint .github/workflows/cd.yml
```

Expected: no diagnostics.

- [ ] **Step 7: Commit**

```bash
git add .github/workflows/cd.yml
git commit -m "ci: make docker release publishing ref-aware"
```

---

### Task 4: Make the Helm Publisher Ref-Aware and Idempotent

**Files:**
- Modify: `deploy/helm/cc-lb/Chart.yaml:1-22`
- Modify: `.github/workflows/publish-chart.yml:1-81`
- Test: `scripts/test_server_release.py`

**Interfaces:**
- Consumes workflow inputs `version: string` and `source_ref: string`.
- Publishes `oci://ghcr.io/isac322/charts/cc-lb:X` from the immutable source tag.
- Accepts an existing exact chart only when `version`, `appVersion`, and `cc-lb.io/source-revision` match.

- [ ] **Step 1: Add the chart source-revision placeholder**

Append to `deploy/helm/cc-lb/Chart.yaml`:

```yaml
annotations:
  cc-lb.io/source-revision: __SOURCE_REVISION__
```

Add these negative assertions to `ChartMetadataTests`:

```python
            with self.assertRaisesRegex(server_release.ReleaseError, "metadata mismatch"):
                server_release.verify_chart(chart, "0.1.1", "other-revision")
            chart.write_text(
                chart.read_text(encoding="utf-8").replace(
                    'appVersion: "0.1.1"', 'appVersion: "0.1.2"'
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(server_release.ReleaseError, "metadata mismatch"):
                server_release.verify_chart(chart, "0.1.1", "deadbeef")
```

- [ ] **Step 2: Run the expanded metadata tests**

Run:

```bash
python3 scripts/test_server_release.py -v
```

Expected: PASS after Task 2's helper implementation; the new negative assertions also pass.

- [ ] **Step 3: Replace chart workflow triggers and checkout**

Remove `release: types: [published]` and define:

```yaml
on:
  workflow_call:
    inputs:
      version:
        description: 'Semver to publish without a leading v'
        required: true
        type: string
      source_ref:
        description: 'Immutable cc-lb source tag to package'
        required: true
        type: string
  workflow_dispatch:
    inputs:
      version:
        description: 'Semver to publish without a leading v'
        required: true
        type: string
      source_ref:
        description: 'Immutable cc-lb source tag to package'
        required: true
        type: string

concurrency:
  group: publish-chart-${{ inputs.version }}
  cancel-in-progress: false
```

Use:

```yaml
      - uses: actions/checkout@v7
        with:
          ref: ${{ inputs.source_ref }}
          fetch-depth: 0

      - name: Resolve source revision
        id: source
        run: echo "sha=$(git rev-parse HEAD)" >> "$GITHUB_OUTPUT"
```

Add exact input validation and remove the old release-event version derivation:

```yaml
      - name: Validate version and source ref
        env:
          SOURCE_REF: ${{ inputs.source_ref }}
          VERSION: ${{ inputs.version }}
        run: |
          set -euo pipefail
          if ! printf '%s' "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'; then
            echo "::error::'$VERSION' must be SemVer without build metadata"
            exit 1
          fi
          if [ "$SOURCE_REF" != "cc-lb-v$VERSION" ]; then
            echo "::error::source_ref '$SOURCE_REF' must equal cc-lb-v$VERSION"
            exit 1
          fi
```

- [ ] **Step 4: Detect and verify an existing chart**

After Helm setup and GHCR login, add:

```yaml
      - name: Inspect existing chart
        id: existing
        env:
          VERSION: ${{ inputs.version }}
          SOURCE_SHA: ${{ steps.source.outputs.sha }}
        run: |
          set -euo pipefail
          mkdir -p "$RUNNER_TEMP/existing-chart"
          error_file="$RUNNER_TEMP/chart-pull-error"
          if helm pull oci://ghcr.io/isac322/charts/cc-lb \
              --version "$VERSION" \
              --destination "$RUNNER_TEMP/existing-chart" \
              2>"$error_file"; then
            tar -xzf "$RUNNER_TEMP/existing-chart/cc-lb-$VERSION.tgz" \
              -C "$RUNNER_TEMP/existing-chart"
            python3 scripts/server_release.py verify-chart \
              --chart "$RUNNER_TEMP/existing-chart/cc-lb/Chart.yaml" \
              --version "$VERSION" \
              --revision "$SOURCE_SHA"
            echo "already_published=true" >> "$GITHUB_OUTPUT"
          elif grep -qiE 'not found|manifest unknown' "$error_file"; then
            echo "already_published=false" >> "$GITHUB_OUTPUT"
          else
            command cat "$error_file" >&2
            echo "::error::could not determine whether chart $VERSION exists"
            exit 1
          fi
```

- [ ] **Step 5: Stamp, lint, package, and push only when absent**

Use a temporary chart copy so the checkout remains unchanged:

```yaml
      - name: Prepare chart
        if: steps.existing.outputs.already_published != 'true'
        env:
          VERSION: ${{ inputs.version }}
          SOURCE_SHA: ${{ steps.source.outputs.sha }}
        run: |
          set -euo pipefail
          cp -R "$CHART" "$RUNNER_TEMP/cc-lb-chart"
          python3 scripts/server_release.py stamp-chart \
            --chart "$RUNNER_TEMP/cc-lb-chart/Chart.yaml" \
            --revision "$SOURCE_SHA"
          helm lint "$RUNNER_TEMP/cc-lb-chart" --set secrets.masterKey="$LINT_MASTER_KEY"
          helm package "$RUNNER_TEMP/cc-lb-chart" \
            --version "$VERSION" \
            --app-version "$VERSION" \
            --destination "$RUNNER_TEMP"

      - name: Push chart
        if: steps.existing.outputs.already_published != 'true'
        env:
          VERSION: ${{ inputs.version }}
        run: helm push "$RUNNER_TEMP/cc-lb-$VERSION.tgz" oci://ghcr.io/isac322/charts
```

- [ ] **Step 6: Pull back and verify every successful invocation**

Add a final step that always pulls the exact chart into a fresh directory and calls `verify-chart` with the source SHA. This runs for both a newly pushed chart and a matching pre-existing chart:

```yaml
      - name: Verify published chart
        env:
          VERSION: ${{ inputs.version }}
          SOURCE_SHA: ${{ steps.source.outputs.sha }}
        run: |
          set -euo pipefail
          verify_dir="$RUNNER_TEMP/verify-chart"
          mkdir -p "$verify_dir"
          helm pull oci://ghcr.io/isac322/charts/cc-lb \
            --version "$VERSION" \
            --destination "$verify_dir"
          tar -xzf "$verify_dir/cc-lb-$VERSION.tgz" -C "$verify_dir"
          python3 scripts/server_release.py verify-chart \
            --chart "$verify_dir/cc-lb/Chart.yaml" \
            --version "$VERSION" \
            --revision "$SOURCE_SHA"
```

- [ ] **Step 7: Validate Helm and workflow syntax locally**

Run:


```bash
tmp_chart="$(mktemp -d)"
command cp -R deploy/helm/cc-lb "$tmp_chart/cc-lb"
python3 scripts/server_release.py stamp-chart \
  --chart "$tmp_chart/cc-lb/Chart.yaml" \
  --revision 0000000000000000000000000000000000000000
helm lint "$tmp_chart/cc-lb" --set secrets.masterKey=ci-lint-placeholder
helm package "$tmp_chart/cc-lb" \
  --version 0.1.1-test.1 \
  --app-version 0.1.1-test.1 \
  --destination "$tmp_chart"
mkdir "$tmp_chart/unpacked"
tar -xzf "$tmp_chart/cc-lb-0.1.1-test.1.tgz" -C "$tmp_chart/unpacked"
python3 scripts/server_release.py verify-chart \
  --chart "$tmp_chart/unpacked/cc-lb/Chart.yaml" \
  --version 0.1.1-test.1 \
  --revision 0000000000000000000000000000000000000000
actionlint .github/workflows/publish-chart.yml
```

Expected: Helm lint succeeds, metadata verification succeeds, and actionlint emits no diagnostics.

- [ ] **Step 8: Commit**

```bash
git add deploy/helm/cc-lb/Chart.yaml .github/workflows/publish-chart.yml scripts/test_server_release.py
git commit -m "ci: verify helm release provenance"
```

---

### Task 5: Add the Serialized Server Release Orchestrator

**Files:**
- Create: `.github/workflows/release-server.yml`
- Test: `scripts/test_server_release.py`

**Interfaces:**
- Automatic trigger: `master` push affecting root `Cargo.toml`.
- Manual inputs: `operation` (`start` or `resume`), `version`, and `target_sha`.
- Calls `.github/workflows/cd.yml` then `.github/workflows/publish-chart.yml` with `version` and `source_ref`.
- Publishes the draft Release only after both reusable workflows pass.

- [ ] **Step 1: Add missing pure state tests before writing the workflow**

Extend `scripts/test_server_release.py` with:

```python
def test_draft_resume_rejects_moved_tag(self) -> None:
    with self.assertRaisesRegex(server_release.ReleaseError, "immutable tag"):
        server_release.decide_release(
            "resume", "release-sha", "other-sha", "draft", True
        )


def test_start_rejects_tag_without_release(self) -> None:
    with self.assertRaisesRegex(server_release.ReleaseError, "tag exists"):
        server_release.decide_release(
            "start", "release-sha", "release-sha", "absent", True
        )
```

Run `python3 scripts/test_server_release.py -v`; expected PASS because Task 2 already implements these invariants.

- [ ] **Step 2: Create workflow triggers, permissions, and concurrency**

Create `.github/workflows/release-server.yml` beginning with:

```yaml
name: release-server

on:
  push:
    branches: [master]
    paths:
      - 'Cargo.toml'
  workflow_dispatch:
    inputs:
      operation:
        description: 'Start a missed release or resume an existing draft'
        required: true
        type: choice
        options: [start, resume]
      version:
        description: 'Cargo server SemVer without a tag prefix'
        required: true
        type: string
      target_sha:
        description: 'Reviewed release PR merge commit or existing release tag commit'
        required: true
        type: string

permissions: {}

concurrency:
  group: release-server
  cancel-in-progress: false
```

- [ ] **Step 3: Implement target inspection and GitHub state collection**

Add a `prepare` job on `ubuntu-latest` with `contents: write`. Check out full history and determine automatic/manual inputs:

```yaml
  prepare:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    outputs:
      action: ${{ steps.decision.outputs.action }}
      version: ${{ steps.target.outputs.version }}
      tag: ${{ steps.target.outputs.tag }}
      target_sha: ${{ steps.target.outputs.target_sha }}
      prerelease: ${{ steps.target.outputs.prerelease }}
    steps:
      - uses: actions/checkout@v7
        with:
          fetch-depth: 0

      - name: Select release target
        id: selected
        env:
          EVENT_NAME: ${{ github.event_name }}
          INPUT_OPERATION: ${{ inputs.operation }}
          INPUT_VERSION: ${{ inputs.version }}
          INPUT_TARGET_SHA: ${{ inputs.target_sha }}
        run: |
          set -euo pipefail
          if [ "$EVENT_NAME" = "push" ]; then
            echo "operation=auto" >> "$GITHUB_OUTPUT"
            echo "target_sha=$GITHUB_SHA" >> "$GITHUB_OUTPUT"
            echo "expected_version=" >> "$GITHUB_OUTPUT"
          else
            echo "operation=$INPUT_OPERATION" >> "$GITHUB_OUTPUT"
            echo "target_sha=$INPUT_TARGET_SHA" >> "$GITHUB_OUTPUT"
            echo "expected_version=$INPUT_VERSION" >> "$GITHUB_OUTPUT"
          fi

      - name: Verify target is on master
        env:
          TARGET_SHA: ${{ steps.selected.outputs.target_sha }}
        run: |
          set -euo pipefail
          git fetch origin master --tags
          git merge-base --is-ancestor "$TARGET_SHA" origin/master

      - name: Inspect release target
        id: target
        env:
          EXPECTED_VERSION: ${{ steps.selected.outputs.expected_version }}
          TARGET_SHA: ${{ steps.selected.outputs.target_sha }}
        run: |
          set -euo pipefail
          args=(
            inspect
            --repo .
            --target "$TARGET_SHA"
            --github-output "$GITHUB_OUTPUT"
          )
          if [ -n "$EXPECTED_VERSION" ]; then
            args+=(--expected-version "$EXPECTED_VERSION")
          fi
          python3 scripts/server_release.py "${args[@]}"
```

Collect tag and Release state without converting registry/API failures into absence:

```yaml
      - name: Inspect tag and Release state
        id: remote
        env:
          GH_TOKEN: ${{ github.token }}
          TAG: ${{ steps.target.outputs.tag }}
        run: |
          set -euo pipefail
          tag_error="$RUNNER_TEMP/tag-fetch-error"
          if git fetch origin "refs/tags/$TAG:refs/tags/$TAG" 2>"$tag_error"; then
            tag_sha=$(git rev-parse "refs/tags/$TAG^{commit}")
          elif grep -qiE "couldn't find remote ref|not our ref" "$tag_error"; then
            tag_sha=""
          else
            command cat "$tag_error" >&2
            exit 1
          fi
          echo "tag_sha=$tag_sha" >> "$GITHUB_OUTPUT"

          error_file="$RUNNER_TEMP/release-api-error"
          if gh api "repos/$GITHUB_REPOSITORY/releases/tags/$TAG" \
              > "$RUNNER_TEMP/release.json" 2>"$error_file"; then
            if jq -e '.draft == true' "$RUNNER_TEMP/release.json" > /dev/null; then
              echo "release_state=draft" >> "$GITHUB_OUTPUT"
            else
              echo "release_state=published" >> "$GITHUB_OUTPUT"
            fi
          elif grep -q 'HTTP 404' "$error_file"; then
            echo "release_state=absent" >> "$GITHUB_OUTPUT"
          else
            command cat "$error_file" >&2
            exit 1
          fi

      - name: Decide release action
        id: decision
        env:
          OPERATION: ${{ steps.selected.outputs.operation }}
          RELEASE_STATE: ${{ steps.remote.outputs.release_state }}
          TAG_SHA: ${{ steps.remote.outputs.tag_sha }}
          TARGET_SHA: ${{ steps.target.outputs.target_sha }}
          VERSION_CHANGED: ${{ steps.target.outputs.version_changed }}
        run: |
          python3 scripts/server_release.py decide \
            --operation "$OPERATION" \
            --release-state "$RELEASE_STATE" \
            --tag-sha "$TAG_SHA" \
            --target-sha "$TARGET_SHA" \
            --version-changed "$VERSION_CHANGED" \
            --github-output "$GITHUB_OUTPUT"
```


- [ ] **Step 4: Create the immutable tag and draft Release for `start`**

Add:

```yaml
      - name: Create source tag
        if: steps.decision.outputs.action == 'start'
        env:
          TAG: ${{ steps.target.outputs.tag }}
          TARGET_SHA: ${{ steps.target.outputs.target_sha }}
          VERSION: ${{ steps.target.outputs.version }}
        run: |
          set -euo pipefail
          git config user.name github-actions[bot]
          git config user.email 41898282+github-actions[bot]@users.noreply.github.com
          git tag -a "$TAG" "$TARGET_SHA" -m "cc-lb $VERSION"
          git push origin "refs/tags/$TAG"

      - name: Create draft GitHub Release
        if: steps.decision.outputs.action == 'start'
        env:
          GH_TOKEN: ${{ github.token }}
          PRERELEASE: ${{ steps.target.outputs.prerelease }}
          TAG: ${{ steps.target.outputs.tag }}
          VERSION: ${{ steps.target.outputs.version }}
        run: |
          set -euo pipefail
          notes=$(cat <<EOF
          ## Artifacts

          - Docker: \`ghcr.io/isac322/cc-lb:$VERSION\`
          - Helm: \`oci://ghcr.io/isac322/charts/cc-lb --version $VERSION\`
          EOF
          )
          args=(
            "$TAG"
            --verify-tag
            --draft
            --generate-notes
            --title "cc-lb $VERSION"
            --notes "$notes"
          )
          if [ "$PRERELEASE" = "true" ]; then
            args+=(--prerelease)
          fi
          gh release create "${args[@]}"
```

A failure after tag creation but before draft creation is an inconsistent state and must fail closed on the next run; document its operator repair path in Task 7.

- [ ] **Step 5: Call publishers in safe order**

Add reusable jobs after `prepare`:

```yaml
  publish-docker:
    needs: prepare
    if: needs.prepare.outputs.action == 'start' || needs.prepare.outputs.action == 'resume'
    uses: ./.github/workflows/cd.yml
    permissions:
      contents: read
      packages: write
    with:
      version: ${{ needs.prepare.outputs.version }}
      source_ref: ${{ needs.prepare.outputs.tag }}
    secrets: inherit

  publish-chart:
    needs: [prepare, publish-docker]
    if: needs.prepare.outputs.action == 'start' || needs.prepare.outputs.action == 'resume'
    uses: ./.github/workflows/publish-chart.yml
    permissions:
      contents: read
      packages: write
    with:
      version: ${{ needs.prepare.outputs.version }}
      source_ref: ${{ needs.prepare.outputs.tag }}
    secrets: inherit
```

Docker runs first. If it fails, no chart is published. A matching existing Docker image is accepted during explicit resume.

- [ ] **Step 6: Publish the draft only after both artifact jobs pass**

Add:

```yaml
  publish-release:
    needs: [prepare, publish-docker, publish-chart]
    if: needs.prepare.outputs.action == 'start' || needs.prepare.outputs.action == 'resume'
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - name: Publish GitHub Release
        env:
          GH_TOKEN: ${{ github.token }}
          TAG: ${{ needs.prepare.outputs.tag }}
        run: gh release edit "$TAG" --draft=false
```

No publisher workflow has an `on: release` trigger, so finalizing the draft has one owner and no second publication entry point.

- [ ] **Step 7: Validate workflow syntax and helper contracts**

Run:

```bash
python3 scripts/test_server_release.py -v
actionlint .github/workflows/release-server.yml .github/workflows/cd.yml .github/workflows/publish-chart.yml
```

Expected: tests PASS; actionlint emits no diagnostics.

- [ ] **Step 8: Commit**

```bash
git add .github/workflows/release-server.yml scripts/test_server_release.py
git commit -m "ci: orchestrate cc-lb server releases"
```

---

### Task 6: Remove Server Publishing from release-plz

**Files:**
- Modify: `release-plz.toml:1-50`
- Modify: `.github/workflows/release-plz.yml:74-143`

**Interfaces:**
- release-plz continues to publish the five configured crates.io packages.
- No release-plz output or job invokes Docker or Helm publication.

- [ ] **Step 1: Remove the server package block**

Delete from `release-plz.toml`:

```toml
[[package]]
name = "cc-lb-server"
release = true
publish = false
semver_check = false
git_tag_name = "v{{ version }}"
```

Rewrite the policy comment at the top so it lists only the five crates.io packages and states that server releases are owned by `.github/workflows/release-server.yml`.

- [ ] **Step 2: Remove server outputs and publisher jobs from release-plz**

In `.github/workflows/release-plz.yml`:

- Remove `release.outputs.cc_lb_server_version`.
- Remove the `Extract cc-lb-server release` step.
- Remove jobs `publish-docker` and `publish-chart`.
- Update the workflow header comment so step 2 describes only package tags, GitHub Releases, and crates.io publishing for managed public packages.
- Preserve preflight and `release-pr` behavior unchanged.

The `release` job should end immediately after `Run release-plz release` and retain `contents: write`, `pull-requests: write`, and `id-token: write` because public package release creation and trusted publishing still require them.

- [ ] **Step 3: Parse configuration and lint the workflow**

Run:

```bash
python3 - <<'PY'
import tomllib
from pathlib import Path
config = tomllib.loads(Path('release-plz.toml').read_text())
names = [package['name'] for package in config['package']]
assert 'cc-lb-server' not in names
assert names == [
    'cc-lb-plugin-wire',
    'cc-lb-pdk-wasmtime-macros',
    'cc-lb-pdk-wasmtime',
    'cc-lb-plugin-conformance',
    'cc-lb-runtime-wasmtime',
]
PY
actionlint .github/workflows/release-plz.yml
```

Expected: Python exits 0; actionlint emits no diagnostics. Do not run release-plz locally; repository policy prohibits local release-plz release commands.

- [ ] **Step 4: Commit**

```bash
git add release-plz.toml .github/workflows/release-plz.yml
git commit -m "ci: separate server releases from release-plz"
```

---

### Task 7: Document Operations and Run the Full Release Gate

**Files:**
- Create: `docs/runbook/server-release.md`

**Interfaces:**
- Gives operators exact release PR, inspection, resume, and source-defect procedures.
- Does not create a public Release or push to GHCR during implementation-branch verification.

- [ ] **Step 1: Write the operator runbook**

Create `docs/runbook/server-release.md` with these sections and commands:

```markdown
# cc-lb server release

The root Cargo workspace version is the server release version. Public crates use release-plz separately.

## Cut a release

1. Create a branch containing only `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md` changes.
2. Set `[workspace.package].version` to the chosen SemVer.
3. Refresh `Cargo.lock` with `cargo metadata --no-deps --format-version 1 > /dev/null`.
4. Move the release notes from `Unreleased` to the chosen version and date.
5. Open the PR as `chore: release cc-lb vX.Y.Z`.
6. Merge only after all required checks pass.

The merge commit becomes immutable tag `cc-lb-vX.Y.Z`. The release workflow publishes Docker first, Helm second, and the GitHub Release last.

## Inspect a release

Use the Actions run named `release-server`, the GitHub Release tag `cc-lb-vX.Y.Z`, image `ghcr.io/isac322/cc-lb:X.Y.Z`, and chart `oci://ghcr.io/isac322/charts/cc-lb --version X.Y.Z`.

## Resume an infrastructure failure

Do not rerun a failed job blindly. Fix the workflow, runner, credential, or registry defect first. Then dispatch `release-server` with:

- operation: `resume`
- version: the draft Release's Cargo version
- target_sha: `git rev-list -n1 cc-lb-vX.Y.Z`

Resume fails unless the Release is still draft, the tag points to `target_sha`, and existing artifacts carry the same source revision.

## Recover a missed automatic run

Dispatch `release-server` with operation `start`, the Cargo version, and the reviewed release PR merge commit SHA. The target must be reachable from `master` and must change `[workspace.package].version` relative to its first parent.

## Source defect after tagging

Never move or delete the immutable source tag to replace code. Leave the failed draft as evidence, fix the source, and cut the next patch release through a new dedicated release PR.

## Inconsistent tag and Release state

If the tag exists without a draft Release, or a draft Release exists without the expected tag, stop. Record the failed Actions URL and exact tag/Release state. Repair only after confirming no image or chart version points to different source content.
```

Replace `X.Y.Z` in examples only as an operator-entered value; it is not repository state duplicated in a file.

- [ ] **Step 2: Run all focused tests**

Run:

```bash
python3 scripts/test_check_crate_versions.py -v
python3 scripts/test_server_release.py -v
actionlint \
  .github/workflows/ci.yml \
  .github/workflows/cd.yml \
  .github/workflows/publish-chart.yml \
  .github/workflows/release-server.yml \
  .github/workflows/release-plz.yml
```

Expected: every Python test PASS; actionlint emits no diagnostics.

- [ ] **Step 3: Run release metadata and Helm smoke checks**

Run:

```bash
cargo metadata --no-deps --format-version 1 > /tmp/cc-lb-cargo-metadata.json
python3 - <<'PY'
import json
import tomllib
from pathlib import Path

with open('/tmp/cc-lb-cargo-metadata.json', encoding='utf-8') as handle:
    data = json.load(handle)
workspace = tomllib.loads(Path('Cargo.toml').read_text())['workspace']['package']['version']
server = next(package for package in data['packages'] if package['name'] == 'cc-lb-server')
assert server['version'] == workspace
PY

tmp_chart="$(mktemp -d)"
command cp -R deploy/helm/cc-lb "$tmp_chart/cc-lb"
revision="$(git rev-parse HEAD)"
python3 scripts/server_release.py stamp-chart \
  --chart "$tmp_chart/cc-lb/Chart.yaml" \
  --revision "$revision"
helm lint "$tmp_chart/cc-lb" --set secrets.masterKey=ci-lint-placeholder
helm package "$tmp_chart/cc-lb" \
  --version 0.1.1-test.1 \
  --app-version 0.1.1-test.1 \
  --destination "$tmp_chart"
mkdir "$tmp_chart/unpacked"
tar -xzf "$tmp_chart/cc-lb-0.1.1-test.1.tgz" -C "$tmp_chart/unpacked"
python3 scripts/server_release.py verify-chart \
  --chart "$tmp_chart/unpacked/cc-lb/Chart.yaml" \
  --version 0.1.1-test.1 \
  --revision "$revision"
```

Expected: Cargo metadata identifies `cc-lb-server` at the workspace version; Helm lint/package and metadata verification succeed.

- [ ] **Step 4: Build and run the production container path**

Use the repository-required Docker daemon endpoint:

```bash
DOCKER_HOST=tcp://localhost:2375 docker build \
  --build-arg GIT_SHA="$(git rev-parse HEAD)" \
  --build-arg SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)" \
  -t cc-lb:release-plan-smoke .
DOCKER_HOST=tcp://localhost:2375 docker run --rm cc-lb:release-plan-smoke --version
```

Expected: image build succeeds and the binary prints the current Cargo version. This local smoke does not push any registry artifact.

- [ ] **Step 5: Run repository-level checks affected by the change**

Run:

```bash
cargo fmt --check
cargo check -p cc-lb-server --all-features
helm lint deploy/helm/cc-lb --set secrets.masterKey=ci-lint-placeholder
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 6: Commit runbook and any verification corrections**

```bash
git add docs/runbook/server-release.md
git commit -m "docs: add cc-lb server release runbook"
```

- [ ] **Step 7: Post-merge production proof**

After the implementation PR merges, create a separate dedicated server release PR with an operator-selected version. Do not mix code changes into it. Monitor `release-server` until:

- the `cc-lb-vX.Y.Z` tag points to the release PR merge commit;
- `ghcr.io/isac322/cc-lb:X.Y.Z` runs and reports `X.Y.Z`;
- the exact chart pulls successfully and reports matching `version`, `appVersion`, and source revision;
- the GitHub Release is published, not draft;
- all required checks pass and the release PR is mergeable before merge.

If publication fails, follow the runbook. Never rerun the failed job without first fixing the root cause.
