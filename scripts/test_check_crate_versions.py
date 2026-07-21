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
    (root / "crates/cc-lb-server").mkdir(parents=True, exist_ok=True)
    (root / "crates/cc-lb-internal").mkdir(parents=True, exist_ok=True)
    (root / "crates/cc-lb-plugin-wire").mkdir(parents=True, exist_ok=True)
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
        self.assertTrue(any("unrelated" in error for error in errors))

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

    def test_release_pr_rejects_build_metadata(self) -> None:
        write_workspace(self.head, "0.1.1+build.1")
        errors = self.validate(
            "chore: release cc-lb v0.1.1+build.1",
            {"Cargo.toml", "Cargo.lock", "CHANGELOG.md"},
        )
        self.assertTrue(any("build metadata" in error for error in errors))

    def test_release_title_without_version_change_fails(self) -> None:
        errors = self.validate(
            "chore: release cc-lb v0.1.0",
            {"Cargo.toml", "Cargo.lock", "CHANGELOG.md"},
        )
        self.assertTrue(any("greater than" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
