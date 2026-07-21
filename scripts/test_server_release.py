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
        git(self.repo, "init", "-q")
        git(self.repo, "config", "user.email", "test@example.com")
        git(self.repo, "config", "user.name", "Test")
        write_manifest(self.repo, "0.1.0")
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-q", "-m", "initial")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def commit(self, message: str) -> str:
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-q", "-m", message)
        return git(self.repo, "rev-parse", "HEAD")

    def test_detects_commit_local_workspace_version_change(self) -> None:
        write_manifest(self.repo, "0.1.1")
        target = self.commit("chore: release cc-lb v0.1.1")
        result = server_release.inspect_target(self.repo, target, "0.1.1")
        self.assertEqual(result.version, "0.1.1")
        self.assertEqual(result.parent_version, "0.1.0")
        self.assertEqual(result.tag, "cc-lb-v0.1.1")
        self.assertFalse(result.prerelease)
        self.assertTrue(result.version_changed)

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

    def test_expected_version_must_match_target(self) -> None:
        write_manifest(self.repo, "0.1.1")
        target = self.commit("chore: release cc-lb v0.1.1")
        with self.assertRaisesRegex(server_release.ReleaseError, "requested version"):
            server_release.inspect_target(self.repo, target, "0.1.2")

    def test_build_metadata_is_rejected(self) -> None:
        write_manifest(self.repo, "0.1.1+build.1")
        target = self.commit("chore: invalid server release")
        with self.assertRaisesRegex(server_release.ReleaseError, "without build metadata"):
            server_release.inspect_target(self.repo, target, None)


class DecideReleaseTests(unittest.TestCase):
    def test_absent_state_starts(self) -> None:
        self.assertEqual(
            server_release.decide_release("start", "abc", None, "absent", True),
            "start",
        )
        self.assertEqual(
            server_release.decide_release("auto", "abc", None, "absent", True),
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



class ReleaseStateTests(unittest.TestCase):
    def test_draft_release_is_detected_from_paginated_list(self) -> None:
        self.assertEqual(
            server_release.release_state(
                [[{"tag_name": "cc-lb-v1.2.3", "draft": True}]],
                "cc-lb-v1.2.3",
            ),
            "draft",
        )

    def test_published_release_is_detected_from_paginated_list(self) -> None:
        self.assertEqual(
            server_release.release_state(
                [[{"tag_name": "cc-lb-v1.2.3", "draft": False}]],
                "cc-lb-v1.2.3",
            ),
            "published",
        )

    def test_missing_release_is_absent(self) -> None:
        self.assertEqual(
            server_release.release_state(
                [[{"tag_name": "cc-lb-v1.2.2", "draft": False}]],
                "cc-lb-v1.2.3",
            ),
            "absent",
        )

    def test_duplicate_release_tags_are_rejected(self) -> None:
        with self.assertRaisesRegex(server_release.ReleaseError, "multiple releases"):
            server_release.release_state(
                [
                    [{"tag_name": "cc-lb-v1.2.3", "draft": True}],
                    [{"tag_name": "cc-lb-v1.2.3", "draft": False}],
                ],
                "cc-lb-v1.2.3",
            )

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

    def test_verify_chart_rejects_wrong_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            chart = Path(temp) / "Chart.yaml"
            chart.write_text(
                "apiVersion: v2\nname: cc-lb\nversion: 0.1.1\n"
                "appVersion: \"0.1.0\"\nannotations:\n"
                "  cc-lb.io/source-revision: deadbeef\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(server_release.ReleaseError, "metadata mismatch"):
                server_release.verify_chart(chart, "0.1.1", "deadbeef")
            with self.assertRaisesRegex(server_release.ReleaseError, "metadata mismatch"):
                server_release.verify_chart(chart, "0.1.1", "cafebabe")
    def test_stamp_quotes_numeric_revision(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            chart = Path(temp) / "Chart.yaml"
            chart.write_text(
                "apiVersion: v2\nname: cc-lb\nversion: 0.1.1\n"
                "appVersion: \"0.1.1\"\nannotations:\n"
                "  cc-lb.io/source-revision: __SOURCE_REVISION__\n",
                encoding="utf-8",
            )
            revision = "0" * 40
            server_release.stamp_chart(chart, revision)
            self.assertIn(
                f'cc-lb.io/source-revision: "{revision}"',
                chart.read_text(encoding="utf-8"),
            )



if __name__ == "__main__":
    unittest.main()
