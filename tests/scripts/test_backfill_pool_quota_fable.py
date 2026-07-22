from __future__ import annotations

import importlib.util
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "backfill-pool-quota-fable.py"


SCHEMA = """
CREATE TABLE upstream_spec_v1 (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    deleted_at INTEGER
);
CREATE TABLE upstream_subscription_quota_checkpoints_v1 (
    upstream_id TEXT NOT NULL,
    window TEXT NOT NULL,
    changed_at_unix_millis INTEGER NOT NULL,
    source TEXT NOT NULL,
    sample_id TEXT NOT NULL,
    utilization REAL
);
CREATE TABLE upstream_plan_tier_history_v1 (
    upstream_id TEXT NOT NULL,
    effective_from_unix_millis INTEGER NOT NULL,
    effective_to_unix_millis INTEGER,
    tier_key TEXT NOT NULL
);
CREATE TABLE plan_tier_ratio_history_v1 (
    tier_key TEXT NOT NULL,
    effective_from_unix_millis INTEGER NOT NULL,
    effective_to_unix_millis INTEGER,
    pro_relative_ratio REAL NOT NULL
);
CREATE TABLE pool_subscription_quota_history_v1 (
    snapshot_at_unix_secs INTEGER NOT NULL,
    quota_window TEXT NOT NULL,
    utilization REAL,
    weighted_utilization_sum REAL NOT NULL,
    capacity_ratio_sum REAL NOT NULL,
    eligible_upstreams INTEGER NOT NULL,
    contributing_upstreams INTEGER NOT NULL,
    stale_upstreams INTEGER NOT NULL,
    missing_observation_upstreams INTEGER NOT NULL,
    missing_metadata_upstreams INTEGER NOT NULL,
    header_contributing_upstreams INTEGER NOT NULL,
    api_contributing_upstreams INTEGER NOT NULL,
    max_observed_at_unix_millis INTEGER,
    computed_at_unix_millis INTEGER NOT NULL,
    policy_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (snapshot_at_unix_secs, quota_window)
);
"""


def load_operator():
    spec = importlib.util.spec_from_file_location("backfill_pool_quota_fable", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def create_fixture(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(SCHEMA)
    connection.executemany(
        "INSERT INTO upstream_spec_v1 (id, kind, deleted_at) VALUES (?, ?, ?)",
        [
            ("u1", "anthropic_oauth", None),
            ("u2", "anthropic_oauth", None),
            ("u3", "anthropic_oauth", None),
            ("u4", "anthropic_oauth", None),
            ("excluded", "anthropic_api_key", None),
            ("deleted", "anthropic_oauth", 1),
        ],
    )
    connection.executemany(
        """
        INSERT INTO upstream_plan_tier_history_v1 (
            upstream_id, effective_from_unix_millis,
            effective_to_unix_millis, tier_key
        ) VALUES (?, ?, ?, ?)
        """,
        [
            ("u1", 0, None, "team"),
            ("u2", 0, None, "pro"),
            ("u3", 0, None, "team"),
        ],
    )
    connection.executemany(
        """
        INSERT INTO plan_tier_ratio_history_v1 (
            tier_key, effective_from_unix_millis,
            effective_to_unix_millis, pro_relative_ratio
        ) VALUES (?, ?, ?, ?)
        """,
        [
            ("team", 0, 250_000, 2.0),
            ("team", 250_000, None, 3.0),
            ("pro", 0, None, 1.0),
        ],
    )
    connection.executemany(
        """
        INSERT INTO upstream_subscription_quota_checkpoints_v1 (
            upstream_id, window, changed_at_unix_millis,
            source, sample_id, utilization
        ) VALUES (?, '7d_fable', ?, ?, ?, ?)
        """,
        [
            ("u1", 150_000, "api", "a", 0.1),
            ("u1", 150_000, "header", "b", 0.5),
            ("u2", 150_000, "api", "a", 0.25),
            ("u4", 150_000, "api", "a", 0.9),
            ("u1", 250_000, "header", "c", 0.4),
            ("u3", 250_000, "api", "a", 0.6),
            ("u2", 275_000, "api", "z", None),
            ("u1", 350_000, "header", "d", 0.9),
            ("excluded", 150_000, "header", "a", 1.0),
            ("deleted", 150_000, "header", "a", 1.0),
        ],
    )
    connection.executemany(
        """
        INSERT INTO pool_subscription_quota_history_v1 (
            snapshot_at_unix_secs, quota_window, utilization,
            weighted_utilization_sum, capacity_ratio_sum,
            eligible_upstreams, contributing_upstreams, stale_upstreams,
            missing_observation_upstreams, missing_metadata_upstreams,
            header_contributing_upstreams, api_contributing_upstreams,
            max_observed_at_unix_millis, computed_at_unix_millis, policy_version
        ) VALUES (?, '5h', 0.0, 0.0, 0.0, 0, 0, 0, 0, 0, 0, 0, NULL, ?, 1)
        """,
        [(100, 100_000), (200, 200_000), (300, 300_000), (400, 400_000)],
    )
    connection.commit()
    connection.close()


class BackfillPoolQuotaFableTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "storage.sqlite"
        create_fixture(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def fable_rows(self) -> list[sqlite3.Row]:
        connection = sqlite3.connect(self.database)
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT *
            FROM pool_subscription_quota_history_v1
            WHERE quota_window = '7d_fable'
            ORDER BY snapshot_at_unix_secs
            """
        ).fetchall()
        connection.close()
        return rows

    def test_build_rows_matches_historical_pool_semantics(self) -> None:
        operator = load_operator()
        connection = sqlite3.connect(self.database)
        connection.row_factory = sqlite3.Row

        checkpoint_count, upstream_count, target_count, rows = operator.build_backfill_rows(
            connection
        )
        connection.close()

        self.assertEqual(checkpoint_count, 8)
        self.assertEqual(upstream_count, 4)
        self.assertEqual(target_count, 2)
        self.assertEqual([row[0] for row in rows], [200, 300])

        first = rows[0]
        self.assertAlmostEqual(first[1], 1.25 / 3.0)
        self.assertEqual(
            first[2:],
            (1.25, 3.0, 4, 2, 1, 1, 1, 1, 150_000, 200_000),
        )

        second = rows[1]
        self.assertAlmostEqual(second[1], 3.25 / 7.0)
        self.assertEqual(
            second[2:],
            (3.25, 7.0, 4, 3, 0, 1, 1, 2, 250_000, 300_000),
        )

    def test_cli_is_dry_run_by_default_and_apply_is_idempotent(self) -> None:
        dry_run = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--database", str(self.database)],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("backfill_rows=2", dry_run.stdout)
        self.assertIn("mode=dry-run", dry_run.stdout)
        self.assertEqual(self.fable_rows(), [])

        applied_outputs = []
        for _ in range(2):
            applied = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--database",
                    str(self.database),
                    "--apply",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("mode=applied", applied.stdout)
            applied_outputs.append(applied.stdout)

        self.assertIn("backfill_rows=2", applied_outputs[0])
        self.assertIn("backfill_rows=0", applied_outputs[1])

        rows = self.fable_rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual([row["snapshot_at_unix_secs"] for row in rows], [200, 300])
        self.assertTrue(all(row["policy_version"] == 1 for row in rows))

    def test_existing_live_fable_rows_are_never_overwritten(self) -> None:
        connection = sqlite3.connect(self.database)
        connection.execute(
            """
            INSERT INTO pool_subscription_quota_history_v1 (
                snapshot_at_unix_secs, quota_window, utilization,
                weighted_utilization_sum, capacity_ratio_sum,
                eligible_upstreams, contributing_upstreams, stale_upstreams,
                missing_observation_upstreams, missing_metadata_upstreams,
                header_contributing_upstreams, api_contributing_upstreams,
                max_observed_at_unix_millis, computed_at_unix_millis,
                policy_version
            ) VALUES (
                300, '7d_fable', 0.777, 7.77, 9.99, 4, 4, 2,
                0, 0, 3, 1, 299999, 300123, 1
            )
            """
        )
        connection.commit()
        connection.close()

        applied = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--database",
                str(self.database),
                "--apply",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertIn("target_timestamps=1", applied.stdout)
        self.assertIn("backfill_rows=1", applied.stdout)
        rows = self.fable_rows()
        self.assertEqual([row["snapshot_at_unix_secs"] for row in rows], [200, 300])
        live = rows[1]
        self.assertEqual(
            (
                live["utilization"],
                live["weighted_utilization_sum"],
                live["capacity_ratio_sum"],
                live["stale_upstreams"],
                live["max_observed_at_unix_millis"],
                live["computed_at_unix_millis"],
            ),
            (0.777, 7.77, 9.99, 2, 299999, 300123),
        )

    def test_apply_rolls_back_every_row_on_failure(self) -> None:
        connection = sqlite3.connect(self.database)
        connection.execute(
            """
            CREATE TRIGGER fail_second_fable_insert
            BEFORE INSERT ON pool_subscription_quota_history_v1
            WHEN NEW.quota_window = '7d_fable'
             AND NEW.snapshot_at_unix_secs = 300
            BEGIN
                SELECT RAISE(ABORT, 'forced test failure');
            END
            """
        )
        connection.commit()
        connection.close()

        applied = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--database",
                str(self.database),
                "--apply",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertNotEqual(applied.returncode, 0)
        self.assertIn("forced test failure", applied.stderr)
        self.assertEqual(self.fable_rows(), [])

    def test_missing_database_is_rejected_without_creating_it(self) -> None:
        missing = Path(self.temp_dir.name) / "missing.sqlite"

        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--database", str(missing)],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("database does not exist", result.stderr)
        self.assertFalse(missing.exists())


if __name__ == "__main__":
    unittest.main()
