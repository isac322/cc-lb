#!/usr/bin/env python3
"""Backfill pooled 7d_fable quota history from retained SQLite checkpoints.

The operator is read-only unless ``--apply`` is supplied. Applied rows are
written with one ``BEGIN IMMEDIATE`` transaction and an idempotent upsert.
"""

from __future__ import annotations

import argparse
import bisect
import sqlite3
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True)
class Interval:
    start: int
    end: int | None
    value: str | float | None


@dataclass(frozen=True)
class Checkpoint:
    upstream_id: str
    changed_at_unix_millis: int
    source: str
    sample_id: str
    utilization: float | None


UPSERT_SQL = """
INSERT INTO pool_subscription_quota_history_v1 (
    snapshot_at_unix_secs,
    quota_window,
    utilization,
    weighted_utilization_sum,
    capacity_ratio_sum,
    eligible_upstreams,
    contributing_upstreams,
    stale_upstreams,
    missing_observation_upstreams,
    missing_metadata_upstreams,
    header_contributing_upstreams,
    api_contributing_upstreams,
    max_observed_at_unix_millis,
    computed_at_unix_millis,
    policy_version
) VALUES (?, '7d_fable', ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT(snapshot_at_unix_secs, quota_window) DO UPDATE SET
    utilization = excluded.utilization,
    weighted_utilization_sum = excluded.weighted_utilization_sum,
    capacity_ratio_sum = excluded.capacity_ratio_sum,
    eligible_upstreams = excluded.eligible_upstreams,
    contributing_upstreams = excluded.contributing_upstreams,
    stale_upstreams = excluded.stale_upstreams,
    missing_observation_upstreams = excluded.missing_observation_upstreams,
    missing_metadata_upstreams = excluded.missing_metadata_upstreams,
    header_contributing_upstreams = excluded.header_contributing_upstreams,
    api_contributing_upstreams = excluded.api_contributing_upstreams,
    max_observed_at_unix_millis = excluded.max_observed_at_unix_millis,
    computed_at_unix_millis = excluded.computed_at_unix_millis,
    policy_version = excluded.policy_version
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill SQLite pooled 7d_fable history from retained checkpoints."
    )
    parser.add_argument(
        "--database",
        type=Path,
        required=True,
        help="path to the migrated SQLite storage database",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the rows; without this flag the operator is read-only",
    )
    return parser.parse_args()


def load_intervals(
    rows: Iterable[sqlite3.Row], key_column: str, value_column: str
) -> tuple[dict[str, list[Interval]], dict[str, list[int]]]:
    intervals: dict[str, list[Interval]] = defaultdict(list)
    for row in rows:
        intervals[row[key_column]].append(
            Interval(
                start=row["effective_from_unix_millis"],
                end=row["effective_to_unix_millis"],
                value=row[value_column],
            )
        )
    starts = {
        key: [interval.start for interval in values]
        for key, values in intervals.items()
    }
    return dict(intervals), starts


def resolve_interval(
    intervals: dict[str, list[Interval]],
    starts: dict[str, list[int]],
    key: str,
    at_unix_millis: int,
) -> str | float | None:
    key_starts = starts.get(key)
    if not key_starts:
        return None
    index = bisect.bisect_right(key_starts, at_unix_millis) - 1
    if index < 0:
        return None
    interval = intervals[key][index]
    if interval.end is not None and at_unix_millis >= interval.end:
        return None
    return interval.value


def utc_timestamp(unix_secs: int) -> str:
    return datetime.fromtimestamp(unix_secs, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_backfill_rows(
    connection: sqlite3.Connection,
) -> tuple[int, int, int, list[tuple[object, ...]]]:
    eligible_upstream_ids = [
        row[0]
        for row in connection.execute(
            """
            SELECT id
            FROM upstream_spec_v1
            WHERE kind = 'anthropic_oauth' AND deleted_at IS NULL
            ORDER BY id
            """
        )
    ]
    eligible_set = set(eligible_upstream_ids)

    checkpoint_stats = connection.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT checkpoints.upstream_id),
            MIN(checkpoints.changed_at_unix_millis),
            MAX(checkpoints.changed_at_unix_millis)
        FROM upstream_subscription_quota_checkpoints_v1 AS checkpoints
        JOIN upstream_spec_v1 AS upstreams ON upstreams.id = checkpoints.upstream_id
        WHERE checkpoints.window = '7d_fable'
          AND upstreams.kind = 'anthropic_oauth'
          AND upstreams.deleted_at IS NULL
        """
    ).fetchone()
    checkpoint_count, checkpoint_upstreams, first_millis, last_millis = checkpoint_stats
    if first_millis is None or last_millis is None:
        return checkpoint_count, checkpoint_upstreams, 0, []

    targets = [
        row[0]
        for row in connection.execute(
            """
            SELECT snapshot_at_unix_secs
            FROM pool_subscription_quota_history_v1
            WHERE quota_window = '5h'
              AND snapshot_at_unix_secs * 1000 >= ?
              AND snapshot_at_unix_secs * 1000 <= ?
              AND snapshot_at_unix_secs < COALESCE(
                    (
                        SELECT MIN(snapshot_at_unix_secs)
                        FROM pool_subscription_quota_history_v1
                        WHERE quota_window = '7d_fable'
                    ),
                    9223372036854775807
                  )
            ORDER BY snapshot_at_unix_secs
            """,
            (first_millis, last_millis),
        )
    ]

    plan_intervals, plan_starts = load_intervals(
        connection.execute(
            """
            SELECT
                upstream_id,
                effective_from_unix_millis,
                effective_to_unix_millis,
                tier_key
            FROM upstream_plan_tier_history_v1
            ORDER BY upstream_id, effective_from_unix_millis
            """
        ),
        "upstream_id",
        "tier_key",
    )
    ratio_intervals, ratio_starts = load_intervals(
        connection.execute(
            """
            SELECT
                tier_key,
                effective_from_unix_millis,
                effective_to_unix_millis,
                pro_relative_ratio
            FROM plan_tier_ratio_history_v1
            ORDER BY tier_key, effective_from_unix_millis
            """
        ),
        "tier_key",
        "pro_relative_ratio",
    )

    checkpoints = iter(
        Checkpoint(
            upstream_id=row[0],
            changed_at_unix_millis=row[1],
            source=row[2],
            sample_id=row[3],
            utilization=row[4],
        )
        for row in connection.execute(
            """
            SELECT
                checkpoints.upstream_id,
                checkpoints.changed_at_unix_millis,
                checkpoints.source,
                checkpoints.sample_id,
                checkpoints.utilization
            FROM upstream_subscription_quota_checkpoints_v1 AS checkpoints
            JOIN upstream_spec_v1 AS upstreams ON upstreams.id = checkpoints.upstream_id
            WHERE checkpoints.window = '7d_fable'
              AND upstreams.kind = 'anthropic_oauth'
              AND upstreams.deleted_at IS NULL
            ORDER BY
                checkpoints.changed_at_unix_millis,
                checkpoints.source,
                checkpoints.sample_id
            """
        )
    )
    next_checkpoint = next(checkpoints, None)
    latest_by_upstream: dict[str, Checkpoint] = {}
    rows: list[tuple[object, ...]] = []

    for target_unix_secs in targets:
        target_unix_millis = target_unix_secs * 1000
        while (
            next_checkpoint is not None
            and next_checkpoint.changed_at_unix_millis <= target_unix_millis
        ):
            if next_checkpoint.utilization is not None:
                latest_by_upstream[next_checkpoint.upstream_id] = next_checkpoint
            next_checkpoint = next(checkpoints, None)

        weighted_sum = 0.0
        ratio_sum = 0.0
        contributing = 0
        missing_observation = 0
        missing_metadata = 0
        header_contributing = 0
        api_contributing = 0
        max_observed: int | None = None

        for upstream_id in eligible_upstream_ids:
            checkpoint = latest_by_upstream.get(upstream_id)
            if checkpoint is None or checkpoint.utilization is None:
                missing_observation += 1
                continue

            tier_key = resolve_interval(
                plan_intervals,
                plan_starts,
                upstream_id,
                target_unix_millis,
            )
            if not isinstance(tier_key, str):
                missing_metadata += 1
                continue

            ratio = resolve_interval(
                ratio_intervals,
                ratio_starts,
                tier_key,
                target_unix_millis,
            )
            if not isinstance(ratio, (int, float)) or isinstance(ratio, bool):
                missing_metadata += 1
                continue

            weighted_sum += checkpoint.utilization * ratio
            ratio_sum += ratio
            contributing += 1
            if checkpoint.source == "header":
                header_contributing += 1
            elif checkpoint.source == "api":
                api_contributing += 1
            if (
                max_observed is None
                or checkpoint.changed_at_unix_millis > max_observed
            ):
                max_observed = checkpoint.changed_at_unix_millis

        if contributing == 0 or ratio_sum <= 0.0:
            continue

        utilization = min(1.0, max(0.0, weighted_sum / ratio_sum))
        rows.append(
            (
                target_unix_secs,
                utilization,
                weighted_sum,
                ratio_sum,
                len(eligible_set),
                contributing,
                missing_observation,
                missing_metadata,
                header_contributing,
                api_contributing,
                max_observed,
                target_unix_millis,
            )
        )

    return checkpoint_count, checkpoint_upstreams, len(targets), rows


def main() -> None:
    args = parse_args()
    database = args.database.resolve()
    if not database.is_file():
        raise SystemExit(f"database does not exist: {database}")

    mode = "rw" if args.apply else "ro"
    connection = sqlite3.connect(
        f"file:{database}?mode={mode}",
        uri=True,
        timeout=5.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA foreign_keys=ON")

    try:
        if args.apply:
            connection.execute("BEGIN IMMEDIATE")
        checkpoint_count, checkpoint_upstreams, target_count, rows = (
            build_backfill_rows(connection)
        )
        if args.apply:
            connection.executemany(UPSERT_SQL, rows)
            connection.commit()
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()

    if rows:
        range_utc = f"{utc_timestamp(rows[0][0])}..{utc_timestamp(rows[-1][0])}"
    else:
        range_utc = "none"
    print(
        f"checkpoints={checkpoint_count} upstreams={checkpoint_upstreams} "
        f"target_timestamps={target_count}"
    )
    print(f"backfill_rows={len(rows)} range_utc={range_utc}")
    print(f"mode={'applied' if args.apply else 'dry-run'}")


if __name__ == "__main__":
    main()
