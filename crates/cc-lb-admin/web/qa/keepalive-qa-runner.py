#!/usr/bin/env python3
"""Prepare and compare isolated Cache Keepalive QA fixtures.

This harness never starts cc-lb, Vite, browsers, Docker containers, or fake
upstreams. The Main harness owns every daemon. This file only seeds an already
migrated isolated database, calls loopback admin APIs, records raw evidence, and
captures read-only query plans.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import json
import math
import os
import re
import socket
import sqlite3
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

ROOT = Path(__file__).resolve().parents[4]
_scratch_env = os.environ.get("KEEPALIVE_SCRATCH_DIR")
if _scratch_env is not None and not _scratch_env.strip():
    raise RuntimeError("KEEPALIVE_SCRATCH_DIR must not be empty")
DEFAULT_SCRATCH = (
    Path(_scratch_env).expanduser().resolve()
    if _scratch_env is not None
    else ROOT / "target" / "keepalive-qa"
)
DEFAULT_MANIFEST = DEFAULT_SCRATCH / "fixture-manifest.json"
DEFAULT_RECORDS = DEFAULT_SCRATCH / "api-records.jsonl"
DEFAULT_EVIDENCE = DEFAULT_SCRATCH / "evidence"
DEFAULT_PG_CONTAINER = "cc-lb-keepalive-qa-postgres"
DEFAULT_TOKEN_ENV = "CC_LB_ADMIN_TOKEN"
SAFE_DB_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SAFE_CONTAINER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
QA_DB_NAME = re.compile(r"(?:keepalive|qa|test|baseline|candidate|scratch)", re.I)
FIXTURE_VERSION = 3
FIXTURE_LIFETIME_SECONDS = 7 * 24 * 60 * 60
CURSOR_HORIZONS = {"24h", "7d", "all"}
CURSOR_FILTERS = {"all", "renewed", "scheduled", "capped", "expired", "not_tracked", "error"}
CURSOR_ORIGINAL_FIELDS = {
    "principal_id",
    "horizon_start_ms",
    "filter",
    "last_message_at_ms",
    "entry_id",
}
HORIZON_DURATION_MS = {"24h": 24 * 60 * 60 * 1_000, "7d": 7 * 24 * 60 * 60 * 1_000}
CONFIG_SNAPSHOT = {
    "refresh_lead_time_5m_secs": 30,
    "refresh_lead_time_1h_secs": 300,
    "max_refreshes_per_session": 12,
    "max_total_duration_secs": 14_400,
    "snapshot_max_bytes": 524_288,
}
KEEPALIVE_CONFIG = {
    "enabled": True,
    **CONFIG_SNAPSHOT,
    "classifier": {
        "extra_wait_for_user_tools": [],
        "treat_end_turn_as_ambiguous": False,
    },
}
UPSTREAM_ID = "00000000-0000-4000-8000-000000000100"
PRINCIPALS = {
    "empty": "00000000-0000-4000-8000-000000000001",
    "low": "00000000-0000-4000-8000-000000000002",
    "high_ui": "00000000-0000-4000-8000-000000000003",
    "disabled": "00000000-0000-4000-8000-000000000004",
    "d100k": "00000000-0000-4000-8000-000000000005",
    "st_scale": "00000000-0000-4000-8000-000000000006",
}
PRINCIPAL_NAMES = {
    "empty": "keepalive-empty",
    "low": "keepalive-low",
    "high_ui": "keepalive-high-ui",
    "disabled": "keepalive-disabled",
    "d100k": "keepalive-d100k",
    "st_scale": "keepalive-st-scale",
}
LOW_IDS = {
    "active": "low-active-01",
    "scheduled": "low-scheduled-01",
    "terminal": "low-terminal-01",
    "expired": "low-expired-01",
    "decision": "decision bare é",
    "collision": "collision-01",
    "transition_decision": "transition-01",
    "transition_session": "transition-session-01",
    "cleanup": "cleanup-expired-01",
}


class HarnessError(RuntimeError):
    pass


@dataclass(frozen=True)
class SessionRow:
    session_key_hash: str
    principal_id: str
    upstream_id: str
    generation: int
    refresh_count: int
    first_scheduled_at: int
    cache_anchor_at: int
    run_at: int
    ttl: str
    status: str
    enqueue_state: str
    current_job_key: str
    encrypted_payload: bytes
    terminal_reason: str | None
    expires_at: int
    created_at: int
    updated_at: int
    accounting_key_id: str | None
    running_since_unix_secs: int | None
    display_reason: str
    error: str | None
    config_snapshot: str | None
    last_message_at_ms: int


@dataclass(frozen=True)
class DecisionRow:
    source_ref_id: str
    decision: str
    reason: str
    generation: int
    ts: int
    principal_id: str
    session_key_hash: str | None
    upstream_id: str
    error: str | None
    ttl: str
    config_snapshot: str | None
    last_message_at_ms: int | None


@dataclass(frozen=True)
class TurnRow:
    source_ref_id: str
    session_key_hash: str
    principal_id: str
    accounting_key_id: str | None
    upstream_id: str
    model: str
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_creation_input_tokens_5m: int
    cache_creation_input_tokens_1h: int
    cache_read_input_tokens: int
    cost_micros: int
    hit_miss: str
    ts: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def normalize_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: normalize_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_json(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise HarnessError("non-finite JSON number cannot be canonicalized")
        return int(value) if value.is_integer() else value
    return value


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        normalize_json(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def sha256_text(value: str | None) -> str | None:
    return None if value is None else sha256_bytes(value.encode())

def decode_cursor_payload(cursor: str) -> dict[str, Any]:
    try:
        padding = "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(cursor + padding))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise HarnessError("opaque cursor is not valid base64url JSON") from error
    if not isinstance(payload, dict):
        raise HarnessError("opaque cursor JSON is not an object")
    return payload


def normalize_cursor_for_comparison(
    cursor: str,
    *,
    phase: str,
    expected_horizon: str,
) -> dict[str, Any]:
    if expected_horizon not in CURSOR_HORIZONS:
        raise HarnessError(f"unsupported expected cursor horizon {expected_horizon}")
    payload = decode_cursor_payload(cursor)
    expected_fields = set(CURSOR_ORIGINAL_FIELDS)
    if phase == "candidate":
        expected_fields.add("horizon")
    if set(payload) != expected_fields:
        raise HarnessError(
            f"{phase} cursor fields {sorted(payload)} do not match expected {sorted(expected_fields)}"
        )
    if phase == "candidate" and payload["horizon"] != expected_horizon:
        raise HarnessError(
            f"candidate cursor horizon tag {payload['horizon']!r} does not match {expected_horizon!r}"
        )
    if payload["filter"] not in CURSOR_FILTERS:
        raise HarnessError(f"cursor has unknown filter {payload['filter']!r}")
    if not isinstance(payload["principal_id"], str) or not payload["principal_id"]:
        raise HarnessError("cursor principal_id must be a non-empty string")
    if not isinstance(payload["last_message_at_ms"], int) or isinstance(
        payload["last_message_at_ms"], bool
    ):
        raise HarnessError("cursor last_message_at_ms must be an integer")
    if not isinstance(payload["entry_id"], str) or not payload["entry_id"]:
        raise HarnessError("cursor entry_id must be a non-empty string")
    horizon_start_ms = payload["horizon_start_ms"]
    if expected_horizon == "all":
        if horizon_start_ms is not None:
            raise HarnessError("all cursor must have a null horizon_start_ms")
    elif not isinstance(horizon_start_ms, int) or isinstance(horizon_start_ms, bool):
        raise HarnessError(f"{expected_horizon} cursor must have an integer horizon_start_ms")
    return {field: payload[field] for field in CURSOR_ORIGINAL_FIELDS}


def cursor_evidence(cursor: str | None) -> dict[str, Any]:
    if cursor is None:
        return {"original_fields_sha256": None, "horizon": None}
    payload = decode_cursor_payload(cursor)
    original = {field: payload.get(field) for field in CURSOR_ORIGINAL_FIELDS}
    return {
        "original_fields_sha256": sha256_json(original),
        "horizon": payload.get("horizon"),
    }


def expected_horizon_for_path(path: str) -> str:
    parsed = urllib.parse.urlsplit(path)
    return urllib.parse.parse_qs(parsed.query).get("horizon", ["24h"])[0]


def normalized_response_for_comparison(
    value: Any,
    *,
    phase: str,
    expected_horizon: str,
) -> Any:
    if not isinstance(value, dict) or "next_cursor" not in value:
        return value
    normalized = dict(value)
    cursor = value["next_cursor"]
    if cursor is None:
        return normalized
    if not isinstance(cursor, str) or not cursor:
        raise HarnessError(f"{phase} response has a non-string non-null cursor")
    normalized["next_cursor"] = normalize_cursor_for_comparison(
        cursor,
        phase=phase,
        expected_horizon=expected_horizon,
    )
    return normalized


def wait_past_cursor_request_second(cursor: str, horizon: str) -> None:
    original = normalize_cursor_for_comparison(
        cursor,
        phase="candidate",
        expected_horizon=horizon,
    )
    horizon_start_ms = original["horizon_start_ms"]
    if not isinstance(horizon_start_ms, int):
        raise HarnessError(f"{horizon} cursor has no bounded request anchor")
    request_now_ms = horizon_start_ms + HORIZON_DURATION_MS[horizon]
    delay_ms = request_now_ms + 1_050 - int(time.time() * 1_000)
    if delay_ms > 0:
        time.sleep(delay_ms / 1_000)


def percentile(values: Sequence[float], percentile_value: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile_value
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_json_bytes(value) + b"\n"
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise HarnessError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise HarnessError(f"expected JSON object in {path}")
    return value

def required_path(raw: str, label: str) -> Path:
    if not raw.strip():
        raise HarnessError(f"{label} must not be empty")
    return Path(raw).expanduser()


def load_fixture_manifest(raw: str) -> tuple[Path, dict[str, Any]]:
    path = required_path(raw, "fixture manifest path")
    manifest = load_json(path)
    if manifest.get("fixture_version") != FIXTURE_VERSION:
        raise HarnessError(
            f"fixture manifest version must be {FIXTURE_VERSION}; reseed every dataset with this runner"
        )
    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict) or not datasets:
        raise HarnessError(f"fixture manifest has no datasets: {path}")
    if manifest.get("principals") != PRINCIPALS or manifest.get("low_ids") != LOW_IDS:
        raise HarnessError("fixture manifest IDs do not match this canonical runner")
    anchors: set[int] = set()
    for key, dataset in datasets.items():
        if not isinstance(dataset, dict) or not dataset:
            raise HarnessError(f"manifest dataset {key} is empty")
        validate_dataset_lifecycle(dataset, key)
        anchors.add(dataset["anchor_ms"])
    if len(anchors) != 1:
        raise HarnessError("all manifest datasets must use one shared fixture anchor")
    return path, manifest

def expected_lifecycle(anchor_ms: int) -> dict[str, Any]:
    return {
        "expires_at_s": anchor_ms // 1_000 + FIXTURE_LIFETIME_SECONDS,
        "active_enqueue_state": "enqueued",
        "run_at": "future",
        "updated_at_s": anchor_ms // 1_000,
    }


def validate_dataset_lifecycle(dataset: dict[str, Any], key: str) -> None:
    anchor_ms = dataset.get("anchor_ms")
    if not isinstance(anchor_ms, int) or isinstance(anchor_ms, bool):
        raise HarnessError(f"manifest dataset {key} has no integer anchor_ms")
    if dataset.get("lifecycle") != expected_lifecycle(anchor_ms):
        raise HarnessError(
            f"manifest dataset {key} does not have the seven-day fixture lifecycle; reseed it"
        )


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
    temporary.write_bytes(canonical_json_bytes(value) + b"\n")
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def require_loopback_url(raw: str, label: str) -> str:
    parsed = urllib.parse.urlsplit(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise HarnessError(f"{label} must be an http(s) origin without credentials")
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(parsed.hostname, parsed.port or 80)}
    except socket.gaierror as error:
        raise HarnessError(f"cannot resolve {label} host") from error
    if not addresses or any(not ip_is_loopback(address) for address in addresses):
        raise HarnessError(f"{label} must resolve only to loopback addresses")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise HarnessError(f"{label} must be an origin without path, query, or fragment")
    return raw.rstrip("/")


def ip_is_loopback(address: str) -> bool:
    import ipaddress

    try:
        return ipaddress.ip_address(address.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def read_secret(env_name: str | None, file_name: str | None) -> str | None:
    if env_name and file_name:
        raise HarnessError("choose either --token-env or --token-file")
    value: str | None = None
    if file_name:
        path = Path(file_name)
        mode = path.stat().st_mode & 0o777
        if mode & 0o077:
            raise HarnessError("token file must not be group/world accessible")
        value = path.read_text().strip()
    elif env_name:
        raw_value = os.environ.get(env_name)
        value = raw_value.strip() if raw_value is not None else None
    return value


def redact_cursor_url(url: str) -> tuple[str, str | None]:
    parsed = urllib.parse.urlsplit(url)
    pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    cursor = next((value for key, value in pairs if key == "cursor"), None)
    redacted = [(key, f"<sha256:{sha256_text(value)}>" if key == "cursor" else value) for key, value in pairs]
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(redacted), "")), sha256_text(cursor)


def fixture_rows(anchor_ms: int, d_count: int, ui_count: int, st_sessions: int, turns_per_session: int) -> tuple[list[SessionRow], list[DecisionRow], list[TurnRow]]:
    sessions: list[SessionRow] = []
    decisions: list[DecisionRow] = []
    turns: list[TurnRow] = []
    snapshot = json.dumps(CONFIG_SNAPSHOT, separators=(",", ":"), sort_keys=True)
    anchor_s = anchor_ms // 1_000

    def session(
        principal: str,
        identifier: str,
        offset_ms: int,
        *,
        refresh_count: int = 0,
        status: str = "active",
        terminal_reason: str | None = None,
        ttl: str = "5m",
        error: str | None = None,
        generation: int = 1,
        reason: str = "agent-in-turn (tool_use: `bash`)",
    ) -> SessionRow:
        message_ms = anchor_ms - offset_ms
        message_s = message_ms // 1_000
        lifecycle_expires_s = anchor_s + FIXTURE_LIFETIME_SECONDS
        return SessionRow(
            session_key_hash=identifier,
            principal_id=principal,
            upstream_id=UPSTREAM_ID,
            generation=generation,
            refresh_count=refresh_count,
            first_scheduled_at=message_s,
            cache_anchor_at=message_s,
            run_at=lifecycle_expires_s - 300,
            ttl=ttl,
            status=status,
            enqueue_state="enqueued" if status == "active" else "pending",
            current_job_key=f"cache_keepalive:{identifier}:{generation}",
            encrypted_payload=b"qa",
            terminal_reason=terminal_reason,
            expires_at=lifecycle_expires_s,
            created_at=message_s,
            updated_at=anchor_s,
            accounting_key_id=None,
            running_since_unix_secs=None,
            display_reason=reason,
            error=error,
            config_snapshot=snapshot,
            last_message_at_ms=message_ms,
        )

    def decision(
        principal: str,
        identifier: str,
        offset_ms: int,
        *,
        nullable_timestamp: bool = False,
        error: str | None = None,
        linked_session: str | None = None,
        generation: int = 1,
    ) -> DecisionRow:
        message_ms = anchor_ms - offset_ms
        return DecisionRow(
            source_ref_id=identifier,
            decision="not_tracked",
            reason="user turn (stop_reason=end_turn)",
            generation=generation,
            ts=message_ms // 1_000,
            principal_id=principal,
            session_key_hash=linked_session,
            upstream_id=UPSTREAM_ID,
            error=error,
            ttl="5m",
            config_snapshot=snapshot,
            last_message_at_ms=None if nullable_timestamp else message_ms,
        )

    low = PRINCIPALS["low"]
    sessions.extend(
        [
            session(low, LOW_IDS["active"], 20_000, refresh_count=3),
            session(low, LOW_IDS["scheduled"], 30_000, refresh_count=0),
            session(low, LOW_IDS["terminal"], 40_000, refresh_count=12, status="terminal", terminal_reason="max_refreshes"),
            session(low, LOW_IDS["expired"], 50_000, refresh_count=2, status="terminal", terminal_reason="expired", ttl="1h"),
            session(low, LOW_IDS["collision"], 70_000, refresh_count=1),
            session(low, LOW_IDS["cleanup"], 80_000, refresh_count=1, status="terminal", terminal_reason="expired"),
        ]
    )
    decisions.extend(
        [
            decision(low, LOW_IDS["decision"], 10_000, nullable_timestamp=True),
            decision(low, LOW_IDS["collision"], 60_000),
            decision(low, LOW_IDS["transition_decision"], 15_000, linked_session=LOW_IDS["transition_session"]),
            decision(low, "low-error-decision", 25_000, error="classifier unavailable"),
        ]
    )
    for turn_number in range(1, 4):
        turns.append(
            TurnRow(
                source_ref_id=f"{LOW_IDS['active']}:turn:{turn_number}",
                session_key_hash=LOW_IDS["active"],
                principal_id=low,
                accounting_key_id=None,
                upstream_id=UPSTREAM_ID,
                model="claude-3-5-sonnet-20241022",
                input_tokens=8_000,
                output_tokens=20,
                cache_creation_input_tokens=0,
                cache_creation_input_tokens_5m=0,
                cache_creation_input_tokens_1h=0,
                cache_read_input_tokens=8_000,
                cost_micros=14_400,
                hit_miss="hit" if turn_number < 3 else "miss",
                ts=anchor_s - 20 + turn_number,
            )
        )
    turns.append(
        TurnRow(
            source_ref_id=f"{LOW_IDS['terminal']}:turn:1",
            session_key_hash=LOW_IDS["terminal"],
            principal_id=low,
            accounting_key_id=None,
            upstream_id=UPSTREAM_ID,
            model="claude-3-5-sonnet-20241022",
            input_tokens=8_000,
            output_tokens=20,
            cache_creation_input_tokens=0,
            cache_creation_input_tokens_5m=0,
            cache_creation_input_tokens_1h=0,
            cache_read_input_tokens=8_000,
            cost_micros=14_400,
            hit_miss="miss",
            ts=anchor_s - 40,
        )
    )

    for principal_key, prefix, count in (("high_ui", "ui", ui_count), ("d100k", "perf", ui_count)):
        principal = PRINCIPALS[principal_key]
        for index in range(count):
            offset_ms = 3_600_000 + index * 1_000
            kind = index % 7
            identifier = f"{prefix}-{index:06d}"
            if kind == 0:
                sessions.append(session(principal, identifier, offset_ms, refresh_count=(index % 11) + 1))
            elif kind == 1:
                sessions.append(session(principal, identifier, offset_ms))
            elif kind == 2:
                sessions.append(session(principal, identifier, offset_ms, refresh_count=12, status="terminal", terminal_reason="max_refreshes"))
            elif kind == 3:
                sessions.append(session(principal, identifier, offset_ms, refresh_count=2, status="terminal", terminal_reason="expired", ttl="1h"))
            elif kind == 4:
                decisions.append(decision(principal, identifier, offset_ms, nullable_timestamp=index % 14 == 4))
            elif kind == 5:
                sessions.append(session(principal, identifier, offset_ms, refresh_count=1, error="renewal dispatch unavailable", reason="renewal dispatch unavailable"))
            else:
                decisions.append(decision(principal, identifier, offset_ms, error="classifier unavailable"))
            if kind in {0, 2, 3, 5} and index % 35 == 0:
                turns.append(
                    TurnRow(
                        source_ref_id=f"{identifier}:turn:1",
                        session_key_hash=identifier,
                        principal_id=principal,
                        accounting_key_id=None,
                        upstream_id=UPSTREAM_ID,
                        model="claude-3-5-sonnet-20241022",
                        input_tokens=4_000,
                        output_tokens=10,
                        cache_creation_input_tokens=0,
                        cache_creation_input_tokens_5m=0,
                        cache_creation_input_tokens_1h=0,
                        cache_read_input_tokens=4_000,
                        cost_micros=7_200,
                        hit_miss="hit",
                        ts=(anchor_ms - offset_ms) // 1_000,
                    )
                )
    high_principal = PRINCIPALS["high_ui"]
    for index in range(120):
        offset_ms = (
            2 * 24 * 60 * 60 * 1_000
            if index < 60
            else 8 * 24 * 60 * 60 * 1_000
        ) + index * 1_000
        identifier = f"ui-history-{index:04d}"
        if index % 2 == 0:
            sessions.append(
                session(
                    high_principal,
                    identifier,
                    offset_ms,
                    refresh_count=index % 5,
                    status="terminal" if index % 4 == 0 else "active",
                    terminal_reason="expired" if index % 4 == 0 else None,
                )
            )
        else:
            decisions.append(
                decision(
                    high_principal,
                    identifier,
                    offset_ms,
                    nullable_timestamp=index % 11 == 0,
                )
            )

    d_principal = PRINCIPALS["d100k"]
    old_base_ms = anchor_ms - 8 * 24 * 60 * 60 * 1_000
    for index in range(d_count):
        message_ms = old_base_ms - index * 1_000
        decisions.append(
            DecisionRow(
                source_ref_id=f"old-decision-{index:06d}",
                decision="not_tracked",
                reason="historical decision",
                generation=1,
                ts=message_ms // 1_000,
                principal_id=d_principal,
                session_key_hash=None,
                upstream_id=UPSTREAM_ID,
                error=None,
                ttl="5m",
                config_snapshot=snapshot,
                last_message_at_ms=None if index % 101 == 0 else message_ms,
            )
        )
    sessions.append(session(d_principal, "perf-detail-session", 3_000_000, refresh_count=3))
    decisions.append(decision(d_principal, "perf-detail-decision", 3_001_000))
    sessions.append(session(d_principal, "perf-collision", 3_003_000, refresh_count=1))
    decisions.append(decision(d_principal, "perf-collision", 3_002_000))

    st_principal = PRINCIPALS["st_scale"]
    for index in range(st_sessions):
        identifier = f"st-session-{index:06d}"
        offset_ms = 3_600_000 + index * 1_000
        sessions.append(session(st_principal, identifier, offset_ms, refresh_count=turns_per_session))
        for turn_number in range(turns_per_session):
            turns.append(
                TurnRow(
                    source_ref_id=f"{identifier}:turn:{turn_number:03d}",
                    session_key_hash=identifier,
                    principal_id=st_principal,
                    accounting_key_id=None,
                    upstream_id=UPSTREAM_ID,
                    model="claude-3-5-sonnet-20241022",
                    input_tokens=2_000,
                    output_tokens=10,
                    cache_creation_input_tokens=0,
                    cache_creation_input_tokens_5m=0,
                    cache_creation_input_tokens_1h=0,
                    cache_read_input_tokens=2_000,
                    cost_micros=3_600,
                    hit_miss="hit" if turn_number + 1 < turns_per_session else "miss",
                    ts=(anchor_ms - offset_ms) // 1_000 + turn_number,
                )
            )
    return sessions, decisions, turns


def principal_rows(anchor_ms: int) -> list[dict[str, Any]]:
    rows = []
    for key, principal_id in PRINCIPALS.items():
        enabled = key != "disabled"
        config = dict(KEEPALIVE_CONFIG)
        config["enabled"] = enabled
        rows.append(
            {
                "id": principal_id,
                "name": PRINCIPAL_NAMES[key],
                "kind": "machine",
                "enabled": enabled,
                "allowed_models": ["claude-3-5-sonnet-20241022"],
                "allowed_upstreams": [UPSTREAM_ID],
                "default_limits": [],
                "router_terminal_strategy": "first-pick",
                "revision": 1,
                "created_at_ms": anchor_ms,
                "updated_at_ms": anchor_ms,
                "cache_keepalive": config,
            }
        )
    return rows


def assert_sqlite_target(path: Path) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    allowed_root = DEFAULT_SCRATCH.resolve()
    if resolved == allowed_root or allowed_root not in resolved.parents:
        raise HarnessError(f"SQLite target must be a file beneath {allowed_root}")
    if not resolved.exists():
        raise HarnessError("SQLite database must already exist and have current migrations applied by cc-lb")
    connection = sqlite3.connect(resolved, timeout=60)
    connection.execute("PRAGMA foreign_keys = ON")
    required = {
        "principals_v1",
        "upstream_spec_v1",
        "managed_keys_v1",
        "cache_keepalive_sessions",
        "cache_keepalive_decisions",
        "cache_keepalive_turns",
    }
    existing = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    missing = sorted(required - existing)
    if missing:
        connection.close()
        raise HarnessError(
            f"SQLite schema is not current; missing tables: {', '.join(missing)}"
        )
    return connection


def sqlite_session_values(row: SessionRow) -> tuple[Any, ...]:
    return (
        row.session_key_hash,
        row.principal_id,
        row.accounting_key_id,
        row.upstream_id,
        row.generation,
        row.refresh_count,
        row.first_scheduled_at,
        row.cache_anchor_at,
        row.run_at,
        row.ttl,
        row.status,
        row.enqueue_state,
        row.running_since_unix_secs,
        row.current_job_key,
        row.encrypted_payload,
        row.terminal_reason,
        row.expires_at,
        row.created_at,
        row.updated_at,
        row.display_reason,
        row.error,
        row.config_snapshot,
        row.last_message_at_ms,
    )




def sqlite_seed(args: argparse.Namespace, sessions: Sequence[SessionRow], decisions: Sequence[DecisionRow], turns: Sequence[TurnRow], principals: Sequence[dict[str, Any]]) -> None:
    connection = assert_sqlite_target(Path(args.sqlite_db))
    try:
        total_existing = sum(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "principals_v1",
                "upstream_spec_v1",
                "cache_keepalive_sessions",
                "cache_keepalive_decisions",
                "cache_keepalive_turns",
            )
        )
        if total_existing and not args.replace_fixture:
            raise HarnessError(
                "SQLite fixture target is not fresh; use a new isolated DB"
            )
        existing = connection.execute(
            f"SELECT COUNT(*) FROM principals_v1 WHERE id IN ({','.join('?' for _ in PRINCIPALS)})",
            tuple(PRINCIPALS.values()),
        ).fetchone()[0]
        if existing and not args.replace_fixture:
            raise HarnessError(
                "fixture principal IDs already exist; use a fresh DB or --replace-fixture"
            )
        connection.execute("BEGIN IMMEDIATE")
        if args.replace_fixture:
            for table, column in (("cache_keepalive_turns", "principal_id"), ("cache_keepalive_decisions", "principal_id"), ("cache_keepalive_sessions", "principal_id"), ("principals_v1", "id")):
                connection.execute(f"DELETE FROM {table} WHERE {column} IN ({','.join('?' for _ in PRINCIPALS)})", tuple(PRINCIPALS.values()))
            connection.execute("DELETE FROM upstream_spec_v1 WHERE id = ?", (UPSTREAM_ID,))
        now_s = args.anchor_ms // 1_000
        connection.execute(
            "INSERT INTO upstream_spec_v1 (id,name,kind,base_url,enabled,warmup_enabled,warmup_dialect_plugin,spec_revision,created_at,updated_at,deleted_at) VALUES (?,?,?,?,?,?,?,?,?,?,NULL)",
            (UPSTREAM_ID, "qa-loopback-upstream", "anthropic_api_key", args.fake_upstream_url, 1, 0, None, 1, now_s, now_s),
        )
        connection.executemany(
            "INSERT INTO principals_v1 (id,name,kind,enabled,allowed_models,allowed_upstreams,default_limits,router_terminal_strategy,revision,created_at,updated_at,deleted_at,cache_keepalive_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,?)",
            [
                (
                    row["id"], row["name"], row["kind"], int(row["enabled"]),
                    json.dumps(row["allowed_models"], separators=(",", ":")),
                    json.dumps(row["allowed_upstreams"], separators=(",", ":")),
                    "[]", row["router_terminal_strategy"], row["revision"], now_s, now_s,
                    json.dumps(row["cache_keepalive"], separators=(",", ":"), sort_keys=True),
                )
                for row in principals
            ],
        )
        connection.executemany(
            "INSERT INTO cache_keepalive_sessions (session_key_hash,principal_id,accounting_key_id,upstream_id,generation,refresh_count,first_scheduled_at,cache_anchor_at,run_at,ttl,status,enqueue_state,running_since_unix_secs,current_job_key,encrypted_payload,terminal_reason,expires_at,created_at,updated_at,display_reason,error,config_snapshot,last_message_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [sqlite_session_values(row) for row in sessions],
        )
        connection.executemany(
            "INSERT INTO cache_keepalive_decisions (source_ref_id,decision,reason,generation,ts,principal_id,session_key_hash,upstream_id,error,ttl,config_snapshot,last_message_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [tuple(asdict(row).values()) for row in decisions],
        )
        connection.executemany(
            "INSERT INTO cache_keepalive_turns (source_ref_id,session_key_hash,principal_id,accounting_key_id,upstream_id,model,input_tokens,output_tokens,cache_creation_input_tokens,cache_creation_input_tokens_5m,cache_creation_input_tokens_1h,cache_read_input_tokens,cost_micros,hit_miss,ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [tuple(asdict(row).values()) for row in turns],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def pg_command(container: str, database: str, sql: str, *, tuples_only: bool = True) -> subprocess.CompletedProcess[str]:
    if not SAFE_CONTAINER.fullmatch(container):
        raise HarnessError("unsafe PostgreSQL container name")
    if not SAFE_DB_NAME.fullmatch(database) or not QA_DB_NAME.search(database):
        raise HarnessError("PostgreSQL database name must be a QA/test/baseline/candidate name")
    command = ["docker", "exec", "-i", container, "psql", "-U", "cclb", "-d", database, "-v", "ON_ERROR_STOP=1"]
    if tuples_only:
        command.extend(["-A", "-t"])
    return subprocess.run(command, input=sql, text=True, capture_output=True, check=True)




def pg_quote(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bytes):
        return "decode('" + value.hex() + "','hex')"
    return "'" + str(value).replace("'", "''") + "'"


def insert_sql(table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]], *, chunk_size: int = 500) -> Iterator[str]:
    chunk: list[Sequence[Any]] = []
    for row in rows:
        chunk.append(row)
        if len(chunk) == chunk_size:
            yield f"INSERT INTO {table} ({','.join(columns)}) VALUES " + ",".join("(" + ",".join(pg_quote(value) for value in item) + ")" for item in chunk) + ";\n"
            chunk.clear()
    if chunk:
        yield f"INSERT INTO {table} ({','.join(columns)}) VALUES " + ",".join("(" + ",".join(pg_quote(value) for value in item) + ")" for item in chunk) + ";\n"


def postgres_seed(args: argparse.Namespace, sessions: Sequence[SessionRow], decisions: Sequence[DecisionRow], turns: Sequence[TurnRow], principals: Sequence[dict[str, Any]]) -> None:
    container, database = args.pg_container, args.pg_database
    table_check = pg_command(
        container,
        database,
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='public' "
        "AND table_name IN "
        "('principals_v1','upstream_spec_v1','managed_api_keys_v1',"
        "'cache_keepalive_sessions','cache_keepalive_decisions',"
        "'cache_keepalive_turns');",
    ).stdout.strip()
    if table_check != "6":
        raise HarnessError("PostgreSQL schema must already have current cc-lb migrations")
    ids = ",".join(pg_quote(value) + "::uuid" for value in PRINCIPALS.values())
    total_existing = pg_command(
        container,
        database,
        "SELECT (SELECT COUNT(*) FROM principals_v1)"
        "+(SELECT COUNT(*) FROM upstream_spec_v1)"
        "+(SELECT COUNT(*) FROM cache_keepalive_sessions)"
        "+(SELECT COUNT(*) FROM cache_keepalive_decisions)"
        "+(SELECT COUNT(*) FROM cache_keepalive_turns);",
    ).stdout.strip()
    if total_existing != "0" and not args.replace_fixture:
        raise HarnessError(
            "PostgreSQL fixture target is not fresh; use a new isolated DB"
        )
    existing = pg_command(container, database, f"SELECT COUNT(*) FROM principals_v1 WHERE id IN ({ids});").stdout.strip()
    if existing != "0" and not args.replace_fixture:
        raise HarnessError("fixture principal IDs already exist; use a fresh DB or --replace-fixture")
    now = datetime.fromtimestamp(args.anchor_ms / 1_000, timezone.utc).isoformat()
    statements = ["BEGIN;\n"]
    if args.replace_fixture:
        for table, column in (("cache_keepalive_turns", "principal_id"), ("cache_keepalive_decisions", "principal_id"), ("cache_keepalive_sessions", "principal_id")):
            statements.append(f"DELETE FROM {table} WHERE {column} IN ({','.join(pg_quote(value) for value in PRINCIPALS.values())});\n")
        statements.append(f"DELETE FROM principals_v1 WHERE id IN ({ids});\n")
        statements.append(f"DELETE FROM upstream_spec_v1 WHERE id={pg_quote(UPSTREAM_ID)}::uuid;\n")
    statements.append(
        "INSERT INTO upstream_spec_v1 (id,name,kind,base_url,enabled,warmup_enabled,warmup_dialect_plugin,spec_revision,created_at,updated_at,deleted_at) VALUES "
        f"({pg_quote(UPSTREAM_ID)}::uuid,'qa-loopback-upstream','anthropic_api_key',{pg_quote(args.fake_upstream_url)},TRUE,FALSE,NULL,1,{pg_quote(now)}::timestamptz,{pg_quote(now)}::timestamptz,NULL);\n"
    )
    principal_values = []
    for row in principals:
        principal_values.append(
            "(" + ",".join(
                [
                    pg_quote(row["id"]) + "::uuid", pg_quote(row["name"]), pg_quote(row["kind"]),
                    pg_quote(json.dumps(row["allowed_models"], separators=(",", ":"))) + "::jsonb",
                    "ARRAY[" + pg_quote(UPSTREAM_ID) + "::uuid]", "'[]'::jsonb", pg_quote(row["enabled"]),
                    "NULL", str(row["revision"]), pg_quote(now) + "::timestamptz",
                    pg_quote(now) + "::timestamptz", pg_quote(row["router_terminal_strategy"]),
                    pg_quote(json.dumps(row["cache_keepalive"], separators=(",", ":"), sort_keys=True)) + "::jsonb",
                ]
            ) + ")"
        )
    statements.append("INSERT INTO principals_v1 (id,name,kind,allowed_models,allowed_upstreams,default_limits,enabled,deleted_at,revision,created_at,updated_at,router_terminal_strategy,cache_keepalive) VALUES " + ",".join(principal_values) + ";\n")
    session_columns = list(SessionRow.__dataclass_fields__)
    decision_columns = list(DecisionRow.__dataclass_fields__)
    turn_columns = list(TurnRow.__dataclass_fields__)
    statements.extend(insert_sql("cache_keepalive_sessions", session_columns, (tuple(asdict(row).values()) for row in sessions)))
    statements.extend(insert_sql("cache_keepalive_decisions", decision_columns, (tuple(asdict(row).values()) for row in decisions)))
    statements.extend(insert_sql("cache_keepalive_turns", turn_columns, (tuple(asdict(row).values()) for row in turns)))
    statements.append("COMMIT;\n")
    pg_command(container, database, "".join(statements), tuples_only=False)


def prepare(args: argparse.Namespace) -> None:
    args.fake_upstream_url = require_loopback_url(args.fake_upstream_url, "fake upstream URL")
    if args.server_url:
        args.server_url = require_loopback_url(args.server_url, "server URL")
    if args.anchor_ms is None:
        args.anchor_ms = int(time.time() * 1_000)
    if args.anchor_ms < 1_000:
        raise HarnessError("--anchor-ms must be a positive Unix millisecond timestamp")
    if args.d_count < 100_000:
        raise HarnessError("--d-count must be at least 100000 for D-scale evidence")
    if args.ui_count < 1_001:
        raise HarnessError("--ui-count must be at least 1001 to prove more than 20 default-limit pages")
    if args.st_sessions < 1 or args.turns_per_session < 1:
        raise HarnessError("S/T scale requires positive session and turn counts")

    manifest_path = required_path(args.manifest, "fixture manifest path")
    if manifest_path.exists():
        manifest = load_json(manifest_path)
        if not isinstance(manifest.get("datasets"), dict):
            raise HarnessError(f"manifest has no datasets object: {manifest_path}")
        anchors = {
            dataset.get("anchor_ms")
            for dataset in manifest["datasets"].values()
            if isinstance(dataset, dict) and dataset
        }
        if anchors and anchors != {args.anchor_ms}:
            raise HarnessError(
                "all baseline/candidate SQLite/PostgreSQL datasets must use the same --anchor-ms"
            )
    else:
        manifest = {
            "fixture_version": FIXTURE_VERSION,
            "created_at_utc": utc_now(),
            "repo": str(ROOT),
            "principals": PRINCIPALS,
            "principal_names": PRINCIPAL_NAMES,
            "low_ids": LOW_IDS,
            "upstream_id": UPSTREAM_ID,
            "datasets": {},
        }

    sessions, decisions, turns = fixture_rows(args.anchor_ms, args.d_count, args.ui_count, args.st_sessions, args.turns_per_session)
    principals = principal_rows(args.anchor_ms)
    if args.engine == "sqlite":
        if not args.sqlite_db:
            raise HarnessError("--sqlite-db is required for SQLite")
        sqlite_seed(args, sessions, decisions, turns, principals)
        database = {"sqlite_db": str(Path(args.sqlite_db).resolve())}
    else:
        if not args.pg_database:
            raise HarnessError("--pg-database is required for PostgreSQL")
        postgres_seed(args, sessions, decisions, turns, principals)
        database = {"pg_container": args.pg_container, "pg_database": args.pg_database}

    manifest["fixture_version"] = FIXTURE_VERSION
    manifest["repo"] = str(ROOT)
    manifest["principals"] = PRINCIPALS
    manifest["principal_names"] = PRINCIPAL_NAMES
    manifest["low_ids"] = LOW_IDS
    manifest["upstream_id"] = UPSTREAM_ID
    dataset_key = f"{args.engine}:{args.phase}"
    manifest["datasets"][dataset_key] = {
        "engine": args.engine,
        "phase": args.phase,
        "server_url": args.server_url,
        "seeded_at_utc": utc_now(),
        "anchor_ms": args.anchor_ms,
        "clock": {
            "mode": "live_utc_requests",
            "fixture_anchor_ms": args.anchor_ms,
            "request_log_anchor": "started_at_utc",
        },
        "runtime_profile": args.runtime_profile,
        "database": database,
        "fixture_counts": {
            "principals": len(principals),
            "sessions": len(sessions),
            "decisions": len(decisions),
            "turns": len(turns),
            "d_old_decisions": args.d_count,
            "ui_entries": args.ui_count,
            "ui_history_entries": 120,
            "st_sessions": args.st_sessions,
            "turns_per_st_session": args.turns_per_session,
        },
        "expected_list_counts": {
            "high_ui": {"24h": args.ui_count, "7d": args.ui_count + 60, "all": args.ui_count + 120},
            "d100k": {"24h": args.ui_count + 4},
        },
        "lifecycle": expected_lifecycle(args.anchor_ms),
        "fixture_sha256": sha256_json({
            "anchor_ms": args.anchor_ms,
            "principals": principals,
            "sessions": [asdict(row) | {"encrypted_payload": sha256_bytes(row.encrypted_payload)} for row in sessions],
            "decisions": [asdict(row) for row in decisions],
            "turns": [asdict(row) for row in turns],
        }),
    }
    atomic_json(manifest_path, manifest)
    print(f"prepared {dataset_key}: {len(sessions)} sessions, {len(decisions)} decisions, {len(turns)} turns")


def http_measure(origin: str, path: str, token: str | None, timeout: float) -> tuple[int, dict[str, str], bytes, float, float, str, str]:
    origin = require_loopback_url(origin, "admin API URL")
    parsed = urllib.parse.urlsplit(origin)
    connection_class = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    started_utc = utc_now()
    started = time.perf_counter_ns()
    connection = connection_class(parsed.hostname, port, timeout=timeout)
    try:
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        first = response.read(1)
        ttfb = (time.perf_counter_ns() - started) / 1_000_000
        body = first + response.read()
        wall = (time.perf_counter_ns() - started) / 1_000_000
        response_headers = {key.lower(): value for key, value in response.getheaders()}
        return response.status, response_headers, body, ttfb, wall, started_utc, utc_now()
    finally:
        connection.close()


def decode_json(body: bytes) -> Any:
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {"_non_json_body_sha256": sha256_bytes(body), "_body_bytes": len(body)}


def api_record(*, case_id: str, phase: str, engine: str, origin: str, path: str, token: str | None, timeout: float, sample: int, cache_state: str, runtime_profile: str) -> tuple[dict[str, Any], Any]:
    status, headers, body, ttfb, wall, started_at, ended_at = http_measure(origin, path, token, timeout)
    value = decode_json(body)
    cursor_out = value.get("next_cursor") if isinstance(value, dict) else None
    cursor_metadata = cursor_evidence(cursor_out if isinstance(cursor_out, str) else None)
    display_url, cursor_in = redact_cursor_url(origin + path)
    record = {
        "case_id": case_id,
        "run_id": str(uuid.uuid4()),
        "phase": phase,
        "engine": engine,
        "sample": sample,
        "cache_state": cache_state,
        "runtime_profile": runtime_profile,
        "started_at_utc": started_at,
        "ended_at_utc": ended_at,
        "request": {
            "method": "GET",
            "exact_url_or_redacted_sha256": display_url,
            "cursor_in_sha256": cursor_in,
        },
        "response": {
            "http_status": status,
            "canonical_body_sha256": sha256_json(value),
            "body_bytes": len(body),
            "cursor_out_sha256": sha256_text(cursor_out if isinstance(cursor_out, str) else None),
            "cursor_out_original_fields_sha256": cursor_metadata["original_fields_sha256"],
            "cursor_out_horizon": cursor_metadata["horizon"],
            "ttfb_ms": round(ttfb, 3),
            "wall_ms": round(wall, 3),
            "retry_after": headers.get("retry-after"),
        },
        "ui": None,
        "backend": {"sql_calls": None, "sql_time_ms": None, "pool_wait_ms": None, "plan_artifact": None},
    }
    return record, value


def raw_request_pair(
    args: argparse.Namespace,
    case_id: str,
    baseline_path: str,
    candidate_path: str,
    sample: int,
    cache_state: str,
) -> tuple[dict[str, Any], dict[str, Any], Any, Any]:
    barrier = threading.Barrier(2)

    def measured(phase: str, origin: str, request_path: str) -> tuple[dict[str, Any], Any]:
        barrier.wait(timeout=args.timeout)
        return api_record(
            case_id=case_id,
            phase=phase,
            engine=args.engine,
            origin=origin,
            path=request_path,
            token=args.token,
            timeout=args.timeout,
            sample=sample,
            cache_state=cache_state,
            runtime_profile=args.runtime_profile,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        baseline_future = executor.submit(measured, "baseline", args.baseline_url, baseline_path)
        candidate_future = executor.submit(measured, "candidate", args.candidate_url, candidate_path)
        baseline_record, baseline_value = baseline_future.result()
        candidate_record, candidate_value = candidate_future.result()
    append_jsonl(Path(args.records), baseline_record)
    append_jsonl(Path(args.records), candidate_record)
    return baseline_record, candidate_record, baseline_value, candidate_value


def request_pair(
    args: argparse.Namespace,
    case_id: str,
    path: str,
    sample: int,
    cache_state: str,
    *,
    candidate_path: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], Any, Any]:
    candidate_path = candidate_path or path
    baseline_record, candidate_record, baseline_value, candidate_value = raw_request_pair(
        args,
        case_id,
        path,
        candidate_path,
        sample,
        cache_state,
    )
    baseline_normalized = normalized_response_for_comparison(
        baseline_value,
        phase="baseline",
        expected_horizon=expected_horizon_for_path(path),
    )
    candidate_normalized = normalized_response_for_comparison(
        candidate_value,
        phase="candidate",
        expected_horizon=expected_horizon_for_path(candidate_path),
    )
    if (
        baseline_record["response"]["http_status"]
        != candidate_record["response"]["http_status"]
        or canonical_json_bytes(baseline_normalized)
        != canonical_json_bytes(candidate_normalized)
    ):
        raise HarnessError(f"{case_id} response mismatch at sample {sample}; inspect raw records")
    return baseline_record, candidate_record, baseline_value, candidate_value


def summarize_timings(records: Sequence[dict[str, Any]]) -> dict[str, float]:
    values = [float(record["response"]["wall_ms"]) for record in records]
    return {
        "p50_ms": round(percentile(values, 0.50), 3),
        "p95_ms": round(percentile(values, 0.95), 3),
        "max_ms": round(max(values), 3),
        "mean_ms": round(statistics.fmean(values), 3),
    }


def list_path(principal: str, *, horizon: str = "24h", status: str = "all", cursor: str | None = None, limit: int | None = None) -> str:
    params: list[tuple[str, str]] = [("horizon", horizon)]
    if status == "error":
        params.append(("error", "true"))
    elif status != "all":
        params.append(("status", status))
    if cursor is not None:
        params.append(("cursor", cursor))
    if limit is not None:
        params.append(("limit", str(limit)))
    return f"/admin/v1/principals/{principal}/cache-keepalive?{urllib.parse.urlencode(params)}"


def assert_api_oracle(
    case_id: str,
    record: dict[str, Any],
    body: Any,
    expected_status: int,
    expected_error: str | None = None,
) -> None:
    actual_status = record["response"]["http_status"]
    if actual_status != expected_status:
        raise HarnessError(
            f"{case_id} expected HTTP {expected_status}, got {actual_status}"
        )
    if expected_error is not None:
        actual_error = body.get("error") if isinstance(body, dict) else None
        if actual_error != expected_error:
            raise HarnessError(
                f"{case_id} expected error {expected_error}, got {actual_error}"
            )


def compare(args: argparse.Namespace) -> None:
    args.records = str(required_path(args.records, "raw records path"))
    args.summary = str(required_path(args.summary, "comparison summary path"))
    if Path(args.records).resolve() == Path(args.summary).resolve():
        raise HarnessError("--records and --summary must name different files")
    args.baseline_url = require_loopback_url(args.baseline_url, "baseline URL")
    args.candidate_url = require_loopback_url(args.candidate_url, "candidate URL")
    args.token = read_secret(args.token_env or (None if args.token_file else DEFAULT_TOKEN_ENV), args.token_file)
    _, manifest = load_fixture_manifest(args.manifest)
    baseline_dataset = manifest.get("datasets", {}).get(f"{args.engine}:baseline")
    candidate_dataset = manifest.get("datasets", {}).get(f"{args.engine}:candidate")
    if not isinstance(baseline_dataset, dict) or not isinstance(candidate_dataset, dict):
        raise HarnessError(f"manifest lacks paired {args.engine} baseline/candidate datasets")
    if baseline_dataset.get("fixture_sha256") != candidate_dataset.get("fixture_sha256"):
        raise HarnessError("baseline and candidate fixture hashes differ")
    validate_dataset_lifecycle(baseline_dataset, f"{args.engine}:baseline")
    validate_dataset_lifecycle(candidate_dataset, f"{args.engine}:candidate")
    if baseline_dataset.get("expected_list_counts") != candidate_dataset.get("expected_list_counts"):
        raise HarnessError("baseline and candidate expected list counts differ")
    expected_clock = {
        "mode": "live_utc_requests",
        "fixture_anchor_ms": baseline_dataset.get("anchor_ms"),
        "request_log_anchor": "started_at_utc",
    }
    if baseline_dataset.get("clock") != expected_clock or candidate_dataset.get("clock") != expected_clock:
        raise HarnessError("manifest must separate the shared fixture anchor from live request log anchors")
    wanted = {item.strip().upper() for item in args.case.split(",") if item.strip()}
    allowed = {"ALL", "SEMANTIC", "FILTERS", "CURSOR", "PERF-01", "PERF-02", "PERF-03", "ST-SCALE"}
    unknown = wanted - allowed
    if not wanted or unknown:
        raise HarnessError(f"--case contains unsupported values: {sorted(unknown) if unknown else 'empty'}")
    if args.warm_samples < 30:
        raise HarnessError("--warm-samples must be at least 30")
    if "ALL" in wanted:
        wanted = {"SEMANTIC", "FILTERS", "CURSOR", "PERF-01", "PERF-02", "PERF-03", "ST-SCALE"}
    summary: dict[str, Any] = {
        "engine": args.engine,
        "runtime_profile": args.runtime_profile,
        "fixture_anchor_ms": baseline_dataset.get("anchor_ms"),
        "request_log_anchor": "started_at_utc",
        "cases": {},
    }

    if "SEMANTIC" in wanted:
        paths = [
            ("SEM-01", "/admin/v1/principals/not-a-uuid/cache-keepalive?limit=0"),
            ("SEM-02", "/admin/v1/principals/ffffffff-ffff-4fff-8fff-ffffffffffff/cache-keepalive?limit=0"),
            ("SEM-03-limit", f"/admin/v1/principals/{PRINCIPALS['low']}/cache-keepalive?limit=101"),
            ("SEM-03-horizon", f"/admin/v1/principals/{PRINCIPALS['low']}/cache-keepalive?horizon=2h"),
            ("SEM-03-status", f"/admin/v1/principals/{PRINCIPALS['low']}/cache-keepalive?status=warm"),
            ("SEM-05-session", f"/admin/v1/principals/{PRINCIPALS['low']}/cache-keepalive/{urllib.parse.quote(LOW_IDS['active'], safe='')}"),
            ("SEM-05-decision", f"/admin/v1/principals/{PRINCIPALS['low']}/cache-keepalive/{urllib.parse.quote(LOW_IDS['decision'], safe='')}"),
            ("SEM-06", f"/admin/v1/principals/{PRINCIPALS['low']}/cache-keepalive/{LOW_IDS['collision']}"),
            ("SEM-08-empty", f"/admin/v1/principals/{PRINCIPALS['empty']}/cache-keepalive?limit=0"),
            ("SEM-08-disabled", f"/admin/v1/principals/{PRINCIPALS['disabled']}/cache-keepalive?limit=0"),
            ("DETAIL-404", f"/admin/v1/principals/{PRINCIPALS['low']}/cache-keepalive/missing-entry"),
        ]
        expected = {
            "SEM-01": (400, "invalid_principal_id"),
            "SEM-02": (404, "unknown_principal"),
            "SEM-03-limit": (400, "invalid_cache_keepalive_limit"),
            "SEM-03-horizon": (400, "invalid_cache_keepalive_horizon"),
            "SEM-03-status": (400, "invalid_cache_keepalive_status"),
            "SEM-05-session": (200, None),
            "SEM-05-decision": (200, None),
            "SEM-06": (200, None),
            "SEM-08-empty": (200, None),
            "SEM-08-disabled": (200, None),
            "DETAIL-404": (404, "unknown_cache_keepalive_entry"),
        }
        for case_id, path in paths:
            baseline_record, _, baseline_body, _ = request_pair(
                args, case_id, path, 0, "fixed_snapshot"
            )
            assert_api_oracle(
                case_id,
                baseline_record,
                baseline_body,
                *expected[case_id],
            )
        _, _, baseline_page, candidate_page = request_pair(
            args,
            "SEM-04-cursor-seed",
            list_path(PRINCIPALS["low"], horizon="24h", status="all", limit=1),
            0,
            "fixed_snapshot",
        )
        baseline_cursor = baseline_page.get("next_cursor") if isinstance(baseline_page, dict) else None
        candidate_cursor = candidate_page.get("next_cursor") if isinstance(candidate_page, dict) else None
        if not isinstance(baseline_cursor, str) or not isinstance(candidate_cursor, str):
            raise HarnessError("SEM-04 could not obtain baseline and candidate cursors")
        baseline_original = normalize_cursor_for_comparison(
            baseline_cursor,
            phase="baseline",
            expected_horizon="24h",
        )
        candidate_original = normalize_cursor_for_comparison(
            candidate_cursor,
            phase="candidate",
            expected_horizon="24h",
        )
        if baseline_original != candidate_original:
            raise HarnessError("SEM-04 decoded original cursor fields differ")
        cursor_paths = [
            (
                "SEM-04-principal",
                list_path(PRINCIPALS["empty"], horizon="24h", cursor=baseline_cursor),
                list_path(PRINCIPALS["empty"], horizon="24h", cursor=candidate_cursor),
            ),
            (
                "SEM-04-horizon",
                list_path(PRINCIPALS["low"], horizon="7d", cursor=baseline_cursor),
                list_path(PRINCIPALS["low"], horizon="7d", cursor=candidate_cursor),
            ),
            (
                "SEM-04-filter",
                list_path(PRINCIPALS["low"], horizon="24h", status="renewed", cursor=baseline_cursor),
                list_path(PRINCIPALS["low"], horizon="24h", status="renewed", cursor=candidate_cursor),
            ),
        ]
        for case_id, baseline_path, candidate_path in cursor_paths:
            baseline_record, _, baseline_body, _ = request_pair(
                args,
                case_id,
                baseline_path,
                0,
                "fixed_snapshot",
                candidate_path=candidate_path,
            )
            assert_api_oracle(
                case_id,
                baseline_record,
                baseline_body,
                400,
                "invalid_input",
            )
        paths.extend((case_id, baseline_path) for case_id, baseline_path, _ in cursor_paths)
        summary["cases"]["SEMANTIC"] = {
            "requests": len(paths) + 1,
            "status": "equal",
            "baseline_original_cursor_sha256": sha256_text(baseline_cursor),
            "decoded_original_fields_sha256": sha256_json(baseline_original),
            "candidate_horizon_tag": "24h",
        }

    if "FILTERS" in wanted:
        count = 0
        for horizon in ("24h", "7d", "all"):
            for status in ("all", "renewed", "scheduled", "capped", "expired", "not_tracked", "error"):
                baseline_record, _, baseline_body, _ = request_pair(args, f"FLOW-08-{horizon}-{status}", list_path(PRINCIPALS["high_ui"], horizon=horizon, status=status), 0, "fixed_snapshot")
                assert_api_oracle(
                    f"FLOW-08-{horizon}-{status}",
                    baseline_record,
                    baseline_body,
                    200,
                )
                count += 1
        summary["cases"]["FILTERS"] = {"requests": count, "status": "equal"}

    if "CURSOR" in wanted:
        seeded: dict[str, tuple[str, str, dict[str, Any]]] = {}
        for horizon in ("24h", "7d", "all"):
            _, _, baseline_page, candidate_page = request_pair(
                args,
                f"CUR-01-{horizon}-seed",
                list_path(PRINCIPALS["high_ui"], horizon=horizon, limit=1),
                0,
                "cursor_seed",
            )
            baseline_cursor = baseline_page.get("next_cursor") if isinstance(baseline_page, dict) else None
            candidate_cursor = candidate_page.get("next_cursor") if isinstance(candidate_page, dict) else None
            if not isinstance(baseline_cursor, str) or not isinstance(candidate_cursor, str):
                raise HarnessError(f"CUR-01 {horizon} did not return paired cursors")
            baseline_original = normalize_cursor_for_comparison(
                baseline_cursor,
                phase="baseline",
                expected_horizon=horizon,
            )
            candidate_original = normalize_cursor_for_comparison(
                candidate_cursor,
                phase="candidate",
                expected_horizon=horizon,
            )
            if baseline_original != candidate_original:
                raise HarnessError(f"CUR-01 {horizon} decoded original cursor fields differ")
            seeded[horizon] = (baseline_cursor, candidate_cursor, candidate_original)

        all_baseline_cursor, all_candidate_cursor, _ = seeded["all"]
        baseline_record, candidate_record, baseline_body, candidate_body = request_pair(
            args,
            "CUR-02-valid-all",
            list_path(PRINCIPALS["high_ui"], horizon="all", cursor=all_baseline_cursor, limit=1),
            0,
            "cursor_scope",
            candidate_path=list_path(
                PRINCIPALS["high_ui"],
                horizon="all",
                cursor=all_candidate_cursor,
                limit=1,
            ),
        )
        assert_api_oracle("CUR-02-valid-all", baseline_record, baseline_body, 200)
        assert_api_oracle("CUR-02-valid-all", candidate_record, candidate_body, 200)

        baseline_24h_cursor, candidate_24h_cursor, _ = seeded["24h"]
        mismatch_paths = [
            (
                "CUR-02-principal",
                list_path(PRINCIPALS["empty"], horizon="24h", cursor=baseline_24h_cursor),
                list_path(PRINCIPALS["empty"], horizon="24h", cursor=candidate_24h_cursor),
            ),
            (
                "CUR-02-horizon",
                list_path(PRINCIPALS["high_ui"], horizon="7d", cursor=baseline_24h_cursor),
                list_path(PRINCIPALS["high_ui"], horizon="7d", cursor=candidate_24h_cursor),
            ),
            (
                "CUR-02-filter",
                list_path(
                    PRINCIPALS["high_ui"],
                    horizon="24h",
                    status="renewed",
                    cursor=baseline_24h_cursor,
                ),
                list_path(
                    PRINCIPALS["high_ui"],
                    horizon="24h",
                    status="renewed",
                    cursor=candidate_24h_cursor,
                ),
            ),
        ]
        for case_id, baseline_path, candidate_path in mismatch_paths:
            baseline_record, candidate_record, baseline_body, candidate_body = request_pair(
                args,
                case_id,
                baseline_path,
                0,
                "cursor_scope",
                candidate_path=candidate_path,
            )
            assert_api_oracle(case_id, baseline_record, baseline_body, 400, "invalid_input")
            assert_api_oracle(case_id, candidate_record, candidate_body, 400, "invalid_input")

        time_advance: dict[str, Any] = {}
        advanced_candidate_cursor: str | None = None
        for horizon in ("24h", "7d"):
            baseline_cursor, candidate_cursor, original = seeded[horizon]
            wait_past_cursor_request_second(candidate_cursor, horizon)
            baseline_record, candidate_record, baseline_body, candidate_body = raw_request_pair(
                args,
                f"CUR-01-{horizon}-advance",
                list_path(PRINCIPALS["high_ui"], horizon=horizon, cursor=baseline_cursor, limit=1),
                list_path(PRINCIPALS["high_ui"], horizon=horizon, cursor=candidate_cursor, limit=1),
                1,
                "time_advanced_cursor",
            )
            assert_api_oracle(
                f"CUR-01-{horizon}-baseline",
                baseline_record,
                baseline_body,
                400,
                "invalid_input",
            )
            assert_api_oracle(
                f"CUR-01-{horizon}-candidate",
                candidate_record,
                candidate_body,
                200,
            )
            next_cursor = candidate_body.get("next_cursor") if isinstance(candidate_body, dict) else None
            if not isinstance(next_cursor, str):
                raise HarnessError(f"CUR-01 {horizon} candidate page 2 has no cursor")
            next_original = normalize_cursor_for_comparison(
                next_cursor,
                phase="candidate",
                expected_horizon=horizon,
            )
            if next_original["horizon_start_ms"] != original["horizon_start_ms"]:
                raise HarnessError(f"CUR-01 {horizon} candidate changed the cursor anchor")
            time_advance[horizon] = {
                "baseline_status": 400,
                "candidate_status": 200,
                "accepted_baseline_bug_delta": True,
                "frozen_horizon_start_ms": original["horizon_start_ms"],
            }
            if horizon == "24h":
                advanced_candidate_cursor = next_cursor

        if advanced_candidate_cursor is None:
            raise HarnessError("CUR-03 lacks a 24h candidate cursor")
        frozen_start = seeded["24h"][2]["horizon_start_ms"]
        chain_pages = 2
        for page in range(3):
            wait_past_cursor_request_second(advanced_candidate_cursor, "24h")
            candidate_record, candidate_body = api_record(
                case_id="CUR-03",
                phase="candidate",
                engine=args.engine,
                origin=args.candidate_url,
                path=list_path(
                    PRINCIPALS["high_ui"],
                    horizon="24h",
                    cursor=advanced_candidate_cursor,
                    limit=1,
                ),
                token=args.token,
                timeout=args.timeout,
                sample=page,
                cache_state="repeated_time_advance",
                runtime_profile=args.runtime_profile,
            )
            append_jsonl(Path(args.records), candidate_record)
            assert_api_oracle("CUR-03", candidate_record, candidate_body, 200)
            next_cursor = candidate_body.get("next_cursor") if isinstance(candidate_body, dict) else None
            if not isinstance(next_cursor, str):
                raise HarnessError("CUR-03 candidate chain terminated before repeated advances")
            next_original = normalize_cursor_for_comparison(
                next_cursor,
                phase="candidate",
                expected_horizon="24h",
            )
            if next_original["horizon_start_ms"] != frozen_start:
                raise HarnessError("CUR-03 candidate cursor anchor changed across the page chain")
            advanced_candidate_cursor = next_cursor
            chain_pages += 1
        summary["cases"]["CURSOR"] = {
            "decoded_original_fields": "exact",
            "candidate_horizon_tags": ["24h", "7d", "all"],
            "time_advance": time_advance,
            "candidate_anchor_chain_pages": chain_pages,
            "baseline_original_cursor_sha256": sha256_text(seeded["24h"][0]),
        }

    if "PERF-01" in wanted:
        baseline_records, candidate_records = [], []
        path = f"/admin/v1/principals/{PRINCIPALS['d100k']}/cache-keepalive?limit=0"
        for sample in range(args.warm_samples + 1):
            baseline, candidate, baseline_body, _ = request_pair(args, "PERF-01", path, sample, "cold" if sample == 0 else "warm")
            assert_api_oracle("PERF-01", baseline, baseline_body, 200)
            if sample:
                baseline_records.append(baseline)
                candidate_records.append(candidate)
        summary["cases"]["PERF-01"] = {"baseline": summarize_timings(baseline_records), "candidate": summarize_timings(candidate_records), "body_equal": True}

    if "PERF-02" in wanted:
        baseline_cursor: str | None = None
        candidate_cursor: str | None = None
        page = 0
        candidate_ids: list[str] = []
        first_record: tuple[dict[str, Any], dict[str, Any]] | None = None
        middle_record: dict[str, Any] | None = None
        terminal_record: dict[str, Any] | None = None
        baseline_bug_delta: dict[str, Any] | None = None
        baseline_active = True
        while True:
            baseline_path = list_path(
                PRINCIPALS["d100k"],
                horizon="24h",
                status="all",
                cursor=baseline_cursor,
            )
            candidate_path = list_path(
                PRINCIPALS["d100k"],
                horizon="24h",
                status="all",
                cursor=candidate_cursor,
            )
            if baseline_active:
                baseline, candidate, baseline_value, candidate_value = raw_request_pair(
                    args,
                    "PERF-02",
                    baseline_path,
                    candidate_path,
                    page,
                    "warm",
                )
                if page == 0:
                    baseline_normalized = normalized_response_for_comparison(
                        baseline_value,
                        phase="baseline",
                        expected_horizon="24h",
                    )
                    candidate_normalized = normalized_response_for_comparison(
                        candidate_value,
                        phase="candidate",
                        expected_horizon="24h",
                    )
                    if canonical_json_bytes(baseline_normalized) != canonical_json_bytes(
                        candidate_normalized
                    ):
                        raise HarnessError("PERF-02 first-page response mismatch")
                    assert_api_oracle("PERF-02", baseline, baseline_value, 200)
                    assert_api_oracle("PERF-02", candidate, candidate_value, 200)
                    first_record = (baseline, candidate)
                elif baseline["response"]["http_status"] == 400:
                    assert_api_oracle(
                        "PERF-02-baseline-time-advance",
                        baseline,
                        baseline_value,
                        400,
                        "invalid_input",
                    )
                    assert_api_oracle("PERF-02-candidate", candidate, candidate_value, 200)
                    baseline_bug_delta = {
                        "page": page + 1,
                        "baseline_cursor_sha256": sha256_text(baseline_cursor),
                        "baseline_status": 400,
                        "candidate_status": 200,
                        "accepted_existing_cursor_bug": True,
                    }
                    baseline_active = False
                else:
                    baseline_normalized = normalized_response_for_comparison(
                        baseline_value,
                        phase="baseline",
                        expected_horizon="24h",
                    )
                    candidate_normalized = normalized_response_for_comparison(
                        candidate_value,
                        phase="candidate",
                        expected_horizon="24h",
                    )
                    if canonical_json_bytes(baseline_normalized) != canonical_json_bytes(
                        candidate_normalized
                    ):
                        raise HarnessError(f"PERF-02 page {page + 1} response mismatch")
                    assert_api_oracle("PERF-02", baseline, baseline_value, 200)
                    assert_api_oracle("PERF-02", candidate, candidate_value, 200)
            else:
                candidate, candidate_value = api_record(
                    case_id="PERF-02",
                    phase="candidate",
                    engine=args.engine,
                    origin=args.candidate_url,
                    path=candidate_path,
                    token=args.token,
                    timeout=args.timeout,
                    sample=page,
                    cache_state="warm",
                    runtime_profile=args.runtime_profile,
                )
                append_jsonl(Path(args.records), candidate)
                assert_api_oracle("PERF-02", candidate, candidate_value, 200)

            candidate_rows = candidate_value.get("rows", []) if isinstance(candidate_value, dict) else []
            candidate_ids.extend(str(row.get("id")) for row in candidate_rows)
            next_candidate_cursor = (
                candidate_value.get("next_cursor") if isinstance(candidate_value, dict) else None
            )
            if page == 12:
                middle_record = candidate
            if next_candidate_cursor is None:
                terminal_record = candidate
                break
            if not isinstance(next_candidate_cursor, str) or not next_candidate_cursor:
                raise HarnessError("PERF-02 candidate returned a non-string non-null cursor")
            normalize_cursor_for_comparison(
                next_candidate_cursor,
                phase="candidate",
                expected_horizon="24h",
            )
            candidate_cursor = next_candidate_cursor
            if baseline_active:
                next_baseline_cursor = (
                    baseline_value.get("next_cursor") if isinstance(baseline_value, dict) else None
                )
                if not isinstance(next_baseline_cursor, str) or not next_baseline_cursor:
                    raise HarnessError("PERF-02 baseline ended before candidate")
                baseline_cursor = next_baseline_cursor
            page += 1
        if page + 1 <= 20:
            raise HarnessError("PERF-02 fixture did not require more than 20 pages")
        expected_count = candidate_dataset.get("expected_list_counts", {}).get("d100k", {}).get("24h")
        if len(candidate_ids) != expected_count:
            raise HarnessError(
                f"PERF-02 candidate returned {len(candidate_ids)} rows, expected {expected_count}"
            )
        if baseline_bug_delta is None:
            raise HarnessError("PERF-02 did not reproduce the accepted baseline time-advance cursor bug")
        summary["cases"]["PERF-02"] = {
            "page_count": page + 1,
            "ordered_ids_sha256": sha256_json(candidate_ids),
            "expected_row_count": expected_count,
            "first": {"baseline_ms": first_record[0]["response"]["wall_ms"], "candidate_ms": first_record[1]["response"]["wall_ms"]} if first_record else None,
            "middle": {"candidate_ms": middle_record["response"]["wall_ms"]} if middle_record else None,
            "terminal": {"candidate_ms": terminal_record["response"]["wall_ms"]} if terminal_record else None,
            "baseline_bug_delta": baseline_bug_delta,
        }

    if "PERF-03" in wanted:
        targets = ["perf-detail-session", "perf-detail-decision", "perf-collision", "missing-detail"]
        target_summary: dict[str, Any] = {}
        for target in targets:
            baseline_records, candidate_records = [], []
            path = f"/admin/v1/principals/{PRINCIPALS['d100k']}/cache-keepalive/{urllib.parse.quote(target, safe='')}"
            for sample in range(args.warm_samples):
                baseline, candidate, baseline_body, _ = request_pair(args, "PERF-03", path, sample, "warm")
                assert_api_oracle(
                    "PERF-03",
                    baseline,
                    baseline_body,
                    404 if target == "missing-detail" else 200,
                )
                baseline_records.append(baseline)
                candidate_records.append(candidate)
            target_summary[target] = {"baseline": summarize_timings(baseline_records), "candidate": summarize_timings(candidate_records)}
        summary["cases"]["PERF-03"] = target_summary

    if "ST-SCALE" in wanted:
        baseline_records, candidate_records = [], []
        path = f"/admin/v1/principals/{PRINCIPALS['st_scale']}/cache-keepalive?limit=0"
        for sample in range(args.warm_samples):
            baseline, candidate, baseline_body, _ = request_pair(args, "ST-SCALE", path, sample, "warm")
            assert_api_oracle("ST-SCALE", baseline, baseline_body, 200)
            baseline_records.append(baseline)
            candidate_records.append(candidate)
        summary["cases"]["ST-SCALE"] = {"baseline": summarize_timings(baseline_records), "candidate": summarize_timings(candidate_records), "scale": "sessions_and_turns_only"}

    summary["completed_at_utc"] = utc_now()
    summary["raw_records"] = str(Path(args.records).resolve())
    output = Path(args.summary)
    atomic_json(output, summary)
    print(f"comparison complete: {output}")


def dataset_from_manifest(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    _, manifest = load_fixture_manifest(args.manifest)
    key = f"{args.engine}:{args.phase}"
    try:
        dataset = manifest["datasets"][key]
    except (KeyError, TypeError) as error:
        raise HarnessError(f"manifest has no dataset {key}") from error
    if not isinstance(dataset, dict) or not dataset:
        raise HarnessError(f"manifest dataset {key} is empty")
    validate_dataset_lifecycle(dataset, key)
    return manifest, dataset


def mutate_sqlite(path: Path, mutation: str, anchor_ms: int) -> dict[str, Any]:
    connection = assert_sqlite_target(path)
    changed = 0
    try:
        connection.execute("BEGIN IMMEDIATE")
        if mutation == "bump-summary":
            changed = connection.execute(
                "UPDATE cache_keepalive_sessions SET refresh_count=refresh_count+1,updated_at=? WHERE principal_id=? AND session_key_hash=?",
                (anchor_ms // 1_000, PRINCIPALS["high_ui"], "ui-000000"),
            ).rowcount
        elif mutation == "reorder":
            pair = connection.execute(
                "SELECT session_key_hash,last_message_at_ms FROM cache_keepalive_sessions WHERE principal_id=? AND session_key_hash IN ('ui-000000','ui-000001')",
                (PRINCIPALS["high_ui"],),
            ).fetchall()
            timestamps = {identifier: timestamp for identifier, timestamp in pair}
            if set(timestamps) != {"ui-000000", "ui-000001"}:
                raise HarnessError("reorder fixture rows are missing")
            changed += connection.execute(
                "UPDATE cache_keepalive_sessions SET last_message_at_ms=? WHERE principal_id=? AND session_key_hash='ui-000000'",
                (timestamps["ui-000001"], PRINCIPALS["high_ui"]),
            ).rowcount
            changed += connection.execute(
                "UPDATE cache_keepalive_sessions SET last_message_at_ms=? WHERE principal_id=? AND session_key_hash='ui-000001'",
                (timestamps["ui-000000"], PRINCIPALS["high_ui"]),
            ).rowcount
        elif mutation == "late-turn":
            session_row = fixture_rows(anchor_ms, 0, 0, 0, 0)[0][0]
            transition = SessionRow(**(asdict(session_row) | {"session_key_hash": LOW_IDS["transition_session"], "principal_id": PRINCIPALS["low"], "current_job_key": f"cache_keepalive:{LOW_IDS['transition_session']}:1", "last_message_at_ms": anchor_ms + 90_000, "created_at": (anchor_ms + 90_000) // 1_000, "updated_at": anchor_ms // 1_000}))
            connection.execute("INSERT OR IGNORE INTO cache_keepalive_sessions (session_key_hash,principal_id,accounting_key_id,upstream_id,generation,refresh_count,first_scheduled_at,cache_anchor_at,run_at,ttl,status,enqueue_state,running_since_unix_secs,current_job_key,encrypted_payload,terminal_reason,expires_at,created_at,updated_at,display_reason,error,config_snapshot,last_message_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", sqlite_session_values(transition))
            changed += connection.execute("INSERT OR IGNORE INTO cache_keepalive_turns (source_ref_id,session_key_hash,principal_id,accounting_key_id,upstream_id,model,input_tokens,output_tokens,cache_creation_input_tokens,cache_creation_input_tokens_5m,cache_creation_input_tokens_1h,cache_read_input_tokens,cost_micros,hit_miss,ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (LOW_IDS["transition_decision"], LOW_IDS["transition_session"], PRINCIPALS["low"], None, UPSTREAM_ID, "claude-3-5-sonnet-20241022", 1000, 10, 0, 0, 0, 1000, 1800, "hit", (anchor_ms + 90_000) // 1_000)).rowcount
        elif mutation == "reactivate":
            changed = connection.execute("UPDATE cache_keepalive_sessions SET generation=generation+1,refresh_count=0,status='active',terminal_reason=NULL,error=NULL,enqueue_state='enqueued',run_at=?,expires_at=?,last_message_at_ms=?,updated_at=? WHERE principal_id=? AND session_key_hash=?", (anchor_ms // 1_000 + FIXTURE_LIFETIME_SECONDS - 300, anchor_ms // 1_000 + FIXTURE_LIFETIME_SECONDS, anchor_ms + 100_000, anchor_ms // 1_000, PRINCIPALS["low"], LOW_IDS["terminal"])).rowcount
        elif mutation == "cleanup":
            changed = connection.execute("DELETE FROM cache_keepalive_sessions WHERE principal_id=? AND session_key_hash=?", (PRINCIPALS["low"], LOW_IDS["cleanup"])).rowcount
            if changed != 1:
                raise HarnessError("cleanup mutation must delete exactly one fixture session")
            remaining = connection.execute(
                "SELECT COUNT(*) FROM cache_keepalive_sessions WHERE principal_id=? AND session_key_hash=?",
                (PRINCIPALS["low"], LOW_IDS["cleanup"]),
            ).fetchone()[0]
            if remaining != 0:
                raise HarnessError("cleanup mutation did not remove its fixture session")
        else:
            raise HarnessError(f"unknown mutation: {mutation}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {"mutation": mutation, "changed_rows": changed}


def mutate_postgres(container: str, database: str, mutation: str, anchor_ms: int) -> dict[str, Any]:
    if mutation == "bump-summary":
        sql = f"UPDATE cache_keepalive_sessions SET refresh_count=refresh_count+1,updated_at={anchor_ms // 1_000} WHERE principal_id={pg_quote(PRINCIPALS['high_ui'])} AND session_key_hash='ui-000000';"
    elif mutation == "reorder":
        sql = f"""WITH current_values AS (
            SELECT session_key_hash,last_message_at_ms
            FROM cache_keepalive_sessions
            WHERE principal_id={pg_quote(PRINCIPALS['high_ui'])}
              AND session_key_hash IN ('ui-000000','ui-000001')
        )
        UPDATE cache_keepalive_sessions target
        SET last_message_at_ms = CASE target.session_key_hash
            WHEN 'ui-000000' THEN (SELECT last_message_at_ms FROM current_values WHERE session_key_hash='ui-000001')
            WHEN 'ui-000001' THEN (SELECT last_message_at_ms FROM current_values WHERE session_key_hash='ui-000000')
        END
        WHERE target.principal_id={pg_quote(PRINCIPALS['high_ui'])}
          AND target.session_key_hash IN ('ui-000000','ui-000001');"""
    elif mutation == "late-turn":
        template = fixture_rows(anchor_ms, 0, 0, 0, 0)[0][0]
        transition = SessionRow(**(asdict(template) | {"session_key_hash": LOW_IDS["transition_session"], "principal_id": PRINCIPALS["low"], "current_job_key": f"cache_keepalive:{LOW_IDS['transition_session']}:1", "last_message_at_ms": anchor_ms + 90_000, "created_at": (anchor_ms + 90_000) // 1_000, "updated_at": anchor_ms // 1_000}))
        sql = next(insert_sql("cache_keepalive_sessions", list(SessionRow.__dataclass_fields__), [tuple(asdict(transition).values())])).replace("INSERT INTO", "INSERT INTO", 1).rstrip(";\n") + " ON CONFLICT DO NOTHING;"
        sql += f" INSERT INTO cache_keepalive_turns (source_ref_id,session_key_hash,principal_id,accounting_key_id,upstream_id,model,input_tokens,output_tokens,cache_creation_input_tokens,cache_creation_input_tokens_5m,cache_creation_input_tokens_1h,cache_read_input_tokens,cost_micros,hit_miss,ts) VALUES ({pg_quote(LOW_IDS['transition_decision'])},{pg_quote(LOW_IDS['transition_session'])},{pg_quote(PRINCIPALS['low'])},NULL,{pg_quote(UPSTREAM_ID)}::uuid,'claude-3-5-sonnet-20241022',1000,10,0,0,0,1000,1800,'hit',{(anchor_ms + 90_000) // 1_000}) ON CONFLICT DO NOTHING;"
    elif mutation == "reactivate":
        sql = f"UPDATE cache_keepalive_sessions SET generation=generation+1,refresh_count=0,status='active',terminal_reason=NULL,error=NULL,enqueue_state='enqueued',run_at={anchor_ms // 1_000 + FIXTURE_LIFETIME_SECONDS - 300},expires_at={anchor_ms // 1_000 + FIXTURE_LIFETIME_SECONDS},last_message_at_ms={anchor_ms + 100_000},updated_at={anchor_ms // 1_000} WHERE principal_id={pg_quote(PRINCIPALS['low'])} AND session_key_hash={pg_quote(LOW_IDS['terminal'])};"
    elif mutation == "cleanup":
        sql = f"WITH deleted AS (DELETE FROM cache_keepalive_sessions WHERE principal_id={pg_quote(PRINCIPALS['low'])} AND session_key_hash={pg_quote(LOW_IDS['cleanup'])} RETURNING 1) SELECT COUNT(*) FROM deleted;"
        completed = pg_command(container, database, sql)
        if completed.stdout.strip() != "1":
            raise HarnessError("cleanup mutation must delete exactly one fixture session")
        return {"mutation": mutation, "changed_rows": 1, "command_output_sha256": sha256_text(completed.stdout)}
    else:
        raise HarnessError(f"unknown mutation: {mutation}")
    completed = pg_command(container, database, sql + " SELECT 1;", tuples_only=False)
    return {"mutation": mutation, "changed_rows": None, "command_output_sha256": sha256_text(completed.stdout)}


def capture_plans(args: argparse.Namespace, dataset: dict[str, Any], output_dir: Path) -> list[str]:
    principal = PRINCIPALS["d100k"]
    cutoff = int(dataset["anchor_ms"]) - 300_000
    horizon_start = int(dataset["anchor_ms"]) - 86_400_000
    cursor_ms = int(dataset["anchor_ms"]) - 4_100_000
    queries = {
        "summary_sessions": f"SELECT session_key_hash,last_message_at_ms FROM cache_keepalive_sessions WHERE principal_id='{principal}' ORDER BY last_message_at_ms DESC,'session:' || session_key_hash ASC",
        "summary_decisions": f"SELECT COUNT(*) FROM cache_keepalive_decisions d WHERE principal_id='{principal}' AND COALESCE(last_message_at_ms,ts*1000)>={cutoff} AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns t WHERE t.source_ref_id=d.source_ref_id)",
        "list_sessions_first": f"SELECT session_key_hash,last_message_at_ms FROM cache_keepalive_sessions WHERE principal_id='{principal}' AND last_message_at_ms>={horizon_start} ORDER BY last_message_at_ms DESC,'session:' || session_key_hash ASC LIMIT 51",
        "list_decisions_first": f"SELECT source_ref_id,COALESCE(last_message_at_ms,ts*1000) AS effective_ms FROM cache_keepalive_decisions d WHERE principal_id='{principal}' AND COALESCE(last_message_at_ms,ts*1000)>={horizon_start} AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns t WHERE t.source_ref_id=d.source_ref_id) ORDER BY effective_ms DESC,'decision:' || source_ref_id ASC LIMIT 51",
        "list_sessions_cursor": f"SELECT session_key_hash,last_message_at_ms FROM cache_keepalive_sessions WHERE principal_id='{principal}' AND last_message_at_ms>={horizon_start} AND (last_message_at_ms<{cursor_ms} OR (last_message_at_ms={cursor_ms} AND 'session:' || session_key_hash>'session:perf-000500')) ORDER BY last_message_at_ms DESC,'session:' || session_key_hash ASC LIMIT 51",
        "list_decisions_cursor": f"SELECT source_ref_id,COALESCE(last_message_at_ms,ts*1000) AS effective_ms FROM cache_keepalive_decisions d WHERE principal_id='{principal}' AND COALESCE(last_message_at_ms,ts*1000)>={horizon_start} AND (COALESCE(last_message_at_ms,ts*1000)<{cursor_ms} OR (COALESCE(last_message_at_ms,ts*1000)={cursor_ms} AND 'decision:' || source_ref_id>'decision:perf-000500')) AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns t WHERE t.source_ref_id=d.source_ref_id) ORDER BY effective_ms DESC,'decision:' || source_ref_id ASC LIMIT 51",
        "detail_session": f"SELECT session_key_hash,last_message_at_ms FROM cache_keepalive_sessions WHERE principal_id='{principal}' AND session_key_hash='perf-detail-session'",
        "detail_decision": f"SELECT source_ref_id,COALESCE(last_message_at_ms,ts*1000) FROM cache_keepalive_decisions d WHERE principal_id='{principal}' AND source_ref_id='perf-detail-decision' AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns t WHERE t.source_ref_id=d.source_ref_id)",
    }
    artifacts: list[str] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, query in queries.items():
        path = output_dir / f"{args.engine}-{args.phase}-{name}.json"
        if path.exists():
            raise HarnessError(f"refusing to overwrite plan artifact {path}")
        if args.engine == "sqlite":
            connection = assert_sqlite_target(Path(dataset["database"]["sqlite_db"]))
            try:
                rows = connection.execute("EXPLAIN QUERY PLAN " + query).fetchall()
            finally:
                connection.close()
            payload = {"captured_at_utc": utc_now(), "engine": args.engine, "phase": args.phase, "query_sha256": sha256_text(query), "plan": rows}
        else:
            result = pg_command(dataset["database"]["pg_container"], dataset["database"]["pg_database"], "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query + ";")
            payload = {"captured_at_utc": utc_now(), "engine": args.engine, "phase": args.phase, "query_sha256": sha256_text(query), "plan": json.loads(result.stdout)}
        atomic_json(path, payload)
        artifacts.append(str(path))
    return artifacts


def validate_records(paths: Sequence[Path]) -> dict[str, Any]:
    if not paths:
        raise HarnessError("at least one --jsonl evidence file is required")
    required = {"case_id", "run_id", "phase", "engine", "sample", "cache_state", "started_at_utc", "ended_at_utc", "request", "response", "runtime_profile"}
    counts: dict[str, int] = {}
    seen_ids: set[str] = set()
    total = 0
    for path in paths:
        if not path.is_file():
            raise HarnessError(f"evidence JSONL does not exist: {path}")
        file_records = 0
        with path.open() as source:
            for line_number, line in enumerate(source, 1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as error:
                    raise HarnessError(f"invalid JSONL {path}:{line_number}") from error
                missing = required - set(record)
                if missing:
                    raise HarnessError(f"record {path}:{line_number} missing {sorted(missing)}")
                run_id = record["run_id"]
                if not isinstance(run_id, str) or not run_id:
                    raise HarnessError(f"record {path}:{line_number} has an empty run_id")
                if run_id in seen_ids:
                    raise HarnessError(f"duplicate run_id {run_id}")
                seen_ids.add(run_id)
                case_id = record["case_id"]
                if not isinstance(case_id, str) or not case_id:
                    raise HarnessError(f"record {path}:{line_number} has an empty case_id")
                counts[case_id] = counts.get(case_id, 0) + 1
                total += 1
                file_records += 1
        if file_records == 0:
            raise HarnessError(f"evidence JSONL is empty: {path}")
    return {"record_count": total, "case_counts": counts, "source_sha256": {str(path): sha256_bytes(path.read_bytes()) for path in paths}}

def sanitize_trace(trace_path: Path, token: str | None) -> None:
    resolved = trace_path.resolve()
    if resolved == DEFAULT_SCRATCH.resolve() or DEFAULT_SCRATCH.resolve() not in resolved.parents:
        raise HarnessError(f"trace must be a file beneath {DEFAULT_SCRATCH}")
    if not resolved.is_file():
        raise HarnessError(f"trace does not exist or is not a file: {resolved}")
    if token is None:
        raise HarnessError(
            "trace redaction verification unavailable: no admin token was provided"
        )
    if not token:
        raise HarnessError("trace redaction token must not be empty")
    secret = token.encode()
    temporary = resolved.with_suffix(f".{uuid.uuid4().hex}.redacted.zip")
    with zipfile.ZipFile(resolved, "r") as source, zipfile.ZipFile(
        temporary, "w"
    ) as target:
        for info in source.infolist():
            target.writestr(info, source.read(info.filename).replace(secret, b"[REDACTED]"))
    with zipfile.ZipFile(temporary, "r") as sanitized:
        if any(secret in sanitized.read(info.filename) for info in sanitized.infolist()):
            temporary.unlink(missing_ok=True)
            raise HarnessError("trace redaction verification failed")
    os.chmod(temporary, 0o600)
    temporary.replace(resolved)




def evidence(args: argparse.Namespace) -> None:
    manifest_path, dataset = dataset_from_manifest(args)
    if args.sanitize_trace:
        trace_path = required_path(args.sanitize_trace, "trace path")
        token = read_secret(
            args.token_env or (None if args.token_file else DEFAULT_TOKEN_ENV),
            args.token_file,
        )
        sanitize_trace(trace_path, token)
        print(f"sanitized trace: {trace_path.resolve()}")
        return
    if args.mutation:
        records_path = required_path(args.records, "mutation records path")
        started = utc_now()
        if args.engine == "sqlite":
            result = mutate_sqlite(Path(dataset["database"]["sqlite_db"]), args.mutation, int(dataset["anchor_ms"]))
        else:
            result = mutate_postgres(dataset["database"]["pg_container"], dataset["database"]["pg_database"], args.mutation, int(dataset["anchor_ms"]))
        record = {
            "case_id": args.case_id or f"MUTATION-{args.mutation}",
            "run_id": str(uuid.uuid4()),
            "phase": args.phase,
            "engine": args.engine,
            "sample": None,
            "cache_state": "fixture_mutation",
            "runtime_profile": dataset["runtime_profile"],
            "started_at_utc": started,
            "ended_at_utc": utc_now(),
            "request": {"method": "LOCAL_FIXTURE_MUTATION", "exact_url_or_redacted_sha256": None, "cursor_in_sha256": None},
            "response": {"http_status": None, "canonical_body_sha256": sha256_json(result), "body_bytes": len(canonical_json_bytes(result)), "cursor_out_sha256": None, "ttfb_ms": None, "wall_ms": None},
            "ui": None,
            "backend": {"sql_calls": 1, "sql_time_ms": None, "pool_wait_ms": None, "plan_artifact": None},
            "mutation": result,
        }
        append_jsonl(records_path, record)
        print(f"applied isolated fixture mutation {args.mutation}")
        return

    paths = [required_path(value, "evidence JSONL path") for value in args.jsonl]
    validation = validate_records(paths)
    output = required_path(args.report, "evidence report path")
    if output.exists():
        raise HarnessError(f"refusing to overwrite evidence report {output}")
    output_dir = required_path(args.output_dir, "plan output directory")
    artifacts = capture_plans(args, dataset, output_dir) if args.capture_plans else []
    report = {
        "created_at_utc": utc_now(),
        "engine": args.engine,
        "phase": args.phase,
        "fixture_manifest_sha256": sha256_bytes(manifest_path.read_bytes()),
        "fixture_sha256": dataset["fixture_sha256"],
        "runtime_profile": dataset["runtime_profile"],
        "plans": artifacts,
        "records": validation,
        "note": "Missing records remain missing; this report never synthesizes evidence or changes QA statuses.",
    }
    atomic_json(output, report)
    print(f"evidence report written: {output}")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Isolated Cache Keepalive actual-server QA harness. It never starts daemons and rejects non-loopback HTTP origins.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""Commands:
  prepare   Seed one already-migrated isolated SQLite DB or Docker PostgreSQL DB.
  compare   Compare baseline and candidate loopback APIs and append raw JSONL.
  evidence  Apply a named isolated fixture transition, capture plans, or validate evidence.

Important variables:
  KEEPALIVE_SCRATCH_DIR      isolated output root (default {DEFAULT_SCRATCH})
  CC_LB_ADMIN_TOKEN          default optional admin token environment variable
  KEEPALIVE_MANIFEST         Playwright fixture manifest path (default {DEFAULT_MANIFEST})
  KEEPALIVE_EVIDENCE_DIR     Playwright evidence directory
  KEEPALIVE_ADMIN_TOKEN_FILE optional mode-0600 token file read into memory only
  KEEPALIVE_PHASE            baseline or candidate for Playwright
  KEEPALIVE_ENGINE           sqlite or postgres for Playwright
  KEEPALIVE_BACKEND_URL      actual loopback cc-lb admin origin
  CC_LB_ADMIN_URL            Vite proxy target; must equal KEEPALIVE_BACKEND_URL

Main-owned static-token QA startup:
  export CC_LB_ADMIN_AUTH_PROVIDERS_JSON='[{{"kind":"static_token","id":"keepalive-qa","token_env":"CC_LB_ADMIN_TOKEN"}}]'
  export CC_LB_ADMIN_TOKEN="$(cat "$KEEPALIVE_ADMIN_TOKEN_FILE")"

The provider JSON contains only an environment-variable reference. Main owns
cc-lb/Vite/fake-upstream/Docker lifecycle. This script never prints tokens,
puts them in argv, writes them to manifests, or permits production-network HTTP.
""",
    )
    commands = result.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare", help="seed an already migrated isolated DB")
    prepare_parser.add_argument("--engine", choices=("sqlite", "postgres"), required=True)
    prepare_parser.add_argument("--phase", choices=("baseline", "candidate"), required=True)
    prepare_parser.add_argument("--manifest", default=os.environ.get("KEEPALIVE_MANIFEST", str(DEFAULT_MANIFEST)))
    prepare_parser.add_argument("--sqlite-db")
    prepare_parser.add_argument("--pg-container", default=DEFAULT_PG_CONTAINER)
    prepare_parser.add_argument("--pg-database")
    prepare_parser.add_argument("--server-url")
    prepare_parser.add_argument("--fake-upstream-url", default="http://127.0.0.1:19080")
    prepare_parser.add_argument("--runtime-profile", default="release")
    prepare_parser.add_argument("--anchor-ms", type=int)
    prepare_parser.add_argument("--d-count", type=int, default=100_000)
    prepare_parser.add_argument("--ui-count", type=int, default=1_201)
    prepare_parser.add_argument("--st-sessions", type=int, default=2_000)
    prepare_parser.add_argument("--turns-per-session", type=int, default=5)
    prepare_parser.add_argument("--replace-fixture", action="store_true", help="delete only deterministic QA fixture IDs before reseeding")
    prepare_parser.set_defaults(function=prepare)

    compare_parser = commands.add_parser("compare", help="compare actual baseline/candidate loopback APIs")
    compare_parser.add_argument("--engine", choices=("sqlite", "postgres"), required=True)
    compare_parser.add_argument("--baseline-url", required=True)
    compare_parser.add_argument("--candidate-url", required=True)
    compare_parser.add_argument("--case", default="all", help="comma-separated: all,semantic,filters,cursor,PERF-01,PERF-02,PERF-03,ST-SCALE")
    compare_parser.add_argument("--manifest", default=os.environ.get("KEEPALIVE_MANIFEST", str(DEFAULT_MANIFEST)))
    compare_parser.add_argument("--warm-samples", type=int, default=30)
    compare_parser.add_argument("--timeout", type=float, default=120.0)
    compare_parser.add_argument("--runtime-profile", default="release")
    compare_parser.add_argument("--records", default=str(DEFAULT_RECORDS))
    compare_parser.add_argument("--summary", default=str(DEFAULT_EVIDENCE / "comparison-summary.json"))
    token_group = compare_parser.add_mutually_exclusive_group()
    token_group.add_argument("--token-env")
    token_group.add_argument("--token-file")
    compare_parser.set_defaults(function=compare)

    evidence_parser = commands.add_parser("evidence", help="mutate isolated fixtures, capture plans, or validate raw evidence")
    evidence_parser.add_argument("--manifest", default=os.environ.get("KEEPALIVE_MANIFEST", str(DEFAULT_MANIFEST)))
    evidence_parser.add_argument("--engine", choices=("sqlite", "postgres"), required=True)
    evidence_parser.add_argument("--phase", choices=("baseline", "candidate"), required=True)
    evidence_parser.add_argument("--mutation", choices=("bump-summary", "reorder", "late-turn", "reactivate", "cleanup"))
    evidence_parser.add_argument("--case-id")
    evidence_parser.add_argument("--records", default=str(DEFAULT_RECORDS))
    evidence_parser.add_argument("--capture-plans", action="store_true")
    evidence_parser.add_argument("--output-dir", default=str(DEFAULT_EVIDENCE / "plans"))
    evidence_parser.add_argument("--jsonl", action="append", default=[])
    evidence_parser.add_argument("--report", default=str(DEFAULT_EVIDENCE / "evidence-report.json"))
    evidence_parser.set_defaults(function=evidence)
    evidence_parser.add_argument("--sanitize-trace")
    evidence_token_group = evidence_parser.add_mutually_exclusive_group()
    evidence_token_group.add_argument("--token-env")
    evidence_token_group.add_argument("--token-file")
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        args.function(args)
        return 0
    except (HarnessError, OSError, sqlite3.Error, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
