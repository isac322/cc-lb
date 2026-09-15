#!/usr/bin/env python3
"""Prepare isolated Admin Web mutation fixtures without starting any daemon.

Main owns cc-lb, Vite, browser, fake-upstream, PostgreSQL, and validation
lifecycles. This helper only creates local configuration/secrets, seeds through
the real loopback Admin API, adds the minimal historical Audit SQL rows that no
Admin API can create deterministically, snapshots/restores isolated databases,
and writes redacted proof manifests.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import ipaddress
import json
import os
import re
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import tomllib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_ROOT = ROOT / "target" / "keepalive-qa" / "admin-web-fixtures"
DEFAULT_MANIFEST = DEFAULT_ROOT / "fixture-manifest.json"
FIXTURE_VERSION = 1
TOKEN_ENV = "CC_LB_ADMIN_TOKEN"
MASTER_KEY_ENV = "CC_LB_MASTER_KEY"
CLUSTER_TOKEN_ENV = "CC_LB_CLUSTER_TOKEN"
AUTH_PROVIDERS_ENV = "CC_LB_ADMIN_AUTH_PROVIDERS_JSON"
POSTGRES_URL_ENV = "ADMIN_WEB_QA_POSTGRES_URL"
QA_NAME = "admin-web-full"
SAFE_CONTAINER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
QA_CONTAINER_NAME = re.compile(
    r"(?:^|[^a-z0-9])(?:admin[-_]?web|qa|test|fixture|scratch)(?:$|[^a-z0-9])",
    re.I,
)
QA_DB_NAME = QA_CONTAINER_NAME

KEEPALIVE_CONFIG = {
    "enabled": True,
    "refresh_lead_time_5m_secs": 30,
    "refresh_lead_time_1h_secs": 300,
    "max_refreshes_per_session": 12,
    "max_total_duration_secs": 14_400,
    "snapshot_max_bytes": 524_288,
    "classifier": {
        "extra_wait_for_user_tools": [],
        "treat_end_turn_as_ambiguous": False,
    },
}

SCENARIOS = {
    "principal_keys": ".agents/skills/user-flow-qa/references/scenarios/principal-api-key-issuance.md",
    "plugins": ".agents/skills/user-flow-qa/references/scenarios/plugin-upload-replacement-and-refs.md",
    "browser_recorder": "crates/cc-lb-admin/web/qa/admin-web-browser-recorder.js",
    "frontend_boundaries": ".agents/skills/user-flow-qa/references/scenarios/frontend-state-boundaries.md",
    "keepalive_runner": "crates/cc-lb-admin/web/qa/keepalive-qa-runner.py",
}

SOURCE_CONTRACTS = {
    "ApiSqlWriteInventory-contract.json": "0fe89efdd4f3a71d568a1050ba01a201ccf2442bbea973949c945cbf00b19820",
    "ApiSqlReadInventory-contract.json": "fbda145e628497fa5cc784e4fb1d45bbc7e6e876364ab4ea898ab40282202525",
    "UiInventoryGlobal-contract.json": "eb5ae11f8ecd8b5bd2384167653336a773f1a7e7a021124b819abf1054fcd1b7",
    "UiInventoryPrincipals-contract.json": "0258d60bbbeb77609a62ffc65e55afde0a8809312cc5ff6b89ea6c6d1314e323",
    "UiInventoryUpstreams-contract.json": "dd052d40178b4481ab22461e074055c69013b18ed0ce5460b77eb06887fef1fd",
    "UiInventoryPlugins-contract.json": "e3d6b1ad502008a2d165740973c28e962d7c4c0c71a43b31f79fd740d8cfc7ac",
    "UiInventorySettings-contract.json": "d2f74a27a980b558d534d8af47c811269890a0d6d9900096801198045a7e20cc",
}

INVENTORY_RECONCILIATION = {
    "captured_on": "2026-09-15",
    "write_endpoint_count": 45,
    "read_endpoint_count": 70,
    "operation_groups": {
        "principals": ["ApiSqlWriteInventory", "UiInventoryPrincipals"],
        "keys": ["ApiSqlWriteInventory", "ApiSqlReadInventory", "UiInventoryPrincipals"],
        "router": ["ApiSqlWriteInventory", "UiInventoryPrincipals"],
        "upstreams": ["ApiSqlWriteInventory", "UiInventoryUpstreams"],
        "warmup_oauth": ["ApiSqlWriteInventory", "UiInventoryUpstreams"],
        "plugins": ["ApiSqlWriteInventory", "UiInventoryPlugins", "UiInventoryPrincipals", "UiInventoryUpstreams"],
        "settings_audit": ["ApiSqlWriteInventory", "ApiSqlReadInventory", "UiInventorySettings"],
        "auth_error_empty": ["ApiSqlReadInventory", "UiInventoryGlobal"],
    },
    "aliases": {
        "config": ["/admin/config/*", "/admin/v1/config/*"],
        "audit": ["/admin/audit", "/admin/v1/audit"],
        "legacy_key_mutations": [
            "/admin/principals/{id}/keys/{key_id}/revoke",
            "/admin/principals/{id}/keys/{key_id}/disable",
            "/admin/principals/{id}/keys/{key_id}/enable",
        ],
    },
}

ENGINE_MATRIX = {
    "sqlite": {
        "role": "default mutation/UI state fixture",
        "database": "fresh migrated SQLite file beneath fixture root",
        "coverage": "all storage-backed Admin mutation semantics and reset snapshots",
    },
    "postgres": {
        "role": "PostgreSQL storage parity fixture",
        "required_server_major": 16,
        "database": "fresh isolated loopback database whose name identifies admin-web/QA/test/fixture/scratch",
        "cluster_env": CLUSTER_TOKEN_ENV,
        "client_fallback": "host psql/pg_dump/pg_restore when present; otherwise only an explicitly recorded owned QA container",
    },
}

GET_SIDE_EFFECTS = [
    {"id": "GET-AUDIT-EXPORT", "method": "GET", "path": "/admin/v1/export", "effect": "appends config_export Audit entry after reading the database catalog"},
    {"id": "GET-AUDIT-CONFIG-CURRENT", "method": "GET", "path": "/admin/config/current", "effect": "appends config_read Audit entry"},
    {"id": "GET-AUDIT-CONFIG-CURRENT-V1", "method": "GET", "path": "/admin/v1/config/current", "effect": "same handler; appends config_read Audit entry"},
    {"id": "GET-AUDIT-CONFIG-DRAFT", "method": "GET", "path": "/admin/config/draft", "effect": "appends config_draft_read Audit entry"},
    {"id": "GET-AUDIT-CONFIG-DRAFT-V1", "method": "GET", "path": "/admin/v1/config/draft", "effect": "same handler; appends config_draft_read Audit entry"},
    {"id": "GET-AUDIT-CONFIG-HISTORY", "method": "GET", "path": "/admin/config/history", "effect": "appends config_history_read Audit entry"},
    {"id": "GET-AUDIT-CONFIG-HISTORY-V1", "method": "GET", "path": "/admin/v1/config/history", "effect": "same handler; appends config_history_read Audit entry"},
    {"id": "GET-AUDIT-CONFIG-DIFF", "method": "GET", "path": "/admin/config/diff?from=1&to=1", "effect": "appends config_diff_read Audit entry when both revisions exist"},
    {"id": "GET-AUDIT-CONFIG-DIFF-V1", "method": "GET", "path": "/admin/v1/config/diff?from=1&to=1", "effect": "same handler; appends config_diff_read Audit entry when both revisions exist"},
    {"id": "GET-AUDIT-AUDIT", "method": "GET", "path": "/admin/audit", "effect": "queries first, then appends audit_query; response does not contain its own new row"},
    {"id": "GET-AUDIT-AUDIT-V1", "method": "GET", "path": "/admin/v1/audit", "effect": "same handler; queries first, then appends audit_query"},
    {"id": "GET-AUDIT-EVENT-DETAIL", "method": "GET", "path": "/admin/v1/events/detail/{event_id}", "effect": "appends request_event_detail_read Audit entry"},
    {"id": "GET-AUDIT-KEY-LIST", "method": "GET", "path": "/admin/v1/principals/{principal_id}/keys", "effect": "appends principal_keys_list Audit entry"},
]

COVERAGE_GROUPS: dict[str, list[dict[str, Any]]] = {
    "principals": [
        {"id": "UI-PR-23", "method": "POST", "path": "/admin/v1/principals", "fixture": "principal.create_target", "reset": "snapshot"},
        {"id": "UI-PR-03", "method": "POST", "path": "/admin/v1/principals/{id}/{enable|disable}", "fixture": "principal.primary", "reset": "snapshot"},
        {"id": "UI-PR-04", "method": "DELETE", "path": "/admin/v1/principals/{id}", "fixture": "principal.delete_target", "reset": "snapshot"},
        {"id": "UI-PR-05", "method": "PUT", "path": "/admin/v1/principals/{id}/allowed_models", "fixture": "principal.primary", "reset": "snapshot"},
        {"id": "UI-PR-06", "method": "PATCH", "path": "/admin/v1/principals/{id}", "fixture": "principal.primary", "reset": "snapshot"},
        {"id": "UI-PR-08/UI-PR-09", "method": "PATCH", "path": "/admin/v1/principals/{id}", "fixture": "principal.primary.cache_keepalive", "reset": "snapshot"},
    ],
    "keys": [
        {"id": "UI-PR-20", "method": "GET", "path": "/admin/v1/principals/{id}/keys", "fixture": "key.preexisting", "reset": "snapshot", "side_effect": "Audit write"},
        {"id": "UI-PR-21", "method": "POST", "path": "/admin/v1/principals/{id}/keys", "fixture": "principal.primary", "reset": "snapshot", "secret_rule": "plaintext response must never enter manifest/evidence"},
        {"id": "UI-PR-22", "method": "POST", "path": "/admin/v1/principals/{id}/keys/{key_id}/revoke", "fixture": "key.preexisting", "reset": "snapshot"},
        {"id": "API-LEGACY-KEY-ENABLE", "method": "POST", "path": "/admin/principals/{id}/keys/{key_id}/{enable|disable}", "fixture": "key.preexisting", "reset": "snapshot", "ui": "backend-only legacy"},
    ],
    "router": [
        {"id": "UI-PR-14", "method": "PUT", "path": "/admin/v1/principals/{id}/router-terminal", "fixture": "principal.primary", "reset": "snapshot"},
        {"id": "API-ROUTER-PREVIEW", "method": "POST", "path": "/admin/v1/router/preview", "fixture": "principal.primary", "reset": "none", "side_effect": "read-only POST; no DB, Audit, or external call"},
        {"id": "UI-PR-15/UI-PR-16/UI-PR-18/UI-PR-19", "method": "POST/DELETE", "path": "/admin/v1/principals/{id}/plugin-chain and /admin/v1/plugin-chain-entries/{id}", "fixture": "plugin.primary", "reset": "snapshot"},
    ],
    "upstreams": [
        {"id": "UI-UP-26", "method": "POST", "path": "/admin/v1/upstreams", "fixture": "upstream.create_target", "reset": "snapshot"},
        {"id": "UI-UP-11/UI-UP-13", "method": "PUT", "path": "/admin/v1/upstreams/{id}", "fixture": "upstream.api_active", "reset": "snapshot"},
        {"id": "UI-UP-12/UI-UP-15/UI-UP-16", "method": "PATCH", "path": "/admin/v1/upstreams/{id}", "fixture": "upstream.api_active", "reset": "snapshot"},
        {"id": "UI-UP-17", "method": "DELETE", "path": "/admin/v1/upstreams/{id}/warmup-dialect-plugin", "fixture": "plugin.primary", "reset": "snapshot"},
        {"id": "UI-UP-24", "method": "DELETE", "path": "/admin/v1/upstreams/{id}", "fixture": "upstream.delete_target", "reset": "snapshot"},
    ],
    "warmup_oauth": [
        {"id": "UI-UP-18-safe-errors", "method": "POST", "path": "/admin/v1/upstreams/{id}/warmup/fire-now", "fixture": "upstream.oauth_missing_credentials", "reset": "snapshot", "ready": "safe deterministic oauth_credentials_missing/disabled/unsupported-kind branches"},
        {"id": "UI-UP-18-success", "method": "POST", "path": "/admin/v1/upstreams/{id}/warmup/fire-now", "fixture": "oauth credentials plus fake messages upstream", "reset": "snapshot", "blocked": "no Admin API can inject OAuth credentials; completing OAuth also calls fixed api.anthropic.com metadata URLs"},
        {"id": "UI-UP-07", "method": "POST", "path": "/admin/v1/upstreams/{id}/subscription-metadata/refresh", "fixture": "real OAuth credentials", "reset": "snapshot", "blocked": "metadata URLs are fixed to api.anthropic.com and require a valid OAuth token for success"},
        {"id": "UI-UP-22/UI-UP-27", "method": "POST", "path": "/admin/v1/upstreams/{id}/oauth/start and /admin/v1/oauth/draft/start", "fixture": "loopback auth/token URLs", "reset": "restart", "prerequisite": "configure-oauth must be ready because OAuth draft Start immediately opens authorize_url in a browser tab"},
        {"id": "UI-UP-23/UI-UP-28/UI-UP-29", "method": "POST", "path": "OAuth complete/create-from-draft", "fixture": "loopback token endpoint", "reset": "snapshot", "blocked": "completion also performs non-configurable metadata requests to api.anthropic.com; do not label mock-only completion a full provider success"},
    ],
    "plugins": [
        {"id": "UI-PLUG-02", "method": "POST", "path": "/admin/v1/plugins/wasm", "fixture": "plugin.primary_wasm", "reset": "snapshot"},
        {"id": "UI-PLUG-03", "method": "POST", "path": "/admin/v1/plugins/wasm", "fixture": "plugin.same_name_alt/higher_version", "reset": "snapshot", "prerequisite": "scratch-built same-name replacement artifacts"},
        {"id": "UI-PLUG-07", "method": "PATCH", "path": "/admin/v1/plugins/registry/{id}", "fixture": "plugin.primary", "reset": "snapshot"},
        {"id": "UI-PLUG-09", "method": "GET", "path": "/admin/v1/plugins/registry/{id}/references", "fixture": "plugin.primary referenced twice", "reset": "none"},
        {"id": "UI-PLUG-08", "method": "DELETE", "path": "/admin/v1/plugins/registry/{id}?cascade=references", "fixture": "plugin.primary referenced twice", "reset": "snapshot"},
        {"id": "UI-PLUG-04", "method": "POST", "path": "/admin/v1/plugins/wasm/gc", "fixture": "unreferenced replaced blob", "reset": "snapshot"},
        {"id": "API-PLUGIN-CHAIN-DIRECT", "method": "PUT/POST", "path": "/admin/v1/plugin-chain-entries/{id} and /rebalance", "fixture": "plugin.primary", "reset": "snapshot", "ui": "backend-only"},
    ],
    "settings_audit": [
        {"id": "UI-SET-04", "method": "PUT", "path": "/admin/config/draft", "fixture": "settings.no_draft or settings.valid_draft", "reset": "snapshot"},
        {"id": "UI-SET-05", "method": "POST", "path": "/admin/config/draft/validate", "fixture": "settings.valid_draft", "reset": "snapshot"},
        {"id": "UI-SET-06", "method": "POST", "path": "/admin/config/apply", "fixture": "settings.validated_draft", "reset": "snapshot plus config file", "warning": "isolated instance only"},
        {"id": "UI-SET-07", "method": "POST", "path": "/admin/config/reload", "fixture": "isolated process", "reset": "restart plus snapshot", "warning": "sends SIGHUP to the isolated cc-lb PID"},
        {"id": "UI-SET-02/GET-side-effects", "method": "GET", "path": "/admin/v1/export and config/audit/detail/key reads", "fixture": "populated", "reset": "snapshot", "side_effect": "Audit writes listed in get_side_effects"},
        {"id": "UI-AUD-01/UI-AUD-09", "method": "GET", "path": "/admin/audit", "fixture": "audit.seeded_history", "reset": "snapshot", "side_effect": "Audit query appends audit_query after selecting response rows"},
    ],
    "auth_error_empty": [
        {"id": "AUTH-401", "method": "any protected", "path": "/admin/v1/*", "fixture": "missing or wrong static token", "reset": "none", "ready": True},
        {"id": "AUTHZ-403", "method": "sensitive/write", "path": "/admin/v1/*", "fixture": None, "reset": "none", "not_applicable": "Current authorize() accepts every authenticated identity/action; there is no restricted-role product state to seed."},
        {"id": "EMPTY-ALL", "method": "GET", "path": "principals/upstreams/plugins/audit", "fixture": "empty snapshot captured before prepare", "reset": "empty snapshot", "ready": True},
        {"id": "ERROR-VALIDATION", "method": "mutation", "path": "stale If-Match, duplicate names, invalid bodies", "fixture": "populated", "reset": "snapshot", "ready": True},
        {"id": "ERROR-STORAGE-GENERIC", "method": "any DB path", "path": None, "fixture": None, "reset": "none", "not_applicable": "A generic forced HTTP 500 is not an inventory operation cell. Record a naturally observed isolated storage/network failure separately; do not corrupt the fixture or add runtime fault injection."},
    ],
}

BLOCKERS = [
    {
        "id": "BLOCK-OAUTH-METADATA-SUCCESS",
        "prerequisites": [
            "valid non-production Anthropic OAuth credential",
            "approved external access to fixed api.anthropic.com metadata endpoints",
        ],
        "alternative": "PKCE start/token exchange can point auth_url/token_url at loopback, but metadata fetch URLs in cc-lb-control/src/anthropic_metadata/fetchers.rs are fixed; record token-only evidence separately, not as full success.",
    },
]

NOT_APPLICABLE = [
    {
        "id": "AUTHZ-403",
        "reason": "crates/cc-lb-admin/src/auth/authorize.rs returns Ok for every identity and AdminAction; current product has no restricted authenticated role state.",
        "classification": "source_unreachable",
    },
    {
        "id": "GENERIC-FORCED-STORAGE-500",
        "reason": "Not a source-inventory operation cell. Only record a concrete naturally reachable handler/storage error branch; no generic fault injector is required.",
        "classification": "not_applicable",
    },
]

ERROR_RECIPES = [
    {"id": "ERR-401", "request": "GET /admin/v1/auth/session with no or wrong token", "expect": [401, "unauthorized"], "reset": "none"},
    {"id": "ERR-PRINCIPAL-DUPLICATE", "request": "POST /admin/v1/principals using qa-principal-primary", "expect": [409, "principal_name_conflict"], "reset": "populated"},
    {"id": "ERR-PRINCIPAL-IF-MATCH", "request": "PATCH /admin/v1/principals/{primary} without If-Match", "expect": [428, "if_match_required"], "reset": "populated"},
    {"id": "ERR-PRINCIPAL-STALE", "request": "PATCH /admin/v1/principals/{primary} with W/\\\"0\\\"", "expect": [409, "stale_revision"], "reset": "populated"},
    {"id": "ERR-ROUTER-STRATEGY", "request": "PUT /admin/v1/principals/{primary}/router-terminal with unsupported strategy", "expect": [400, "invalid_router_terminal_strategy"], "reset": "populated"},
    {"id": "ERR-UPSTREAM-DUPLICATE", "request": "POST /admin/v1/upstreams using qa-api-active", "expect": [409, "upstream_name_conflict"], "reset": "populated"},
    {"id": "ERR-METADATA-NON-OAUTH", "request": "POST /admin/v1/upstreams/{api_active}/subscription-metadata/refresh", "expect": [400, "not_oauth_upstream"], "reset": "populated"},
    {"id": "ERR-WARMUP-NON-OAUTH", "request": "POST /admin/v1/upstreams/{api_active}/warmup/fire-now", "expect": [400, "warmup_unsupported_for_kind"], "reset": "populated"},
    {"id": "ERR-WARMUP-MISSING-CREDENTIALS", "request": "POST /admin/v1/upstreams/{oauth_missing_credentials}/warmup/fire-now", "expect": [400, "oauth_credentials_missing"], "reset": "populated"},
    {"id": "ERR-PLUGIN-REFERENCED", "request": "DELETE /admin/v1/plugins/registry/{plugin} without cascade", "expect": [409, "plugin_registry_referenced"], "reset": "populated", "requires": "plugin artifacts"},
    {"id": "ERR-PLUGIN-STALE-FINGERPRINT", "request": "cascade delete after changing a reference using the old fingerprint", "expect": [409, "references_changed"], "reset": "populated", "requires": "plugin artifacts"},
    {"id": "ERR-CONFIG-STALE", "request": "PUT /admin/config/draft with expected_revision 0", "expect": [409, "stale_draft_revision"], "reset": "populated"},
    {"id": "ERR-CONFIG-INVALID", "request": "save isolated invalid draft then validate its revision", "expect": [200, "valid=false"], "reset": "populated"},
]


class FixtureError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, canonical_bytes(value) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    temporary.replace(path)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise FixtureError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise FixtureError(f"expected JSON object in {path}")
    return value


def fixture_root(raw: str) -> Path:
    path = Path(raw).expanduser().resolve()
    default = DEFAULT_ROOT.resolve()
    safe_default = path == default or default in path.parents
    safe_temp = any(part.startswith("cc-lb-admin-web-qa-") for part in path.parts)
    if not (safe_default or safe_temp):
        raise FixtureError(
            f"fixture root must be beneath {default} or have a cc-lb-admin-web-qa-* temp component"
        )
    if path in {Path("/").resolve(), Path.home().resolve(), ROOT.resolve()}:
        raise FixtureError("refusing unsafe fixture root")
    return path


def load_manifest(raw: str) -> tuple[Path, dict[str, Any]]:
    path = Path(raw).expanduser().resolve()
    manifest = load_json(path)
    if manifest.get("fixture_version") != FIXTURE_VERSION:
        raise FixtureError(f"fixture version must be {FIXTURE_VERSION}")
    root = fixture_root(str(manifest.get("root", "")))
    if root not in path.parents:
        raise FixtureError("manifest must be beneath its declared fixture root")
    manifest["source_contracts"] = SOURCE_CONTRACTS
    manifest["inventory_reconciliation"] = INVENTORY_RECONCILIATION
    manifest["coverage_groups"] = COVERAGE_GROUPS
    manifest["get_side_effects"] = GET_SIDE_EFFECTS
    manifest["error_recipes"] = ERROR_RECIPES
    manifest["engine_matrix"] = ENGINE_MATRIX
    manifest["secret_material_policy"] = {
        "values_in_manifest": False,
        "secret_files_mode": "0600",
        "plaintext_key_response": "discarded immediately; neither value nor hash is retained",
    }
    preexisting_key = (
        manifest.get("fixtures", {})
        .get("key", {})
        .get("preexisting", {})
    )
    if isinstance(preexisting_key, dict):
        preexisting_key.pop("plaintext_sha256_prefix", None)
    commands = manifest.setdefault("main_owned_commands", {})
    commands["refresh_manifest"] = (
        f"python3 crates/cc-lb-admin/web/qa/admin-web-fixtures.py "
        f"refresh-manifest --manifest {path}"
    )
    commands["configure_oauth"] = (
        f"python3 crates/cc-lb-admin/web/qa/admin-web-fixtures.py "
        f"configure-oauth --manifest {path} --origin http://127.0.0.1:19081 "
        "--confirm-stopped"
    )
    if manifest.get("engine") == "postgres":
        commands["configure_postgres_pool"] = (
            f"python3 crates/cc-lb-admin/web/qa/admin-web-fixtures.py "
            f"configure-postgres-pool --manifest {path} "
            "--postgres-container <owned-qa-container> --confirm-stopped"
        )
    manifest["scenario_refs"] = SCENARIOS
    manifest["blockers"] = BLOCKERS
    manifest["not_applicable"] = NOT_APPLICABLE
    manifest["prerequisite_status"] = fixture_prerequisite_status(
        manifest.get("fixtures", {})
    )
    manifest["prerequisite_status"]["oauth_start_loopback"] = (
        "ready"
        if manifest.get("oauth_origin")
        else "pending_configure_oauth"
    )
    manifest.setdefault(
        "required_environment_keys",
        [AUTH_PROVIDERS_ENV, TOKEN_ENV, MASTER_KEY_ENV]
        + (
            [
                CLUSTER_TOKEN_ENV,
                manifest.get("database", {}).get(
                    "postgres_url_env",
                    POSTGRES_URL_ENV,
                ),
            ]
            if manifest.get("engine") == "postgres"
            else []
        ),
    )
    return path, manifest


def write_secret(path: Path, value: str) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, value.encode() + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def read_secret(path: Path) -> str:
    mode = path.stat().st_mode & 0o777
    if mode & 0o077:
        raise FixtureError(f"secret file must be mode 0600: {path}")
    value = path.read_text().strip()
    if not value:
        raise FixtureError(f"secret file is empty: {path}")
    return value


def loopback_origin(raw: str, label: str) -> str:
    parsed = urllib.parse.urlsplit(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise FixtureError(f"{label} must be an http(s) origin")
    if parsed.username or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise FixtureError(f"{label} must not contain credentials, path, query, or fragment")
    try:
        addresses = {entry[4][0] for entry in socket.getaddrinfo(parsed.hostname, parsed.port or 80)}
    except socket.gaierror as error:
        raise FixtureError(f"cannot resolve {label}") from error
    if not addresses or any(not ipaddress.ip_address(value.split("%", 1)[0]).is_loopback for value in addresses):
        raise FixtureError(f"{label} must resolve only to loopback addresses")
    return raw.rstrip("/")


def loopback_address(raw: str, label: str) -> tuple[str, int]:
    if raw.count(":") != 1:
        raise FixtureError(f"{label} must be 127.0.0.1:PORT")
    host, port_raw = raw.split(":", 1)
    try:
        port = int(port_raw)
    except ValueError as error:
        raise FixtureError(f"{label} has invalid port") from error
    if not ipaddress.ip_address(host).is_loopback or not 1 <= port <= 65535:
        raise FixtureError(f"{label} must use a loopback IP and valid port")
    return host, port


def postgres_parts(raw: str) -> dict[str, Any]:
    parsed = urllib.parse.urlsplit(raw)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise FixtureError(
            "PostgreSQL URL must use postgres:// or postgresql://"
        )
    try:
        addresses = {
            entry[4][0]
            for entry in socket.getaddrinfo(
                parsed.hostname,
                parsed.port or 5432,
            )
        }
    except socket.gaierror as error:
        raise FixtureError("cannot resolve PostgreSQL host") from error
    if not addresses or any(
        not ipaddress.ip_address(value.split("%", 1)[0]).is_loopback
        for value in addresses
    ):
        raise FixtureError("PostgreSQL host must resolve only to loopback")
    database = parsed.path.removeprefix("/")
    if not database or not QA_DB_NAME.search(database):
        raise FixtureError(
            "PostgreSQL database name must include admin-web, qa, test, fixture, or scratch as a token bounded by the name edges or non-alphanumeric characters"
        )
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "user": urllib.parse.unquote(parsed.username or ""),
        "password": urllib.parse.unquote(parsed.password or ""),
        "database": database,
    }


def validate_postgres_container(raw: str) -> str:
    if not SAFE_CONTAINER.fullmatch(raw):
        raise FixtureError("unsafe PostgreSQL container name")
    if not QA_CONTAINER_NAME.search(raw):
        raise FixtureError(
            "PostgreSQL container name must include an owned admin-web, qa, test, fixture, or scratch token bounded by the name edges or non-alphanumeric characters"
        )
    return raw


def toml_string(value: str) -> str:
    return json.dumps(value)


def config_text(
    *,
    engine: str,
    root: Path,
    proxy_addr: str,
    admin_addr: str,
    metrics_addr: str,
    postgres_url: str | None,
    oauth_origin: str | None,
) -> str:
    lines = [
        "# Generated disposable Admin Web QA configuration.",
        "[listener]",
        f"proxy_addr = {toml_string(proxy_addr)}",
        f"admin_addr = {toml_string(admin_addr)}",
        f"metrics_addr = {toml_string(metrics_addr)}",
        "",
        "[body]",
        "messages_cap_bytes = 33554432",
        "files_cap_bytes = 104857600",
        "",
        "[timeouts]",
        "upstream_total_secs = 30",
        "drain_secs = 5",
        "",
        "[storage]",
    ]
    if engine == "sqlite":
        lines.extend(["kind = \"sqlite\"", f"path = {toml_string(str(root / 'storage.sqlite'))}"])
    else:
        if postgres_url is None:
            raise FixtureError("PostgreSQL URL is required")
        lines.extend(
            [
                "kind = \"postgres\"",
                f"url = {toml_string(postgres_url)}",
                "",
                "[storage.pool]",
                "max_connections = 10",
                "acquire_timeout_secs = 5",
                "statement_timeout_secs = 25",
                "sslmode = \"disable\"",
            ]
        )
    lines.extend(
        [
            "",
            "[aead]",
            f"key_env = {toml_string(MASTER_KEY_ENV)}",
            "",
            "[observability]",
            "tracing_level = \"info\"",
            "log_redaction = true",
            "user_prompt_redaction = true",
            "",
            "[[admin.auth.providers]]",
            'kind = "static_token"',
            f"id = {toml_string(QA_NAME)}",
            f"token_env = {toml_string(TOKEN_ENV)}",
            "",
            "[circuit_breaker]",
            "failures_to_open = 5",
            "window_secs = 10",
            "half_open_after_secs = 30",
        ]
    )
    if engine == "postgres":
        admin_host, admin_port = loopback_address(admin_addr, "admin address")
        lines.extend(
            [
                "",
                "[cluster]",
                f"instance_url = {toml_string(f'http://{admin_host}:{admin_port}')}",
                f"token_env = {toml_string(CLUSTER_TOKEN_ENV)}",
            ]
        )
    if oauth_origin:
        origin = loopback_origin(oauth_origin, "OAuth origin")
        lines.extend(
            [
                "",
                "[oauth.anthropic]",
                "client_id = \"admin-web-qa-client\"",
                f"auth_url = {toml_string(origin + '/oauth/authorize')}",
                f"token_url = {toml_string(origin + '/oauth/token')}",
                f"redirect_uri = {toml_string(origin + '/oauth/callback')}",
                "scopes = [\"org:profile\", \"user:inference\"]",
            ]
        )
    return "\n".join(lines) + "\n"


class ApiClient:
    def __init__(self, origin: str, token: str | None, timeout: float = 30.0):
        self.origin = loopback_origin(origin, "Admin API URL")
        self.token = token
        self.timeout = timeout
        self.parsed = urllib.parse.urlsplit(self.origin)

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any | None = None,
        headers: Mapping[str, str] | None = None,
        expected: Iterable[int] = (200,),
        multipart: tuple[str, bytes] | None = None,
    ) -> tuple[int, dict[str, str], Any]:
        if not path.startswith("/"):
            raise FixtureError("Admin API path must be absolute")
        request_headers = {"Accept": "application/json"}
        if self.token:
            request_headers["Authorization"] = f"Bearer {self.token}"
        if headers:
            request_headers.update(headers)
        payload: bytes | None = None
        if multipart is not None:
            filename, wasm = multipart
            boundary = f"----cc-lb-admin-web-qa-{secrets.token_hex(12)}"
            payload = multipart_body(boundary, filename, wasm)
            request_headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        elif body is not None:
            payload = canonical_bytes(body)
            request_headers["Content-Type"] = "application/json"
        connection_type = http.client.HTTPSConnection if self.parsed.scheme == "https" else http.client.HTTPConnection
        connection = connection_type(self.parsed.hostname, self.parsed.port, timeout=self.timeout)
        try:
            connection.request(method, path, body=payload, headers=request_headers)
            response = connection.getresponse()
            response_body = response.read()
            response_headers = {name.lower(): value for name, value in response.getheaders()}
        finally:
            connection.close()
        try:
            value = json.loads(response_body) if response_body else None
        except json.JSONDecodeError:
            value = {"_body_sha256": sha256_bytes(response_body), "_body_bytes": len(response_body)}
        allowed = set(expected)
        if response.status not in allowed:
            safe = json.dumps(value, sort_keys=True)[:500]
            raise FixtureError(f"{method} {path} expected {sorted(allowed)}, got {response.status}: {safe}")
        return response.status, response_headers, value


def multipart_body(boundary: str, filename: str, wasm: bytes) -> bytes:
    safe_name = Path(filename).name.replace('"', "")
    prefix = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="original_filename"\r\n\r\n'
        f"{safe_name}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="bytes"; filename="{safe_name}"\r\n'
        "Content-Type: application/wasm\r\n\r\n"
    ).encode()
    return prefix + wasm + f"\r\n--{boundary}--\r\n".encode()


def etag(revision: int) -> str:
    return f'W/"{revision}"'


def init_fixture(args: argparse.Namespace) -> None:
    root = fixture_root(args.root)
    if root.exists() and any(root.iterdir()):
        raise FixtureError(f"fixture root is not empty: {root}")
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(root, 0o700)
    for address, label in ((args.proxy_addr, "proxy address"), (args.admin_addr, "admin address"), (args.metrics_addr, "metrics address")):
        loopback_address(address, label)
    postgres_url = None
    if args.engine == "postgres":
        postgres_url = os.environ.get(args.postgres_url_env, "").strip()
        if not postgres_url:
            raise FixtureError(f"missing PostgreSQL URL environment variable {args.postgres_url_env}")
        postgres_parts(postgres_url)
    config_path = root / "cc-lb.toml"
    token_path = root / "admin-token"
    master_key_path = root / "master-key"
    cluster_token_path = root / "cluster-token"
    write_secret(token_path, secrets.token_urlsafe(32))
    write_secret(master_key_path, secrets.token_hex(32))
    if args.engine == "postgres":
        write_secret(cluster_token_path, secrets.token_urlsafe(32))
    config_path.write_text(
        config_text(
            engine=args.engine,
            root=root,
            proxy_addr=args.proxy_addr,
            admin_addr=args.admin_addr,
            metrics_addr=args.metrics_addr,
            postgres_url=postgres_url,
            oauth_origin=args.oauth_origin,
        )
    )
    os.chmod(config_path, 0o600)
    admin_host, admin_port = loopback_address(args.admin_addr, "admin address")
    manifest_path = (
        Path(args.manifest).expanduser().resolve()
        if args.manifest
        else root / "fixture-manifest.json"
    )
    if manifest_path != root / "fixture-manifest.json" and root not in manifest_path.parents:
        raise FixtureError("manifest must be beneath fixture root")
    manifest = {
        "fixture_version": FIXTURE_VERSION,
        "created_at_utc": utc_now(),
        "repo": str(ROOT),
        "root": str(root),
        "engine": args.engine,
        "database": (
            {"sqlite_path": str(root / "storage.sqlite")}
            if args.engine == "sqlite"
            else {
                "postgres_url_env": args.postgres_url_env,
                "postgres_container": (
                    validate_postgres_container(args.postgres_container)
                    if args.postgres_container
                    else None
                ),
            }
        ),
        "config": {"path": str(config_path), "sha256": sha256_bytes(config_path.read_bytes())},
        "admin_origin": f"http://{admin_host}:{admin_port}",
        "secret_refs": {
            TOKEN_ENV: str(token_path),
            MASTER_KEY_ENV: str(master_key_path),
            CLUSTER_TOKEN_ENV: str(cluster_token_path) if args.engine == "postgres" else None,
            AUTH_PROVIDERS_ENV: f'[{json.dumps({"kind": "static_token", "id": QA_NAME, "token_env": TOKEN_ENV}, separators=(",", ":"))}]',
        },
        "required_environment_keys": (
            [AUTH_PROVIDERS_ENV, TOKEN_ENV, MASTER_KEY_ENV]
            + ([CLUSTER_TOKEN_ENV, args.postgres_url_env] if args.engine == "postgres" else [])
        ),
        "engine_matrix": ENGINE_MATRIX,
        "secret_material_policy": {
            "values_in_manifest": False,
            "secret_files_mode": "0600",
            "plaintext_key_response": "discarded immediately; neither value nor hash is retained",
        },
        "main_owned_commands": {
            "serve": f"target/debug/cc-lb serve --config {config_path}",
            "vite": "cd crates/cc-lb-admin/web && CC_LB_ADMIN_URL=<admin_origin> bun run dev --host 127.0.0.1 --port <vite_port>",
            "prepare": f"python3 crates/cc-lb-admin/web/qa/admin-web-fixtures.py prepare --manifest {manifest_path}",
            "empty_snapshot": f"python3 crates/cc-lb-admin/web/qa/admin-web-fixtures.py snapshot --manifest {manifest_path} --name empty --confirm-stopped",
            "populated_snapshot": f"python3 crates/cc-lb-admin/web/qa/admin-web-fixtures.py snapshot --manifest {manifest_path} --name populated --confirm-stopped",
            "reset": f"python3 crates/cc-lb-admin/web/qa/admin-web-fixtures.py reset --manifest {manifest_path} --name <empty|populated> --confirm-stopped",
            "proof": f"python3 crates/cc-lb-admin/web/qa/admin-web-fixtures.py proof --manifest {manifest_path}",
        },
        "execution_contract": {
            "empty_order": ["init", "start isolated app so migrations complete", "snapshot --name empty"],
            "populated_order": ["prepare", "snapshot --name populated", "proof"],
            "independent_mutation_comparison": "Before every UI mutation and its direct-API comparison, stop the app, reset the same named snapshot, restart, and verify proof fixture_sha256. Alternatively clone that snapshot into two isolated databases with the same master key.",
            "never": ["production URL", "production database", "production credential", "shared service mutation", "two mutations against one unrecovered state"],
        },
        "coverage_groups": COVERAGE_GROUPS,
        "get_side_effects": GET_SIDE_EFFECTS,
        "inventory_reconciliation": INVENTORY_RECONCILIATION,
        "error_recipes": ERROR_RECIPES,
        "scenario_refs": SCENARIOS,
        "blockers": BLOCKERS,
        "not_applicable": NOT_APPLICABLE,
        "prerequisite_status": {
            **fixture_prerequisite_status({}),
            "oauth_start_loopback": (
                "ready" if args.oauth_origin else "pending_configure_oauth"
            ),
        },
        "state": "initialized_not_migrated",
        "fixtures": {},
        "snapshots": {},
    }
    atomic_json(manifest_path, manifest)
    print(f"initialized isolated {args.engine} fixture manifest: {manifest_path}")


def require_fresh_api(client: ApiClient) -> None:
    client.request("GET", "/admin/health", expected=(200,))
    client.request("GET", "/admin/v1/auth/session", expected=(200,))
    _, _, principals = client.request("GET", "/admin/v1/principals", expected=(200,))
    _, _, upstreams = client.request("GET", "/admin/v1/upstreams", expected=(200,))
    _, _, plugins = client.request("GET", "/admin/v1/plugins/registry", expected=(200,))
    principal_rows = principals.get("principals", []) if isinstance(principals, dict) else []
    upstream_rows = upstreams.get("upstreams", []) if isinstance(upstreams, dict) else []
    plugin_rows = plugins.get("entries", []) if isinstance(plugins, dict) else []
    non_builtin = [row for row in plugin_rows if not row.get("is_builtin")]
    if principal_rows or upstream_rows or non_builtin:
        raise FixtureError("prepare requires an empty migrated fixture; reset the empty snapshot first")


def create_upstream(client: ApiClient, body: dict[str, Any]) -> dict[str, Any]:
    _, _, value = client.request("POST", "/admin/v1/upstreams", body=body, expected=(201,))
    if not isinstance(value, dict) or not isinstance(value.get("id"), str):
        raise FixtureError("upstream create response is missing id")
    return value


def create_principal(client: ApiClient, body: dict[str, Any]) -> dict[str, Any]:
    _, _, created = client.request("POST", "/admin/v1/principals", body=body, expected=(201,))
    principal_id = created.get("id") if isinstance(created, dict) else None
    if not isinstance(principal_id, str):
        raise FixtureError("principal create response is missing id")
    _, _, value = client.request("GET", f"/admin/v1/principals/{principal_id}", expected=(200,))
    return value


def prepare_fixture(args: argparse.Namespace) -> None:
    manifest_path, manifest = load_manifest(args.manifest)
    token_path = Path(manifest["secret_refs"][TOKEN_ENV])
    token = read_secret(token_path)
    origin = loopback_origin(args.admin_url or manifest["admin_origin"], "Admin API URL")
    client = ApiClient(origin, token, args.timeout)
    if (args.plugin_wasm_alt or args.plugin_wasm_higher) and not all(
        (args.plugin_wasm, args.plugin_wasm_alt, args.plugin_wasm_higher)
    ):
        raise FixtureError(
            "replacement fixture requires base, same-version alternate, and higher-version WASM together"
        )
    require_fresh_api(client)

    api_active = create_upstream(
        client,
        {
            "name": "qa-api-active",
            "kind": "anthropic_api_key",
            "base_url": loopback_origin(args.fake_upstream_url, "fake upstream URL"),
            "api_key_value": "sk-ant-admin-web-qa-" + secrets.token_urlsafe(24),
            "warmup_enabled": False,
        },
    )
    api_disabled = create_upstream(
        client,
        {
            "name": "qa-api-disabled",
            "kind": "anthropic_api_key",
            "base_url": loopback_origin(args.fake_upstream_url, "fake upstream URL"),
            "api_key_value": "sk-ant-admin-web-qa-" + secrets.token_urlsafe(24),
            "warmup_enabled": False,
        },
    )
    _, _, api_disabled = client.request(
        "POST",
        f"/admin/v1/upstreams/{api_disabled['id']}/disable",
        body={},
        headers={"If-Match": etag(int(api_disabled["spec_revision"]))},
        expected=(200,),
    )
    oauth_missing = create_upstream(
        client,
        {
            "name": "qa-oauth-missing-credentials",
            "kind": "anthropic_oauth",
            "base_url": loopback_origin(args.fake_upstream_url, "fake upstream URL"),
            "warmup_enabled": True,
        },
    )
    delete_upstream = create_upstream(
        client,
        {
            "name": "qa-upstream-delete-target",
            "kind": "anthropic_api_key",
            "base_url": loopback_origin(args.fake_upstream_url, "fake upstream URL"),
            "api_key_value": "sk-ant-admin-web-qa-" + secrets.token_urlsafe(24),
            "warmup_enabled": False,
        },
    )

    allowed_upstreams = [api_active["id"], oauth_missing["id"]]
    primary = create_principal(
        client,
        {
            "name": "qa-principal-primary",
            "kind": "machine",
            "allowed_models": ["claude-sonnet-4-5-20250929", "claude-haiku-4-5-20251001"],
            "allowed_upstreams": allowed_upstreams,
            "default_limits": [{"kind": "requests", "window_secs": 60, "cap_micros": 10_000_000}],
            "cache_keepalive": KEEPALIVE_CONFIG,
        },
    )
    disabled = create_principal(
        client,
        {
            "name": "qa-principal-disabled",
            "kind": "human",
            "allowed_models": [],
            "allowed_upstreams": [api_active["id"]],
        },
    )
    _, _, disabled = client.request(
        "POST",
        f"/admin/v1/principals/{disabled['id']}/disable",
        body={},
        headers={"If-Match": etag(int(disabled["revision"]))},
        expected=(200,),
    )
    delete_principal = create_principal(
        client,
        {
            "name": "qa-principal-delete-target",
            "kind": "machine",
            "allowed_models": ["claude-sonnet-4-5-20250929"],
            "allowed_upstreams": [api_active["id"]],
        },
    )
    create_target_name = "qa-principal-create-target"

    _, _, issued = client.request(
        "POST",
        f"/admin/v1/principals/{primary['id']}/keys",
        body={"label": "preexisting-revoke-target"},
        expected=(201,),
    )
    key_id = issued.get("key_id") if isinstance(issued, dict) else None
    if not isinstance(key_id, str):
        raise FixtureError("issued key response is missing key_id")
    issued = None

    plugin_fixture: dict[str, Any] = {"state": "blocked_missing_base_artifact"}
    if args.plugin_wasm:
        wasm_path = Path(args.plugin_wasm).expanduser().resolve()
        wasm = wasm_path.read_bytes()
        _, _, plugin = client.request(
            "POST",
            "/admin/v1/plugins/wasm",
            multipart=(wasm_path.name, wasm),
            expected=(200, 201),
        )
        plugin_id = plugin.get("id") if isinstance(plugin, dict) else None
        if not isinstance(plugin_id, str):
            raise FixtureError("plugin upload response is missing id")
        _, _, registry = client.request(
            "GET",
            "/admin/v1/plugins/registry",
            expected=(200,),
        )
        registry_entry = next(
            (
                entry
                for entry in registry.get("entries", [])
                if isinstance(entry, dict) and entry.get("id") == plugin_id
            ),
            None,
        )
        supported_slots = (
            set(registry_entry.get("supported_slots", []))
            if registry_entry
            else set()
        )
        if "shape" not in supported_slots:
            raise FixtureError(
                "base plugin must declare shape support for principal and warmup references"
            )
        _, _, chain = client.request(
            "POST",
            f"/admin/v1/principals/{primary['id']}/plugin-chain",
            body={"slot": "shape", "wasm_registry_id": plugin_id, "config": {}},
            expected=(201,),
        )
        _, _, api_active = client.request(
            "PATCH",
            f"/admin/v1/upstreams/{api_active['id']}",
            body={
                "warmup_dialect_plugin": {
                    "wasm_registry_id": plugin_id,
                    "wire_version": 1,
                    "config": {},
                }
            },
            headers={"If-Match": etag(int(api_active["spec_revision"]))},
            expected=(200,),
        )
        _, _, references = client.request(
            "GET",
            f"/admin/v1/plugins/registry/{plugin_id}/references",
            expected=(200,),
        )
        plugin_fixture = {
            "state": (
                "ready_with_replacement_artifacts"
                if args.plugin_wasm_alt and args.plugin_wasm_higher
                else "ready_base_references"
            ),
            "id": plugin_id,
            "revision": plugin.get("revision"),
            "sha256": plugin.get("sha256_hex"),
            "chain_entry_id": (
                chain.get("id") if isinstance(chain, dict) else None
            ),
            "reference_fingerprint": (
                references.get("reference_fingerprint")
                if isinstance(references, dict)
                else None
            ),
            "refcount": (
                references.get("refcount")
                if isinstance(references, dict)
                else None
            ),
            "supported_slots": sorted(supported_slots),
            "artifact": {
                "path": str(wasm_path),
                "sha256": sha256_bytes(wasm),
            },
            "replacement_artifacts": artifact_refs(
                args.plugin_wasm_alt,
                args.plugin_wasm_higher,
            ),
        }
    config_path = Path(manifest["config"]["path"])
    with config_path.open("rb") as config_file:
        draft_config = tomllib.load(config_file)
    observability = draft_config.setdefault("observability", {})
    if isinstance(observability, dict):
        observability["tracing_level"] = (
            "debug"
            if observability.get("tracing_level") != "debug"
            else "info"
        )
    _, _, draft = client.request(
        "PUT",
        "/admin/config/draft",
        body={"draft": draft_config, "expected_revision": 0},
        expected=(200,),
    )
    draft_revision = int(draft["revision"])
    _, _, validation = client.request(
        "POST",
        "/admin/config/draft/validate",
        body={"expected_revision": draft_revision},
        expected=(200,),
    )
    if not validation.get("valid"):
        raise FixtureError(
            f"generated fixture draft did not validate: {validation.get('error')}"
        )

    audit_seed = seed_audit_history(manifest, primary, api_active)
    fixtures = {
        "principal": {
            "primary": public_entity(primary),
            "disabled": public_entity(disabled),
            "delete_target": public_entity(delete_principal),
            "create_target": {"name": create_target_name},
        },
        "key": {
            "preexisting": {
                "principal_id": primary["id"],
                "key_id": key_id,
            }
        },
        "upstream": {
            "api_active": public_entity(api_active),
            "api_disabled": public_entity(api_disabled),
            "oauth_missing_credentials": public_entity(oauth_missing),
            "delete_target": public_entity(delete_upstream),
        },
        "plugin": plugin_fixture,
        "settings": {
            "draft_revision": draft_revision,
            "validated": True,
            "change": "observability.tracing_level",
        },
        "audit": audit_seed,
    }
    manifest["fixtures"] = fixtures
    manifest["prerequisite_status"] = fixture_prerequisite_status(fixtures)
    manifest["prepared_at_utc"] = utc_now()
    manifest["state"] = "populated"
    manifest["fixture_sha256"] = sha256_json(fixtures)
    atomic_json(manifest_path, manifest)
    print(f"prepared real Admin API fixture: {manifest_path}")


def artifact_refs(alt: str | None, higher: str | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, raw in (("same_name_alt", alt), ("higher_version", higher)):
        if raw:
            path = Path(raw).expanduser().resolve()
            result[key] = {"path": str(path), "sha256": sha256_bytes(path.read_bytes())}
        else:
            result[key] = None
    return result

def fixture_prerequisite_status(fixtures: Mapping[str, Any]) -> dict[str, Any]:
    plugin = fixtures.get("plugin", {}) if isinstance(fixtures, Mapping) else {}
    replacement = plugin.get("replacement_artifacts", {})
    replacement_ready = (
        isinstance(replacement, Mapping)
        and isinstance(replacement.get("same_name_alt"), Mapping)
        and isinstance(replacement.get("higher_version"), Mapping)
    )
    return {
        "plugin_base_upload_references": (
            "ready"
            if str(plugin.get("state", "")).startswith("ready")
            else "pending_artifact"
        ),
        "plugin_same_version_and_higher_replacement": (
            "ready" if replacement_ready else "pending_scratch_artifacts"
        ),
    }


def refresh_manifest(args: argparse.Namespace) -> None:
    manifest_path, manifest = load_manifest(args.manifest)
    manifest["catalog_refreshed_at_utc"] = utc_now()
    atomic_json(manifest_path, manifest)
    print(f"refreshed fixture catalog classifications: {manifest_path}")


def replace_private_text(path: Path, value: str) -> None:
    temporary = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, value.encode())
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    temporary.replace(path)


def configure_oauth(args: argparse.Namespace) -> None:
    if not args.confirm_stopped:
        raise FixtureError("configure-oauth requires --confirm-stopped")
    manifest_path, manifest = load_manifest(args.manifest)
    require_server_stopped(args.admin_url or manifest["admin_origin"])
    origin = loopback_origin(args.origin, "OAuth origin")
    snapshots = manifest.get("snapshots", {})
    selected = args.snapshot or sorted(snapshots)
    targets = [(Path(manifest["config"]["path"]), manifest["config"])]
    for name in selected:
        snapshot = snapshots.get(safe_snapshot_name(name))
        if not isinstance(snapshot, dict):
            raise FixtureError(f"unknown snapshot {name}")
        targets.append((Path(snapshot["config"]["path"]), snapshot["config"]))
    for path, record in targets:
        text = path.read_text()
        parsed = tomllib.loads(text)
        existing = parsed.get("oauth", {}).get("anthropic")
        if existing:
            expected = origin + "/oauth/authorize"
            if existing.get("auth_url") != expected:
                raise FixtureError(
                    f"refusing to replace existing OAuth configuration in {path}"
                )
        else:
            text += (
                "\n[oauth.anthropic]\n"
                'client_id = "admin-web-qa-client"\n'
                f"auth_url = {toml_string(origin + '/oauth/authorize')}\n"
                f"token_url = {toml_string(origin + '/oauth/token')}\n"
                f"redirect_uri = {toml_string(origin + '/oauth/callback')}\n"
                'scopes = ["org:profile", "user:inference"]\n'
            )
            replace_private_text(path, text)
        record["sha256"] = sha256_bytes(path.read_bytes())
    manifest["oauth_origin"] = origin
    manifest["prerequisite_status"] = fixture_prerequisite_status(
        manifest.get("fixtures", {})
    )
    manifest["prerequisite_status"]["oauth_start_loopback"] = "ready"
    manifest["oauth_configured_at_utc"] = utc_now()
    atomic_json(manifest_path, manifest)
    print(
        f"configured loopback OAuth in current config and {len(selected)} snapshot config(s)"
    )

def configure_postgres_pool(args: argparse.Namespace) -> None:
    if not args.confirm_stopped:
        raise FixtureError("configure-postgres-pool requires --confirm-stopped")
    manifest_path, manifest = load_manifest(args.manifest)
    if manifest.get("engine") != "postgres":
        raise FixtureError("configure-postgres-pool requires a PostgreSQL fixture")
    if args.postgres_container:
        manifest["database"]["postgres_container"] = validate_postgres_container(
            args.postgres_container
        )
    require_server_stopped(args.admin_url or manifest["admin_origin"])
    snapshots = manifest.get("snapshots", {})
    selected = args.snapshot or sorted(snapshots)
    targets = [(Path(manifest["config"]["path"]), manifest["config"])]
    for name in selected:
        snapshot = snapshots.get(safe_snapshot_name(name))
        if not isinstance(snapshot, dict):
            raise FixtureError(f"unknown snapshot {name}")
        targets.append((Path(snapshot["config"]["path"]), snapshot["config"]))
    configured_statements: set[int] = set()
    for path, record in targets:
        text = path.read_text()
        parsed = tomllib.loads(text)
        request_timeout = int(parsed.get("timeouts", {}).get("upstream_total_secs", 30))
        statement_timeout = min(25, request_timeout - 1)
        if statement_timeout < 1:
            raise FixtureError("request timeout is too small for a positive statement timeout")
        pool = parsed.get("storage", {}).get("pool")
        if pool is None:
            text += (
                "\n[storage.pool]\n"
                "max_connections = 10\n"
                "acquire_timeout_secs = 5\n"
                f"statement_timeout_secs = {statement_timeout}\n"
                'sslmode = "disable"\n'
            )
        else:
            current = int(pool.get("statement_timeout_secs", 30))
            if current >= request_timeout:
                pattern = re.compile(
                    r"(?m)^statement_timeout_secs\s*=\s*\d+\s*$"
                )
                if not pattern.search(text):
                    raise FixtureError(
                        f"cannot locate statement_timeout_secs in {path}"
                    )
                text = pattern.sub(
                    f"statement_timeout_secs = {statement_timeout}",
                    text,
                    count=1,
                )
        parsed_after = tomllib.loads(text)
        final_statement = int(
            parsed_after["storage"]["pool"]["statement_timeout_secs"]
        )
        if final_statement >= request_timeout:
            raise FixtureError(
                "PostgreSQL statement timeout must be less than request timeout"
            )
        replace_private_text(path, text)
        record["sha256"] = sha256_bytes(path.read_bytes())
        configured_statements.add(final_statement)
    manifest["postgres_pool"] = {
        "statement_timeout_secs": sorted(configured_statements),
        "constraint": "statement_timeout_secs < timeouts.upstream_total_secs",
    }
    manifest["postgres_pool_configured_at_utc"] = utc_now()
    atomic_json(manifest_path, manifest)
    print(
        f"configured PostgreSQL pool timeout in current config and {len(selected)} snapshot config(s)"
    )


def public_entity(value: Mapping[str, Any]) -> dict[str, Any]:
    keys = ("id", "name", "kind", "enabled", "revision", "spec_revision", "warmup_enabled")
    return {key: value[key] for key in keys if key in value}


def audit_rows(primary: Mapping[str, Any], upstream: Mapping[str, Any], now: int) -> list[tuple[Any, ...]]:
    actor_rows = [
        (now - 3600, "qa-audit-duplicate", primary["id"], "/qa/fixture/old", upstream["name"], None, 200, 0, 0, 7, None, None, None, None, "qa_fixture_old", "fixture", "static-token", "admin-web-full", "break_glass", None, "admin", json.dumps({"fixture": "old"}, separators=(",", ":"))),
        (now - 120, "qa-audit-duplicate", primary["id"], "/qa/fixture/recent", upstream["name"], "claude-sonnet-4-5-20250929", 409, 1, 2, 11, "qa", None, 1234, "fixture_conflict", "qa_fixture_recent", "fixture", "static-token", "admin-web-full", "break_glass", "qa@example.invalid", "admin", json.dumps({"fixture": "recent"}, separators=(",", ":"))),
        (now - 60, "qa-audit-system", "", "/qa/fixture/system", "", None, 503, 0, 0, 3, None, None, None, None, None, "scheduler", None, None, None, None, "system", json.dumps({"fixture": "system"}, separators=(",", ":"))),
        (now - 30, "qa-audit-other-actor", "", "/qa/fixture/external", "", None, 200, 0, 0, 2, None, None, None, None, "qa_fixture_external", "fixture", "cloudflare-access", "qa-subject", "human", "qa-user@example.invalid", "admin", json.dumps({"fixture": "external"}, separators=(",", ":"))),
    ]
    return actor_rows


def seed_audit_history(manifest: Mapping[str, Any], primary: Mapping[str, Any], upstream: Mapping[str, Any]) -> dict[str, Any]:
    now = int(time.time())
    rows = audit_rows(primary, upstream, now)
    columns = "ts,request_id,principal_id,route,upstream,model,status,input_tokens,output_tokens,duration_ms,agent_label,api_key_id,cost_usd_micros,limit_violation,admin_action,actor,actor_authority,actor_subject,actor_kind,actor_email,kind,payload"
    if manifest["engine"] == "sqlite":
        database = Path(manifest["database"]["sqlite_path"])
        connection = sqlite3.connect(database, timeout=30)
        try:
            table = connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='audit_log_v1'").fetchone()
            if table is None:
                raise FixtureError("SQLite database is not migrated: audit_log_v1 missing")
            connection.executemany(f"INSERT INTO audit_log_v1 ({columns}) VALUES ({','.join('?' for _ in range(22))})", rows)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
    else:
        url = postgres_url(manifest)
        parts = postgres_parts(url)
        values = []
        for row in rows:
            rendered = []
            for index, value in enumerate(row):
                if value is None:
                    rendered.append("NULL")
                elif index == 0:
                    rendered.append(f"to_timestamp({int(value)})")
                elif index == 21:
                    rendered.append(
                        "decode(" + pg_quote(str(value).encode().hex()) + ",'hex')"
                    )
                else:
                    rendered.append(pg_quote(value))
            values.append("(" + ",".join(rendered) + ")")
        postgres_psql(
            manifest,
            parts,
            f"INSERT INTO audit_log_v1 ({columns}) VALUES {','.join(values)};",
        )
    return {
        "seeded_rows": len(rows),
        "request_ids": ["qa-audit-duplicate", "qa-audit-system", "qa-audit-other-actor"],
        "anchor_unix_secs": now,
        "note": "Only historical Audit diversity is direct SQL; all entity and mutation setup uses the real Admin API.",
    }


def pg_quote(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def postgres_url(manifest: Mapping[str, Any]) -> str:
    env_name = manifest["database"]["postgres_url_env"]
    value = os.environ.get(env_name, "").strip()
    if not value:
        raise FixtureError(f"missing PostgreSQL URL environment variable {env_name}")
    return value


def pg_environment(parts: Mapping[str, Any]) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "PGHOST": str(parts["host"]),
            "PGPORT": str(parts["port"]),
            "PGDATABASE": str(parts["database"]),
        }
    )
    if parts.get("user"):
        environment["PGUSER"] = str(parts["user"])
    if parts.get("password"):
        environment["PGPASSWORD"] = str(parts["password"])
    return environment


def postgres_container(manifest: Mapping[str, Any]) -> str:
    raw = manifest.get("database", {}).get("postgres_container")
    if not isinstance(raw, str) or not raw:
        raise FixtureError(
            "PostgreSQL host clients are unavailable and no explicit owned QA container is recorded"
        )
    return validate_postgres_container(raw)


def docker_executable() -> str:
    executable = shutil.which("docker")
    if executable is None:
        raise FixtureError("docker is required for the explicit PostgreSQL QA container")
    return executable


def postgres_container_prefix(
    manifest: Mapping[str, Any],
    parts: Mapping[str, Any],
    client: str,
) -> list[str]:
    user = str(parts.get("user") or "")
    if not user:
        raise FixtureError(
            "PostgreSQL URL must include a user for container-local clients"
        )
    return [
        docker_executable(),
        "exec",
        "-i",
        postgres_container(manifest),
        client,
        "-U",
        user,
        "-d",
        str(parts["database"]),
    ]


def postgres_psql(
    manifest: Mapping[str, Any],
    parts: Mapping[str, Any],
    sql: str,
) -> None:
    executable = shutil.which("psql")
    if executable:
        command = [executable, "-v", "ON_ERROR_STOP=1", "-f", "-"]
        environment = pg_environment(parts)
    else:
        command = postgres_container_prefix(manifest, parts, "psql") + [
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            "-",
        ]
        environment = os.environ.copy()
    subprocess.run(
        command,
        input=sql,
        text=True,
        check=True,
        env=environment,
        stdout=subprocess.DEVNULL,
    )


def postgres_dump(
    manifest: Mapping[str, Any],
    parts: Mapping[str, Any],
    target: Path,
) -> None:
    executable = shutil.which("pg_dump")
    if executable:
        command = [
            executable,
            "--format=custom",
            "--no-owner",
            "--no-privileges",
        ]
        environment = pg_environment(parts)
    else:
        command = postgres_container_prefix(manifest, parts, "pg_dump") + [
            "--format=custom",
            "--no-owner",
            "--no-privileges",
        ]
        environment = os.environ.copy()
    descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as output:
            subprocess.run(
                command,
                check=True,
                env=environment,
                stdout=output,
            )
    except Exception:
        target.unlink(missing_ok=True)
        raise


def postgres_restore(
    manifest: Mapping[str, Any],
    parts: Mapping[str, Any],
    source: Path,
) -> None:
    executable = shutil.which("pg_restore")
    options = [
        "--clean",
        "--if-exists",
        "--no-owner",
        "--no-privileges",
        "--exit-on-error",
    ]
    if executable:
        subprocess.run(
            [
                executable,
                *options,
                "--dbname",
                str(parts["database"]),
                str(source),
            ],
            check=True,
            env=pg_environment(parts),
            stdout=subprocess.DEVNULL,
        )
    else:
        command = postgres_container_prefix(manifest, parts, "pg_restore") + options
        with source.open("rb") as dump:
            subprocess.run(
                command,
                stdin=dump,
                check=True,
                env=os.environ.copy(),
                stdout=subprocess.DEVNULL,
            )


def snapshot_fixture(args: argparse.Namespace) -> None:
    if not args.confirm_stopped:
        raise FixtureError("snapshot requires --confirm-stopped")
    manifest_path, manifest = load_manifest(args.manifest)
    require_server_stopped(args.admin_url or manifest["admin_origin"])
    name = safe_snapshot_name(args.name)
    if name in manifest.get("snapshots", {}):
        raise FixtureError(f"snapshot is already registered: {name}")
    root = fixture_root(manifest["root"])
    snapshots = root / "snapshots"
    snapshots.mkdir(mode=0o700, exist_ok=True)
    final_directory = snapshots / name
    if final_directory.exists():
        raise FixtureError(f"snapshot directory already exists: {final_directory}")
    stage_directory = snapshots / f".{name}.{uuid.uuid4().hex}.tmp"
    stage_directory.mkdir(mode=0o700)
    snapshot_state = (
        "empty_migrated"
        if name == "empty"
        and manifest.get("state") == "initialized_not_migrated"
        else manifest.get("state")
    )
    try:
        staged_config = stage_directory / "cc-lb.toml"
        copy_mode_0600(Path(manifest["config"]["path"]), staged_config)
        if manifest["engine"] == "sqlite":
            source = Path(manifest["database"]["sqlite_path"])
            if not source.exists():
                raise FixtureError(
                    "SQLite database does not exist; start the isolated app once to apply migrations"
                )
            staged_database = stage_directory / "storage.sqlite"
            source_connection = sqlite3.connect(
                f"file:{source}?mode=ro",
                uri=True,
                timeout=30,
            )
            target_connection = sqlite3.connect(staged_database)
            try:
                source_connection.backup(target_connection)
            finally:
                target_connection.close()
                source_connection.close()
            os.chmod(staged_database, 0o600)
            database_extra: dict[str, Any] = {}
        else:
            parts = postgres_parts(postgres_url(manifest))
            staged_database = stage_directory / "database.pgdump"
            postgres_dump(manifest, parts, staged_database)
            database_extra = {"database": parts["database"]}
        config_sha = sha256_bytes(staged_config.read_bytes())
        database_sha = sha256_bytes(staged_database.read_bytes())
        stage_directory.replace(final_directory)
    except Exception:
        shutil.rmtree(stage_directory, ignore_errors=True)
        raise
    final_config = final_directory / staged_config.name
    final_database = final_directory / staged_database.name
    snapshot = {
        "created_at_utc": utc_now(),
        "state": snapshot_state,
        "fixture_sha256": manifest.get("fixture_sha256"),
        "config": {
            "path": str(final_config),
            "sha256": config_sha,
        },
        "database": {
            "path": str(final_database),
            "sha256": database_sha,
            **database_extra,
        },
        "atomic_directory": str(final_directory),
    }
    manifest["state"] = snapshot_state
    manifest.setdefault("snapshots", {})[name] = snapshot
    try:
        atomic_json(manifest_path, manifest)
    except Exception:
        shutil.rmtree(final_directory, ignore_errors=True)
        raise
    print(f"captured isolated snapshot {name}")


def safe_snapshot_name(raw: str) -> str:
    if not raw or any(
        character
        not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        for character in raw
    ):
        raise FixtureError(
            "snapshot name may contain only letters, numbers, underscore, and hyphen"
        )
    return raw


def copy_mode_0600(source: Path, target: Path) -> None:
    if target.exists():
        raise FixtureError(f"refusing to overwrite {target}")
    temporary = target.with_suffix(target.suffix + f".{uuid.uuid4().hex}.tmp")
    shutil.copyfile(source, temporary)
    os.chmod(temporary, 0o600)
    temporary.replace(target)


def require_server_stopped(origin: str) -> None:
    parsed = urllib.parse.urlsplit(loopback_origin(origin, "Admin API URL"))
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        connection = socket.create_connection(
            (parsed.hostname, port),
            timeout=0.5,
        )
    except ConnectionRefusedError:
        return
    except (TimeoutError, OSError) as error:
        raise FixtureError(
            "cannot prove the isolated Admin API is stopped"
        ) from error
    connection.close()
    raise FixtureError("isolated Admin API is still reachable; stop cc-lb before reset")


def reset_fixture(args: argparse.Namespace) -> None:
    if not args.confirm_stopped:
        raise FixtureError("reset requires --confirm-stopped")
    manifest_path, manifest = load_manifest(args.manifest)
    require_server_stopped(args.admin_url or manifest["admin_origin"])
    name = safe_snapshot_name(args.name)
    snapshot = manifest.get("snapshots", {}).get(name)
    if not isinstance(snapshot, dict):
        raise FixtureError(f"unknown snapshot {name}")
    config_source = Path(snapshot["config"]["path"])
    if sha256_bytes(config_source.read_bytes()) != snapshot["config"]["sha256"]:
        raise FixtureError("config snapshot checksum mismatch")
    config_target = Path(manifest["config"]["path"])
    restore_file(config_source, config_target)
    database_source = Path(snapshot["database"]["path"])
    if sha256_bytes(database_source.read_bytes()) != snapshot["database"]["sha256"]:
        raise FixtureError("database snapshot checksum mismatch")
    if manifest["engine"] == "sqlite":
        target = Path(manifest["database"]["sqlite_path"])
        restore_file(database_source, target)
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(target) + suffix)
            if sidecar.exists():
                sidecar.unlink()
    else:
        parts = postgres_parts(postgres_url(manifest))
        if parts["database"] != snapshot["database"]["database"]:
            raise FixtureError(
                "snapshot database name does not match current QA database"
            )
        postgres_restore(manifest, parts, database_source)
    manifest["state"] = snapshot.get("state", "restored")
    manifest["last_reset"] = {
        "name": name,
        "at_utc": utc_now(),
        "fixture_sha256": snapshot.get("fixture_sha256"),
    }
    atomic_json(manifest_path, manifest)
    print(f"restored isolated snapshot {name}; restart cc-lb before use")


def restore_file(source: Path, target: Path) -> None:
    if not source.exists():
        raise FixtureError(f"snapshot artifact missing: {source}")
    temporary = target.with_suffix(target.suffix + f".{uuid.uuid4().hex}.restore")
    shutil.copyfile(source, temporary)
    os.chmod(temporary, 0o600)
    temporary.replace(target)


def proof_fixture(args: argparse.Namespace) -> None:
    manifest_path, manifest = load_manifest(args.manifest)
    token = read_secret(Path(manifest["secret_refs"][TOKEN_ENV]))
    origin = args.admin_url or manifest["admin_origin"]
    client = ApiClient(origin, token, args.timeout)
    fixtures = manifest.get("fixtures", {})
    checks: list[dict[str, Any]] = []

    def check(
        check_id: str,
        method: str,
        path: str,
        expected: Sequence[int] = (200,),
    ) -> Any:
        started = utc_now()
        status, headers, value = client.request(
            method,
            path,
            expected=expected,
        )
        checks.append(
            {
                "id": check_id,
                "method": method,
                "path": path,
                "started_at_utc": started,
                "ended_at_utc": utc_now(),
                "status": status,
                "body_sha256": sha256_json(value),
                "etag": headers.get("etag"),
                "counts": public_counts(value),
            }
        )
        return value

    check("health", "GET", "/admin/health")
    check("auth-session", "GET", "/admin/v1/auth/session")
    principals = check("principals", "GET", "/admin/v1/principals")
    upstreams = check("upstreams", "GET", "/admin/v1/upstreams")
    plugins = check("plugins", "GET", "/admin/v1/plugins/registry")
    draft_state = check("config-draft", "GET", "/admin/config/draft")
    check("config-history", "GET", "/admin/config/history")
    audit = check("audit-all", "GET", "/admin/audit?limit=1000")
    primary_id = fixtures.get("principal", {}).get("primary", {}).get("id")
    key_id = fixtures.get("key", {}).get("preexisting", {}).get("key_id")
    key_list = None
    if primary_id:
        check(
            "principal-primary",
            "GET",
            f"/admin/v1/principals/{primary_id}",
        )
        key_list = check(
            "principal-keys",
            "GET",
            f"/admin/v1/principals/{primary_id}/keys",
        )
        check(
            "router-terminal",
            "GET",
            f"/admin/v1/principals/{primary_id}/router-terminal",
        )
    plugin_id = fixtures.get("plugin", {}).get("id")
    plugin_references = None
    if plugin_id:
        plugin_references = check(
            "plugin-references",
            "GET",
            f"/admin/v1/plugins/registry/{plugin_id}/references",
        )
    assert_fixture_invariants(
        manifest,
        principals,
        upstreams,
        plugins,
        draft_state,
        audit,
        key_list,
        plugin_references,
    )
    wrong = ApiClient(
        origin,
        "intentionally-wrong-admin-web-qa-token",
        args.timeout,
    )
    status, _, value = wrong.request(
        "GET",
        "/admin/v1/auth/session",
        expected=(401,),
    )
    checks.append(
        {
            "id": "auth-401",
            "method": "GET",
            "path": "/admin/v1/auth/session",
            "status": status,
            "body_sha256": sha256_json(value),
        }
    )
    proof = {
        "created_at_utc": utc_now(),
        "manifest_sha256_before_proof": sha256_bytes(manifest_path.read_bytes()),
        "fixture_sha256": manifest.get("fixture_sha256"),
        "entity_counts": {
            "principals": len(principals.get("principals", [])),
            "upstreams": len(upstreams.get("upstreams", [])),
            "plugins": len(
                [
                    entry
                    for entry in plugins.get("entries", [])
                    if not entry.get("is_builtin")
                ]
            ),
        },
        "preexisting_key_id": key_id,
        "checks": checks,
        "blockers": manifest["blockers"],
        "not_applicable": manifest["not_applicable"],
        "prerequisite_status": manifest["prerequisite_status"],
        "note": "Several sensitive GET proof checks append Audit entries by product design. Reset the populated snapshot before a timing or direct-vs-UI mutation comparison.",
    }
    output = (
        Path(args.output).expanduser().resolve()
        if args.output
        else fixture_root(manifest["root"]) / "proof.json"
    )
    if output.exists() and not args.replace:
        raise FixtureError(f"proof already exists: {output}; pass --replace")
    atomic_json(output, proof)
    print(f"wrote redacted fixture proof: {output}")


def assert_fixture_invariants(
    manifest: Mapping[str, Any],
    principals: Mapping[str, Any],
    upstreams: Mapping[str, Any],
    plugins: Mapping[str, Any],
    draft_state: Mapping[str, Any],
    audit: Mapping[str, Any],
    key_list: Mapping[str, Any] | None,
    plugin_references: Mapping[str, Any] | None,
) -> None:
    fixtures = manifest.get("fixtures", {})
    if manifest.get("state") != "populated":
        raise FixtureError("proof requires a populated fixture snapshot/state")
    expected_principals = {
        item.get("id")
        for key, item in fixtures.get("principal", {}).items()
        if key != "create_target" and isinstance(item, dict)
    }
    actual_principals = {
        item.get("id") for item in principals.get("principals", [])
    }
    if not expected_principals or not expected_principals.issubset(
        actual_principals
    ):
        raise FixtureError("principal fixture IDs are missing from the Admin API")
    expected_upstreams = {
        item.get("id")
        for item in fixtures.get("upstream", {}).values()
        if isinstance(item, dict)
    }
    actual_upstreams = {
        item.get("id") for item in upstreams.get("upstreams", [])
    }
    if not expected_upstreams or not expected_upstreams.issubset(
        actual_upstreams
    ):
        raise FixtureError("upstream fixture IDs are missing from the Admin API")
    expected_draft_revision = fixtures.get("settings", {}).get(
        "draft_revision"
    )
    if draft_state.get("revision") != expected_draft_revision:
        raise FixtureError(
            "config draft revision does not match fixture manifest"
        )
    if draft_state.get("last_validated_revision") != expected_draft_revision:
        raise FixtureError(
            "config draft is not validated at its current revision"
        )
    expected_key = fixtures.get("key", {}).get("preexisting", {}).get("key_id")
    if expected_key and (
        not key_list
        or expected_key
        not in {item.get("key_id") for item in key_list.get("keys", [])}
    ):
        raise FixtureError("preexisting key is missing from key list")
    expected_audit_ids = set(
        fixtures.get("audit", {}).get("request_ids", [])
    )
    actual_audit_ids = {
        item.get("request_id") for item in audit.get("entries", [])
    }
    if not expected_audit_ids.issubset(actual_audit_ids):
        raise FixtureError("seeded Audit history rows are missing")
    plugin_fixture = fixtures.get("plugin", {})
    non_builtin = [
        entry for entry in plugins.get("entries", []) if not entry.get("is_builtin")
    ]
    if str(plugin_fixture.get("state", "")).startswith("ready"):
        if len(non_builtin) != 1:
            raise FixtureError(
                "plugin fixture requires exactly one non-builtin registry entry"
            )
        if not plugin_references or plugin_references.get("refcount") != 2:
            raise FixtureError("plugin fixture must have exactly two references")
    elif non_builtin:
        raise FixtureError(
            "unexpected non-builtin plugin exists without fixture artifacts"
        )



def public_counts(value: Any) -> dict[str, int] | None:
    if not isinstance(value, dict):
        return None
    counts = {}
    for key in ("principals", "upstreams", "entries", "keys", "history", "references"):
        if isinstance(value.get(key), list):
            counts[key] = len(value[key])
    return counts or None


def show_plan(args: argparse.Namespace) -> None:
    _, manifest = load_manifest(args.manifest)
    result = {
        "required_environment_keys": manifest["required_environment_keys"],
        "secret_refs": {
            key: value
            for key, value in manifest["secret_refs"].items()
            if key != AUTH_PROVIDERS_ENV
        },
        "auth_provider_json": manifest["secret_refs"][AUTH_PROVIDERS_ENV],
        "main_owned_commands": manifest["main_owned_commands"],
        "engine_matrix": manifest["engine_matrix"],
        "secret_material_policy": manifest["secret_material_policy"],
        "execution_contract": manifest["execution_contract"],
        "inventory_reconciliation": manifest["inventory_reconciliation"],
        "coverage_groups": manifest["coverage_groups"],
        "get_side_effects": manifest["get_side_effects"],
        "error_recipes": manifest["error_recipes"],
        "scenario_refs": manifest["scenario_refs"],
        "blockers": manifest["blockers"],
        "not_applicable": manifest["not_applicable"],
        "prerequisite_status": manifest["prerequisite_status"],
        "oauth_origin": manifest.get("oauth_origin"),
        "current_state": manifest.get("state"),
        "fixture_sha256": manifest.get("fixture_sha256"),
    }
    print(json.dumps(result, sort_keys=True, indent=2))


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Isolated Admin Web mutation fixture CLI. It never starts cc-lb, Vite, a browser, fake upstreams, containers, or production services.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""Main startup environment (values stay in mode-0600 files):
  export {AUTH_PROVIDERS_ENV}='[{{"kind":"static_token","id":"{QA_NAME}","token_env":"{TOKEN_ENV}"}}]'
  export {TOKEN_ENV}="$(cat <manifest secret_refs.{TOKEN_ENV}>)"
  export {MASTER_KEY_ENV}="$(cat <manifest secret_refs.{MASTER_KEY_ENV}>)"
  # PostgreSQL only: export {CLUSTER_TOKEN_ENV}="$(cat <manifest secret_refs.{CLUSTER_TOKEN_ENV}>)"

Capture `empty` after first migration boot, then prepare and capture `populated`.
Always stop cc-lb before reset. Proof GETs intentionally add Audit rows.
""",
    )
    commands = result.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="create disposable config, secret refs, and coverage manifest")
    init.add_argument("--engine", choices=("sqlite", "postgres"), required=True)
    init.add_argument("--root", default=str(DEFAULT_ROOT))
    init.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    init.add_argument("--proxy-addr", default="127.0.0.1:53251")
    init.add_argument("--admin-addr", default="127.0.0.1:53252")
    init.add_argument("--metrics-addr", default="127.0.0.1:53253")
    init.add_argument("--postgres-url-env", default=POSTGRES_URL_ENV)
    init.add_argument(
        "--postgres-container",
        help="explicit owned QA PostgreSQL container used only when host psql/pg_dump/pg_restore are absent",
    )
    init.add_argument("--oauth-origin", help="optional loopback fake OAuth origin; metadata endpoints remain fixed and blocked")
    init.set_defaults(function=init_fixture)

    prepare = commands.add_parser("prepare", help="seed a fresh migrated fixture through the real loopback Admin API")
    prepare.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    prepare.add_argument("--admin-url")
    prepare.add_argument("--fake-upstream-url", default="http://127.0.0.1:19080")
    prepare.add_argument("--plugin-wasm", help="valid router+shape WASM for referenced-plugin baseline")
    prepare.add_argument("--plugin-wasm-alt", help="same embedded name/version and different SHA for confirmation flow")
    prepare.add_argument("--plugin-wasm-higher", help="same embedded name and higher version for automatic replacement")
    prepare.add_argument("--timeout", type=float, default=30.0)
    prepare.set_defaults(function=prepare_fixture)
    snapshot = commands.add_parser("snapshot", help="capture DB and config while cc-lb is stopped")
    snapshot.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    snapshot.add_argument("--name", required=True)
    snapshot.add_argument("--admin-url")
    snapshot.add_argument("--confirm-stopped", action="store_true")
    snapshot.set_defaults(function=snapshot_fixture)

    reset = commands.add_parser("reset", help="restore an isolated snapshot while cc-lb is stopped")
    reset.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    reset.add_argument("--name", required=True)
    reset.add_argument("--admin-url")
    reset.add_argument("--confirm-stopped", action="store_true")
    reset.set_defaults(function=reset_fixture)

    proof = commands.add_parser("proof", help="write redacted API readiness evidence")
    proof.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    proof.add_argument("--admin-url")
    proof.add_argument("--output")
    proof.add_argument("--timeout", type=float, default=30.0)
    proof.add_argument("--replace", action="store_true")
    proof.set_defaults(function=proof_fixture)

    refresh = commands.add_parser(
        "refresh-manifest",
        help="rewrite only static coverage, prerequisite, blocker, and not-applicable classifications",
    )
    refresh.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    refresh.set_defaults(function=refresh_manifest)

    oauth = commands.add_parser(
        "configure-oauth",
        help="append loopback OAuth endpoints to current and selected snapshot configs while cc-lb is stopped",
    )
    oauth.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    oauth.add_argument("--origin", default="http://127.0.0.1:19081")
    oauth.add_argument("--snapshot", action="append", default=[])
    oauth.add_argument("--admin-url")
    oauth.add_argument("--confirm-stopped", action="store_true")
    oauth.set_defaults(function=configure_oauth)

    postgres_pool = commands.add_parser(
        "configure-postgres-pool",
        help="set a product-valid statement timeout in current and selected PostgreSQL snapshot configs",
    )
    postgres_pool.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    postgres_pool.add_argument("--snapshot", action="append", default=[])
    postgres_pool.add_argument(
        "--postgres-container",
        help="record an explicit owned QA PostgreSQL container for client fallback",
    )
    postgres_pool.add_argument("--admin-url")
    postgres_pool.add_argument("--confirm-stopped", action="store_true")
    postgres_pool.set_defaults(function=configure_postgres_pool)

    plan = commands.add_parser("plan", help="print execution commands, coverage map, and blockers")
    plan.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    plan.set_defaults(function=show_plan)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        args.function(args)
        return 0
    except (FixtureError, OSError, sqlite3.Error, subprocess.CalledProcessError, KeyError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
