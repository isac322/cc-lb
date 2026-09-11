#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

mode="strict"
if [[ "${1:-}" == "--warn" ]]; then
  mode="warn"
  shift
fi
if [[ $# -ne 0 ]]; then
  echo "usage: $0 [--warn]" >&2
  exit 2
fi

for tool in cargo rg python3; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "ERROR: required tool not found: $tool" >&2
    exit 2
  }
done
python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' || {
  echo "ERROR: python3 >= 3.11 is required (tomllib)" >&2
  exit 2
}


work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

nextest_json="$work_dir/nextest.json"
source_files="$work_dir/source-files.txt"
metadata_json="$work_dir/metadata.json"

if [[ -n "${TEST_TIER_NEXTEST_JSON:-}" ]]; then
  cp "$TEST_TIER_NEXTEST_JSON" "$nextest_json"
else
  cargo nextest list --workspace --all-features --message-format json >"$nextest_json"
fi
cargo metadata --format-version 1 >"$metadata_json"

rg --files \
  -g 'crates/*/src/**/*.rs' \
  -g 'crates/*/tests/**/*.rs' \
  -g 'tests/**/*.rs' \
  | LC_ALL=C sort -u >"$source_files"

python3 - "$mode" "$nextest_json" "$source_files" "$metadata_json" <<'PY'
from __future__ import annotations

import collections
import datetime as dt
import json
import pathlib
import re
import sys
import tomllib
from dataclasses import dataclass

MODE, NEXTEST_PATH, SOURCE_LIST, METADATA_PATH = sys.argv[1:]
ROOT = pathlib.Path.cwd()
VALID_TIERS = ("t2__", "t3__", "t3_postgres__", "t4__", "t5__", "tx__")
CODES = {
    "concrete-sqlite", "concrete-postgres", "concrete-fs", "wasmtime",
    "socket", "subprocess", "real-sleep", "real-clock", "rand", "env",
    "global-recorder", "port-handoff", "retry", "silent-skip", "ignore",
    "uncontrolled-spawn", "multi-thread", "raw-sql", "no-seam",
    "weakened-assertion", "elapsed-assert",
}
PERMANENT_EXCEPTIONS = {
    ("multi-thread", "os-thread claim"),
    ("port-handoff", "t5 process"),
}

@dataclass(frozen=True)
class Finding:
    code: str
    path: str
    line: int
    detail: str


def load_nextest(path: str) -> list[str]:
    data = json.loads(pathlib.Path(path).read_text())
    names: list[str] = []

    suites = data.get("rust-suites", {}) if isinstance(data, dict) else {}
    if isinstance(suites, dict):
        for suite_id, suite in suites.items():
            if not isinstance(suite, dict):
                continue
            binary_id = suite.get("binary-id") or suite.get("binary_id") or suite_id
            testcases = suite.get("testcases", {})
            if isinstance(testcases, dict):
                names.extend(f"{binary_id}::{name}" for name in testcases)

    if names:
        return sorted(names)

    def visit(value):
        if isinstance(value, dict):
            testcases = value.get("testcases")
            if isinstance(testcases, dict):
                names.extend(testcases)
            for key, child in value.items():
                if key in {"name", "test_name"} and isinstance(child, str) and "::" in child:
                    names.append(child)
                visit(child)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(data)
    return sorted(names)


def tier_from_name(name: str) -> str:
    segments = name.split("::")
    found = [tier for tier in VALID_TIERS if any(seg.startswith(tier) for seg in segments)]
    return found[0] if len(found) == 1 else ""


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def preceding_annotations_start(text: str, offset: int) -> int:
    start = text.rfind("\n", 0, offset) + 1
    cursor = start
    while cursor > 0:
        previous_end = cursor - 1
        previous_start = text.rfind("\n", 0, previous_end) + 1
        previous = text[previous_start:previous_end].strip()
        if previous.startswith("//") or previous.startswith("#["):
            start = previous_start
            cursor = previous_start
            continue
        break
    return start


def matching_brace(text: str, start: int) -> int | None:
    depth = 0
    index = start
    while index < len(text):
        if text.startswith("//", index):
            newline = text.find("\n", index + 2)
            index = len(text) if newline == -1 else newline + 1
            continue
        if text.startswith("/*", index):
            comment_depth = 1
            index += 2
            while index < len(text) and comment_depth:
                if text.startswith("/*", index):
                    comment_depth += 1
                    index += 2
                elif text.startswith("*/", index):
                    comment_depth -= 1
                    index += 2
                else:
                    index += 1
            continue
        raw = re.match(r'(?:b)?r(#{0,16})"', text[index:])
        if raw:
            terminator = '"' + raw.group(1)
            end = text.find(terminator, index + raw.end())
            index = len(text) if end == -1 else end + len(terminator)
            continue
        if text[index] == '"':
            index += 1
            while index < len(text):
                if text[index] == "\\":
                    index += 2
                elif text[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            continue
        char_literal = re.match(r"'(?:\\.|[^\\'])'", text[index:])
        if char_literal:
            index += char_literal.end()
            continue
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return None


def extract_test_blocks(text: str):
    test_attr = re.compile(r"#\s*\[(?:tokio::test(?:\([^]]*\))?|test|rstest(?:\([^]]*\))?|proptest)\s*\]")
    fn = re.compile(r"(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:<[^>{}]*>)?\s*\(")
    position = 0
    while True:
        match = test_attr.search(text, position)
        if not match:
            return
        cursor = match.end()
        while True:
            cursor += len(text[cursor:]) - len(text[cursor:].lstrip())
            if text.startswith("//", cursor):
                newline = text.find("\n", cursor + 2)
                cursor = len(text) if newline == -1 else newline + 1
                continue
            if text.startswith("#[", cursor):
                attribute_end = text.find("]", cursor + 2)
                if attribute_end == -1:
                    break
                cursor = attribute_end + 1
                continue
            break
        fn_match = fn.match(text, cursor)
        if not fn_match:
            position = match.end()
            continue
        brace = text.find("{", fn_match.end())
        if brace == -1:
            position = fn_match.end()
            continue
        end = matching_brace(text, brace)
        if end is None:
            position = fn_match.end()
            continue
        prefix_start = preceding_annotations_start(text, match.start())
        yield fn_match.group(1), text[prefix_start:end], prefix_start
        position = end


def exceptions_for(block: str, path: str, base_line: int, findings: list[Finding]):
    allowed: set[str] = set()
    pattern = re.compile(r"//\s*tier-allow\(([^)]+)\):\s*([^\n]*)")
    for match in pattern.finditer(block):
        code, rest = match.group(1), match.group(2).strip()
        expires_match = re.search(r"\s+until=(\d{4}-\d{2}-\d{2})\s*$", rest)
        expires = expires_match.group(1) if expires_match else None
        reason = rest[:expires_match.start()].strip() if expires_match else rest
        line = base_line + line_number(block, match.start()) - 1
        if code not in CODES:
            findings.append(Finding("no-seam", path, line, f"unknown tier-allow code {code!r}"))
            continue
        permanent = (code, reason) in PERMANENT_EXCEPTIONS
        if permanent and expires:
            findings.append(Finding("no-seam", path, line, "permanent exception must not carry until"))
            continue
        if not permanent and not expires:
            findings.append(Finding("no-seam", path, line, "temporary tier-allow requires until=YYYY-MM-DD"))
            continue
        if expires and dt.date.fromisoformat(expires) < dt.date.today():
            findings.append(Finding(code, path, line, f"expired tier-allow ({expires})"))
            continue
        allowed.add(code)
    return allowed


def check_patterns(text: str, path: str, base_line: int, patterns, allowed: set[str], findings: list[Finding]):
    for code, regex, detail in patterns:
        if code in allowed:
            continue
        match = re.search(regex, text, re.M | re.S)
        if match:
            findings.append(Finding(code, path, base_line + line_number(text, match.start()) - 1, detail))

def check_fast_context(block: str, path: str, base_line: int, allowed: set[str], findings: list[Finding]):
    if "real-sleep" not in allowed:
        sleep = re.search(r"tokio::time::(?:sleep|interval|timeout)", block)
        if sleep and not re.search(r"start_paused\s*=\s*true", block):
            findings.append(Finding("real-sleep", path, base_line + line_number(block, sleep.start()) - 1, "Tokio time without start_paused"))
    if "uncontrolled-spawn" not in allowed:
        spawn = re.search(r"tokio::spawn\s*\(", block)
        observed = re.search(r"JoinSet|\.await\b|\.abort\s*\(|oneshot::|mpsc::|Notify", block)
        if spawn and not observed:
            findings.append(Finding("uncontrolled-spawn", path, base_line + line_number(block, spawn.start()) - 1, "spawn has no join, abort, or bounded-channel observation"))



COMMON = [
    ("ignore", r"#\s*\[ignore\b", "#[ignore] is forbidden"),
    ("silent-skip", r"(?i:eprintln!\(\s*\"skip)|return\s+Ok\(\(\)\)\s*;?\s*//[^\n]*skip|Result\s*<\s*Option\s*<|let\s+Some\([^)]*\)\s*=.*?else\s*\{\s*return\s+Ok\(", "silent test skip"),
    ("retry", r"SERVER_START_ATTEMPTS|\battempts?\s*<\s*[A-Za-z0-9_]|\bfor\s+_\s+in\s+0\.\.\d+\s*\{[^}]*(?:\bsleep\b|\btry_again\b|\bretry\b)|\bloop\s*\{[^}]*\bsleep\b", "retry-until-green pattern"),
    ("port-handoff", r"reserve_addr|free_addr|TcpListener::bind\([^)]*:0[^)]*\)[^;]*\.local_addr\(\)[^;]*;\s*drop", "port handoff race"),
    ("env", r"(?:std::)?env::(?:set_var|remove_var)|std::env::var", "ambient environment access"),
    ("global-recorder", r"install_prometheus|set_global_recorder|install_recorder", "process-global metrics recorder"),
    ("real-sleep", r"std::thread::sleep", "real blocking sleep"),
    ("elapsed-assert", r"elapsed\(\)\s*[<>]|assert[^\n]*(?:p99|p50)|(?:p99|p50)[^\n]*assert", "non-functional elapsed assertion"),
]
FAST = [
    ("concrete-fs", r"(?:std|tokio)::fs::|tempfile\b", "concrete filesystem in T1/T2"),
    ("concrete-sqlite", r"open_sqlite|SqliteStorage|SqlitePool", "concrete SQLite in T1/T2"),
    ("concrete-postgres", r"sqlx::|PgPool|PostgresStorage", "concrete PostgreSQL in T1/T2"),
    ("wasmtime", r"wasmtime::", "Wasmtime in T1/T2"),
    ("socket", r"TcpListener|TcpStream|UdpSocket|wiremock|MockServer", "socket in T1/T2"),
    ("subprocess", r"process::Command|CARGO_BIN_EXE", "subprocess in T1/T2"),
    ("real-clock", r"SystemClock|SystemTime::now|std::time::Instant::now|use\s+std::time(?:::Instant|::\{[^}]*\bInstant\b)|Utc::now", "real clock in T1/T2"),
    ("rand", r"rand::|make_rng|Uuid::new_v4", "unseeded randomness in T1/T2"),
    ("multi-thread", r"flavor\s*=\s*\"multi_thread\"", "multi-thread Tokio runtime in T1/T2"),
]
T3 = [
    ("subprocess", r"process::Command|CARGO_BIN_EXE|spawn_test_server", "subprocess in T3"),
    ("real-clock", r"SystemClock", "real clock injected into T3 adapter"),
    ("retry", r"emit_until_received|startup_delay", "retry/delay in T3 protocol test"),
]
T4 = [("subprocess", r"process::Command|CARGO_BIN_EXE|spawn_test_server", "subprocess in T4")]

nextest_names = load_nextest(NEXTEST_PATH)
findings: list[Finding] = []
for name in nextest_names:
    tiers = [tier for tier in VALID_TIERS if any(seg.startswith(tier) for seg in name.split("::"))]
    if len(tiers) > 1:
        findings.append(Finding("no-seam", "<nextest>", 0, f"multiple tier segments: {name}"))

source_paths = [line for line in pathlib.Path(SOURCE_LIST).read_text().splitlines() if line]
for rel in source_paths:
    path = ROOT / rel
    try:
        text = path.read_text()
    except UnicodeDecodeError:
        continue
    if path.suffix == ".rs":
        blocks = list(extract_test_blocks(text))
        directory_match = re.search(r"/(t2|t3|t4|t5)/", f"/{rel}") if "/tests/" in f"/{rel}" or rel.startswith("tests/") else None
        for fn_name, block, offset in blocks:
            tier = tier_from_name(fn_name)
            if not tier:
                tier = f"{directory_match.group(1)}__" if directory_match else ""
            base_line = line_number(text, offset)
            allowed = exceptions_for(block, rel, base_line, findings)
            patterns = [pattern for pattern in COMMON if not (tier == "tx__" and pattern[0] == "elapsed-assert")]
            if tier in {"", "t2__"}:
                patterns += FAST
            elif tier in {"t3__", "t3_postgres__"}:
                patterns += T3
            elif tier == "t4__":
                patterns += T4
            check_patterns(block, rel, base_line, patterns, allowed, findings)
            if tier in {"", "t2__"}:
                check_fast_context(block, rel, base_line, allowed, findings)

            if directory_match:
                expected = directory_match.group(1) + "__"
                if not fn_name.startswith(expected) and not (expected == "t3__" and fn_name.startswith("t3_postgres__")):
                    findings.append(Finding("no-seam", rel, base_line, f"{rel} requires {expected} test prefix"))
        # Standalone test/helper modules receive common rules across the file.
        if "/tests/" in f"/{rel}" or rel.endswith("_tests.rs") or rel.endswith("/tests.rs"):
            allowed = exceptions_for(text, rel, 1, findings)
            check_patterns(text, rel, 1, [pattern for pattern in COMMON if pattern[0] != "elapsed-assert"], allowed, findings)

def without_cfg_test_modules(text: str) -> str:
    ranges: list[tuple[int, int]] = []
    cfg_test = re.compile(r"#\s*\[cfg\([^]]*\btest\b[^]]*\)\]")
    for match in cfg_test.finditer(text):
        module = re.search(r"\bmod\s+[A-Za-z_][A-Za-z0-9_]*\s*\{", text[match.end():])
        if not module:
            continue
        brace = match.end() + module.end() - 1
        end = matching_brace(text, brace)
        if end is not None:
            ranges.append((match.start(), end))
    if not ranges:
        return text
    chars = list(text)
    for start, end in ranges:
        for index in range(start, end):
            if chars[index] != "\n":
                chars[index] = " "
    return "".join(chars)


# Production source rules.
production_patterns = [
    ("real-clock", re.compile(r"SystemTime::now\("), {"crates/cc-lb-clock/src/lib.rs"}),
    ("rand", re.compile(r"rand::(?:make_rng|thread_rng)\("), {
        "crates/cc-lb-engine/src/lifecycle.rs",
        "crates/cc-lb-server/src/app.rs",
        "crates/cc-lb-server/src/bootstrap.rs",
        "crates/cc-lb-server/src/chaos.rs",
        "crates/cc-lb-server/src/reconcile.rs",
    }),
    ("env", re.compile(r"std::env::var"), {"crates/cc-lb-storage-conformance/src/postgres_fixture.rs"}),
]
for rel in source_paths:
    if not re.match(r"crates/[^/]+/src/", rel):
        continue
    if rel.endswith("_tests.rs") or rel.endswith("/tests.rs") or "/tests/" in rel:
        continue
    text = without_cfg_test_modules((ROOT / rel).read_text(errors="replace"))
    for code, pattern, allow_files in production_patterns:
        if rel in allow_files or (code == "env" and rel.startswith("crates/cc-lb-server/src/")):
            continue
        match = pattern.search(text)
        if match:
            findings.append(Finding(code, rel, line_number(text, match.start()), "production DI seam rule"))

# Registry is enforced for populated entries during migration and completely in strict mode.
registry_path = ROOT / "docs/testing/upper-tier-registry.toml"
ledger_path = ROOT / "docs/testing/migration-ledger.toml"
registry = tomllib.loads(registry_path.read_text()) if registry_path.exists() else {"entry": []}
ledger = tomllib.loads(ledger_path.read_text()) if ledger_path.exists() else {"meta": {"migration_open": False}}
entries = registry.get("entry", [])
journeys = {
    "proxy-messages-nonstream", "proxy-messages-stream", "proxy-count-tokens",
    "proxy-models", "admin-crud", "admin-sse", "metrics-scrape", "process-boot",
    "process-cli", "process-reload", "process-drain", "multi-replica",
}
classes = {
    "happy", "connect-failure", "timeout-before-headers", "timeout-mid-body",
    "malformed-chunk-or-eof", "half-close-rst", "downstream-cancel",
    "401-refresh-replay", "tls-handshake", "signal-drain", "restart-recovery",
    "multi-replica-notify", "parity-composition-root",
}
keys = [entry.get("key", "") for entry in entries]
for key, count in collections.Counter(keys).items():
    if not key or count != 1:
        findings.append(Finding("no-seam", str(registry_path), 0, f"registry key must be non-empty and unique: {key!r}"))
for entry in entries:
    journey, terminal_class = entry.get("journey"), entry.get("class")
    if journey not in journeys:
        findings.append(Finding("no-seam", str(registry_path), 0, f"unknown registry journey: {journey!r}"))
    if terminal_class not in classes:
        findings.append(Finding("no-seam", str(registry_path), 0, f"unknown registry class: {terminal_class!r}"))
    if entry.get("key") != f"{journey}/{terminal_class}":
        findings.append(Finding("no-seam", str(registry_path), 0, f"registry key mismatch: {entry.get('key')!r}"))
    if not entry.get("why_no_fake") or not entry.get("p0"):
        findings.append(Finding("no-seam", str(registry_path), 0, f"registry evidence missing: {entry.get('key')!r}"))

registered = [entry.get("test", "") for entry in entries if entry.get("test", "")]
for test, count in collections.Counter(registered).items():
    if count != 1:
        findings.append(Finding("no-seam", str(registry_path), 0, f"registry test duplicated: {test}"))
upper_tests = sorted(name for name in nextest_names if tier_from_name(name) in {"t4__", "t5__"})
for name in upper_tests:
    if sum(name == registered_name or name.endswith("::" + registered_name) for registered_name in registered) != 1:
        findings.append(Finding("no-seam", str(registry_path), 0, f"unregistered upper-tier test: {name}"))
for name in registered:
    if sum(test == name or test.endswith("::" + name) for test in upper_tests) != 1:
        findings.append(Finding("no-seam", str(registry_path), 0, f"registry test missing or duplicated: {name}"))
if MODE == "strict":
    for entry in entries:
        if not entry.get("test"):
            findings.append(Finding("no-seam", str(registry_path), 0, f"blank registry test: {entry.get('key')}"))

# Ledger invariants, identity resolution, and test-count loss gate.
meta = ledger.get("meta", {})
valid_actions = {"MOVE", "DELETE", "SPLIT", "MERGE"}
registry_keys = set(keys)
merge_groups: dict[tuple[str, ...], dict[str, set[str]]] = {}
delete_reduction = 0
nextest_name_set = set(nextest_names)


def ledger_ref_resolves(reference: str) -> bool:
    if reference in nextest_name_set:
        return True
    if "/" in reference and (ROOT / reference).is_file():
        return True

    # Some tests are intentionally absent from this collection because their
    # cfg is mutually exclusive with --all-features or targets another OS.
    # Accept those only when the exact leaf is declared as a test in the
    # referenced package's source.
    parts = reference.split("::")
    if len(parts) < 2:
        return False
    package, leaf = parts[0], parts[-1]
    package_prefix = f"crates/{package}/"
    declaration = re.compile(
        rf"#\[(?:tokio::)?test(?:\([^\]]*\))?\]"
        rf"(?:\s*#\[[^\]]+\])*\s*(?:async\s+)?fn\s+{re.escape(leaf)}\b"
    )
    for rel in source_paths:
        if rel.startswith(package_prefix) and declaration.search(
            (ROOT / rel).read_text(errors="replace")
        ):
            return True
    return False


old_identities: list[str] = []
for move in ledger.get("move", []):
    action = move.get("action")
    if action not in valid_actions:
        findings.append(Finding("no-seam", str(ledger_path), 0, f"unknown ledger action: {action!r}"))
        continue
    old = move.get("old", [])
    new = move.get("new", [])
    old_values = old if isinstance(old, list) else [old]
    new_values = new if isinstance(new, list) else ([new] if new else [])
    old_identities.extend(value for value in old_values if value)
    loss = move.get("loss", [])
    guardian = move.get("guardian", "")
    if loss and guardian not in registry_keys:
        findings.append(Finding("weakened-assertion", str(ledger_path), 0, f"loss requires a registered guardian: {guardian!r}"))
    if action == "DELETE":
        delete_reduction += len(old_values)
        duplicate_of = move.get("duplicate_of", "")
        if not duplicate_of:
            findings.append(Finding("weakened-assertion", str(ledger_path), 0, f"DELETE requires duplicate_of: {old_values!r}"))
        elif duplicate_of not in registry_keys and not ledger_ref_resolves(duplicate_of):
            findings.append(Finding("weakened-assertion", str(ledger_path), 0, f"DELETE duplicate_of does not resolve: {duplicate_of!r}"))
    else:
        if not new_values:
            findings.append(Finding("no-seam", str(ledger_path), 0, f"{action} requires a new identity: {old_values!r}"))
        for reference in new_values:
            if not ledger_ref_resolves(reference):
                findings.append(Finding("no-seam", str(ledger_path), 0, f"ledger target does not resolve: {reference!r}"))
    if action == "MERGE":
        group_key = tuple(sorted(new_values))
        group = merge_groups.setdefault(group_key, {"old": set(), "new": set()})
        group["old"].update(value for value in old_values if value)
        group["new"].update(value for value in new_values if value)
        if not move.get("asserted"):
            findings.append(Finding("weakened-assertion", str(ledger_path), 0, f"MERGE requires asserted expressions: {old_values!r}"))

for identity, count in collections.Counter(old_identities).items():
    if count != 1:
        findings.append(Finding("no-seam", str(ledger_path), 0, f"ledger old identity must be unique: {identity!r}"))

added_tests = meta.get("added_tests", [])
if not isinstance(added_tests, list):
    findings.append(Finding("no-seam", str(ledger_path), 0, "meta.added_tests must be an identity list"))
    added_tests = []
for identity, count in collections.Counter(added_tests).items():
    if count != 1:
        findings.append(Finding("no-seam", str(ledger_path), 0, f"added test identity must be unique: {identity!r}"))
    if identity not in nextest_name_set:
        findings.append(Finding("no-seam", str(ledger_path), 0, f"added test identity is not collected: {identity!r}"))

baseline_path = ROOT / "docs/testing/test-count-baseline.txt"
if meta.get("migration_open", False):
    if not baseline_path.exists():
        findings.append(Finding("no-seam", str(baseline_path), 0, "migration baseline is missing"))
    else:
        baseline = int(baseline_path.read_text().strip())
        merge_reduction = sum(max(0, len(group["old"]) - len(group["new"])) for group in merge_groups.values())
        required = baseline - delete_reduction - merge_reduction + len(added_tests)
        if len(nextest_names) < required:
            findings.append(Finding("weakened-assertion", str(ledger_path), 0, f"test count {len(nextest_names)} < required {required}"))
else:
    final_test_count = meta.get("final_test_count")
    if not isinstance(final_test_count, int) or final_test_count < 1:
        findings.append(Finding("no-seam", str(ledger_path), 0, "closed migration requires a positive meta.final_test_count"))
    elif len(nextest_names) < final_test_count:
        findings.append(Finding("weakened-assertion", str(ledger_path), 0, f"test count {len(nextest_names)} < closed-ledger floor {final_test_count}"))
    recorded_baseline = meta.get("baseline_test_count")
    if baseline_path.exists() and recorded_baseline != int(baseline_path.read_text().strip()):
        findings.append(Finding("no-seam", str(ledger_path), 0, "meta.baseline_test_count does not match test-count-baseline.txt"))
# cc-lb-testkit dependency boundary.
metadata = json.loads(pathlib.Path(METADATA_PATH).read_text())
packages = {pkg["name"]: pkg for pkg in metadata["packages"]}
allow = {
    "cc-lb-aead", "cc-lb-clock", "cc-lb-domain", "cc-lb-routing",
    "cc-lb-storage-api", "cc-lb-request-log", "async-trait", "bytes", "http",
    "http-body-util", "tower", "tokio", "tokio-util", "metrics", "metrics-util",
    "serde_json", "sha2", "uuid",
}
testkit = packages.get("cc-lb-testkit")
if testkit:
    for dep in testkit.get("dependencies", []):
        if dep.get("kind") is None and dep["name"] not in allow:
            findings.append(Finding("no-seam", "crates/cc-lb-testkit/Cargo.toml", 0, f"dependency not in allowlist: {dep['name']}"))
    for pkg in metadata["packages"]:
        for dep in pkg.get("dependencies", []):
            if dep["name"] == "cc-lb-testkit" and dep.get("kind") != "dev":
                findings.append(Finding("no-seam", pkg["manifest_path"], 0, "cc-lb-testkit must be a dev-dependency"))

# Deduplicate file-wide and per-test matches.
findings = sorted(set(findings), key=lambda f: (f.code, f.path, f.line, f.detail))
counts = collections.Counter(f.code for f in findings)
for finding in findings:
    location = f"{finding.path}:{finding.line}" if finding.line else finding.path
    print(f"{finding.code}\t{location}\t{finding.detail}")
print("test-tier findings: " + " ".join(f"{code}={counts[code]}" for code in sorted(counts)))

if findings and MODE == "strict":
    sys.exit(1)
PY
