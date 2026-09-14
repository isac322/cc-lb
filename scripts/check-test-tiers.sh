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
  -g 'fuzz/**/*.rs' \
  -g '**/benches/**/*.rs' \
  | LC_ALL=C sort -u >"$source_files"

python3 - "$mode" "$nextest_json" "$source_files" "$metadata_json" <<'PY'
from __future__ import annotations

import csv
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


def tier_markers(segments: list[str]) -> list[str]:
    return [
        tier
        for tier in VALID_TIERS
        if any(segment.startswith(tier) for segment in segments)
    ]


def tier_from_name(name: str) -> str:
    found = tier_markers(name.split("::"))
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

def rust_code_mask(text: str) -> str:
    chars = list(text)
    index = 0
    while index < len(text):
        start = index
        if text.startswith("//", index):
            end = text.find("\n", index + 2)
            end = len(text) if end == -1 else end
        elif text.startswith("/*", index):
            depth = 1
            end = index + 2
            while end < len(text) and depth:
                if text.startswith("/*", end):
                    depth += 1
                    end += 2
                elif text.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
        else:
            raw = re.match(r'(?:b)?r(#{0,16})"', text[index:])
            if raw:
                terminator = '"' + raw.group(1)
                found = text.find(terminator, index + raw.end())
                end = len(text) if found == -1 else found + len(terminator)
            elif text[index] == '"':
                end = index + 1
                while end < len(text):
                    if text[end] == "\\":
                        end += 2
                    elif text[end] == '"':
                        end += 1
                        break
                    else:
                        end += 1
            else:
                char_literal = re.match(r"'(?:\\.|[^\\'])'", text[index:])
                if not char_literal:
                    index += 1
                    continue
                end = index + char_literal.end()
        for masked in range(start, end):
            if chars[masked] != "\n":
                chars[masked] = " "
        index = end
    return "".join(chars)


def braced_modules(text: str) -> list[tuple[str, int, int, int]]:
    masked = rust_code_mask(text)
    modules: list[tuple[str, int, int, int]] = []
    pattern = re.compile(
        r"\b(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+"
        r"([A-Za-z_][A-Za-z0-9_]*)\s*\{"
    )
    for match in pattern.finditer(masked):
        brace = match.end() - 1
        end = matching_brace(text, brace)
        if end is not None:
            modules.append((match.group(1), match.start(), brace, end))
    return modules


def module_segments_at(
    modules: list[tuple[str, int, int, int]], offset: int
) -> list[str]:
    containing = [
        (brace, name)
        for name, _start, brace, end in modules
        if brace < offset < end
    ]
    return [name for _brace, name in sorted(containing)]


def cfg_test_braced_modules(text: str) -> list[tuple[int, int]]:
    masked = rust_code_mask(text)
    cfg_test = re.compile(r"#\s*\[cfg\([^]]*\btest\b[^]]*\)\]")
    decorated_module = re.compile(
        r"(?:\s|#\s*\[[^]]*\])*"
        r"(?:pub(?:\s*\([^)]*\))?\s+)?"
        r"mod\s+[A-Za-z_][A-Za-z0-9_]*\s*\{"
    )
    ranges: set[tuple[int, int]] = set()
    for match in cfg_test.finditer(masked):
        module = decorated_module.match(masked, match.end())
        if not module:
            continue
        brace = module.end() - 1
        end = matching_brace(text, brace)
        if end is not None:
            ranges.add((match.start(), end))
    return sorted(ranges)


def mask_ranges(text: str, ranges: list[tuple[int, int]]) -> str:
    if not ranges:
        return text
    chars = list(text)
    for start, end in ranges:
        for index in range(start, end):
            if chars[index] != "\n":
                chars[index] = " "
    return "".join(chars)


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
    (
        "silent-skip",
        r"(?i:eprintln!\(\s*\"skip)"
        r"|return\s+Ok\(\(\)\)\s*;?\s*//[^\n]*skip"
        r"|(?:async\s+)?fn\s+\w+\s*\([^)]*\)\s*->\s*[^{;]*Option\s*<[^>{;]*Fixture\b"
        r"|let\s+Some\([^)]*\)\s*=.*?else\s*\{\s*return\s+Ok\(\(\)\)",
        "silent test skip",
    ),
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
T4 = [
    ("subprocess", r"process::Command|CARGO_BIN_EXE|spawn_test_server", "subprocess in T4"),
]

def directory_tier(rel: str) -> str:
    if "/tests/" not in f"/{rel}" and not rel.startswith("tests/"):
        return ""
    match = re.search(r"/(t2|t3|t4|t5)/", f"/{rel}")
    return f"{match.group(1)}__" if match else ""


def path_tier_claim(rel: str) -> str:
    if (
        rel.startswith(("tests/load/", "tests/loom/", "tests/stress-suite/", "fuzz/"))
        or rel.startswith("benches/")
        or "/benches/" in rel
    ):
        return "tx__"
    return directory_tier(rel)


def is_test_source(rel: str) -> bool:
    return (
        "/tests/" in f"/{rel}"
        or rel.endswith("_tests.rs")
        or rel.endswith("/tests.rs")
        or rel.startswith("crates/cc-lb-testkit/src/")
        or path_tier_claim(rel) == "tx__"
    )

# Fixture crates are production dependencies of higher-tier tests despite their
# location under tests/. Their #[test] blocks and cfg(test) helpers are still
# checked against their own tier claims below.
def is_fixture_production_source(rel: str) -> bool:
    return bool(re.fullmatch(r"tests/fixtures/[^/]+/src/.+\.rs", rel))


def patterns_for_tier(tier: str):
    patterns = [
        pattern
        for pattern in COMMON
        if not (tier == "tx__" and pattern[0] == "elapsed-assert")
    ]
    if tier in {"", "t2__"}:
        patterns += FAST
    elif tier in {"t3__", "t3_postgres__"}:
        patterns += T3
    elif tier == "t4__":
        patterns += T4
    return patterns


def resolve_external_module(
    rel: str,
    module_name: str,
    explicit_path: str | None,
    enclosing_modules: list[str],
    source_set: set[str],
) -> str | None:
    parent = ROOT / rel
    is_integration_root = bool(
        re.fullmatch(r"crates/[^/]+/tests/[^/]+\.rs", rel)
        or re.fullmatch(r"tests/[^/]+\.rs", rel)
    )
    is_binary_root = bool(re.fullmatch(r"crates/[^/]+/src/bin/[^/]+\.rs", rel))
    root_style = parent.name in {"lib.rs", "main.rs", "mod.rs"} or is_integration_root or is_binary_root
    module_base = parent.parent if root_style else parent.parent / parent.stem
    module_base = module_base.joinpath(*enclosing_modules)

    candidates: list[pathlib.Path] = []
    if explicit_path:
        candidates.extend((parent.parent / explicit_path, module_base / explicit_path))
    else:
        candidates.extend(
            (
                module_base / f"{module_name}.rs",
                module_base / module_name / "mod.rs",
            )
        )
    for candidate in candidates:
        try:
            candidate_rel = candidate.resolve().relative_to(ROOT.resolve()).as_posix()
        except ValueError:
            continue
        if candidate_rel in source_set:
            return candidate_rel
    return None


nextest_names = load_nextest(NEXTEST_PATH)
findings: list[Finding] = []
for name in nextest_names:
    tiers = tier_markers(name.split("::"))
    if len(tiers) > 1:
        findings.append(Finding("no-seam", "<nextest>", 0, f"multiple tier segments: {name}"))

source_paths = [line for line in pathlib.Path(SOURCE_LIST).read_text().splitlines() if line]
source_set = set(source_paths)
source_texts = {
    rel: (ROOT / rel).read_text(errors="replace")
    for rel in source_paths
    if rel.endswith(".rs")
}
source_modules = {rel: braced_modules(text) for rel, text in source_texts.items()}
test_records: dict[str, list[tuple[str, str, int, str, list[str]]]] = collections.defaultdict(list)

for rel, text in source_texts.items():
    path_claim = path_tier_claim(rel)
    for fn_name, block, offset in extract_test_blocks(text):
        segments = module_segments_at(source_modules[rel], offset) + [fn_name]
        markers = tier_markers(segments)
        if len(markers) > 1:
            findings.append(
                Finding(
                    "no-seam",
                    rel,
                    line_number(text, offset),
                    f"multiple tier segments in source test: {'::'.join(segments)}",
                )
            )
        tier = markers[0] if len(markers) == 1 else path_claim
        test_records[rel].append((fn_name, block, offset, tier, segments))

module_edges: list[tuple[str, str, int, tuple[str, ...]]] = []
external_module = re.compile(
    r"(?P<attrs>(?:\s*#\s*\[[^]]*\]\s*)*)"
    r"(?:pub(?:\s*\([^)]*\))?\s+)?"
    r"mod\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*;"
)
path_attr = re.compile(r'#\s*\[\s*path\s*=\s*"([^"]+)"\s*\]')
for rel, text in source_texts.items():
    masked = rust_code_mask(text)
    for declaration in external_module.finditer(masked):
        module_name = declaration.group("name")
        attrs = text[declaration.start("attrs"):declaration.end("attrs")]
        explicit = path_attr.search(attrs)
        enclosing = module_segments_at(source_modules[rel], declaration.start())
        child = resolve_external_module(
            rel,
            module_name,
            explicit.group(1) if explicit else None,
            enclosing,
            source_set,
        )
        if child is None:
            continue
        markers = tier_markers(enclosing + [module_name])
        if len(markers) > 1:
            findings.append(
                Finding(
                    "no-seam",
                    rel,
                    line_number(text, declaration.start()),
                    f"multiple tier segments in module path: {'::'.join(enclosing + [module_name])}",
                )
            )
        module_edges.append(
            (rel, child, declaration.start(), tuple(markers if len(markers) == 1 else ()))
        )

module_contexts: dict[str, set[str]] = collections.defaultdict(set)
context_changed = True
while context_changed:
    context_changed = False
    for parent, child, _offset, hinted in module_edges:
        before = len(module_contexts[child])
        if hinted:
            module_contexts[child].update(hinted)
        else:
            module_contexts[child].update(module_contexts[parent])
        context_changed |= before != len(module_contexts[child])

# Tests in an external tier-marked module inherit that module segment even
# though the segment is not written again in the child source file.
normalized_records: dict[
    str, list[tuple[str, str, int, str, list[str]]]
] = collections.defaultdict(list)
file_tiers = collections.defaultdict(set)
for rel, records in test_records.items():
    path_claim = path_tier_claim(rel)
    if path_claim:
        file_tiers[rel].add(path_claim)
    if rel.startswith("crates/cc-lb-testkit/src/"):
        file_tiers[rel].add("t2__")
    inherited = module_contexts[rel]
    for fn_name, block, offset, tier, segments in records:
        if not tier and len(inherited) == 1:
            tier = next(iter(inherited))
        normalized_records[rel].append((fn_name, block, offset, tier, segments))
        file_tiers[rel].add(tier)
test_records = normalized_records

# Support-only files have no test record from which to receive their fixed
# classification, so retain explicit path/testkit claims as association seeds.
for rel in source_texts:
    path_claim = path_tier_claim(rel)
    if path_claim:
        file_tiers[rel].add(path_claim)
    if rel.startswith("crates/cc-lb-testkit/src/"):
        file_tiers[rel].add("t2__")

# A helper inherits only from the module that includes it. Tier-marked children
# keep their direct claim; unmarked children receive the parent's complete
# inherited context. Child claims never flow back to a neutral parent or across
# to sibling modules.
changed = True
while changed:
    changed = False
    for parent, child, _offset, hinted in module_edges:
        child_before = len(file_tiers[child])
        if hinted:
            file_tiers[child].update(hinted)
        else:
            file_tiers[child].update(file_tiers[parent])
        changed |= child_before != len(file_tiers[child])

for rel, text in source_texts.items():
    records = test_records[rel]
    for fn_name, block, offset, tier, segments in records:
        base_line = line_number(text, offset)
        allowed = exceptions_for(block, rel, base_line, findings)
        check_patterns(block, rel, base_line, patterns_for_tier(tier), allowed, findings)
        if tier in {"", "t2__"}:
            check_fast_context(block, rel, base_line, allowed, findings)

        expected = directory_tier(rel)
        markers = set(tier_markers(segments))
        markers.update(module_contexts[rel])
        expected_present = expected in markers or (
            expected == "t3__" and "t3_postgres__" in markers
        )
        if expected and not expected_present:
            findings.append(Finding("no-seam", rel, base_line, f"{rel} requires {expected} test prefix"))

    test_ranges = [(offset, offset + len(block)) for _name, block, offset, _tier, _segments in records]
    if is_test_source(rel):
        helper_text = mask_ranges(text, test_ranges)
        if is_fixture_production_source(rel):
            production_helper_text = mask_ranges(helper_text, cfg_test_braced_modules(helper_text))
            allowed = exceptions_for(production_helper_text, rel, 1, findings)
            check_patterns(production_helper_text, rel, 1, COMMON, allowed, findings)
        else:
            tiers = sorted(file_tiers[rel])
            allowed = exceptions_for(helper_text, rel, 1, findings)
            patterns = patterns_for_tier(tiers[0]) if len(tiers) == 1 else COMMON
            check_patterns(helper_text, rel, 1, patterns, allowed, findings)
            continue

    for module_start, module_end in cfg_test_braced_modules(text):
        module_tiers = {
            tier
            for _name, _block, offset, tier, _segments in records
            if module_start <= offset < module_end
        }
        if not module_tiers:
            continue
        module_text = text[module_start:module_end]
        local_test_ranges = [
            (max(0, start - module_start), min(module_end, end) - module_start)
            for start, end in test_ranges
            if module_start <= start < module_end
        ]
        helper_text = mask_ranges(module_text, local_test_ranges)
        base_line = line_number(text, module_start)
        allowed = exceptions_for(helper_text, rel, base_line, findings)
        patterns = (
            patterns_for_tier(next(iter(module_tiers)))
            if len(module_tiers) == 1
            else COMMON
        )
        check_patterns(helper_text, rel, base_line, patterns, allowed, findings)


def without_cfg_test_modules(text: str) -> str:
    return mask_ranges(text, cfg_test_braced_modules(text))


# Production source rules.
production_patterns = [
    (
        "real-clock",
        re.compile(r"SystemTime::now\("),
        {
            "crates/cc-lb-clock/src/lib.rs":
                "the SystemClock adapter is the production wall-clock boundary",
            "crates/cc-lb-admin/src/auth/cloudflare_access.rs":
                "Cloudflare Access JWT expiry validation needs current time at the auth boundary",
        },
    ),
    (
        "rand",
        re.compile(r"rand::(?:make_rng|thread_rng)\("),
        {
            "crates/cc-lb-engine/src/lifecycle.rs":
                "Lifecycle owns the production default RNG; tests inject a fixed seed",
            "crates/cc-lb-server/src/chaos.rs":
                "the chaos composition boundary samples configured production faults",
            "crates/cc-lb-server/src/reconcile.rs":
                "the reconcile composition boundary owns production jitter",
        },
    ),
    (
        "env",
        re.compile(r"std::env::var\s*\("),
        {
            "crates/cc-lb-storage-conformance/src/postgres_fixture.rs":
                "the PostgreSQL fixture resolves its explicitly named CI database URL",
            "crates/cc-lb-server/src/app.rs":
                "the app composition root resolves configured startup paths and secrets",
            "crates/cc-lb-server/src/bootstrap.rs":
                "bootstrap resolves explicitly configured upstream credential variables",
            "crates/cc-lb-server/src/chaos.rs":
                "the chaos composition boundary reads its documented fault controls",
            "crates/cc-lb-admin/src/auth/mod.rs":
                "static-token auth resolves the provider's explicitly configured token_env",
        },
    ),
]
for rel in source_paths:
    if not re.match(r"crates/[^/]+/src/", rel):
        continue
    if rel.endswith("_tests.rs") or rel.endswith("/tests.rs") or "/tests/" in rel:
        continue
    text = without_cfg_test_modules(source_texts[rel])
    for code, pattern, allow_files in production_patterns:
        if rel in allow_files:
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

# The classification CSV is the committed pre-migration inventory for the
# cc-lb-server external test binary. Restrict this check to its direct Rust
# function rows: macro-generated conformance cases and narrative/non-function
# rows elsewhere in the CSV do not have a stable source identity to compare.
classification_path = ROOT / "docs/testing/classification.csv"
classification_prefix = "crates/cc-lb-server/tests/"
classification_equivalents = {
    (
        "cc-lb-server::integration::invalid_new_config_keeps_old::"
        "config_reload_accepts_plugin_unrelated_change_and_records_success"
    ): (
        "cc-lb-server::integration::t3__invalid_new_config_keeps_old::"
        "t3__config_reload_accepts_plugin_unrelated_change_and_keeps_runtime_view"
    ),
}
classification_ledger_equivalents = {
    (
        "cc-lb-server::integration::composite_signer_dispatch::"
        "router_choice_dispatches_to_matching_oauth_upstream_not_first_anthropic_direct"
    ): (
        "cc-lb-server::integration::composite_signer_dispatch::"
        "t2__router_choice_dispatches_to_matching_oauth_upstream_not_first_anthropic_direct"
    ),
    (
        "cc-lb-server::integration::dispatch_uses_resolved_upstream_base_url::"
        "dispatch_uses_resolved_upstream_base_url_not_first_route_dialect"
    ): (
        "cc-lb-server::integration::dispatch_uses_resolved_upstream_base_url::"
        "t2__dispatch_uses_resolved_upstream_base_url_not_first_route_dialect"
    ),
}


def normalized_test_leaf(name: str) -> str:
    return re.sub(r"^(?:t2__|t3_postgres__|t3__|t4__|t5__|tx__)", "", name)


current_server_leaves = {
    normalized_test_leaf(fn_name)
    for rel, records in test_records.items()
    if rel.startswith("crates/cc-lb-server/")
    for fn_name, _block, _offset, _tier, _segments in records
}
baseline_identities: list[str] = []
if not classification_path.exists():
    findings.append(Finding("no-seam", str(classification_path), 0, "classification inventory is missing"))
else:
    with classification_path.open(newline="") as classification_file:
        for row in csv.DictReader(classification_file):
            source = row.get("file", "")
            leaf = row.get("fn", "")
            if not source.startswith(classification_prefix) or not source.endswith(".rs"):
                continue
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", leaf):
                findings.append(
                    Finding(
                        "no-seam",
                        str(classification_path),
                        0,
                        f"external Rust baseline row is not a function identity: {source!r} {leaf!r}",
                    )
                )
                continue
            module = source[len(classification_prefix):-3].replace("/", "::")
            identity = f"cc-lb-server::integration::{module}::{leaf}"
            baseline_identities.append(identity)
            if normalized_test_leaf(leaf) in current_server_leaves:
                continue
            equivalent = classification_equivalents.get(identity)
            if equivalent and ledger_ref_resolves(equivalent):
                continue
            ledger_identity = classification_ledger_equivalents.get(identity, identity)
            if ledger_identity in old_identities:
                continue
            findings.append(
                Finding(
                    "weakened-assertion",
                    str(ledger_path),
                    0,
                    f"removed baseline test identity is missing from ledger old: {identity!r}",
                )
            )

for identity, count in collections.Counter(baseline_identities).items():
    if count != 1:
        findings.append(
            Finding(
                "no-seam",
                str(classification_path),
                0,
                f"classification baseline identity must be unique: {identity!r}",
            )
        )

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
