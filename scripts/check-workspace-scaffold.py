#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
EXPECTED_PATH = ROOT / "tests/scaffold/expected-crates.txt"


def main() -> int:
    expected = {
        line.strip()
        for line in EXPECTED_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        return result.returncode

    metadata = json.loads(result.stdout)
    actual = {package["name"] for package in metadata["packages"]}
    missing = sorted(expected - actual)
    if missing:
        print("workspace is missing expected crates:", file=sys.stderr)
        for name in missing:
            print(f"- {name}", file=sys.stderr)
        return 1

    print(f"Workspace scaffold guard OK: {len(expected)} expected crates are registered.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
