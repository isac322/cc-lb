#!/usr/bin/env python3
"""Stamp and verify cc-lb Helm chart release metadata."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SOURCE_PLACEHOLDER = "cc-lb.io/source-revision: __SOURCE_REVISION__"


class ChartMetadataError(RuntimeError):
    pass


def stamp_chart(chart_yaml: Path, revision: str) -> None:
    text = chart_yaml.read_text(encoding="utf-8")
    if text.count(SOURCE_PLACEHOLDER) != 1:
        raise ChartMetadataError(
            "Chart.yaml must contain exactly one source-revision placeholder"
        )
    chart_yaml.write_text(
        text.replace(SOURCE_PLACEHOLDER, f'cc-lb.io/source-revision: "{revision}"'),
        encoding="utf-8",
    )


def chart_field(text: str, pattern: str, name: str) -> str:
    match = re.search(pattern, text, flags=re.MULTILINE)
    if not match:
        raise ChartMetadataError(f"Chart.yaml has no {name}")
    return match.group(1)


def verify_chart(chart_yaml: Path, version: str, revision: str) -> None:
    text = chart_yaml.read_text(encoding="utf-8")
    actual = (
        chart_field(text, r'^version:\s*["\']?([^"\'\s]+)', "version"),
        chart_field(text, r'^appVersion:\s*["\']?([^"\'\s]+)', "appVersion"),
        chart_field(
            text,
            r'^\s{2}cc-lb\.io/source-revision:\s*["\']?([^"\'\s]+)',
            "source revision",
        ),
    )
    expected = (version, version, revision)
    if actual != expected:
        raise ChartMetadataError(
            f"chart metadata mismatch: expected {expected}, got {actual}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stamp and verify cc-lb Helm chart release metadata."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    stamp = commands.add_parser("stamp")
    stamp.add_argument("--chart", required=True, type=Path)
    stamp.add_argument("--revision", required=True)

    verify = commands.add_parser("verify")
    verify.add_argument("--chart", required=True, type=Path)
    verify.add_argument("--version", required=True)
    verify.add_argument("--revision", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "stamp":
            stamp_chart(args.chart, args.revision)
        else:
            verify_chart(args.chart, args.version, args.revision)
    except ChartMetadataError as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
