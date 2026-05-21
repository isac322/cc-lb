#!/usr/bin/env python3
import csv
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <rss.csv>", file=sys.stderr)
        return 2
    samples = []
    with open(sys.argv[1], newline="") as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            row = next(csv.reader([line]))
            if row == ["unix_time", "rss_kib"]:
                continue
            if len(row) != 2:
                continue
            samples.append(int(row[1]))
    if not samples:
        print("no samples", file=sys.stderr)
        return 1
    initial = samples[0]
    final = samples[-1]
    print(f"initial_rss_kib={initial}")
    print(f"final_rss_kib={final}")
    print(f"delta_kib={final - initial}")
    print(f"samples={len(samples)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
