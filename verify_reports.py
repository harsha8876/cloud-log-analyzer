#!/usr/bin/env python3
"""Compare actual Hadoop TSV reports with counts computed from large.log."""

from collections import Counter
from pathlib import Path
import sys


BASE = Path(__file__).resolve().parent
REPORTS = ("pages", "ip", "status")


def expected_counts():
    counts = {name: Counter() for name in REPORTS}
    with (BASE / "large.log").open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            fields = line.split()
            if len(fields) != 4:
                raise ValueError(f"large.log line {line_number}: expected four fields")
            ip, timestamp, page, status = fields
            counts["pages"][page] += 1
            counts["ip"][ip] += 1
            counts["status"][status] += 1
    if sum(counts["pages"].values()) != 10_000:
        raise ValueError("large.log must contain exactly 10,000 records")
    return counts


def verify_report(name, expected):
    path = BASE / "results" / "large" / f"{name}.tsv"
    actual = {}
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 2:
                raise ValueError(f"line {line_number}: expected key<TAB>count")
            key, value = fields
            if key in actual:
                raise ValueError(f"line {line_number}: duplicate key {key!r}")
            count = int(value)
            if count < 0:
                raise ValueError(f"line {line_number}: negative count")
            actual[key] = count
    errors = []
    total = sum(actual.values())
    if total != 10_000:
        errors.append(f"total is {total}, expected 10000")
    for key in sorted(set(expected) | set(actual)):
        if key not in actual:
            errors.append(f"missing key {key!r}")
        elif key not in expected:
            errors.append(f"unexpected key {key!r}")
        elif actual[key] != expected[key]:
            errors.append(f"{key!r}: got {actual[key]}, expected {expected[key]}")
    if errors:
        raise ValueError("; ".join(errors))


def main():
    try:
        expected = expected_counts()
    except (OSError, ValueError, UnicodeError) as error:
        for name in REPORTS:
            print(f"FAIL {name}: cannot compute expected counts: {error}")
        return 1

    failed = False
    for name in REPORTS:
        try:
            verify_report(name, expected[name])
        except (OSError, ValueError, UnicodeError) as error:
            print(f"FAIL {name}: {error}")
            failed = True
        else:
            print(f"PASS {name}: every key and count matches; total = 10000")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
