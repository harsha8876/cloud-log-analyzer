#!/usr/bin/env python3
"""Emit one page/count pair for each four-field web log record."""

import sys


def main():
    for line_number, line in enumerate(sys.stdin, start=1):
        fields = line.split()
        if len(fields) != 4:
            print(
                f"Warning: malformed record on line {line_number}: "
                f"expected 4 fields, got {len(fields)}",
                file=sys.stderr,
            )
            continue
        print(f"{fields[2]}\t1")


if __name__ == "__main__":
    main()
