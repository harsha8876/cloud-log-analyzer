#!/usr/bin/env python3
"""Sum counts for consecutive pages in sorted tab-separated input."""

import sys


def main():
    current_page = None
    total = 0
    for line in sys.stdin:
        page, count = line.rstrip("\n").split("\t")
        count = int(count)
        if current_page is not None and page != current_page:
            print(f"{current_page}\t{total}")
            total = 0
        current_page = page
        total += count
    if current_page is not None:
        print(f"{current_page}\t{total}")


if __name__ == "__main__":
    main()
