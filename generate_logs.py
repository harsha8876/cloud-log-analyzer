#!/usr/bin/env python3
"""Generate a reproducible synthetic web log next to this script."""

from datetime import datetime, timedelta
from pathlib import Path
import random


def main():
    rng = random.Random(42)
    ips = [f"192.0.2.{number}" for number in range(1, 51)]
    pages = ("/home", "/about", "/products", "/contact", "/login", "/missing")
    start = datetime(2026, 11, 1, 10, 0, 0)
    output = Path(__file__).resolve().parent / "large.log"
    with output.open("w", encoding="utf-8", newline="\n") as stream:
        for index in range(10_000):
            ip = rng.choice(ips)
            page = rng.choice(pages)
            status = 404 if page == "/missing" else (200 if rng.random() < 0.95 else 500)
            timestamp = (start + timedelta(seconds=index)).isoformat()
            stream.write(f"{ip} {timestamp} {page} {status}\n")
    print(f"Generated 10,000 records: {output}")


if __name__ == "__main__":
    main()
