"""Select an aggregation field, then invoke the preserved page mapper."""

import sys
import mapper


def selected_records(stream, mode):
    index = {"page": 2, "ip": 0, "status": 3}[mode]
    for line in stream:
        fields = line.split()
        if not fields:
            continue
        if len(fields) != 4:
            raise ValueError("Expected a validated four-column record")
        fields[2] = fields[index]
        yield " ".join(fields) + "\n"


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("page", "ip", "status"):
        raise SystemExit("Usage: python3 streaming_mapper.py page|ip|status")
    sys.stdin = selected_records(sys.stdin, sys.argv[1])
    mapper.main()
