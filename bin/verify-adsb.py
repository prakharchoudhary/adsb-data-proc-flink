#!/usr/bin/env python3
"""
Usage: python verify_adsb.py <path/to/file.json.gz>
"""

import gzip
import json
import sys

def verify(path: str):
    print(f"File: {path}\n")

    try:
        with gzip.open(path, "rb") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: File not found — {path}")
        sys.exit(1)
    except (gzip.BadGzipFile, OSError):
        # Server may return plain JSON despite the .gz extension
        try:
            with open(path, "r") as f:
                data = json.load(f)
            print("NOTE: File is plain JSON (not gzip-compressed).")
        except json.JSONDecodeError as e:
            print(f"ERROR: Not a valid gzip file and not valid JSON — {e}")
            sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON — {e}")
        sys.exit(1)

    snapshot_time = data.get("now", "unknown")
    aircraft = data.get("aircraft", [])

    print(f"Snapshot time : {snapshot_time}")
    print(f"Aircraft count: {len(aircraft)}")

    if not aircraft:
        print("\nWARNING: No aircraft records in this snapshot.")
        return

    print(f"\n--- First record ---")
    print(json.dumps(aircraft[0], indent=2))

    print(f"\n--- Field coverage across all {len(aircraft)} records ---")
    field_counts: dict[str, int] = {}
    for record in aircraft:
        for key in record:
            field_counts[key] = field_counts.get(key, 0) + 1

    for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
        pct = count / len(aircraft) * 100
        print(f"  {field:<20} {count:>5} records  ({pct:.0f}%)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python verify_adsb.py <path/to/file.json.gz>")
        sys.exit(1)

    verify(sys.argv[1])