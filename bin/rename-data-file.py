
#!/usr/bin/env python3
from pathlib import Path
import argparse
import sys


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rename files ending with .json.gz to .json (extension fix only)."
    )
    parser.add_argument(
        "root_path",
        help="Root directory to scan (example: ./data/raw)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned renames without changing files.",
    )
    args = parser.parse_args()

    root = Path(args.root_path).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"Error: '{root}' is not a valid directory.", file=sys.stderr)
        return 1

    count = 0
    for src in root.rglob("*.json.gz"):
        dst = src.with_suffix("")  # removes only final ".gz" -> ".json"
        if dst.exists():
            print(f"Skip (target exists): {src} -> {dst}")
            continue

        if args.dry_run:
            print(f"Would rename: {src} -> {dst}")
        else:
            src.rename(dst)
            print(f"Renamed: {src} -> {dst}")
        count += 1

    print(f"Done. Processed {count} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
