#!/usr/bin/env python3
from pathlib import Path
import argparse
import gzip
import shutil

def main():
    p = argparse.ArgumentParser(description="Decompress *.json.gz files to *.json")
    p.add_argument("root_path", help="Directory to scan, e.g. ./data/raw")
    p.add_argument("--delete-gz", action="store_true", help="Delete original .gz after success")
    args = p.parse_args()

    root = Path(args.root_path).expanduser().resolve()
    for src in root.rglob("*.json.gz"):
        dst = src.with_suffix("")  # removes only .gz
        if dst.exists():
            print(f"Skip (exists): {dst}")
            continue
        with gzip.open(src, "rb") as fin, open(dst, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        print(f"Decompressed: {src} -> {dst}")
        if args.delete_gz:
            src.unlink()
            print(f"Deleted: {src}")

if __name__ == "__main__":
    main()
