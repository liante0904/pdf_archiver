from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def _rclone_bin() -> str:
    return (
        os.getenv("RCLONE_BIN")
        or (os.path.expanduser("~/.local/bin/rclone") if os.path.exists(os.path.expanduser("~/.local/bin/rclone")) else "")
        or shutil.which("rclone")
        or "/usr/bin/rclone"
    )


def _unique_keep_paths(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as fp:
        rows = list(csv.DictReader(fp))
    paths = sorted({(row.get("keep_remote_path") or "").strip() for row in rows})
    return [path for path in paths if path and "://" not in path and not path.startswith("/")]


def _remote_size(remote: str, rel_path: str) -> int | None:
    proc = subprocess.run(
        [_rclone_bin(), "lsjson", f"{remote.rstrip('/')}/{rel_path}"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return None
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None
    if isinstance(data, list) and data:
        size = data[0].get("Size")
        return int(size) if size is not None else None
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy canonical exact-pdf_url files from OneDrive to Google Drive.")
    parser.add_argument("--input", default="tmp/dedup_plan/pdf_url_remote_delete_candidates.csv")
    parser.add_argument("--source-remote", default=os.getenv("RCLONE_REMOTE", "onedrive:/archive/pdf"))
    parser.add_argument("--dest-remote", default=os.getenv("GDRIVE_REMOTE", "gdrive:/archive/pdf"))
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    keep_paths = _unique_keep_paths(Path(args.input))
    log(f"Loaded unique canonical paths: {len(keep_paths)}")

    copied = 0
    verified = 0
    for rel_path in keep_paths:
        src = f"{args.source_remote.rstrip('/')}/{rel_path}"
        dst = f"{args.dest_remote.rstrip('/')}/{rel_path}"
        src_size = _remote_size(args.source_remote, rel_path)
        if src_size is None:
            raise RuntimeError(f"Source missing or unreadable: {src}")

        if not args.execute:
            log(f"DRY-RUN copyto {src} {dst}")
            continue

        proc = subprocess.run([_rclone_bin(), "copyto", src, dst], capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or f"rclone copyto failed: {src} -> {dst}")
        copied += 1

        dst_size = _remote_size(args.dest_remote, rel_path)
        if dst_size != src_size:
            raise RuntimeError(f"Size mismatch after copy: {rel_path} source={src_size} dest={dst_size}")
        verified += 1
        log(f"VERIFIED {rel_path} size={src_size}")

    if not args.execute:
        log("Dry-run only. Re-run with --execute to copy and verify.")
    log(f"Done. copied={copied} verified={verified} dry_run={not args.execute}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
