from __future__ import annotations

import argparse
import csv
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


def _read_candidates(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as fp:
        rows = list(csv.DictReader(fp))
    candidates = []
    for row in rows:
        keep_path = (row.get("keep_remote_path") or "").strip()
        delete_path = (row.get("delete_remote_path") or "").strip()
        if not keep_path or not delete_path:
            continue
        if "://" in keep_path or "://" in delete_path:
            continue
        if keep_path == delete_path:
            continue
        candidates.append(row)
    return candidates


def _rclone_check(remote: str, rel_path: str) -> bool:
    proc = subprocess.run([_rclone_bin(), "lsf", f"{remote.rstrip('/')}/{rel_path}"], capture_output=True, text=True)
    return proc.returncode == 0 and bool(proc.stdout.strip())


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete verified exact pdf_url duplicate files from OneDrive.")
    parser.add_argument("--input", default="tmp/dedup_plan/pdf_url_remote_delete_candidates.csv")
    parser.add_argument("--remote", default=os.getenv("RCLONE_REMOTE", "onedrive:/archive/pdf"))
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    candidates = _read_candidates(Path(args.input))
    log(f"Loaded delete candidates: {len(candidates)}")

    deleted = 0
    skipped = 0
    for row in candidates:
        keep_path = row["keep_remote_path"].strip()
        delete_path = row["delete_remote_path"].strip()
        if not _rclone_check(args.remote, keep_path):
            skipped += 1
            log(f"SKIP keep missing canonical_report_id={row['canonical_report_id']} keep={keep_path}")
            continue
        if not _rclone_check(args.remote, delete_path):
            skipped += 1
            log(f"SKIP delete missing duplicate_report_id={row['duplicate_report_id']} delete={delete_path}")
            continue

        full_delete = f"{args.remote.rstrip('/')}/{delete_path}"
        if not args.execute:
            log(f"DRY-RUN deletefile {full_delete}")
            continue

        proc = subprocess.run([_rclone_bin(), "deletefile", full_delete], capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or f"rclone deletefile failed for {full_delete}")
        deleted += 1
        log(f"DELETED {full_delete}")

    if not args.execute:
        log("Dry-run only. Re-run with --execute after DB alias update and Google Drive verification.")
    log(f"Done. deleted={deleted} skipped={skipped} dry_run={not args.execute}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
