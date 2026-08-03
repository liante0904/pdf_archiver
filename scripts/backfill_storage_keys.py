"""Restore missing Google Drive storage keys without re-downloading PDFs.

This tool intentionally separates a costly remote listing from DB writes:

1. ``--refresh-manifest`` lists the remote once and writes a local JSONL manifest.
2. The default mode creates a CSV plan from that manifest and the live DB.
3. ``--execute`` applies only uniquely matched rows from an existing manifest.

It never changes ``file_path`` (the historical source download URL), status values,
or hashes.  Ambiguous and missing matches remain untouched for later review.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import fcntl
import json
import os
import re
import shutil
import subprocess
import sys
import time
import unicodedata
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

import asyncpg

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from _bootstrap import build_postgres_dsn


ARCHIVE_TABLE = '"tbl_sec_reports_pdf_archive"'
SOURCE_TABLE = '"tbl_sec_reports"'
REPORT_ID_RE = re.compile(r"_(\d+)\.pdf$", re.IGNORECASE)
DEFAULT_DIR = Path("tmp/storage_key_backfill")
DEFAULT_MANIFEST = DEFAULT_DIR / "gdrive_manifest.jsonl"
DEFAULT_PLAN = DEFAULT_DIR / "storage_key_backfill_plan.csv"
LOCK_FILE = os.getenv("STORAGE_KEY_BACKFILL_LOCK_FILE", "/tmp/pdf_storage_key_backfill.lock")


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def rclone_bin() -> str:
    return (
        os.getenv("RCLONE_BIN")
        or (str(Path.home() / ".local/bin/rclone") if (Path.home() / ".local/bin/rclone").exists() else "")
        or shutil.which("rclone")
        or "/usr/bin/rclone"
    )


def normalize_path(value: str) -> str:
    return unicodedata.normalize("NFC", value.strip().lstrip("/"))


def extract_report_id(remote_path: str) -> int | None:
    match = REPORT_ID_RE.search(Path(remote_path).name)
    return int(match.group(1)) if match else None


@dataclass(frozen=True)
class RemoteFile:
    remote_path: str
    remote_size: int
    report_id: int


def parse_lsf_line(line: str) -> RemoteFile | None:
    """Parse ``rclone lsf --format ps`` output into a usable file record."""
    parts = line.rstrip("\n").split(";", 1)
    if len(parts) != 2:
        return None
    remote_path = normalize_path(parts[0])
    report_id = extract_report_id(remote_path)
    try:
        remote_size = int(parts[1])
    except ValueError:
        return None
    if report_id is None or remote_size <= 0:
        return None
    return RemoteFile(remote_path=remote_path, remote_size=remote_size, report_id=report_id)


def refresh_manifest(remote: str, manifest_path: Path) -> tuple[int, int]:
    """Write a complete, immutable-for-this-run remote file listing to JSONL."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [rclone_bin(), "lsf", "-R", "--format", "ps", "--files-only", remote]
    log(f"Scanning remote once: {' '.join(cmd[:-1])} {remote}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert proc.stdout is not None
    files = skipped = 0
    with manifest_path.open("w", encoding="utf-8") as fp:
        for line in proc.stdout:
            item = parse_lsf_line(line)
            if item is None:
                skipped += 1
                continue
            fp.write(json.dumps(asdict(item), ensure_ascii=False) + "\n")
            files += 1
    stderr = proc.stderr.read() if proc.stderr else ""
    if proc.wait() != 0:
        raise RuntimeError(stderr.strip() or "rclone remote listing failed")
    log(f"Manifest saved: files_with_report_id={files} skipped={skipped} path={manifest_path}")
    return files, skipped


def load_manifest(manifest_path: Path) -> dict[int, list[RemoteFile]]:
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}. Run with --refresh-manifest first.")
    by_report_id: dict[int, list[RemoteFile]] = defaultdict(list)
    with manifest_path.open(encoding="utf-8") as fp:
        for lineno, line in enumerate(fp, start=1):
            try:
                raw = json.loads(line)
                item = RemoteFile(
                    remote_path=normalize_path(str(raw["remote_path"])),
                    remote_size=int(raw["remote_size"]),
                    report_id=int(raw["report_id"]),
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(f"Invalid manifest line {lineno}: {exc}") from exc
            by_report_id[item.report_id].append(item)
    return by_report_id


async def fetch_candidates(conn: asyncpg.Connection, limit: int | None) -> list[dict]:
    limit_clause = "" if limit is None else "LIMIT $1"
    rows = await conn.fetch(
        f"""
        SELECT a.report_id, a.firm_nm, a.file_name, a.file_size, a.updated_at
        FROM {ARCHIVE_TABLE} a
        JOIN {SOURCE_TABLE} s USING (report_id)
        WHERE a.archive_status = 'ARCHIVED'
          AND a.pdf_sync_status = 2
          AND NULLIF(BTRIM(a.storage_key), '') IS NULL
        ORDER BY a.report_id ASC
        {limit_clause}
        """,
        *(() if limit is None else (limit,)),
    )
    return [dict(row) for row in rows]


def build_plan(candidates: list[dict], remote_by_id: dict[int, list[RemoteFile]]) -> list[dict]:
    plan: list[dict] = []
    for row in candidates:
        report_id = int(row["report_id"])
        matches = remote_by_id.get(report_id, [])
        if len(matches) == 1:
            item = matches[0]
            state = "matched"
            remote_path, remote_size = item.remote_path, item.remote_size
        elif not matches:
            state, remote_path, remote_size = "missing", "", ""
        else:
            state = "ambiguous"
            remote_path = " | ".join(sorted(item.remote_path for item in matches))
            remote_size = ""
        plan.append(
            {
                "report_id": report_id,
                "firm_nm": row.get("firm_nm") or "",
                "state": state,
                "remote_path": remote_path,
                "remote_size": remote_size,
                "match_count": len(matches),
            }
        )
    return plan


def write_plan(plan: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["report_id", "firm_nm", "state", "remote_path", "remote_size", "match_count"])
        writer.writeheader()
        writer.writerows(plan)


async def apply_matches(conn: asyncpg.Connection, plan: list[dict], batch_size: int) -> tuple[int, int]:
    matches = [row for row in plan if row["state"] == "matched"]
    applied = skipped = 0
    for start in range(0, len(matches), batch_size):
        async with conn.transaction():
            for row in matches[start : start + batch_size]:
                result = await conn.execute(
                    f"""
                    UPDATE {ARCHIVE_TABLE}
                    SET storage_backend = 'googledrive',
                        storage_key = $2,
                        file_name = $3,
                        file_size = $4,
                        updated_at = NOW()
                    WHERE report_id = $1
                      AND archive_status = 'ARCHIVED'
                      AND pdf_sync_status = 2
                      AND NULLIF(BTRIM(storage_key), '') IS NULL
                    """,
                    int(row["report_id"]),
                    row["remote_path"],
                    Path(row["remote_path"]).name,
                    int(row["remote_size"]),
                )
                count = int(result.rsplit(" ", 1)[-1])
                if count == 1:
                    applied += 1
                elif count == 0:
                    skipped += 1  # v3 or another backfill changed it after planning.
                else:
                    raise RuntimeError(f"Unexpected update count: {result}")
        log(f"Applied {min(start + batch_size, len(matches))}/{len(matches)} matched rows")
    return applied, skipped


async def run(args: argparse.Namespace) -> int:
    if args.refresh_manifest:
        refresh_manifest(args.remote, args.manifest)
    remote_by_id = load_manifest(args.manifest)
    conn = await asyncpg.connect(build_postgres_dsn(), ssl=False)
    try:
        candidates = await fetch_candidates(conn, args.limit)
        plan = build_plan(candidates, remote_by_id)
        write_plan(plan, args.plan)
        counts: dict[str, int] = defaultdict(int)
        for item in plan:
            counts[item["state"]] += 1
        log(f"Plan: candidates={len(plan)} matched={counts['matched']} missing={counts['missing']} ambiguous={counts['ambiguous']} csv={args.plan}")
        if not args.execute:
            log("Dry-run only. Review the CSV, then run again with --execute using the same manifest.")
            return 0
        applied, skipped = await apply_matches(conn, plan, args.batch_size)
        log(f"Execute complete: applied={applied} skipped_after_recheck={skipped} untouched_missing={counts['missing']} untouched_ambiguous={counts['ambiguous']}")
        return 0
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill missing GDrive storage keys from a read-only remote manifest.")
    parser.add_argument("--remote", default=os.getenv("GDRIVE_REMOTE", os.getenv("RCLONE_REMOTE", "gdrive:archive/pdf")))
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--refresh-manifest", action="store_true", help="List the remote and overwrite the local manifest before planning.")
    parser.add_argument("--execute", action="store_true", help="Apply only unique report_id-to-remote-path matches.")
    parser.add_argument("--limit", type=int, help="Restrict candidate rows; useful for a pilot.")
    parser.add_argument("--batch-size", type=int, default=250)
    args = parser.parse_args()
    if args.execute and args.refresh_manifest:
        parser.error("Run --refresh-manifest as a dry-run first; execute only with a reviewed manifest.")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")

    lock = open(LOCK_FILE, "w")
    try:
        try:
            fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            log("Another storage-key backfill is already running.")
            return 0
        return asyncio.run(run(args))
    finally:
        lock.close()


if __name__ == "__main__":
    raise SystemExit(main())
