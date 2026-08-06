"""Incrementally reconcile archive rows with immutable Google Drive file IDs.

Run ``--refresh-manifest`` first (read-only).  It snapshots GDrive once, then
the default mode produces a report_id-descending plan.  ``--execute`` records
only unambiguous IDs.  ``--requeue-missing`` additionally changes only the
planned absent rows to v3 retry state, bounded by ``--max-requeues``.
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
from collections import Counter, defaultdict
from pathlib import Path

import asyncpg

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from _bootstrap import build_postgres_dsn

ARCHIVE = '"tbl_sec_reports_pdf_archive"'
SOURCE = '"tbl_sec_reports"'
REPORT_ID_RE = re.compile(r"_(\d+)\.pdf$", re.I)
LOCK_FILE = "/tmp/pdf_gdrive_id_verify.lock"
DEFAULT_DIR = Path("tmp/gdrive_file_id_verify")


def rclone_bin() -> str:
    return os.getenv("RCLONE_BIN") or shutil.which("rclone") or "/usr/bin/rclone"


def report_id_from_path(path: str) -> int | None:
    match = REPORT_ID_RE.search(Path(path).name)
    return int(match.group(1)) if match else None


def refresh_manifest(remote: str, path: Path) -> int:
    """Stream ``path;size;file_id`` records; do not make any DB change."""
    path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        [rclone_bin(), "lsf", "-R", "--files-only", "--format", "psi", remote],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    assert proc.stdout is not None
    count = 0
    with path.open("w", encoding="utf-8") as fp:
        for line in proc.stdout:
            parts = line.rstrip("\n").split(";", 2)
            if len(parts) != 3:
                continue
            report_id = report_id_from_path(parts[0])
            if report_id is None or not parts[2]:
                continue
            try:
                size = int(parts[1])
            except ValueError:
                continue
            fp.write(json.dumps({"report_id": report_id, "storage_key": parts[0], "file_size": size, "gdrive_file_id": parts[2]}, ensure_ascii=False) + "\n")
            count += 1
    stderr = proc.stderr.read() if proc.stderr else ""
    if proc.wait() != 0:
        raise RuntimeError(stderr.strip() or "rclone listing failed")
    print(f"Manifest: files={count} path={path}")
    return count


def load_manifest(path: Path) -> dict[int, list[dict]]:
    if not path.is_file():
        raise FileNotFoundError(f"Manifest not found: {path}; run --refresh-manifest first")
    result: dict[int, list[dict]] = defaultdict(list)
    for line in path.open(encoding="utf-8"):
        row = json.loads(line)
        result[int(row["report_id"])].append(row)
    return result


async def candidates(conn: asyncpg.Connection, limit: int) -> list[dict]:
    rows = await conn.fetch(
        f"""SELECT s.report_id, s.firm_nm, s.report_date, a.pdf_sync_status,
                         a.archive_status, a.gdrive_file_id
              FROM {SOURCE} s JOIN {ARCHIVE} a USING (report_id)
              WHERE a.pdf_sync_status=2
                AND NULLIF(BTRIM(a.gdrive_file_id), '') IS NULL
              ORDER BY s.report_id DESC LIMIT $1""", limit)
    return [dict(row) for row in rows]


def build_plan(rows: list[dict], remote: dict[int, list[dict]]) -> list[dict]:
    plan = []
    for row in rows:
        matches = remote.get(int(row["report_id"]), [])
        state = "verified" if len(matches) == 1 else "missing" if not matches else "ambiguous"
        item = matches[0] if state == "verified" else {}
        plan.append({
            "report_id": int(row["report_id"]), "firm_nm": row["firm_nm"] or "",
            "report_date": str(row["report_date"]), "state": state,
            "storage_key": item.get("storage_key", ""), "file_size": item.get("file_size", ""),
            "gdrive_file_id": item.get("gdrive_file_id", ""),
        })
    return plan


def write_plan(plan: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["report_id", "firm_nm", "report_date", "state", "storage_key", "file_size", "gdrive_file_id"])
        writer.writeheader(); writer.writerows(plan)


async def apply(conn: asyncpg.Connection, plan: list[dict], max_requeues: int, requeue_missing: bool) -> tuple[int, int]:
    verified = [row for row in plan if row["state"] == "verified"]
    updated = 0
    for row in verified:
        result = await conn.execute(
            f"""UPDATE {ARCHIVE}
                 SET storage_backend='googledrive', storage_key=$2, file_size=$3,
                     file_name=split_part($2, '/', array_length(string_to_array($2, '/'), 1)),
                     gdrive_file_id=$4, updated_at=NOW()
                 WHERE report_id=$1 AND NULLIF(BTRIM(gdrive_file_id), '') IS NULL""",
            row["report_id"], row["storage_key"], int(row["file_size"]), row["gdrive_file_id"],
        )
        updated += int(result.rsplit(" ", 1)[-1])
    requeued = 0
    if requeue_missing:
        for row in [item for item in plan if item["state"] == "missing"][:max_requeues]:
            result = await conn.execute(
                f"""UPDATE {ARCHIVE} SET archive_status='INIT', pdf_sync_status=3, retry_count=0,
                       storage_key=NULL, file_name=NULL, file_size=NULL, updated_at=NOW()
                     WHERE report_id=$1 AND NULLIF(BTRIM(gdrive_file_id), '') IS NULL""", row["report_id"],
            )
            requeued += int(result.rsplit(" ", 1)[-1])
    return updated, requeued


async def run(args: argparse.Namespace) -> int:
    if args.refresh_manifest:
        refresh_manifest(args.remote, args.manifest)
    remote = load_manifest(args.manifest)
    conn = await asyncpg.connect(build_postgres_dsn(), ssl=False)
    try:
        plan = build_plan(await candidates(conn, args.limit), remote)
        write_plan(plan, args.plan)
        counts = Counter(row["state"] for row in plan)
        print(f"Plan: candidates={len(plan)} verified={counts['verified']} missing={counts['missing']} ambiguous={counts['ambiguous']} csv={args.plan}")
        if args.execute:
            updated, requeued = await apply(conn, plan, args.max_requeues, args.requeue_missing)
            print(f"Applied: file_ids={updated} requeued_missing={requeued}")
    finally:
        await conn.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote", default=os.getenv("GDRIVE_REMOTE", "gdrive:archive/pdf"))
    parser.add_argument("--manifest", type=Path, default=DEFAULT_DIR / "manifest.jsonl")
    parser.add_argument("--plan", type=Path, default=DEFAULT_DIR / "plan.csv")
    parser.add_argument("--refresh-manifest", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--requeue-missing", action="store_true")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--max-requeues", type=int, default=25)
    args = parser.parse_args()
    if args.limit <= 0 or args.max_requeues <= 0:
        parser.error("limits must be positive")
    lock = open(LOCK_FILE, "w")
    try:
        fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return asyncio.run(run(args))
    finally:
        lock.close()


if __name__ == "__main__":
    raise SystemExit(main())
