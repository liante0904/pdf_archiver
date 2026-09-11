"""Restore one archived PDF from its canonical brokerage URL.

Use this only when the archive metadata exists but the remote object must be
restored.  The downloaded bytes must match the SHA-256 already stored in the
archive row before this script is allowed to upload anything.

Example (run on arm2, where the WARP egress and rclone credentials live):
    uv run --env-file .env python scripts/restore_archived_pdf.py 231822639 --execute
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cloud_store import CloudStore
from pdf_archiver_v3 import (
    LOCAL_BUFFER,
    RCLONE_CONFIG,
    RCLONE_REMOTE,
    _download_wget,
    _is_pdf,
    compute_hash,
    db_connect,
    fetch_gdrive_file_id,
    upsert_archive,
)


async def restore(report_id: int, *, execute: bool) -> int:
    conn = await db_connect()
    try:
        row = await conn.fetchrow(
            '''
            SELECT s.report_id, s.firm_nm, s.article_title, s.report_date,
                   COALESCE(NULLIF(BTRIM(s.pdf_url), ''),
                            NULLIF(BTRIM(s.report_unique_key), ''),
                            NULLIF(BTRIM(s.telegram_url), '')) AS pdf_url,
                   a.storage_key, a.file_size, encode(a.pdf_hash, 'hex') AS pdf_hash
            FROM "tbl_sec_reports" s
            JOIN "tbl_sec_reports_pdf_archive" a ON a.report_id = s.report_id
            WHERE s.report_id = $1
            ''',
            report_id,
        )
        if not row:
            raise RuntimeError(f"report_id={report_id}: report/archive metadata not found")
        if not row["pdf_url"] or not row["storage_key"] or not row["pdf_hash"]:
            raise RuntimeError(f"report_id={report_id}: URL, storage_key, and stored hash are all required")

        target = LOCAL_BUFFER / "restore" / str(report_id) / Path(row["storage_key"]).name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.unlink(missing_ok=True)
        if not await _download_wget(row["pdf_url"], target):
            raise RuntimeError(f"report_id={report_id}: brokerage URL did not return a usable PDF")
        if not await _is_pdf(target):
            raise RuntimeError(f"report_id={report_id}: downloaded payload is not a PDF")

        actual_hash, actual_hash_bytes = compute_hash(target)
        if actual_hash != row["pdf_hash"]:
            raise RuntimeError(
                f"report_id={report_id}: SHA-256 mismatch; expected {row['pdf_hash']}, got {actual_hash}"
            )
        if row["file_size"] and target.stat().st_size != row["file_size"]:
            raise RuntimeError(
                f"report_id={report_id}: size mismatch; expected {row['file_size']}, got {target.stat().st_size}"
            )

        print(f"verified report_id={report_id} bytes={target.stat().st_size} sha256={actual_hash}")
        if not execute:
            print("dry run: rerun with --execute to upload the verified PDF")
            return 0

        async with CloudStore(RCLONE_REMOTE, config=RCLONE_CONFIG) as store:
            await store.upload(str(target), row["storage_key"])
        gdrive_file_id = await fetch_gdrive_file_id(row["storage_key"])
        await upsert_archive(
            conn,
            report_id,
            row["firm_nm"] or "UNKNOWN",
            row["article_title"] or "untitled",
            str(row["report_date"] or ""),
            row["pdf_url"],
            row["storage_key"],
            target.stat().st_size,
            0,
            actual_hash,
            actual_hash_bytes,
            True,
            gdrive_file_id=gdrive_file_id,
        )
        print(f"restored report_id={report_id} storage_key={row['storage_key']}")
        return 0
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Restore one verified brokerage PDF to its existing archive key")
    parser.add_argument("report_id", type=int)
    parser.add_argument("--execute", action="store_true", help="upload after URL/PDF/hash verification")
    args = parser.parse_args()
    return asyncio.run(restore(args.report_id, execute=args.execute))


if __name__ == "__main__":
    raise SystemExit(main())
