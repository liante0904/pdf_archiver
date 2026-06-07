"""
Hash-based DB archive alias applier.

Reads db_alias_updates.csv from plan_content_dedup.py and applies
POINT_ARCHIVE_METADATA_TO_CANONICAL_PATH updates: duplicate report_ids
get their archive metadata pointed to the canonical's storage_key/pdf_hash.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import sys
import time
from pathlib import Path

import asyncpg

from _bootstrap import build_postgres_dsn

ARCHIVE_TABLE = '"tbl_sec_reports_pdf_archive"'
SOURCE_TABLE = '"tbl_sec_reports"'


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


async def apply_aliases(input_path: Path, execute: bool) -> None:
    with input_path.open("r", encoding="utf-8", newline="") as fp:
        rows = list(csv.DictReader(fp))

    # Group by canonical_report_id for batch processing
    log(f"Loaded {len(rows)} alias candidates from {input_path}")

    conn = await asyncpg.connect(build_postgres_dsn())
    try:
        upserted = 0
        skipped = 0
        batch_size = 100

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            async with conn.transaction():
                for row in batch:
                    duplicate_id = int(row["duplicate_report_id"])
                    canonical_id = int(row["canonical_report_id"])

                    # Fetch canonical archive metadata
                    canonical = await conn.fetchrow(
                        f"""
                        SELECT storage_key, file_path, file_name, file_size,
                               pdf_hash, page_count, archive_status, pdf_sync_status
                        FROM {ARCHIVE_TABLE}
                        WHERE report_id = $1
                        """,
                        canonical_id,
                    )
                    if not canonical:
                        log(f"SKIP duplicate={duplicate_id}: canonical={canonical_id} not in archive")
                        skipped += 1
                        continue

                    canonical_hash = canonical["pdf_hash"]
                    canonical_key = canonical["storage_key"]
                    canonical_path = canonical["file_path"]
                    canonical_fname = canonical["file_name"]
                    canonical_size = canonical["file_size"]
                    canonical_pages = canonical["page_count"]

                    # Verify the hash matches
                    if canonical_hash is None:
                        log(f"SKIP duplicate={duplicate_id}: canonical={canonical_id} has no pdf_hash")
                        skipped += 1
                        continue

                    if not execute:
                        log(f"DRY-RUN alias duplicate={duplicate_id} → canonical={canonical_id}")
                        continue

                    result = await conn.execute(
                        f"""
                        UPDATE {ARCHIVE_TABLE}
                        SET storage_backend = 'onedrive',
                            storage_key = $2,
                            file_path = $3,
                            file_name = $4,
                            file_size = COALESCE($5, {ARCHIVE_TABLE}.file_size),
                            page_count = COALESCE($6, {ARCHIVE_TABLE}.page_count),
                            pdf_hash = $7,
                            archive_status = 'ARCHIVED',
                            download_status_yn = 'Y',
                            pdf_sync_status = 2,
                            updated_at = NOW()
                        WHERE report_id = $1
                        """,
                        duplicate_id,
                        canonical_key,
                        canonical_path,
                        canonical_fname,
                        canonical_size,
                        canonical_pages,
                        canonical_hash,
                    )
                    count = int(result.split()[-1])
                    if count == 1:
                        upserted += 1
                    elif count == 0:
                        log(f"SKIP duplicate={duplicate_id}: not found in archive table")
                        skipped += 1
                    else:
                        raise RuntimeError(f"Unexpected update count {count} for duplicate={duplicate_id}")

            log(f"Batch {i // batch_size + 1}: upserted={upserted} skipped={skipped}")

    finally:
        await conn.close()

    log(f"Done. upserted={upserted} skipped={skipped} execute={execute}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply hash-based DB archive aliases.")
    parser.add_argument("--input", default="tmp/dedup_plan/db_alias_updates.csv")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    try:
        asyncio.run(apply_aliases(Path(args.input), args.execute))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
