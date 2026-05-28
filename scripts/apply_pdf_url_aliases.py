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


def _read_candidates(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as fp:
        rows = list(csv.DictReader(fp))
    verified = []
    for row in rows:
        keep_path = (row.get("keep_remote_path") or "").strip()
        delete_path = (row.get("delete_remote_path") or "").strip()
        if not keep_path or not delete_path:
            continue
        if "://" in keep_path or keep_path.startswith("/"):
            continue
        verified.append(row)
    return verified


async def _fetch_backup(conn: asyncpg.Connection, report_ids: list[int]) -> list[dict]:
    if not report_ids:
        return []
    rows = await conn.fetch(
        f"""
        SELECT
            report_id,
            pdf_url,
            pdf_hash,
            storage_backend,
            storage_key,
            file_path,
            file_name,
            file_size,
            page_count,
            archive_status,
            download_status_yn,
            pdf_sync_status,
            sync_status,
            updated_at
        FROM {ARCHIVE_TABLE}
        WHERE report_id = ANY($1::bigint[])
        ORDER BY report_id
        """,
        report_ids,
    )
    backups = []
    for row in rows:
        item = dict(row)
        pdf_hash = item.get("pdf_hash")
        item["pdf_hash_hex"] = bytes(pdf_hash).hex() if pdf_hash else ""
        item.pop("pdf_hash", None)
        backups.append(item)
    return backups


def _write_backup(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "report_id",
        "pdf_url",
        "pdf_hash_hex",
        "storage_backend",
        "storage_key",
        "file_path",
        "file_name",
        "file_size",
        "page_count",
        "archive_status",
        "download_status_yn",
        "pdf_sync_status",
        "sync_status",
        "updated_at",
    ]
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


async def apply_aliases(input_path: Path, backup_path: Path, execute: bool) -> None:
    candidates = _read_candidates(input_path)
    duplicate_ids = [int(row["duplicate_report_id"]) for row in candidates]
    log(f"Loaded verified remote delete candidates: {len(candidates)}")

    conn = await asyncpg.connect(build_postgres_dsn())
    try:
        backups = await _fetch_backup(conn, duplicate_ids)
        _write_backup(backup_path, backups)
        log(f"Wrote archive metadata backup rows={len(backups)} path={backup_path}")

        missing = sorted(set(duplicate_ids) - {int(row["report_id"]) for row in backups})
        if missing:
            log(f"Archive rows missing and will be inserted as aliases: {missing[:20]}")

        if not execute:
            log("Dry-run only. Re-run with --execute to update archive metadata.")
            return

        skipped = 0
        async with conn.transaction():
            upserts = 0
            for row in candidates:
                duplicate_id = int(row["duplicate_report_id"])
                canonical_id = int(row["canonical_report_id"])
                keep_path = row["keep_remote_path"].strip()
                remote_size = int(row["remote_size"] or 0) or None
                file_name = Path(keep_path).name
                result = await conn.execute(
                    f"""
                    INSERT INTO {ARCHIVE_TABLE} (
                        report_id,
                        firm_nm,
                        title,
                        reg_dt,
                        pdf_url,
                        pdf_hash,
                        storage_backend,
                        storage_key,
                        file_path,
                        file_name,
                        file_size,
                        page_count,
                        archive_status,
                        download_status_yn,
                        pdf_sync_status,
                        sync_status,
                        created_at,
                        updated_at
                    )
                    SELECT
                        s_dup.report_id,
                        s_dup.firm_nm,
                        s_dup.article_title,
                        s_dup.reg_dt::text,
                        s_dup.pdf_url,
                        COALESCE(canon.pdf_hash, s_dup.pdf_hash),
                        'onedrive',
                        $2,
                        $2,
                        $3,
                        $4,
                        canon.page_count,
                        'ARCHIVED',
                        'Y',
                        2,
                        2,
                        NOW(),
                        NOW()
                    FROM {SOURCE_TABLE} s_dup
                    JOIN {SOURCE_TABLE} s_canon
                      ON s_canon.report_id = $5
                    LEFT JOIN {ARCHIVE_TABLE} canon
                      ON canon.report_id = s_canon.report_id
                    WHERE s_dup.report_id = $1
                      AND NULLIF(BTRIM(s_dup.pdf_url), '') = NULLIF(BTRIM(s_canon.pdf_url), '')
                      AND COALESCE(s_dup.pdf_sync_status, 0) = 2
                      AND COALESCE(s_canon.pdf_sync_status, 0) = 2
                    ON CONFLICT (report_id) DO UPDATE SET
                        storage_backend = 'onedrive',
                        storage_key = EXCLUDED.storage_key,
                        file_path = EXCLUDED.file_path,
                        file_name = EXCLUDED.file_name,
                        file_size = COALESCE(EXCLUDED.file_size, {ARCHIVE_TABLE}.file_size),
                        archive_status = 'ARCHIVED',
                        download_status_yn = 'Y',
                        pdf_sync_status = 2,
                        sync_status = COALESCE({ARCHIVE_TABLE}.sync_status, 2),
                        pdf_hash = COALESCE(EXCLUDED.pdf_hash, {ARCHIVE_TABLE}.pdf_hash),
                        page_count = COALESCE(EXCLUDED.page_count, {ARCHIVE_TABLE}.page_count),
                        updated_at = NOW()
                    """,
                    duplicate_id,
                    keep_path,
                    file_name,
                    remote_size,
                    canonical_id,
                )
                count = int(result.split()[-1])
                if count == 1:
                    upserts += count
                elif count == 0:
                    log(f"Skipped duplicate_report_id={duplicate_id} (canonical={canonical_id}): URL mismatch or pdf_sync_status != 2. Check manually.")
                    skipped += 1
                else:
                    raise RuntimeError(f"Unexpected upsert count {count} for duplicate_report_id={duplicate_id}")
            log(f"Committed archive alias upserts: {upserts}, skipped: {skipped}")
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply DB archive aliases for verified exact pdf_url duplicate files.")
    parser.add_argument("--input", default="tmp/dedup_plan/pdf_url_remote_delete_candidates.csv")
    parser.add_argument("--backup", default="tmp/dedup_plan/pdf_url_alias_backup.csv")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    try:
        asyncio.run(apply_aliases(Path(args.input), Path(args.backup), args.execute))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
