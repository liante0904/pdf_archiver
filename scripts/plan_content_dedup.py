"""
비파괴적 PDF 내용 중복 제거 계획 수립 스크립트

이 스크립트는 DB의 pdf_hash 정보를 바탕으로 중복된 리포트들을 찾아내어 안전한 제거 계획을 수립합니다:
1. 동일한 pdf_hash를 가진 레코드들을 그룹화하고 '생존자'와 '중복자'를 선정합니다.
2. 중복된 레코드들의 메타데이터를 생존자의 파일 경로로 연결(Alias)하는 DB 업데이트 계획을 생성합니다.
3. 안전하게 삭제 가능한 OneDrive 파일 목록을 추출합니다.
4. 동일한 pdf_url을 가진 경우에 대해서도 중복 제거 계획을 수립할 수 있습니다.
5. 결과는 지정된 출력 디렉토리에 여러 CSV 파일로 저장됩니다.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import re
import shutil
import subprocess
import sys
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

import asyncpg

from _bootstrap import build_postgres_dsn


REPORT_ID_RE = re.compile(r"_(\d+)\.pdf$", re.I)
SOURCE_TABLE = '"tbl_sec_reports"'
ARCHIVE_TABLE = '"tbl_sec_reports_pdf_archive"'
EMPTY_SHA256_HEX = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def _hex(value: bytes | memoryview | None) -> str:
    if value is None:
        return ""
    return bytes(value).hex()


def _norm_path(value: str) -> str:
    return unicodedata.normalize("NFC", value.strip())


def _extract_report_id(path: str) -> int | None:
    match = REPORT_ID_RE.search(Path(path).name)
    return int(match.group(1)) if match else None


def _month_from_report_date(value: object) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(text) < 6:
        return ""
    return f"{text[:4]}-{text[4:6]}"


def _rclone_bin() -> str:
    return (
        os.getenv("RCLONE_BIN")
        or (os.path.expanduser("~/.local/bin/rclone") if os.path.exists(os.path.expanduser("~/.local/bin/rclone")) else "")
        or shutil.which("rclone")
        or "/usr/bin/rclone"
    )


async def _fetch_db_rows() -> list[dict]:
    conn = await asyncpg.connect(build_postgres_dsn())
    try:
        rows = await conn.fetch(
            f"""
            SELECT
                s.report_id,
                s.firm_nm,
                s.article_title AS title,
                s.report_date::text AS report_date,
                encode(s.pdf_hash, 'hex') AS pdf_hash_hex,
                s.pdf_url,
                COALESCE(s.pdf_sync_status, 0) AS source_pdf_sync_status,
                a.file_path AS archive_file_path,
                a.storage_backend,
                a.storage_key,
                a.file_size,
                a.page_count,
                COALESCE(a.pdf_sync_status, a.sync_status, 0) AS archive_pdf_sync_status
            FROM {SOURCE_TABLE} s
            LEFT JOIN {ARCHIVE_TABLE} a
              ON a.report_id = s.report_id
            WHERE s.pdf_hash IS NOT NULL
              AND encode(s.pdf_hash, 'hex') != $1
            ORDER BY encode(s.pdf_hash, 'hex'), s.report_id
            """,
            EMPTY_SHA256_HEX,
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


async def _fetch_pdf_url_duplicate_rows() -> list[dict]:
    conn = await asyncpg.connect(build_postgres_dsn())
    try:
        rows = await conn.fetch(
            f"""
            WITH duplicate_urls AS (
                SELECT
                    NULLIF(BTRIM(pdf_url), '') AS pdf_url_key,
                    COUNT(*) AS group_count,
                    MIN(report_id) AS canonical_report_id
                FROM {SOURCE_TABLE}
                WHERE NULLIF(BTRIM(pdf_url), '') IS NOT NULL
                  AND COALESCE(pdf_sync_status, 0) = 2
                GROUP BY NULLIF(BTRIM(pdf_url), '')
                HAVING COUNT(*) > 1
            )
            SELECT
                s.report_id,
                s.firm_nm,
                s.article_title AS title,
                s.report_date::text AS report_date,
                encode(s.pdf_hash, 'hex') AS pdf_hash_hex,
                NULLIF(BTRIM(s.pdf_url), '') AS pdf_url_key,
                d.group_count,
                d.canonical_report_id,
                a.file_path AS archive_file_path,
                a.storage_backend,
                a.storage_key,
                a.file_size,
                a.page_count,
                COALESCE(a.pdf_sync_status, a.sync_status, 0) AS archive_pdf_sync_status
            FROM duplicate_urls d
            JOIN {SOURCE_TABLE} s
              ON NULLIF(BTRIM(s.pdf_url), '') = d.pdf_url_key
            LEFT JOIN {ARCHIVE_TABLE} a
              ON a.report_id = s.report_id
            ORDER BY d.pdf_url_key, s.report_id
            """
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


def _scan_remote(remote: str) -> tuple[dict[int, list[dict]], list[dict]]:
    cmd = [_rclone_bin(), "lsf", "-R", "--format", "psh", "--files-only", remote]
    log("Scanning remote with rclone lsf -R --format psh")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"rclone failed with exit code {proc.returncode}")

    by_report_id: dict[int, list[dict]] = defaultdict(list)
    all_files: list[dict] = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split(";")
        path = _norm_path(parts[0])
        size = parts[1] if len(parts) > 1 else ""
        remote_hash = parts[2] if len(parts) > 2 else ""
        report_id = _extract_report_id(path)
        item = {
            "remote_path": path,
            "remote_size": size,
            "remote_hash": remote_hash,
            "remote_report_id": report_id or "",
        }
        all_files.append(item)
        if report_id is not None:
            by_report_id[report_id].append(item)

    return by_report_id, all_files


def _scan_remote_prefix(remote: str, prefix: str) -> dict[int, list[dict]]:
    remote_prefix = f"{remote.rstrip('/')}/{prefix.strip('/')}"
    cmd = [_rclone_bin(), "lsf", "-R", "--format", "ps", "--files-only", remote_prefix]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        log(f"skip prefix={prefix} err={proc.stderr.strip()}")
        return {}

    by_report_id: dict[int, list[dict]] = defaultdict(list)
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split(";")
        rel_path = _norm_path(parts[0])
        report_id = _extract_report_id(rel_path)
        if report_id is None:
            continue
        by_report_id[report_id].append(
            {
                "remote_path": f"{prefix.strip('/')}/{rel_path}",
                "remote_size": parts[1] if len(parts) > 1 else "",
                "remote_hash": "",
                "remote_report_id": report_id,
            }
        )
    return by_report_id


def _scan_affected_prefixes(remote: str, prefixes: list[str]) -> dict[int, list[dict]]:
    by_report_id: dict[int, list[dict]] = defaultdict(list)
    for idx, prefix in enumerate(prefixes, start=1):
        log(f"Scanning affected prefix {idx}/{len(prefixes)}: {prefix}")
        for report_id, items in _scan_remote_prefix(remote, prefix).items():
            by_report_id[report_id].extend(items)
    return by_report_id


def _choose_survivor(rows: list[dict], remote_by_id: dict[int, list[dict]], policy: str) -> dict:
    def has_remote(row: dict) -> int:
        return 0 if remote_by_id.get(int(row["report_id"])) else 1

    def is_done(row: dict) -> int:
        done = row.get("source_pdf_sync_status") == 2 or row.get("archive_pdf_sync_status") == 2
        return 0 if done else 1

    if policy == "newest-reg-dt":
        return sorted(rows, key=lambda r: (has_remote(r), is_done(r), str(r.get("report_date") or ""), int(r["report_id"])))[-1]
    return sorted(rows, key=lambda r: (has_remote(r), is_done(r), int(r["report_id"])))[0]


def _first_remote_path(row: dict, remote_by_id: dict[int, list[dict]]) -> str:
    paths = remote_by_id.get(int(row["report_id"])) or []
    if paths:
        return paths[0]["remote_path"]
    return row.get("archive_file_path") or row.get("storage_key") or ""


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _remote_hash_groups(all_files: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for item in all_files:
        remote_hash = str(item.get("remote_hash") or "").strip()
        size = str(item.get("remote_size") or "").strip()
        if remote_hash and remote_hash != "-":
            grouped[(size, remote_hash)].append(item)

    rows: list[dict] = []
    for (size, remote_hash), files in grouped.items():
        if len(files) < 2:
            continue
        keep = sorted(files, key=lambda f: (str(f.get("remote_report_id") or "999999999999"), f["remote_path"]))[0]
        for item in sorted(files, key=lambda f: f["remote_path"]):
            rows.append(
                {
                    "remote_hash": remote_hash,
                    "remote_size": size,
                    "group_count": len(files),
                    "keep_remote_path": keep["remote_path"],
                    "remote_path": item["remote_path"],
                    "remote_report_id": item.get("remote_report_id", ""),
                    "remote_action": "KEEP_REMOTE" if item["remote_path"] == keep["remote_path"] else "REVIEW_REMOTE_DUPLICATE",
                }
            )
    return rows


def build_plan(db_rows: list[dict], remote_by_id: dict[int, list[dict]], all_files: list[dict], policy: str) -> dict[str, list[dict]]:
    by_hash: dict[str, list[dict]] = defaultdict(list)
    for row in db_rows:
        by_hash[row["pdf_hash_hex"]].append(row)

    duplicate_groups: list[dict] = []
    alias_updates: list[dict] = []
    remote_deletes: list[dict] = []

    for pdf_hash_hex, rows in sorted(by_hash.items()):
        if len(rows) < 2:
            continue

        survivor = _choose_survivor(rows, remote_by_id, policy)
        survivor_id = int(survivor["report_id"])
        canonical_path = _first_remote_path(survivor, remote_by_id)
        duplicate_groups.append(
            {
                "pdf_hash_hex": pdf_hash_hex,
                "group_count": len(rows),
                "canonical_report_id": survivor_id,
                "canonical_remote_path": canonical_path,
                "canonical_firm_nm": survivor.get("firm_nm") or "",
                "canonical_title": survivor.get("title") or "",
                "canonical_report_date": survivor.get("report_date") or "",
            }
        )

        for row in rows:
            report_id = int(row["report_id"])
            if report_id == survivor_id:
                continue
            duplicate_paths = remote_by_id.get(report_id) or []
            alias_updates.append(
                {
                    "pdf_hash_hex": pdf_hash_hex,
                    "duplicate_report_id": report_id,
                    "canonical_report_id": survivor_id,
                    "canonical_remote_path": canonical_path,
                    "duplicate_archive_file_path": row.get("archive_file_path") or "",
                    "duplicate_storage_key": row.get("storage_key") or "",
                    "duplicate_remote_paths": "|".join(p["remote_path"] for p in duplicate_paths),
                    "intended_db_action": "POINT_ARCHIVE_METADATA_TO_CANONICAL_PATH",
                }
            )
            if canonical_path:
                for item in duplicate_paths:
                    if item["remote_path"] != canonical_path:
                        remote_deletes.append(
                            {
                                "pdf_hash_hex": pdf_hash_hex,
                                "duplicate_report_id": report_id,
                                "canonical_report_id": survivor_id,
                                "keep_remote_path": canonical_path,
                                "delete_remote_path": item["remote_path"],
                                "remote_size": item.get("remote_size", ""),
                                "remote_hash": item.get("remote_hash", ""),
                                "delete_after": "DB_ALIAS_UPDATE_AND_BACKUP_VERIFIED",
                            }
                        )

    return {
        "db_duplicate_groups": duplicate_groups,
        "db_alias_updates": alias_updates,
        "remote_delete_candidates": remote_deletes,
        "remote_hash_duplicate_groups": _remote_hash_groups(all_files),
    }


def build_pdf_url_plan(rows: list[dict], remote_by_id: dict[int, list[dict]]) -> dict[str, list[dict]]:
    by_url: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_url[row["pdf_url_key"]].append(row)

    groups: list[dict] = []
    alias_updates: list[dict] = []
    remote_deletes: list[dict] = []
    scope_prefixes: set[str] = set()

    for pdf_url_key, items in sorted(by_url.items()):
        if len(items) < 2:
            continue
        survivor = sorted(items, key=lambda row: int(row["report_id"]))[0]
        survivor_id = int(survivor["report_id"])
        canonical_paths = remote_by_id.get(survivor_id) or []
        verified_canonical_path = canonical_paths[0]["remote_path"] if canonical_paths else ""
        canonical_path = verified_canonical_path or (
            survivor.get("archive_file_path") or survivor.get("storage_key") or ""
        )

        groups.append(
            {
                "pdf_url": pdf_url_key,
                "group_count": len(items),
                "canonical_report_id": survivor_id,
                "canonical_remote_path": canonical_path,
                "canonical_firm_nm": survivor.get("firm_nm") or "",
                "canonical_title": survivor.get("title") or "",
                "canonical_report_date": survivor.get("report_date") or "",
                "canonical_pdf_hash_hex": survivor.get("pdf_hash_hex") or "",
            }
        )

        for row in items:
            month = _month_from_report_date(row.get("report_date"))
            firm = str(row.get("firm_nm") or "").strip()
            if month and firm:
                scope_prefixes.add(f"{month}/{firm}")

            report_id = int(row["report_id"])
            if report_id == survivor_id:
                continue

            duplicate_paths = remote_by_id.get(report_id) or []
            alias_updates.append(
                {
                    "pdf_url": pdf_url_key,
                    "duplicate_report_id": report_id,
                    "canonical_report_id": survivor_id,
                    "canonical_remote_path": canonical_path,
                    "duplicate_archive_file_path": row.get("archive_file_path") or "",
                    "duplicate_storage_key": row.get("storage_key") or "",
                    "duplicate_remote_paths": "|".join(p["remote_path"] for p in duplicate_paths),
                    "month_firm_scope": f"{month}/{firm}" if month and firm else "",
                    "intended_db_action": "POINT_ARCHIVE_METADATA_TO_MIN_REPORT_ID_FOR_SAME_PDF_URL",
                }
            )
            if verified_canonical_path:
                for item in duplicate_paths:
                    if item["remote_path"] != verified_canonical_path:
                        remote_deletes.append(
                            {
                                "pdf_url": pdf_url_key,
                                "duplicate_report_id": report_id,
                                "canonical_report_id": survivor_id,
                                "keep_remote_path": verified_canonical_path,
                                "delete_remote_path": item["remote_path"],
                                "remote_size": item.get("remote_size", ""),
                                "delete_after": "DB_ALIAS_UPDATE_AND_CANONICAL_ONEDRIVE_VERIFIED",
                            }
                        )

    return {
        "pdf_url_duplicate_groups": groups,
        "pdf_url_alias_updates": alias_updates,
        "pdf_url_remote_delete_candidates": remote_deletes,
        "pdf_url_remote_scope_prefixes": [{"remote_prefix": prefix} for prefix in sorted(scope_prefixes)],
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Build a non-destructive PDF content deduplication plan.")
    parser.add_argument("--remote", default=os.getenv("RCLONE_REMOTE", "onedrive:/archive/pdf"))
    parser.add_argument("--output-dir", default="tmp/dedup_plan")
    parser.add_argument("--no-rclone", action="store_true", help="Deprecated; DB-only planning is now the default.")
    parser.add_argument(
        "--scan-remote-full",
        action="store_true",
        help="Expensive: scan the entire remote with rclone lsf -R. Do not use for large archives.",
    )
    parser.add_argument(
        "--scan-affected-prefixes",
        action="store_true",
        help="Scan only YYYY-MM/firm prefixes affected by pdf_url duplicate groups.",
    )
    parser.add_argument(
        "--include-pdf-url",
        action="store_true",
        help="Also plan exact pdf_url duplicates where archived rows keep the lowest report_id.",
    )
    parser.add_argument("--survivor-policy", choices=("min-report-id", "newest-reg-dt"), default="min-report-id")
    args = parser.parse_args()

    db_rows = await _fetch_db_rows()
    log(f"Loaded DB rows with pdf_hash: {len(db_rows)}")

    remote_by_id: dict[int, list[dict]] = defaultdict(list)
    all_files: list[dict] = []
    if args.scan_remote_full:
        remote_by_id, all_files = _scan_remote(args.remote)
        log(f"Loaded remote files: {len(all_files)}")
    else:
        log("Skipping full remote scan. Use --scan-remote-full only for small or narrowed remotes.")

    plan = build_plan(db_rows, remote_by_id, all_files, args.survivor_policy)
    output_dir = Path(args.output_dir)

    pdf_url_plan = None
    if args.include_pdf_url:
        pdf_url_rows = await _fetch_pdf_url_duplicate_rows()
        log(f"Loaded archived exact pdf_url duplicate rows: {len(pdf_url_rows)}")
        if args.scan_affected_prefixes:
            pre_plan = build_pdf_url_plan(pdf_url_rows, {})
            prefixes = [row["remote_prefix"] for row in pre_plan["pdf_url_remote_scope_prefixes"]]
            affected_remote_by_id = _scan_affected_prefixes(args.remote, prefixes)
            pdf_url_plan = build_pdf_url_plan(pdf_url_rows, affected_remote_by_id)
        else:
            pdf_url_plan = build_pdf_url_plan(pdf_url_rows, {})

    _write_csv(
        output_dir / "db_duplicate_groups.csv",
        ["pdf_hash_hex", "group_count", "canonical_report_id", "canonical_remote_path", "canonical_firm_nm", "canonical_title", "canonical_report_date"],
        plan["db_duplicate_groups"],
    )
    _write_csv(
        output_dir / "db_alias_updates.csv",
        ["pdf_hash_hex", "duplicate_report_id", "canonical_report_id", "canonical_remote_path", "duplicate_archive_file_path", "duplicate_storage_key", "duplicate_remote_paths", "intended_db_action"],
        plan["db_alias_updates"],
    )
    _write_csv(
        output_dir / "remote_delete_candidates.csv",
        ["pdf_hash_hex", "duplicate_report_id", "canonical_report_id", "keep_remote_path", "delete_remote_path", "remote_size", "remote_hash", "delete_after"],
        plan["remote_delete_candidates"],
    )
    _write_csv(
        output_dir / "remote_hash_duplicate_groups.csv",
        ["remote_hash", "remote_size", "group_count", "keep_remote_path", "remote_path", "remote_report_id", "remote_action"],
        plan["remote_hash_duplicate_groups"],
    )
    if pdf_url_plan:
        _write_csv(
            output_dir / "pdf_url_duplicate_groups.csv",
            ["pdf_url", "group_count", "canonical_report_id", "canonical_remote_path", "canonical_firm_nm", "canonical_title", "canonical_report_date", "canonical_pdf_hash_hex"],
            pdf_url_plan["pdf_url_duplicate_groups"],
        )
        _write_csv(
            output_dir / "pdf_url_alias_updates.csv",
            ["pdf_url", "duplicate_report_id", "canonical_report_id", "canonical_remote_path", "duplicate_archive_file_path", "duplicate_storage_key", "duplicate_remote_paths", "month_firm_scope", "intended_db_action"],
            pdf_url_plan["pdf_url_alias_updates"],
        )
        _write_csv(
            output_dir / "pdf_url_remote_scope_prefixes.csv",
            ["remote_prefix"],
            pdf_url_plan["pdf_url_remote_scope_prefixes"],
        )
        _write_csv(
            output_dir / "pdf_url_remote_delete_candidates.csv",
            ["pdf_url", "duplicate_report_id", "canonical_report_id", "keep_remote_path", "delete_remote_path", "remote_size", "delete_after"],
            pdf_url_plan["pdf_url_remote_delete_candidates"],
        )

    log(f"Wrote plan CSVs to {output_dir}")
    log(
        "Summary: "
        f"db_duplicate_groups={len(plan['db_duplicate_groups'])} "
        f"db_alias_updates={len(plan['db_alias_updates'])} "
        f"remote_delete_candidates={len(plan['remote_delete_candidates'])} "
        f"remote_hash_duplicate_rows={len(plan['remote_hash_duplicate_groups'])}"
    )
    if pdf_url_plan:
        log(
            "pdf_url Summary: "
            f"groups={len(pdf_url_plan['pdf_url_duplicate_groups'])} "
            f"alias_updates={len(pdf_url_plan['pdf_url_alias_updates'])} "
            f"affected_prefixes={len(pdf_url_plan['pdf_url_remote_scope_prefixes'])} "
            f"remote_delete_candidates={len(pdf_url_plan['pdf_url_remote_delete_candidates'])}"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
