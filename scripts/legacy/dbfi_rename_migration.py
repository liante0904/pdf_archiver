# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "asyncpg",
# ]
# ///
"""
DB금융투자 → DB증권 사명 변경 마이그레이션

1. OneDrive: YYYY-MM/DB금융투자/ → YYYY-MM/DB증권/ (rclone move)
2. PostgreSQL tbl_sec_reports_pdf_archive: firm_nm, file_path 업데이트

사용법:
  # dry-run (기본값 — 실제 변경 없음)
  python3 scripts/dbfi_rename_migration.py

  # 실제 실행
  python3 scripts/dbfi_rename_migration.py --execute
"""

import asyncio
import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
import pdf_archiver_async as archiver

OLD_FIRM = "DB금융투자"
NEW_FIRM = "DB증권"

RCLONE_BIN = (
    os.getenv("RCLONE_BIN")
    or (os.path.expanduser("~/.local/bin/rclone") if os.path.exists(os.path.expanduser("~/.local/bin/rclone")) else None)
    or shutil.which("rclone")
    or "/usr/bin/rclone"
)
RCLONE_REMOTE = os.getenv("RCLONE_REMOTE", "onedrive:/archive/pdf")
RCLONE_CONFIG = os.getenv("RCLONE_CONFIG", os.path.expanduser("~/.config/rclone/rclone.conf"))


def rclone(*args):
    return subprocess.run(
        [RCLONE_BIN, "--config", RCLONE_CONFIG, *args],
        capture_output=True, text=True,
    )


def get_month_folders():
    res = rclone("lsf", "--dirs-only", RCLONE_REMOTE)
    if res.returncode != 0:
        print(f"[ERROR] rclone lsf failed: {res.stderr.strip()}")
        sys.exit(1)
    return [line.strip("/") for line in res.stdout.splitlines() if line.strip()]


def folder_exists(remote_path):
    res = rclone("lsf", "--dirs-only", remote_path)
    return res.returncode == 0 and bool(res.stdout.strip())


def count_files(remote_path):
    res = rclone("lsf", "-R", "--files-only", remote_path)
    if res.returncode != 0:
        return 0
    return len([l for l in res.stdout.splitlines() if l.strip()])


def migrate_remote(execute: bool):
    months = sorted(get_month_folders())
    print(f"\n[원격] 조사할 월별 폴더: {len(months)}개")

    total_moved = 0
    for month in months:
        src = f"{RCLONE_REMOTE}/{month}/{OLD_FIRM}"
        dst = f"{RCLONE_REMOTE}/{month}/{NEW_FIRM}"

        if not folder_exists(src):
            continue

        n = count_files(src)
        print(f"  [{month}] {OLD_FIRM}/ → {NEW_FIRM}/  ({n}개 파일)")

        if execute:
            res = rclone("move", src, dst, "--transfers", "5", "--delete-empty-src-dirs")
            if res.returncode != 0:
                print(f"    [ERROR] rclone move 실패: {res.stderr.strip()}")
            else:
                print(f"    [OK] 이동 완료")
                total_moved += n
        else:
            total_moved += n

    action = "이동 완료" if execute else "이동 예정 (dry-run)"
    print(f"\n[원격] {action}: 총 {total_moved}개 파일\n")


async def migrate_db(execute: bool):
    if archiver.DB_BACKEND != "postgres":
        print("[DB] SQLite 백엔드 — file_path 내 사명 치환 및 firm_nm 업데이트")
        import aiosqlite
        async with aiosqlite.connect(archiver.DB_PATH) as db:
            async with db.execute(
                "SELECT report_id, firm_nm, file_path FROM tbl_sec_reports_pdf_archive WHERE firm_nm = ?",
                (OLD_FIRM,),
            ) as cur:
                rows = await cur.fetchall()

            print(f"[DB] 업데이트 대상: {len(rows)}개 레코드")
            for report_id, firm_nm, file_path in rows:
                new_path = file_path.replace(f"/{OLD_FIRM}/", f"/{NEW_FIRM}/") if file_path else file_path
                print(f"  report_id={report_id}  {firm_nm} → {NEW_FIRM}")
                print(f"    path: {file_path}")
                print(f"    →    {new_path}")
                if execute:
                    await db.execute(
                        "UPDATE tbl_sec_reports_pdf_archive SET firm_nm=?, file_path=? WHERE report_id=?",
                        (NEW_FIRM, new_path, report_id),
                    )
            if execute:
                await db.commit()
        return

    # PostgreSQL
    conn = await archiver.get_db_connection()
    try:
        rows = await conn.fetch(
            'SELECT report_id, firm_nm, file_path FROM "tbl_sec_reports_pdf_archive" WHERE firm_nm = $1',
            OLD_FIRM,
        )
        print(f"[DB] 업데이트 대상: {len(rows)}개 레코드")
        for row in rows:
            new_path = row["file_path"].replace(f"/{OLD_FIRM}/", f"/{NEW_FIRM}/") if row["file_path"] else row["file_path"]
            print(f"  report_id={row['report_id']}  {row['firm_nm']} → {NEW_FIRM}")
            if execute:
                await conn.execute(
                    'UPDATE "tbl_sec_reports_pdf_archive" SET firm_nm=$1, file_path=$2 WHERE report_id=$3',
                    NEW_FIRM, new_path, row["report_id"],
                )
    finally:
        await conn.close()


async def main(execute: bool):
    print("=" * 60)
    print(f"사명 변경 마이그레이션: {OLD_FIRM} → {NEW_FIRM}")
    print(f"모드: {'[실제 실행]' if execute else '[DRY-RUN — 변경 없음]'}")
    print("=" * 60)

    print("\n--- 1단계: OneDrive 원격 폴더 이동 ---")
    migrate_remote(execute)

    print("--- 2단계: DB 메타데이터 업데이트 ---")
    await migrate_db(execute)

    print("\n" + "=" * 60)
    if execute:
        print("마이그레이션 완료.")
    else:
        print("Dry-run 완료. 실제 실행하려면: python3 scripts/dbfi_rename_migration.py --execute")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="실제로 변경 실행 (기본값: dry-run)")
    args = parser.parse_args()
    asyncio.run(main(execute=args.execute))
