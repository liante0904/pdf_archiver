# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""
흥국증권 OneDrive 잘못된 폴더 재배치

문제: report_date가 시간값으로 처리돼 0300-00/Unknown(28)/ 같은 이상한 폴더에
저장된 흥국증권 파일들을 올바른 YYYY-MM/흥국증권/ 경로로 이동하고
tbl_sec_reports_pdf_archive의 file_path, storage_key를 업데이트한다.

사용법:
  # dry-run (기본값 — 실제 변경 없음)
  python3 scripts/fix_heungkuk_folders.py

  # 실제 실행
  python3 scripts/fix_heungkuk_folders.py --execute
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import unicodedata
from pathlib import Path

RCLONE_BIN = (
    os.path.expanduser("~/.local/bin/rclone")
    if os.path.exists(os.path.expanduser("~/.local/bin/rclone"))
    else shutil.which("rclone") or "/usr/bin/rclone"
)
RCLONE_CONFIG = os.path.expanduser("~/.config/rclone/rclone.conf")
RCLONE_REMOTE = "onedrive:/archive/pdf"

PSQL_CONTAINER = "main-postgres"
PSQL_USER = "ssh_reports_hub"
PSQL_DB = "ssh_reports_hub"


# ── rclone 헬퍼 ─────────────────────────────────────────────────────────────

def rclone(*args):
    return subprocess.run(
        [RCLONE_BIN, "--config", RCLONE_CONFIG, *args],
        capture_output=True, text=True,
    )


# ── PostgreSQL 헬퍼 ──────────────────────────────────────────────────────────

def psql_rows(sql: str) -> list[list[str]]:
    """docker exec을 통해 psql 실행, 탭 구분 행 리스트 반환."""
    result = subprocess.run(
        ["docker", "exec", PSQL_CONTAINER, "psql",
         "-U", PSQL_USER, "-d", PSQL_DB,
         "-t", "-A", "-F", "\t",
         "-c", sql],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"[ERROR] psql 실패: {result.stderr.strip()}", file=sys.stderr)
        return []
    return [line.split("\t") for line in result.stdout.splitlines() if line.strip()]


def psql_exec(sql: str) -> bool:
    result = subprocess.run(
        ["docker", "exec", PSQL_CONTAINER, "psql",
         "-U", PSQL_USER, "-d", PSQL_DB,
         "-c", sql],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  [ERROR] psql 실패: {result.stderr.strip()}", file=sys.stderr)
        return False
    return True


# ── 파일명/경로 생성 (pdf_archiver_async._make_file_path 동일 로직) ───────────

def _build_remote_path(firm: str, title: str, report_date: str, report_id: int) -> tuple[str, str]:
    """(relative_path, y_m) 반환. relative_path = storage_key 값으로 사용."""
    clean_dt = re.sub(r"[^0-9]", "", str(report_date)) if report_date else "00000000"
    y_m = f"{clean_dt[:4]}-{clean_dt[4:6]}"
    yy_mm_dd = clean_dt[2:8]
    normalized = unicodedata.normalize("NFC", title or "")
    safe_title = re.sub(r'[\\/:*?"<>|!@#$%^&*.ⓒ,;\[\]\(\)]', " ", normalized)
    safe_title = "_".join(safe_title.split())[:60].strip("_") or "untitled"
    filename = f"{yy_mm_dd}_{safe_title}_{report_id}.pdf"
    return f"{y_m}/{firm}/{filename}", y_m


# ── 비정상 폴더 탐색 ─────────────────────────────────────────────────────────

def get_wrong_folders() -> list[str]:
    """연도가 2001~2030 범위 밖인 YYYY-MM 폴더를 비정상으로 분류."""
    res = rclone("lsf", "--dirs-only", RCLONE_REMOTE)
    if res.returncode != 0:
        print(f"[ERROR] rclone lsf 실패: {res.stderr.strip()}")
        sys.exit(1)
    wrong = []
    for line in res.stdout.splitlines():
        folder = line.strip("/")
        m = re.match(r"^(\d{4})-(\d{2})$", folder)
        if m and not (2001 <= int(m.group(1)) <= 2030):
            wrong.append(folder)
    return sorted(wrong)


def collect_files(wrong_folders: list[str]) -> list[tuple[str, str, str]]:
    """비정상 폴더 내 파일 목록: [(wrong_folder, subfolder, filename), ...]"""
    files = []
    for folder in wrong_folders:
        res = rclone("lsf", "--dirs-only", f"{RCLONE_REMOTE}/{folder}")
        if res.returncode != 0:
            continue
        for sub_line in res.stdout.splitlines():
            subfolder = sub_line.strip("/")
            res2 = rclone("lsf", f"{RCLONE_REMOTE}/{folder}/{subfolder}")
            if res2.returncode != 0:
                continue
            for fname in res2.stdout.splitlines():
                if fname.strip().endswith(".pdf"):
                    files.append((folder, subfolder, fname.strip()))
    return files


def extract_report_id(filename: str) -> int | None:
    m = re.search(r"_(\d+)\.pdf$", filename)
    return int(m.group(1)) if m else None


# ── 메타데이터 일괄 조회 ─────────────────────────────────────────────────────

def fetch_metadata(report_ids: list[int]) -> dict[int, dict]:
    if not report_ids:
        return {}
    ids_str = ",".join(str(i) for i in report_ids)
    rows = psql_rows(f"""
        SELECT report_id, firm_nm, report_date, article_title
        FROM tbl_sec_reports
        WHERE report_id IN ({ids_str})
    """)
    meta = {}
    for parts in rows:
        if len(parts) >= 4:
            meta[int(parts[0])] = {
                "firm_nm": parts[1],
                "report_date": parts[2],
                "title": parts[3],
            }
    return meta


# ── DB 업데이트 ──────────────────────────────────────────────────────────────

def update_db(report_id: int, storage_key: str):
    escaped = storage_key.replace("'", "''")
    psql_exec(f"""
        UPDATE tbl_sec_reports_pdf_archive
        SET file_path = '{escaped}',
            storage_key = '{escaped}'
        WHERE report_id = {report_id}
    """)
    print(f"  [DB] report_id={report_id} file_path/storage_key 업데이트 완료")


# ── 메인 ─────────────────────────────────────────────────────────────────────

def main(execute: bool):
    print("=" * 65)
    print("흥국증권 OneDrive 폴더 재배치 마이그레이션")
    print(f"모드: {'[실제 실행]' if execute else '[DRY-RUN — 변경 없음]'}")
    print("=" * 65)

    wrong_folders = get_wrong_folders()
    print(f"\n비정상 날짜 폴더 {len(wrong_folders)}개: {wrong_folders}")

    files = collect_files(wrong_folders)
    print(f"총 {len(files)}개 파일\n")

    if not files:
        print("처리할 파일 없음.")
        return

    report_ids = [extract_report_id(f) for _, _, f in files]
    report_ids = [r for r in report_ids if r]
    metadata = fetch_metadata(report_ids)

    ok = skip = err = 0
    for wrong_folder, subfolder, filename in files:
        report_id = extract_report_id(filename)
        if not report_id:
            print(f"[SKIP] report_id 추출 불가: {filename}")
            skip += 1
            continue

        meta = metadata.get(report_id)
        if not meta:
            print(f"[SKIP] DB 미조회: report_id={report_id}  ({filename})")
            skip += 1
            continue

        new_rel_path, _ = _build_remote_path(
            meta["firm_nm"], meta["title"], meta["report_date"], report_id
        )
        src = f"{RCLONE_REMOTE}/{wrong_folder}/{subfolder}/{filename}"
        dst = f"{RCLONE_REMOTE}/{new_rel_path}"

        tag = "MOVE" if execute else "DRY"
        print(f"[{tag}] report_id={report_id}")
        print(f"  FROM: {wrong_folder}/{subfolder}/{filename}")
        print(f"  TO:   {new_rel_path}")

        if execute:
            res = rclone("moveto", src, dst)
            if res.returncode != 0:
                print(f"  [ERROR] rclone moveto 실패: {res.stderr.strip()}")
                err += 1
                continue
            print(f"  [OK] rclone 이동 완료")
            update_db(report_id, new_rel_path)

        ok += 1

    print(f"\n{'─'*65}")
    print(f"결과: 처리={ok}  스킵={skip}  오류={err}")
    if not execute:
        print("\nDry-run 완료. 실제 실행하려면:")
        print("  python3 scripts/fix_heungkuk_folders.py --execute")
    else:
        # 빈 폴더 정리
        print("\n빈 폴더 정리 중...")
        for folder in wrong_folders:
            rclone("rmdirs", f"{RCLONE_REMOTE}/{folder}")
        print("완료.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="실제 변경 실행 (기본: dry-run)")
    args = parser.parse_args()
    main(execute=args.execute)
