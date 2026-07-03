"""
DB금융투자(DBfi) 라이브 스모크 테스트 스크립트

이 스크립트는 DB금융투자의 리포트 다운로드 기능이 정상 작동하는지 테스트합니다:
1. DB에서 가장 최신(또는 지정된) DB금융투자 리포트 레코드를 가져옵니다.
2. `pdf_archiver_async` 모듈의 `download_dbfi_pdf` 함수를 호출하여 실제 다운로드를 시도합니다.
3. 다운로드 성공 여부를 출력하며, 테스트용으로 받은 파일은 옵션에 따라 삭제하거나 유지할 수 있습니다.
"""
import argparse
import asyncio
import os
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("LOG_FILE", "/tmp/pdf_archiver_smoke.log")

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pdf_archiver_async as archiver  # noqa: E402


def _row_get(row, key):
    if isinstance(row, dict):
        return row.get(key)
    try:
        return row[key]
    except Exception:
        return None


async def fetch_dbfi_row(report_id=None):
    print("fetch_dbfi_row: opening DB")
    conn = await archiver.get_db_connection()
    try:
        if report_id:
            sql = """
                SELECT report_id, sec_firm_order, report_unique_key, pdf_url, telegram_url,
                       download_url, firm_nm, article_title, report_date
                FROM tbl_sec_reports
                WHERE report_id = $1
            """
            row = await conn.fetchrow(sql, int(report_id))
        else:
            sql = """
                SELECT report_id, sec_firm_order, report_unique_key, pdf_url, telegram_url,
                       download_url, firm_nm, article_title, report_date
                FROM tbl_sec_reports
                WHERE sec_firm_order = 19
                  AND COALESCE(NULLIF(pdf_url, ''), NULLIF(report_unique_key, ''), NULLIF(telegram_url, ''),
                               NULLIF(download_url, '')) IS NOT NULL
                ORDER BY report_date DESC
                LIMIT 1
            """
            row = await conn.fetchrow(sql)
        print("fetch_dbfi_row: selected row")
        return row
    finally:
        await conn.close()


async def run_smoke(report_id=None, output_dir=None, keep_file=False):
    print("run_smoke: start")
    row = await fetch_dbfi_row(report_id=report_id)
    if not row:
        print("No DBfi row found.")
        return 1

    source_url = (
        _row_get(row, "report_unique_key")
        or _row_get(row, "pdf_url")
        or _row_get(row, "telegram_url")
        or _row_get(row, "download_url")
    )

    if not source_url:
        print("DBfi row has no usable source URL.")
        return 1

    report_id_value = _row_get(row, "report_id")
    title = _row_get(row, "article_title") or "DBfi Smoke"
    firm = _row_get(row, "firm_nm") or "DBfi"
    report_date = _row_get(row, "report_date") or ""

    if output_dir:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        out_dir = Path(tempfile.mkdtemp(prefix="dbfi-smoke-"))

    target_path = out_dir / f"{report_id_value}.pdf"
    print(f"run_smoke: downloading to {target_path}")
    result = await archiver.download_dbfi_pdf(
        source_url,
        target_path,
        title=title,
        report_id=report_id_value,
        firm=firm,
        report_date=report_date,
    )

    print("run_smoke: download call finished")
    print(f"report_id={report_id_value}")
    print(f"source_url={source_url}")
    print(f"target_path={target_path}")
    print(f"result={result}")

    if not result:
        return 2

    if not keep_file and target_path.exists():
        target_path.unlink()
    print("DBfi smoke test completed successfully.")
    return 0


def parse_args():
    parser = argparse.ArgumentParser(description="DBfi live smoke test for pdf_archiver")
    parser.add_argument("--report-id", type=str, default=None, help="Target report_id. If omitted, newest DBfi row is used.")
    parser.add_argument("--output-dir", type=str, default=None, help="Directory to write the downloaded PDF.")
    parser.add_argument("--keep-file", action="store_true", help="Keep the downloaded PDF file on disk.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(asyncio.run(run_smoke(report_id=args.report_id, output_dir=args.output_dir, keep_file=args.keep_file)))
