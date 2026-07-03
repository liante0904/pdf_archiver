import asyncio
import os
import asyncpg
import logging
import sys
import shutil
from pathlib import Path

from _bootstrap import build_postgres_dsn

# 기존 archiver 모듈의 함수들을 가져오기 위해 경로 설정
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from pdf_archiver_async import PDFArchiver

async def archive_specific_ids(report_ids):
    logging.basicConfig(level=logging.INFO)
    archiver = PDFArchiver()
    
    # URL 직접 설정
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 5개 ID에 대한 정보만 직접 쿼리
    query = f"""
        SELECT report_id as id, report_id, sec_firm_order, key, pdf_url, telegram_url, download_url,
               firm_nm, article_title, report_date
        FROM tbl_sec_reports
        WHERE report_id = ANY($1)
    """
    rows = await conn.fetch(query, report_ids)
    print(f"Found {len(rows)} reports to archive.")

    for row in rows:
        success = await archiver.download_task(row)
        if success:
            print(f"Successfully downloaded: {row['report_id']}")
        else:
            print(f"Failed to download: {row['report_id']}")

    # 성공한 것들 DB 업데이트 및 업로드
    if archiver.success_downloads:
        print(f"Updating metadata for {len(archiver.success_downloads)} files...")
        for payload in archiver.success_downloads:
            # 윈도우 파일 시스템 호환을 위한 경로 정규화 (rclone 업로드 시의 경로)
            # archiver._make_file_path 가 생성하는 상대 경로 구조를 사용
            r_id = payload["report_id"]
            firm = payload["firm_nm"]
            title = payload["title"]
            path = payload["path"]
            size = payload["size"]
            pages = payload["pages"]
            report_date = payload["report_date"]
            rel_path = path.relative_to(archiver.local_dir)
            
            await conn.execute(
                'INSERT INTO "tbl_sec_reports_pdf_archive" (report_id, firm_nm, title, file_path, file_size, page_count, report_date) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (report_id) DO UPDATE SET file_path=EXCLUDED.file_path',
                int(r_id), firm, title, str(rel_path), size, pages, report_date
            )
            await conn.execute('UPDATE tbl_sec_reports SET sync_status = 2 WHERE report_id = $1', int(r_id))
        
        # rclone 업로드 (move)
        print("Uploading to OneDrive...")
        RCLONE_BIN = shutil.which("rclone") or "/usr/bin/rclone"
        RCLONE_REMOTE = os.getenv("RCLONE_REMOTE", "onedrive:/archive/pdf")
        
        # rclone move 실행
        import subprocess
        cmd = [RCLONE_BIN, "move", str(archiver.local_dir), RCLONE_REMOTE, "--transfers", "5", "--ignore-existing"]
        subprocess.run(cmd)
        print("Done.")
    else:
        print("No files were successfully downloaded.")

    await conn.close()

if __name__ == "__main__":
    ids = [1540, 116, 118, 119, 1495]
    asyncio.run(archive_specific_ids(ids))
