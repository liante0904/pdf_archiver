import asyncio
import os
import asyncpg
import logging
import sys
import shutil
import subprocess
from pathlib import Path

from _bootstrap import build_postgres_dsn

# 기존 archiver 모듈의 함수들을 가져오기 위해 경로 설정
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from pdf_archiver_async import PDFArchiver, get_pdf_page_count

async def archive_id_1495():
    logging.basicConfig(level=logging.INFO)
    archiver = PDFArchiver()
    
    # URL 직접 설정
    conn = await asyncpg.connect(build_postgres_dsn())
    
    # 1495 ID 정보 가져오기
    row = await conn.fetchrow('SELECT * FROM tbl_sec_reports WHERE report_id = 1495')
    if not row:
        print("Report ID 1495 not found in DB.")
        return

    # 사용자가 제공한 URL
    manual_url = "https://securities.miraeasset.com/bbs/download/2129909.pdf?attachmentId=2129909"
    
    target_path = archiver._make_file_path(row['firm_nm'], row['article_title'], row['reg_dt'], row['report_id'])
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading from: {manual_url}")
    # curl로 다운로드 시도 (User-Agent 추가)
    cmd = [
        "curl", "-L", "-k",
        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "-o", str(target_path),
        manual_url
    ]
    
    proc = subprocess.run(cmd, capture_output=True)
    
    if target_path.exists() and target_path.stat().st_size > 1024:
        print(f"Successfully downloaded 1495. Size: {target_path.stat().st_size}")
        pages = await get_pdf_page_count(target_path)
        
        rel_path = target_path.relative_to(archiver.local_dir)
        
        # DB 등록
        await conn.execute(
            'INSERT INTO "tbl_sec_reports_pdf_archive" (report_id, firm_nm, title, file_path, file_size, page_count, reg_dt) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (report_id) DO UPDATE SET file_path=EXCLUDED.file_path',
            1495, row['firm_nm'], row['article_title'], str(rel_path), target_path.stat().st_size, pages, row['reg_dt']
        )
        await conn.execute('UPDATE tbl_sec_reports SET sync_status = 2 WHERE report_id = 1495')
        
        # rclone 업로드
        print("Uploading to OneDrive...")
        RCLONE_BIN = shutil.which("rclone") or "/usr/bin/rclone"
        RCLONE_REMOTE = os.getenv("RCLONE_REMOTE", "onedrive:/archive/pdf")
        move_cmd = [RCLONE_BIN, "move", str(archiver.local_dir), RCLONE_REMOTE, "--ignore-existing"]
        subprocess.run(move_cmd)
        print("1495 Archived successfully.")
    else:
        print(f"Failed to download 1495. Error: {proc.stderr.decode()}")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(archive_id_1495())
