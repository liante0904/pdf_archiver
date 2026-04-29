import asyncio
import os
import asyncpg
import subprocess
import re
import unicodedata

from _bootstrap import build_postgres_dsn

async def find_orphan_pdfs(firm_filter="LS"):
    # 1. DB에서 현재 관리 중인 모든 report_id 가져오기
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    rows = await conn.fetch('SELECT report_id FROM "tbl_sec_reports_pdf_archive"')
    active_ids = {int(r['report_id']) for r in rows}
    await conn.close()
    print(f"[INFO] DB에 등록된 리포트 수: {len(active_ids)}개")

    # 2. rclone을 통해 원격지의 파일 목록 가져오기
    # LS증권 폴더가 포함된 경로들을 조사 (예: 2024-05/LS증권/...)
    base_remote = "onedrive:/archive/pdf"
    print(f"[INFO] '{base_remote}'에서 '{firm_filter}' 관련 파일 목록을 가져오는 중...")
    
    cmd = ["rclone", "lsf", "-R", "--files-only", base_remote]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return

    all_files = result.stdout.splitlines()
    orphan_files = []
    
    # report_id 추출 패턴 (_12345.pdf)
    id_pattern = re.compile(r'_(\d+)\.pdf$')

    for file_path in all_files:
        # NFC 정규화 및 필터링
        norm_path = unicodedata.normalize('NFC', file_path)
        if firm_filter in norm_path:
            match = id_pattern.search(norm_path)
            if match:
                r_id = int(match.group(1))
                if r_id not in active_ids:
                    orphan_files.append(norm_path)

    print(f"\n[결과] {firm_filter} 관련 고아 파일 {len(orphan_files)}개 발견")
    print("-" * 60)
    for f in orphan_files[:20]: # 상위 20개만 출력
        print(f"  - {f}")
    if len(orphan_files) > 20:
        print(f"  ...외 {len(orphan_files)-20}개 더 있음")
    
    return orphan_files

if __name__ == "__main__":
    asyncio.run(find_orphan_pdfs("LS"))
