import asyncio
import asyncpg
import subprocess
import re
import unicodedata

from _bootstrap import build_postgres_dsn

async def test_orphan_search_single_month(month_folder="2026-04"):
    # 1. DB 모든 ID 로드
    conn = await asyncpg.connect(build_postgres_dsn())
    print(f"[1/3] 메인 DB에서 모든 리포트 ID 로드 중...")
    rows = await conn.fetch('SELECT report_id FROM tbl_sec_reports')
    active_ids = {int(r['report_id']) for r in rows if r['report_id'] is not None}
    await conn.close()
    print(f"      - 로드 완료: {len(active_ids)} 건")

    # 2. rclone 실행 (특정 월 폴더만 타겟팅)
    print(f"[2/3] {month_folder} 폴더 스캔 시작 (LS/이베스트 타겟팅)...")
    remote_path = f"onedrive:/archive/pdf/{month_folder}"
    
    rclone_cmd = [
        "rclone", "lsf", "-R", 
        "--include", "LS증권/**", 
        "--include", "이베스트투자증권/**", 
        "--files-only", 
        remote_path
    ]
    
    process = subprocess.run(rclone_cmd, capture_output=True, text=True)
    
    if process.returncode != 0:
        print(f"Error: {process.stderr}")
        return

    lines = process.stdout.splitlines()
    orphan_files = []
    id_pattern = re.compile(r'_(\d+)\.pdf$')

    # 3. 비교
    print(f"[3/3] 고아 파일 식별 중 (총 {len(lines)}개 파일)...")
    for line in lines:
        norm_path = unicodedata.normalize('NFC', line)
        match = id_pattern.search(norm_path)
        if match:
            r_id = int(match.group(1))
            if r_id not in active_ids:
                orphan_files.append(f"{month_folder}/{norm_path}")

    print(f"\n[테스트 결과 - {month_folder}]")
    print(f"- 발견된 LS/이베스트 파일: {len(lines)}개")
    print(f"- 식별된 고아 PDF: {len(orphan_files)}개")

    if orphan_files:
        print("\n[발견된 고아 파일 리스트]")
        for o in orphan_files:
            print(f"  ! {o}")
    else:
        print("\n- 해당 폴더에는 고아 파일이 없습니다.")

if __name__ == "__main__":
    import sys
    month = sys.argv[1] if len(sys.argv) > 1 else "2026-04"
    asyncio.run(test_orphan_search_single_month(month))
