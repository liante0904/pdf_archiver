import asyncio
import asyncpg
import subprocess
import re
import unicodedata

from _bootstrap import build_postgres_dsn

async def find_orphans():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # TBL_SEC_REPORTS의 모든 report_id 로드
    rows = await conn.fetch('SELECT report_id FROM tbl_sec_reports')
    active_ids = {r['report_id'] for r in rows}
    await conn.close()
    
    print(f"[INFO] 메인 DB에 활성 상태인 전체 리포트 수: {len(active_ids)}건")
    print("[INFO] OneDrive에서 파일 목록을 가져오는 중... (시간이 다소 소요될 수 있습니다)")
    
    cmd = ["rclone", "lsf", "-R", "--files-only", "onedrive:/archive/pdf"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    
    if proc.returncode != 0:
        print(f"Error running rclone: {proc.stderr}")
        return
        
    lines = proc.stdout.splitlines()
    
    orphan_files = []
    ls_ebest_files = 0
    id_pattern = re.compile(r'_(\d+)\.pdf$')
    
    for line in lines:
        norm_line = unicodedata.normalize('NFC', line)
        if "LS" in norm_line or "이베스트" in norm_line:
            ls_ebest_files += 1
            match = id_pattern.search(norm_line)
            if match:
                r_id = int(match.group(1))
                if r_id not in active_ids:
                    orphan_files.append(norm_line)
                    
    print(f"\n--- 고아 PDF 탐색 결과 ---")
    print(f"원드라이브 내 LS/이베스트 관련 파일 수: {ls_ebest_files}개")
    print(f"DB에 없는 고아 파일 수: {len(orphan_files)}개")
    
    if orphan_files:
        with open("tests/orphan_pdfs_to_delete.txt", "w", encoding="utf-8") as f:
            for orphan in orphan_files:
                f.write(f"{orphan}\n")
        
        print(f"\n[고아 파일 예시 상위 10개]")
        for o in orphan_files[:10]:
            print(f" - {o}")
        print("\n전체 목록은 tests/orphan_pdfs_to_delete.txt 에 저장되었습니다.")

if __name__ == "__main__":
    asyncio.run(find_orphans())
