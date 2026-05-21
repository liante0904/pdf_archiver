"""
고속 고아 파일 탐색 스크립트 (V3)

이 스크립트는 rclone lsf의 결과를 스트리밍 방식으로 처리하여 메모리 사용량을 최소화하면서
DB에 존재하지 않는 OneDrive 파일(고아 파일)을 빠르게 찾아냅니다.
주로 LS증권/이베스트투자증권 폴더를 대상으로 합니다.
"""
import asyncio
import asyncpg
import subprocess
import re
import unicodedata
import sys

from _bootstrap import build_postgres_dsn

async def find_orphans_high_speed():
    # 1. DB 모든 ID 로드 (O(1) 조회를 위해 set 사용)
    conn = await asyncpg.connect(build_postgres_dsn())
    print("[1/3] 메인 DB에서 모든 리포트 ID 로드 중...")
    rows = await conn.fetch('SELECT report_id FROM tbl_sec_reports')
    active_ids = {int(r['report_id']) for r in rows if r['report_id'] is not None}
    await conn.close()
    print(f"      - 로드 완료: {len(active_ids)} 건")

    # 2. rclone 실행 (디렉토리 가지치기 필터 적용)
    # LS증권과 이베스트투자증권 폴더만 깊게 탐색
    print("[2/3] OneDrive 스캔 시작 (LS/이베스트 폴더만 타겟팅)...")
    rclone_cmd = [
        "rclone", "lsf", "-R", 
        "--fast-list",  # 목록 조회 속도 최적화
        "--include", "*/LS증권/**", 
        "--include", "*/이베스트투자증권/**", 
        "--files-only", 
        "onedrive:/archive/pdf"
    ]
    
    process = subprocess.Popen(rclone_cmd, stdout=subprocess.PIPE, text=True, bufsize=1)
    
    orphan_files = []
    total_scanned = 0
    id_pattern = re.compile(r'_(\d+)\.pdf$')

    # 3. 스트리밍 비교
    print("[3/3] 실시간 고아 파일 식별 중...")
    try:
        for line in process.stdout:
            total_scanned += 1
            full_path = line.strip()
            norm_path = unicodedata.normalize('NFC', full_path)
            
            match = id_pattern.search(norm_path)
            if match:
                r_id = int(match.group(1))
                if r_id not in active_ids:
                    orphan_files.append(norm_path)
            
            if total_scanned % 1000 == 0:
                print(f"      - 스캔 중... {total_scanned}개 파일 확인됨 (현재 고아 {len(orphan_files)}개)")

    except KeyboardInterrupt:
        process.terminate()
        print("\n중단되었습니다.")
        return

    print(f"\n[최종 결과]")
    print(f"- 스캔한 LS/이베스트 파일 수: {total_scanned}개")
    print(f"- 찾은 고아 PDF 수: {len(orphan_files)}개")

    if orphan_files:
        save_path = "tests/orphan_ls_pdfs.txt"
        with open(save_path, "w", encoding="utf-8") as f:
            for orphan in orphan_files:
                f.write(f"{orphan}\n")
        print(f"- 목록이 '{save_path}'에 저장되었습니다.")
        
        # 샘플 출력
        print("\n[고아 파일 샘플]")
        for o in orphan_files[:5]:
            print(f"  ! {o}")
    else:
        print("- 고아 파일이 발견되지 않았습니다.")

if __name__ == "__main__":
    asyncio.run(find_orphans_high_speed())
