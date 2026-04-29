import asyncio
import asyncpg
import subprocess
import re
import unicodedata
from collections import defaultdict

from _bootstrap import build_postgres_dsn

async def checksum_based_scan():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    print("[1/3] DB 데이터 로드 중...")
    rows = await conn.fetch('SELECT report_id FROM tbl_sec_reports WHERE firm_nm LIKE \'%LS%\' OR firm_nm LIKE \'%이베스트%\'')
    active_ids = {int(r['report_id']) for r in rows}
    await conn.close()
    print(f"      - LS/이베스트 활성 ID {len(active_ids)}건 로드 완료")

    # 2. 2021년부터 월별 순회
    print("[2/3] 2021년부터 월별 체크썸 스캔 시작...")
    
    # { (size, hash): [path1, path2, ...] }
    duplicates_to_delete = []
    orphans_to_check = []
    
    # 현재부터 2021년까지 역순으로 (최신 가비지부터)
    years = [2026, 2025, 2024, 2023, 2022, 2021]
    months = [f"{y}-{m:02d}" for y in years for m in range(12, 0, -1)]
    # 2026년 4월 이후는 제외
    months = [m for m in months if m <= "2026-04" and m >= "2021-01"]

    file_pattern = re.compile(r'_(\d+)\.pdf$')

    for month in months:
        remote_path = f"onedrive:/archive/pdf/{month}"
        
        # p: path, s: size, h: hash
        cmd = [
            "rclone", "lsf", "-R", 
            "--include", "LS증권/**", 
            "--include", "이베스트투자증권/**", 
            "--format", "psh",
            "--files-only", 
            remote_path
        ]
        
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0: continue
        
        # 체크썸 맵: {(size, hash): [file_info, ...]}
        checksum_map = defaultdict(list)
        
        for line in proc.stdout.splitlines():
            parts = line.split(';')
            if len(parts) < 3: continue
            
            f_path, f_size, f_hash = parts[0], parts[1], parts[2]
            norm_path = unicodedata.normalize('NFC', f_path)
            
            match = file_pattern.search(norm_path)
            f_id = int(match.group(1)) if match else None
            
            file_info = {
                'full_path': f"{month}/{norm_path}",
                'id': f_id,
                'size': f_size,
                'hash': f_hash
            }
            
            checksum_map[(f_size, f_hash)].append(file_info)

        # 체크썸 그룹별 분석
        for (size, hsh), files in checksum_map.items():
            if len(files) > 1:
                # 중복 발견!
                # 1. DB에 있는 ID들 중 가장 낮은 것을 찾음
                alive_files = [f for f in files if f['id'] in active_ids]
                if alive_files:
                    alive_files.sort(key=lambda x: x['id'])
                    survivor = alive_files[0]
                    # 나머지는 모두 삭제 대상
                    for f in files:
                        if f['full_path'] != survivor['full_path']:
                            duplicates_to_delete.append(f['full_path'])
                else:
                    # DB에 하나도 없다면 모두 고아
                    orphans_to_check.extend([f['full_path'] for f in files])
            else:
                # 단일 파일인데 DB에 없는 경우
                f = files[0]
                if f['id'] not in active_ids:
                    orphans_to_check.append(f['full_path'])

        print(f"      - {month} 완료 (누적 삭제대상: {len(duplicates_to_delete)})")

    print(f"\n[최종 분석 완료]")
    print(f"- 내용 중복으로 인한 삭제 대상: {len(duplicates_to_delete)}개")
    print(f"- DB에 없는 고아 파일: {len(orphans_to_check)}개")

    # 결과 저장
    with open("tests/ls_checksum_duplicates.txt", "w", encoding="utf-8") as f:
        for d in duplicates_to_delete: f.write(f"{d}\n")
    
    with open("tests/ls_checksum_orphans.txt", "w", encoding="utf-8") as f:
        for o in orphans_to_check: f.write(f"{o}\n")

    if duplicates_to_delete:
        print("\n[삭제 대상 샘플 (내용 동일)]")
        for d in duplicates_to_delete[:10]: print(f"  - {d}")

if __name__ == "__main__":
    asyncio.run(checksum_based_scan())
