import asyncio
import asyncpg
import subprocess
import re
import unicodedata
from collections import defaultdict

from _bootstrap import build_postgres_dsn

def extract_id(filename):
    match = re.search(r'_(\d+)\.pdf$', filename)
    return int(match.group(1)) if match else None

async def divide_and_conquer_ls():
    conn = await asyncpg.connect(build_postgres_dsn())
    
    # 1. DB에서 모든 LS/이베스트 활성 ID를 메모리에 로드 (조회 속도 극대화)
    print("[1] DB에서 활성 ID 로드 중...")
    rows = await conn.fetch('SELECT report_id FROM tbl_sec_reports WHERE firm_nm LIKE \'%LS%\' OR firm_nm LIKE \'%이베스트%\'')
    active_ids = {int(r['report_id']) for r in rows}
    print(f"    - {len(active_ids)}개 ID 로드 완료.")

    # 2. 월별 순회 (2021-01 ~ 2026-04)
    months = [f"{y}-{m:02d}" for y in range(2021, 2027) for m in range(1, 13)]
    months = [m for m in months if "2021-01" <= m <= "2026-04"]

    print("\n[2] 월별 LS증권 폴더 정밀 스캔 시작...")
    
    all_delete_targets = []
    all_rename_targets = []

    for month in months:
        # LS증권 폴더만 직접 공략
        remote_path = f"onedrive:/archive/pdf/{month}/LS증권"
        
        # p: path, s: size, h: hash
        cmd = ["rclone", "lsf", "-R", "--format", "psh", "--files-only", remote_path]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        
        if proc.returncode != 0:
            continue # 폴더 없으면 패스

        lines = proc.stdout.splitlines()
        if not lines: continue

        # 체크썸 그룹핑: {(size, hash): [file_info, ...]}
        checksum_groups = defaultdict(list)
        for line in lines:
            parts = line.split(';')
            if len(parts) < 3: continue
            f_path, f_size, f_hash = parts[0], parts[1], parts[2]
            f_name = unicodedata.normalize('NFC', f_path)
            f_id = extract_id(f_name)
            
            checksum_groups[(f_size, f_hash)].append({
                'full_path': f"{month}/LS증권/{f_name}",
                'name': f_name,
                'id': f_id
            })

        month_delete = 0
        month_rename = 0

        # 그룹별 판별
        for (size, hsh), files in checksum_groups.items():
            # Case 1 & 2: 중복 파일 처리
            if len(files) > 1:
                # DB에 있는 ID들 중 가장 낮은 것을 생존자로 선택
                alive_files = [f for f in files if f['id'] in active_ids]
                alive_files.sort(key=lambda x: x['id'])
                
                if alive_files:
                    survivor = alive_files[0]
                else:
                    # DB에 하나도 없다면 그냥 목록의 첫 번째를 임시 생존자로 (나중에 리네임될 수도 있음)
                    survivor = files[0]

                for f in files:
                    if f['full_path'] != survivor['full_path']:
                        all_delete_targets.append(f['full_path'])
                        month_delete += 1
            
            # Case 3: 단일 파일인데 ID가 DB에 없는 경우 (리네임 혹은 고아)
            else:
                f = files[0]
                if f['id'] not in active_ids:
                    # 이 파일은 내용이 유니크한데 ID가 DB에 없음 -> 일단 기록 (리네임 후보)
                    all_rename_targets.append(f['full_path'])
                    month_rename += 1

        if month_delete > 0 or month_rename > 0:
            print(f"  - {month}: {len(lines)}개 파일 확인 (삭제대상: {month_delete}, 고아/리네임: {month_rename})")

    await conn.close()

    print(f"\n[최종 결과]")
    print(f"- 삭제 대상(중복/가비지): {len(all_delete_targets)}개")
    print(f"- 리네임/조사 대상(고아): {len(all_rename_targets)}개")

    # 결과 저장
    with open("tests/ls_delete_list.txt", "w", encoding="utf-8") as f:
        for t in all_delete_targets: f.write(f"{t}\n")
    with open("tests/ls_check_list.txt", "w", encoding="utf-8") as f:
        for t in all_rename_targets: f.write(f"{t}\n")
    
    print("\n상세 목록이 tests/ls_delete_list.txt 와 ls_check_list.txt 에 저장되었습니다.")

if __name__ == "__main__":
    asyncio.run(divide_and_conquer_ls())
