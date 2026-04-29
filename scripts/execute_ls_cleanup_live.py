import asyncio
import asyncpg
import subprocess
import re
import unicodedata
import os
from collections import defaultdict

from _bootstrap import build_postgres_dsn

def normalize_title(text):
    return re.sub(r'\s+', '', text).lower()

def extract_id(filename):
    match = re.search(r'_(\d+)\.pdf$', filename)
    return int(match.group(1)) if match else None

async def execute_live_cleanup():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    print("[1] DB 데이터 인덱싱 중...")
    # 전수 조사 및 논리적 매핑 구축
    rows = await conn.fetch("""
        SELECT report_id, firm_nm, article_title, reg_dt 
        FROM tbl_sec_reports 
        WHERE firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%'
    """)
    
    active_ids = set()
    logical_map = {} # (month, ntitle) -> lowest_id
    id_to_info = {}

    for r in rows:
        rid = int(r['report_id'])
        active_ids.add(rid)
        id_to_info[rid] = r
        month = f"{r['reg_dt'][:4]}-{r['reg_dt'][4:6]}"
        ntitle = normalize_title(r['article_title'])
        key = (month, ntitle)
        if key not in logical_map or rid < logical_map[key]:
            logical_map[key] = rid

    print(f"    - {len(active_ids)}개 레코드 로드 완료.")

    # 2. 월별 순회 및 실시간 처리
    months = [f"{y}-{m:02d}" for y in range(2021, 2027) for m in range(1, 13)]
    months = [m for m in months if "2021-01" <= m <= "2026-04"]

    print("\n[2] 월별 LS증권 실시간 정규화 시작...")

    for month in months:
        remote_base = f"onedrive:/archive/pdf/{month}/LS증권"
        cmd = ["rclone", "lsf", "-R", "--format", "psh", "--files-only", remote_base]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0: continue

        lines = proc.stdout.splitlines()
        if not lines: continue

        # 체크썸 그룹핑
        checksum_groups = defaultdict(list)
        for line in lines:
            parts = line.split(';')
            if len(parts) < 3: continue
            f_path, f_size, f_hash = parts[0], parts[1], parts[2]
            f_name = unicodedata.normalize('NFC', f_path)
            f_id = extract_id(f_name)
            checksum_groups[(f_size, f_hash)].append({
                'rel_path': f_name,
                'full_remote': f"{remote_base}/{f_name}",
                'id': f_id,
                'name': f_name
            })

        for (size, hsh), files in checksum_groups.items():
            # --- Case 1 & 2: 중복 처리 ---
            if len(files) > 1:
                # 생존자 결정 (DB에 있는 가장 낮은 ID)
                alive_files = [f for f in files if f['id'] in active_ids]
                alive_files.sort(key=lambda x: x['id'])
                
                if alive_files:
                    survivor = alive_files[0]
                else:
                    # DB에 하나도 없으면 논리적으로 매칭되는 ID가 있는지 확인
                    f = files[0]
                    f_id = f['id']
                    f_name = f['name']
                    # 파일명에서 타이틀 추출 시도 (YYYYMMDD_TITLE_ID.pdf)
                    m = re.search(r'^\d+_(.*)_\d+\.pdf$', f_name)
                    ntitle = normalize_title(m.group(1)) if m else ""
                    best_id = logical_map.get((month, ntitle))
                    if best_id:
                        # 이 파일들 중 하나를 이 ID로 리네임하고 나머지는 지울 것임
                        survivor = files[0]
                        survivor['target_id'] = best_id
                    else:
                        survivor = files[0] # 그냥 첫 번째 유지

                for f in files:
                    if f['full_remote'] == survivor.get('full_remote'):
                        # 생존자가 리네임이 필요한 경우
                        if 'target_id' in survivor:
                            new_name = survivor['name'].replace(str(survivor['id']), str(survivor['target_id']))
                            new_remote = f"{remote_base}/{new_name}"
                            print(f"  [RENAME] {month}: {survivor['id']} -> {survivor['target_id']} (Reason: Orphan with valid record)")
                            subprocess.run(["rclone", "moveto", survivor['full_remote'], new_remote])
                            # DB 메타데이터 업데이트 (있을 경우)
                            await conn.execute('UPDATE "tbl_sec_reports_pdf_archive" SET report_id = $1, file_path = $2 WHERE report_id = $3', 
                                             survivor['target_id'], f"{month}/LS증권/{new_name}", survivor['id'])
                        continue
                    
                    # 나머지 가비지 삭제
                    print(f"  [DELETE] {month}: {f['id']} (Reason: Duplicate checksum {hsh[:8]})")
                    subprocess.run(["rclone", "deletefile", f['full_remote']])

            # --- Case 3: 단일 고아 파일 처리 ---
            else:
                f = files[0]
                if f['id'] not in active_ids:
                    m = re.search(r'^\d+_(.*)_\d+\.pdf$', f['name'])
                    ntitle = normalize_title(m.group(1)) if m else ""
                    best_id = logical_map.get((month, ntitle))
                    
                    if best_id:
                        new_name = f['name'].replace(str(f['id']), str(best_id))
                        new_remote = f"{remote_base}/{new_name}"
                        print(f"  [RENAME] {month}: {f['id']} -> {best_id} (Reason: Correcting ID for unique file)")
                        subprocess.run(["rclone", "moveto", f['full_remote'], new_remote])
                        # 메타데이터 업데이트/삽입
                        meta_exists = await conn.fetchval('SELECT 1 FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', f['id'])
                        if meta_exists:
                            await conn.execute('UPDATE "tbl_sec_reports_pdf_archive" SET report_id = $1, file_path = $2 WHERE report_id = $3', 
                                             best_id, f"{month}/LS증권/{new_name}", f['id'])
                        else:
                            # 메타데이터가 없으면 새로 생성 (필요시)
                            info = id_to_info.get(best_id)
                            if info:
                                await conn.execute('INSERT INTO "tbl_sec_reports_pdf_archive" (report_id, firm_nm, title, file_path, reg_dt) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',
                                                 best_id, info['firm_nm'], info['article_title'], f"{month}/LS증권/{new_name}", info['reg_dt'])

        print(f"  > {month} 처리 완료.")

    await conn.close()
    print("\n[모든 작업이 완료되었습니다.]")

if __name__ == "__main__":
    asyncio.run(execute_live_cleanup())
