import asyncio
import asyncpg
import subprocess
import re
import unicodedata
from collections import defaultdict

from _bootstrap import build_postgres_dsn

def normalize_text(text):
    return re.sub(r'\s+', '', text).lower()

async def analyze_orphan_actions():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    print("[1/3] DB 데이터 로드 중...")
    rows = await conn.fetch('SELECT report_id, firm_nm, article_title, report_date FROM tbl_sec_reports')
    
    id_to_record = {}
    logical_to_best_id = {} # (firm, title, date) -> lowest_id
    
    for r in rows:
        rid = int(r['report_id'])
        id_to_record[rid] = r
        firm = "LS" if "LS" in r['firm_nm'] or "이베스트" in r['firm_nm'] else r['firm_nm']
        ntitle = normalize_text(r['article_title'])
        key = (firm, ntitle, r['report_date'])
        
        if key not in logical_to_best_id or rid < logical_to_best_id[key]:
            logical_to_best_id[key] = rid
            
    await conn.close()

    print("[2/3] OneDrive 파일 목록 재스캔 중...")
    rclone_cmd = ["rclone", "lsf", "-R", "--fast-list", "--include", "*/LS증권/**", "--include", "*/이베스트투자증권/**", "--files-only", "onedrive:/archive/pdf"]
    proc = subprocess.run(rclone_cmd, capture_output=True, text=True)
    all_files = [unicodedata.normalize('NFC', f) for f in proc.stdout.splitlines()]
    
    # 파일명 -> 경로 맵 생성
    id_to_path = {}
    file_pattern = re.compile(r'(\d{6,8})_(.*)_(\d+)\.pdf$')
    
    for path in all_files:
        filename = path.split('/')[-1]
        match = file_pattern.search(filename)
        if match:
            f_id = int(match.group(3))
            id_to_path[f_id] = path

    print("[3/3] 실행 계획 수립 중...")
    actions = {
        'DELETE': [],  # 중복 가비지 (이미 정상 ID 파일이 있음)
        'RENAME': [],  # 이름 오류 (정상 ID 레코드는 있는데 파일이 없음)
        'KEEP': [],    # 진짜 고아 (DB에 레코드 없음)
    }

    for f_id, path in id_to_path.items():
        # 1. 정상 파일인가?
        if f_id in id_to_record:
            continue
            
        # 2. 논리적 짝을 찾기 위해 파일명 파싱
        filename = path.split('/')[-1]
        match = file_pattern.search(filename)
        f_date, f_title, _ = match.groups()
        if len(f_date) == 6: f_date = "20" + f_date
        
        firm = "LS"
        key = (firm, normalize_text(f_title), f_date)
        best_id = logical_to_best_id.get(key)
        
        if best_id:
            # 짝꿍(낮은 ID 레코드)이 DB에 있음
            if best_id in id_to_path:
                # Case A: 짝꿍 파일도 이미 존재함 -> 삭제 대상
                actions['DELETE'].append({'path': path, 'reason': f"Duplicate of ID {best_id}"})
            else:
                # Case B: 짝꿍 레코드는 있는데 파일이 없음 -> 리네임 대상
                new_path = path.replace(str(f_id), str(best_id))
                actions['RENAME'].append({'from': path, 'to': new_path, 'new_id': best_id})
        else:
            # Case C: DB에 아예 흔적도 없음 -> 일단 보존
            actions['KEEP'].append(path)

    print(f"\n--- 최종 스마트 클리닝 계획 ---")
    print(f"1. 삭제 대상 (중복): {len(actions['DELETE'])}개")
    print(f"2. 리네임 대상 (이름복구): {len(actions['RENAME'])}개")
    print(f"3. 보존 대상 (DB미등록): {len(actions['KEEP'])}개")

    if actions['DELETE']:
        print("\n[삭제 예시]")
        for d in actions['DELETE'][:5]: print(f"  - {d['path']} ({d['reason']})")
    
    if actions['RENAME']:
        print("\n[리네임 예시]")
        for r in actions['RENAME'][:5]: print(f"  - {r['from']} -> ..._{r['new_id']}.pdf")

    # 결과 저장
    import json
    with open("tests/cleaning_actions.json", "w", encoding="utf-8") as f:
        json.dump(actions, f, indent=2, ensure_ascii=False)
    print(f"\n상세 계획이 'tests/cleaning_actions.json'에 저장되었습니다.")

if __name__ == "__main__":
    asyncio.run(analyze_orphan_actions())
