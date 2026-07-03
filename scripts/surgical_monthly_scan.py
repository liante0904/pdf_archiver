import asyncio
import asyncpg
import subprocess
import re
import unicodedata
from collections import defaultdict

from _bootstrap import build_postgres_dsn

def normalize_title(text):
    return re.sub(r'\s+', '', text).lower()

async def surgical_monthly_scan():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    print("[1/3] DB 데이터 로드 및 월별 인덱싱 중...")
    # LS/이베스트 전수 조사
    rows = await conn.fetch("""
        SELECT report_id, firm_nm, article_title, report_date 
        FROM tbl_sec_reports 
        WHERE firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%'
    """)
    
    db_by_month = defaultdict(lambda: {
        'ids': set(), 
        'logical_map': {} # (ntitle) -> lowest_id
    })
    
    for r in rows:
        rid = int(r['report_id'])
        report_date = r['report_date'] # YYYYMMDD
        month = f"{report_date[:4]}-{report_date[4:6]}" # YYYY-MM
        
        db_by_month[month]['ids'].add(rid)
        
        ntitle = normalize_title(r['article_title'])
        if ntitle not in db_by_month[month]['logical_map'] or rid < db_by_month[month]['logical_map'][ntitle]:
            db_by_month[month]['logical_map'][ntitle] = rid
            
    await conn.close()
    print(f"      - DB 로드 완료: {len(rows)} 건")

    # 2. 월별 폴더 순회 스캔
    print("[2/3] 월별 폴더 순회 및 정밀 대조 시작...")
    months = sorted(db_by_month.keys())
    
    file_pattern = re.compile(r'(\d{6,8})_(.*)_(\d+)\.pdf$')
    
    results = { 'DELETE': [], 'RENAME': [], 'KEEP': [] }
    total_files_scanned = 0

    for month in months:
        # 해당 월의 LS 관련 폴더만 리스팅
        # 원드라이브 구조: archive/pdf/YYYY-MM/LS증권/...
        remote_path = f"onedrive:/archive/pdf/{month}"
        
        # LS증권과 이베스트투자증권 폴더만 가져오기
        cmd = ["rclone", "lsf", "-R", "--fast-list", "--include", "LS증권/**", "--include", "이베스트투자증권/**", "--files-only", remote_path]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        
        if proc.returncode != 0: continue # 폴더가 없거나 에러 시 스킵
        
        month_files = [unicodedata.normalize('NFC', f) for f in proc.stdout.splitlines()]
        total_files_scanned += len(month_files)
        
        # 이 월에 존재하는 모든 파일 ID 세트 (중복 확인용)
        present_ids_in_month = set()
        for f in month_files:
            m = file_pattern.search(f.split('/')[-1])
            if m: present_ids_in_month.add(int(m.group(3)))

        # 정밀 대조
        for f_path in month_files:
            filename = f_path.split('/')[-1]
            match = file_pattern.search(filename)
            if not match: continue
            
            f_date, f_title, f_id = match.groups()
            f_id = int(f_id)
            
            # Case 1: DB에 있는 정상 파일
            if f_id in db_by_month[month]['ids']:
                continue
            
            # Case 2: DB에는 없지만, 동일 제목의 리포트가 해당 월에 있는지 확인
            ntitle = normalize_title(f_title)
            best_id = db_by_month[month]['logical_map'].get(ntitle)
            
            full_remote_path = f"{month}/{f_path}"
            
            if best_id:
                if best_id in present_ids_in_month:
                    # 정상 ID 파일이 이미 존재함 -> 확실한 삭제 대상
                    results['DELETE'].append({'path': full_remote_path, 'reason': f"Duplicate of {best_id}"})
                else:
                    # 정상 ID 레코드는 있는데 파일이 없음 -> 리네임 대상
                    new_filename = filename.replace(str(f_id), str(best_id))
                    new_path = full_remote_path.replace(filename, new_filename)
                    results['RENAME'].append({'from': full_remote_path, 'to': new_path, 'new_id': best_id})
            else:
                # DB에 아예 흔적도 없음 -> 보존
                results['KEEP'].append(full_remote_path)

        print(f"      - {month} 완료: {len(month_files)}개 확인됨")

    print(f"\n[최종 분석 완료]")
    print(f"- 스캔 파일 수: {total_files_scanned}개")
    print(f"- 가비지 삭제 대상: {len(results['DELETE'])}개")
    print(f"- 이름 복구 대상: {len(results['RENAME'])}개")
    print(f"- 보존 대상: {len(results['KEEP'])}개")

    # 결과 저장
    import json
    with open("tests/surgical_scan_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    if results['DELETE']:
        print(f"\n[삭제 대상 예시]")
        for d in results['DELETE'][:5]: print(f"  - {d['path']}")

if __name__ == "__main__":
    asyncio.run(surgical_monthly_scan())
