import asyncio
import asyncpg
import subprocess
import re
import unicodedata
from collections import defaultdict

from _bootstrap import build_postgres_dsn

def normalize_text(text):
    return re.sub(r'\s+', '', text).lower()

async def deep_orphan_scan():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    print("[1/3] DB 데이터 로드 및 인덱싱 중...")
    rows = await conn.fetch('SELECT report_id, firm_nm, article_title, reg_dt FROM tbl_sec_reports')
    
    active_ids = set()
    logical_map = defaultdict(list) # (firm, title, date) -> [ids]
    
    for r in rows:
        rid = int(r['report_id']) if r['report_id'] else None
        if rid:
            active_ids.add(rid)
            firm = "LS" if "LS" in r['firm_nm'] or "이베스트" in r['firm_nm'] else r['firm_nm']
            norm_title = normalize_text(r['article_title'])
            logical_map[(firm, norm_title, r['reg_dt'])].append(rid)
    
    await conn.close()
    print(f"      - DB 로드 완료: {len(active_ids)} 건")

    # 2. rclone 스캔 (LS/이베스트 전체)
    print("[2/3] OneDrive 스캔 시작 (LS/이베스트 타겟팅)...")
    rclone_cmd = [
        "rclone", "lsf", "-R", 
        "--include", "*/LS증권/**", 
        "--include", "*/이베스트투자증권/**", 
        "--files-only", 
        "onedrive:/archive/pdf"
    ]
    
    process = subprocess.Popen(rclone_cmd, stdout=subprocess.PIPE, text=True, bufsize=1)
    
    # {REG_DT}_{title}_{id}.pdf
    # 패턴: (\d{6})_(.*)_(\d+)\.pdf
    file_pattern = re.compile(r'(\d{6,8})_(.*)_(\d+)\.pdf$')
    
    to_delete = [] # 확실한 중복/가비지
    unknown_orphans = [] # DB에 흔적도 없는 파일
    
    total_scanned = 0
    print("[3/3] 딥 스캔 및 교차 검증 중...")

    try:
        for line in process.stdout:
            total_scanned += 1
            full_path = line.strip()
            norm_path = unicodedata.normalize('NFC', full_path)
            filename = norm_path.split('/')[-1]
            
            match = file_pattern.search(filename)
            if match:
                f_date, f_title, f_id = match.groups()
                f_id = int(f_id)
                # 날짜 8자리 맞추기 (YYMMDD -> 20YYMMDD)
                if len(f_date) == 6: f_date = "20" + f_date
                
                # 검증 1: ID가 살아있는가?
                if f_id in active_ids:
                    continue
                
                # 검증 2: ID는 죽었지만, 논리적으로 같은 리포트가 있는가?
                firm = "LS" # 타겟팅 스캔이므로 LS로 가정
                norm_f_title = normalize_text(f_title)
                
                if logical_map.get((firm, norm_f_title, f_date)):
                    # 같은 리포트가 다른 ID로 존재함! -> 확실한 가비지
                    to_delete.append({
                        'path': norm_path,
                        'reason': f"Duplicate (Alternative IDs: {logical_map[(firm, norm_f_title, f_date)]})"
                    })
                else:
                    # DB에 아예 없음 -> 진짜 고아
                    unknown_orphans.append(norm_path)
            
            if total_scanned % 1000 == 0:
                print(f"      - 확인 중... {total_scanned}개 파일 (가비지 {len(to_delete)}, 완전고아 {len(unknown_orphans)})")

    except KeyboardInterrupt:
        process.terminate()
        print("\n중단되었습니다.")
        return

    print(f"\n[최종 분석 완료]")
    print(f"- 총 스캔 파일: {total_scanned}개")
    print(f"- 삭제 확정 (중복 가비지): {len(to_delete)}개")
    print(f"- 주의 필요 (DB 미등록): {len(unknown_orphans)}개")

    if to_delete:
        with open("tests/ls_garbage_files.txt", "w", encoding="utf-8") as f:
            for item in to_delete:
                f.write(f"{item['path']} | {item['reason']}\n")
        print("- 삭제 대상 리스트가 'tests/ls_garbage_files.txt'에 저장되었습니다.")

    if unknown_orphans:
        with open("tests/ls_unknown_orphans.txt", "w", encoding="utf-8") as f:
            for item in unknown_orphans:
                f.write(f"{item}\n")
        print("- 주의 대상 리스트가 'tests/ls_unknown_orphans.txt'에 저장되었습니다.")

if __name__ == "__main__":
    asyncio.run(deep_orphan_scan())
