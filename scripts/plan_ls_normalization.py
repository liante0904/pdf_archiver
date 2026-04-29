import asyncio
import asyncpg
import re
from urllib.parse import urlparse, parse_qs, urlunparse

from _bootstrap import build_postgres_dsn

def clean_ls_url(url):
    """URL에서 board_no와 board_seq만 남기고 나머지는 제거합니다."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    
    board_no = qs.get('board_no', [''])[0]
    board_seq = qs.get('board_seq', [''])[0]
    
    if not board_no or not board_seq:
        return url # 추출 실패 시 원본 유지
        
    return f"https://www.ls-sec.co.kr/EtwFrontBoard/View.jsp?board_no={board_no}&board_seq={board_seq}"

async def plan_ls_normalization():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 중복 그룹 추출
    query = """
        WITH normalized_reports AS (
            SELECT 
                report_id, reg_dt, key, firm_nm, article_title, "sync_status",
                LOWER(REGEXP_REPLACE(article_title, '\s+', '', 'g')) as norm_title
            FROM tbl_sec_reports
            WHERE (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
        ),
        dup_groups AS (
            SELECT norm_title, reg_dt
            FROM normalized_reports
            GROUP BY norm_title, reg_dt
            HAVING COUNT(*) > 1
        )
        SELECT n.* FROM normalized_reports n
        JOIN dup_groups d ON n.norm_title = d.norm_title AND n.reg_dt = d.reg_dt
        ORDER BY n.reg_dt DESC, n.norm_title, n.report_id
    """
    
    rows = await conn.fetch(query)
    
    groups = {}
    for r in rows:
        key = (r['norm_title'], r['reg_dt'])
        if key not in groups:
            groups[key] = []
        groups[key].append(dict(r))

    print(f"--- LS/이베스트 정규화 실행 계획 (ID 유지 + KEY 업데이트) ---")
    
    update_list = [] # (survivor_id, new_key)
    delete_ids = []
    
    for key, members in groups.items():
        # ID 순으로 정렬 (낮은 순)
        members.sort(key=lambda x: x['report_id'])
        survivor = members[0]
        duplicates = members[1:]
        
        # KEY는 가장 높은 ID의 것을 가져와서 정규화 (사용자님 원칙)
        highest_id_member = members[-1]
        new_key = clean_ls_url(highest_id_member['key'])
        
        update_list.append((survivor['report_id'], new_key))
        delete_ids.extend([d['report_id'] for d in duplicates])
        
        # 상위 5개 그룹만 예시 출력
        if len(update_list) <= 5:
            print(f"\n[그룹] {survivor['reg_dt']} | {survivor['article_title']}")
            print(f"  √ 유지(Survivor): ID {survivor['report_id']}")
            print(f"    - Old KEY: {survivor['key']}")
            print(f"    - New KEY: {new_key} (from ID {highest_id_member['report_id']})")
            for d in duplicates:
                print(f"  × 삭제(Delete): ID {d['report_id']}")

    print(f"\n" + "="*50)
    print(f"총 정리 대상 그룹: {len(groups)}개")
    print(f"업데이트 대상 (낮은 ID): {len(update_list)}건")
    print(f"삭제 대상 (높은 ID): {len(delete_ids)}건")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(plan_ls_normalization())
