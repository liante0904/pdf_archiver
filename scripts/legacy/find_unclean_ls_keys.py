import asyncio
import asyncpg
from urllib.parse import urlparse, parse_qs

from _bootstrap import build_postgres_dsn

async def find_unclean_ls_keys():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. LS/이베스트 관련 모든 레코드 조회
    query = """
        SELECT report_id, key, firm_nm, article_title
        FROM tbl_sec_reports
        WHERE (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
    """
    rows = await conn.fetch(query)
    
    unclean_records = []
    
    # 정규화된 표준 포맷 (정확히 이 형식이어야 함)
    # https://www.ls-sec.co.kr/EtwFrontBoard/View.jsp?board_no=XX&board_seq=YY
    
    for r in rows:
        url = r['key']
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        
        # 1. 도메인이 ebestsec 인 경우 (전환 누락)
        is_ebest_domain = 'ebestsec.co.kr' in parsed.netloc
        
        # 2. 파라미터 개수가 2개가 아니거나, board_no/board_seq 외의 것이 있는 경우
        extra_params = [k for k in qs.keys() if k not in ('board_no', 'board_seq')]
        has_extra = len(extra_params) > 0
        is_missing = 'board_no' not in qs or 'board_seq' not in qs
        
        # 3. URL 구조가 표준과 다른 경우 (View.jsp 가 아니거나 등)
        is_standard_path = parsed.path == '/EtwFrontBoard/View.jsp'
        
        if is_ebest_domain or has_extra or is_missing or not is_standard_path:
            unclean_records.append({
                'id': r['report_id'],
                'key': url,
                'firm': r['firm_nm'],
                'title': r['article_title'],
                'reason': f"Domain:{is_ebest_domain}, Extra:{extra_params}, Missing:{is_missing}, Path:{parsed.path}"
            })

    print(f"--- LS/이베스트 불필요 파라미터/비표준 KEY 조사 결과 ---")
    print(f"전체 조사 대상: {len(rows)}건")
    print(f"불필요 파라미터 포함 등 '지저분한' 레코드: {len(unclean_records)}건")
    
    if unclean_records:
        print("\n[상세 내역 예시 (상위 20건)]")
        for u in unclean_records[:20]:
            print(f"ID: {u['id']} | {u['reason']}")
            print(f"  URL: {u['key']}")
            print(f"  Title: {u['title'][:60]}")
            print("-" * 80)
            
    await conn.close()

if __name__ == "__main__":
    asyncio.run(find_unclean_ls_keys())
