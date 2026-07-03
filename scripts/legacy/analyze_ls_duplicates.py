import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def analyze_ls_duplicates():
    conn = await asyncpg.connect(build_postgres_dsn())
    
    # 1. (증권사, 제목, 날짜)가 동일하지만 report_id나 KEY가 다른 중복 항목들 찾기
    # LS증권과 이베스트 투자증권 대상
    query = """
        WITH dup_groups AS (
            SELECT firm_nm, article_title, report_date, COUNT(*) as group_count
            FROM tbl_sec_reports
            WHERE firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%'
            GROUP BY firm_nm, article_title, report_date
            HAVING COUNT(*) > 1
        )
        SELECT r.report_id, r.firm_nm, r.article_title, r.report_date, r.key, r.sync_status
        FROM tbl_sec_reports r
        INNER JOIN dup_groups d ON r.firm_nm = d.firm_nm 
                               AND r.article_title = d.article_title 
                               AND r.report_date = d.report_date
        ORDER BY r.report_date DESC, r.article_title
    """
    
    rows = await conn.fetch(query)
    
    print(f"--- LS/이베스트 중복 레코드 분석 결과 ---")
    print(f"중복 그룹에 속한 총 레코드 수: {len(rows)}개")
    
    if rows:
        print("\n[중복 사례 예시 (상위 5개 그룹)]")
        current_group = None
        group_count = 0
        for r in rows:
            group_key = (r['firm_nm'], r['article_title'], r['report_date'])
            if group_key != current_group:
                if group_count >= 5: break
                print(f"\n그룹: {r['report_date']} | {r['firm_nm']} | {r['article_title']}")
                current_group = group_key
                group_count += 1
            print(f"  - ID: {r['report_id']} | KEY: {r['key'][:50]}... | Status: {r['sync_status']}")
            
    await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_ls_duplicates())
