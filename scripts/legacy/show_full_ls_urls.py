import asyncio
import asyncpg
import re

from _bootstrap import build_postgres_dsn

async def show_full_ls_urls():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    query = """
        WITH normalized_reports AS (
            SELECT 
                report_id, report_date, key, firm_nm, article_title,
                LOWER(REGEXP_REPLACE(article_title, '\s+', '', 'g')) as norm_title
            FROM tbl_sec_reports
            WHERE firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%'
        ),
        dup_groups AS (
            SELECT norm_title, report_date
            FROM normalized_reports
            GROUP BY norm_title, report_date
            HAVING COUNT(*) > 1
        )
        SELECT n.* FROM normalized_reports n
        JOIN dup_groups d ON n.norm_title = d.norm_title AND n.report_date = d.report_date
        ORDER BY n.report_date DESC, n.norm_title, n.report_id
    """
    
    rows = await conn.fetch(query)
    
    print(f"--- LS/이베스트 중복 그룹 Full URL 상세 리스트 ---")
    
    current_group_key = None
    for r in rows:
        group_id = (r['norm_title'], r['report_date'])
        if group_id != current_group_key:
            print(f"\n" + "="*120)
            print(f"[그룹] 날짜: {r['report_date']} | 제목: {r['article_title']}")
            print("-"*120)
            current_group_key = group_id
        
        print(f"ID: {r['report_id']:<10} | KEY: {r['key']}")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(show_full_ls_urls())
