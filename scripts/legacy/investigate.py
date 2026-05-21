import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def investigate():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. 2021-01월 데이터 중 관련된 것들 조회
    rows = await conn.fetch("""
        SELECT report_id, reg_dt, firm_nm, article_title 
        FROM tbl_sec_reports 
        WHERE reg_dt LIKE '202101%' AND (firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%')
        ORDER BY reg_dt, article_title
    """)
    
    for r in rows:
        if '카카오' in r['article_title'] or 'LG이노텍' in r['article_title'] or '현대차' in r['article_title']:
            print(f"Active Record: ID {r['report_id']} | Date: {r['reg_dt']} | Title: {r['article_title']}")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(investigate())
