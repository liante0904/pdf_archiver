import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def check_ls_date_range():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    row = await conn.fetchrow("""
        SELECT MIN(reg_dt) as min_dt, MAX(reg_dt) as max_dt, COUNT(*) as cnt 
        FROM tbl_sec_reports 
        WHERE firm_nm LIKE '%LS%' OR firm_nm LIKE '%이베스트%'
    """)
    print(f"LS/이베스트 데이터 기간: {row['min_dt']} ~ {row['max_dt']} (총 {row['cnt']}건)")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_ls_date_range())
