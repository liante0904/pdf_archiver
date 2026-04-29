import asyncio
import os
import asyncpg

from _bootstrap import build_postgres_dsn

async def get_report_details(report_ids):
    postgres_url = build_postgres_dsn()
    try:
        conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print(f"{'ID':<10} | {'Firm':<15} | {'Title':<40} | {'Reg Date'}")
    print("-" * 80)

    for r_id in report_ids:
        row = await conn.fetchrow(
            'SELECT report_id, firm_nm, article_title, reg_dt FROM tbl_sec_reports WHERE report_id = $1',
            r_id
        )
        if row:
            title = (row['article_title'][:37] + '..') if len(row['article_title']) > 37 else row['article_title']
            print(f"{row['report_id']:<10} | {row['firm_nm']:<15} | {title:<40} | {row['reg_dt']}")
        else:
            print(f"{r_id:<10} | Not found in TBL_SEC_REPORTS")

    await conn.close()

if __name__ == "__main__":
    ids = [1540, 116, 118, 119, 1495]
    asyncio.run(get_report_details(ids))
