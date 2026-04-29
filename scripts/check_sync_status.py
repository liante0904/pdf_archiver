import asyncio
import os
import asyncpg

from _bootstrap import build_postgres_dsn

async def check_sync_status(report_ids):
    postgres_url = build_postgres_dsn()
    try:
        conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print(f"{'ID':<10} | {'Status':<10} | {'Retries':<10}")
    print("-" * 40)

    rows = await conn.fetch(
        'SELECT report_id, sync_status, retry_count FROM tbl_sec_reports WHERE report_id = ANY($1)',
        report_ids
    )
    
    for row in rows:
        print(f"{row['report_id']:<10} | {row['sync_status']:<10} | {row['retry_count']:<10}")

    await conn.close()

if __name__ == "__main__":
    ids = [1540, 116, 118, 119, 1495]
    asyncio.run(check_sync_status(ids))
