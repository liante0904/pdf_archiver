import asyncio
import os
import asyncpg

from _bootstrap import build_postgres_dsn

async def update_to_reprocess(report_ids):
    postgres_url = build_postgres_dsn()
    try:
        conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print(f"Updating status to 3 for IDs: {report_ids}")
    
    result = await conn.execute(
        'UPDATE tbl_sec_reports SET sync_status = 3, retry_count = 0 WHERE report_id = ANY($1)',
        report_ids
    )
    
    print(f"Update result: {result}")
    await conn.close()

if __name__ == "__main__":
    ids = [1540, 116, 118, 119, 1495]
    asyncio.run(update_to_reprocess(ids))
