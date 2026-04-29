import asyncio
import asyncpg
import os

from _bootstrap import build_postgres_dsn

async def check():
    conn = await asyncpg.connect(build_postgres_dsn())
    
    # 1. 컬럼 구조 확인
    columns = await conn.fetch("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'tbl_sec_reports'
        ORDER BY ordinal_position
    """)
    print('--- TBL_SEC_REPORTS Columns ---')
    for c in columns:
        print(f"{c['column_name']}: {c['data_type']}")
    
    # 2. 이관 가능성 확인 (sync_status=1 이지만 archive에 없는 것)
    missing_count = await conn.fetchval("""
        SELECT COUNT(*) FROM tbl_sec_reports 
        WHERE report_id IS NOT NULL 
          AND report_id NOT IN (SELECT report_id FROM tbl_sec_reports_pdf_archive)
    """)
    
    synced_missing = await conn.fetchval("""
        SELECT COUNT(*) FROM tbl_sec_reports 
        WHERE sync_status = 1
          AND report_id IS NOT NULL 
          AND report_id NOT IN (SELECT report_id FROM tbl_sec_reports_pdf_archive)
    """)
    
    print(f'\nTotal reports not in archive: {missing_count}')
    print(f'Synced reports (sync_status=1) not in archive: {synced_missing}')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check())
