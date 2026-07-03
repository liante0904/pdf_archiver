import asyncio
import asyncpg
import os
import sys
from pathlib import Path

from _bootstrap import build_postgres_dsn

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db_tables import PDF_ARCHIVE_TABLE, SOURCE_REPORTS_TABLE

async def migrate_data():
    try:
        conn = await asyncpg.connect(build_postgres_dsn())
        print("Connected to DB.")
        
        # ARCHIVE_PATH가 있는 데이터를 tbl_sec_reports_pdf_archive로 이관
        # ON CONFLICT를 사용하여 이미 존재하는 데이터는 건너뜀
        insert_query = f"""
            INSERT INTO {PDF_ARCHIVE_TABLE} (
                report_id, firm_nm, title, file_path, file_size, page_count, report_date,
                pdf_sync_status, sync_status, retry_count
            )
            SELECT 
                report_id, 
                firm_nm, 
                article_title, 
                archive_path, 
                0, -- file_size (알 수 없음)
                0, -- page_count (알 수 없음)
                report_date,
                2,
                2,
                0
            FROM {SOURCE_REPORTS_TABLE}
            WHERE archive_path IS NOT NULL
              AND report_id IS NOT NULL
            ON CONFLICT (report_id) DO NOTHING
        """
        
        print(f"Migrating data from {SOURCE_REPORTS_TABLE} to {PDF_ARCHIVE_TABLE}...")
        result = await conn.execute(insert_query)
        print(f"Migration complete: {result}")
        
        # 이관 후 총 건수 확인
        total = await conn.fetchval(f'SELECT COUNT(*) FROM {PDF_ARCHIVE_TABLE}')
        print(f"Total rows in {PDF_ARCHIVE_TABLE}: {total}")
        
        await conn.close()
    except Exception as e:
        print(f"Error during migration: {e}")

if __name__ == "__main__":
    asyncio.run(migrate_data())
