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

async def migrate_status_2():
    conn = await asyncpg.connect(build_postgres_dsn())
    
    print("Migrating sync_status=2 records...")

    await conn.execute(f"""
        ALTER TABLE {PDF_ARCHIVE_TABLE}
        ADD COLUMN IF NOT EXISTS "pdf_hash" BYTEA,
        ADD COLUMN IF NOT EXISTS "title" TEXT,
        ADD COLUMN IF NOT EXISTS "author" TEXT,
        ADD COLUMN IF NOT EXISTS "has_text" BOOLEAN,
        ADD COLUMN IF NOT EXISTS "is_encrypted" BOOLEAN,
        ADD COLUMN IF NOT EXISTS "storage_backend" TEXT DEFAULT 'onedrive',
        ADD COLUMN IF NOT EXISTS "storage_key" TEXT,
        ADD COLUMN IF NOT EXISTS "last_accessed_at" TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS "created_at" TIMESTAMPTZ DEFAULT NOW(),
        ADD COLUMN IF NOT EXISTS "updated_at" TIMESTAMPTZ DEFAULT NOW(),
        ADD COLUMN IF NOT EXISTS "pdf_sync_status" INTEGER DEFAULT 0,
        ADD COLUMN IF NOT EXISTS "sync_status" INTEGER DEFAULT 0,
        ADD COLUMN IF NOT EXISTS "retry_count" INTEGER DEFAULT 0;
        """)
    
    # sync_status=2 이고 아직 아카이브 테이블에 없는 데이터 이관
    # file_path가 없더라도 우선 메타데이터 확보를 위해 이관 (필요시 'N/A' 또는 NULL 처리)
    insert_query = f"""
        INSERT INTO {PDF_ARCHIVE_TABLE} (
            report_id, firm_nm, title, file_path, file_size, page_count, reg_dt,
            pdf_sync_status, sync_status, retry_count
        )
        SELECT 
            report_id, 
            firm_nm, 
            article_title, 
            COALESCE(archive_path, pdf_url, 'N/A'), 
            0, 
            0, 
            reg_dt,
            2,
            2,
            0
        FROM {SOURCE_REPORTS_TABLE}
        WHERE sync_status = 2
          AND report_id IS NOT NULL
        ON CONFLICT (report_id) DO NOTHING
    """
    
    result = await conn.execute(insert_query)
    print(f"Migration result: {result}")
    
    total = await conn.fetchval(f'SELECT COUNT(*) FROM {PDF_ARCHIVE_TABLE}')
    print(f"Total rows in {PDF_ARCHIVE_TABLE} after migration: {total}")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(migrate_status_2())
