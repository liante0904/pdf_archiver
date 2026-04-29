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

async def migrate():
    conn = await asyncpg.connect(build_postgres_dsn())
    print("Connected to DB.")
    
    try:
        # 1. 스키마 확장
        print("Expanding schema for 'tbl_sec_reports_pdf_archive'...")
        await conn.execute(f"""
            ALTER TABLE {SOURCE_REPORTS_TABLE}
            ADD COLUMN IF NOT EXISTS "pdf_sync_status" INTEGER DEFAULT 0;
        """)
        await conn.execute(f"""
            ALTER TABLE {PDF_ARCHIVE_TABLE} 
            ADD COLUMN IF NOT EXISTS "pdf_url" TEXT,
            ADD COLUMN IF NOT EXISTS "download_url" TEXT,
            ADD COLUMN IF NOT EXISTS "telegram_url" TEXT,
            ADD COLUMN IF NOT EXISTS "key" TEXT,
            ADD COLUMN IF NOT EXISTS "archive_status" TEXT,
            ADD COLUMN IF NOT EXISTS "file_name" TEXT,
            ADD COLUMN IF NOT EXISTS "download_status_yn" TEXT,
            ADD COLUMN IF NOT EXISTS "pdf_sync_status" INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS "sync_status" INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS "retry_count" INTEGER DEFAULT 0;
        """)
        await conn.execute(f"""
            ALTER TABLE {PDF_ARCHIVE_TABLE}
            DROP COLUMN IF EXISTS "attach_url";
        """)

        # 2. 데이터 풀 마이그레이션 (TBL_SEC_REPORTS -> tbl_sec_reports_pdf_archive)
        # 이미 존재하는 레코드는 정보를 업데이트하고, 없는 레코드는 새로 삽입합니다.
        print("Migrating all PDF related data from TBL_SEC_REPORTS...")
        
        # UPSERT Query
        upsert_query = f"""
            INSERT INTO {PDF_ARCHIVE_TABLE} (
                report_id, pdf_url, download_url, telegram_url, 
                key, archive_status, file_name, download_status_yn, 
                pdf_sync_status, sync_status, retry_count, file_path
            )
            SELECT 
                report_id, 
                "PDF_URL", 
                "DOWNLOAD_URL", 
                "TELEGRAM_URL", 
                "KEY", 
                "ARCHIVE_STATUS", 
                "ARCHIVE_FILE_NAME", 
                "DOWNLOAD_STATUS_YN", 
                COALESCE("pdf_sync_status", sync_status, 0), 
                sync_status, 
                retry_count,
                "ARCHIVE_PATH"
            FROM {SOURCE_REPORTS_TABLE}
            WHERE report_id IS NOT NULL
            ON CONFLICT (report_id) DO UPDATE SET
                pdf_url = EXCLUDED.pdf_url,
                download_url = EXCLUDED.download_url,
                telegram_url = EXCLUDED.telegram_url,
                key = EXCLUDED.key,
                archive_status = EXCLUDED.archive_status,
                file_name = EXCLUDED.file_name,
                download_status_yn = EXCLUDED.download_status_yn,
                pdf_sync_status = EXCLUDED.pdf_sync_status,
                sync_status = EXCLUDED.sync_status,
                retry_count = EXCLUDED.retry_count,
                file_path = COALESCE(NULLIF(EXCLUDED.file_path, ''), {PDF_ARCHIVE_TABLE}.file_path);
        """
        
        result = await conn.execute(upsert_query)
        print(f"Migration result: {result}")

        # 3. 불필요한 중복 컬럼 제거 (정규화)
        print("Removing redundant columns (firm_nm, title, reg_dt) from archive table...")
        await conn.execute(f"""
            ALTER TABLE {PDF_ARCHIVE_TABLE} 
            DROP COLUMN IF EXISTS "firm_nm",
            DROP COLUMN IF EXISTS "title",
            DROP COLUMN IF EXISTS "reg_dt";
        """)

        print("Finalizing...")
        total_count = await conn.fetchval(f"SELECT COUNT(*) FROM {PDF_ARCHIVE_TABLE}")
        print(f"Total rows in {PDF_ARCHIVE_TABLE}: {total_count}")
        
    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(migrate())
