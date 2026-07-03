import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def migrate_and_delete_ls():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    source_id = 231728318 # 정상 데이터(높은 ID)
    target_id = 1989      # 생존자(낮은 ID)
    
    print(f"Migrating data from {source_id} to {target_id} and deleting {source_id}...")
    
    async with conn.transaction():
        # 1. source의 데이터를 가져오기
        source_row = await conn.fetchrow('SELECT * FROM tbl_sec_reports WHERE report_id = $1', source_id)
        if not source_row:
            print(f"Source ID {source_id} not found.")
            return
            
        # 2. target(1989) 업데이트 (KEY, REG_DT 등 source의 정확한 정보로 덮어쓰기)
        await conn.execute("""
            UPDATE tbl_sec_reports 
            SET key = $1, report_date = $2, "sync_status" = $3, "retry_count" = $4
            WHERE report_id = $5
        """, source_row['key'], source_row['report_date'], source_row['sync_status'], source_row['retry_count'], target_id)
        
        # 3. 메타데이터 이관 (혹시 source에 파일 정보가 있다면)
        source_meta = await conn.fetchrow('SELECT * FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', source_id)
        if source_meta:
            await conn.execute("""
                INSERT INTO "tbl_sec_reports_pdf_archive" (report_id, firm_nm, title, file_path, file_size, page_count, report_date)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (report_id) DO UPDATE SET file_path = EXCLUDED.file_path
            """, target_id, source_meta['firm_nm'], source_meta['title'], source_meta['file_path'], 
                source_meta['file_size'], source_meta['page_count'], source_meta['report_date'])
        
        # 4. source(231728318) 삭제
        await conn.execute('DELETE FROM "tbl_sec_reports_pdf_archive" WHERE report_id = $1', source_id)
        await conn.execute('DELETE FROM tbl_sec_reports WHERE report_id = $1', source_id)
        
    print("Migration and deletion completed successfully.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(migrate_and_delete_ls())
