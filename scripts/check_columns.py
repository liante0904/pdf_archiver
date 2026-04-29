import asyncio
import asyncpg
import os

from _bootstrap import build_postgres_dsn

async def main():
    conn = await asyncpg.connect(build_postgres_dsn())
    for table in ['tbl_sec_reports', 'tbl_sec_reports_pdf_archive']:
        rows = await conn.fetch(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table}'
        """)
        print(f"Columns in {table}:")
        for row in rows:
            print(f" - {row['column_name']}")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
