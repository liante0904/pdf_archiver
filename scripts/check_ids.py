import asyncio
import asyncpg

from _bootstrap import build_postgres_dsn

async def check_ids():
    conn = await asyncpg.connect(build_postgres_dsn())
    
    ids = [231812913, 231812912, 231819281, 231819280, 231819276]
    
    print("--- 5개 ID DB 존재 여부 확인 ---")
    rows = await conn.fetch('SELECT report_id, firm_nm, article_title FROM tbl_sec_reports WHERE report_id = ANY($1::bigint[])', ids)
    
    found_ids = [r['report_id'] for r in rows]
    for i in ids:
        if i in found_ids:
            row = next(r for r in rows if r['report_id'] == i)
            print(f"ID {i}: 존재함 ({row['article_title']})")
        else:
            print(f"ID {i}: DB에 없음 (이전에 삭제된 고아 ID)")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_ids())
