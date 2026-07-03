import asyncio
import asyncpg
import re

from _bootstrap import build_postgres_dsn

async def handle_remaining_records():
    postgres_url = build_postgres_dsn()
    conn = await asyncpg.connect(postgres_url)
    
    # 1. ID 8693 삭제
    print(f"Deleting ID 8693 (Test)...")
    await conn.execute('DELETE FROM tbl_sec_reports WHERE report_id = 8693')
    await conn.execute('DELETE FROM "tbl_sec_reports_pdf_archive" WHERE report_id = 8693')
    
    # 2. ID 1989 제목으로 다른 레코드 찾기
    # 제목: [차용호 테크 미드/스몰캡] 하나마이크론(067310): 중장기적 성장에 주목
    target_title = "[차용호 테크 미드/스몰캡] 하나마이크론(067310): 중장기적 성장에 주목"
    norm_title = re.sub(r'\s+', '', target_title).lower()
    
    print(f"\nSearching for duplicates of: {target_title}")
    
    query = """
        SELECT report_id, report_date, key, "sync_status"
        FROM tbl_sec_reports
        WHERE LOWER(REGEXP_REPLACE(article_title, '\s+', '', 'g')) = $1
    """
    matches = await conn.fetch(query, norm_title)
    
    if matches:
        print(f"Found {len(matches)} matching records:")
        for m in matches:
            print(f" - ID: {m['report_id']} | Date: {m['report_date']} | Status: {m['sync_status']} | KEY: {m['key']}")
            
        # 1989 외에 다른 ID가 있다면 1989 삭제 제안
        if len(matches) > 1:
            print(f"\nID 1989 is a duplicate. Proposing deletion of ID 1989.")
    else:
        print("No other matching records found for ID 1989.")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(handle_remaining_records())
