import asyncio
import asyncpg
import os
from secret_env import load_workspace_secret_env_defaults

load_workspace_secret_env_defaults()

async def run():
    conn = await asyncpg.connect(os.getenv('POSTGRES_URL'))
    # 최근 10개의 Mirae Asset 리포트를 조회합니다.
    rows = await conn.fetch("SELECT report_id, firm_nm, article_title, report_url, report_date FROM source_reports WHERE firm_nm = '미래에셋' ORDER BY report_date DESC LIMIT 10")
    for row in rows:
        print(f"ID: {row['report_id']}, Title: {row['article_title']}, URL: {row['report_url']}, Date: {row['report_date']}")
    
    # 특정 ID (2339548) 조회
    print("\nSearching for ID 2339548:")
    row = await conn.fetchrow("SELECT * FROM source_reports WHERE report_id = '2339548'")
    if row:
        for k, v in dict(row).items():
            print(f"{k}: {v}")
    else:
        print("ID 2339548 not found.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(run())
