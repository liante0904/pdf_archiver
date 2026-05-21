import asyncio
import asyncpg
import os
from secret_env import load_workspace_secret_env_defaults

load_workspace_secret_env_defaults()

async def run():
    conn = await asyncpg.connect(os.getenv('POSTGRES_URL'))
    rows = await conn.fetch("SELECT DISTINCT firm_nm FROM source_reports WHERE firm_nm LIKE '%미래%'")
    for row in rows:
        print(f"'{row['firm_nm']}'")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(run())
