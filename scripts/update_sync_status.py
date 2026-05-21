"""
리포트 동기화 상태 업데이트 스크립트

지정된 report_id들에 대해 동기화 상태(sync_status)를 3(재처리 대기)으로 변경합니다:
- 주로 다운로드 실패나 유실된 파일을 아카이버가 다시 처리하도록 강제할 때 사용합니다.
- retry_count도 0으로 초기화하여 즉시 재시도되도록 합니다.
"""
import asyncio
import os
import asyncpg

from _bootstrap import build_postgres_dsn

async def update_to_reprocess(report_ids):
    postgres_url = build_postgres_dsn()
    try:
        conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print(f"Updating status to 3 for IDs: {report_ids}")
    
    result = await conn.execute(
        'UPDATE tbl_sec_reports SET sync_status = 3, retry_count = 0 WHERE report_id = ANY($1)',
        report_ids
    )
    
    print(f"Update result: {result}")
    await conn.close()

if __name__ == "__main__":
    ids = [1540, 116, 118, 119, 1495]
    asyncio.run(update_to_reprocess(ids))
